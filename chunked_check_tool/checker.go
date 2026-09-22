package main

import (
	"context"
	"errors"
	"regexp"

	"github.com/minio/minio-go/v7"
)

// chunkSigRe matches the streaming chunked-upload signature header that S3
// embeds in object bodies when a client uploads via streaming SigV4 with
// chunked transfer encoding. The first 128 bytes fetched via RangeGet are
// enough to see the first chunk header.
//
// Format: <hex-size>;chunk-signature=<64-hex>\r\n
// The chunk size is a hex string (at least one digit). The signature is
// exactly 64 lowercase or uppercase hex chars. The line terminator is \r\n
// or \n — both occur in practice depending on the client.
//
// Unanchored: boundary probes read [off_i-128, off_i+128] (256 bytes), so
// the next part's chunk header sits at byte 128 of the slice, not byte 0.
// The anchored form would miss every boundary match. False-positive risk
// is unchanged: the literal ;chunk-signature= + 64 hex + CR/LF is ~73
// chars of structure, ~zero realistic match in binary payload.
var chunkSigRe = regexp.MustCompile(`[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}[\r\n]`)

// trailerRe matches the aws-chunked trailer checksum marker emitted at the
// end of STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER and
// STREAMING-UNSIGNED-PAYLOAD-TRAILER uploads: 0\r\nx-amz-checksum-<algo>:...
// The unsigned-trailer variant has no ;chunk-signature= in the part head,
// so trailerRe is the ONLY strong feature that catches it — it must run on
// tail probes ([Size-128, Size]) and boundary probes ([off_i-128, off_i+128]).
//
// Algorithms: sha256, crc32, crc32c, sha1, crc64 (AWS 2024 SDK set).
// Explicit alternation, not [a-z0-9]+, to reject non-AWS x-amz-checksum-foo:.
// Unanchored: the trailer lands mid-slice in tail/boundary probes.
var trailerRe = regexp.MustCompile(`x-amz-checksum-(sha256|crc32|crc32c|sha1|crc64):`)

// extractHTTPStatusCode pulls the HTTP status code off err if it wraps a
// minio.ErrorResponse. Returns 0 when err carries no HTTP status (e.g.
// context.DeadlineExceeded, net.OpError) — the caller writes "N/A" in that
// case.
func extractHTTPStatusCode(err error) int {
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		return er.StatusCode
	}
	return 0
}

// extractS3Code pulls the S3 error Code string (e.g. "InvalidRange",
// "NoSuchKey") off err. Returns "" when err is not a minio.ErrorResponse.
// Distinct from the HTTP status code — the S3 Code carries semantic info
// that HTTP status doesn't (e.g. 404 could be NoSuchKey or NoSuchBucket).
func extractS3Code(err error) string {
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		return er.Code
	}
	return ""
}

// extractRequestID pulls the x-amz-request-id value off err if it wraps a
// minio.ErrorResponse. Returns "" for non-S3 errors (e.g. context timeout,
// net.OpError) — caller writes "N/A" in that case.
func extractRequestID(err error) string {
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		return er.RequestID
	}
	return ""
}

// isNormalETag reports whether etag is a normal (single-part) S3 ETag:
// exactly 32 lowercase hex characters. Anything else — uppercase hex,
// wrong length, a `-N` multipart suffix, empty — is treated as multipart
// (or at least "not a normal ETag") and skips the Range GET probe.
func isNormalETag(etag string) bool {
	if len(etag) != 32 {
		return false
	}
	for i := 0; i < 32; i++ {
		c := etag[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// Checker classifies a single VerifyTask: routes to corrupted/ok/failed
// outputs based on RangeGet probes (head/boundary/tail or whole-object
// fast-path) per buildProbes.
type Checker struct {
	// ctx bounds every S3 call the checker makes (HEAD probes, RangeGets).
	// It is the hard context of the two-stage shutdown: cancelled only on a
	// second signal, so a graceful drain (first signal) finishes normally,
	// while a hard abort fails in-flight probes fast — the error path then
	// records the task in check_failed/mp_check_failed, which is exactly the
	// -check-file retry input.
	ctx    context.Context
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

func NewChecker(ctx context.Context, worker S3API, out *Output, stats *Stats, cfg *Config) *Checker {
	return &Checker{ctx: ctx, worker: worker, out: out, stats: stats, cfg: cfg}
}

// Handle routes task to verify. The size==0 normal-object shortcut stays
// here (RangeGet on an empty body returns 416 → would misclassify as
// check_failed). Listed-counter bumps (list_obj/list_mp) are NOT done here —
// the S3 lister bumps them in check mode before pushing the task. File-input
// modes (-list-file / -check-file) do not bump them either (no S3 LIST);
// their summary reports read instead of list_all.
//
// HeadFirst (set by the file-input sources) HEADs the object to fill ETag/Size
// before probing — file input lines omit them, and Size is needed by
// buildProbesStatic to compute the tail probe (@Size-128). The HEAD ETag is
// also the authoritative object type: the line describes the object as it
// was during a previous run, and it may have been overwritten since.
//   - HEAD failure routes by the LINE's declared type (all we have):
//     regular → check_failed, multipart → mp_check_failed (offsets preserved
//     for the -check-file retry).
//   - drift to regular (line multipart, HEAD normal ETag): drop the stale
//     offsets and probe head+tail as a regular object.
//   - drift to multipart (line regular, HEAD multipart ETag): no offsets
//     available → verify() routes to mp.txt unverified (never claim clean).
func (c *Checker) Handle(task VerifyTask) {
	if task.HeadFirst {
		etag, size, err := c.worker.HeadObject(c.ctx, task.Key)
		if err != nil {
			if task.IsMultipart {
				c.out.WriteMpCheckFailed(task.Key, task.Offsets)
				c.out.WriteMpCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				c.stats.IncrMpCheckFailed()
			} else {
				c.out.WriteCheckFailed(task.Key)
				c.out.WriteCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				c.stats.IncrCheckFailed()
			}
			return
		}
		task.ETag = etag
		task.Size = size
		if headIsMultipart := !isNormalETag(etag); headIsMultipart != task.IsMultipart {
			task.IsMultipart = headIsMultipart
			if !headIsMultipart {
				task.Offsets = nil
			}
		}
	}
	if !task.IsMultipart && task.Size == 0 {
		c.stats.IncrOkObjects()
		if c.cfg.IsSuccessLog {
			c.out.WriteSuccess(task.OwnerID, task.Key)
		}
		return
	}
	c.verify(task)
}

// verify routes task to probe-and-route. Multipart + nil Offsets is the
// "multipart check off / unparseable offset etag" path → write to mp_all
// without claiming ok_mp. Otherwise buildProbes produces the probe plan
// for this object's shape (small-object fast-path / head+tail / head+
// boundary+tail) and probeAndRoute runs each probe; first settled
// (corrupted or failed) wins. All probes clean → ok_object / ok_mp.
func (c *Checker) verify(task VerifyTask) {
	if task.IsMultipart && len(task.Offsets) == 0 {
		if c.cfg.MultipartCheckMode == MultipartCheckModeOffset {
			c.out.WriteListParseFailedLog(task.Key, task.OwnerID, task.Size, task.ETag)
			c.out.WriteListParseFailed(task.OwnerID, task.Key)
			c.stats.IncrListParseFailed()
		} else {
			c.out.WriteMultipartAll(task.OwnerID, task.Key)
		}
		return
	}
	result, err := runProbes(c.ctx, c.worker, task, c.cfg.WholeObjectProbeThreshold)
	switch result {
	case probeCorrupted:
		if task.IsMultipart {
			c.out.WriteCorruptedMultipart(task.OwnerID, task.Key, task.Offsets)
			c.stats.IncrCorruptedMp()
		} else {
			c.out.WriteCorrupted(task.OwnerID, task.Key)
			c.stats.IncrCorruptedObjects()
		}
	case probeFailed:
		if task.IsMultipart {
			c.out.WriteMpCheckFailed(task.Key, task.Offsets)
			c.out.WriteMpCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			c.stats.IncrMpCheckFailed()
		} else {
			c.out.WriteCheckFailed(task.Key)
			c.out.WriteCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			c.stats.IncrCheckFailed()
		}
	case probeClean:
		if task.IsMultipart {
			if c.cfg.IsMultipartSuccessLog {
				c.out.WriteMultipartOk(task.OwnerID, task.Key)
			}
			c.stats.IncrOkMp()
		} else {
			if c.cfg.IsSuccessLog {
				c.out.WriteSuccess(task.OwnerID, task.Key)
			}
			c.stats.IncrOkObjects()
		}
	}
}

// probeResult is the outcome of running a VerifyTask's probe matrix.
type probeResult int

const (
	// probeClean: every probe succeeded and none matched chunkSigRe or
	// trailerRe. The object is not corrupted (by this tool's detection).
	probeClean probeResult = iota
	// probeCorrupted: at least one probe matched chunkSigRe or trailerRe.
	// The object is corrupted.
	probeCorrupted
	// probeFailed: a RangeGet returned an error before any probe matched.
	// err is the RangeGet error; the caller routes to check_failed /
	// mp_check_failed.
	probeFailed
)

// runProbes runs the probe matrix for task (head/boundary/tail or
// small-object whole-read) and returns the first non-clean result. All
// probes clean → probeClean. Any RangeGet error → probeFailed (err set, no
// further probes run — matches the early-exit behavior of the old per-probe
// loop). Any probe matching chunkSigRe OR trailerRe → probeCorrupted.
//
// Used by list-check and the file-input check modes (-list-file /
// -check-file), so all of them detect corruption identically: same probe
// positions (buildProbesStatic), same regexes (chunkSigRe || trailerRe), same
// early-exit on first settled result. Backup mode no longer probes — it
// relays whatever the check stage flagged. threshold is
// WholeObjectProbeThreshold from cfg (caller passes it in so runProbes has no
// cfg dependency).
func runProbes(ctx context.Context, worker S3API, task VerifyTask, threshold int) (probeResult, error) {
	for _, p := range buildProbesStatic(task, threshold) {
		body, err := worker.RangeGetAt(ctx, task.Key, p.start, p.length)
		if err != nil {
			return probeFailed, err
		}
		if chunkSigRe.Match(body) || trailerRe.Match(body) {
			return probeCorrupted, nil
		}
	}
	return probeClean, nil
}

// probeSpec is one RangeGet probe in a VerifyTask's plan.
type probeSpec struct {
	start  int64
	length int64
}

// probeWindow is the half-window size for head/tail/boundary probes.
// Head and tail read probeWindow bytes; boundary reads 2*probeWindow
// bytes straddling the part offset.
const probeWindow int64 = 128

// buildProbes returns the probe plan for task based on its shape:
//
//   - 0 < Size <= WholeObjectProbeThreshold → single RangeGetAt(0, thr)
//     reads the whole object; chunkSigRe OR trailerRe match against one
//     body. (Size==0 never reaches here — Handle short-circuits.)
//   - normal object, Size > threshold → head@0 + tail@Size-probeWindow.
//   - multipart N parts, Size > threshold → head@0 + (N-1) boundary
//     probes [off_i-probeWindow, off_i+probeWindow] + tail@Size-probeWindow.
//
// Tail and boundary probes clamp start to 0 and length to Size when the
// object is smaller than the window — the small-object fast-path already
// handles tiny objects, but Size can still be < probeWindow in the normal
// branch (e.g. Size=200, threshold=1024: 2 probes, second reads [72,200)).
func (c *Checker) buildProbes(task VerifyTask) []probeSpec {
	return buildProbesStatic(task, c.cfg.WholeObjectProbeThreshold)
}

// buildProbesStatic is the package-level probe planner shared by list-check
// and the file-input check modes (-list-file / -check-file). threshold is
// WholeObjectProbeThreshold (0 disables the small-object fast-path).
func buildProbesStatic(task VerifyTask, threshold int) []probeSpec {
	thr := int64(threshold)
	if task.Size > 0 && thr > 0 && task.Size <= thr {
		return []probeSpec{{0, thr}}
	}
	if !task.IsMultipart {
		return headTail(task.Size, probeWindow)
	}
	offs := task.Offsets
	out := make([]probeSpec, 0, len(offs)+1)
	out = append(out, probeSpec{0, probeWindow}) // head @ part 0
	for i := 1; i < len(offs); i++ {             // internal boundaries
		s := offs[i] - probeWindow
		if s < 0 {
			s = 0
		}
		e := offs[i] + probeWindow
		if task.Size > 0 && e > task.Size {
			e = task.Size
		}
		if e > s {
			out = append(out, probeSpec{s, e - s})
		}
	}
	if task.Size > 0 { // tail (skip degenerate Size==0)
		// Only append the tail — head@0 was already added at line 252.
		// Calling headTail here would duplicate the head probe.
		out = append(out, tailProbe(task.Size, probeWindow))
	}
	return out
}

// headTail returns the head and tail probes for a non-multipart object.
// Tail start clamps to 0 for Size < probeWindow; tail length clamps to Size.
func headTail(size, probe int64) []probeSpec {
	return []probeSpec{{0, probe}, tailProbe(size, probe)}
}

// tailProbe returns the tail probe spec for an object of `size` bytes with
// the given probe window. Tail start clamps to 0 for Size < probeWindow;
// tail length clamps to Size. Used by headTail (non-multipart path) and
// directly by the multipart path (which adds head@0 separately at line 252
// and only needs the tail appended after boundary probes — calling headTail
// there would duplicate the head probe).
func tailProbe(size, probe int64) probeSpec {
	tailStart := size - probe
	if tailStart < 0 {
		tailStart = 0
	}
	tailLen := probe
	if size < probe {
		tailLen = size
	}
	return probeSpec{tailStart, tailLen}
}

// probeAndRoute removed: runProbes + verify's switch replaced it. The old
// per-probe early-exit loop (probeAndRoute returning bool to break) is now
// inside runProbes, which returns the first non-clean result and lets the
// caller (verify / verifyCorrupt) do mode-specific routing.
