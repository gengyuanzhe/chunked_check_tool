package main

import (
	"context"
	"errors"
	"regexp"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
)

// chunkSigRe matches the streaming chunked-upload signature header embedded
// in object bodies when a client uploads via streaming SigV4 with chunked
// transfer encoding. Format: <hex-size>;chunk-signature=<64-hex>\r\n
// CRLF is mandatory (RFC 7230 chunked; the server writes the body verbatim).
// Unanchored so it matches whether the header lands at byte 0 of a probe
// slice or mid-slice.
var chunkSigRe = regexp.MustCompile(`[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}\r\n`)

// trailerRe matches the aws-chunked trailer end-structure of a
// STREAMING-UNSIGNED-PAYLOAD-TRAILER upload:
//
//	0\r\nx-amz-checksum-<algo>:<value>\r\n\r\n
//
// No leading \r\n: an empty payload (payloadSize==0) has no data chunk, so
// AWS SDK v2 emits it with no preceding CRLF — requiring one would miss
// empty objects. <algo> is [a-z0-9]+ (not an explicit list) so future AWS
// algorithms won't evade detection; <value> is [^\r\n]* (bounded by ':' and
// \r\n\r\n).
//
// Only single-PUT trailer uploads land on private storage; multipart parts
// encoded as trailer uploads are rejected (root AGENTS.md §1), so trailerRe
// runs only on single-PUT probe paths (see matchFunc). The signed -TRAILER
// variant is not matched (its value is followed by x-amz-trailer-signature,
// not \r\n\r\n) but is caught by chunkSigRe.
var trailerRe = regexp.MustCompile(`0\r\nx-amz-checksum-[a-z0-9]+:[^\r\n]*\r\n\r\n`)

// extractHTTPStatusCode pulls the HTTP status code off err if it wraps an
// *awshttp.ResponseError (every S3 protocol error from the aws SDK does).
// Returns 0 when err carries no HTTP response (e.g. context.DeadlineExceeded,
// net.OpError) — the caller writes "N/A" in that case.
func extractHTTPStatusCode(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.Response.StatusCode
	}
	return 0
}

// extractS3Code pulls the S3 error Code string (e.g. "NoSuchKey",
// "InvalidRange") off err if it wraps a smithy.APIError. Returns "" for
// non-S3 errors (e.g. context timeout, net.OpError).
// Distinct from the HTTP status code — the S3 Code carries semantic info
// that HTTP status doesn't (e.g. 404 could be NoSuchKey or NoSuchBucket).
func extractS3Code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// extractRequestID pulls the x-amz-request-id value off err if it wraps an
// *awshttp.ResponseError. Returns "" for non-S3 errors (e.g. context timeout,
// net.OpError) — caller writes "N/A" in that case.
func extractRequestID(err error) string {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.Response.Header.Get("x-amz-request-id")
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
	// journal tracks scan progress for kill -9 resume (Mode 2 S3 listing
	// only; nil = off).
	journal *JournalTracker
}

func NewChecker(ctx context.Context, worker S3API, out *Output, stats *Stats, cfg *Config) *Checker {
	return &Checker{ctx: ctx, worker: worker, out: out, stats: stats, cfg: cfg}
}

// SetJournal wires the progress tracker. Setter (not a constructor param)
// to keep existing test constructions untouched.
func (c *Checker) SetJournal(j *JournalTracker) {
	c.journal = j
}

// Handle routes task to verify. The size==0 normal-object shortcut stays
// here (RangeGet on an empty body returns 416 → would misclassify as
// check_failed). Listed-counter bumps (list_obj/list_mp) are NOT done here —
// the S3 lister bumps them in check mode before pushing the task. The
// file-input mode (-check-file) does not bump them either (no S3 LIST);
// its summary reports read instead of list_all.
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
	if c.journal != nil {
		// Terminal AFTER the body pushed the result line into its output
		// channel: the deferred call runs after every write in Handle, so
		// the tracker only counts terminals whose lines are already queued
		// — the precondition of the checkpoint's snapshot-then-flush
		// ordering (see runJournalCheckpoints).
		defer c.journal.Terminal(task.JournalPrefix, task.PageSeq)
	}
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
		c.out.WriteSuccess(task.OwnerID, task.Key) // Output gates it on the success-log switch
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
			c.out.WriteListParseFailed(task.Key)
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
		// The success lines are gated by Output, not here: bucket mode
		// honours is_success_log / is_multipart_success_log, -check-file
		// always records them.
		if task.IsMultipart {
			c.out.WriteMultipartOk(task.OwnerID, task.Key, task.Offsets)
			c.stats.IncrOkMp()
		} else {
			c.out.WriteSuccess(task.OwnerID, task.Key)
			c.stats.IncrOkObjects()
		}
	}
}

// probeResult is the outcome of running a VerifyTask's probe matrix.
type probeResult int

const (
	// probeClean: every probe succeeded and none matched a corruption regex.
	probeClean probeResult = iota
	// probeCorrupted: at least one probe matched a corruption regex.
	probeCorrupted
	// probeFailed: a RangeGet returned an error before any probe matched.
	// err is the RangeGet error; the caller routes to check_failed /
	// mp_check_failed.
	probeFailed
)

// runProbes runs the probe matrix and returns the first non-clean result:
// all clean → probeClean; any RangeGet error → probeFailed (no further
// probes); any probe matching a corruption regex → probeCorrupted.
// Regex selection by task shape (matchFunc): single PUT runs chunkSigRe ||
// trailerRe; multipart runs chunkSigRe only. Shared by list-check and the
// file-input check mode (-check-file). Backup mode does not
// probe. threshold is WholeObjectProbeThreshold from cfg.
func runProbes(ctx context.Context, worker S3API, task VerifyTask, threshold int) (probeResult, error) {
	match := matchFunc(task)
	for _, p := range buildProbesStatic(task, threshold) {
		body, err := worker.RangeGetAt(ctx, task.Key, p.start, p.length)
		if err != nil {
			return probeFailed, err
		}
		if match(body) {
			return probeCorrupted, nil
		}
	}
	return probeClean, nil
}

// matchFunc returns the corruption-match function for the task shape:
// multipart → chunkSigRe only (multipart parts can't be trailer uploads on
// private storage); single PUT → chunkSigRe || trailerRe.
func matchFunc(task VerifyTask) func([]byte) bool {
	if task.IsMultipart {
		return chunkSigRe.Match
	}
	return func(b []byte) bool { return chunkSigRe.Match(b) || trailerRe.Match(b) }
}

// probeSpec is one RangeGet probe in a VerifyTask's plan.
type probeSpec struct {
	start  int64
	length int64
}

// probeWindow is the read size for head/tail/part probes (bytes).
const probeWindow int64 = 128

// buildProbes returns the probe plan for task:
//   - 0 < Size <= WholeObjectProbeThreshold → single RangeGetAt(0, thr)
//     (Size==0 never reaches here — Handle short-circuits.)
//   - single PUT, Size > threshold → head@0 + tail@Size-W.
//   - multipart N parts, Size > threshold → one probe per part offset,
//     each reads [off_i, off_i+W). Multipart runs chunkSigRe only, which
//     matches ;chunk-signature= at chunk HEADS, so each part's head is
//     covered by its own probe. No tail probe (heads-only matching).
//
// Probes clamp length to Size when the object is smaller than the window.
func (c *Checker) buildProbes(task VerifyTask) []probeSpec {
	return buildProbesStatic(task, c.cfg.WholeObjectProbeThreshold)
}

// buildProbesStatic is the package-level probe planner shared by list-check
// and the file-input check mode (-check-file). threshold is
// WholeObjectProbeThreshold (0 disables the small-object fast-path).
func buildProbesStatic(task VerifyTask, threshold int) []probeSpec {
	thr := int64(threshold)
	if task.Size > 0 && thr > 0 && task.Size <= thr {
		return []probeSpec{{0, thr}}
	}
	if !task.IsMultipart {
		return headTail(task.Size, probeWindow)
	}
	// Multipart: one probe per part offset, each reading [off, off+W).
	// chunkSigRe matches chunk heads, so each part's head is covered by its
	// own probe — no part depends on another's probe, and no tail probe.
	offs := task.Offsets
	out := make([]probeSpec, 0, len(offs))
	for _, off := range offs {
		e := off + probeWindow
		if task.Size > 0 && e > task.Size {
			e = task.Size
		}
		if e > off {
			out = append(out, probeSpec{off, e - off})
		}
	}
	return out
}

// headTail returns the head and tail probes for a single-PUT object.
func headTail(size, probe int64) []probeSpec {
	return []probeSpec{{0, probe}, tailProbe(size, probe)}
}

// tailProbe returns the tail probe spec. Used only by headTail (single-PUT);
// multipart has no tail probe.
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
