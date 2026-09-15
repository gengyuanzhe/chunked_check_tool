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
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

func NewChecker(worker S3API, out *Output, stats *Stats, cfg *Config) *Checker {
	return &Checker{worker: worker, out: out, stats: stats, cfg: cfg}
}

// Handle routes task to verify. The size==0 normal-object shortcut stays
// here (RangeGet on an empty body returns 416 → would misclassify as
// check_failed). Listed-counter bumps (list_obj/list_mp) are NOT done here —
// the S3 lister bumps them in check mode before pushing the task. List-file
// source does not bump them (no S3 LIST); its summary reports read
// instead of list_all.
func (c *Checker) Handle(task VerifyTask) {
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
		c.out.WriteMultipartAll(task.OwnerID, task.Key)
		return
	}
	settled := false
	for _, p := range c.buildProbes(task) {
		if c.probeAndRoute(task, p.start, p.length) {
			settled = true
			break
		}
	}
	if settled {
		return
	}
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
	thr := int64(c.cfg.WholeObjectProbeThreshold)
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
		out = append(out, headTail(task.Size, probeWindow)...)
	}
	return out
}

// headTail returns the head and tail probes for a non-multipart object
// (or the tail pair appended after boundary probes for multipart). Tail
// start clamps to 0 for Size < probeWindow; tail length clamps to Size.
func headTail(size, probe int64) []probeSpec {
	tailStart := size - probe
	if tailStart < 0 {
		tailStart = 0
	}
	tailLen := probe
	if size < probe {
		tailLen = size
	}
	return []probeSpec{{0, probe}, {tailStart, tailLen}}
}

// probeAndRoute does one RangeGetAt at [start, start+length) and routes
// the result by IsMultipart. Returns true when the task is settled
// (corrupted or failed) so the caller stops further probes. Normal and
// multipart share this path; routing depends on IsMultipart only, not
// probe position. chunkSigRe and trailerRe both match against the same
// body — one match decision, not two probes.
func (c *Checker) probeAndRoute(task VerifyTask, start, length int64) bool {
	body, err := c.worker.RangeGetAt(context.Background(), task.Key, start, length)
	if err != nil {
		if task.IsMultipart {
			c.out.WriteMpCheckFailed(task.Key)
			c.out.WriteMpCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			c.stats.IncrMpCheckFailed()
		} else {
			c.out.WriteCheckFailed(task.Key)
			c.out.WriteCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			c.stats.IncrCheckFailed()
		}
		return true
	}
	if chunkSigRe.Match(body) || trailerRe.Match(body) {
		if task.IsMultipart {
			c.out.WriteCorruptedMultipart(task.OwnerID, task.Key, task.Offsets)
			c.stats.IncrCorruptedMp()
		} else {
			c.out.WriteCorrupted(task.OwnerID, task.Key)
			c.stats.IncrCorruptedObjects()
		}
		return true
	}
	return false
}
