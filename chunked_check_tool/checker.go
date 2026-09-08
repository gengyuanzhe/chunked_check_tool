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
var chunkSigRe = regexp.MustCompile(`^[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}[\r\n]`)

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
// outputs based on a 128-byte RangeGet at each offset in task.Offsets.
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
// source does not bump them, so summary shows list_all: 0 in list-file mode.
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
// "segcheck off" path → write to mp_all without claiming ok_mp. Normal
// objects (IsMultipart=false, Offsets=nil) get a single probe at offset 0 —
// no slice, no loop, no allocation. Multipart objects iterate Offsets.
// If any probe settles (corrupted or failed) the task is done; otherwise
// all probes clean → ok_object / ok_mp.
func (c *Checker) verify(task VerifyTask) {
	if task.IsMultipart && len(task.Offsets) == 0 {
		c.out.WriteMultipartAll(task.OwnerID, task.Key)
		return
	}
	settled := false
	if !task.IsMultipart {
		settled = c.probeAndRoute(task, 0)
	} else {
		for _, off := range task.Offsets {
			if c.probeAndRoute(task, off) {
				settled = true
				break
			}
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

// probeAndRoute does one 128-byte RangeGet at off and routes the result by
// IsMultipart. Returns true when the task is settled (corrupted or failed)
// so the caller stops further probes. Normal and multipart share this path.
func (c *Checker) probeAndRoute(task VerifyTask, off int64) bool {
	body, err := c.worker.RangeGetAt(context.Background(), task.Key, off, 128)
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
	if chunkSigRe.Match(body) {
		if task.IsMultipart {
			c.out.WriteCorruptedMultipart(task.OwnerID, task.Key)
			c.stats.IncrCorruptedMp()
		} else {
			c.out.WriteCorrupted(task.OwnerID, task.Key)
			c.stats.IncrCorruptedObjects()
		}
		return true
	}
	return false
}
