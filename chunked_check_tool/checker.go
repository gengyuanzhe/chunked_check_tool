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

// Checker classifies a single listed object: multipart (skip Range GET),
// normal (Range GET 128 bytes + regex), corrupted, or check-failed.
type Checker struct {
	worker     S3API
	out        *Output
	stats      *Stats
	successLog bool
}

// NewChecker builds a Checker. successLog controls whether normal (non-
// corrupted) objects are written to the success log.
func NewChecker(worker S3API, out *Output, stats *Stats, successLog bool) *Checker {
	return &Checker{worker: worker, out: out, stats: stats, successLog: successLog}
}

// Handle classifies obj. Every object that reaches Handle counts as
// "listed/processed" (listedTotal), regardless of outcome — in check mode
// the lister does not IncrListed (it sends objects to objCh), so the
// checker is responsible for bumping the counter for each object it
// consumes. IncrListed is therefore the first thing we do.
func (c *Checker) Handle(obj ObjectInfo) {
	c.stats.IncrListed()

	if !isNormalETag(obj.ETag) {
		c.out.WriteMultipart(obj.ETag, obj.Key)
		c.stats.IncrMultipart()
		return
	}

	// Size=0 objects cannot be RangeGet'd (S3 returns 416 Range Not
	// Satisfiable since the requested byte range doesn't overlap with
	// an empty body). An empty body also cannot contain a chunked-upload
	// signature, so the corruption check is inconclusive — treat as
	// normal.
	if obj.Size == 0 {
		if c.successLog {
			c.out.WriteSuccess(obj.Key)
		}
		return
	}

	body, err := c.worker.RangeGet(context.Background(), obj.Key)
	if err != nil {
		c.out.WriteCheckFailed(obj.Key)
		c.out.WriteCheckFailedLog(obj.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
		c.stats.IncrCheckFailed()
		return
	}

	if chunkSigRe.Match(body) {
		c.out.WriteCorrupted(obj.Key)
		c.stats.IncrCorrupted()
	} else if c.successLog {
		c.out.WriteSuccess(obj.Key)
	}
}
