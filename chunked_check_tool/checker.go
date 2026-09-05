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
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

// NewChecker builds a Checker. cfg carries success-log toggle plus the
// multipart-segment-check config (switch + segment size).
func NewChecker(worker S3API, out *Output, stats *Stats, cfg *Config) *Checker {
	return &Checker{worker: worker, out: out, stats: stats, cfg: cfg}
}

// Handle classifies obj. Every object that reaches Handle counts as
// "listed/processed" (listedTotal), regardless of outcome — in check mode
// the lister does not IncrListed (it sends objects to objCh), so the
// checker is responsible for bumping the counter for each object it
// consumes. IncrListed is therefore the first thing we do.
func (c *Checker) Handle(obj ObjectInfo) {
	c.stats.IncrListed()

	if !isNormalETag(obj.ETag) {
		// Multipart object. If the multipart segment check is enabled,
		// probe the first 128 bytes of each segment for the chunked-upload
		// signature; any match means the multipart is corrupted.
		if c.cfg.IsMultipartCheck && c.cfg.MultipartSegmentSize > 0 && obj.Size > 0 {
			c.checkMultipartSegments(obj)
		} else {
			c.out.WriteMultipartAll(obj.OwnerID, obj.Key)
			c.stats.IncrMultipart()
		}
		return
	}

	// Size=0 objects cannot be RangeGet'd (S3 returns 416 Range Not
	// Satisfiable since the requested byte range doesn't overlap with
	// an empty body). An empty body also cannot contain a chunked-upload
	// signature, so the corruption check is inconclusive — treat as
	// normal.
	if obj.Size == 0 {
		if c.cfg.IsSuccessLog {
			c.out.WriteSuccess(obj.OwnerID, obj.Key)
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
		c.out.WriteCorrupted(obj.OwnerID, obj.Key)
		c.stats.IncrCorrupted()
	} else if c.cfg.IsSuccessLog {
		c.out.WriteSuccess(obj.OwnerID, obj.Key)
	}
}

// checkMultipartSegments probes the first 128 bytes of each segment of obj
// (segments of cfg.MultipartSegmentSize bytes starting at offset 0, segSize,
// 2*segSize, ...). If ANY segment's body matches the chunked-upload signature
// regex, the object is flagged as corrupted multipart. If a segment RangeGet
// returns an error, the object is flagged as multipart_check_failed (distinct
// from check_failed — segment GET errors are a separate failure mode and get
// their own file + counter). Otherwise the object is recorded as a clean
// multipart (→ ok_multipart_objects.txt when is_success_log, else dropped).
func (c *Checker) checkMultipartSegments(obj ObjectInfo) {
	segSize := c.cfg.MultipartSegmentSize
	numSegs := (obj.Size + segSize - 1) / segSize
	for i := int64(0); i < numSegs; i++ {
		offset := i * segSize
		body, err := c.worker.RangeGetAt(context.Background(), obj.Key, offset, 128)
		if err != nil {
			c.out.WriteMultipartCheckFailed(obj.Key)
			c.out.WriteMultipartCheckFailedLog(obj.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			c.stats.IncrMultipartCheckFailed()
			return
		}
		if chunkSigRe.Match(body) {
			c.out.WriteCorruptedMultipart(obj.OwnerID, obj.Key)
			c.stats.IncrCorruptedMultipart()
			return
		}
	}
	// No segment matched — record as a clean multipart.
	if c.cfg.IsMultipartSuccessLog {
		c.out.WriteMultipartOk(obj.OwnerID, obj.Key)
	}
	c.stats.IncrMultipart()
}
