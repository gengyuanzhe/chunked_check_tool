package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectInfo is the subset of S3 object metadata we care about.
//
// OwnerID drives per-owner output partitioning: each owner gets its own
// subfolder under output_dir, so result files (corrupted/ok/multipart) are
// sharded by owner. Populated from minio.ObjectInfo.Owner.ID in ListPage.
// Empty when the LIST response omits owner (some S3 implementations) —
// writes then route to the "_unknown" subfolder.
type ObjectInfo struct {
	Key     string
	ETag    string
	Size    int64
	OwnerID string
}

// trimETagQuotes strips a single pair of surrounding double quotes that S3
// returns ETags wrapped in (e.g. `"abcdef..."`). minio-go does not strip
// them; without this, a normal 32-hex ETag fails isNormalETag and is
// misclassified as multipart.
func trimETagQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// S3API is the S3 surface area the checker depends on.
//
// ListPage performs a single LIST request (V2). It accepts both a
// start-after key (for the manual -nextmarker resumption point used in
// Mode 1 root pagination) and a continuationToken (for token-based
// pagination of subsequent pages). The returned string is the
// NextContinuationToken to feed into the next call's continuationToken
// parameter; it is "" when the response was not truncated (final page).
//
// Using the continuation token instead of the last-key heuristic is
// required for correctness: S3 counts CommonPrefixes toward maxKeys, so
// a page may return Contents=[] + CommonPrefixes=[N] + IsTruncated=true.
// The last-key heuristic produces an empty cursor in that case and the
// remaining common prefixes are silently never fetched.
//
// RangeGet fetches the first 128 bytes of an object (for chunked-upload
// signature verification).
//
// RangeGetAt fetches `length` bytes starting at `offset` (for multipart
// segment signature verification — each segment's first 128 bytes are
// inspected in turn). offset is a byte offset into the object body.
//
// HeadObject returns the object's ETag (quotes stripped) and size via a
// HEAD request — backup mode's authoritative regular-vs-multipart check
// and the size source for relay part boundaries.
//
// DownloadRange opens a streaming ranged GET of `length` bytes starting at
// `start` — the backup relay pipes this reader straight into an upload
// without buffering. Node failover applies only to the initial request;
// once the reader is returned, a mid-read failure surfaces as a read error
// from the relay (the stream cannot be replayed).
//
// PutObject uploads size bytes from r to bucket/key and returns the
// destination ETag (quotes stripped) — the backup list archive and the
// regular-object relay.
//
// PutObjectStream is the relay variant for large bodies: it never buffers
// r, so a node fault mid-upload is NOT retried (the stream cannot be
// replayed) and surfaces as an error to the caller. DisableMultipart pins
// it to a single PUT (multipart splitting would break the plain-MD5 ETag
// verification).
//
// CreateMultipart/UploadPart/CompleteMultipart/AbortMultipart are the
// multipart lifecycle used by the multipart relay: parts are re-uploaded
// at the input line's original offsets so that a byte-faithful relay
// reproduces the source ETag (md5-of-part-md5s-N).
type S3API interface {
	ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error)
	RangeGet(ctx context.Context, key string) ([]byte, error)
	RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error)
	HeadObject(ctx context.Context, key string) (string, int64, error)
	DownloadRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error)
	PutObjectStream(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error)
	CreateMultipart(ctx context.Context, bucket, key string) (string, error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error)
	CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []UploadedPart) (string, error)
	AbortMultipart(ctx context.Context, bucket, key, uploadID string) error
}

// UploadedPart identifies one uploaded part for CompleteMultipart.
type UploadedPart struct {
	PartNumber int
	ETag       string
}

// NewMinioClient constructs a minio.Client bound to a single endpoint.
// When secure is true the transport skips TLS verification (typical for
// internal nodes with self-signed certs); set secure=false for plain HTTP.
func NewMinioClient(endpoint, ak, sk string, secure bool) (*minio.Client, error) {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       secure,
		Transport:    tr,
		BucketLookup: minio.BucketLookupAuto,
	})
}

// minioCoreAPI is the subset of *minio.Core that S3Client calls for
// listing and multipart uploads. Extracting it as an interface lets tests
// inject a fake to verify the V1/V2 dispatch and cursor normalization
// without a live endpoint. All methods are defined on Core with value
// receivers, so *minio.Core satisfies this interface.
type minioCoreAPI interface {
	ListObjectsV2(bucketName, prefix, startAfter, continuationToken, delimiter string, maxKeys int) (minio.ListBucketV2Result, error)
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListBucketResult, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error)
	NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error)
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error
}

// S3Client wraps minio.Core for ListPage (exposes CommonPrefixes, which the
// higher-level minio.Client.ListObjects channel API does not) and minio.Client
// for RangeGet.
//
// Node failover (spec §7/§14): when a list/get call returns a node-fault
// error (connection refused, timeout, HTTP 5xx — NOT 4xx business errors
// like 404/403), the client marks its bound node failed in the pool,
// rebuilds its minio client on the next alive node, and retries the call
// exactly once. If the retry also fails the original error is returned so
// the caller records list_failed/check_failed. Each worker owns its own
// S3Client so no synchronization is needed on the mutable fields.
type S3Client struct {
	core    minioCoreAPI
	client  *minio.Client
	bucket  string
	stats   *Stats
	pool    *NodePool
	nodeIdx int
	cfg     *Config
}

// NewS3Client builds an S3Client from an existing minio.Client and a bucket.
// stats records per-call latency (list_calls / list_avg_latency_ms).
// pool/nodeIdx/cfg enable node-failover rebuild (nil pool disables it —
// used by tests that inject a pre-built minio client).
func NewS3Client(client *minio.Client, bucket string, stats *Stats, pool *NodePool, nodeIdx int, cfg *Config) *S3Client {
	return &S3Client{
		core:    &minio.Core{Client: client},
		client:  client,
		bucket:  bucket,
		stats:   stats,
		pool:    pool,
		nodeIdx: nodeIdx,
		cfg:     cfg,
	}
}

// ListPage lists one page of objects under prefix.
//
// NOTE: minio.Core.ListObjectsV2 does not accept a context.Context; it uses
// context.Background() internally. The caller's ctx is therefore not honored
// for cancellation of the underlying HTTP request. This is a known limitation
// of the minio-go v7 Core API. RangeGet does accept ctx.
func (c *S3Client) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	delimiter := ""
	if delim {
		delimiter = "/"
	}
	result, err := c.listPageOnce(prefix, startAfter, continuationToken, delimiter, maxKeys)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		// Node-fault path: mark, rebuild on next alive node, retry once.
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			result, err = c.listPageOnce(prefix, startAfter, continuationToken, delimiter, maxKeys)
		}
	}
	if err != nil {
		return nil, nil, "", err
	}
	objs := make([]ObjectInfo, 0, len(result.contents))
	for _, o := range result.contents {
		objs = append(objs, ObjectInfo{Key: o.Key, ETag: trimETagQuotes(o.ETag), Size: o.Size, OwnerID: o.Owner.ID})
	}
	prefixes := make([]string, 0, len(result.commonPrefixes))
	for _, cp := range result.commonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	return objs, prefixes, result.next, nil
}

// listResult is the normalized output of one LIST call, independent of
// whether V1 or V2 was used. `next` is the cursor to feed back into the
// next call's continuationToken parameter (V2: NextContinuationToken;
// V1: NextMarker or last Contents key when no delimiter was used).
type listResult struct {
	contents       []minio.ObjectInfo
	commonPrefixes []minio.CommonPrefix
	next           string
}

// listPageOnce issues a single LIST request and records its latency against
// stats. It dispatches on cfg.ListAPIVersion: V1 uses Core.ListObjects with
// a marker cursor; V2 (default) uses Core.ListObjectsV2 with a continuation
// token. Both paths normalize to a listResult so ListPage is agnostic to
// the wire protocol.
//
// V1 cursor semantics: the first page passes startAfter (if any) as the
// marker; subsequent pages pass the previous call's returned `next` value
// (which is NextMarker, or the last Contents key when NextMarker is empty).
// The caller always feeds `next` back via the continuationToken slot, so
// listPageOnce maps continuationToken → marker when ListAPIVersion=1.
func (c *S3Client) listPageOnce(prefix, startAfter, continuationToken, delimiter string, maxKeys int) (listResult, error) {
	start := time.Now()
	if c.cfg != nil && c.cfg.ListAPIVersion == 1 {
		marker := startAfter
		if continuationToken != "" {
			marker = continuationToken
		}
		r, err := c.core.ListObjects(c.bucket, prefix, marker, delimiter, maxKeys)
		if c.stats != nil {
			c.stats.AddListCall(time.Since(start))
		}
		if err != nil {
			return listResult{}, err
		}
		next := ""
		if r.IsTruncated {
			// S3 populates NextMarker only when a delimiter is set. With no
			// delimiter the caller must use the last returned key as the
			// next marker. If Contents is empty and IsTruncated is true
			// (theoretically possible with delimiter-only pages), we fall
			// back to "" — the caller stops, matching V2's behavior when
			// NextContinuationToken is empty on a truncated response.
			switch {
			case r.NextMarker != "":
				next = r.NextMarker
			case len(r.Contents) > 0:
				next = r.Contents[len(r.Contents)-1].Key
			}
		}
		return listResult{
			contents:       r.Contents,
			commonPrefixes: r.CommonPrefixes,
			next:           next,
		}, nil
	}
	r, err := c.core.ListObjectsV2(c.bucket, prefix, startAfter, continuationToken, delimiter, maxKeys)
	if c.stats != nil {
		c.stats.AddListCall(time.Since(start))
	}
	if err != nil {
		return listResult{}, err
	}
	next := ""
	if r.IsTruncated {
		next = r.NextContinuationToken
	}
	return listResult{
		contents:       r.Contents,
		commonPrefixes: r.CommonPrefixes,
		next:           next,
	}, nil
}

// rebuild swaps the bound minio client to the next alive node. Returns true
// when a new node was assigned and the client was successfully rebuilt.
func (c *S3Client) rebuild() bool {
	if c.pool == nil || c.cfg == nil {
		return false
	}
	newIdx := c.pool.Assign(c.nodeIdx)
	if newIdx < 0 {
		return false
	}
	client, err := NewMinioClient(c.pool.Endpoint(newIdx), c.cfg.AK, c.cfg.SK, c.cfg.Scheme == "https")
	if err != nil {
		return false
	}
	c.client = client
	c.core = &minio.Core{Client: client}
	c.nodeIdx = newIdx
	return true
}

// RangeGet returns the first 128 bytes of an object, used to inspect the
// chunked-upload streaming signature header. The call is timed once for
// stats.AddGetCall — covers the initial attempt plus any node-fault retry,
// so getCalls == per-object GET count.
func (c *S3Client) RangeGet(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	defer func() {
		if c.stats != nil {
			c.stats.AddGetCall(time.Since(start))
		}
	}()
	body, err := c.rangeGetOnce(ctx, key)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			body, err = c.rangeGetOnce(ctx, key)
		}
	}
	return body, err
}

func (c *S3Client) rangeGetOnce(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var opts minio.GetObjectOptions
	if err := opts.SetRange(0, 127); err != nil {
		return nil, err
	}
	obj, err := c.client.GetObject(ctx, c.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	body, err := io.ReadAll(io.LimitReader(obj, 128))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// RangeGetAt fetches `length` bytes starting at byte `offset` of an object,
// used by the multipart segment check (each segment's first 128 bytes are
// inspected for the chunked-upload signature). Same node-failover retry
// semantics as RangeGet; AddGetCall is bumped once per public call.
func (c *S3Client) RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	start := time.Now()
	defer func() {
		if c.stats != nil {
			c.stats.AddGetCall(time.Since(start))
		}
	}()
	body, err := c.rangeGetAtOnce(ctx, key, offset, length)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			body, err = c.rangeGetAtOnce(ctx, key, offset, length)
		}
	}
	return body, err
}

func (c *S3Client) rangeGetAtOnce(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var opts minio.GetObjectOptions
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, err
	}
	obj, err := c.client.GetObject(ctx, c.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	body, err := io.ReadAll(io.LimitReader(obj, length))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// HeadObject returns the object's ETag (quotes stripped) and size.
// Used by backup mode: isNormalETag on the etag decides regular vs
// multipart, and the size bounds the relay's last part. Same node-failover
// retry semantics as RangeGet.
func (c *S3Client) HeadObject(ctx context.Context, key string) (string, int64, error) {
	etag, size, err := c.headObjectOnce(ctx, key)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			etag, size, err = c.headObjectOnce(ctx, key)
		}
	}
	if err != nil {
		return "", 0, err
	}
	return trimETagQuotes(etag), size, nil
}

func (c *S3Client) headObjectOnce(ctx context.Context, key string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	info, err := c.client.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return "", 0, err
	}
	return info.ETag, info.Size, nil
}

// DownloadRange opens a streaming ranged GET of length bytes starting at
// start from the client's bound bucket. The caller must Close the reader.
// The GET is issued eagerly (Core.GetObject), so a 404/5xx returns here
// instead of hiding inside the reader — mid-read failures after this point
// surface as read errors (the stream cannot be replayed). Node failover
// applies only to this initial request.
func (c *S3Client) DownloadRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	obj, err := c.downloadRangeOnce(ctx, key, start, length)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			obj, err = c.downloadRangeOnce(ctx, key, start, length)
		}
	}
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *S3Client) downloadRangeOnce(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	var opts minio.GetObjectOptions
	if err := opts.SetRange(start, start+length-1); err != nil {
		return nil, err
	}
	// Core.GetObject, NOT the lazy client.GetObject + Stat() probe: Stat()
	// issues a HEAD carrying our Range header, and servers that answer that
	// HEAD+Range with Content-Length: 0 make Stat report Size=0 without an
	// error — the first Read on the lazy object then returns io.EOF and the
	// relay's streaming-signed PUT fails with "http: ContentLength=N with
	// Body length 0" for every object. Core.GetObject issues the ranged GET
	// directly and returns the raw response body.
	body, info, _, err := c.core.GetObject(ctx, c.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	// info.Size comes from the response Content-Length. For a 206 it must
	// equal the requested length; anything else would otherwise surface
	// later as the cryptic signer error or a short/over-long part upload.
	if info.Size != length {
		body.Close()
		return nil, fmt.Errorf("get %s/%s bytes=%d-%d: response Content-Length %d, want %d", c.bucket, key, start, start+length-1, info.Size, length)
	}
	return body, nil
}

// PutObject uploads size bytes from r to bucket/key and returns the
// destination ETag (quotes stripped). The reader is fully drained into
// memory once so the node-fault retry can replay it — used for the backup
// list archive (a few MB). The regular-object relay also goes through
// here; large relays stream via UploadPart instead, so the buffering is
// bounded by design.
func (c *S3Client) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	if int64(len(body)) != size {
		return "", fmt.Errorf("put %s/%s: short read: got %d bytes, want %d", bucket, key, len(body), size)
	}
	etag, err := c.putObjectOnce(ctx, bucket, key, bytes.NewReader(body), size)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			etag, err = c.putObjectOnce(ctx, bucket, key, bytes.NewReader(body), size)
		}
	}
	if err != nil {
		return "", err
	}
	return trimETagQuotes(etag), nil
}

func (c *S3Client) putObjectOnce(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	info, err := c.client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

// PutObjectStream uploads r to bucket/key without buffering it in memory.
// Unlike PutObject there is no node-fault retry: the caller's reader is a
// live download stream that cannot be replayed, so any failure (including
// mid-upload node faults) is returned to the caller.
//
// DisableMultipart forces the single-PUT path: minio-go's PutObject
// auto-splits into multipart when size exceeds the part size (16MiB by
// default), and the resulting md5-of-part-md5s-N ETag could never match
// the source regular object's plain MD5 — every >16MiB regular relay
// would fail the ETag verification. Regular objects are single-PUT
// uploads by definition, so >5GiB cannot legitimately occur; if one
// somehow did, the server rejects the oversized single PUT at upload
// time instead of succeeding as multipart and failing ETag verification
// after the bytes were already transferred.
func (c *S3Client) PutObjectStream(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	info, err := c.client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		DisableMultipart: true,
	})
	if err != nil {
		return "", err
	}
	return trimETagQuotes(info.ETag), nil
}

// CreateMultipart starts a multipart upload in bucket/key and returns the
// upload ID.
func (c *S3Client) CreateMultipart(ctx context.Context, bucket, key string) (string, error) {
	uploadID, err := c.core.NewMultipartUpload(ctx, bucket, key, minio.PutObjectOptions{})
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			uploadID, err = c.core.NewMultipartUpload(ctx, bucket, key, minio.PutObjectOptions{})
		}
	}
	return uploadID, err
}

// UploadPart streams one part (partNum is 1-based) of a multipart upload
// and returns the part's ETag. The reader is NOT replayable: a mid-stream
// node fault surfaces as an error and the caller aborts the upload.
func (c *S3Client) UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error) {
	part, err := c.core.PutObjectPart(ctx, bucket, key, uploadID, partNum, r, size, minio.PutObjectPartOptions{})
	if err != nil {
		return "", err
	}
	return trimETagQuotes(part.ETag), nil
}

// CompleteMultipart commits the multipart upload and returns the
// destination object's ETag.
func (c *S3Client) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []UploadedPart) (string, error) {
	cp := make([]minio.CompletePart, len(parts))
	for i, p := range parts {
		cp[i] = minio.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	info, err := c.core.CompleteMultipartUpload(ctx, bucket, key, uploadID, cp, minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return trimETagQuotes(info.ETag), nil
}

// AbortMultipart discards an in-progress multipart upload.
func (c *S3Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.core.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

// isNodeFaultErr reports whether err is a transient node-fault error that
// should trigger failover: connection errors, timeouts, and HTTP 5xx
// responses. Business-level 4xx errors (404 Not Found, 403 Forbidden,
// 400 Bad Request) are NOT node faults and propagate to the caller.
func isNodeFaultErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Connection-level errors from net/http surface as strings
	// ("connection refused", "i/o timeout", "EOF", "no such host").
	// minio-go wraps these in its own urlError wrapper; the underlying
	// error string is still reachable via err.Error().
	msg := err.Error()
	for _, sig := range nodeFaultSigs {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	// minio ErrorResponse with a 5xx status code is a node fault.
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		if er.StatusCode >= 500 && er.StatusCode < 600 {
			return true
		}
	}
	return false
}

// nodeFaultSigs are the substring signatures of transient connection errors
// produced by the Go net/http stack. These all indicate that the bound node
// is unhealthy and failover is warranted.
var nodeFaultSigs = []string{
	"connection refused",
	"i/o timeout",
	"timeout",
	"EOF",
	"no such host",
	"connection reset",
	"broken pipe",
	"dial tcp",
	"connect: ",
	"transport",
}

// FakeS3 is an in-memory S3API for testing.
type FakeS3 struct {
	Objects               []ObjectInfo
	CommonPrefixes        []string
	NextContinuationToken string
	Body                  []byte
	Err                   error
	// FailNext, when non-nil, causes the next ListPage call to return
	// this error and then clears it — used to exercise the node-failover
	// retry path of callers (the caller, not FakeS3, decides whether to
	// retry).
	FailNext error
	// Calls counts ListPage invocations.
	Calls int
	// RangeGetHandler, when non-nil, is invoked by RangeGetAt to return
	// per-offset bodies. Used by multipart-segment-check tests to simulate
	// "first segment clean, second segment matches the chunk signature".
	// When nil, RangeGetAt returns Body (same as RangeGet).
	RangeGetHandler func(offset, length int64) ([]byte, error)

	// Heads maps key → HEAD result. A missing key returns HeadErr (or a
	// default normal ETag with size 1 when HeadErr is nil) so tests only
	// set what they care about.
	Heads   map[string]HeadInfo
	HeadErr error

	// RelayBodies maps key → full object content served by DownloadRange
	// (ranged). Falls back to Body for unset keys. DownloadErr, when
	// non-nil, fails every DownloadRange call.
	RelayBodies map[string][]byte
	DownloadErr error

	// Multipart lifecycle knobs and state. CreateErr/UploadErr/CompleteErr
	// fail the respective call. Uploads tracks in-progress part content;
	// Completed/Aborted record outcomes for assertion.
	CreateErr   error
	UploadErr   error
	CompleteErr error
	Uploads     map[string]map[int][]byte
	nextUpload  int
	Completed   []FakeCompletedUpload
	Aborted     []string

	// PutErr, when non-nil, makes PutObject fail. Calls are recorded in
	// Puts for assertion.
	PutErr error
	Puts   []PutCall
}

// HeadInfo is the FakeS3 HEAD result.
type HeadInfo struct {
	ETag string
	Size int64
}

// FakeCompletedUpload records one completed multipart upload: the combined
// ETag and the per-part content keyed by part number.
type FakeCompletedUpload struct {
	Bucket string
	Key    string
	ETag   string
	Parts  map[int][]byte
}

// PutCall records one FakeS3.PutObject invocation.
type PutCall struct {
	Bucket  string
	Key     string
	Content string
}

func (f *FakeS3) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	f.Calls++
	if f.FailNext != nil {
		err := f.FailNext
		f.FailNext = nil
		return nil, nil, "", err
	}
	if f.Err != nil {
		return nil, nil, "", f.Err
	}
	return f.Objects, f.CommonPrefixes, f.NextContinuationToken, nil
}

func (f *FakeS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Body, nil
}

func (f *FakeS3) RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	if f.RangeGetHandler != nil {
		return f.RangeGetHandler(offset, length)
	}
	return f.Body, nil
}

func (f *FakeS3) HeadObject(ctx context.Context, key string) (string, int64, error) {
	if f.HeadErr != nil {
		return "", 0, f.HeadErr
	}
	if h, ok := f.Heads[key]; ok {
		return h.ETag, h.Size, nil
	}
	// Default: a normal 32-hex ETag so unset keys behave as regular objects.
	return "0123456789abcdef0123456789abcdef", 1, nil
}

func (f *FakeS3) DownloadRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	if f.DownloadErr != nil {
		return nil, f.DownloadErr
	}
	body := f.Body
	if b, ok := f.RelayBodies[key]; ok {
		body = b
	}
	if start < 0 || start >= int64(len(body)) {
		return io.NopCloser(strings.NewReader("")), nil
	}
	end := start + length
	if end > int64(len(body)) {
		end = int64(len(body))
	}
	return io.NopCloser(bytes.NewReader(body[start:end])), nil
}

func (f *FakeS3) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	f.Puts = append(f.Puts, PutCall{Bucket: bucket, Key: key, Content: string(body)})
	if f.PutErr != nil {
		return "", f.PutErr
	}
	return fmt.Sprintf("%x", md5.Sum(body)), nil
}

func (f *FakeS3) PutObjectStream(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	return f.PutObject(ctx, bucket, key, r, size)
}

func (f *FakeS3) CreateMultipart(ctx context.Context, bucket, key string) (string, error) {
	if f.CreateErr != nil {
		return "", f.CreateErr
	}
	if f.Uploads == nil {
		f.Uploads = map[string]map[int][]byte{}
	}
	f.nextUpload++
	id := fmt.Sprintf("up-%d", f.nextUpload)
	f.Uploads[id] = map[int][]byte{}
	return id, nil
}

func (f *FakeS3) UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error) {
	if f.UploadErr != nil {
		return "", f.UploadErr
	}
	parts, ok := f.Uploads[uploadID]
	if !ok {
		return "", fmt.Errorf("unknown uploadID %q", uploadID)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	parts[partNum] = body
	return fmt.Sprintf("%x", md5.Sum(body)), nil
}

func (f *FakeS3) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []UploadedPart) (string, error) {
	if f.CompleteErr != nil {
		return "", f.CompleteErr
	}
	stored, ok := f.Uploads[uploadID]
	if !ok {
		return "", fmt.Errorf("unknown uploadID %q", uploadID)
	}
	content := make(map[int][]byte, len(parts))
	for _, p := range parts {
		body, ok := stored[p.PartNumber]
		if !ok {
			return "", fmt.Errorf("part %d not uploaded", p.PartNumber)
		}
		content[p.PartNumber] = body
	}
	etag := multipartETag(content)
	delete(f.Uploads, uploadID)
	f.Completed = append(f.Completed, FakeCompletedUpload{Bucket: bucket, Key: key, ETag: etag, Parts: content})
	return etag, nil
}

func (f *FakeS3) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	delete(f.Uploads, uploadID)
	f.Aborted = append(f.Aborted, uploadID)
	return nil
}

// multipartETag computes the S3 multipart ETag over the parts: the MD5 of
// the concatenated per-part binary MD5s, suffixed with the part count.
func multipartETag(parts map[int][]byte) string {
	nums := make([]int, 0, len(parts))
	for n := range parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	h := md5.New()
	for _, n := range nums {
		sum := md5.Sum(parts[n])
		h.Write(sum[:])
	}
	return fmt.Sprintf("%x-%d", h.Sum(nil), len(nums))
}

var _ S3API = (*FakeS3)(nil)
var _ S3API = (*S3Client)(nil)

var ErrNodeDown = errors.New("node down")
