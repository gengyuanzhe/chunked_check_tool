package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
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
// HeadObject returns the object's ETag (quotes stripped) via a HEAD
// request — backup mode's authoritative regular-vs-multipart check.
//
// CopyObject server-side-copies srcKey from the client's bound source
// bucket into dstBucket under dstKey. Default copy semantics: the source
// object's user metadata is preserved (no REPLACE directive is sent).
//
// PutObject uploads size bytes from r to bucket/key — used to archive the
// backup list file into the target bucket.
type S3API interface {
	ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error)
	RangeGet(ctx context.Context, key string) ([]byte, error)
	RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error)
	HeadObject(ctx context.Context, key string) (string, error)
	CopyObject(ctx context.Context, srcKey, dstBucket, dstKey string) error
	PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) error
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

// minioListAPI is the subset of *minio.Core that S3Client calls for
// listing. Extracting it as an interface lets tests inject a fake to
// verify the V1/V2 dispatch and cursor normalization without a live
// endpoint. Both methods are defined on Core with value receivers, so
// *minio.Core satisfies this interface.
type minioListAPI interface {
	ListObjectsV2(bucketName, prefix, startAfter, continuationToken, delimiter string, maxKeys int) (minio.ListBucketV2Result, error)
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListBucketResult, error)
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
	core    minioListAPI
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

// HeadObject returns the object's ETag with surrounding quotes stripped.
// Used by backup mode: isNormalETag on the result decides regular vs
// multipart. Same node-failover retry semantics as RangeGet.
func (c *S3Client) HeadObject(ctx context.Context, key string) (string, error) {
	etag, err := c.headObjectOnce(ctx, key)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			etag, err = c.headObjectOnce(ctx, key)
		}
	}
	if err != nil {
		return "", err
	}
	return trimETagQuotes(etag), nil
}

func (c *S3Client) headObjectOnce(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	info, err := c.client.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

// CopyObject server-side-copies srcKey (from the client's bound bucket)
// into dstBucket/dstKey. No UserMetadata/ReplaceMetadata is set on the
// destination, so the copy preserves the source's user metadata (minio-go
// copies source metadata when none is provided). Same node-failover retry
// semantics as RangeGet.
func (c *S3Client) CopyObject(ctx context.Context, srcKey, dstBucket, dstKey string) error {
	err := c.copyObjectOnce(ctx, srcKey, dstBucket, dstKey)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			err = c.copyObjectOnce(ctx, srcKey, dstBucket, dstKey)
		}
	}
	return err
}

func (c *S3Client) copyObjectOnce(ctx context.Context, srcKey, dstBucket, dstKey string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	dst := minio.CopyDestOptions{Bucket: dstBucket, Object: dstKey}
	src := minio.CopySrcOptions{Bucket: c.bucket, Object: srcKey}
	_, err := c.client.CopyObject(ctx, dst, src)
	return err
}

// PutObject uploads size bytes from r to bucket/key. Used once per backup
// run to archive the input list file into the target bucket. Same
// node-failover retry semantics as RangeGet.
func (c *S3Client) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) error {
	// r must be re-readable across the retry; copy into memory once (the
	// list file is at most a few MB).
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	err = c.putObjectOnce(ctx, bucket, key, bytes.NewReader(body), size)
	if err != nil && c.pool != nil && isNodeFaultErr(err) {
		c.pool.MarkFailed(c.nodeIdx)
		if c.rebuild() {
			err = c.putObjectOnce(ctx, bucket, key, bytes.NewReader(body), size)
		}
	}
	return err
}

func (c *S3Client) putObjectOnce(ctx context.Context, bucket, key string, r io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err := c.client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{})
	return err
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

	// Heads maps key → ETag (quotes already stripped) returned by
	// HeadObject. A missing key returns HeadErr (or a default normal ETag
	// when HeadErr is nil) so tests only set what they care about.
	Heads   map[string]string
	HeadErr error
	// CopyErr, when non-nil, makes CopyObject fail. Calls are recorded in
	// Copies for assertion.
	CopyErr error
	Copies  []CopyCall
	// PutErr, when non-nil, makes PutObject fail. Calls are recorded in
	// Puts for assertion.
	PutErr error
	Puts   []PutCall
}

// CopyCall records one FakeS3.CopyObject invocation.
type CopyCall struct {
	SrcKey    string
	DstBucket string
	DstKey    string
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

func (f *FakeS3) HeadObject(ctx context.Context, key string) (string, error) {
	if f.HeadErr != nil {
		return "", f.HeadErr
	}
	if etag, ok := f.Heads[key]; ok {
		return etag, nil
	}
	// Default: a normal 32-hex ETag so unset keys behave as regular objects.
	return "0123456789abcdef0123456789abcdef", nil
}

func (f *FakeS3) CopyObject(ctx context.Context, srcKey, dstBucket, dstKey string) error {
	f.Copies = append(f.Copies, CopyCall{SrcKey: srcKey, DstBucket: dstBucket, DstKey: dstKey})
	if f.CopyErr != nil {
		return f.CopyErr
	}
	return nil
}

func (f *FakeS3) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.Puts = append(f.Puts, PutCall{Bucket: bucket, Key: key, Content: string(body)})
	if f.PutErr != nil {
		return f.PutErr
	}
	return nil
}

var _ S3API = (*FakeS3)(nil)
var _ S3API = (*S3Client)(nil)

var ErrNodeDown = errors.New("node down")
