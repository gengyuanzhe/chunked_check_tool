package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

// ObjectInfo is the subset of S3 object metadata we care about.
//
// OwnerID drives per-owner output partitioning: each owner gets its own
// subfolder under output_dir, so result files (corrupted/ok/multipart) are
// sharded by owner. Populated from the LIST response's Owner.ID in ListPage
// (V2 requests set FetchOwner — the aws SDK does not default it on the wire).
// Empty when the LIST response omits owner (some S3 implementations) —
// writes then route to the "_unknown" subfolder.
type ObjectInfo struct {
	Key     string
	ETag    string
	Size    int64
	OwnerID string
}

// trimETagQuotes strips a single pair of surrounding double quotes that S3
// returns ETags wrapped in (e.g. `"abcdef..."`). The aws SDK does not strip
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
// PutObjectLocal streams the local file at path into bucket/key and returns
// the destination ETag (quotes stripped) — the backup list archive. Nothing
// is buffered: the input list is the disaster-scenario artifact and can be
// gigabytes, so memory stays bounded by one multipart part regardless of
// file size. Files above putLocalPartSize are split into multipart parts
// (the archive's ETag has no verifier, so the single-PUT pinning
// PutObjectStream needs for ETag faithfulness does not apply). Node-fault
// retry re-runs the whole upload — a local file is replayable by re-reading
// it from disk.
//
// PutObjectStream is the relay variant for large bodies: it never buffers
// r, so a node fault mid-upload is NOT retried (the live download stream
// cannot be replayed — unlike PutObjectLocal's local file) and surfaces as
// an error to the caller. A single PUT (the aws SDK's PutObject never
// auto-splits into multipart, which would break the plain-MD5 ETag
// verification: the md5-of-part-md5s-N ETag of a multipart upload could
// never match the source regular object's plain MD5).
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
	PutObjectLocal(ctx context.Context, bucket, key, path string) (string, error)
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

// NewAWSClient constructs an aws-sdk-go-v2 s3.Client bound to a single
// endpoint. minio-go rejected bucket names shorter than 3 chars client-side
// (the S3 3-63 naming rule) before any request left the process, which made
// short buckets the self-developed S3 serves unreachable; the aws client
// does no such validation with path-style addressing. endpoint must be
// host:port (no scheme) — the scheme comes from cfg.Scheme, single source;
// NewAWSClient builds scheme://endpoint itself. When secure is true the
// transport skips TLS verification (typical for internal nodes with
// self-signed certs). dialTimeout caps a single TCP dial attempt (0 =
// net/http default); headerTimeout caps waiting for response headers after
// the request is sent (0 = net/http default). Both bound black-hole /
// hung-server scenarios that the OS default SYN retries would otherwise
// hang for ~75-127s.
//
// The client is built via s3.New with fully explicit options (no
// LoadDefaultConfig): env vars and ~/.aws/config must never leak into a
// data-integrity tool's credentials or retry behavior. Request checksum
// calculation keeps the SDK default (when_supported): a non-seekable relay
// stream over plain HTTP can only be sent through the aws-chunked trailer
// path — the SDK has no signed-chunk streaming — and disabling checksums
// would make those PUTs fail at signing instead.
func NewAWSClient(endpoint, ak, sk string, secure bool, dialTimeout, headerTimeout time.Duration) (*s3.Client, error) {
	// endpoint must be host:port, no scheme — the scheme is a separate
	// config field and the single source for it.
	if strings.Contains(endpoint, "://") {
		return nil, fmt.Errorf("invalid endpoint %q: must be host:port without scheme (scheme comes from cfg.scheme)", endpoint)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("invalid endpoint %q: want host:port", endpoint)
	}

	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if dialTimeout > 0 {
		tr.DialContext = (&net.Dialer{Timeout: dialTimeout}).DialContext
	}
	if headerTimeout > 0 {
		tr.ResponseHeaderTimeout = headerTimeout
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	scheme := "http"
	if secure {
		scheme = "https"
	}
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(scheme + "://" + endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		HTTPClient:   &http.Client{Transport: tr},
	}), nil
}

// awsS3API is the subset of *s3.Client that S3Client calls. Extracting it
// as an interface lets tests inject a fake to verify the V1/V2 dispatch and
// cursor normalization without a live endpoint. The variadic option
// functions are part of the shape (listPageOnce passes the mp-offset
// header option on LIST calls only).
type awsS3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3Client wraps one aws-sdk-go-v2 client for a single bucket.
//
// Node failover (spec §7/§14): when a list/get call returns a node-fault
// error (connection refused, timeout, HTTP 5xx — NOT 4xx business errors
// like 404/403), the client marks its bound node failed in the pool,
// rebuilds its s3 client on the next alive node, and retries the call
// exactly once. If the retry also fails the original error is returned so
// the caller records list_failed/check_failed. Each worker owns its own
// S3Client so no synchronization is needed on the mutable fields.
type S3Client struct {
	api     awsS3API
	bucket  string
	stats   *Stats
	pool    *NodePool
	nodeIdx int
	cfg     *Config
}

// NewS3Client builds an S3Client from an existing s3.Client and a bucket.
// stats records per-call latency (list_calls / list_avg_latency_ms).
// pool/nodeIdx/cfg enable node-failover rebuild (nil pool disables it —
// used by tests that inject a pre-built client).
func NewS3Client(client *s3.Client, bucket string, stats *Stats, pool *NodePool, nodeIdx int, cfg *Config) *S3Client {
	return &S3Client{
		api:     client,
		bucket:  bucket,
		stats:   stats,
		pool:    pool,
		nodeIdx: nodeIdx,
		cfg:     cfg,
	}
}

// ListPage lists one page of objects under prefix.
func (c *S3Client) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	delimiter := ""
	if delim {
		delimiter = "/"
	}
	result, err := c.listPageOnce(ctx, prefix, startAfter, continuationToken, delimiter, maxKeys)
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		// Node fault: retried on a new node when isolated, else on the same
		// node (below the isolate threshold — transient blip).
		result, err = c.listPageOnce(ctx, prefix, startAfter, continuationToken, delimiter, maxKeys)
	}
	if err != nil {
		return nil, nil, "", err
	}
	objs := make([]ObjectInfo, 0, len(result.contents))
	for _, o := range result.contents {
		var ownerID string
		if o.Owner != nil {
			ownerID = aws.ToString(o.Owner.ID)
		}
		objs = append(objs, ObjectInfo{
			Key:     aws.ToString(o.Key),
			ETag:    trimETagQuotes(aws.ToString(o.ETag)),
			Size:    aws.ToInt64(o.Size),
			OwnerID: ownerID,
		})
	}
	prefixes := make([]string, 0, len(result.commonPrefixes))
	for _, cp := range result.commonPrefixes {
		prefixes = append(prefixes, aws.ToString(cp.Prefix))
	}
	return objs, prefixes, result.next, nil
}

// listResult is the normalized output of one LIST call, independent of
// whether V1 or V2 was used. `next` is the cursor to feed back into the
// next call's continuationToken parameter (V2: NextContinuationToken;
// V1: NextMarker or last Contents key when no delimiter was used).
type listResult struct {
	contents       []types.Object
	commonPrefixes []types.CommonPrefix
	next           string
}

// listPageOnce issues a single LIST request and records its latency against
// stats. It dispatches on cfg.ListAPIVersion: V1 uses ListObjects with
// a marker cursor; V2 (default) uses ListObjectsV2 with a continuation
// token. Both paths normalize to a listResult so ListPage is agnostic to
// the wire protocol.
//
// V1 cursor semantics: the first page passes startAfter (if any) as the
// marker; subsequent pages pass the previous call's returned `next` value
// (which is NextMarker, or the last Contents key when NextMarker is empty).
// The caller always feeds `next` back via the continuationToken slot, so
// listPageOnce maps continuationToken → marker when ListAPIVersion=1.
//
// In multipart_check_mode=offset + is_check the call carries
// withMpOffsetListHeader (see mpoffset.go) — LIST requests only, by
// construction: the option is passed here and nowhere else.
func (c *S3Client) listPageOnce(ctx context.Context, prefix, startAfter, continuationToken, delimiter string, maxKeys int) (listResult, error) {
	start := time.Now()
	var opts []func(*s3.Options)
	if c.mpOffsetList() {
		opts = append(opts, withMpOffsetListHeader)
	}
	if c.cfg != nil && c.cfg.ListAPIVersion == 1 {
		marker := startAfter
		if continuationToken != "" {
			marker = continuationToken
		}
		input := &s3.ListObjectsInput{
			Bucket: aws.String(c.bucket),
			Prefix: aws.String(prefix),
		}
		if marker != "" {
			input.Marker = aws.String(marker)
		}
		if delimiter != "" {
			input.Delimiter = aws.String(delimiter)
		}
		if maxKeys > 0 {
			input.MaxKeys = aws.Int32(int32(maxKeys))
		}
		r, err := c.api.ListObjects(ctx, input, opts...)
		if c.stats != nil {
			c.stats.AddListCall(time.Since(start))
		}
		if err != nil {
			return listResult{}, err
		}
		next := ""
		if aws.ToBool(r.IsTruncated) {
			// S3 populates NextMarker only when a delimiter is set. With no
			// delimiter the caller must use the last returned key as the
			// next marker. If Contents is empty and IsTruncated is true
			// (theoretically possible with delimiter-only pages), we fall
			// back to "" — the caller stops, matching V2's behavior when
			// NextContinuationToken is empty on a truncated response.
			switch {
			case aws.ToString(r.NextMarker) != "":
				next = aws.ToString(r.NextMarker)
			case len(r.Contents) > 0:
				next = aws.ToString(r.Contents[len(r.Contents)-1].Key)
			}
		}
		return listResult{
			contents:       r.Contents,
			commonPrefixes: r.CommonPrefixes,
			next:           next,
		}, nil
	}
	input := &s3.ListObjectsV2Input{
		Bucket:     aws.String(c.bucket),
		Prefix:     aws.String(prefix),
		FetchOwner: aws.Bool(true),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if maxKeys > 0 {
		input.MaxKeys = aws.Int32(int32(maxKeys))
	}
	r, err := c.api.ListObjectsV2(ctx, input, opts...)
	if c.stats != nil {
		c.stats.AddListCall(time.Since(start))
	}
	if err != nil {
		return listResult{}, err
	}
	next := ""
	if aws.ToBool(r.IsTruncated) {
		next = aws.ToString(r.NextContinuationToken)
	}
	return listResult{
		contents:       r.Contents,
		commonPrefixes: r.CommonPrefixes,
		next:           next,
	}, nil
}

// mpOffsetList reports whether this client's LIST requests must carry the
// internal-list-mp-offset header (multipart_check_mode=offset in check
// mode). Backup/list-file modes never issue LISTs, so their calls carrying
// no option is harmless anyway.
func (c *S3Client) mpOffsetList() bool {
	return c.cfg != nil && c.cfg.IsCheck && c.cfg.MultipartCheckMode == MultipartCheckModeOffset
}

// handleNodeFault is the shared node-fault leg of every S3 call: it records
// the fault against the bound node and rebinds the client for the single
// retry. The retry ALWAYS moves off the faulting node when another one is
// available:
//
//   - node reached the isolate threshold (or was already isolated by another
//     worker) → rebind via Assign (rotation skipping isolated nodes). False
//     when no live node remains: callers skip the retry and propagate the
//     original error.
//   - below threshold → transient-fault hypothesis, but the retry still goes
//     to a DIFFERENT node via AssignOther: if the node is actually down, a
//     same-node retry would burn another retry round and surface a business
//     failure for a work item a healthy node could have served. Falls back
//     to the same node when it is the only one alive.
//
// Returns false when there is no pool (test-injected clients): no retry.
func (c *S3Client) handleNodeFault() bool {
	if c.pool == nil {
		return false
	}
	if c.pool.RecordFault(c.nodeIdx) {
		return c.rebuild()
	}
	other := c.pool.AssignOther(c.nodeIdx)
	if other < 0 {
		return false
	}
	return c.rebuildOn(other)
}

// rebuild swaps the bound s3 client to the next alive node. Returns true
// when a new node was assigned and the client was successfully rebuilt.
func (c *S3Client) rebuild() bool {
	if c.pool == nil || c.cfg == nil {
		return false
	}
	newIdx := c.pool.Assign(c.nodeIdx)
	if newIdx < 0 {
		return false
	}
	return c.rebuildOn(newIdx)
}

// rebuildOn rebinds the client to the given node index.
func (c *S3Client) rebuildOn(newIdx int) bool {
	client, err := NewAWSClient(c.pool.Endpoint(newIdx), c.cfg.AK, c.cfg.SK, c.cfg.Scheme == "https",
		time.Duration(c.cfg.DialTimeout)*time.Second, time.Duration(c.cfg.ResponseHeaderTimeout)*time.Second)
	if err != nil {
		return false
	}
	c.api = client
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
	body, err := c.rangeGetAtOnce(ctx, key, 0, 128)
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		body, err = c.rangeGetAtOnce(ctx, key, 0, 128)
	}
	return body, err
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
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		body, err = c.rangeGetAtOnce(ctx, key, offset, length)
	}
	return body, err
}

func (c *S3Client) rangeGetAtOnce(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, length))
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
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		etag, size, err = c.headObjectOnce(ctx, key)
	}
	if err != nil {
		return "", 0, err
	}
	return trimETagQuotes(etag), size, nil
}

func (c *S3Client) headObjectOnce(ctx context.Context, key string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := c.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", 0, err
	}
	return aws.ToString(out.ETag), aws.ToInt64(out.ContentLength), nil
}

// DownloadRange opens a streaming ranged GET of length bytes starting at
// start from the client's bound bucket. The caller must Close the reader.
// A 404/5xx surfaces at call time (the SDK issues the GET eagerly), so it
// does not hide inside the reader — mid-read failures after this point
// surface as read errors (the stream cannot be replayed). Node failover
// applies only to this initial request.
func (c *S3Client) DownloadRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	obj, err := c.downloadRangeOnce(ctx, key, start, length)
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		obj, err = c.downloadRangeOnce(ctx, key, start, length)
	}
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *S3Client) downloadRangeOnce(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1)),
	})
	if err != nil {
		return nil, err
	}
	// ContentLength comes from the response Content-Length. For a 206 it
	// must equal the requested length; anything else would otherwise
	// surface later as a short/over-long part upload with no actionable
	// error.
	if aws.ToInt64(out.ContentLength) != length {
		out.Body.Close()
		return nil, fmt.Errorf("get %s/%s bytes=%d-%d: response Content-Length %d, want %d", c.bucket, key, start, start+length-1, aws.ToInt64(out.ContentLength), length)
	}
	return out.Body, nil
}

// withUnsignedPayload is the per-call option for PUTs whose body is a live
// download stream (the relay paths: PutObjectStream, relay UploadPart). The
// v2 SDK cannot sign a non-seekable body over plain HTTP — the payload-hash
// middleware needs to read and rewind the stream, and the aws-chunked
// trailer escape only engages under TLS — so the wire would fail with
// "request stream is not seekable" before any bytes leave. Swapping the
// payload hash for UNSIGNED-PAYLOAD (x-amz-content-sha256: UNSIGNED-PAYLOAD)
// sends the body raw with no framing and no buffering, which is exactly the
// relay's memory-bounded streaming requirement; the byte-faithfulness of
// the relay is already verified end-to-end by the ETag comparison, not by
// the transport signature. minio's PutObjectStream previously achieved the
// same streaming via signed chunk frames (STREAMING-AWS4-HMAC-SHA256-PAYLOAD),
// which v2 does not implement.
func withUnsignedPayload(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware(stack)
	})
}

// putLocalPartSize is the multipart part size (and the single-PUT
// threshold) for PutObjectLocal.
const putLocalPartSize = int64(16 << 20)

// PutObjectLocal streams the local file at path into bucket/key and returns
// the destination ETag (quotes stripped). Nothing is buffered: the backup
// list archive can be gigabytes (the corrupted-objects list IS the disaster
// artifact), so memory stays bounded by one multipart part regardless of
// file size. Files above putLocalPartSize are re-uploaded as multipart
// parts (bounded to 10000 parts by growing the part size for absurd
// archive sizes) — the archive's ETag has no verifier, so the single-PUT
// pinning PutObjectStream needs does not apply. Node-fault retry re-opens
// the file and re-runs the whole upload: a local file is replayable from
// disk.
func (c *S3Client) PutObjectLocal(ctx context.Context, bucket, key, path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	upload := func() (string, error) {
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer f.Close()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()
		if fi.Size() <= putLocalPartSize {
			return c.putOnce(ctx, bucket, key, f, fi.Size())
		}
		return c.putLocalMultipart(ctx, bucket, key, f, fi.Size())
	}
	etag, err := upload()
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		etag, err = upload()
	}
	return etag, err
}

// putOnce streams one single PUT of the whole file. f is seekable, so the
// SDK signs it with a full payload hash (raw body, no chunked framing).
func (c *S3Client) putOnce(ctx context.Context, bucket, key string, f *os.File, size int64) (string, error) {
	out, err := c.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return "", err
	}
	return trimETagQuotes(aws.ToString(out.ETag)), nil
}

// putLocalMultipart uploads the file as multipart parts read via
// SectionReader (seekable, streamed — never the whole file in memory).
// Any failure aborts the upload so no partial object lingers.
func (c *S3Client) putLocalMultipart(ctx context.Context, bucket, key string, f *os.File, size int64) (string, error) {
	uploadID, err := c.CreateMultipart(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	partSize := putLocalPartSize
	if size/partSize > 10000 {
		partSize = (size + 9999) / 10000
	}
	nParts := (size + partSize - 1) / partSize
	parts := make([]UploadedPart, 0, nParts)
	for i := int64(0); i < nParts; i++ {
		off := i * partSize
		length := partSize
		if size-off < length {
			length = size - off
		}
		etag, err := c.UploadPart(ctx, bucket, key, uploadID, int(i)+1, io.NewSectionReader(f, off, length), length)
		if err != nil {
			c.AbortMultipart(ctx, bucket, key, uploadID)
			return "", err
		}
		parts = append(parts, UploadedPart{PartNumber: int(i) + 1, ETag: etag})
	}
	return c.CompleteMultipart(ctx, bucket, key, uploadID, parts)
}

// PutObjectStream uploads r to bucket/key without buffering it in memory.
// Unlike PutObjectLocal there is no node-fault retry: the caller's reader is
// a live download stream that cannot be replayed (a local file can be
// re-opened), so any failure (including mid-upload node faults) is returned
// to the caller.
//
// The aws SDK's PutObject is always a single PUT (no auto multipart), which
// is exactly the relay's requirement: a multipart destination ETag
// (md5-of-part-md5s-N) could never match the source regular object's plain
// MD5, so every large regular relay would fail the ETag verification.
// Regular objects are single-PUT uploads by definition, so >5GiB cannot
// legitimately occur; if one somehow did, the server rejects the oversized
// single PUT at upload time instead of succeeding as multipart and failing
// ETag verification after the bytes were already transferred.
func (c *S3Client) PutObjectStream(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	out, err := c.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          r,
		ContentLength: aws.Int64(size),
	}, withUnsignedPayload)
	if err != nil {
		return "", err
	}
	return trimETagQuotes(aws.ToString(out.ETag)), nil
}

// CreateMultipart starts a multipart upload in bucket/key and returns the
// upload ID.
func (c *S3Client) CreateMultipart(ctx context.Context, bucket, key string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	out, err := c.api.CreateMultipartUpload(ctx, input)
	if err != nil && isNodeFaultErr(err) && c.handleNodeFault() {
		out, err = c.api.CreateMultipartUpload(ctx, input)
	}
	if err != nil {
		return "", err
	}
	return aws.ToString(out.UploadId), nil
}

// UploadPart streams one part (partNum is 1-based) of a multipart upload
// and returns the part's ETag. The reader is NOT replayable: a mid-stream
// node fault surfaces as an error and the caller aborts the upload.
func (c *S3Client) UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error) {
	out, err := c.api.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(int32(partNum)),
		Body:          r,
		ContentLength: aws.Int64(size),
	}, withUnsignedPayload)
	if err != nil {
		return "", err
	}
	return trimETagQuotes(aws.ToString(out.ETag)), nil
}

// CompleteMultipart commits the multipart upload and returns the
// destination object's ETag.
func (c *S3Client) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []UploadedPart) (string, error) {
	cp := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		cp[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(p.PartNumber)),
			ETag:       aws.String(p.ETag),
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	out, err := c.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: cp},
	})
	if err != nil {
		return "", err
	}
	return trimETagQuotes(aws.ToString(out.ETag)), nil
}

// AbortMultipart discards an in-progress multipart upload.
func (c *S3Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := c.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

// isNodeFaultErr reports whether err is a transient node-fault error that
// should count toward isolating the bound node: connection-level failures,
// timeouts, and server-trouble 5xx responses. Business-level rejections
// (404 Not Found, 403 Forbidden, 400 Bad Request) are NOT node faults and
// propagate to the caller unchanged.
//
// Classification order matters:
//
//  1. *awshttp.ResponseError with a real HTTP response (StatusCode != 0)
//     FIRST, decided by status code alone. The type check must run before
//     the substring heuristics — a 4xx Message is server-written text and
//     may contain transport-flavored words ("request body read timeout",
//     "unexpected EOF"); letting string matching see it would misclassify
//     a business rejection as a node fault and burn healthy nodes. 501/505
//     are excluded even though they are 5xx: they are capability/protocol
//     gaps every node shares, so failover can never help.
//
//     StatusCode == 0 is NOT a real response: the SDK wraps transport-level
//     send failures (dial refused, RST, ...) in an awshttp.ResponseError
//     with a synthesized empty response — "https response error StatusCode:
//     0 ... request send failed, dial tcp ... connection refused". Those
//     must fall through to the heuristics below, or every dead-node dial
//     would classify as a business error and recovery/failover would
//     silently stop working.
//  2. context.DeadlineExceeded (our 30s call timeouts).
//  3. Substring heuristics over err.Error() — transport-layer failures
//     from the Go net stack, whether raw or wrapped by the SDK.
func isNodeFaultErr(err error) bool {
	if err == nil {
		return false
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.Response != nil && re.Response.StatusCode != 0 {
		switch re.Response.StatusCode {
		case http.StatusNotImplemented, http.StatusHTTPVersionNotSupported:
			return false
		}
		return re.Response.StatusCode >= 500 && re.Response.StatusCode < 600
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()
	for _, sig := range nodeFaultSigs {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// nodeFaultSigs are the substring signatures of transport-layer (non-S3)
// errors from the Go net stack. These only run for errors that are NOT a
// *awshttp.ResponseError, so their breadth cannot leak business errors into
// the failover path. Real error shapes, per signature:
//
//   - dial-phase failures on every platform start with "dial tcp <addr>:"
//     ("connect: connection refused", "connect: connection timed out",
//     "connectex: ..." on Windows, "no route to host", ...).
//   - established-connection failures: RST ("connection reset"), write on a
//     closed socket ("broken pipe"), socket deadline ("i/o timeout").
//   - "timeout" (lowercase) covers "net/http: TLS handshake timeout" and
//     friends; the capital-T form ("Client.Timeout exceeded while awaiting
//     headers") is wrapped in context.DeadlineExceeded and caught by the
//     errors.Is check above.
//   - "EOF" matches io.EOF ("EOF") and "unexpected EOF" — the peer closed
//     the connection mid-stream (idle-timeout kills in front of the node).
//   - "no such host" is DNS resolution failure.
//   - "transport" covers http2 transport internals.
//
// "connect: " was dropped: every Go error containing it is a dial error
// already caught by "dial tcp".
var nodeFaultSigs = []string{
	"dial tcp",
	"connection refused",
	"connection reset",
	"broken pipe",
	"i/o timeout",
	"timeout",
	"EOF",
	"no such host",
	"transport",
}

var _ S3API = (*S3Client)(nil)

var ErrNodeDown = errors.New("node down")
