package main

// FakeS3 — the in-memory S3API test double (moved here from s3client.go so
// the production file carries no test scaffolding; scriptedS3 in
// lister_test.go and countingS3 in walker_test.go are its siblings).

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

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

	// PutErr, when non-nil, makes PutObjectStream/PutObjectLocal fail.
	// Calls are recorded in Puts for assertion.
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

// PutCall records one FakeS3 upload invocation (PutObjectStream or
// PutObjectLocal).
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

// recordPut drains r, records the call, and returns the MD5 etag (or the
// injected PutErr). Shared by PutObjectStream and PutObjectLocal.
func (f *FakeS3) recordPut(bucket, key string, r io.Reader) (string, error) {
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
	return f.recordPut(bucket, key, r)
}

// PutObjectLocal reads the local file — a real disk read, matching what the
// production method streams — and records it like a streamed put.
func (f *FakeS3) PutObjectLocal(ctx context.Context, bucket, key, path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	return f.recordPut(bucket, key, fh)
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
