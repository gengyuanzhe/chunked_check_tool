package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeCore is a test double for the awsS3API interface. It records which
// list method was called and with what arguments, and returns canned
// results.
type fakeCore struct {
	v2Calls int
	v1Calls int

	lastV2Bucket     string
	lastV2Marker     string
	lastV2FetchOwner bool
	lastV1Bucket     string
	lastV1Marker     string

	v2Result *s3.ListObjectsV2Output
	v1Result *s3.ListObjectsOutput
	err      error
}

func (f *fakeCore) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.v2Calls++
	f.lastV2Bucket = aws.ToString(params.Bucket)
	f.lastV2FetchOwner = aws.ToBool(params.FetchOwner)
	// The caller distinguishes "first page" (StartAfter set, ContinuationToken
	// nil) from "subsequent page" (ContinuationToken set, StartAfter nil).
	// For test assertions we record whichever is non-empty as the "marker"
	// the implementation passed.
	switch {
	case aws.ToString(params.StartAfter) != "":
		f.lastV2Marker = aws.ToString(params.StartAfter)
	case aws.ToString(params.ContinuationToken) != "":
		f.lastV2Marker = aws.ToString(params.ContinuationToken)
	default:
		f.lastV2Marker = ""
	}
	return f.v2Result, f.err
}

func (f *fakeCore) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	f.v1Calls++
	f.lastV1Bucket = aws.ToString(params.Bucket)
	f.lastV1Marker = aws.ToString(params.Marker)
	return f.v1Result, f.err
}

// The non-list half of awsS3API is unused by the list tests; stubs keep
// fakeCore satisfying the interface.
func (f *fakeCore) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, f.err
}
func (f *fakeCore) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, f.err
}
func (f *fakeCore) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, f.err
}
func (f *fakeCore) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, f.err
}
func (f *fakeCore) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, f.err
}
func (f *fakeCore) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, f.err
}
func (f *fakeCore) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, f.err
}

var (
	v1Obj = func(key string) types.Object {
		return types.Object{Key: aws.String(key), ETag: aws.String(`"0123456789abcdef0123456789abcdef"`)}
	}
	v2Obj = v1Obj
)

// TestListPageV1_UsesMarkerAndNextMarker verifies that with
// ListAPIVersion=1, S3Client.ListPage calls ListObjects (not V2), passes
// the startAfter/marker correctly, and returns NextMarker as the
// next cursor when the response is truncated.
func TestListPageV1_UsesMarkerAndNextMarker(t *testing.T) {
	core := &fakeCore{
		v1Result: &s3.ListObjectsOutput{
			Contents:    []types.Object{v1Obj("a")},
			IsTruncated: aws.Bool(true),
			NextMarker:  aws.String("next-marker-from-s3"),
		},
	}
	c := &S3Client{api: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

	// First call: startAfter="seed-key", continuationToken="" → marker should
	// be "seed-key".
	objs, prefixes, next, err := c.ListPage(context.Background(), "pfx/", "seed-key", "", true, 1000)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if core.v1Calls != 1 || core.v2Calls != 0 {
		t.Errorf("V1 calls=%d V2 calls=%d, want V1=1 V2=0", core.v1Calls, core.v2Calls)
	}
	if core.lastV1Marker != "seed-key" {
		t.Errorf("first-call marker=%q want %q", core.lastV1Marker, "seed-key")
	}
	if next != "next-marker-from-s3" {
		t.Errorf("next=%q want %q", next, "next-marker-from-s3")
	}
	if len(objs) != 1 || objs[0].Key != "a" {
		t.Errorf("objs=%v want [a]", objs)
	}
	if len(prefixes) != 0 {
		t.Errorf("prefixes=%v want []", prefixes)
	}

	// Second call: startAfter="" (caller clears it after first page),
	// continuationToken="next-marker-from-s3" → marker should be the
	// continuation token.
	_, _, _, err = c.ListPage(context.Background(), "pfx/", "", "next-marker-from-s3", true, 1000)
	if err != nil {
		t.Fatalf("ListPage second: %v", err)
	}
	if core.lastV1Marker != "next-marker-from-s3" {
		t.Errorf("second-call marker=%q want %q", core.lastV1Marker, "next-marker-from-s3")
	}
}

// TestListPageV1_LastKeyFallback verifies that when V1 returns
// IsTruncated=true but NextMarker is empty (the no-delimiter case), the
// implementation falls back to the last Contents key as the next marker.
func TestListPageV1_LastKeyFallback(t *testing.T) {
	core := &fakeCore{
		v1Result: &s3.ListObjectsOutput{
			Contents: []types.Object{
				v1Obj("k1"),
				v1Obj("k2"),
				v1Obj("lastkey"),
			},
			IsTruncated: aws.Bool(true),
			// NextMarker intentionally nil — no delimiter was used.
		},
	}
	c := &S3Client{api: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

	_, _, next, err := c.ListPage(context.Background(), "pfx/", "", "", false, 1000)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if next != "lastkey" {
		t.Errorf("next=%q want %q (last Contents key fallback)", next, "lastkey")
	}
}

// TestListPageV1_NotTruncated verifies that a non-truncated V1 response
// yields next="" so the caller stops paginating.
func TestListPageV1_NotTruncated(t *testing.T) {
	core := &fakeCore{
		v1Result: &s3.ListObjectsOutput{
			Contents:    []types.Object{v1Obj("only")},
			IsTruncated: aws.Bool(false),
		},
	}
	c := &S3Client{api: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

	_, _, next, err := c.ListPage(context.Background(), "pfx/", "", "", true, 1000)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if next != "" {
		t.Errorf("next=%q want empty (not truncated)", next)
	}
}

// TestListPageV2_Regression verifies the default (V2) path still calls
// ListObjectsV2 and uses NextContinuationToken as the cursor.
func TestListPageV2_Regression(t *testing.T) {
	core := &fakeCore{
		v2Result: &s3.ListObjectsV2Output{
			Contents:              []types.Object{v2Obj("a")},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("tok-v2"),
		},
	}
	// ListAPIVersion=0 should fall through to the V2 branch (default).
	c := &S3Client{api: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 0}}

	_, _, next, err := c.ListPage(context.Background(), "pfx/", "", "prev-tok", true, 1000)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if core.v2Calls != 1 || core.v1Calls != 0 {
		t.Errorf("V2 calls=%d V1 calls=%d, want V2=1 V1=0", core.v2Calls, core.v1Calls)
	}
	if core.lastV2Marker != "prev-tok" {
		t.Errorf("V2 marker=%q want %q", core.lastV2Marker, "prev-tok")
	}
	if next != "tok-v2" {
		t.Errorf("next=%q want %q", next, "tok-v2")
	}
}

// TestListPageV2_SetsFetchOwner pins the V2 request shape that keeps
// per-owner output partitioning working: minio-go always sent fetch-owner
// on the wire; the aws SDK does NOT default it, so ListPage must set it
// explicitly or every OwnerID comes back empty and all results collapse
// into _unknown/.
func TestListPageV2_SetsFetchOwner(t *testing.T) {
	core := &fakeCore{v2Result: &s3.ListObjectsV2Output{}}
	c := &S3Client{api: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 2}}
	if _, _, _, err := c.ListPage(context.Background(), "p", "", "", false, 100); err != nil {
		t.Fatal(err)
	}
	if !core.lastV2FetchOwner {
		t.Error("V2 request must set FetchOwner=true (owner-driven output partitioning depends on it)")
	}
}
