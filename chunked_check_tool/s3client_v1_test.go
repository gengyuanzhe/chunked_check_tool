package main

import (
	"context"
	"testing"

	"github.com/minio/minio-go/v7"
)

// fakeCore is a test double for the minioListAPI interface. It records
// which method was called and with what arguments, and returns canned
// results.
type fakeCore struct {
	v2Calls int
	v1Calls int

	lastV2Bucket string
	lastV2Marker string
	lastV1Bucket string
	lastV1Marker string

	v2Result minio.ListBucketV2Result
	v1Result minio.ListBucketResult
	err      error
}

func (f *fakeCore) ListObjectsV2(bucketName, prefix, startAfter, continuationToken, delimiter string, maxKeys int) (minio.ListBucketV2Result, error) {
	f.v2Calls++
	f.lastV2Bucket = bucketName
	// The caller distinguishes "first page" (startAfter set, continuationToken
	// empty) from "subsequent page" (continuationToken set, startAfter
	// empty). For test assertions we record whichever is non-empty as the
	// "marker" the implementation passed.
	switch {
	case startAfter != "":
		f.lastV2Marker = startAfter
	case continuationToken != "":
		f.lastV2Marker = continuationToken
	default:
		f.lastV2Marker = ""
	}
	return f.v2Result, f.err
}

func (f *fakeCore) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListBucketResult, error) {
	f.v1Calls++
	f.lastV1Bucket = bucket
	f.lastV1Marker = marker
	return f.v1Result, f.err
}

// TestListPageV1_UsesMarkerAndNextMarker verifies that with
// ListAPIVersion=1, S3Client.ListPage calls Core.ListObjects (not V2),
// passes the startAfter/marker correctly, and returns NextMarker as the
// next cursor when the response is truncated.
func TestListPageV1_UsesMarkerAndNextMarker(t *testing.T) {
	core := &fakeCore{
		v1Result: minio.ListBucketResult{
			Contents:    []minio.ObjectInfo{{Key: "a", ETag: `"0123456789abcdef0123456789abcdef"`}},
			IsTruncated: true,
			NextMarker:  "next-marker-from-s3",
		},
	}
	c := &S3Client{core: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

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
		v1Result: minio.ListBucketResult{
			Contents: []minio.ObjectInfo{
				{Key: "k1", ETag: `"0123456789abcdef0123456789abcdef"`},
				{Key: "k2", ETag: `"0123456789abcdef0123456789abcdef"`},
				{Key: "lastkey", ETag: `"0123456789abcdef0123456789abcdef"`},
			},
			IsTruncated: true,
			// NextMarker intentionally empty — no delimiter was used.
		},
	}
	c := &S3Client{core: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

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
		v1Result: minio.ListBucketResult{
			Contents:    []minio.ObjectInfo{{Key: "only", ETag: `"0123456789abcdef0123456789abcdef"`}},
			IsTruncated: false,
		},
	}
	c := &S3Client{core: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 1}}

	_, _, next, err := c.ListPage(context.Background(), "pfx/", "", "", true, 1000)
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if next != "" {
		t.Errorf("next=%q want empty (not truncated)", next)
	}
}

// TestListPageV2_Regression verifies the default (V2) path still calls
// Core.ListObjectsV2 and uses NextContinuationToken as the cursor.
func TestListPageV2_Regression(t *testing.T) {
	core := &fakeCore{
		v2Result: minio.ListBucketV2Result{
			Contents:              []minio.ObjectInfo{{Key: "a", ETag: `"0123456789abcdef0123456789abcdef"`}},
			IsTruncated:           true,
			NextContinuationToken: "tok-v2",
		},
	}
	// ListAPIVersion=0 should fall through to the V2 branch (default).
	c := &S3Client{core: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 0}}

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
