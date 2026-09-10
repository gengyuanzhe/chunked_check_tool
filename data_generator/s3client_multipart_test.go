package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
)

type fakeMultipartCall struct {
	op        string
	endpoint  int
	bucket    string
	object    string
	uploadID  string
	partID    int
	size      int64
	checksum  minio.ChecksumType
	parts     []minio.CompletePart
	err       error
}

type fakeMultipartState struct {
	mu           sync.Mutex
	calls        []fakeMultipartCall
	nextUploadID int
	uploads      map[string]map[int]int64
	initErr      error
	partErrAt    int
	completeErr  error
	abortErr     error
}

type fakeMultipartClient struct {
	endpointIdx int
	state       *fakeMultipartState
}

func (f *fakeMultipartClient) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error) {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	err := f.state.initErr
	f.state.calls = append(f.state.calls, fakeMultipartCall{op: "init", endpoint: f.endpointIdx, bucket: bucket, object: object, checksum: opts.Checksum, err: err})
	if err != nil {
		return "", err
	}
	f.state.nextUploadID++
	id := fmt.Sprintf("upload-%d", f.state.nextUploadID)
	if f.state.uploads == nil {
		f.state.uploads = map[string]map[int]int64{}
	}
	f.state.uploads[id] = map[int]int64{}
	return id, nil
}

func (f *fakeMultipartClient) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error) {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	var err error
	if f.state.partErrAt != 0 && f.state.partErrAt == partID {
		err = errors.New("simulated part failure")
	}
	f.state.calls = append(f.state.calls, fakeMultipartCall{op: "part", endpoint: f.endpointIdx, bucket: bucket, object: object, uploadID: uploadID, partID: partID, size: size, err: err})
	if err != nil {
		return minio.ObjectPart{}, err
	}
	if f.state.uploads[uploadID] == nil {
		f.state.uploads[uploadID] = map[int]int64{}
	}
	f.state.uploads[uploadID][partID] = size
	return minio.ObjectPart{PartNumber: partID, Size: size}, nil
}

func (f *fakeMultipartClient) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	err := f.state.completeErr
	f.state.calls = append(f.state.calls, fakeMultipartCall{op: "complete", endpoint: f.endpointIdx, bucket: bucket, object: object, uploadID: uploadID, parts: parts, checksum: opts.Checksum, err: err})
	if err != nil {
		return minio.UploadInfo{}, err
	}
	return minio.UploadInfo{Bucket: bucket, Key: object}, nil
}

func (f *fakeMultipartClient) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	f.state.calls = append(f.state.calls, fakeMultipartCall{op: "abort", endpoint: f.endpointIdx, bucket: bucket, object: object, uploadID: uploadID})
	return f.state.abortErr
}

func newMultipartUploaderWithFake(state *fakeMultipartState, useTrailer bool, endpoints []string) *S3Uploader {
	pool := &NodePool{endpoints: endpoints, scheme: "http"}
	factory := func(endpoint string) (minioCoreAPI, error) {
		for i, e := range endpoints {
			if e == endpoint {
				return &fakeMultipartClient{endpointIdx: i, state: state}, nil
			}
		}
		return nil, fmt.Errorf("unknown endpoint %q", endpoint)
	}
	return &S3Uploader{
		pool:        pool,
		useTrailer:  useTrailer,
		coreFactory: factory,
		cores:       make(map[int]minioCoreAPI),
	}
}

func TestS3Uploader_MultipartPatternHappyPath(t *testing.T) {
	state := &fakeMultipartState{uploads: map[string]map[int]int64{}}
	u := newMultipartUploaderWithFake(state, false, []string{"1.1.1.1:80", "2.2.2.2:80"})

	size := int64(15 * 1024 * 1024)
	content := make([]byte, size)
	partSize := int64(5 * 1024 * 1024)
	pattern := []int{0, 0, 1, 0, 1}

	if err := u.UploadObjectMultipart(context.Background(), "bkt", "k1", bytes.NewReader(content), size, partSize, pattern); err != nil {
		t.Fatalf("UploadObjectMultipart: %v", err)
	}
	wantOps := []struct {
		op       string
		endpoint int
	}{
		{"init", 0},
		{"part", 0},
		{"part", 1},
		{"part", 0},
		{"complete", 1},
	}
	if len(state.calls) != len(wantOps) {
		t.Fatalf("call count = %d, want %d (calls=%+v)", len(state.calls), len(wantOps), state.calls)
	}
	for i, want := range wantOps {
		got := state.calls[i]
		if got.op != want.op || got.endpoint != want.endpoint {
			t.Errorf("call[%d] = %s/ep%d, want %s/ep%d", i, got.op, got.endpoint, want.op, want.endpoint)
		}
	}
	for i, wantSize := range []int64{partSize, partSize, partSize} {
		if state.calls[1+i].size != wantSize {
			t.Errorf("part[%d].size = %d, want %d", i, state.calls[1+i].size, wantSize)
		}
	}
	if state.calls[4].bucket != "bkt" || state.calls[4].object != "k1" {
		t.Errorf("complete bucket/object = %q/%q, want bkt/k1", state.calls[4].bucket, state.calls[4].object)
	}
	if len(state.calls[4].parts) != 3 {
		t.Errorf("complete parts len = %d, want 3", len(state.calls[4].parts))
	}
	for i, p := range state.calls[4].parts {
		if p.PartNumber != i+1 {
			t.Errorf("complete parts[%d].PartNumber = %d, want %d", i, p.PartNumber, i+1)
		}
	}
}

func TestS3Uploader_MultipartFailureTriggersAbort(t *testing.T) {
	state := &fakeMultipartState{
		uploads:   map[string]map[int]int64{},
		partErrAt: 2,
	}
	u := newMultipartUploaderWithFake(state, false, []string{"1.1.1.1:80", "2.2.2.2:80"})

	content := make([]byte, 15*1024*1024)
	pattern := []int{0, 0, 1, 0, 1}

	err := u.UploadObjectMultipart(context.Background(), "bkt", "k1", bytes.NewReader(content), int64(len(content)), 5*1024*1024, pattern)
	if err == nil {
		t.Fatal("expected error from part 2 failure, got nil")
	}
	wantOps := []struct {
		op       string
		endpoint int
	}{
		{"init", 0},
		{"part", 0},
		{"part", 1},
		{"abort", 0},
	}
	if len(state.calls) != len(wantOps) {
		t.Fatalf("call count = %d, want %d (calls=%+v)", len(state.calls), len(wantOps), state.calls)
	}
	for i, want := range wantOps {
		got := state.calls[i]
		if got.op != want.op || got.endpoint != want.endpoint {
			t.Errorf("call[%d] = %s/ep%d, want %s/ep%d", i, got.op, got.endpoint, want.op, want.endpoint)
		}
	}
}

func TestS3Uploader_MultipartUseTrailerSetsChecksum(t *testing.T) {
	state := &fakeMultipartState{uploads: map[string]map[int]int64{}}
	u := newMultipartUploaderWithFake(state, true, []string{"1.1.1.1:80", "2.2.2.2:80"})

	content := make([]byte, 15*1024*1024)
	pattern := []int{0, 0, 1, 0, 1}

	if err := u.UploadObjectMultipart(context.Background(), "bkt", "k1", bytes.NewReader(content), int64(len(content)), 5*1024*1024, pattern); err != nil {
		t.Fatalf("UploadObjectMultipart: %v", err)
	}
	if state.calls[0].checksum != minio.ChecksumSHA256 {
		t.Errorf("init checksum = %d, want %d", state.calls[0].checksum, minio.ChecksumSHA256)
	}
	last := state.calls[len(state.calls)-1]
	if last.checksum != minio.ChecksumSHA256 {
		t.Errorf("complete checksum = %d, want %d", last.checksum, minio.ChecksumSHA256)
	}
}

func TestS3Uploader_MultipartPatternLengthMismatch(t *testing.T) {
	state := &fakeMultipartState{uploads: map[string]map[int]int64{}}
	u := newMultipartUploaderWithFake(state, false, []string{"1.1.1.1:80"})

	content := make([]byte, 15*1024*1024)
	pattern := []int{0, 0, 1, 1}
	err := u.UploadObjectMultipart(context.Background(), "bkt", "k1", bytes.NewReader(content), int64(len(content)), 5*1024*1024, pattern)
	if err == nil {
		t.Fatal("expected pattern length mismatch error, got nil")
	}
	if len(state.calls) != 0 {
		t.Errorf("expected no calls on mismatch, got %d", len(state.calls))
	}
}
