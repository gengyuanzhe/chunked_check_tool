package main

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
)

type recordingUploader struct {
	mu          sync.Mutex
	uploads     []recordedUpload
	multipart   []recordedMultipart
	bucketOK    bool
	putErr      error
	multipartErr error
}

type recordedUpload struct {
	endpointIdx int
	bucket      string
	key         string
	size        int64
	partSize    int64
	multipart   bool
}

type recordedMultipart struct {
	bucket   string
	key      string
	size     int64
	partSize int64
	pattern  []int
}

func (r *recordingUploader) UploadObject(ctx context.Context, endpointIdx int, bucket, key string, body io.Reader, size, partSize int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.putErr != nil {
		return false, r.putErr
	}
	multipart := size > partSize
	r.uploads = append(r.uploads, recordedUpload{
		endpointIdx: endpointIdx,
		bucket:      bucket,
		key:         key,
		size:        size,
		partSize:    partSize,
		multipart:   multipart,
	})
	io.Copy(io.Discard, body)
	return multipart, nil
}

func (r *recordingUploader) UploadObjectMultipart(ctx context.Context, bucket, key string, body io.Reader, size, partSize int64, pattern []int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.multipartErr != nil {
		return r.multipartErr
	}
	r.multipart = append(r.multipart, recordedMultipart{
		bucket:   bucket,
		key:      key,
		size:     size,
		partSize: partSize,
		pattern:  append([]int(nil), pattern...),
	})
	io.Copy(io.Discard, body)
	return nil
}

func (r *recordingUploader) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return r.bucketOK, nil
}

var _ Uploader = (*recordingUploader)(nil)

func TestProcessOne_BranchesToMultipartWhenPatternSet(t *testing.T) {
	cfg := &Config{
		Endpoints:                []string{"a:80", "b:80"},
		Bucket:                   "bkt",
		Depth:                    1,
		Width:                    2,
		FilesPerDir:              1,
		ObjectSizeMin:            15 * 1024 * 1024,
		ObjectSizeMax:            15 * 1024 * 1024,
		PartSizeMin:             5 * 1024 * 1024,
		PartSizeMax:             5 * 1024 * 1024,
		Concurrency:              1,
		MultipartEndpointPattern: []int{0, 0, 1, 0, 1},
	}
	pool := NewNodePool(cfg)
	uploader := &recordingUploader{bucketOK: true}
	stats := NewStats()
	progress := NewProgress(stats, 0, io.Discard)
	md5w, _ := NewMD5Writer(filepath.Join(t.TempDir(), "md5.txt"))
	defer md5w.Close()

	r := rand.New(rand.NewSource(0))
	key := ObjectKey{Key: "data/k1", Idx: 0}
	if err := processOne(context.Background(), cfg, pool, uploader, stats, progress, md5w, r, key); err != nil {
		t.Fatalf("processOne: %v", err)
	}
	if len(uploader.uploads) != 0 {
		t.Errorf("UploadObject called %d times, want 0 (pattern routes to manual)", len(uploader.uploads))
	}
	if len(uploader.multipart) != 1 {
		t.Fatalf("UploadObjectMultipart called %d times, want 1", len(uploader.multipart))
	}
	m := uploader.multipart[0]
	if m.bucket != "bkt" || m.key != "data/k1" {
		t.Errorf("multipart bucket/key = %q/%q, want bkt/data/k1", m.bucket, m.key)
	}
	wantPattern := []int{0, 0, 1, 0, 1}
	if len(m.pattern) != len(wantPattern) {
		t.Fatalf("pattern len = %d, want %d", len(m.pattern), len(wantPattern))
	}
	for i, v := range m.pattern {
		if v != wantPattern[i] {
			t.Errorf("pattern[%d] = %d, want %d", i, v, wantPattern[i])
		}
	}
	if snap := stats.Snapshot(); snap.MultipartObjs != 1 || snap.SingleObjs != 0 {
		t.Errorf("stats = mp=%d single=%d, want mp=1 single=0", snap.MultipartObjs, snap.SingleObjs)
	}
}

func TestProcessOne_FallsBackToUploadObjectWhenPatternEmpty(t *testing.T) {
	cfg := &Config{
		Endpoints:     []string{"a:80"},
		Bucket:        "bkt",
		Depth:         1,
		Width:         2,
		FilesPerDir:   1,
		ObjectSizeMin: 1024,
		ObjectSizeMax: 1024,
		PartSizeMin:  5 * 1024 * 1024,
		PartSizeMax:  5 * 1024 * 1024,
		Concurrency:   1,
	}
	pool := NewNodePool(cfg)
	uploader := &recordingUploader{bucketOK: true}
	stats := NewStats()
	progress := NewProgress(stats, 0, io.Discard)
	md5w, _ := NewMD5Writer(filepath.Join(t.TempDir(), "md5.txt"))
	defer md5w.Close()

	r := rand.New(rand.NewSource(0))
	key := ObjectKey{Key: "data/k1", Idx: 0}
	if err := processOne(context.Background(), cfg, pool, uploader, stats, progress, md5w, r, key); err != nil {
		t.Fatalf("processOne: %v", err)
	}
	if len(uploader.uploads) != 1 {
		t.Errorf("UploadObject called %d times, want 1", len(uploader.uploads))
	}
	if len(uploader.multipart) != 0 {
		t.Errorf("UploadObjectMultipart called %d times, want 0", len(uploader.multipart))
	}
}

func TestRunWorkers_EndToEndSmall(t *testing.T) {
	cfg := &Config{
		Endpoints:     []string{"a:80", "b:80"},
		Scheme:        "http",
		AK:            "ak",
		SK:            "sk",
		Bucket:        "testbucket",
		Depth:         2,
		Width:         3,
		FilesPerDir:   2,
		ObjectSizeMin: 100,
		ObjectSizeMax: 1000,
		PartSizeMin:  5 * 1024 * 1024,
		PartSizeMax:  5 * 1024 * 1024,
		Concurrency:   2,
	}
	pool := NewNodePool(cfg)
	uploader := &recordingUploader{bucketOK: true}
	stats := NewStats()
	progress := NewProgress(stats, 1000, io.Discard)

	dir := t.TempDir()
	md5Path := filepath.Join(dir, "md5.txt")
	md5w, err := NewMD5Writer(md5Path)
	if err != nil {
		t.Fatalf("NewMD5Writer: %v", err)
	}

	ctx := context.Background()
	if err := runWorkers(ctx, cfg, pool, uploader, stats, progress, md5w, io.Discard); err != nil {
		t.Fatalf("runWorkers: %v", err)
	}
	if err := md5w.Close(); err != nil {
		t.Fatalf("md5 Close: %v", err)
	}

	// Total = (depth + (width-1)*(depth-1) + width) * files_per_dir = (2 + 2*1 + 3) * 2 = 14
	wantTotal := 14
	if snap := stats.Snapshot(); snap.Uploaded != int64(wantTotal) {
		t.Errorf("Uploaded = %d, want %d", snap.Uploaded, wantTotal)
	} else if snap.Failed != 0 {
		t.Errorf("Failed = %d, want 0", snap.Failed)
	} else if snap.SingleObjs != int64(wantTotal) {
		t.Errorf("SingleObjs = %d, want %d (all small, no multipart)", snap.SingleObjs, wantTotal)
	} else if snap.MultipartObjs != 0 {
		t.Errorf("MultipartObjs = %d, want 0", snap.MultipartObjs)
	}

	if len(uploader.uploads) != wantTotal {
		t.Errorf("uploader got %d calls, want %d", len(uploader.uploads), wantTotal)
	}

	// Verify round-robin: 14 objects / 2 endpoints → 7 each
	counts := map[int]int{}
	for _, u := range uploader.uploads {
		counts[u.endpointIdx]++
	}
	for idx, c := range counts {
		if c != 7 {
			t.Errorf("endpoint %d got %d calls, want 7 (round-robin)", idx, c)
		}
	}

	// Verify md5 file has 10 lines, each `testbucket|<key>|<md5hex>`
	data, err := os.ReadFile(md5Path)
	if err != nil {
		t.Fatalf("read md5 file: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != wantTotal {
		t.Errorf("md5 file has %d lines, want %d", len(lines), wantTotal)
	}
	for i, l := range lines {
		parts := strings.Split(l, "|")
		if len(parts) != 3 {
			t.Errorf("line[%d] = %q, want 3 |-separated fields", i, l)
			continue
		}
		if parts[0] != "testbucket" {
			t.Errorf("line[%d] bucket = %q, want testbucket", i, parts[0])
		}
		if len(parts[2]) != 32 {
			t.Errorf("line[%d] md5 = %q, want 32 hex chars", i, parts[2])
		}
	}
}

func TestRunWorkers_LargeObjectsMultipart(t *testing.T) {
	cfg := &Config{
		Endpoints:     []string{"a:80"},
		Bucket:        "b",
		Depth:         1,
		Width:         2,
		FilesPerDir:   3,
		ObjectSizeMin: 6 * 1024 * 1024, // 6MiB > 5MiB partSize → multipart
		ObjectSizeMax: 6 * 1024 * 1024,
		PartSizeMin:  5 * 1024 * 1024,
		PartSizeMax:  5 * 1024 * 1024,
		Concurrency:   1,
	}
	pool := NewNodePool(cfg)
	uploader := &recordingUploader{bucketOK: true}
	stats := NewStats()
	progress := NewProgress(stats, 0, io.Discard)
	dir := t.TempDir()
	md5w, _ := NewMD5Writer(filepath.Join(dir, "md5.txt"))

	if err := runWorkers(context.Background(), cfg, pool, uploader, stats, progress, md5w, io.Discard); err != nil {
		t.Fatalf("runWorkers: %v", err)
	}
	md5w.Close()

	// Total = (depth + (width-1)*(depth-1) + width) * files_per_dir = (1 + 0 + 2) * 3 = 9
	if snap := stats.Snapshot(); snap.MultipartObjs != 9 {
		t.Errorf("MultipartObjs = %d, want 9", snap.MultipartObjs)
	}
}

func TestRunWorkers_FailuresRecordedButFlowContinues(t *testing.T) {
	cfg := &Config{
		Endpoints:     []string{"a:80"},
		Bucket:        "b",
		Depth:         1,
		Width:         2,
		FilesPerDir:   5,
		ObjectSizeMin: 1,
		ObjectSizeMax: 1,
		PartSizeMin:  5 * 1024 * 1024,
		PartSizeMax:  5 * 1024 * 1024,
		Concurrency:   1,
	}
	uploader := &recordingUploader{
		bucketOK: true,
		putErr:   &minio.ErrorResponse{Code: "InternalError", StatusCode: 500},
	}
	pool := NewNodePool(cfg)
	stats := NewStats()
	progress := NewProgress(stats, 0, io.Discard)
	dir := t.TempDir()
	md5w, _ := NewMD5Writer(filepath.Join(dir, "md5.txt"))
	defer md5w.Close()

	_ = runWorkers(context.Background(), cfg, pool, uploader, stats, progress, md5w, io.Discard)

	// Total = (1 + 0 + 2) * 5 = 15
	if snap := stats.Snapshot(); snap.Uploaded != 0 || snap.Failed != 15 {
		t.Errorf("snapshot = %+v, want Uploaded=0 Failed=15", snap)
	}
}
