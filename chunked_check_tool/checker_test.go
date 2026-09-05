package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestIsNormalETag(t *testing.T) {
	cases := []struct {
		etag   string
		normal bool
	}{
		{"0123456789abcdef0123456789abcdef", true},
		{"0123456789abcdef0123456789abcdef-2", false},
		{"ABCDEF0123456789abcdef0123456789abcdef", false}, // uppercase + too long
		{"", false},
		{"short", false},
		{"0123456789ABCDEF0123456789ABCDEF", false}, // uppercase
	}
	for _, c := range cases {
		got := isNormalETag(c.etag)
		if got != c.normal {
			t.Errorf("isNormalETag(%q)=%v want %v", c.etag, got, c.normal)
		}
	}
}

func TestChunkSigRegex(t *testing.T) {
	cases := []struct {
		body  string
		match bool
	}{
		{"1000;chunk-signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\r\n", true},
		{"ff;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\n", true},
		{"hello world", false},
		{"1000;chunk-signature=short\n", false}, // signature not 64
		{";chunk-signature=abcdef\n", false},    // empty chunk size
		{"1000;notchunk-signature=abc\n", false},
	}
	for _, c := range cases {
		got := chunkSigRe.Match([]byte(c.body))
		if got != c.match {
			t.Errorf("Match(%q)=%v want %v", c.body, got, c.match)
		}
	}
}

func TestCheckerHandleNormal(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object content here")}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 1})
	// success not enabled, no files written yet (deferred to Close)
}

func TestCheckerHandleCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	body := []byte("1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n")
	worker := &FakeS3{Body: body}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 1})
	if s.Snapshot().Corrupted != 1 {
		t.Errorf("corrupted=%d want 1", s.Snapshot().Corrupted)
	}
}

func TestCheckerHandleMultipartSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{
		Body: nil,
		Err:  nil,
	}
	// 用一个 wrap 检测是否调用 RangeGet
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 1})
	if called {
		t.Error("RangeGet should not be called for multipart")
	}
	if s.Snapshot().Multipart != 1 {
		t.Errorf("multipart=%d want 1", s.Snapshot().Multipart)
	}
}

func TestCheckerHandleRangeGetError(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 1})
	if s.Snapshot().CheckFailed != 1 {
		t.Errorf("checkfailed=%d want 1", s.Snapshot().CheckFailed)
	}
}

func TestCheckerHandleEmptyObjectSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{Err: errFake416}
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 0})
	if called {
		t.Error("RangeGet should not be called for size=0 object")
	}
	if got := s.Snapshot().CheckFailed; got != 0 {
		t.Errorf("checkfailed=%d want 0 (empty object should not be check-failed)", got)
	}
}

var errFake416 = errors.New("416 Range Not Satisfiable")

func TestCheckerHandleCheckFailedLogsStructured(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, _ := NewOutput(cfg)
	s := NewStats()
	worker := &FakeS3{Err: minio.ErrorResponse{
		Code:       "InvalidRange",
		Message:    "The requested range is not satisfiable",
		StatusCode: 416,
		RequestID:  "REQ-1234-ABCD",
	}}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "path/obj", ETag: "0123456789abcdef0123456789abcdef", Size: 1})
	if err := out.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// check_failed.txt keeps only the key — the .log next to it carries
	// the structured error info.
	cfBytes, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatalf("read check_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(cfBytes)); line != "path/obj" {
		t.Errorf("check_failed.txt = %q, want %q", line, "path/obj")
	}

	// check_failed.log is slog text-handler output. req_id must appear
	// right after msg (before key).
	logBytes, err := os.ReadFile(filepath.Join(dir, "check_failed.log"))
	if err != nil {
		t.Fatalf("read check_failed.log: %v", err)
	}
	log := string(logBytes)
	for _, want := range []string{
		`level=ERROR`,
		`msg="check failed"`,
		`req_id=REQ-1234-ABCD`,
		`key=path/obj`,
		`http_code=416`,
		`s3_code=InvalidRange`,
	} {
		if !strings.Contains(log, want) {
			t.Errorf("check_failed.log missing %q\nfull log:\n%s", want, log)
		}
	}
	// Field order: msg must come before req_id, which must come before key.
	msgIdx := strings.Index(log, `msg="check failed"`)
	reqIdx := strings.Index(log, `req_id=REQ-1234-ABCD`)
	keyIdx := strings.Index(log, `key=path/obj`)
	if !(msgIdx < reqIdx && reqIdx < keyIdx) {
		t.Errorf("field order wrong: msg@%d req_id@%d key@%d\n%s", msgIdx, reqIdx, keyIdx, log)
	}
}

// chunkSigBody is a 128-byte body that matches chunkSigRe (the streaming
// chunked-upload signature header). Used to simulate a corrupted multipart
// segment. The trailing bytes after the regex match are filler.
var chunkSigBody = []byte("1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n" + strings.Repeat("x", 128-72-2))

// multipartCfg builds a Config with the multipart segment check enabled at
// the given segment size. Used by the TestCheckerMultipartSegmentCheck* tests.
func multipartCfg(dir string, segSize int64) *Config {
	return &Config{
		OutputDir:            dir,
		IsCheck:              true,
		IsMultipartCheck:     true,
		MultipartSegmentSize: segSize,
	}
}

func TestCheckerMultipartSegmentCheckCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody}
	c := NewChecker(worker, out, s, cfg)
	// multipart etag + Size=10MB → 2 segments at offset 0 and 5MB. Body
	// matches at offset 0, so it's flagged immediately as corrupted.
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024})
	if got := s.Snapshot().CorruptedMultipart; got != 1 {
		t.Errorf("corrupted_multipart=%d want 1", got)
	}
	if got := s.Snapshot().Multipart; got != 0 {
		t.Errorf("multipart=%d want 0 (corrupted multipart should not also count as plain multipart)", got)
	}
}

func TestCheckerMultipartSegmentCheckClean(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object body, no chunk signature here")}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024})
	if got := s.Snapshot().CorruptedMultipart; got != 0 {
		t.Errorf("corrupted_multipart=%d want 0", got)
	}
	if got := s.Snapshot().Multipart; got != 1 {
		t.Errorf("multipart=%d want 1 (clean multipart should still be recorded as multipart)", got)
	}
}

func TestCheckerMultipartSegmentCheckRangeError(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024})
	if got := s.Snapshot().CheckFailed; got != 1 {
		t.Errorf("check_failed=%d want 1 (RangeGet error during segment check should surface as check_failed)", got)
	}
	if got := s.Snapshot().CorruptedMultipart; got != 0 {
		t.Errorf("corrupted_multipart=%d want 0", got)
	}
}

func TestCheckerMultipartSegmentCheckDisabled(t *testing.T) {
	dir := t.TempDir()
	// switch OFF: existing behavior, no segment check, recorded as plain multipart
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: false, MultipartSegmentSize: 5 * 1024 * 1024}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody} // would match if we checked — but we don't
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024})
	if got := s.Snapshot().CorruptedMultipart; got != 0 {
		t.Errorf("corrupted_multipart=%d want 0 (switch off)", got)
	}
	if got := s.Snapshot().Multipart; got != 1 {
		t.Errorf("multipart=%d want 1 (switch off, plain multipart)", got)
	}
}

// TestCheckerMultipartSegmentCheckSecondSegmentMatches verifies the "ANY
// segment" semantics: when the first segment does NOT match but a later
// one does, the object is still flagged as corrupted multipart. Uses
// RangeGetHandler to return different bodies per offset.
func TestCheckerMultipartSegmentCheckSecondSegmentMatches(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	cleanBody := []byte("clean segment, no signature")
	worker := &FakeS3{
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			if offset == 0 {
				return cleanBody, nil
			}
			return chunkSigBody, nil // second segment matches
		},
	}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024})
	if got := s.Snapshot().CorruptedMultipart; got != 1 {
		t.Errorf("corrupted_multipart=%d want 1 (second segment should trigger)", got)
	}
}

// helper: track RangeGet calls
type callTrackingS3 struct {
	*FakeS3
	called *bool
}

func (c *callTrackingS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	*c.called = true
	return c.FakeS3.RangeGet(ctx, key)
}
