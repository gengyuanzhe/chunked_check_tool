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
		{";chunk-signature=abcdef\n", false},     // empty chunk size
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
	c := NewChecker(worker, out, s, false)
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
	c := NewChecker(worker, out, s, false)
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
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, false)
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
	c := NewChecker(worker, out, s, false)
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
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, false)
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
	}}
	c := NewChecker(worker, out, s, false)
	c.Handle(ObjectInfo{Key: "path/obj", ETag: "0123456789abcdef0123456789abcdef", Size: 1})
	if err := out.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// check_failed.txt keeps the original "key err" format.
	cfBytes, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatalf("read check_failed.txt: %v", err)
	}
	if !strings.HasPrefix(string(cfBytes), "path/obj ") {
		t.Errorf("check_failed.txt line = %q, want prefix %q", string(cfBytes), "path/obj ")
	}

	// check_failed.log is now slog text-handler output. Assert presence of
	// structured fields rather than a fixed column order.
	logBytes, err := os.ReadFile(filepath.Join(dir, "check_failed.log"))
	if err != nil {
		t.Fatalf("read check_failed.log: %v", err)
	}
	log := string(logBytes)
	for _, want := range []string{
		`level=ERROR`,
		`msg="check failed"`,
		`key=path/obj`,
		`http_code=416`,
		`s3_code=InvalidRange`,
	} {
		if !strings.Contains(log, want) {
			t.Errorf("check_failed.log missing %q\nfull log:\n%s", want, log)
		}
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
