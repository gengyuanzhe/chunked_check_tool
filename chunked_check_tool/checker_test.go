package main

import (
	"context"
	"errors"
	"testing"
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

// helper: track RangeGet calls
type callTrackingS3 struct {
	*FakeS3
	called *bool
}

func (c *callTrackingS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	*c.called = true
	return c.FakeS3.RangeGet(ctx, key)
}
