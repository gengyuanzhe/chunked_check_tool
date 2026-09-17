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
		// De-anchored: matches mid-body, not just at byte 0. Boundary probes
		// read [off-128, off+128] where the next part's head lands at byte 128.
		{"garbage prefix 1000;chunk-signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\r\n", true},
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
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object content here")}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "", ETag: "0123456789abcdef0123456789abcdef", Size: 1, IsMultipart: false, Offsets: nil})
	// success not enabled, no files written yet (deferred to Close)
}

func TestCheckerHandleCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	body := []byte("1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n")
	worker := &FakeS3{Body: body}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 1, IsMultipart: false, Offsets: nil})
	if s.Snapshot().CorruptedObjects != 1 {
		t.Errorf("corrupted=%d want 1", s.Snapshot().CorruptedObjects)
	}
}

func TestCheckerHandleMultipartSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{
		Body: nil,
		Err:  nil,
	}
	// 用一个 wrap 检测是否调用 RangeGet
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", IsMultipart: true, Offsets: nil})
	if called {
		t.Error("RangeGet should not be called for multipart")
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0 (segment check off — not verified, must not claim clean)", got)
	}
}

func TestCheckerHandleRangeGetError(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 1, IsMultipart: false, Offsets: nil})
	if s.Snapshot().CheckFailed != 1 {
		t.Errorf("checkfailed=%d want 1", s.Snapshot().CheckFailed)
	}
}

func TestCheckerHandleEmptyObjectSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{Err: errFake416}
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 0, IsMultipart: false, Offsets: nil})
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
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Err: minio.ErrorResponse{
		Code:       "InvalidRange",
		Message:    "The requested range is not satisfiable",
		StatusCode: 416,
		RequestID:  "REQ-1234-ABCD",
	}}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "path/obj", Size: 1, IsMultipart: false, Offsets: nil})
	if err := out.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// check_failed.txt keeps only the key — the .log next to it carries
	// the structured error info.
	cfBytes, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatalf("read check_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(cfBytes)); line != "test-bkt|path/obj" {
		t.Errorf("check_failed.txt = %q, want %q", line, "test-bkt|path/obj")
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
		`bucket=test-bkt`,
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
		OutputDir:               dir,
		IsCheck:                 true,
		MultipartCheckMode: MultipartCheckModeSegment,
		IsSuccessLog:            true,
		IsMultipartSuccessLog:   true,
		MultipartSegmentSize:    segSize,
	}
}

// TestCheckerMultipartSegmentCheckCorrupted: switch on, segment matches →
// corrupted multipart file goes to <owner>/corrupted_mp.txt
// (per-owner routing).
func TestCheckerMultipartSegmentCheckCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody}
	c := NewChecker(worker, out, s, cfg)
	// multipart etag + Size=10MB → 2 segments at offset 0 and 5MB. Body
	// matches at offset 0, so it's flagged immediately as corrupted.
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1", got)
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0 (corrupted multipart should not also count as clean)", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// Per-owner file must exist with the key (default format <bucket>|<key>).
	data, err := os.ReadFile(filepath.Join(dir, "owner-A", "corrupted_mp.txt"))
	if err != nil {
		t.Fatalf("read corrupted_mp: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k" {
		t.Errorf("corrupted_mp.txt = %q, want %q", line, "test-bkt|k")
	}
}

func TestCheckerMultipartSegmentCheckClean(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object body, no chunk signature here")}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupted_mp=%d want 0", got)
	}
	if got := s.Snapshot().OkMp; got != 1 {
		t.Errorf("ok_mp=%d want 1 (clean multipart passed segment check)", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// IsSuccessLog=true → clean multipart goes to <owner>/ok_mp.txt (bucket|key)
	data, err := os.ReadFile(filepath.Join(dir, "owner-A", "ok_mp.txt"))
	if err != nil {
		t.Fatalf("read ok_multipart: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k" {
		t.Errorf("ok_mp.txt = %q, want %q", line, "test-bkt|k")
	}
}

// TestCheckerMultipartSegmentCheckRangeError: segment RangeGet error →
// writes to mp_check_failed.txt at the root (NOT check_failed), and
// bumps MpCheckFailed (not CheckFailed).
func TestCheckerMultipartSegmentCheckRangeError(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})
	if got := s.Snapshot().MpCheckFailed; got != 1 {
		t.Errorf("mp_check_failed=%d want 1 (segment RangeGet error should bump MpCheckFailed)", got)
	}
	if got := s.Snapshot().CheckFailed; got != 0 {
		t.Errorf("check_failed=%d want 0 (segment errors go to mp_check_failed, not check_failed)", got)
	}
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupted_mp=%d want 0", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// File at root (not per-owner).
	data, err := os.ReadFile(filepath.Join(dir, "mp_check_failed.txt"))
	if err != nil {
		t.Fatalf("read mp_check_failed: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k|2|0|5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt|k|2|0|5242880")
	}
}

// TestCheckerOffsetModeCorruptedCarriesOffsets — offset mode: a corrupt
// multipart lands in corrupted_mp.txt with the etag-derived partcnt+offsets
// appended (bkt|key|partcnt|off0|off1|...), ready to feed -backup-file.
// The boundary probe at [off-128, off+128] straddles part 0's tail and
// part 1's head; the handler returns a chunkSig body for that boundary
// window and clean bytes otherwise.
func TestCheckerOffsetModeCorruptedCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, IsMultipartSuccessLog: true}
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	partBoundary := int64(5242880)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		// Boundary probe centered on partBoundary: start = partBoundary-128.
		if offset <= partBoundary && offset+length > partBoundary {
			return chunkSigBody, nil
		}
		return []byte("clean part body, no signature"), nil
	}}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: 10 * 1024 * 1024, Offsets: []int64{0, 5242880}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1", got)
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "owner-A", "corrupted_mp.txt"))
	if err != nil {
		t.Fatalf("read corrupted_mp: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k|2|0|5242880" {
		t.Errorf("corrupted_mp.txt = %q, want %q", line, "test-bkt|k|2|0|5242880")
	}
}

// TestCheckerOffsetModeFallbackWritesMpAll — offset mode with nil offsets
// (ETag did not parse): the object is unverified and lands in mp.txt.
func TestCheckerOffsetModeFallbackWritesMpAll(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody} // would match — but nothing is probed
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: nil})
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupted_mp=%d want 0 (unverified, not probed)", got)
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0 (unverified must not claim clean)", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "owner-A", "mp.txt"))
	if err != nil {
		t.Fatalf("read mp.txt: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k" {
		t.Errorf("mp.txt = %q, want %q", line, "test-bkt|k")
	}
}

// TestCheckerMultipartSegmentCheckDisabled: switch off → all multipart go to
// <owner>/mp.txt (key only, no etag).
func TestCheckerMultipartSegmentCheckDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff, MultipartSegmentSize: 5 * 1024 * 1024}
	out, _ := NewOutput(cfg, "test-bkt", false)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody} // would match if we checked — but we don't
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: nil})
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupt_mp=%d want 0 (switch off)", got)
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0 (switch off — segment check not performed, no clean claim)", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "owner-A", "mp.txt"))
	if err != nil {
		t.Fatalf("read mp: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt|k" {
		t.Errorf("mp.txt = %q, want %q (bucket|key)", line, "test-bkt|k")
	}
}

// TestCheckerMultipartSegmentCheckSecondSegmentMatches verifies the "ANY
// segment" semantics: when the first segment does NOT match but a later
// one does, the object is still flagged as corrupted multipart. Uses
// RangeGetHandler to return different bodies per offset.
func TestCheckerMultipartSegmentCheckSecondSegmentMatches(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg, "test-bkt", false)
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
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1 (second segment should trigger)", got)
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

// rangeGetCountingS3 wraps FakeS3 and counts RangeGetAt calls. Used to
// assert the small-object fast-path issues exactly 1 probe and the
// large-normal head+tail path issues exactly 2.
type rangeGetCountingS3 struct {
	*FakeS3
	count *int
}

func (c *rangeGetCountingS3) RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	*c.count++
	return c.FakeS3.RangeGetAt(ctx, key, offset, length)
}

// trailerBody is a body that matches trailerRe but NOT chunkSigRe — the
// unsigned-payload-trailer variant. Models the "hello world" example from
// AGENTS.md §1.1: b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:<base64>\r\n\r\n
var trailerBody = []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")

func TestTrailerRegex(t *testing.T) {
	cases := []struct {
		body  string
		match bool
	}{
		{"x-amz-checksum-sha256:abc=", true},
		{"x-amz-checksum-crc32c:abc=", true},
		{"x-amz-checksum-crc64:abc=", true},
		{"x-amz-checksum-sha1:abc=", true},
		{"x-amz-checksum-crc32:abc=", true},
		{"prefix 0\r\nx-amz-checksum-sha256:abc=\r\n\r\n", true}, // mid-body
		{"not-a-checksum:abc", false},
		{"x-amz-checksum-foo:abc", false}, // unknown algo
		{"hello world", false},
	}
	for _, c := range cases {
		got := trailerRe.Match([]byte(c.body))
		if got != c.match {
			t.Errorf("trailerRe.Match(%q)=%v want %v", c.body, got, c.match)
		}
	}
}

func TestCheckerSmallObjectFastPathClean(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsSuccessLog: true, WholeObjectProbeThreshold: 1024}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	count := 0
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: []byte("normal small object content")}, count: &count}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 50, IsMultipart: false, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 0 {
		t.Errorf("corrupted=%d want 0", got)
	}
	if got := s.Snapshot().OkObjects; got != 1 {
		t.Errorf("ok_obj=%d want 1", got)
	}
	if count != 1 {
		t.Errorf("RangeGetAt calls=%d want 1 (small-object fast-path)", count)
	}
}

func TestCheckerSmallObjectFastPathCorruptedChunkSig(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, WholeObjectProbeThreshold: 1024}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	count := 0
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: chunkSigBody}, count: &count}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 50, IsMultipart: false, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1", got)
	}
	if count != 1 {
		t.Errorf("RangeGetAt calls=%d want 1 (small-object fast-path)", count)
	}
}

func TestCheckerSmallObjectFastPathCorruptedTrailer(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, WholeObjectProbeThreshold: 1024}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	count := 0
	// trailerBody has no ;chunk-signature= — previously MISSED by the
	// head-only probe. The fast-path reads the whole body and trailerRe
	// now catches it.
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: trailerBody}, count: &count}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: int64(len(trailerBody)), IsMultipart: false, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1 (trailer marker should be caught)", got)
	}
	if count != 1 {
		t.Errorf("RangeGetAt calls=%d want 1 (small-object fast-path)", count)
	}
}

func TestCheckerLargeNormalObjectHeadTail(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, WholeObjectProbeThreshold: 1024}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	const size int64 = 4096
	// Body of 4096 bytes: clean head, trailer marker in the last 128 bytes
	// (the tail probe window [3968, 4096)). Head probe reads clean bytes
	// (no match) so the tail probe must run and catch the trailer.
	tail := []byte("\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")
	body := make([]byte, size)
	for i := range body {
		body[i] = 'x'
	}
	copy(body[size-int64(len(tail)):], tail)
	count := 0
	// RangeGetHandler slices the body by [offset, offset+length) so the
	// head probe (offset 0, 128 bytes) sees only clean 'x' bytes and the
	// tail probe (offset 3968, 128 bytes) sees the trailer marker.
	worker := &rangeGetCountingS3{
		FakeS3: &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
			end := offset + length
			if end > size {
				end = size
			}
			if offset >= size {
				return nil, nil
			}
			return body[offset:end], nil
		}},
		count: &count,
	}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: size, IsMultipart: false, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1 (tail probe should catch trailer)", got)
	}
	if count != 2 {
		t.Errorf("RangeGetAt calls=%d want 2 (head + tail)", count)
	}
}

func TestCheckerMultipartBoundaryProbeCatchesTrailerInPartTail(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	// 2 parts, boundary at 5MiB. Boundary probe reads [5242752, 5253008].
	// Return a body that contains a trailer marker in that window (prev
	// part tail ending the trailer + clean next part head). The head@0
	// probe and tail@10MiB probe get clean bytes.
	boundary := int64(5242880)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		if offset <= boundary && offset+length > boundary {
			return trailerBody, nil
		}
		return []byte("clean part body, no signature"), nil
	}}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: 10 * 1024 * 1024, Offsets: []int64{0, 5242880}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1 (boundary probe should catch trailer)", got)
	}
}

func TestCheckerMultipartBoundaryProbeCatchesChunkSigInNextPartHead(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()
	// 2 parts, boundary at 5MiB. Boundary probe reads [5242752, 5253008]
	// where byte 128 of the 256-byte slice is the next part's head. The
	// de-anchored chunkSigRe must match at byte 128 — fails without the
	// de-anchor (regression guard).
	boundary := int64(5242880)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		if offset <= boundary && offset+length > boundary {
			return chunkSigBody, nil
		}
		return []byte("clean part body, no signature"), nil
	}}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: 10 * 1024 * 1024, Offsets: []int64{0, 5242880}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1 (de-anchored regex should match next part head)", got)
	}
}

// TestRunProbesThreeStates — runProbes returns probeClean / probeCorrupted
// / probeFailed directly so list-check / -list-file / -backup-file all share
// the same detection. This test pins the three return paths independently of
// any routing.
func TestRunProbesThreeStates(t *testing.T) {
	t.Run("clean", func(t *testing.T) {
		f := &FakeS3{Body: []byte("nothing signature-like in here")}
		task := VerifyTask{Key: "k", Size: 40, IsMultipart: false}
		result, err := runProbes(f, task, 1024)
		if err != nil {
			t.Errorf("err = %v, want nil", err)
		}
		if result != probeClean {
			t.Errorf("result = %v, want probeClean", result)
		}
	})
	t.Run("corrupted by chunkSigRe", func(t *testing.T) {
		f := &FakeS3{Body: []byte(corruptBody)}
		task := VerifyTask{Key: "k", Size: int64(len(corruptBody)), IsMultipart: false}
		result, _ := runProbes(f, task, 1024)
		if result != probeCorrupted {
			t.Errorf("result = %v, want probeCorrupted", result)
		}
	})
	t.Run("corrupted by trailerRe", func(t *testing.T) {
		f := &FakeS3{Body: trailerBody}
		task := VerifyTask{Key: "k", Size: int64(len(trailerBody)), IsMultipart: false}
		result, _ := runProbes(f, task, 1024)
		if result != probeCorrupted {
			t.Errorf("result = %v, want probeCorrupted (unsigned-trailer has no chunk-signature, only trailer marker)", result)
		}
	})
	t.Run("failed on GET error", func(t *testing.T) {
		f := &FakeS3{Err: context.DeadlineExceeded}
		task := VerifyTask{Key: "k", Size: 40, IsMultipart: false}
		result, err := runProbes(f, task, 1024)
		if result != probeFailed {
			t.Errorf("result = %v, want probeFailed", result)
		}
		if err == nil {
			t.Errorf("err = nil, want the RangeGet error")
		}
	})
}

// corruptBody matches chunkSigRe (64-hex signature + CRLF). Defined in
// backup_test.go but referenced here so runProbes tests cover the signed
// variant without duplicating the constant.
var _ = corruptBody

// TestCheckerMultipartProbeCountIsNPlusOne — regression guard: a multipart
// object with N parts gets exactly N+1 probes (head@0 + (N-1) boundary +
// tail@Size-128). A previous bug duplicated the head probe (headTail was
// called at the tail step, returning head+tail and adding head a second
// time), making it N+2. Tests that only checked match outcome missed it
// because both head reads returned identical bytes; this test pins the count.
func TestCheckerMultipartProbeCountIsNPlusOne(t *testing.T) {
	dir := t.TempDir()
	// threshold=0 disables small-object fast-path so we exercise the
	// head+boundary+tail matrix directly.
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "test-bkt", false)
	defer out.Close()
	s := NewStats()

	const size int64 = 10 * 1024 * 1024 // 10 MiB, 2 parts at [0, 5MiB)
	offs := []int64{0, 5 * 1024 * 1024}
	count := 0
	worker := &rangeGetCountingS3{
		FakeS3: &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
			return []byte("clean part body, no signature"), nil
		}},
		count: &count,
	}
	c := NewChecker(worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: size, Offsets: offs})

	// N=2 parts → expect N+1 = 3 probes (head + 1 boundary + tail). The old
	// bug ran 4 (head added twice via headTail at the tail step).
	if count != 3 {
		t.Errorf("RangeGetAt calls = %d, want 3 (head@0 + 1 boundary + tail; old bug duplicated head → 4)", count)
	}
}
