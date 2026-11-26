package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
		{"ff;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n", true},
		{"hello world", false},
		{"1000;chunk-signature=short\r\n", false}, // signature not 64
		{";chunk-signature=abcdef\r\n", false},    // empty chunk size
		{"1000;notchunk-signature=abc\r\n", false},
		{"ff;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\n", false}, // bare \n not CRLF — protocol mandates CRLF
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object content here")}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "", ETag: "0123456789abcdef0123456789abcdef", Size: 1, IsMultipart: false, Offsets: nil})
	// success not enabled, no files written yet (deferred to Close)
}

func TestCheckerHandleCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	body := []byte("1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n")
	worker := &FakeS3{Body: body}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 1, IsMultipart: false, Offsets: nil})
	if s.Snapshot().CorruptedObjects != 1 {
		t.Errorf("corrupted=%d want 1", s.Snapshot().CorruptedObjects)
	}
}

func TestCheckerHandleMultipartSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{
		Body: nil,
		Err:  nil,
	}
	// 用一个 wrap 检测是否调用 RangeGet
	c := NewChecker(context.Background(), &callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: 1, IsMultipart: false, Offsets: nil})
	if s.Snapshot().CheckFailed != 1 {
		t.Errorf("checkfailed=%d want 1", s.Snapshot().CheckFailed)
	}
}

func TestCheckerHandleEmptyObjectSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{Err: errFake416}
	c := NewChecker(context.Background(), &callTrackingS3{FakeS3: worker, called: &called}, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Err: fakeS3Err(416, "InvalidRange", "The requested range is not satisfiable", "REQ-1234-ABCD")}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	if line := strings.TrimSpace(string(cfBytes)); line != "test-bkt\x1fpath%2Fobj" {
		t.Errorf("check_failed.txt = %q, want %q", line, "test-bkt\x1fpath%2Fobj")
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
		OutputDir:             dir,
		IsCheck:               true,
		MultipartCheckMode:    MultipartCheckModeSegment,
		IsSuccessLog:          true,
		IsMultipartSuccessLog: true,
		MultipartSegmentSize:  segSize,
	}
}

// TestCheckerMultipartSegmentCheckCorrupted: switch on, segment matches →
// corrupted multipart file goes to <owner>/corrupted_mp.txt
// (per-owner routing).
func TestCheckerMultipartSegmentCheckCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := multipartCfg(dir, 5*1024*1024)
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object body, no chunk signature here")}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fk\x1f2\x1f0\x1f5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt\x1fk\x1f2\x1f0\x1f5242880")
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	partBoundary := int64(5242880)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		// Boundary probe centered on partBoundary: start = partBoundary-128.
		if offset <= partBoundary && offset+length > partBoundary {
			return chunkSigBody, nil
		}
		return []byte("clean part body, no signature"), nil
	}}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fk\x1f2\x1f0\x1f5242880" {
		t.Errorf("corrupted_mp.txt = %q, want %q", line, "test-bkt\x1fk\x1f2\x1f0\x1f5242880")
	}
}

// TestCheckerOffsetModeFallbackWritesListParseFailed — offset mode with nil
// offsets (ETag did not parse): the object lands in list_parse_failed.txt
// (NOT mp.txt), with a structured log entry in list_parse_failed.log.
func TestCheckerOffsetModeFallbackWritesListParseFailed(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody} // would match — but nothing is probed
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", ETag: "abc-def-ghi", IsMultipart: true, Offsets: nil})
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupted_mp=%d want 0 (unverified, not probed)", got)
	}
	if got := s.Snapshot().OkMp; got != 0 {
		t.Errorf("ok_mp=%d want 0 (unverified must not claim clean)", got)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// Object must land in root-level list_parse_failed.txt, NOT mp.txt.
	data, err := os.ReadFile(filepath.Join(dir, "list_parse_failed.txt"))
	if err != nil {
		t.Fatalf("read list_parse_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fk" {
		t.Errorf("list_parse_failed.txt = %q, want %q", line, "test-bkt\x1fk")
	}
	if _, err := os.Stat(filepath.Join(dir, "owner-A", "mp.txt")); !os.IsNotExist(err) {
		t.Errorf("mp.txt should not exist in mode=offset; stat err=%v", err)
	}
	// list_parse_failed.log must record the unparseable ETag for diagnosis.
	logData, err := os.ReadFile(filepath.Join(dir, "list_parse_failed.log"))
	if err != nil {
		t.Fatalf("read list_parse_failed.log: %v", err)
	}
	logLine := string(logData)
	if !strings.Contains(logLine, "multipart etag parse failed") {
		t.Errorf("list_parse_failed.log missing message: %q", logLine)
	}
	if !strings.Contains(logLine, `key=k`) {
		t.Errorf("list_parse_failed.log missing key: %q", logLine)
	}
	if !strings.Contains(logLine, `etag=abc-def-ghi`) {
		t.Errorf("list_parse_failed.log missing etag: %q", logLine)
	}
}

// TestCheckerMultipartSegmentCheckDisabled: switch off → all multipart go to
// <owner>/mp.txt (key only, no etag).
func TestCheckerMultipartSegmentCheckDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff, MultipartSegmentSize: 5 * 1024 * 1024}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	s := NewStats()
	worker := &FakeS3{Body: chunkSigBody} // would match if we checked — but we don't
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
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
	c := NewChecker(context.Background(), worker, out, s, cfg)
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

// trailerBody matches trailerRe but NOT chunkSigRe (unsigned-payload-trailer
// variant). Only single-PUT trailer uploads land on private storage.
var trailerBody = []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")

func TestTrailerRegex(t *testing.T) {
	cases := []struct {
		body  string
		match bool
	}{
		// Full unsigned-trailer end-structure (the exact wire form on disk).
		{"\r\n0\r\nx-amz-checksum-sha256:abc=\r\n\r\n", true},  // non-empty payload
		{"0\r\nx-amz-checksum-sha256:abc=\r\n\r\n", true},     // empty payload: NO leading \r\n (payloadSize==0 → only finalChunkBytes) — must still match
		{"\r\n0\r\nx-amz-checksum-crc32c:abc=\r\n\r\n", true},
		{"\r\n0\r\nx-amz-checksum-crc64:abc=\r\n\r\n", true},
		{"\r\n0\r\nx-amz-checksum-sha1:abc=\r\n\r\n", true},
		{"\r\n0\r\nx-amz-checksum-crc32:abc=\r\n\r\n", true},
		{"\r\n0\r\nx-amz-checksum-crc64nvme:abc=\r\n\r\n", true}, // future AWS algo — widened set must not miss it
		{"\r\n0\r\nx-amz-checksum-foo:abc=\r\n\r\n", true},       // algo [a-z0-9]+: framing is the anchor, not the algo list
		{"data here\r\n0\r\nx-amz-checksum-sha256:abc=\r\n\r\n", true}, // mid-body with preceding data
		{"\r\n0\r\nx-amz-checksum-sha256:\r\n\r\n", true},              // empty value still matches (framing is the signal)
		// Signed -TRAILER variant: base64 is followed by \r\nx-amz-trailer-signature,
		// not \r\n\r\n — this pattern must NOT match it (chunkSigRe catches it instead).
		{"\r\n0\r\nx-amz-checksum-sha256:abc=\r\nx-amz-trailer-signature:deadbeef\r\n\r\n", false},
		// Missing the full framing — must not match.
		{"x-amz-checksum-sha256:abc=", false},  // no 0\r\n prefix, no \r\n\r\n suffix
		{"\r\n0\r\nx-amz-checksum-sha256:abc=", false}, // no trailing \r\n\r\n
		{"not-a-checksum:abc", false},
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	count := 0
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: []byte("normal small object content")}, count: &count}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	count := 0
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: chunkSigBody}, count: &count}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	count := 0
	// trailerBody has no ;chunk-signature= — previously MISSED by the
	// head-only probe. The fast-path reads the whole body and trailerRe
	// now catches it.
	worker := &rangeGetCountingS3{FakeS3: &FakeS3{Body: trailerBody}, count: &count}
	c := NewChecker(context.Background(), worker, out, s, cfg)
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
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
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
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", Size: size, IsMultipart: false, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1 (tail probe should catch trailer)", got)
	}
	if count != 2 {
		t.Errorf("RangeGetAt calls=%d want 2 (head + tail)", count)
	}
}

func TestCheckerMultipartProbeCatchesChunkSigInPartHead(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	// 2 parts, part 1 starts at 5MiB. Its probe reads [5242880, 5243008)
	// where the part head sits at byte 0 of the slice. The de-anchored
	// chunkSigRe must match there (regression guard against re-anchoring).
	boundary := int64(5242880)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		if offset == boundary {
			return chunkSigBody, nil
		}
		return []byte("clean part body, no signature"), nil
	}}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: 10 * 1024 * 1024, Offsets: []int64{0, 5242880}})
	if got := s.Snapshot().CorruptedMp; got != 1 {
		t.Errorf("corrupted_mp=%d want 1 (chunkSigRe should match part head)", got)
	}
}

// TestCheckerMultipartDoesNotRunTrailerRe — multipart probes run chunkSigRe
// only, so a trailer marker in a multipart probe window reads clean.
func TestCheckerMultipartDoesNotRunTrailerRe(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	const size int64 = 4096
	tail := []byte("\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")
	body := make([]byte, size)
	for i := range body {
		body[i] = 'x'
	}
	copy(body[size-int64(len(tail)):], tail)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		end := offset + length
		if end > size {
			end = size
		}
		if offset >= size {
			return nil, nil
		}
		return body[offset:end], nil
	}}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: size, Offsets: []int64{0, size / 2}})
	if got := s.Snapshot().CorruptedMp; got != 0 {
		t.Errorf("corrupted_mp=%d want 0 (multipart does not run trailerRe)", got)
	}
}

// TestCheckerSinglePutRunsTrailerRe — single-PUT probes run trailerRe, so a
// trailer marker in the tail is flagged. Positive counterpart to the above.
func TestCheckerSinglePutRunsTrailerRe(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()
	const size int64 = 4096
	tail := []byte("\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")
	body := make([]byte, size)
	for i := range body {
		body[i] = 'x'
	}
	copy(body[size-int64(len(tail)):], tail)
	worker := &FakeS3{RangeGetHandler: func(offset, length int64) ([]byte, error) {
		end := offset + length
		if end > size {
			end = size
		}
		if offset >= size {
			return nil, nil
		}
		return body[offset:end], nil
	}}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", IsMultipart: false, Size: size, Offsets: nil})
	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1 (single-PUT probes must run trailerRe)", got)
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
		result, err := runProbes(context.Background(), f, task, 1024)
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
		result, _ := runProbes(context.Background(), f, task, 1024)
		if result != probeCorrupted {
			t.Errorf("result = %v, want probeCorrupted", result)
		}
	})
	t.Run("corrupted by trailerRe", func(t *testing.T) {
		f := &FakeS3{Body: trailerBody}
		task := VerifyTask{Key: "k", Size: int64(len(trailerBody)), IsMultipart: false}
		result, _ := runProbes(context.Background(), f, task, 1024)
		if result != probeCorrupted {
			t.Errorf("result = %v, want probeCorrupted (unsigned-trailer has no chunk-signature, only trailer marker)", result)
		}
	})
	t.Run("failed on GET error", func(t *testing.T) {
		f := &FakeS3{Err: context.DeadlineExceeded}
		task := VerifyTask{Key: "k", Size: 40, IsMultipart: false}
		result, err := runProbes(context.Background(), f, task, 1024)
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

// TestCheckerMultipartProbeCountIsN — N parts get exactly N probes
// (head@0 + (N-1) boundary; no tail). Pins the count against a past bug that
// duplicated the head probe (N+2) and against the now-removed tail probe (N+1).
func TestCheckerMultipartProbeCountIsN(t *testing.T) {
	dir := t.TempDir()
	// threshold=0 disables small-object fast-path so we exercise the
	// head+boundary matrix directly.
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
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
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Size: size, Offsets: offs})

	// N=2 parts → expect N = 2 probes (head@0 + 1 boundary, no tail).
	if count != 2 {
		t.Errorf("RangeGetAt calls = %d, want 2", count)
	}
}

// TestCheckerHeadFirstFillsSizeEnablesTailProbe — HeadFirst fills Size via
// HEAD so the tail probe is built (Size==0 would skip it). Single-PUT only;
// multipart has no tail probe.
func TestCheckerHeadFirstFillsSizeEnablesTailProbe(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	s := NewStats()

	const size int64 = 4096
	tail := []byte("\r\n0\r\nx-amz-checksum-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\r\n\r\n")
	body := make([]byte, size)
	for i := range body {
		body[i] = 'x'
	}
	copy(body[size-int64(len(tail)):], tail)

	worker := &FakeS3{
		Heads: map[string]HeadInfo{"k": {ETag: "0123456789abcdef0123456789abcdef", Size: size}},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			end := offset + length
			if end > size {
				end = size
			}
			if offset >= size {
				return nil, nil
			}
			return body[offset:end], nil
		},
	}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", IsMultipart: false, HeadFirst: true})

	if got := s.Snapshot().CorruptedObjects; got != 1 {
		t.Errorf("corrupted=%d want 1", got)
	}
}

// TestCheckerHeadFirstHeadFailureRoutesToMpCheckFailed — when HEAD fails for
// a multipart HeadFirst task, the task routes to mp_check_failed.txt +
// mp_check_failed.log with the line's offsets preserved (so the file is
// feedable back into -check-file for retry), mirroring backup mode's
// backup_failed stage=head.
func TestCheckerHeadFirstHeadFailureRoutesToMpCheckFailed(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "mybucket", FileInputCheckFile) // file input → mp_check_failed enabled
	s := NewStats()

	worker := &FakeS3{HeadErr: fakeS3Err(404, "NoSuchKey", "", "")}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", IsMultipart: true, Offsets: []int64{0, 5242880}, HeadFirst: true})

	if got := s.Snapshot().MpCheckFailed; got != 1 {
		t.Errorf("MpCheckFailed=%d want 1", got)
	}
	// Close flushes the mp_check_failed writer goroutine before we read.
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "mp_check_failed.txt"))
	if err != nil {
		t.Fatalf("read mp_check_failed.txt: %v", err)
	}
	// Line shape = bkt|key|partcnt|off0|off1, feedable back into -check-file.
	wantLine := "mybucket\x1fk\x1f2\x1f0\x1f5242880"
	if !strings.Contains(string(data), wantLine) {
		t.Errorf("mp_check_failed.txt = %q, want substring %q", string(data), wantLine)
	}
	logData, err := os.ReadFile(filepath.Join(dir, "mp_check_failed.log"))
	if err != nil {
		t.Fatalf("read mp_check_failed.log: %v", err)
	}
	if !strings.Contains(string(logData), "key=k") {
		t.Errorf("mp_check_failed.log missing key=k: %q", string(logData))
	}
}

// TestCheckerHeadFirstHeadFailureRegularRoutesToCheckFailed — the same HEAD
// failure for a regular -check-file line (bkt|key, no offsets) routes to
// check_failed.txt in the bkt|key shape, not mp_check_failed.
func TestCheckerHeadFirstHeadFailureRegularRoutesToCheckFailed(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	out, _ := NewOutput(cfg, "mybucket", FileInputCheckFile)
	s := NewStats()

	worker := &FakeS3{HeadErr: fakeS3Err(404, "NoSuchKey", "", "")}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", HeadFirst: true})

	if got := s.Snapshot().CheckFailed; got != 1 {
		t.Errorf("CheckFailed=%d want 1", got)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatalf("read check_failed.txt: %v", err)
	}
	if got, want := string(data), "mybucket\x1fk\n"; got != want {
		t.Errorf("check_failed.txt = %q, want %q", got, want)
	}
}

// TestCheckerHeadFirstDriftToRegularProbesHeadTail — type drift: the input
// line declares multipart with offsets, but the HEAD ETag says the object
// was overwritten as a regular single-PUT object. The HEAD is authoritative:
// the stale offsets are dropped and the object is probed head+tail as
// regular, so corruption lands in corrupted_objects.txt (not corrupted_mp).
func TestCheckerHeadFirstDriftToRegularProbesHeadTail(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, WholeObjectProbeThreshold: 0}
	out, _ := NewOutput(cfg, "mybucket", FileInputCheckFile)
	s := NewStats()

	corrupt := []byte(corruptBody + "rest of a now-regular object")
	worker := &FakeS3{
		Heads: map[string]HeadInfo{"k": {ETag: md5Hex(corrupt), Size: int64(len(corrupt))}},
		Body:  corrupt,
	}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", IsMultipart: true, Offsets: []int64{0, 5}, HeadFirst: true})

	snap := s.Snapshot()
	if snap.CorruptedObjects != 1 || snap.CorruptedMp != 0 {
		t.Errorf("corrupt_obj=%d corrupt_mp=%d, want 1/0 (drifted object probes as regular)", snap.CorruptedObjects, snap.CorruptedMp)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "_unknown", "corrupted_objects.txt"))
	if err != nil {
		t.Fatalf("read corrupted_objects.txt: %v", err)
	}
	if got, want := string(data), "mybucket|k\n"; got != want {
		t.Errorf("corrupted_objects.txt = %q, want %q", got, want)
	}
}

// TestCheckerHeadFirstDriftToMultipartWritesMpTxt — the reverse drift: a
// regular line (bkt|key, no offsets) whose HEAD reveals a multipart object.
// No offsets are available, so the object lands unverified in mp.txt —
// never ok_mp ("unverified must not be claimed clean"). This is the path
// that keeps mp.txt open in -check-file mode even with
// multipart_check_mode=0.
func TestCheckerHeadFirstDriftToMultipartWritesMpTxt(t *testing.T) {
	dir := t.TempDir()
	// mode=0: without FileInputCheckFile the mp.txt writer would be disabled
	// and the drifted object would vanish silently.
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff}
	out, _ := NewOutput(cfg, "mybucket", FileInputCheckFile)
	s := NewStats()

	worker := &FakeS3{
		Heads: map[string]HeadInfo{"k": {ETag: "0123456789abcdef0123456789abcdef-2", Size: 10}},
		Body:  []byte("clean body"),
	}
	c := NewChecker(context.Background(), worker, out, s, cfg)
	c.Handle(VerifyTask{Key: "k", HeadFirst: true})

	snap := s.Snapshot()
	if snap.OkMp != 0 {
		t.Errorf("ok_mp=%d, want 0 (no offsets — must not claim clean)", snap.OkMp)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "_unknown", "mp.txt"))
	if err != nil {
		t.Fatalf("read mp.txt: %v", err)
	}
	if got, want := string(data), "mybucket|k\n"; got != want {
		t.Errorf("mp.txt = %q, want %q", got, want)
	}
}
