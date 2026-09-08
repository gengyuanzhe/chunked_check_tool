// backup_test.go
package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
)

// corruptBody matches chunkSigRe (64-hex signature + CRLF), like the
// bodies in checker_test.go.
const corruptBody = "1000;chunk-signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\r\n"

func md5Hex(b []byte) string {
	return fmt.Sprintf("%x", md5.Sum(b))
}

// relayBody is a two-part multipart source body: offsets [0,5) and [5,10).
var relayBody = []byte("AAAAABBBBB")

func relayParts() map[int][]byte {
	return map[int][]byte{1: relayBody[0:5], 2: relayBody[5:]}
}

// newBackupTestEnv wires a BackupChecker against a FakeS3 and a
// NewBackupOutput. The returned flush func closes the output (flushing the
// async writer goroutines) exactly once — call it before reading result
// files; t.Cleanup also holds a reference so a forgotten call is harmless.
func newBackupTestEnv(t *testing.T, f *FakeS3) (*BackupChecker, string, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, BackupBucket: "dstbucket"}
	out, err := NewBackupOutput(cfg, "mybucket")
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	flush := func() {
		once.Do(func() {
			if err := out.Close(); err != nil {
				t.Logf("output close: %v", err)
			}
		})
	}
	t.Cleanup(flush)
	stats := NewStats()
	return NewBackupChecker(f, out, stats, cfg), dir, flush
}

func readBackupFile(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// TestBackupCheckerRegularRelaysAndMatchesETag — a regular object is
// downloaded in one piece, streamed to the backup bucket, and the
// returned ETag must equal the HEAD ETag.
func TestBackupCheckerRegularRelaysAndMatchesETag(t *testing.T) {
	content := []byte("hello backup world")
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"k1": {ETag: md5Hex(content), Size: int64(len(content))}},
		RelayBodies: map[string][]byte{"k1": content},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "k1", RawLine: "mybucket|k1"})
	flush()

	if len(f.Puts) != 1 || f.Puts[0].Bucket != "dstbucket" || f.Puts[0].Key != "k1" {
		t.Fatalf("puts = %+v, want one dstbucket/k1", f.Puts)
	}
	if f.Puts[0].Content != string(content) {
		t.Errorf("relayed content = %q, want %q", f.Puts[0].Content, content)
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mybucket|k1\n" {
		t.Errorf("backup_ok.txt = %q, want %q", got, "mybucket|k1\n")
	}
	if len(f.Aborted) != 0 {
		t.Errorf("aborted = %v, want none for regular relay", f.Aborted)
	}
}

// TestBackupCheckerRegularETagMismatch — a faithful relay whose source
// ETag disagrees with the destination ETag flags backup_failed (stage=
// etag); the bad copy stays in the destination bucket as evidence.
func TestBackupCheckerRegularETagMismatch(t *testing.T) {
	content := []byte("hello backup world")
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"k1": {ETag: "00000000000000000000000000000000", Size: int64(len(content))}},
		RelayBodies: map[string][]byte{"k1": content},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "k1", RawLine: "mybucket|k1"})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|k1\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=etag") {
		t.Errorf("backup_failed.log missing stage=etag: %q", logContent)
	}
	// The uploaded copy remains recorded (evidence).
	if len(f.Puts) != 1 {
		t.Errorf("puts = %d, want 1 (bad copy kept)", len(f.Puts))
	}
}

// TestBackupCheckerRegularZeroSize — empty regular objects skip the
// download entirely; the empty-object MD5 is a constant so the ETag check
// still applies.
func TestBackupCheckerRegularZeroSize(t *testing.T) {
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"empty": {ETag: md5Hex(nil), Size: 0}},
		RelayBodies: map[string][]byte{"empty": nil},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "empty", RawLine: "mybucket|empty"})
	flush()

	if len(f.Puts) != 1 || f.Puts[0].Content != "" {
		t.Fatalf("puts = %+v, want one empty upload", f.Puts)
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mybucket|empty\n" {
		t.Errorf("backup_ok.txt = %q", got)
	}
}

// TestBackupCheckerMultipartCorruptRelaysByOffsets — a corrupted multipart
// object is re-uploaded as parts split exactly at the input line's
// offsets, and the resulting multipart ETag must equal the source ETag.
func TestBackupCheckerMultipartCorruptRelaysByOffsets(t *testing.T) {
	srcETag := multipartETag(relayParts())
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"mp1": {ETag: srcETag, Size: int64(len(relayBody))}},
		Body:        []byte(corruptBody), // verify probe hits the signature
		RelayBodies: map[string][]byte{"mp1": relayBody},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if len(f.Completed) != 1 {
		t.Fatalf("completed = %+v, want one multipart upload", f.Completed)
	}
	got := f.Completed[0]
	if got.Bucket != "dstbucket" || got.Key != "mp1" {
		t.Errorf("completed = %+v, want dstbucket/mp1", got)
	}
	if got.ETag != srcETag {
		t.Errorf("completed etag = %q, want src etag %q", got.ETag, srcETag)
	}
	if string(got.Parts[1]) != "AAAAA" || string(got.Parts[2]) != "BBBBB" {
		t.Errorf("parts = %v, want split at offsets [0,5)", got.Parts)
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_ok.txt = %q", got)
	}
	if len(f.Aborted) != 0 {
		t.Errorf("aborted = %v, want none on success", f.Aborted)
	}
}

// TestBackupCheckerMultipartCorruptAtSecondOffset — corruption found at a
// later offset still backs the object up.
func TestBackupCheckerMultipartCorruptAtSecondOffset(t *testing.T) {
	probes := 0
	srcETag := multipartETag(relayParts())
	f := &FakeS3{
		Heads: map[string]HeadInfo{"mp1": {ETag: srcETag, Size: int64(len(relayBody))}},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			probes++
			if offset == 0 {
				return []byte("clean segment, no signature"), nil
			}
			return []byte(corruptBody), nil
		},
		RelayBodies: map[string][]byte{"mp1": relayBody},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if probes != 2 {
		t.Errorf("probes = %d, want 2", probes)
	}
	if len(f.Completed) != 1 {
		t.Fatalf("completed = %+v, want one (corrupt at second offset still backs up)", f.Completed)
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_ok.txt = %q", got)
	}
}

// TestBackupCheckerMultipartCleanSkips — all offsets clean: no relay, key
// goes to backup_skipped_clean.txt.
func TestBackupCheckerMultipartCleanSkips(t *testing.T) {
	probes := 0
	f := &FakeS3{
		Heads: map[string]HeadInfo{"mp1": {ETag: "0123456789abcdef0123456789abcdef-2", Size: int64(len(relayBody))}},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			probes++
			return []byte("no signature in this body"), nil
		},
		RelayBodies: map[string][]byte{"mp1": relayBody},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if len(f.Completed) != 0 {
		t.Fatalf("completed = %+v, want 0 (clean multipart is not backed up)", f.Completed)
	}
	if len(f.Puts) != 0 {
		t.Errorf("puts = %+v, want none", f.Puts)
	}
	if got := readBackupFile(t, dir, "backup_skipped_clean.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_skipped_clean.txt = %q", got)
	}
	if got := c.stats.Snapshot().BackupSkippedClean; got != 1 {
		t.Errorf("BackupSkippedClean = %d, want 1", got)
	}
	if probes != 2 {
		t.Errorf("probes = %d, want 2 (all offsets probed)", probes)
	}
}

// TestBackupCheckerMultipartETagMismatch — the relay succeeded but the
// reassembled ETag differs from the source (e.g. offsets were not the
// original part boundaries) → backup_failed stage=etag, copy kept.
func TestBackupCheckerMultipartETagMismatch(t *testing.T) {
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"mp1": {ETag: "0123456789abcdef0123456789abcdef-2", Size: int64(len(relayBody))}},
		Body:        []byte(corruptBody),
		RelayBodies: map[string][]byte{"mp1": relayBody},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=etag") {
		t.Errorf("backup_failed.log missing stage=etag: %q", logContent)
	}
	if len(f.Completed) != 1 {
		t.Errorf("completed = %d, want 1 (copy kept as evidence)", len(f.Completed))
	}
}

// TestBackupCheckerMismatchLogsHeadETag — both mismatch directions land in
// mismatch.txt (raw line) AND mismatch.log, which must carry the line's
// declared type, the HEAD ETag/size, and an actionable reason. The log is
// what makes an all-mismatch run diagnosable: without it the raw line
// alone cannot tell whether the line shape or the server's ETag is at
// fault.
func TestBackupCheckerMismatchLogsHeadETag(t *testing.T) {
	f := &FakeS3{Heads: map[string]HeadInfo{
		// k1: line says regular, HEAD says multipart (the corrupted_mp.txt
		// direct-feed case).
		"k1": {ETag: "0123456789abcdef0123456789abcdef-2", Size: 10},
		// k2: line says multipart, HEAD says regular.
		"k2": {ETag: "0123456789abcdef0123456789abcdef", Size: 10},
	}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "k1", RawLine: "mybucket|k1"})
	c.Handle(BackupTask{Key: "k2", RawLine: "mybucket|k2|1|0", IsMultipart: true, Offsets: []int64{0}})
	flush()

	if got := readBackupFile(t, dir, "mismatch.txt"); got != "mybucket|k1\nmybucket|k2|1|0\n" {
		t.Errorf("mismatch.txt = %q, want both raw lines", got)
	}
	logContent := readBackupFile(t, dir, "mismatch.log")
	for _, want := range []string{
		"key=k1",
		"line_is_multipart=false",
		"head_etag=0123456789abcdef0123456789abcdef-2",
		"head_size=10",
		"line is regular (bkt|key) but HEAD ETag is multipart-style",
		"key=k2",
		"line_is_multipart=true",
		"head_etag=0123456789abcdef0123456789abcdef",
		"line declares multipart (partcnt present) but HEAD ETag is a plain MD5",
	} {
		if !strings.Contains(logContent, want) {
			t.Errorf("mismatch.log missing %q:\n%s", want, logContent)
		}
	}
	if got := c.stats.Snapshot().BackupMismatch; got != 2 {
		t.Errorf("BackupMismatch = %d, want 2", got)
	}
	if len(f.Puts) != 0 {
		t.Errorf("puts = %+v, want none (mismatch never relays)", f.Puts)
	}
}

func TestBackupCheckerHeadErrorFails(t *testing.T) {
	f := &FakeS3{HeadErr: minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "gone", RawLine: "mybucket|gone"})
	flush()

	if len(f.Puts) != 0 && len(f.Completed) != 0 {
		t.Fatalf("no upload expected")
	}
	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|gone\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	if got := c.stats.Snapshot().BackupFailed; got != 1 {
		t.Errorf("BackupFailed = %d, want 1", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=head") {
		t.Errorf("backup_failed.log missing stage=head: %q", logContent)
	}
}

func TestBackupCheckerVerifyErrorFails(t *testing.T) {
	f := &FakeS3{
		Heads: map[string]HeadInfo{"mp1": {ETag: "0123456789abcdef0123456789abcdef-1", Size: 10}},
		Err:   errors.New("read tcp i/o timeout"),
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|1|0", IsMultipart: true, Offsets: []int64{0}})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|mp1|1|0\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=verify") {
		t.Errorf("backup_failed.log missing stage=verify: %q", logContent)
	}
}

// TestBackupCheckerUploadPartErrorAborts — a part-upload failure aborts
// the multipart upload and records backup_failed stage=upload.
func TestBackupCheckerUploadPartErrorAborts(t *testing.T) {
	f := &FakeS3{
		Heads:     map[string]HeadInfo{"mp1": {ETag: "0123456789abcdef0123456789abcdef-2", Size: int64(len(relayBody))}},
		Body:      []byte(corruptBody),
		UploadErr: errors.New("connection reset by peer"),
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=upload") {
		t.Errorf("backup_failed.log missing stage=upload: %q", logContent)
	}
	if len(f.Aborted) != 1 {
		t.Errorf("aborted = %v, want the in-progress upload aborted", f.Aborted)
	}
	if len(f.Completed) != 0 {
		t.Errorf("completed = %d, want 0", len(f.Completed))
	}
}

// TestBackupCheckerDownloadErrorAborts — a part download failure aborts too.
func TestBackupCheckerDownloadErrorAborts(t *testing.T) {
	f := &FakeS3{
		Heads:       map[string]HeadInfo{"mp1": {ETag: "0123456789abcdef0123456789abcdef-2", Size: int64(len(relayBody))}},
		Body:        []byte(corruptBody),
		DownloadErr: errors.New("dial tcp: connection refused"),
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mybucket|mp1|2|0|5\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	if len(f.Aborted) != 1 {
		t.Errorf("aborted = %v, want the in-progress upload aborted", f.Aborted)
	}
}

func TestBackupCheckerMismatchBothDirections(t *testing.T) {
	f := &FakeS3{Heads: map[string]HeadInfo{
		"reg-line-mp-head": {ETag: "0123456789abcdef0123456789abcdef-2", Size: 10}, // line says regular, HEAD says multipart
		"mp-line-reg-head": {ETag: "0123456789abcdef0123456789abcdef", Size: 10},   // line says multipart, HEAD says regular
	}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "reg-line-mp-head", RawLine: "mybucket|reg-line-mp-head"})
	c.Handle(BackupTask{Key: "mp-line-reg-head", RawLine: "mybucket|mp-line-reg-head|1|0", IsMultipart: true, Offsets: []int64{0}})
	flush()

	if len(f.Puts) != 0 || len(f.Completed) != 0 {
		t.Fatalf("no upload expected on mismatch")
	}
	want := "mybucket|reg-line-mp-head\nmybucket|mp-line-reg-head|1|0\n"
	if got := readBackupFile(t, dir, "mismatch.txt"); got != want {
		t.Errorf("mismatch.txt = %q, want %q", got, want)
	}
	if got := c.stats.Snapshot().BackupMismatch; got != 2 {
		t.Errorf("BackupMismatch = %d, want 2", got)
	}
}

// TestBackupCheckerStatsSnapshot — counters through one of each outcome.
func TestBackupCheckerStatsSnapshot(t *testing.T) {
	regContent := []byte("regular body")
	f := &FakeS3{
		Heads: map[string]HeadInfo{
			"ok-reg": {ETag: md5Hex(regContent), Size: int64(len(regContent))},
			"ok-mp":  {ETag: multipartETag(relayParts()), Size: int64(len(relayBody))},
			"mm":     {ETag: "0123456789abcdef0123456789abcdef-2", Size: 10},
		},
		Body:        []byte(corruptBody),
		RelayBodies: map[string][]byte{"ok-reg": regContent, "ok-mp": relayBody},
	}
	c, _, _ := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "ok-reg", RawLine: "mybucket|ok-reg"})
	c.Handle(BackupTask{Key: "ok-mp", RawLine: "mybucket|ok-mp|2|0|5", IsMultipart: true, Offsets: []int64{0, 5}})
	c.Handle(BackupTask{Key: "mm", RawLine: "mybucket|mm"})

	snap := c.stats.Snapshot()
	if snap.BackupOk != 2 || snap.BackupMismatch != 1 || snap.BackupFailed != 0 || snap.BackupSkippedClean != 0 {
		t.Errorf("backup stats = ok:%d failed:%d mismatch:%d clean:%d, want 2/0/1/0",
			snap.BackupOk, snap.BackupFailed, snap.BackupMismatch, snap.BackupSkippedClean)
	}
}
