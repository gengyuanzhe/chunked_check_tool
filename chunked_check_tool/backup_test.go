// backup_test.go
package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
)

const normalETag = "0123456789abcdef0123456789abcdef"
const mpETag = "0123456789abcdef0123456789abcdef-3"

// corruptBody matches chunkSigRe (64-hex signature + CRLF), like the
// bodies in checker_test.go.
const corruptBody = "1000;chunk-signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\r\n"

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

func TestBackupCheckerRegularBacksUpDirectly(t *testing.T) {
	f := &FakeS3{Heads: map[string]string{"k1": normalETag}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "k1", RawLine: "mybucket|k1", IsMultipart: false})
	flush()

	if len(f.Copies) != 1 {
		t.Fatalf("copies = %d, want 1", len(f.Copies))
	}
	if f.Copies[0] != (CopyCall{SrcKey: "k1", DstBucket: "dstbucket", DstKey: "k1"}) {
		t.Errorf("copy = %+v, want k1→dstbucket/k1", f.Copies[0])
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "k1\n" {
		t.Errorf("backup_ok.txt = %q, want %q", got, "k1\n")
	}
}

func TestBackupCheckerMultipartCorruptBacksUp(t *testing.T) {
	probes := 0
	f := &FakeS3{
		Heads: map[string]string{"mp1": mpETag},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			probes++
			return []byte(corruptBody), nil
		},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5242880", IsMultipart: true, Offsets: []int64{0, 5242880}})
	flush()

	if len(f.Copies) != 1 {
		t.Fatalf("copies = %d, want 1", len(f.Copies))
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mp1\n" {
		t.Errorf("backup_ok.txt = %q", got)
	}
	if probes != 1 {
		t.Errorf("probes = %d, want 1 (stop at first corrupt offset)", probes)
	}
}

func TestBackupCheckerMultipartCorruptAtSecondOffset(t *testing.T) {
	probes := 0
	f := &FakeS3{
		Heads: map[string]string{"mp1": mpETag},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			probes++
			if offset == 0 {
				return []byte("clean data here"), nil
			}
			return []byte(corruptBody), nil
		},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5242880", IsMultipart: true, Offsets: []int64{0, 5242880}})
	flush()

	if len(f.Copies) != 1 {
		t.Fatalf("copies = %d, want 1 (corrupt at second offset still backs up)", len(f.Copies))
	}
	if probes != 2 {
		t.Errorf("probes = %d, want 2", probes)
	}
	if got := readBackupFile(t, dir, "backup_ok.txt"); got != "mp1\n" {
		t.Errorf("backup_ok.txt = %q", got)
	}
}

func TestBackupCheckerMultipartCleanSkips(t *testing.T) {
	probes := 0
	f := &FakeS3{
		Heads: map[string]string{"mp1": mpETag},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			probes++
			return []byte("no signature in this body"), nil
		},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|2|0|5242880", IsMultipart: true, Offsets: []int64{0, 5242880}})
	flush()

	if len(f.Copies) != 0 {
		t.Fatalf("copies = %d, want 0 (clean multipart is not backed up)", len(f.Copies))
	}
	if got := readBackupFile(t, dir, "backup_skipped_clean.txt"); got != "mp1\n" {
		t.Errorf("backup_skipped_clean.txt = %q", got)
	}
	if got := c.stats.Snapshot().BackupSkippedClean; got != 1 {
		t.Errorf("BackupSkippedClean = %d, want 1", got)
	}
	if probes != 2 {
		t.Errorf("probes = %d, want 2 (all offsets probed)", probes)
	}
}

func TestBackupCheckerHeadErrorFails(t *testing.T) {
	f := &FakeS3{HeadErr: minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "gone", RawLine: "mybucket|gone", IsMultipart: false})
	flush()

	if len(f.Copies) != 0 {
		t.Fatalf("copies = %d, want 0", len(f.Copies))
	}
	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "gone\n" {
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
		Heads: map[string]string{"mp1": mpETag},
		Err:   errors.New("read tcp i/o timeout"),
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "mp1", RawLine: "mybucket|mp1|1|0", IsMultipart: true, Offsets: []int64{0}})
	flush()

	if len(f.Copies) != 0 {
		t.Fatalf("copies = %d, want 0", len(f.Copies))
	}
	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "mp1\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=verify") {
		t.Errorf("backup_failed.log missing stage=verify: %q", logContent)
	}
}

func TestBackupCheckerCopyErrorFails(t *testing.T) {
	f := &FakeS3{
		Heads:   map[string]string{"k1": normalETag},
		CopyErr: minio.ErrorResponse{Code: "NoSuchBucket", StatusCode: 404},
	}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "k1", RawLine: "mybucket|k1", IsMultipart: false})
	flush()

	if got := readBackupFile(t, dir, "backup_failed.txt"); got != "k1\n" {
		t.Errorf("backup_failed.txt = %q", got)
	}
	logContent := readBackupFile(t, dir, "backup_failed.log")
	if !strings.Contains(logContent, "stage=copy") {
		t.Errorf("backup_failed.log missing stage=copy: %q", logContent)
	}
}

func TestBackupCheckerMismatchBothDirections(t *testing.T) {
	f := &FakeS3{Heads: map[string]string{
		"reg-line-mp-head":  mpETag,     // line says regular, HEAD says multipart
		"mp-line-reg-head":  normalETag, // line says multipart, HEAD says regular
	}}
	c, dir, flush := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "reg-line-mp-head", RawLine: "mybucket|reg-line-mp-head", IsMultipart: false})
	c.Handle(BackupTask{Key: "mp-line-reg-head", RawLine: "mybucket|mp-line-reg-head|1|0", IsMultipart: true, Offsets: []int64{0}})
	flush()

	if len(f.Copies) != 0 {
		t.Fatalf("copies = %d, want 0 (mismatch never backs up)", len(f.Copies))
	}
	want := "mybucket|reg-line-mp-head\nmybucket|mp-line-reg-head|1|0\n"
	if got := readBackupFile(t, dir, "mismatch.txt"); got != want {
		t.Errorf("mismatch.txt = %q, want %q", got, want)
	}
	if got := c.stats.Snapshot().BackupMismatch; got != 2 {
		t.Errorf("BackupMismatch = %d, want 2", got)
	}
}

// TestBackupCheckerRegularZeroSize — a regular object with unknown size is
// still backed up directly (the input list is authoritative: it came from a
// prior corrupted_objects.txt). Unlike Checker.Handle there is no size==0
// shortcut.
func TestBackupCheckerRegularZeroSize(t *testing.T) {
	f := &FakeS3{Heads: map[string]string{"empty": normalETag}}
	c, _, _ := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "empty", RawLine: "mybucket|empty", IsMultipart: false})

	if len(f.Copies) != 1 {
		t.Fatalf("copies = %d, want 1 (zero-size regular still backs up)", len(f.Copies))
	}
}

// TestBackupCheckerStatsSnapshot — counters through one of each outcome.
func TestBackupCheckerStatsSnapshot(t *testing.T) {
	f := &FakeS3{
		Heads: map[string]string{
			"ok-reg": normalETag,
			"ok-mp":  mpETag,
			"mm":     mpETag,
		},
		RangeGetHandler: func(offset, length int64) ([]byte, error) {
			return []byte(corruptBody), nil
		},
	}
	c, _, _ := newBackupTestEnv(t, f)

	c.Handle(BackupTask{Key: "ok-reg", RawLine: "mybucket|ok-reg"})
	c.Handle(BackupTask{Key: "ok-mp", RawLine: "mybucket|ok-mp|1|0", IsMultipart: true, Offsets: []int64{0}})
	c.Handle(BackupTask{Key: "mm", RawLine: "mybucket|mm"})

	snap := c.stats.Snapshot()
	if snap.BackupOk != 2 || snap.BackupMismatch != 1 || snap.BackupFailed != 0 || snap.BackupSkippedClean != 0 {
		t.Errorf("backup stats = ok:%d failed:%d mismatch:%d clean:%d, want 2/0/1/0",
			snap.BackupOk, snap.BackupFailed, snap.BackupMismatch, snap.BackupSkippedClean)
	}
}
