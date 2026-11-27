package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// ownerSub returns the path to a per-owner file: <dir>/<ownerID>/<name>.
func ownerSub(dir, ownerID, name string) string {
	return filepath.Join(dir, ownerID, name)
}

// TestOutputPerOwnerRouting verifies that writes for two different owners
// land in their respective per-owner subfolders, not at the root.
func TestOutputPerOwnerRouting(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsSuccessLog: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorrupted("owner-A", "obj/a1")
	o.WriteCorrupted("owner-B", "obj/b1")
	o.WriteSuccess("owner-A", "obj/a-ok")
	o.WriteMultipartAll("owner-B", "obj/b-mp")
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		path string
		want string
	}{
		{ownerSub(dir, "owner-A", "corrupted_objects.txt"), "test-bkt\x1fobj%2Fa1"},
		{ownerSub(dir, "owner-B", "corrupted_objects.txt"), "test-bkt\x1fobj%2Fb1"},
		{ownerSub(dir, "owner-A", "ok_objects.txt"), "test-bkt\x1fobj%2Fa-ok"},
		{ownerSub(dir, "owner-B", "mp.txt"), "test-bkt\x1fobj%2Fb-mp"},
	} {
		data, err := os.ReadFile(c.path)
		if err != nil {
			t.Fatalf("read %s: %v", c.path, err)
		}
		if line := strings.TrimSpace(string(data)); line != c.want {
			t.Errorf("%s = %q, want %q", c.path, line, c.want)
		}
	}
	// Root must NOT have these files — they live per-owner.
	for _, name := range []string{"corrupted_objects.txt", "ok_objects.txt", "mp.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s should not exist at root, got %v", name, err)
		}
	}
}

// TestOutputMultipartAllKeyOnly verifies the switch-off multipart file is
// key-only (the old `<key>|<etag>` format is gone).
func TestOutputMultipartAllKeyOnly(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartAll("owner-A", "key/with|pipe")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fkey%2Fwith%7Cpipe" {
		t.Errorf("multipart line = %q, want %q (bucket\x1fkey, values escaped)", line, "test-bkt\x1fkey%2Fwith%7Cpipe")
	}
}

// TestOutputCorruptedMultipartPerOwner — switch on: corrupted multipart
// writes to <owner>/corrupted_mp.txt.
func TestOutputCorruptedMultipartPerOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", nil)
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt\x1fmp%2Fk1")
	}
}

// TestOutputOffsetModeCorruptedMultipartCarriesOffsets — offset mode:
// corrupted_mp.txt lines carry partcnt+offsets in the -backup-file input
// shape (bkt\x1fkey\x1fpartcnt\x1foff0\x1foff1\x1f...) so the file can be fed
// straight back into -backup-file.
func TestOutputOffsetModeCorruptedMultipartCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880, 10485760})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1\x1f3\x1f0\x1f5242880\x1f10485760" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt\x1fmp%2Fk1\x1f3\x1f0\x1f5242880\x1f10485760")
	}
}

// TestOutputSegmentModeCorruptedMultipartIgnoresOffsets — segment mode:
// offsets are synthetic [0, seg, 2*seg, ...] guesses, NOT real part
// boundaries, so corrupted_mp.txt keeps the plain bucket\x1fkey shape
// (renderLineFmt) even when offsets are passed.
func TestOutputSegmentModeCorruptedMultipartIgnoresOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1" {
		t.Errorf("corrupted_mp = %q, want %q (segment mode keeps plain line shape)", line, "test-bkt\x1fmp%2Fk1")
	}
}

// TestOutputOffsetModeListParseFailedFallback — offset mode: mp.txt is
// disabled; multipart objects whose ETag did not parse (server without the
// feature) land in root-level list_parse_failed.txt instead. Line format
// `bucket|key`, aligned with check_failed.txt for -check-file retry.
func TestOutputOffsetModeListParseFailedFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteListParseFailed("mp/nosupport")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "list_parse_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fnosupport" {
		t.Errorf("list_parse_failed.txt = %q, want %q", line, "test-bkt\x1fmp%2Fnosupport")
	}
	if _, err := os.Stat(ownerSub(dir, "owner-A", "mp.txt")); !os.IsNotExist(err) {
		t.Errorf("mp.txt should not exist in mode=offset; stat err=%v", err)
	}
}

// TestOutputOffsetModeMpCheckFailedEnabled — offset mode: mp_check_failed
// stays enabled (offset probes can fail just like segment probes).
func TestOutputOffsetModeMpCheckFailedEnabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMpCheckFailed("mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "mp_check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880")
	}
}

// TestOutputCheckFileModeCorruptedMultipartCarriesOffsets — check-file mode
// with multipart_check_mode=0: input lines carry real part boundaries, so
// corrupted_mp.txt preserves them (check-file → backup-file loop without
// re-deriving offsets).
func TestOutputCheckFileModeCorruptedMultipartCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff}
	o, _ := NewOutput(cfg, "test-bkt", FileInputCheckFile)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880")
	}
}

// TestOutputMultipartOkPerOwner — switch on + is_multipart_success_log: clean
// multipart writes to <owner>/ok_mp.txt. Segment mode keeps the plain line:
// its offsets are synthetic, not part boundaries, so they stay off the line
// even when passed.
func TestOutputMultipartOkPerOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, IsSuccessLog: true, IsMultipartSuccessLog: true, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartOk("owner-A", "mp/clean", []int64{0, 5 * 1024 * 1024})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "ok_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fclean" {
		t.Errorf("ok_multipart = %q, want %q", line, "test-bkt\x1fmp%2Fclean")
	}
}

// TestOutputOffsetModeOkMpCarriesOffsets — offset mode: ok_mp.txt lines carry
// partcnt+offsets (real part boundaries), the same shape corrupted_mp.txt
// writes.
func TestOutputOffsetModeOkMpCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, IsMultipartSuccessLog: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartOk("owner-A", "mp/k1", []int64{0, 5242880, 10485760})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "ok_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1\x1f3\x1f0\x1f5242880\x1f10485760" {
		t.Errorf("ok_mp = %q, want %q", line, "test-bkt\x1fmp%2Fk1\x1f3\x1f0\x1f5242880\x1f10485760")
	}
}

// TestOutputMultipartOkSkippedWhenSuccessLogOff — switch on but
// is_multipart_success_log off: ok_multipart file must not be created.
func TestOutputMultipartOkSkippedWhenSuccessLogOff(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, IsSuccessLog: true, IsMultipartSuccessLog: false, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartOk("owner-A", "mp/clean", []int64{0, 1024}) // no-op
	o.Close()
	if _, err := os.Stat(ownerSub(dir, "owner-A", "ok_mp.txt")); !os.IsNotExist(err) {
		t.Errorf("ok_mp.txt should not exist when is_multipart_success_log off, got %v", err)
	}
}

// TestOutputCheckFailedAtRoot — check_failed.txt/.log stay at the output_dir
// root (global), not per-owner.
func TestOutputCheckFailedAtRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCheckFailed("obj/c")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fobj%2Fc" {
		t.Errorf("check_failed.txt = %q, want %q", line, "test-bkt\x1fobj%2Fc")
	}
	// Per-owner folder should NOT have been created just from a check_failed.
	if _, err := os.Stat(filepath.Join(dir, "owner-A")); !os.IsNotExist(err) {
		t.Errorf("owner folder should not exist for check_failed-only write, got %v", err)
	}
}

// TestOutputMpCheckFailedAtRoot — mp_check_failed.txt/.log stay
// at root (new process file for segment-check RangeGet errors).
func TestOutputMpCheckFailedAtRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMpCheckFailed("mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "mp_check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt\x1fmp%2Fk1\x1f2\x1f0\x1f5242880")
	}
}

// TestOutputMpCheckFailedSkippedWhenSwitchOff — switch off: the file
// is never created (segment check doesn't run, no one writes here).
func TestOutputMpCheckFailedSkippedWhenSwitchOff(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMpCheckFailed("mp/k1", []int64{0, 5242880}) // no-op
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "mp_check_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("mp_check_failed.txt should not exist when switch off, got %v", err)
	}
}

// TestOutputListFailedLogStructured verifies list_failed.log carries the
// structured error info (slog text format) with req_id positioned after msg
// and before the key field. The .txt next to it carries only the prefix.
// list_failed is global (root), not per-owner.
func TestOutputListFailedLogStructured(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	er := fakeS3Err(500, "InternalError", "we crashed", "REQ-LF-1")
	o.WriteListFailed("prefix/x", "")
	o.WriteListFailedLog("prefix/x", extractHTTPStatusCode(er), extractS3Code(er), extractRequestID(er), er)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	txtBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatalf("read list_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(txtBytes)); line != "prefix%2Fx" {
		t.Errorf("list_failed.txt = %q, want %q", line, "prefix%2Fx")
	}

	logBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.log"))
	if err != nil {
		t.Fatalf("read list_failed.log: %v", err)
	}
	log := string(logBytes)
	for _, want := range []string{
		`level=ERROR`,
		`msg="list failed"`,
		`req_id=REQ-LF-1`,
		`bucket=test-bkt`,
		`prefix=prefix/x`,
		`http_code=500`,
		`s3_code=InternalError`,
	} {
		if !strings.Contains(log, want) {
			t.Errorf("list_failed.log missing %q\nfull log:\n%s", want, log)
		}
	}
}

// TestOutputListOnlySkipsPerOwnerFiles — list-only mode (is_check=false):
// list_failed still opens at root, but the per-object channels/files are
// never created (no per-owner dirs).
func TestOutputListOnlySkipsPerOwnerFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false, IsSuccessLog: false}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	// Defensive: even if someone calls these, they should be no-ops.
	o.WriteCorrupted("owner-A", "k1")
	o.WriteMultipartAll("owner-A", "k2")
	o.WriteCheckFailed("k3")
	o.WriteSuccess("owner-A", "k4")
	o.WriteListFailed("prefix/", "")
	o.WriteMpCheckFailed("k5", []int64{0, 5242880})
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	// list_failed must exist (lister writes to it in both modes).
	if _, err := os.Stat(filepath.Join(dir, "list_failed.txt")); err != nil {
		t.Errorf("list_failed.txt should exist: %v", err)
	}
	// Per-owner and other root process files must NOT exist.
	for _, name := range []string{
		"corrupted_objects.txt",
		"mp.txt",
		"corrupted_mp.txt",
		"ok_mp.txt",
		"check_failed.txt",
		"mp_check_failed.txt",
		"ok_objects.txt",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s should not exist at root in list-only mode, got %v", name, err)
		}
	}
	// No owner subdirectory should exist.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			t.Errorf("unexpected owner subdirectory %q in list-only mode", e.Name())
		}
	}
}

func TestBackupOutputWritesThreeFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, BackupOutputDir: dir, IsCheck: true}
	out, err := NewBackupOutput(cfg, "mybucket")
	if err != nil {
		t.Fatal(err)
	}
	out.WriteBackupOk("k1")
	out.WriteBackupOk("k2")
	out.WriteBackupFailed("k3")
	out.WriteMismatch("mybucket|k4|1|0")
	out.WriteParseFailed("mybucket|bad")
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dir, "backup_ok.txt", "k1\nk2\n")
	assertFileContent(t, dir, "backup_failed.txt", "k3\n")
	assertFileContent(t, dir, "mismatch.txt", "mybucket|k4|1|0\n")
	assertFileContent(t, dir, "parse_failed.txt", "mybucket|bad\n")
	// backup_skipped_clean.txt is gone: backup relays every object the input
	// list names — the corruption verdict was made by the check run.
	if _, err := os.Stat(filepath.Join(dir, "backup_skipped_clean.txt")); !os.IsNotExist(err) {
		t.Errorf("backup_skipped_clean.txt should not exist (backup never probes): %v", err)
	}
}

// TestBackupOutputNoCheckModeFiles — backup output must not create the
// per-owner check-mode files; only the backup files + list_failed exist.
func TestBackupOutputNoCheckModeFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, BackupOutputDir: dir, IsCheck: true, IsSuccessLog: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 1024, IsMultipartSuccessLog: true}
	out, err := NewBackupOutput(cfg, "mybucket")
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"corrupted_objects.txt", "mp.txt", "corrupted_mp.txt", "ok_mp.txt", "ok_objects.txt", "check_failed.txt", "mp_check_failed.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("backup output created check-mode file %s", name)
		}
	}
}

func assertFileContent(t *testing.T, dir, name, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if string(data) != want {
		t.Errorf("%s = %q, want %q", name, string(data), want)
	}
}

// assertFileContentUnordered compares file content line-by-line after
// sorting both sides. Use it for files written by concurrent workers
// (backup_ok.txt etc.): the landing order of lines from parallel workers
// is nondeterministic, so an exact-string assert flakes by design.
func assertFileContentUnordered(t *testing.T, dir, name, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	gotLines := strings.Split(string(data), "\n")
	wantLines := strings.Split(want, "\n")
	sort.Strings(gotLines)
	sort.Strings(wantLines)
	if strings.Join(gotLines, "\n") != strings.Join(wantLines, "\n") {
		t.Errorf("%s = %q, want %q (line set)", name, string(data), want)
	}
}

// TestNewOutputCheckFileModeEnablesMultipartResults — check-file lines carry
// explicit offsets, so verification results must route to the multipart
// result files even when multipart_check_mode=0 (that mode only governs
// offset synthesis in bucket mode). mp.txt stays on: a regular line whose
// HEAD reveals a multipart object has no offsets and lands there unverified.
func TestNewOutputCheckFileModeEnablesMultipartResults(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartSuccessLog: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputCheckFile)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteCorruptedMultipart("", "mp-bad", []int64{0, 1024})
	o.WriteMultipartOk("", "mp-ok", []int64{0, 1024})
	o.WriteMpCheckFailed("mp-err", []int64{0, 1024})
	o.WriteMultipartAll("", "mp-drift") // type-drift fallback — enabled here
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_mp.txt"), "test-bkt\x1fmp-bad\x1f2\x1f0\x1f1024\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_mp.txt"), "test-bkt\x1fmp-ok\x1f2\x1f0\x1f1024\n")
	assertFileContent(t, dir, "mp_check_failed.txt", "test-bkt\x1fmp-err\x1f2\x1f0\x1f1024\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "mp.txt"), "test-bkt\x1fmp-drift\n")
}

// TestNewOutputCheckFileModeAlwaysLogsSuccess — check-file records clean
// objects unconditionally, like its failure results and like backup mode:
// neither is_success_log nor is_multipart_success_log gates ok_objects.txt /
// ok_mp.txt here (both are left at their default, false).
func TestNewOutputCheckFileModeAlwaysLogsSuccess(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputCheckFile)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteSuccess("", "reg-ok")
	o.WriteMultipartOk("", "mp-ok", []int64{0, 1024})
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_objects.txt"), "test-bkt\x1freg-ok\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_mp.txt"), "test-bkt\x1fmp-ok\x1f2\x1f0\x1f1024\n")
}

// TestOutputPeriodicFlushRoot — kill -9 safety of the resume inputs: a line
// pushed to a root writer channel must reach the file WITHOUT Close(),
// within outputFlushInterval. Before periodic flush, lines sat in the 64KB
// bufio buffer until channel close at run end — a SIGKILL then lost the
// tail of check_failed.txt (a -check-file retry input) even though the
// lines had already been "written".
func TestOutputPeriodicFlushRoot(t *testing.T) {
	orig := outputFlushInterval
	outputFlushInterval = 10 * time.Millisecond
	defer func() { outputFlushInterval = orig }()

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.WriteCheckFailed("obj/k1")

	path := filepath.Join(dir, "check_failed.txt")
	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(data)) == "test-bkt\x1fobj%2Fk1" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("line not flushed to %s without Close within deadline (kill -9 would lose it): content=%q err=%v", path, data, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestOutputPeriodicFlushOwner — same kill -9 bound for per-owner files: the
// single fan-out goroutine must flush EVERY opened owner's writer on the
// interval, not only on channel close.
func TestOutputPeriodicFlushOwner(t *testing.T) {
	orig := outputFlushInterval
	outputFlushInterval = 10 * time.Millisecond
	defer func() { outputFlushInterval = orig }()

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	o.WriteCorrupted("owner-A", "obj/a1")
	o.WriteCorrupted("owner-B", "obj/b1")

	for _, c := range []struct{ ownerID, key string }{
		{"owner-A", "obj/a1"},
		{"owner-B", "obj/b1"},
	} {
		path := ownerSub(dir, c.ownerID, "corrupted_objects.txt")
		deadline := time.Now().Add(2 * time.Second)
		for {
			data, err := os.ReadFile(path)
			if err == nil && strings.TrimSpace(string(data)) == "test-bkt\x1f"+url.QueryEscape(c.key) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("line not flushed to %s without Close within deadline (kill -9 would lose it): content=%q err=%v", path, data, err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
}

// TestOutputFlushAndWaitRoot — FlushAndWait must put every previously
// pushed line into the file WITHOUT Close: the writer drains the channel
// backlog (up to 1024 lines never touched by the bufio ticker) and only
// then flushes. Without the drain, lines sitting in the channel buffer
// would be invisible to the flush and the journal's ordering guarantee
// (spec §5) would have a hole.
func TestOutputFlushAndWaitRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	for i := 0; i < 1500; i++ { // more than the 1024 channel capacity
		o.WriteCheckFailed(fmt.Sprintf("obj/k%04d", i))
	}
	o.FlushAndWait()
	data, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "\n"); got != 1500 {
		t.Errorf("lines on disk without Close = %d, want 1500 (drain + flush)", got)
	}
}

// TestOutputFlushAndWaitOwner — the per-owner fan-out goroutine drains and
// flushes EVERY opened owner's writer, not just the most recent one.
func TestOutputFlushAndWaitOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	for i := 0; i < 300; i++ {
		o.WriteCorrupted("owner-A", fmt.Sprintf("obj/a%04d", i))
		o.WriteCorrupted("owner-B", fmt.Sprintf("obj/b%04d", i))
	}
	o.FlushAndWait()
	for _, owner := range []string{"owner-A", "owner-B"} {
		data, err := os.ReadFile(ownerSub(dir, owner, "corrupted_objects.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(string(data), "\n"); got != 300 {
			t.Errorf("%s lines on disk without Close = %d, want 300", owner, got)
		}
	}
}

// TestOutputFlushAndWaitLiveProducer — FlushAndWait while a producer is
// still pushing must not deadlock, and once the producer finishes and one
// more FlushAndWait runs, every line is durable. Run with -race.
func TestOutputFlushAndWaitLiveProducer(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	const N = 2000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < N; i++ {
			o.WriteCheckFailed(fmt.Sprintf("obj/k%04d", i))
			if i%100 == 0 {
				o.FlushAndWait() // concurrent flusher vs producer
			}
		}
	}()
	o.FlushAndWait() // races the producer on purpose
	<-done
	o.FlushAndWait()
	data, err := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "\n"); got != N {
		t.Errorf("lines on disk after producer done + FlushAndWait = %d, want %d", got, N)
	}
}

// TestOutputRecordFileSeparatorAndEscape — all record files use \x1f (ASCII
// Unit Separator) as the field separator and url.QueryEscape every server
// field (key/prefix/token) at the write boundary. A key containing '|',
// newline, or \x1f can therefore never tear the line structure. Bucket is
// CLI-provided (not a server field) and stays raw; tool-generated numbers
// stay raw. This is boundary B: unconditional, independent of
// encoding_type_url.
func TestOutputRecordFileSeparatorAndEscape(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)

	o.WriteCheckFailed("weird|key\nnext")
	o.WriteListFailed("pfx|with|pipe", "tok\x1fbad")
	o.WriteMpCheckFailed("mp|k", []int64{0, 100})
	o.Close()

	data, _ := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fweird%7Ckey%0Anext" {
		t.Errorf("check_failed.txt = %q, want %q", line, "test-bkt\x1fweird%7Ckey%0Anext")
	}
	data, _ = os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "pfx%7Cwith%7Cpipe\x1ftok%1Fbad" {
		t.Errorf("list_failed.txt = %q, want %q", line, "pfx%7Cwith%7Cpipe\x1ftok%1Fbad")
	}
	data, _ = os.ReadFile(filepath.Join(dir, "mp_check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fmp%7Ck\x1f2\x1f0\x1f100" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt\x1fmp%7Ck\x1f2\x1f0\x1f100")
	}
}

// TestOutputListFailedSingleFieldAndEmpty — a failed first page writes the
// bare prefix (no trailing separator); the root prefix failure writes an
// empty line (the -resume-list legal entry). An empty token with an empty
// prefix stays empty.
func TestOutputListFailedSingleFieldAndEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteListFailed("data/2026/", "")
	o.WriteListFailed("", "")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	// split on the raw content; the final "" element is the file's trailing
	// newline, not a third entry.
	lines := strings.Split(string(data[:len(data)-1]), "\n")
	if len(lines) != 2 || lines[0] != "data%2F2026%2F" || lines[1] != "" {
		t.Errorf("list_failed.txt lines = %#v, want [data%%2F2026%%2F, \"\"]", lines)
	}
}

// TestOutputOffsetLineEscapesKey — corrupted_mp.txt in offset mode writes
// bkt\x1fkey\x1fpartcnt\x1foff0|... with the key escaped (bucket and
// offsets raw).
func TestOutputOffsetLineEscapesKey(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner", "corrupt|ed.bin", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "owner", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fcorrupt%7Ced.bin\x1f2\x1f0\x1f5242880" {
		t.Errorf("corrupted_mp.txt = %q, want %q", line, "test-bkt\x1fcorrupt%7Ced.bin\x1f2\x1f0\x1f5242880")
	}
}

// TestOutputPerOwnerLineEscapesKey — per-owner result files use
// bucket\x1f<escaped-key>: the key is QueryEscape'd so its \x1f, |, \n
// become %1F, %7C, %0A and cannot tear the line structure; bucket comes
// from the CLI and stays raw.
func TestOutputPerOwnerLineEscapesKey(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorrupted("abc123", "a|b\nc\x1f")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "abc123", "corrupted_objects.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt\x1fa%7Cb%0Ac%1F" {
		t.Errorf("corrupted_objects.txt = %q, want %q", line, "test-bkt\x1fa%7Cb%0Ac%1F")
	}
}
