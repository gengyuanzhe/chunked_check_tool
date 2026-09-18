package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
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
		{ownerSub(dir, "owner-A", "corrupted_objects.txt"), "test-bkt|obj/a1"},
		{ownerSub(dir, "owner-B", "corrupted_objects.txt"), "test-bkt|obj/b1"},
		{ownerSub(dir, "owner-A", "ok_objects.txt"), "test-bkt|obj/a-ok"},
		{ownerSub(dir, "owner-B", "mp.txt"), "test-bkt|obj/b-mp"},
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt|key/with|pipe" {
		t.Errorf("multipart line = %q, want %q (bucket|key, key may contain |)", line, "test-bkt|key/with|pipe")
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt|mp/k1")
	}
}

// TestOutputOffsetModeCorruptedMultipartCarriesOffsets — offset mode:
// corrupted_mp.txt lines carry partcnt+offsets in the -list-file/
// -backup-file input shape (bkt|key|partcnt|off0|off1|...) so the file can
// be fed straight back into -backup-file. result_line_format does not apply
// to this file in offset mode.
func TestOutputOffsetModeCorruptedMultipartCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset, ResultLineFormat: "<key>@<bucket>"}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880, 10485760})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1|3|0|5242880|10485760" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt|mp/k1|3|0|5242880|10485760")
	}
}

// TestOutputSegmentModeCorruptedMultipartIgnoresOffsets — segment mode:
// offsets are synthetic [0, seg, 2*seg, ...] guesses, NOT real part
// boundaries, so corrupted_mp.txt must keep the result_line_format shape
// even when offsets are passed.
func TestOutputSegmentModeCorruptedMultipartIgnoresOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1" {
		t.Errorf("corrupted_mp = %q, want %q (segment mode keeps line format)", line, "test-bkt|mp/k1")
	}
}

// TestOutputOffsetModeMultipartAllFallback — offset mode: mp.txt stays
// enabled as the fallback for multipart objects whose ETag did not parse
// (server without the feature). Lines keep the result_line_format shape —
// those objects have no offsets to write.
func TestOutputOffsetModeMultipartAllFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartAll("owner-A", "mp/nosupport")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/nosupport" {
		t.Errorf("mp.txt = %q, want %q", line, "test-bkt|mp/nosupport")
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1|2|0|5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt|mp/k1|2|0|5242880")
	}
}

// TestOutputListFileModeCorruptedMultipartCarriesOffsets — list-file mode:
// input lines carry real part boundaries, so corrupted_mp.txt preserves
// them (list-file → backup-file loop without re-deriving offsets).
func TestOutputListFileModeCorruptedMultipartCarriesOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeOff}
	o, _ := NewOutput(cfg, "test-bkt", FileInputListFile)
	o.WriteCorruptedMultipart("owner-A", "mp/k1", []int64{0, 5242880})
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1|2|0|5242880" {
		t.Errorf("corrupted_mp = %q, want %q", line, "test-bkt|mp/k1|2|0|5242880")
	}
}

// TestOutputMultipartOkPerOwner — switch on + is_multipart_success_log: clean
// multipart writes to <owner>/ok_mp.txt.
func TestOutputMultipartOkPerOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, IsSuccessLog: true, IsMultipartSuccessLog: true, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartOk("owner-A", "mp/clean")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "ok_mp.txt"))
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/clean" {
		t.Errorf("ok_multipart = %q, want %q", line, "test-bkt|mp/clean")
	}
}

// TestOutputMultipartOkSkippedWhenSuccessLogOff — switch on but
// is_multipart_success_log off: ok_multipart file must not be created.
func TestOutputMultipartOkSkippedWhenSuccessLogOff(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, MultipartCheckMode: MultipartCheckModeSegment, IsSuccessLog: true, IsMultipartSuccessLog: false, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	o.WriteMultipartOk("owner-A", "mp/clean") // no-op
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt|obj/c" {
		t.Errorf("check_failed.txt = %q, want %q", line, "test-bkt|obj/c")
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
	if line := strings.TrimSpace(string(data)); line != "test-bkt|mp/k1|2|0|5242880" {
		t.Errorf("mp_check_failed.txt = %q, want %q", line, "test-bkt|mp/k1|2|0|5242880")
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
	er := minio.ErrorResponse{
		Code:       "InternalError",
		Message:    "we crashed",
		StatusCode: 500,
		RequestID:  "REQ-LF-1",
	}
	o.WriteListFailed("prefix/x", "")
	o.WriteListFailedLog("prefix/x", extractHTTPStatusCode(er), extractS3Code(er), extractRequestID(er), er)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	txtBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatalf("read list_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(txtBytes)); line != "prefix/x" {
		t.Errorf("list_failed.txt = %q, want %q", line, "prefix/x")
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

// TestNewOutputListFileModeEnablesMultipartResults — list-file tasks always
// carry explicit offsets, so verification results must route to the
// multipart result files even when multipart_check_mode=0 (that mode only
// governs offset synthesis in bucket mode).
func TestNewOutputListFileModeEnablesMultipartResults(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartSuccessLog: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputListFile)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteCorruptedMultipart("", "mp-bad", []int64{0, 1024})
	o.WriteMultipartOk("", "mp-ok")
	o.WriteMpCheckFailed("mp-err", []int64{0, 1024})
	o.WriteMultipartAll("", "mp-all") // catalog path — must stay gated OFF
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_mp.txt"), "test-bkt|mp-bad|2|0|1024\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_mp.txt"), "test-bkt|mp-ok\n")
	assertFileContent(t, dir, "mp_check_failed.txt", "test-bkt|mp-err|2|0|1024\n")
	if _, err := os.Stat(filepath.Join(dir, "_unknown", "mp.txt")); !os.IsNotExist(err) {
		t.Errorf("mp.txt should not be written in list-file mode")
	}
}

// TestNewOutputListFileModeOkMpOptIn — ok_mp.txt stays opt-in via
// is_multipart_success_log (default false), matching bucket-mode
// philosophy; corrupted/failed results are unconditional.
func TestNewOutputListFileModeOkMpOptIn(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, err := NewOutput(cfg, "test-bkt", FileInputListFile)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteMultipartOk("", "mp-ok")
	o.WriteCorruptedMultipart("", "mp-bad", []int64{0, 1024})
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_mp.txt"), "test-bkt|mp-bad|2|0|1024\n")
	if _, err := os.Stat(filepath.Join(dir, "_unknown", "ok_mp.txt")); !os.IsNotExist(err) {
		t.Errorf("ok_mp.txt should require is_multipart_success_log=true")
	}
}
