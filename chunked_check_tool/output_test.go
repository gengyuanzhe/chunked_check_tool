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
	o, _ := NewOutput(cfg)
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
		{ownerSub(dir, "owner-A", "corrupted_objects.txt"), "obj/a1"},
		{ownerSub(dir, "owner-B", "corrupted_objects.txt"), "obj/b1"},
		{ownerSub(dir, "owner-A", "ok_objects.txt"), "obj/a-ok"},
		{ownerSub(dir, "owner-B", "multipart_objects.txt"), "obj/b-mp"},
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
	for _, name := range []string{"corrupted_objects.txt", "ok_objects.txt", "multipart_objects.txt"} {
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
	o, _ := NewOutput(cfg)
	o.WriteMultipartAll("owner-A", "key/with|pipe")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "multipart_objects.txt"))
	if line := strings.TrimSpace(string(data)); line != "key/with|pipe" {
		t.Errorf("multipart line = %q, want %q (key only, no etag)", line, "key/with|pipe")
	}
}

// TestOutputCorruptedMultipartPerOwner — switch on: corrupted multipart
// writes to <owner>/corrupted_multipart_objects.txt.
func TestOutputCorruptedMultipartPerOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: true, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg)
	o.WriteCorruptedMultipart("owner-A", "mp/k1")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "corrupted_multipart_objects.txt"))
	if line := strings.TrimSpace(string(data)); line != "mp/k1" {
		t.Errorf("corrupted_multipart = %q, want %q", line, "mp/k1")
	}
}

// TestOutputMultipartOkPerOwner — switch on + is_success_log: clean multipart
// writes to <owner>/ok_multipart_objects.txt.
func TestOutputMultipartOkPerOwner(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: true, IsSuccessLog: true, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg)
	o.WriteMultipartOk("owner-A", "mp/clean")
	o.Close()
	data, _ := os.ReadFile(ownerSub(dir, "owner-A", "ok_multipart_objects.txt"))
	if line := strings.TrimSpace(string(data)); line != "mp/clean" {
		t.Errorf("ok_multipart = %q, want %q", line, "mp/clean")
	}
}

// TestOutputMultipartOkSkippedWhenSuccessLogOff — switch on but is_success_log
// off: ok_multipart file must not be created.
func TestOutputMultipartOkSkippedWhenSuccessLogOff(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: true, IsSuccessLog: false, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg)
	o.WriteMultipartOk("owner-A", "mp/clean") // no-op
	o.Close()
	if _, err := os.Stat(ownerSub(dir, "owner-A", "ok_multipart_objects.txt")); !os.IsNotExist(err) {
		t.Errorf("ok_multipart_objects.txt should not exist when is_success_log off, got %v", err)
	}
}

// TestOutputCheckFailedAtRoot — check_failed.txt/.log stay at the output_dir
// root (global), not per-owner.
func TestOutputCheckFailedAtRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg)
	o.WriteCheckFailed("obj/c")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "obj/c" {
		t.Errorf("check_failed.txt = %q, want %q", line, "obj/c")
	}
	// Per-owner folder should NOT have been created just from a check_failed.
	if _, err := os.Stat(filepath.Join(dir, "owner-A")); !os.IsNotExist(err) {
		t.Errorf("owner folder should not exist for check_failed-only write, got %v", err)
	}
}

// TestOutputMultipartCheckFailedAtRoot — multipart_check_failed.txt/.log stay
// at root (new process file for segment-check RangeGet errors).
func TestOutputMultipartCheckFailedAtRoot(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: true, MultipartSegmentSize: 5 * 1024 * 1024}
	o, _ := NewOutput(cfg)
	o.WriteMultipartCheckFailed("mp/k1")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "multipart_check_failed.txt"))
	if line := strings.TrimSpace(string(data)); line != "mp/k1" {
		t.Errorf("multipart_check_failed.txt = %q, want %q", line, "mp/k1")
	}
}

// TestOutputMultipartCheckFailedSkippedWhenSwitchOff — switch off: the file
// is never created (segment check doesn't run, no one writes here).
func TestOutputMultipartCheckFailedSkippedWhenSwitchOff(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsMultipartCheck: false}
	o, _ := NewOutput(cfg)
	o.WriteMultipartCheckFailed("mp/k1") // no-op
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "multipart_check_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("multipart_check_failed.txt should not exist when switch off, got %v", err)
	}
}

// TestOutputListFailedLogStructured verifies list_failed.log carries the
// structured error info (slog text format) with req_id positioned after msg
// and before the key field. The .txt next to it carries only the prefix.
// list_failed is global (root), not per-owner.
func TestOutputListFailedLogStructured(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg)
	er := minio.ErrorResponse{
		Code:       "InternalError",
		Message:    "we crashed",
		StatusCode: 500,
		RequestID:  "REQ-LF-1",
	}
	o.WriteListFailed("prefix/x")
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
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Defensive: even if someone calls these, they should be no-ops.
	o.WriteCorrupted("owner-A", "k1")
	o.WriteMultipartAll("owner-A", "k2")
	o.WriteCheckFailed("k3")
	o.WriteSuccess("owner-A", "k4")
	o.WriteListFailed("prefix/")
	o.WriteMultipartCheckFailed("k5")
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
		"multipart_objects.txt",
		"corrupted_multipart_objects.txt",
		"ok_multipart_objects.txt",
		"check_failed.txt",
		"multipart_check_failed.txt",
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
