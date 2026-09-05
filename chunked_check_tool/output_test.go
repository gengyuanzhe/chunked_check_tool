package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestOutputWritesCorruptedAndMultipart(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true, IsSuccessLog: true}
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteCorrupted("obj/a")
	o.WriteMultipart("abc123-2", "obj/b")
	o.WriteListFailed("prefix/x")
	o.WriteCheckFailed("obj/c")
	o.WriteSuccess("obj/ok")
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(name, want string) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s missing %q:\n%s", name, want, string(data))
		}
	}
	check("corrupted_objects.txt", "obj/a")
	check("multipart_objects.txt", "obj/b|abc123-2")
	// .txt files now only carry the key/prefix (the .log file next to them
	// carries the structured error info). Assert the .txt is bare — no
	// trailing error text.
	lfBytes, _ := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if line := strings.TrimSpace(string(lfBytes)); line != "prefix/x" {
		t.Errorf("list_failed.txt = %q, want %q", line, "prefix/x")
	}
	cfBytes, _ := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
	if line := strings.TrimSpace(string(cfBytes)); line != "obj/c" {
		t.Errorf("check_failed.txt = %q, want %q", line, "obj/c")
	}
	check("success_objects.log", "obj/ok")
}

func TestOutputNoSuccessWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsSuccessLog: false}
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteSuccess("x") // should be no-op
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "success_objects.log")); !os.IsNotExist(err) {
		t.Errorf("success log should not exist, got %v", err)
	}
}

// In list-only mode (is_check=false), only list_failed.txt should be
// created — the object files (corrupted/multipart/check_failed/success)
// must not exist, because nothing writes to them and we don't want to
// leave empty files lying around.
func TestOutputListOnlySkipsObjectFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false, IsSuccessLog: false}
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Defensive: even if someone calls these, they should be no-ops.
	o.WriteCorrupted("k1")
	o.WriteMultipart("etag-2", "k2")
	o.WriteCheckFailed("k3")
	o.WriteSuccess("k4")
	o.WriteListFailed("prefix/")
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	// list_failed must exist (lister writes to it in both modes).
	if _, err := os.Stat(filepath.Join(dir, "list_failed.txt")); err != nil {
		t.Errorf("list_failed.txt should exist: %v", err)
	}
	// Object files must NOT exist.
	for _, name := range []string{
		"corrupted_objects.txt",
		"multipart_objects.txt",
		"check_failed.txt",
		"success_objects.log",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s should not exist in list-only mode, got %v", name, err)
		}
	}
}

func TestOutputMultipartFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	o, _ := NewOutput(cfg)
	o.WriteMultipart("deadbeef-3", "key/with|pipe")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "multipart_objects.txt"))
	line := strings.TrimSpace(string(data))
	if line != "key/with|pipe|deadbeef-3" {
		t.Errorf("multipart line = %q", line)
	}
}

// TestOutputListFailedLogStructured verifies list_failed.log carries the
// structured error info (slog text format) with req_id positioned after msg
// and before the key field. The .txt next to it carries only the prefix.
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

	// .txt carries only the prefix
	txtBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatalf("read list_failed.txt: %v", err)
	}
	if line := strings.TrimSpace(string(txtBytes)); line != "prefix/x" {
		t.Errorf("list_failed.txt = %q, want %q", line, "prefix/x")
	}

	// .log carries structured fields
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
