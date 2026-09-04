package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	o.WriteListFailed("prefix/x", "timeout")
	o.WriteCheckFailed("obj/c", "500")
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
	check("list_failed.txt", "prefix/x")
	check("list_failed.txt", "timeout")
	check("check_failed.txt", "obj/c")
	check("check_failed.txt", "500")
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
	o.WriteCheckFailed("k3", "err")
	o.WriteSuccess("k4")
	o.WriteListFailed("prefix/", "timeout")
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
