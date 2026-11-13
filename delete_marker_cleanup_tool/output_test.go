package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestOutput(t *testing.T, mode string) (*Output, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	cfg := &Config{OutputDir: dir, OutputDirTimestamp: false}
	o, err := NewOutput(cfg, mode)
	if err != nil {
		t.Fatal(err)
	}
	return o, dir
}

func TestWriteCheckResult_FourFieldRow(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteCheckResult("bkt", "photos/a.jpg", "v1", "2026-11-13T15:30:00Z", "marker")
	o.WriteCheckResult("bkt", "photos/b.jpg", "null", "2026-11-13T15:31:00Z", "not_marker")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), string(b))
	}
	want0 := "bkt\x1fphotos/a.jpg\x1fv1\x1f2026-11-13T15:30:00Z\x1fmarker"
	want1 := "bkt\x1fphotos/b.jpg\x1fnull\x1f2026-11-13T15:31:00Z\x1fnot_marker"
	if lines[0] != want0 {
		t.Errorf("line0=%q want %q", lines[0], want0)
	}
	if lines[1] != want1 {
		t.Errorf("line1=%q want %q", lines[1], want1)
	}
}

func TestWriteDeleted_ThreeFieldRow(t *testing.T) {
	o, dir := newTestOutput(t, "delete")
	o.WriteDeleted("bkt", "key1", "v1", "2026-11-13T15:30:00Z")
	o.WriteDeleted("bkt", "key2", "already-deleted", "")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "deleted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[0] != "bkt\x1fkey1\x1fv1\x1f2026-11-13T15:30:00Z" {
		t.Errorf("line0=%q", lines[0])
	}
	if lines[1] != "bkt\x1fkey2\x1falready-deleted\x1f" {
		t.Errorf("line1=%q", lines[1])
	}
}

func TestHeadFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.FlushAndWait()
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "head_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("head_failed.txt should not exist when no failures; err=%v", err)
	}
}

func TestWriteHeadFailed_CreatesAndWrites(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteHeadFailed("bkt", "k", "v", "2026-11-13T15:30:00Z")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "head_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := "bkt\x1fk\x1fv\x1f2026-11-13T15:30:00Z\n"
	if string(b) != want {
		t.Errorf("got %q want %q", string(b), want)
	}
}

func TestWriteHeadFailedLog_WritesSlog(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteHeadFailedLog("bkt", "k", "v", 500, "InternalError", "req-1", nil)
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "head_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{"bkt", "k", "v", "500", "InternalError", "req-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("head_failed.log missing %q: %q", want, out)
		}
	}
}

func TestDeleteFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t, "delete")
	o.FlushAndWait()
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "delete_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("delete_failed.txt should not exist when no failures")
	}
}

func TestWriteDeleteFailedLog_WritesSlog(t *testing.T) {
	o, dir := newTestOutput(t, "delete")
	o.WriteDeleteFailedLog("bkt", "k", "v", 500, "InternalError", "req-2", nil)
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "delete_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{"bkt", "k", "v", "500", "InternalError", "req-2"} {
		if !strings.Contains(out, want) {
			t.Errorf("delete_failed.log missing %q: %q", want, out)
		}
	}
}
