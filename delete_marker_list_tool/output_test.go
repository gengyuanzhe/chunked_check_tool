package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestOutput(t *testing.T) (*Output, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	cfg := &Config{OutputDir: dir, OutputDirTimestamp: false}
	o, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	return o, dir
}

func TestWriteDeleteMarker_RowShape(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteDeleteMarker("photos/a.jpg", "v1")
	o.WriteDeleteMarker("photos/b.jpg", "null")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), string(b))
	}
	want0 := "testbucket\x1fphotos/a.jpg\x1fv1"
	want1 := "testbucket\x1fphotos/b.jpg\x1fnull"
	if lines[0] != want0 {
		t.Errorf("line0=%q want %q", lines[0], want0)
	}
	if lines[1] != want1 {
		t.Errorf("line1=%q want %q", lines[1], want1)
	}
}

func TestWriteInvalidKey_UrlEncodesKey(t *testing.T) {
	o, dir := newTestOutput(t)
	// key contains \x1f — must be URL-encoded and routed to invalid_keys.txt
	o.WriteDeleteMarker("a\x1fb", "v1")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimRight(string(b), "\n")
	// "a\x1fb" → url.QueryEscape → "a%1Fb"
	want := "testbucket\x1fa%1Fb\x1fv1"
	if line != want {
		t.Errorf("got %q want %q", line, want)
	}
	// delete_markers.txt must be empty
	dm, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(dm)) != "" {
		t.Errorf("delete_markers.txt should be empty, got %q", string(dm))
	}
}

func TestWriteListFailed_Contract(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteListFailed("photos/") // non-empty prefix
	o.WriteListFailed("")        // empty prefix → empty line
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Two lines: "photos/" and ""
	want := "photos/\n\n"
	if string(b) != want {
		t.Errorf("got %q want %q", string(b), want)
	}
}

func TestListFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t)
	o.FlushAndWait()
	o.Close()
	// No WriteListFailed calls → file should not exist
	if _, err := os.Stat(filepath.Join(dir, "list_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("list_failed.txt should not exist when no failures; err=%v", err)
	}
}

func TestWriteListFailedLog_WritesSlog(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteListFailedLog("photos/", 500, "InternalError", "req-123", nil)
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "list_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{"list failed", "photos/", "500", "InternalError", "req-123"} {
		if !strings.Contains(out, want) {
			t.Errorf("list_failed.log missing %q: %q", want, out)
		}
	}
}
