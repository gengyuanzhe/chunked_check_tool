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
	o.WriteDeleteMarker("photos/a.jpg", "v1", "2026-11-13T15:30:00Z")
	o.WriteDeleteMarker("photos/b.jpg", "null", "2026-11-13T15:31:00Z")
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
	// server 字段（key/versionID）一律 QueryEscape：'/'→%2F；"null" 恒等。
	want0 := "testbucket\x1fphotos%2Fa.jpg\x1fv1\x1f2026-11-13T15:30:00Z"
	want1 := "testbucket\x1fphotos%2Fb.jpg\x1fnull\x1f2026-11-13T15:31:00Z"
	if lines[0] != want0 {
		t.Errorf("line0=%q want %q", lines[0], want0)
	}
	if lines[1] != want1 {
		t.Errorf("line1=%q want %q", lines[1], want1)
	}
}

// TestWriteDeleteMarker_EscapesTearingChars 验证含撕裂类字符（\x1f 字段分隔、
// \n/\r 行分隔）的 key 统一 QueryEscape 进 delete_markers.txt——编码后行结构
// 不会被撕裂，不再有 invalid_keys 分流。
func TestWriteDeleteMarker_EscapesTearingChars(t *testing.T) {
	cases := []struct{ name, key, escaped string }{
		{"unit_sep", "a\x1fb", "a%1Fb"},
		{"lf", "a\nb", "a%0Ab"},
		{"cr", "a\rb", "a%0Db"},
		{"crlf", "a\r\nb", "a%0D%0Ab"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, dir := newTestOutput(t)
			o.WriteDeleteMarker(c.key, "v1", "2026-11-13T15:30:00Z")
			o.FlushAndWait()
			o.Close()
			b, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
			if len(lines) != 1 {
				t.Fatalf("got %d lines, want 1 (row must not tear): %q", len(lines), string(b))
			}
			want := "testbucket\x1f" + c.escaped + "\x1fv1\x1f2026-11-13T15:30:00Z"
			if lines[0] != want {
				t.Errorf("line=%q want %q", lines[0], want)
			}
		})
	}
}

// TestWriteDeleteMarker_EscapesVersionID 验证 versionID 也统一 QueryEscape
// （AWS 风格 version-id 含 '/' 等字符）。
func TestWriteDeleteMarker_EscapesVersionID(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteDeleteMarker("a.txt", "3/L4kqtJlcpX", "2026-11-13T15:30:00Z")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := "testbucket\x1fa.txt\x1f3%2FL4kqtJlcpX\x1f2026-11-13T15:30:00Z"
	if strings.TrimRight(string(b), "\n") != want {
		t.Errorf("got %q want %q", string(b), want)
	}
}

func TestWriteListFailed_Contract(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteListFailed("photos/", "", "") // non-empty prefix, no markers (from page 1)
	o.WriteListFailed("", "", "")        // empty prefix + no markers → root prefix from page 1
	o.WriteListFailed("photos/", "k1", "v1") // page-level resume: prefix + markers
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Three lines, each `prefix\x1fkeyMarker\x1fversionIdMarker\n`（三字段均编码）
	want := "photos%2F\x1f\x1f\n\x1f\x1f\nphotos%2F\x1fk1\x1fv1\n"
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
	o.WriteListFailedLog("photos/", "", "", 500, "InternalError", "req-123", nil)
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
