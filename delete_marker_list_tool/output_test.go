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
	want0 := "testbucket\x1fphotos/a.jpg\x1fv1\x1f2026-11-13T15:30:00Z"
	want1 := "testbucket\x1fphotos/b.jpg\x1fnull\x1f2026-11-13T15:31:00Z"
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
	o.WriteDeleteMarker("a\x1fb", "v1", "2026-11-13T15:30:00Z")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimRight(string(b), "\n")
	// "a\x1fb" → url.QueryEscape → "a%1Fb"
	want := "testbucket\x1fa%1Fb\x1fv1\x1f2026-11-13T15:30:00Z"
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

// TestWriteInvalidKey_NewlineRoutedToInvalidKeys 验证含 \n / \r 的 key 也被
// 路由到 invalid_keys.txt（防止撕裂 delete_markers.txt 行结构）。
// \x1f 撕裂字段、\n/\r 撕裂整行——三类字符走同一兜底路径。
func TestWriteInvalidKey_NewlineRoutedToInvalidKeys(t *testing.T) {
	cases := []struct{ name, key, escaped string }{
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
			b, err := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
			if err != nil {
				t.Fatal(err)
			}
			line := strings.TrimRight(string(b), "\n")
			want := "testbucket\x1f" + c.escaped + "\x1fv1\x1f2026-11-13T15:30:00Z"
			if line != want {
				t.Errorf("got %q want %q", line, want)
			}
			// delete_markers.txt must be empty (single-line key only there).
			dm, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(string(dm)) != "" {
				t.Errorf("delete_markers.txt should be empty, got %q", string(dm))
			}
		})
	}
}

// TestContainsRowSep 覆盖三类撕裂字符：\x1f（字段分隔）、\n/\r（行分隔）。
func TestContainsRowSep(t *testing.T) {
	for _, c := range []struct {
		s    string
		want bool
	}{
		{"photos/a.jpg", false},
		{"", false},
		{"a\x1fb", true},
		{"a\nb", true},
		{"a\rb", true},
		{"a\r\nb", true},
		{"a\x1f\nb", true},
	} {
		if got := containsRowSep(c.s); got != c.want {
			t.Errorf("containsRowSep(%q)=%v want %v", c.s, got, c.want)
		}
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
	// Three lines, each `prefix\x1fkeyMarker\x1fversionIdMarker\n`
	want := "photos/\x1f\x1f\n\x1f\x1f\nphotos/\x1fk1\x1fv1\n"
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
