package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWriteListFailedTokenFormat — WriteListFailed(prefix, token) writes
// `prefix|token` to list_failed.txt; empty token writes just `prefix`.
// -resume-list parses these lines and feeds (prefix, token) back into BFS.
func TestWriteListFailedTokenFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)

	// First page failed: no token → single field.
	o.WriteListFailed("data/2026/", "")
	// Later page failed: token from previous successful page.
	o.WriteListFailed("data/2027/", "abc123token")
	// Another first-page failure.
	o.WriteListFailed("data/2028/", "")

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatalf("read list_failed.txt: %v", err)
	}
	want := "data%2F2026%2F\ndata%2F2027%2F\x1fabc123token\ndata%2F2028%2F\n"
	if string(data) != want {
		t.Errorf("list_failed.txt = %q, want %q", string(data), want)
	}
}

// TestWriteListFailedPipePrefixEscaped — a prefix containing '|' (legal in
// S3 keys) is now safely escapable: the line lands in list_failed.txt with
// the pipe percent-encoded, and no invalid_keys diversion exists anymore.
func TestWriteListFailedPipePrefixEscaped(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)

	o.WriteListFailed("data/a|b/", "tokenX")

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	listData, _ := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if got, want := string(listData), "data%2Fa%7Cb%2F\x1ftokenX\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q", got, want)
	}
}

// TestParseResumeListLine — parse one line of list_failed.txt back into
// (prefix, token). Covers both single-field (first page failed) and
// two-field (later page failed) shapes.
func TestParseResumeListLine(t *testing.T) {
	cases := []struct {
		line   string
		prefix string
		token  string
	}{
		// handwritten plain prefixes (QueryEscape is identity) keep working
		{"data/2026/", "data/2026/", ""},
		{"data/2027/\x1fabc123", "data/2027/", "abc123"},
		{"data/2028/\x1f", "data/2028/", ""},
		// tool-written lines: server fields escaped at the write boundary
		{"data%2F2027%2F\x1ftok%7Cen", "data/2027/", "tok|en"},
		{"dir%0Asub\x1f", "dir\nsub", ""},
	}
	for _, c := range cases {
		e, err := parseResumeListLine(c.line)
		if err != nil {
			t.Errorf("parseResumeListLine(%q) err: %v", c.line, err)
			continue
		}
		if e.prefix != c.prefix || e.token != c.token {
			t.Errorf("parseResumeListLine(%q) = {%q, %q}, want {%q, %q}",
				c.line, e.prefix, e.token, c.prefix, c.token)
		}
	}
}

// TestParseResumeListLineEmpty — empty line is a LEGAL root-prefix failure
// record (prefix=="", token==""), not an error. Whitespace-only lines are
// errors: WriteListFailed never emits them, so they indicate a manually
// corrupted file.
func TestParseResumeListLineEmpty(t *testing.T) {
	// "" is legal: root prefix failed on first page.
	e, err := parseResumeListLine("")
	if err != nil {
		t.Errorf("parseResumeListLine(\"\") err: %v (empty line is a legal root-prefix entry)", err)
	}
	if e.prefix != "" || e.token != "" {
		t.Errorf("parseResumeListLine(\"\") = {%q, %q}, want {\"\", \"\"}", e.prefix, e.token)
	}
	// Whitespace-only lines are errors (manually corrupted file).
	for _, line := range []string{"   ", "\t"} {
		if _, err := parseResumeListLine(line); err == nil {
			t.Errorf("parseResumeListLine(%q) should error on whitespace-only line", line)
		}
	}
}

// TestParseResumeListLineDecodeFailure — a stray %XX escape in a field (a
// manually mangled file, not tool output) fails loudly at the input
// boundary instead of silently mis-decoding.
func TestParseResumeListLineDecodeFailure(t *testing.T) {
	for _, line := range []string{"bad%zz\x1ftok", "good%2Fp\x1fbad%qq"} {
		if _, err := parseResumeListLine(line); err == nil {
			t.Errorf("parseResumeListLine(%q) should error on invalid escape", line)
		}
	}
}

// TestResumeListRoundTrip — WriteListFailed output parses back into the
// exact (prefix, token) pair, including pipe/newline/\x1f-bearing values
// that the old '|' format could not carry.
func TestResumeListRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)

	cases := []struct{ prefix, token string }{
		{"data/2026/", ""},
		{"", ""},
		{"weird|prefix/", "tok\n1"},
		{"pfx\x1funit/", "tok%2Fpipe"},
	}
	for _, c := range cases {
		o.WriteListFailed(c.prefix, c.token)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	entries, err := readResumeList(filepath.Join(dir, "list_failed.txt"), o, stats)
	if err != nil {
		t.Fatalf("readResumeList: %v", err)
	}
	if len(entries) != len(cases) {
		t.Fatalf("got %d entries, want %d", len(entries), len(cases))
	}
	for i, c := range cases {
		if entries[i].prefix != c.prefix || entries[i].token != c.token {
			t.Errorf("entries[%d] = {%q, %q}, want {%q, %q}", i, entries[i].prefix, entries[i].token, c.prefix, c.token)
		}
	}
}

// TestReadResumeListEndToEnd — write a list_failed.txt file, then
// readResumeList parses all lines and returns entries.
//
// NOTE: list_failed.txt is assumed to be written by WriteListFailed, which
// routes prefix-with-| lines to invalid_keys.txt. So every line in
// list_failed.txt has prefix without |, and SplitN("|", 2) is unambiguous.
// Manually editing list_failed.txt to inject a prefix with | would silently
// mis-split (the | would be treated as the separator), but that's a contract
// violation outside the tool's responsibility.
func TestReadResumeListEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", FileInputNone)

	// Write the resume-list file manually (simulating a prior run's output).
	resumePath := filepath.Join(dir, "list_failed.txt")
	content := "data/2026/\ndata/2027/\x1ftokenABC\ndata/2028/\x1ftokenDEF\n"
	if err := os.WriteFile(resumePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	stats := NewStats()
	entries, err := readResumeList(resumePath, o, stats)
	if err != nil {
		t.Fatalf("readResumeList: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[0].prefix != "data/2026/" || entries[0].token != "" {
		t.Errorf("entries[0] = {%q, %q}, want {%q, %q}",
			entries[0].prefix, entries[0].token, "data/2026/", "")
	}
	if entries[1].prefix != "data/2027/" || entries[1].token != "tokenABC" {
		t.Errorf("entries[1] = {%q, %q}, want {%q, %q}",
			entries[1].prefix, entries[1].token, "data/2027/", "tokenABC")
	}
	if entries[2].prefix != "data/2028/" || entries[2].token != "tokenDEF" {
		t.Errorf("entries[2] = {%q, %q}, want {%q, %q}",
			entries[2].prefix, entries[2].token, "data/2028/", "tokenDEF")
	}
}
