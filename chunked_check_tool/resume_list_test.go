package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteListFailedTokenFormat — WriteListFailed(prefix, token) writes
// `prefix|token` to list_failed.txt; empty token writes just `prefix`.
// -resume-list parses these lines and feeds (prefix, token) back into BFS.
func TestWriteListFailedTokenFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", false)

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
	want := "data/2026/\ndata/2027/|abc123token\ndata/2028/\n"
	if string(data) != want {
		t.Errorf("list_failed.txt = %q, want %q", string(data), want)
	}
}

// TestWriteListFailedInvalidPrefix — prefix containing '|' violates the
// field-separator contract. Such lines cannot be parsed by -resume-list
// (SplitN("|", 2) would split inside the prefix), so they are routed to
// invalid_keys.txt instead of list_failed.txt.
func TestWriteListFailedInvalidPrefix(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: false}
	o, _ := NewOutput(cfg, "test-bkt", false)

	o.WriteListFailed("data/a|b/", "tokenX")

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	// list_failed.txt must NOT contain the violating prefix.
	listData, _ := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if strings.Contains(string(listData), "data/a|b/") {
		t.Errorf("list_failed.txt should not contain prefix with |: %q", string(listData))
	}
	// invalid_keys.txt must contain it.
	invalidData, _ := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
	if !strings.Contains(string(invalidData), "data/a|b/") {
		t.Errorf("invalid_keys.txt = %q, want substring %q", string(invalidData), "data/a|b/")
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
		{"data/2026/", "data/2026/", ""},
		{"data/2027/|abc123", "data/2027/", "abc123"},
		{"data/2028/|", "data/2028/", ""},
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

// TestParseResumeListLineEmpty — empty lines are rejected (not silently
// treated as empty-prefix entries).
func TestParseResumeListLineEmpty(t *testing.T) {
	for _, line := range []string{"", "   ", "\t"} {
		if _, err := parseResumeListLine(line); err == nil {
			t.Errorf("parseResumeListLine(%q) should error on empty line", line)
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
	o, _ := NewOutput(cfg, "test-bkt", false)

	// Write the resume-list file manually (simulating a prior run's output).
	resumePath := filepath.Join(dir, "list_failed.txt")
	content := "data/2026/\ndata/2027/|tokenABC\ndata/2028/|tokenDEF\n"
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
