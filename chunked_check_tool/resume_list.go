package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// resumeEntry is one parsed line of a -resume-list file: a prefix and an
// optional continuation token to resume listing from.
type resumeEntry struct {
	prefix string
	token  string
}

// parseResumeListLine parses one line of list_failed.txt written by a prior
// Mode 2 run. Format:
//   - `` (empty line) — root prefix failed on its first page, no token
//   - `prefix` (single field) — a non-root prefix failed on its first page
//   - `prefix\x1ftoken` — a later page failed; token is the cursor to resume from
//
// Both fields arrive URL-escaped (boundary B on the write side); each is
// unescaped exactly once here. A plain prefix without special characters is
// its own escape form, so handwritten lines keep working. An invalid escape
// (stray %XX) errors — that is a mangled file, not tool output.
//
// Empty line is a LEGAL entry: it records a root-prefix failure (prefix="",
// token=""), which happens when the user ran with no -prefix and the very
// first LIST against the bucket failed. Treating it as "empty line" error
// (the old behavior) silently dropped the only resume entry, making the run
// un-resumable. An empty line is therefore parsed as {prefix:"", token:""}.
func parseResumeListLine(line string) (resumeEntry, error) {
	if line == "" {
		// Root prefix failed on the first page: prefix=="", token=="".
		// Legal resume entry — the run had no -prefix and its first LIST
		// against the bucket failed.
		return resumeEntry{prefix: "", token: ""}, nil
	}
	// Whitespace-only lines are errors: WriteListFailed never emits them
	// (it writes the escaped prefix, never padding), so they indicate a
	// manually corrupted file.
	if strings.TrimSpace(line) == "" {
		return resumeEntry{}, fmt.Errorf("whitespace-only line: %q", line)
	}
	prefix, token, err := splitEscaped2(line)
	if err != nil {
		return resumeEntry{}, fmt.Errorf("resume-list line: %w", err)
	}
	return resumeEntry{prefix: prefix, token: token}, nil
}

// splitEscaped2 splits a record-file line into two fields at the first \x1f
// (escaped fields can never contain one) and unescapes each. A line without
// a separator yields (whole-line, "", nil).
func splitEscaped2(line string) (string, string, error) {
	a, b, hasSep := strings.Cut(line, rowSep)
	if !hasSep {
		b = ""
	}
	ua, err := url.QueryUnescape(a)
	if err != nil {
		return "", "", fmt.Errorf("field 1 invalid escape %q: %w", a, err)
	}
	if b == "" {
		return ua, "", nil
	}
	ub, err := url.QueryUnescape(b)
	if err != nil {
		return "", "", fmt.Errorf("field 2 invalid escape %q: %w", b, err)
	}
	return ua, ub, nil
}

// readResumeList reads a -resume-list file line-by-line, parses each line as
// a (prefix, token) pair, and returns the entries. Malformed lines
// (whitespace-only, or an invalid %XX escape) are written to parse_failed.txt
// and bump the parse-failed counter; processing continues.
func readResumeList(path string, out *Output, stats *Stats) ([]resumeEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open resume-list %q: %w", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	const maxLineLen = 1 << 20 // 1 MiB, same as fileSource
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	var entries []resumeEntry
	for scanner.Scan() {
		line := scanner.Text()
		e, perr := parseResumeListLine(line)
		if perr != nil {
			// Only mangled lines reach here in normal operation —
			// list_failed.txt is machine-written with valid escapes; a
			// parse failure means the file was hand-edited or truncated.
			out.WriteParseFailed(line)
			stats.IncrParseFailed()
			continue
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return entries, fmt.Errorf("scan resume-list: %w", err)
	}
	return entries, nil
}
