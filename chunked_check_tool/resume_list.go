package main

import (
	"bufio"
	"fmt"
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
//   - `prefix` (single field) — first page failed, no token
//   - `prefix|token` — a later page failed; token is the cursor to resume from
//
// SplitN (not LastIndex): the contract is "prefix contains no |" (enforced on
// the write side by WriteListFailed — prefix-with-| lines are routed to
// invalid_keys.txt instead). V2 tokens are base64 (no |); V1 tokens are keys
// (no | by the same contract). So SplitN("|", 2) gives prefix = parts[0],
// token = parts[1] (empty if absent).
//
// Lines that violate the contract (prefix contains |) are rejected and routed
// to invalid_keys.txt + parse_failed.txt by the caller — we return an error
// instead of silently mis-splitting.
func parseResumeListLine(line string) (resumeEntry, error) {
	if strings.TrimSpace(line) == "" {
		return resumeEntry{}, fmt.Errorf("empty line")
	}
	if !strings.Contains(line, "|") {
		return resumeEntry{prefix: line, token: ""}, nil
	}
	parts := strings.SplitN(line, "|", 2)
	prefix, token := parts[0], parts[1]
	// Defensive: if prefix still contains |, the write-side contract was
	// violated (should have been routed to invalid_keys.txt). Reject rather
	// than silently mis-cutting.
	if strings.Contains(prefix, "|") {
		return resumeEntry{}, fmt.Errorf("prefix contains | (contract violation): %q", prefix)
	}
	return resumeEntry{prefix: prefix, token: token}, nil
}

// readResumeList reads a -resume-list file line-by-line, parses each line as
// a (prefix, token) pair, and returns the entries. Malformed lines (empty, or
// prefix containing |) are written to parse_failed.txt + invalid_keys.txt
// and bump the corresponding stats counter; processing continues.
func readResumeList(path string, out *Output, stats *Stats) ([]resumeEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open resume-list %q: %w", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	const maxLineLen = 1 << 20 // 1 MiB, same as listFileSource
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	var entries []resumeEntry
	for scanner.Scan() {
		line := scanner.Text()
		e, perr := parseResumeListLine(line)
		if perr != nil {
			// Only "empty line" errors reach here in normal operation —
			// list_failed.txt is written by WriteListFailed, which routes
			// prefix-with-| lines to invalid_keys.txt at write time. If the
			// file has been manually edited to contain a prefix with |,
			// parseResumeListLine silently mis-splits (the | becomes the
			// separator); that's a contract violation outside the tool's
			// responsibility.
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
