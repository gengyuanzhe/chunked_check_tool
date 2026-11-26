// backup_source.go — parser for the mixed input line format shared by
// -backup-file and -check-file.
package main

import (
	"fmt"
	"net/url"
	"strings"
)

// BackupTask is the unit of backup work. IsMultipart and Offsets come from
// the input line's shape (2 fields → regular, ≥3 fields → multipart); the
// HEAD ETag in the backup worker is the authoritative type check. RawLine
// carries the original input line so mismatch/failed output preserves it
// verbatim (and stays re-feedable into -backup-file).
type BackupTask struct {
	Key         string
	RawLine     string
	IsMultipart bool
	Offsets     []int64
}

// unescapeKey decodes one record-file server field. A stray %XX escape (a
// hand-mangled line, not tool output — the write side always escapes
// validly) errors so the line lands in parse_failed instead of silently
// mis-decoding.
func unescapeKey(field, line string, lineNum int) (string, error) {
	k, err := url.QueryUnescape(field)
	if err != nil {
		return "", &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("key invalid escape %q: %v", field, err)}
	}
	return k, nil
}

// parseMixedLine parses one line of the mixed regular/multipart input format
// consumed by -backup-file and -check-file. The lines are the outputs of a
// check run: corrupted_objects.txt (regular) and corrupted_mp.txt /
// mp_check_failed.txt / mismatch.txt (multipart, offsets included), so every
// failure file of the check stage is directly re-feedable.
//
// Format (mixed, self-describing by field count; fields separated by \x1f,
// key URL-escaped at the write boundary — plain keys without special
// characters are their own escape form, so handwritten lines keep working):
//   - regular:   bkt\x1fkey
//   - multipart: bkt\x1fkey\x1fpartcnt\x1foffset0\x1foffset1\x1f...
//
// Semantic rules:
//   - bkt must equal expectedBucket (both forms; bucket is CLI-provided
//     and never escaped)
//   - regular: key must be non-empty
//   - multipart: all parseListFileLine rules (partcnt>=1, offset0==0,
//     strictly increasing, offset count == partcnt) via delegation
func parseMixedLine(line string, expectedBucket string, lineNum int) (BackupTask, error) {
	if strings.TrimSpace(line) == "" {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty line"}
	}
	parts := strings.Split(line, rowSep)
	if len(parts) < 2 {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "too few fields (want bkt\\x1fkey or bkt\\x1fkey\\x1fpartcnt\\x1foffsets...)"}
	}
	if parts[0] != expectedBucket {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("bucket mismatch: got %q want %q", parts[0], expectedBucket)}
	}
	if len(parts) == 2 {
		// Reject whitespace-only keys at parse time (the aws SDK does no
		// client-side name validation, so this is our own input hygiene).
		if strings.TrimSpace(parts[1]) == "" {
			return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty key"}
		}
		key, err := unescapeKey(parts[1], line, lineNum)
		if err != nil {
			return BackupTask{}, err
		}
		return BackupTask{Key: key, RawLine: line, IsMultipart: false}, nil
	}
	task, err := parseListFileLine(line, expectedBucket, lineNum)
	if err != nil {
		return BackupTask{}, err
	}
	return BackupTask{Key: task.Key, RawLine: line, IsMultipart: true, Offsets: task.Offsets}, nil
}
