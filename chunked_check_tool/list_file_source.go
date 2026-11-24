// list_file_source.go — parser for the legacy -list-file input format.
package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parseListFileLine parses one line of the -list-file input.
//
// Format: bkt\x1fkey\x1fpartcnt\x1foffset0\x1foffset1\x1f... (key
// URL-escaped at the write boundary; plain keys without special characters
// are their own escape form, so handwritten lines keep working).
//
// Semantic rules (per spec §3.2):
//   - bkt must equal expectedBucket (CLI-provided, never escaped)
//   - partcnt >= 1 and integer
//   - exactly partcnt offsets follow
//   - all offsets >= 0, strictly increasing
//   - offset0 must be 0
//
// On success returns a VerifyTask with IsMultipart=true (list-file tasks are
// always multipart), ETag="" and Size=0 (the file does not carry them) and
// HeadFirst=true (the checker HEADs the object to fill them in before probing).
//
// -list-file is the legacy re-check vehicle from the days when list+check
// could not obtain multipart offsets itself; -check-file (parseCheckFileLine)
// is its mixed-format successor and accepts these lines unchanged.
func parseListFileLine(line string, expectedBucket string, lineNum int) (VerifyTask, error) {
	if strings.TrimSpace(line) == "" {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty line"}
	}
	parts := strings.Split(line, rowSep)
	if len(parts) < 3 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "too few fields (want bkt\\x1fkey\\x1fpartcnt\\x1foffsets...)"}
	}
	bkt, key, partcntStr := parts[0], parts[1], parts[2]
	if bkt != expectedBucket {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("bucket mismatch: got %q want %q", bkt, expectedBucket)}
	}
	// TrimSpace rejects whitespace-only keys at parse time: minio-go's
	// CheckValidObjectName used to fail them per call; the aws SDK has no
	// such client check, so this is now our own input hygiene — reject
	// here with a resolvable line number (parse_failed) instead of
	// per-object failure entries.
	if strings.TrimSpace(key) == "" {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty key"}
	}
	key, err := unescapeKey(key, line, lineNum)
	if err != nil {
		return VerifyTask{}, err
	}
	partcnt, err := strconv.Atoi(partcntStr)
	if err != nil {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("partcnt not an integer: %q", partcntStr)}
	}
	if partcnt < 1 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("partcnt must be >= 1, got %d", partcnt)}
	}
	if len(parts) != 3+partcnt {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset count != partcnt: got %d offsets, partcnt=%d", len(parts)-3, partcnt)}
	}
	offs := make([]int64, 0, partcnt)
	for i := 0; i < partcnt; i++ {
		s := parts[3+i]
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset not an integer: %q", s)}
		}
		if v < 0 {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset negative: %d", v)}
		}
		offs = append(offs, v)
	}
	if offs[0] != 0 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset0 must be 0, got %d", offs[0])}
	}
	for i := 1; i < partcnt; i++ {
		if offs[i] <= offs[i-1] {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "offsets must be strictly increasing"}
		}
	}
	return VerifyTask{
		Key:         key,
		IsMultipart: true,
		Offsets:     offs,
		HeadFirst:   true, // list-file lines omit ETag/Size — checker HEADs to fill them in
	}, nil
}
