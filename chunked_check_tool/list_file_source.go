// list_file_source.go
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// InputSource produces VerifyTasks and pushes them to objCh until EOF or
// context cancellation. Reserved for future extension (e.g. etag-based
// offset extraction). The S3 list path (Mode 1/2/3) is currently inline in
// main.go; listFileSource is the first InputSource implementation.
type InputSource interface {
	Run(ctx context.Context, objCh chan<- VerifyTask) error
}

// MalformedLineError carries the original line and its 1-based line number
// so the caller can write it to list_failed for resumable debugging.
type MalformedLineError struct {
	Line    string
	LineNum int
	Reason  string
}

func (e *MalformedLineError) Error() string {
	return fmt.Sprintf("line %d: %s: %q", e.LineNum, e.Reason, e.Line)
}

// parseListFileLine parses one line of the list file.
//
// Format: bkt|key|partcnt|offset0|offset1|...
//
// Semantic rules (per spec §3.2):
//   - bkt must equal expectedBucket
//   - partcnt >= 1 and integer
//   - exactly partcnt offsets follow
//   - all offsets >= 0, strictly increasing
//   - offset0 must be 0
//
// On success returns a VerifyTask with IsMultipart=true (list-file tasks are
// always multipart), ETag="" and Size=0 (the file does not carry them).
func parseListFileLine(line string, expectedBucket string, lineNum int) (VerifyTask, error) {
	if strings.TrimSpace(line) == "" {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty line"}
	}
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "too few fields (want bkt|key|partcnt|offsets...)"}
	}
	bkt, key, partcntStr := parts[0], parts[1], parts[2]
	if bkt != expectedBucket {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("bucket mismatch: got %q want %q", bkt, expectedBucket)}
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
	}, nil
}

// listFileSource reads a list file line-by-line and pushes VerifyTasks to
// objCh. Malformed lines are written to list_failed (with line number) and
// bump IncrListFailed; processing continues. The source does NOT bump
// listed_obj/listed_mp counters (per spec §6 — list-file mode bypasses S3
// LIST, so "listed" semantics don't apply; summary shows list_all: 0).
type listFileSource struct {
	path   string
	bucket string
	out    *Output
	stats  *Stats
}

func newListFileSource(path, bucket string, out *Output, stats *Stats) *listFileSource {
	return &listFileSource{path: path, bucket: bucket, out: out, stats: stats}
}

func (s *listFileSource) Run(ctx context.Context, objCh chan<- VerifyTask) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open list file %q: %w", s.path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// Allow long lines (default 64KB limit is too small for huge offset lists).
	const maxLineLen = 1 << 20 // 1 MiB
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Text()
		task, err := parseListFileLine(line, s.bucket, lineNum)
		if err != nil {
			var mle *MalformedLineError
			if errors.As(err, &mle) {
				s.out.WriteListFailed(mle.Line)
				s.out.WriteListFailedLog(mle.Line, 0, "", "", err)
				s.stats.IncrListFailed()
				continue
			}
			// Non-malformed error (shouldn't happen for parseListFileLine).
			s.out.WriteListFailed(line)
			s.out.WriteListFailedLog(line, 0, "", "", err)
			s.stats.IncrListFailed()
			continue
		}
		select {
		case objCh <- task:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return scanner.Err()
}
