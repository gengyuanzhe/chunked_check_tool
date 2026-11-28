// file_source.go — shared line-file reader for the file-input modes
// (-check-file, -backup-file).
package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

// MalformedLineError carries the original line and its 1-based line number
// so the caller can write it to parse_failed for manual fixing.
type MalformedLineError struct {
	Line    string
	LineNum int
	Reason  string
}

func (e *MalformedLineError) Error() string {
	return fmt.Sprintf("line %d: %s: %q", e.LineNum, e.Reason, e.Line)
}

// fileSource reads a line-per-task input file and pushes parsed tasks to ch.
// It is the shared skeleton of the two file-input modes; the only
// mode-specific part is the parse function:
//
//	-check-file:  parseCheckFileLine (mixed regular/multipart)
//	-backup-file: parseMixedLine     (mixed regular/multipart)
//
// Malformed lines are written to parse_failed (with the raw line) and bump
// IncrParseFailed; processing continues. Every line read (valid or
// malformed) bumps IncrReadLine — the cumulative progress counter for the
// file modes.
type fileSource[T any] struct {
	path   string
	bucket string
	out    *Output
	stats  *Stats
	parse  func(line, expectedBucket string, lineNum int) (T, error)
}

func newFileSource[T any](path, bucket string, out *Output, stats *Stats, parse func(string, string, int) (T, error)) *fileSource[T] {
	return &fileSource[T]{path: path, bucket: bucket, out: out, stats: stats, parse: parse}
}

// Run reads the input file line by line and pushes each parsed task to ch,
// blocking while the workers are busy. It returns when the file is
// exhausted.
//
// A malformed line is not an error: it goes to parse_failed and the run
// continues. An error means the file itself could not be read to the end,
// so the lines after that point were never processed. The error names the
// line where it stopped, and the caller must fail the run on it.
func (s *fileSource[T]) Run(ch chan<- T) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open input file %q: %w", s.path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// Allow long lines (default 64KB limit is too small for huge offset lists).
	const maxLineLen = 1 << 20 // 1 MiB
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		s.stats.IncrReadLine()
		// scanner.Text() allocates a fresh string per line, so task fields
		// that alias it (Key, RawLine) are safe to retain.
		line := scanner.Text()
		task, err := s.parse(line, s.bucket, lineNum)
		if err != nil {
			// Parse functions only return MalformedLineError; errors.As is
			// kept so the invariant is explicit (a non-malformed error would
			// still be recorded the same way).
			var mle *MalformedLineError
			if !errors.As(err, &mle) {
				_ = mle
			}
			s.out.WriteParseFailed(line)
			s.out.WriteListFailedLog(line, 0, "", "", err)
			s.stats.IncrParseFailed()
			continue
		}
		ch <- task
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("read input file %q: line %d is longer than the %d-byte limit; it and the lines after it were not processed",
				s.path, lineNum+1, maxLineLen)
		}
		return fmt.Errorf("read input file %q after line %d: %w", s.path, lineNum, err)
	}
	return nil
}
