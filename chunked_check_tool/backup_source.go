// backup_source.go
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

// BackupTask is the unit of backup work. IsMultipart and Offsets come from
// the input line's shape (2 fields → regular, ≥3 fields → multipart); the
// HEAD ETag in the backup worker is the authoritative type check. RawLine
// carries the original input line so mismatch output preserves it verbatim.
type BackupTask struct {
	Key         string
	RawLine     string
	IsMultipart bool
	Offsets     []int64
}

// parseBackupFileLine parses one line of the backup list file.
//
// Format (mixed, self-describing by field count):
//   - regular:   bkt|key
//   - multipart: bkt|key|partcnt|offset0|offset1|...
//
// Semantic rules:
//   - bkt must equal expectedBucket (both forms)
//   - regular: key must be non-empty
//   - multipart: all parseListFileLine rules (partcnt>=1, offset0==0,
//     strictly increasing, offset count == partcnt) via delegation
func parseBackupFileLine(line string, expectedBucket string, lineNum int) (BackupTask, error) {
	if strings.TrimSpace(line) == "" {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty line"}
	}
	parts := strings.Split(line, "|")
	if len(parts) < 2 {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "too few fields (want bkt|key or bkt|key|partcnt|offsets...)"}
	}
	if parts[0] != expectedBucket {
		return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("bucket mismatch: got %q want %q", parts[0], expectedBucket)}
	}
	if len(parts) == 2 {
		if parts[1] == "" {
			return BackupTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty key"}
		}
		return BackupTask{Key: parts[1], RawLine: line, IsMultipart: false}, nil
	}
	task, err := parseListFileLine(line, expectedBucket, lineNum)
	if err != nil {
		return BackupTask{}, err
	}
	return BackupTask{Key: task.Key, RawLine: line, IsMultipart: true, Offsets: task.Offsets}, nil
}

// backupSource reads a backup list file line-by-line and pushes BackupTasks
// to ch. Malformed lines go to list_failed (same as listFileSource) and
// processing continues. Every line read (valid or malformed) bumps
// IncrReadLine — the cumulative read progress counter for backup mode.
type backupSource struct {
	path   string
	bucket string
	out    *Output
	stats  *Stats
}

func newBackupSource(path, bucket string, out *Output, stats *Stats) *backupSource {
	return &backupSource{path: path, bucket: bucket, out: out, stats: stats}
}

func (s *backupSource) Run(ctx context.Context, ch chan<- BackupTask) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open backup list file %q: %w", s.path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	const maxLineLen = 1 << 20 // 1 MiB, same as listFileSource
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		s.stats.IncrReadLine()
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// scanner.Text() allocates a fresh string per line, so task fields
		// that alias it (Key, RawLine) are safe to retain.
		line := scanner.Text()
		task, err := parseBackupFileLine(line, s.bucket, lineNum)
		if err != nil {
			s.out.WriteListFailed(line)
			s.out.WriteListFailedLog(line, 0, "", "", err)
			s.stats.IncrListFailed()
			continue
		}
		select {
		case ch <- task:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return scanner.Err()
}
