// backup_source.go — parser for the mixed input line format shared by
// -backup-file and -check-file.
package main

import (
	"fmt"
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

// parseMixedLine parses one line of the mixed regular/multipart input format
// consumed by -backup-file and -check-file. The lines are the outputs of a
// check run: corrupted_objects.txt (regular) and corrupted_mp.txt /
// mp_check_failed.txt / mismatch.txt (multipart, offsets included), so every
// failure file of the check stage is directly re-feedable.
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
func parseMixedLine(line string, expectedBucket string, lineNum int) (BackupTask, error) {
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
		// Same TrimSpace rule as parseListFileLine (minio-go rejects
		// whitespace-only names per call; parse time is the better place).
		if strings.TrimSpace(parts[1]) == "" {
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
