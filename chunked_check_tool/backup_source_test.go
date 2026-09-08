// backup_source_test.go
package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseBackupFileLine(t *testing.T) {
	const bkt = "mybucket"
	cases := []struct {
		name     string
		line     string
		wantKey  string
		wantMP   bool
		wantOffs []int64
		wantErr  string // empty means no error; substring match on Reason
	}{
		{
			name:    "regular two fields",
			line:    "mybucket|k1",
			wantKey: "k1",
			wantMP:  false,
		},
		{
			name:    "regular key with path",
			line:    "mybucket|dir/sub/obj.bin",
			wantKey: "dir/sub/obj.bin",
			wantMP:  false,
		},
		{
			name:     "multipart single part",
			line:     "mybucket|k1|1|0",
			wantKey:  "k1",
			wantMP:   true,
			wantOffs: []int64{0},
		},
		{
			name:     "multipart three parts increasing",
			line:     "mybucket|path/obj|3|0|5242880|10485760",
			wantKey:  "path/obj",
			wantMP:   true,
			wantOffs: []int64{0, 5242880, 10485760},
		},
		{
			name:    "regular bucket mismatch",
			line:    "other|k",
			wantErr: "bucket mismatch",
		},
		{
			name:    "multipart bucket mismatch",
			line:    "other|k|1|0",
			wantErr: "bucket mismatch",
		},
		{
			name:    "single field",
			line:    "justakey",
			wantErr: "too few fields",
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: "empty line",
		},
		{
			name:    "regular empty key",
			line:    "mybucket|",
			wantErr: "empty key",
		},
		// multipart validation errors are delegated to parseListFileLine —
		// spot-check the main rules to lock the delegation in place.
		{
			name:    "partcnt not integer",
			line:    "mybucket|k|x|0",
			wantErr: "partcnt not an integer",
		},
		{
			name:    "partcnt zero",
			line:    "mybucket|k|0",
			wantErr: "partcnt must be >= 1",
		},
		{
			name:    "offset count mismatch",
			line:    "mybucket|k|3|0|5242880",
			wantErr: "offset count != partcnt",
		},
		{
			name:    "offset0 not zero",
			line:    "mybucket|k|1|100",
			wantErr: "offset0 must be 0",
		},
		{
			name:    "offsets not strictly increasing",
			line:    "mybucket|k|2|0|0",
			wantErr: "offsets must be strictly increasing",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task, err := parseBackupFileLine(c.line, bkt, 1)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if task.Key != c.wantKey {
					t.Errorf("Key=%q want %q", task.Key, c.wantKey)
				}
				if task.IsMultipart != c.wantMP {
					t.Errorf("IsMultipart=%v want %v", task.IsMultipart, c.wantMP)
				}
				if len(c.wantOffs) == 0 {
					if task.Offsets != nil {
						t.Errorf("Offsets=%v want nil", task.Offsets)
					}
				} else if !reflect.DeepEqual(task.Offsets, c.wantOffs) {
					t.Errorf("Offsets=%v want %v", task.Offsets, c.wantOffs)
				}
				if task.RawLine != c.line {
					t.Errorf("RawLine=%q want %q", task.RawLine, c.line)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			var mle *MalformedLineError
			if !errors.As(err, &mle) {
				t.Fatalf("expected *MalformedLineError, got %T: %v", err, err)
			}
			if !strings.Contains(mle.Reason, c.wantErr) {
				t.Errorf("err reason = %q, want substring %q", mle.Reason, c.wantErr)
			}
		})
	}
}

func TestBackupSourceRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, err := NewOutput(cfg, "mybucket", false)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewStats()

	// Mixed file: 2 regular + 2 multipart valid, 1 malformed each kind.
	content := "mybucket|reg1\n" +
		"mybucket|mp1|1|0\n" +
		"mybucket|dir/reg2.bin\n" +
		"mybucket|mp2|3|0|5242880|10485760\n" +
		"mybucket|bad|1|100\n" +
		"otherbucket|reg3\n"
	filePath := filepath.Join(dir, "backup.txt")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src := newBackupSource(filePath, "mybucket", out, stats)
	ch := make(chan BackupTask, 16)
	go func() {
		if err := src.Run(context.Background(), ch); err != nil {
			t.Logf("Run returned err: %v", err)
		}
		close(ch)
	}()

	got := []BackupTask{}
	for task := range ch {
		got = append(got, task)
	}
	if len(got) != 4 {
		t.Fatalf("got %d tasks, want 4 (2 malformed lines skipped): %+v", len(got), got)
	}
	// Order preserved; fields from the line shape.
	if got[0].Key != "reg1" || got[0].IsMultipart {
		t.Errorf("task0 = %+v, want reg1 regular", got[0])
	}
	if got[1].Key != "mp1" || !got[1].IsMultipart || !reflect.DeepEqual(got[1].Offsets, []int64{0}) {
		t.Errorf("task1 = %+v, want mp1 multipart [0]", got[1])
	}
	if got[2].Key != "dir/reg2.bin" || got[2].IsMultipart {
		t.Errorf("task2 = %+v, want dir/reg2.bin regular", got[2])
	}
	if got[3].Key != "mp2" || !got[3].IsMultipart || len(got[3].Offsets) != 3 {
		t.Errorf("task3 = %+v, want mp2 multipart 3 offsets", got[3])
	}
	// RawLine preserved verbatim for mismatch output.
	if got[3].RawLine != "mybucket|mp2|3|0|5242880|10485760" {
		t.Errorf("RawLine = %q, want original line", got[3].RawLine)
	}
	// Malformed lines → list_failed + IncrListFailed (2 of them).
	if got := stats.Snapshot().ListFailed; got != 2 {
		t.Errorf("ListFailed=%d want 2", got)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	failedContent, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(failedContent), "mybucket|bad|1|100") {
		t.Errorf("list_failed.txt missing malformed multipart line: %q", failedContent)
	}
	if !strings.Contains(string(failedContent), "otherbucket|reg3") {
		t.Errorf("list_failed.txt missing bucket-mismatch line: %q", failedContent)
	}
}

// TestBackupSourceRunCancel — a cancelled context makes Run return
// ctx.Err() promptly instead of pushing the remaining lines.
func TestBackupSourceRunCancel(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, err := NewOutput(cfg, "mybucket", false)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewStats()

	content := "mybucket|reg1\nmybucket|reg2\n"
	filePath := filepath.Join(dir, "backup.txt")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := newBackupSource(filePath, "mybucket", out, stats)
	ch := make(chan BackupTask, 16)
	err = src.Run(ctx, ch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupSourceRunOpenError(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, err := NewOutput(cfg, "mybucket", false)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	src := newBackupSource(filepath.Join(dir, "nope.txt"), "mybucket", out, NewStats())
	err = src.Run(context.Background(), make(chan BackupTask, 1))
	if err == nil || !strings.Contains(err.Error(), "nope.txt") {
		t.Fatalf("Run err = %v, want open error mentioning path", err)
	}
}
