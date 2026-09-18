// check_file_source_test.go
package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestParseCheckFileLine — the mixed format shared with -backup-file: 2
// fields → regular task (no offsets, HEAD decides), ≥3 fields → multipart
// task with the line's offsets. Malformed cases are inherited from
// parseMixedLine/parseListFileLine via delegation.
func TestParseCheckFileLine(t *testing.T) {
	const bkt = "mybucket"
	cases := []struct {
		name     string
		line     string
		wantKey  string
		wantMp   bool
		wantOffs []int64
		wantErr  string
	}{
		{
			name:    "regular line",
			line:    "mybucket|k1",
			wantKey: "k1",
			wantMp:  false,
		},
		{
			name:     "multipart line",
			line:     "mybucket|mp1|2|0|5242880",
			wantKey:  "mp1",
			wantMp:   true,
			wantOffs: []int64{0, 5242880},
		},
		{
			name:    "bucket mismatch regular",
			line:    "other|k1",
			wantErr: "bucket mismatch",
		},
		{
			name:    "bucket mismatch multipart",
			line:    "other|k|1|0",
			wantErr: "bucket mismatch",
		},
		{
			name:    "empty key regular",
			line:    "mybucket|",
			wantErr: "empty key",
		},
		{
			name:    "single field",
			line:    "mybucket",
			wantErr: "too few fields",
		},
		{
			// Delegated multipart validation.
			name:    "offset0 not zero",
			line:    "mybucket|k|1|100",
			wantErr: "offset0 must be 0",
		},
		{
			name:    "offset count mismatch",
			line:    "mybucket|k|3|0|5",
			wantErr: "offset count != partcnt",
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: "empty line",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task, err := parseCheckFileLine(c.line, bkt, 1)
			if c.wantErr != "" {
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
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if task.Key != c.wantKey {
				t.Errorf("Key=%q want %q", task.Key, c.wantKey)
			}
			if task.IsMultipart != c.wantMp {
				t.Errorf("IsMultipart=%v want %v", task.IsMultipart, c.wantMp)
			}
			if len(task.Offsets) != len(c.wantOffs) {
				t.Fatalf("Offsets=%v want %v", task.Offsets, c.wantOffs)
			}
			for i := range c.wantOffs {
				if task.Offsets[i] != c.wantOffs[i] {
					t.Errorf("Offsets[%d]=%d want %d", i, task.Offsets[i], c.wantOffs[i])
				}
			}
			// File-input tasks always HEAD first: ETag/Size come from the
			// HEAD, and the HEAD ETag re-validates the object type.
			if !task.HeadFirst {
				t.Errorf("HeadFirst=false, want true")
			}
			if task.ETag != "" || task.Size != 0 {
				t.Errorf("ETag=%q Size=%d, want empty/0 (filled by HEAD)", task.ETag, task.Size)
			}
		})
	}
}

// TestCheckFileSourceRunEndToEnd — mixed regular/multipart input through the
// generic fileSource: valid lines of both shapes push, malformed lines land
// in parse_failed, all read lines count.
func TestCheckFileSourceRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, err := NewOutput(cfg, "mybucket", FileInputCheckFile)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewStats()

	content := "mybucket|reg1\n" +
		"mybucket|mp1|2|0|5242880\n" +
		"mybucket|bad|1|100\n" +
		"mybucket|reg2\n"
	filePath := filepath.Join(dir, "check.txt")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src := newFileSource[VerifyTask](filePath, "mybucket", out, stats, parseCheckFileLine)
	objCh := make(chan VerifyTask, 16)
	go func() {
		if err := src.Run(context.Background(), objCh); err != nil {
			t.Logf("Run returned err: %v", err)
		}
		close(objCh)
	}()

	got := []VerifyTask{}
	for task := range objCh {
		got = append(got, task)
	}
	if len(got) != 3 {
		t.Fatalf("got %d tasks, want 3 (malformed line skipped): %+v", len(got), got)
	}
	if got[0].Key != "reg1" || got[0].IsMultipart {
		t.Errorf("task0 = %+v, want regular reg1", got[0])
	}
	if got[1].Key != "mp1" || !got[1].IsMultipart || len(got[1].Offsets) != 2 {
		t.Errorf("task1 = %+v, want multipart mp1 with 2 offsets", got[1])
	}
	if got[2].Key != "reg2" || got[2].IsMultipart {
		t.Errorf("task2 = %+v, want regular reg2", got[2])
	}
	if got := stats.Snapshot().ReadLines; got != 4 {
		t.Errorf("ReadLines=%d want 4 (all lines read, malformed included)", got)
	}
	if got := stats.Snapshot().ParseFailed; got != 1 {
		t.Errorf("ParseFailed=%d want 1", got)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("output close: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "parse_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "mybucket|bad|1|100") {
		t.Errorf("parse_failed.txt = %q, want substring %q", string(data), "mybucket|bad|1|100")
	}
}

// TestCheckFileSourceLegacyListFileCompat — a legacy -list-file
// (multipart-only) is a valid -check-file subset: every line parses.
func TestCheckFileSourceLegacyListFileCompat(t *testing.T) {
	line := "mybucket|mp1|3|0|5242880|10485760"
	task, err := parseCheckFileLine(line, "mybucket", 1)
	if err != nil {
		t.Fatalf("legacy list-file line must parse as check-file input: %v", err)
	}
	if !task.IsMultipart || len(task.Offsets) != 3 || !task.HeadFirst {
		t.Errorf("task = %+v, want multipart with 3 offsets and HeadFirst", task)
	}
}
