// list_file_source_test.go
package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseListFileLine(t *testing.T) {
	const bkt = "mybucket"
	cases := []struct {
		name     string
		line     string
		wantKey  string
		wantOffs []int64
		wantErr  string // empty means no error; substring match on err.Error()
	}{
		{
			name:     "single part",
			line:     "mybucket|k1|1|0",
			wantKey:  "k1",
			wantOffs: []int64{0},
		},
		{
			name:     "three parts increasing",
			line:     "mybucket|path/obj|3|0|5242880|10485760",
			wantKey:  "path/obj",
			wantOffs: []int64{0, 5242880, 10485760},
		},
		{
			name:    "bucket mismatch",
			line:    "other|k|1|0",
			wantErr: "bucket mismatch",
		},
		{
			name:    "too few fields",
			line:    "mybucket|k",
			wantErr: "too few fields",
		},
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
			name:    "offset not integer",
			line:    "mybucket|k|2|0|abc",
			wantErr: "offset not an integer",
		},
		{
			name:    "offsets not strictly increasing",
			line:    "mybucket|k|3|0|5242880|5242880",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "offsets decreasing",
			line:    "mybucket|k|3|0|5242880|100",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "negative offset",
			line:    "mybucket|k|2|0|-5",
			wantErr: "offset negative",
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: "empty line",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseListFileLine(c.line, bkt, 1)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
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

func TestParseListFileLineSuccessFields(t *testing.T) {
	task, err := parseListFileLine("mybucket|path/obj|3|0|5242880|10485760", "mybucket", 42)
	if err != nil {
		t.Fatal(err)
	}
	if task.Key != "path/obj" {
		t.Errorf("Key=%q want path/obj", task.Key)
	}
	if !task.IsMultipart {
		t.Errorf("IsMultipart=false, want true (list-file is always multipart)")
	}
	if len(task.Offsets) != 3 {
		t.Errorf("len(Offsets)=%d want 3", len(task.Offsets))
	}
	if task.Offsets[2] != 10485760 {
		t.Errorf("Offsets[2]=%d want 10485760", task.Offsets[2])
	}
	// ETag/Size empty for list-file tasks (caller doesn't know them).
	if task.ETag != "" || task.Size != 0 {
		t.Errorf("ETag=%q Size=%d, want empty/0", task.ETag, task.Size)
	}
}

func TestListFileSourceRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, err := NewOutput(cfg, "mybucket")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	stats := NewStats()

	// File: 2 valid lines + 1 malformed (offset0 != 0) + 1 valid.
	content := "mybucket|k1|1|0\n" +
		"mybucket|k2|3|0|5242880|10485760\n" +
		"mybucket|bad|1|100\n" +
		"mybucket|k3|2|0|9999\n"
	filePath := filepath.Join(dir, "list.txt")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src := newListFileSource(filePath, "mybucket", out, stats)
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
	if got[0].Key != "k1" || got[1].Key != "k2" || got[2].Key != "k3" {
		t.Errorf("keys in wrong order: %+v", got)
	}
	// list-file source must NOT bump listed counters (per spec §6).
	if got := stats.Snapshot().ListedMp; got != 0 {
		t.Errorf("ListedMp=%d want 0 (list-file source does not bump listed)", got)
	}
	if got := stats.Snapshot().ListedObjects; got != 0 {
		t.Errorf("ListedObjects=%d want 0", got)
	}
	// Malformed line → list_failed + IncrListFailed.
	if got := stats.Snapshot().ListFailed; got != 1 {
		t.Errorf("ListFailed=%d want 1 (one malformed line)", got)
	}
}
