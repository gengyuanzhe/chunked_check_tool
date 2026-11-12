package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRow_ThreeFields(t *testing.T) {
	r, ok := ParseRow("bkt\x1fkey/a\x1fv1", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Bkt != "bkt" || r.Key != "key/a" || r.VersionID != "v1" {
		t.Errorf("got %+v", r)
	}
}

func TestParseRow_InvalidKeysBasenameURLDecodesKey(t *testing.T) {
	r, ok := ParseRow("bkt\x1fa%1Fb\x1fv1", "invalid_keys.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Key != "a\x1fb" {
		t.Errorf("Key=%q want %q", r.Key, "a\x1fb")
	}
}

func TestParseRow_WrongFieldCount(t *testing.T) {
	if _, ok := ParseRow("only\x1ftwo", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 2-field line")
	}
	if _, ok := ParseRow("bkt\x1fk\x1fv\x1fextra", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 4-field line")
	}
}

func TestParseRow_NullVersionIdPassesThrough(t *testing.T) {
	r, ok := ParseRow("bkt\x1fk\x1fnull", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.VersionID != "null" {
		t.Errorf("VersionID=%q want %q", r.VersionID, "null")
	}
}

func TestRunWorkers_ModeCheck_ThreeStates(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fmarker.txt\x1fdm-1",
		"testbucket\x1fregular.txt\x1fv-1",
		"testbucket\x1fgone.txt\x1fv-1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out2 := string(b)
	if !strings.Contains(out2, "testbucket\x1fmarker.txt\x1fdm-1\x1fmarker") {
		t.Errorf("missing marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fregular.txt\x1fv-1\x1fnot_marker") {
		t.Errorf("missing not_marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fgone.txt\x1fv-1\x1fabsent") {
		t.Errorf("missing absent state: %q", out2)
	}
}

func TestRunWorkers_ModeDelete_NoSuchVersionCountsAsSuccess(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fany.txt\x1fv-1",
		"testbucket\x1fany.txt\x1falready-deleted",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d deleted lines, want 2: %q", len(lines), string(b))
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "delete_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("delete_failed.txt should not exist: %v", err)
	}
}
