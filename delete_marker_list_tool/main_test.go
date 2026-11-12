package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_EndToEnd_HappyPath(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.URL,
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "testbucket", ""); err != nil {
		t.Fatalf("run: %v", err)
	}
	// run.log exists with summary
	rl, err := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rl), "=== summary ===") {
		t.Errorf("run.log missing summary: %q", rl)
	}
	if !strings.Contains(string(rl), "sk=***") {
		t.Errorf("run.log should mask sk: %q", rl)
	}
	// delete_markers.txt has the two markers
	dm, err := os.ReadFile(filepath.Join(dir, "out", "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dm), "testbucket\x1fa.txt\x1fdm-a") {
		t.Errorf("missing a.txt marker: %q", dm)
	}
	if !strings.Contains(string(dm), "testbucket\x1fsub/c.txt\x1fdm-c") {
		t.Errorf("missing sub/c.txt marker: %q", dm)
	}
}

func TestRun_RejectsInvalidListType(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - 1.2.3.4:80",
		"ak: a",
		"sk: s",
		"list_type: 5",
		"output_dir: " + filepath.Join(dir, "out"),
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "testbucket", "")
	if err == nil {
		t.Errorf("expected error for list_type=5")
	}
}

func TestRun_BucketDoesNotExist(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.URL,
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	// fake server returns 200 for HEAD on testbucket; use a different bucket name
	// so BucketExists returns false.
	err := run(cfgPath, "nonexistentbucket", "")
	if err == nil {
		t.Errorf("expected error for non-existent bucket")
	}
}
