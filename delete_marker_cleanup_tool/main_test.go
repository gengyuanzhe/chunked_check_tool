package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_ModeCheck_EndToEnd(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fmarker.txt\x1fdm-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "check", inputPath); err != nil {
		t.Fatalf("run: %v", err)
	}
	rl, _ := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if !strings.Contains(string(rl), "=== summary ===") {
		t.Errorf("run.log missing summary: %q", rl)
	}
	if !strings.Contains(string(rl), "sk=***") {
		t.Errorf("run.log should mask sk: %q", rl)
	}
	if !strings.Contains(string(rl), "mode=check") {
		t.Errorf("run.log missing mode=check: %q", rl)
	}
	cr, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if !strings.Contains(string(cr), "testbucket\x1fmarker.txt\x1fdm-1\x1f\x1fmarker") {
		t.Errorf("missing marker result: %q", cr)
	}
}

func TestRun_ModeDelete_EndToEnd(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fany.txt\x1fv-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	if err := run(cfgPath, "delete", inputPath); err != nil {
		t.Fatalf("run: %v", err)
	}
	d, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if !strings.Contains(string(d), "testbucket\x1fany.txt\x1fv-1") {
		t.Errorf("missing deleted row: %q", d)
	}
}

func TestRun_InvalidMode(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints: [\"1.2.3.4:80\"]",
		"ak: a",
		"sk: s",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "audit", "/tmp/nonexistent")
	if err == nil {
		t.Errorf("expected error for invalid mode 'audit'")
	}
}

func TestRun_BucketPreflight_TransportErrorExits(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fmarker.txt\x1fdm-1"}
	os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644)
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - 127.0.0.1:1",
		"scheme: http",
		"ak: a",
		"sk: s",
		"dial_timeout: 1",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "check", inputPath)
	if err == nil {
		t.Errorf("expected transport error on preflight")
	}
}

func TestRun_BucketPreflight_FirstRowParseFailureExits(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	os.WriteFile(inputPath, []byte("only\x1ftwo-fields\n"), 0644)
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "check", inputPath)
	if err == nil {
		t.Errorf("expected error for first-row parse failure")
	}
}
