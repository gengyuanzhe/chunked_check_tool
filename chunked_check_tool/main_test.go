package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunIntegration_FakeS3 documents the intended integration surface.
// A real run needs a live minio/S3 because run() builds minio clients via
// newWorker -> NewMinioClient, which cannot be injected with a FakeS3
// without refactoring the worker factory. The test is therefore skipped
// in CI; run it manually against a real endpoint (see TestRunEndToEnd_smoke).
func TestRunIntegration_FakeS3(t *testing.T) {
	t.Skip("integration test requires real minio; see manual test plan")
}

// TestRunEndToEnd_smoke drives the full run() against a real endpoint
// specified via S3_ENDPOINT/S3_AK/S3_SK/S3_BUCKET env vars. It is skipped
// when S3_ENDPOINT is unset.
func TestRunEndToEnd_smoke(t *testing.T) {
	if os.Getenv("S3_ENDPOINT") == "" {
		t.Skip("set S3_ENDPOINT to run smoke test")
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - " + os.Getenv("S3_ENDPOINT") +
		"\nscheme: " + envOr("S3_SCHEME", "http") +
		"\nak: " + os.Getenv("S3_AK") +
		"\nsk: " + os.Getenv("S3_SK") +
		"\nlist_type: " + envOr("S3_LIST_TYPE", "2") +
		"\nlist_api_version: " + envOr("S3_LIST_API_VERSION", "2") +
		"\nlist_concurrency: 2\ncheck_concurrency: 2\noutput_dir: " + dir +
		"\nis_check: true\nis_success_log: false\nprogress_interval: 1000\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var buf bytes.Buffer
	if err := run(ctx, cfg, os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX"), "", "", "", &buf); err != nil {
		t.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestRunListFileDispatch exercises the list-file dispatch branch in run()
// (main.go: close→wait→summary→return). It uses a list file containing a
// single malformed line (bucket mismatch), which parseListFileLine rejects
// before any S3 GET is issued, so no real endpoint is contacted. The
// endpoint 127.0.0.1:1 is a placeholder — minio client construction is lazy
// (no dial), and check workers receive no tasks because objCh is closed
// immediately after the list-file source rejects the only line.
func TestRunListFileDispatch(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - 127.0.0.1:1\n" +
		"scheme: http\n" +
		"ak: test\n" +
		"sk: test\n" +
		"list_type: 2\n" +
		"list_api_version: 2\n" +
		"list_concurrency: 2\n" +
		"check_concurrency: 2\n" +
		"output_dir: " + dir + "\n" +
		"is_check: true\n" +
		"is_success_log: false\n" +
		"progress_interval: 1000\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	// List file: one malformed line (bucket mismatch). parseListFileLine
	// rejects this before pushing to objCh, so no GET is ever issued.
	listPath := filepath.Join(dir, "list.txt")
	listContent := "wrongbucket|k|1|0\n"
	if err := os.WriteFile(listPath, []byte(listContent), 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err = run(context.Background(), cfg, "mybucket", "", "", listPath, "", &buf)
	if err != nil {
		t.Fatalf("run returned err: %v (want nil — list-file mode returns nil on completion)", err)
	}

	out := buf.String()
	if !strings.Contains(out, "list_failed: 1") {
		t.Errorf("stdout = %q, want substring %q", out, "list_failed: 1")
	}
	// list-file summary reports input consumption instead of list_all (no
	// S3 LIST happens): the one malformed line was still read.
	if !strings.Contains(out, "read: 1") {
		t.Errorf("stdout = %q, want substring %q (list-file summary reports read)", out, "read: 1")
	}
	if strings.Contains(out, "list_all:") {
		t.Errorf("stdout = %q should not contain list_all (all-zero noise in list-file mode)", out)
	}

	// The malformed line must be persisted to list_failed.txt for resumable
	// debugging. cfg.OutputDir carries the timestamp suffix (default-on
	// output_dir_timestamp), which run() created.
	listFailedPath := filepath.Join(cfg.OutputDir, "list_failed.txt")
	content, err := os.ReadFile(listFailedPath)
	if err != nil {
		t.Fatalf("read list_failed.txt: %v", err)
	}
	if !strings.Contains(string(content), "wrongbucket|k|1|0") {
		t.Errorf("list_failed.txt = %q, want substring %q", string(content), "wrongbucket|k|1|0")
	}
}

// TestRunListFileMultipartResultsEndToEnd — list-file mode must land
// verification results in the multipart result files even with
// is_multipart_segment_check=false (offsets come from the list file).
// OwnerID is empty for list-file tasks, so results route to _unknown/.
func TestRunListFileMultipartResultsEndToEnd(t *testing.T) {
	f := newFakeBackupS3Server(t)
	f.objects["mp1"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte(corruptBody)}
	f.objects["mpclean"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte("clean multipart content")}
	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:             []string{host},
		Scheme:                "http",
		AK:                    "t",
		SK:                    "t",
		OutputDir:             dir,
		IsCheck:               true,
		IsMultipartSuccessLog: true,
		ProgressInterval:      1000,
		CheckConcurrency:      2,
		ListConcurrency:       2,
		ListAPIVersion:        2,
	}
	listPath := filepath.Join(dir, "list.txt")
	content := "srcbucket|mp1|1|0\n" +
		"srcbucket|mpclean|1|0\n" +
		"srcbucket|bad|1|100\n"
	if err := os.WriteFile(listPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), cfg, "srcbucket", "", "", listPath, "", &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}

	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_mp.txt"), "srcbucket|mp1\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_mp.txt"), "srcbucket|mpclean\n")
	assertFileContent(t, dir, "list_failed.txt", "srcbucket|bad|1|100\n")

	out := buf.String()
	if !strings.Contains(out, "corrupt_mp: 1") || !strings.Contains(out, "ok_mp: 1") {
		t.Errorf("summary missing mp counts\nfull:\n%s", out)
	}
}

// TestRunListFileProgressLine — the list-file progress line must fire
// through the full run() wiring (mode-aware printer + read/total counters),
// not just direct MaybePrint calls: interval=1 fires one line per task.
func TestRunListFileProgressLine(t *testing.T) {
	f := newFakeBackupS3Server(t)
	f.objects["mp1"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte(corruptBody)}
	f.objects["mpclean"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte("clean multipart content")}
	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:        []string{host},
		Scheme:           "http",
		AK:               "t",
		SK:               "t",
		OutputDir:        dir,
		IsCheck:          true,
		ProgressInterval: 1,
		CheckConcurrency: 2,
		ListConcurrency:  2,
		ListAPIVersion:   2,
	}
	listPath := filepath.Join(dir, "list.txt")
	content := "srcbucket|mp1|1|0\n" +
		"srcbucket|mpclean|1|0\n"
	if err := os.WriteFile(listPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), cfg, "srcbucket", "", "", listPath, "", &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "[progress] read=") {
		t.Errorf("no list-file progress line in output:\n%s", out)
	}
	// A task is only handed to a worker after its line was read, so the
	// progress line following the second task shows read=2.
	if !strings.Contains(out, "read=2 ") {
		t.Errorf("progress output missing read=2:\n%s", out)
	}
	for _, want := range []string{"ok_mp=", "corrupt_mp=", "get_calls=", "(checked="} {
		if !strings.Contains(out, want) {
			t.Errorf("list-file progress line missing %q\nfull:\n%s", want, out)
		}
	}
	if strings.Contains(out, "list_all=") || strings.Contains(out, "list_calls=") {
		t.Errorf("list-file progress should not contain list_* noise:\n%s", out)
	}
}
