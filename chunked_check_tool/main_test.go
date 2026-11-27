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
// A real run needs a live S3 because run() builds s3 clients via
// newWorker -> NewAWSClient, which cannot be injected with a FakeS3
// without refactoring the worker factory. The test is therefore skipped
// in CI; run it manually against a real endpoint (see TestRunEndToEnd_smoke).
func TestRunIntegration_FakeS3(t *testing.T) {
	t.Skip("integration test requires real S3; see manual test plan")
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
		"\nlist_concurrency: 2\ncheck_concurrency: 2\nmax_keys: 1000\noutput_dir: " + dir +
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
	if err := run(ctx, ctx, cfg, os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX"), "", "", "", "", "", &buf); err != nil {
		t.Fatal(err)
	}
}

// TestRunCheckFileDispatch exercises the -check-file dispatch branch in
// run(): a single malformed line (bucket mismatch) is rejected before any
// S3 GET.
func TestRunCheckFileDispatch(t *testing.T) {
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
		"progress_interval: 1000\nmax_keys: 1000\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	checkPath := filepath.Join(dir, "check.txt")
	if err := os.WriteFile(checkPath, []byte("wrongbucket|k\n"), 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	err = run(context.Background(), context.Background(), cfg, "mybucket", "", "", checkPath, "", "", "", &buf)
	if err != nil {
		t.Fatalf("run returned err: %v (want nil)", err)
	}

	out := buf.String()
	for _, want := range []string{"parse_failed: 1", "read: 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want substring %q", out, want)
		}
	}
	if strings.Contains(out, "list_all:") {
		t.Errorf("stdout = %q should not contain list_all (all-zero noise in check-file mode)", out)
	}
	parseFailedPath := filepath.Join(cfg.OutputDir, "parse_failed.txt")
	content, err := os.ReadFile(parseFailedPath)
	if err != nil {
		t.Fatalf("read parse_failed.txt: %v", err)
	}
	if !strings.Contains(string(content), "wrongbucket|k") {
		t.Errorf("parse_failed.txt = %q, want substring %q", string(content), "wrongbucket|k")
	}
}

// TestRunCheckFileEndToEnd — -check-file against the fake S3 server: mixed
// regular/multipart input (the failure-file shapes of a prior run), with
// multipart_check_mode=0 proving the mode's irrelevance for file inputs
// (offsets come from the lines), and both success-log switches left off
// proving check-file records clean objects regardless of them. Covers both
// type-drift directions: line multipart + HEAD regular (probed as regular →
// ok_obj) and line regular + HEAD multipart (no offsets → mp.txt unverified).
func TestRunCheckFileEndToEnd(t *testing.T) {
	f := newFakeBackupS3Server(t)
	f.objects["regcorrupt"] = fakeBackupObject{ETag: md5Hex([]byte(corruptBody)), Content: []byte(corruptBody)}
	f.objects["mpcorrupt"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte(corruptBody)}
	f.objects["mpclean"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-1", Content: []byte("clean multipart content")}
	f.objects["driftreg"] = fakeBackupObject{ETag: md5Hex([]byte("now a regular object")), Content: []byte("now a regular object")}
	f.objects["driftmp"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-2", Content: []byte("now a multipart object")}
	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:          []string{host},
		Scheme:             "http",
		AK:                 "t",
		SK:                 "t",
		OutputDir:          dir,
		IsCheck:            true,
		MultipartCheckMode: MultipartCheckModeOff,
		ProgressInterval:   1000,
		CheckConcurrency:   2,
		ListConcurrency:    2,
		ListAPIVersion:     2,
	}
	checkPath := filepath.Join(dir, "check.txt")
	content := "srcbucket\x1fregcorrupt\n" +
		"srcbucket\x1fmpcorrupt\x1f1\x1f0\n" +
		"srcbucket\x1fmpclean\x1f1\x1f0\n" +
		"srcbucket\x1fdriftreg\x1f1\x1f0\n" +
		"srcbucket\x1fdriftmp\n" +
		"srcbucket\x1fbad\x1f1\x1f100\n"
	if err := os.WriteFile(checkPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), context.Background(), cfg, "srcbucket", "", "", checkPath, "", "", "", &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}

	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_objects.txt"), "srcbucket\x1fregcorrupt\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "corrupted_mp.txt"), "srcbucket\x1fmpcorrupt\x1f1\x1f0\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_mp.txt"), "srcbucket\x1fmpclean\x1f1\x1f0\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "ok_objects.txt"), "srcbucket\x1fdriftreg\n")
	assertFileContent(t, dir, filepath.Join("_unknown", "mp.txt"), "srcbucket\x1fdriftmp\n")
	assertFileContent(t, dir, "parse_failed.txt", "srcbucket\x1fbad\x1f1\x1f100\n")

	out := buf.String()
	for _, want := range []string{
		"read: 6",
		"ok_obj: 1 corrupt_obj: 1 ok_mp: 1 corrupt_mp: 1 check_failed: 0 mp_check_failed: 0",
		"parse_failed: 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q\nfull:\n%s", want, out)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestRunCheckFileProgressLine — the check-file progress line must fire
// through the full run() wiring (mode-aware printer + read/total counters),
// not just direct MaybePrint calls: interval=1 fires one line per task.
func TestRunCheckFileProgressLine(t *testing.T) {
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
	checkPath := filepath.Join(dir, "check.txt")
	content := "srcbucket\x1fmp1\x1f1\x1f0\n" +
		"srcbucket\x1fmpclean\x1f1\x1f0\n"
	if err := os.WriteFile(checkPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), context.Background(), cfg, "srcbucket", "", "", checkPath, "", "", "", &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "[progress] read=") {
		t.Errorf("no check-file progress line in output:\n%s", out)
	}
	// A task is only handed to a worker after its line was read, so the
	// progress line following the second task shows read=2.
	if !strings.Contains(out, "read=2 ") {
		t.Errorf("progress output missing read=2:\n%s", out)
	}
	for _, want := range []string{"ok_obj=", "corrupt_obj=", "ok_mp=", "corrupt_mp=", "check_failed=", "mp_check_failed=", "get_calls=", "(checked="} {
		if !strings.Contains(out, want) {
			t.Errorf("check-file progress line missing %q\nfull:\n%s", want, out)
		}
	}
	if strings.Contains(out, "list_all=") || strings.Contains(out, "list_calls=") {
		t.Errorf("check-file progress should not contain list_* noise:\n%s", out)
	}
}
