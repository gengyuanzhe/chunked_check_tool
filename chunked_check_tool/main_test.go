package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
	if err := run(ctx, cfg, os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX"), "", "", &buf); err != nil {
		t.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
