package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig_DefaultsAndParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
ak: ACCESSKEY
sk: SECRETKEY
list_type: 2
list_concurrency: 4
check_concurrency: 8
output_dir: ./out
is_check: true
is_success_log: true
progress_interval: 50000
obj_ch_capacity: 5000
output_ch_capacity: 2048
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheme != "http" {
		t.Errorf("default scheme = %q, want http", cfg.Scheme)
	}
	if len(cfg.Endpoints) != 2 || cfg.Endpoints[1] != "10.0.0.2:9000" {
		t.Errorf("endpoints = %v", cfg.Endpoints)
	}
	if cfg.ListType != 2 || cfg.ListConcurrency != 4 || cfg.CheckConcurrency != 8 {
		t.Errorf("concurrency fields wrong: %+v", cfg)
	}
	if !cfg.IsSuccessLog || cfg.ProgressInterval != 50000 {
		t.Errorf("bool/int fields wrong: %+v", cfg)
	}
	if cfg.ObjChCapacity != 5000 {
		t.Errorf("obj_ch_capacity = %d, want 5000", cfg.ObjChCapacity)
	}
	if cfg.OutputChCapacity != 2048 {
		t.Errorf("output_ch_capacity = %d, want 2048", cfg.OutputChCapacity)
	}
}

func TestLoadConfig_ChannelCapacityDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: ak
sk: sk
bucket: b
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	// 0 means "not configured" — caller falls back to current defaults
	// (objCh: formula max(check_concurrency×4, 2000); output channels: 1024).
	if cfg.ObjChCapacity != 0 {
		t.Errorf("obj_ch_capacity default = %d, want 0 (unset)", cfg.ObjChCapacity)
	}
	if cfg.OutputChCapacity != 0 {
		t.Errorf("output_ch_capacity default = %d, want 0 (unset)", cfg.OutputChCapacity)
	}
}

func TestLoadConfig_MissingEndpoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("ak: x\nsk: y\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing endpoints")
	}
}

func TestLoadConfig_SchemeHttps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 1.2.3.4:9000\nscheme: https\nak: x\nsk: y\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheme != "https" {
		t.Errorf("scheme = %q, want https", cfg.Scheme)
	}
}

func TestLoadConfig_MultipartCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
is_multipart_segment_check: true
multipart_segment_size: 5242880
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.IsMultipartSegmentCheck {
		t.Errorf("is_multipart_segment_check = false, want true")
	}
	if cfg.MultipartSegmentSize != 5242880 {
		t.Errorf("multipart_segment_size = %d, want 5242880", cfg.MultipartSegmentSize)
	}
}

func TestLoadConfig_BackupBucket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
backup_bucket: backup-target
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackupBucket != "backup-target" {
		t.Errorf("backup_bucket = %q, want %q", cfg.BackupBucket, "backup-target")
	}
}

// TestLoadConfig_BackupBucketDefaultEmpty — no "reasonable" default for the
// backup target; it must be explicit. Backup mode validates non-empty at
// startup (LoadConfig is mode-agnostic because the mode comes from flags).
func TestLoadConfig_BackupBucketDefaultEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackupBucket != "" {
		t.Errorf("backup_bucket default = %q, want empty (unset)", cfg.BackupBucket)
	}
}

// assertStampedOutputDir checks that cfg.OutputDir is base + "_" + a
// YYYYMMDD_HHMMSS stamp (8 digits, underscore, 6 digits).
func assertStampedOutputDir(t *testing.T, cfg *Config, base string) {
	t.Helper()
	stampLen := len(time.Now().Format("20060102_150405"))
	if !strings.HasPrefix(cfg.OutputDir, base+"_") {
		t.Errorf("output_dir = %q, want prefix %q", cfg.OutputDir, base+"_")
		return
	}
	stamp := strings.TrimPrefix(cfg.OutputDir, base+"_")
	if len(stamp) != stampLen {
		t.Errorf("output_dir = %q, stamp %q is not %d chars (YYYYMMDD_HHMMSS)", cfg.OutputDir, stamp, stampLen)
	}
}

// TestLoadConfig_OutputDirTimestampDefaultOn — the key is absent, so the
// default (true) applies: output_dir gets a sibling-suffix stamp
// (./out → ./out_<YYYYMMDD_HHMMSS>) so repeated runs land in separate
// directories instead of appending into the same result files.
func TestLoadConfig_OutputDirTimestampDefaultOn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\noutput_dir: ./out\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.OutputDirTimestamp {
		t.Errorf("output_dir_timestamp default = false, want true")
	}
	assertStampedOutputDir(t, cfg, "./out")
}

// TestLoadConfig_OutputDirTimestampExplicitFalse — an explicit false uses
// output_dir verbatim (fixed dir, resume-friendly append mode). The raw YAML
// value passes through untouched — no path cleaning.
func TestLoadConfig_OutputDirTimestampExplicitFalse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\noutput_dir: ./out\noutput_dir_timestamp: false\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDirTimestamp {
		t.Errorf("output_dir_timestamp = true, want false")
	}
	if cfg.OutputDir != "./out" {
		t.Errorf("output_dir = %q, want %q", cfg.OutputDir, "./out")
	}
}

// TestLoadConfig_OutputDirTimestampTrailingSlash — ./out/ must stamp to
// ./out_<stamp> (trailing separator trimmed), NOT become ./out/_<stamp>
// which would be a subdir inside ./out.
func TestLoadConfig_OutputDirTimestampTrailingSlash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\noutput_dir: ./out/\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	assertStampedOutputDir(t, cfg, "./out")
}

// TestLoadConfig_OutputDirTimestampEmptyDir — no output_dir configured: the
// "." default is stamped too (._<stamp> in the CWD).
func TestLoadConfig_OutputDirTimestampEmptyDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	assertStampedOutputDir(t, cfg, ".")
}

// TestLoadConfig_OutputDirTimestampExplicitTrue — explicit true behaves the
// same as the default (sibling-suffix stamp).
func TestLoadConfig_OutputDirTimestampExplicitTrue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\noutput_dir: /tmp/e2e_out\noutput_dir_timestamp: true\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.OutputDirTimestamp {
		t.Errorf("output_dir_timestamp = false, want true")
	}
	assertStampedOutputDir(t, cfg, "/tmp/e2e_out")
}

// TestLoadConfig_SegmentCheckWithoutSize — is_multipart_segment_check=true
// with multipart_segment_size=0 is a contradictory config: the user asked for
// segment check but provided no segment size. Fail fast at startup rather
// than silently treating every multipart as clean (which would mask bugs).
func TestLoadConfig_SegmentCheckWithoutSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
is_multipart_segment_check: true
multipart_segment_size: 0
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error when is_multipart_segment_check=true but multipart_segment_size=0")
	}
}
