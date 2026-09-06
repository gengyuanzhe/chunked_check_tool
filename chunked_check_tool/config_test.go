package main

import (
	"os"
	"path/filepath"
	"testing"
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
