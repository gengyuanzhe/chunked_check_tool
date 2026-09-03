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
