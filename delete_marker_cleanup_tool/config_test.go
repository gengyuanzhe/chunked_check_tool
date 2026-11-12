package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig_HappyPath(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
scheme: http
ak: myak
sk: mysk
op_concurrency: 4
output_dir: ./out
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpConcurrency != 4 {
		t.Errorf("OpConcurrency=%d want 4", cfg.OpConcurrency)
	}
	if cfg.OutputDirTimestamp != true {
		t.Errorf("OutputDirTimestamp default should be true")
	}
	if cfg.Scheme != "http" {
		t.Errorf("Scheme=%q want http", cfg.Scheme)
	}
	if cfg.OutputDir == "./out" {
		t.Errorf("OutputDir not rewritten with timestamp suffix")
	}
	if !strings.HasPrefix(filepath.Base(cfg.OutputDir), "out_") {
		t.Errorf("OutputDir base=%q want out_<stamp>", filepath.Base(cfg.OutputDir))
	}
}

func TestLoadConfig_OpConcurrencyDefaultsTo8(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpConcurrency != 8 {
		t.Errorf("OpConcurrency=%d want 8 (default)", cfg.OpConcurrency)
	}
}

func TestLoadConfig_RejectsMissingEndpoints(t *testing.T) {
	p := writeConfigFile(t, `ak: a
sk: s
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for missing endpoints")
	}
}

func TestLoadConfig_RejectsMissingAKSK(t *testing.T) {
	p := writeConfigFile(t, `endpoints: ["1.2.3.4:80"]
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for missing ak/sk")
	}
}

func TestLoadConfig_OutputDirTimestampExplicitFalse(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
output_dir: ./fixed
output_dir_timestamp: false
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "./fixed" {
		t.Errorf("OutputDir=%q want ./fixed", cfg.OutputDir)
	}
}

func TestLoadConfig_UnknownFieldsIgnored(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: 2
list_concurrency: 8
is_check: true
multipart_check_mode: 1
`)
	if _, err := LoadConfig(p); err != nil {
		t.Fatalf("unknown fields should be ignored: %v", err)
	}
}

func TestLoadConfig_OpConcurrencyNegativeRejected(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
op_concurrency: -1
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for op_concurrency=-1")
	}
}
