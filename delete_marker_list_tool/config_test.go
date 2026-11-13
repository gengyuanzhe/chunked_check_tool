package main

import (
	"os"
	"path/filepath"
	"strconv"
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
list_type: 2
list_concurrency: 4
output_dir: ./out
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListType != 2 {
		t.Errorf("ListType=%d want 2", cfg.ListType)
	}
	if cfg.ListConcurrency != 4 {
		t.Errorf("ListConcurrency=%d want 4", cfg.ListConcurrency)
	}
	if cfg.OutputDirTimestamp != true {
		t.Errorf("OutputDirTimestamp default should be true")
	}
	if cfg.Scheme != "http" {
		t.Errorf("Scheme=%q want http", cfg.Scheme)
	}
	// timestamp suffix applied to OutputDir
	if cfg.OutputDir == "./out" {
		t.Errorf("OutputDir not rewritten with timestamp suffix")
	}
	if !strings.HasPrefix(filepath.Base(cfg.OutputDir), "out_") {
		t.Errorf("OutputDir base=%q want out_<stamp>", filepath.Base(cfg.OutputDir))
	}
}

func TestLoadConfig_RejectsInvalidListType(t *testing.T) {
	for _, v := range []int{0, 3, -1} {
		body := "endpoints: [\"1.2.3.4:80\"]\nak: a\nsk: s\nlist_type: " + strconv.Itoa(v) + "\n"
		p := writeConfigFile(t, body)
		_, err := LoadConfig(p)
		if err == nil {
			t.Errorf("list_type=%d: expected error, got nil", v)
		}
	}
}

func TestLoadConfig_OutputDirTimestampSiblingSuffix(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: 2
output_dir: ./out/
output_dir_timestamp: true
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(cfg.OutputDir)
	// ./out/ → ./out_<stamp> (not ./out/_<stamp>)
	if filepath.Base(filepath.Dir(cfg.OutputDir)) != "." && filepath.Base(filepath.Dir(cfg.OutputDir)) != "out" {
		// On some platforms filepath.Dir("./out_<stamp>") = "."; accept either way.
	}
	if base == "out" || !strings.HasPrefix(base, "out_") {
		t.Errorf("OutputDir base=%q want out_<stamp>", base)
	}
}

func TestLoadConfig_OutputDirTimestampExplicitFalse(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: 2
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
	// 复用 chunked_check_tool 的 config.yaml 风格——本工具只读自己认识的字段。
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: 2
unknown_field: whatever
is_check: true
multipart_check_mode: 1
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("unknown fields should be ignored: %v", err)
	}
	if cfg.ListType != 2 {
		t.Errorf("ListType=%d want 2", cfg.ListType)
	}
}

func TestLoadConfig_LatencyWindowDefault60(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
scheme: http
ak: a
sk: s
list_type: 2
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LatencyWindowSec != 60 {
		t.Errorf("LatencyWindowSec default=%d, want 60", cfg.LatencyWindowSec)
	}
}

func TestLoadConfig_LatencyWindowExplicitAndNegative(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
scheme: http
ak: a
sk: s
list_type: 2
latency_window_sec: 30
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LatencyWindowSec != 30 {
		t.Errorf("LatencyWindowSec=%d, want 30", cfg.LatencyWindowSec)
	}

	pBad := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
scheme: http
ak: a
sk: s
list_type: 2
latency_window_sec: -1
`)
	if _, err := LoadConfig(pBad); err == nil {
		t.Errorf("expected error for latency_window_sec=-1")
	}
}
