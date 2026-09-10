package main

import (
	"os"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	f, err := os.CreateTemp("", "cfg-*.yaml")
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString(body); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestLoadConfig_Valid(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80", "2.2.2.2:80"]
scheme: http
ak: accesskey
sk: secretkey
bucket: testbucket
prefix: data/
depth: 3
width: 4
files_per_dir: 10
object_size_min: 1024
object_size_max: 65536
part_size_min: 5242880
part_size_max: 10485760
output_dir: ./out
concurrency: 4
progress_interval: 50
md5_file: md5.txt
use_trailer: true
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("expected ok, got err: %v", err)
	}
	if cfg.Scheme != "http" {
		t.Errorf("Scheme = %q, want http", cfg.Scheme)
	}
	if cfg.OutputDir != "./out" {
		t.Errorf("OutputDir = %q, want ./out", cfg.OutputDir)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("Concurrency = %d, want 4", cfg.Concurrency)
	}
	if cfg.MD5File != "md5.txt" {
		t.Errorf("MD5File = %q, want md5.txt", cfg.MD5File)
	}
	if cfg.Prefix != "data/" {
		t.Errorf("Prefix = %q, want data/", cfg.Prefix)
	}
	if !cfg.UseTrailer {
		t.Errorf("UseTrailer = false, want true")
	}
}

func TestLoadConfig_DefaultsWhenOptional(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 2
width: 3
files_per_dir: 5
object_size_min: 1
object_size_max: 100
part_size_min: 5242880
part_size_max: 5242880
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("expected ok, got err: %v", err)
	}
	if cfg.Scheme != "http" {
		t.Errorf("default Scheme = %q, want http", cfg.Scheme)
	}
	if cfg.OutputDir != "." {
		t.Errorf("default OutputDir = %q, want .", cfg.OutputDir)
	}
	if cfg.Concurrency != 8 {
		t.Errorf("default Concurrency = %d, want 8", cfg.Concurrency)
	}
	if cfg.ProgressInterval != 100 {
		t.Errorf("default ProgressInterval = %d, want 100", cfg.ProgressInterval)
	}
	if cfg.MD5File != "md5.txt" {
		t.Errorf("default MD5File = %q, want md5.txt", cfg.MD5File)
	}
	if cfg.Prefix != "" {
		t.Errorf("default Prefix = %q, want empty", cfg.Prefix)
	}
}

func TestLoadConfig_MissingEndpoints(t *testing.T) {
	path := writeCfg(t, `ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 1
object_size_max: 1
part_size_min: 5242880
part_size_max: 5242880
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for missing endpoints, got nil")
	}
}

func TestLoadConfig_MissingCreds(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80"]
ak: ""
sk: ""
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 1
object_size_max: 1
part_size_min: 5242880
part_size_max: 5242880
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for empty ak/sk, got nil")
	}
}

func TestLoadConfig_PartSizeMinBelow5MiB(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 1
object_size_max: 1
part_size_min: 1048576
part_size_max: 5242880
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for part_size_min < 5MiB, got nil")
	}
}

func TestLoadConfig_SizeMaxLessThanMin(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 1000
object_size_max: 100
part_size_min: 5242880
part_size_max: 5242880
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for object_size_max < min, got nil")
	}
}

func TestLoadConfig_MultipartEndpointPattern_TooShort(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80", "2.2.2.2:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 10485760
object_size_max: 10485760
part_size_min: 5242880
part_size_max: 5242880
multipart_endpoint_pattern: [0, 0]
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for pattern len < 3, got nil")
	}
}

func TestLoadConfig_MultipartPattern_ObjectSizeNotExceedPartSizeMax(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80", "2.2.2.2:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 5242880
object_size_max: 5242880
part_size_min: 5242880
part_size_max: 5242880
multipart_endpoint_pattern: [0, 0, 1, 0, 1]
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for object_size_min <= part_size_max with pattern set, got nil")
	}
}

func TestLoadConfig_MultipartEndpointPattern_EndpointOutOfRange(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80", "2.2.2.2:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 10485760
object_size_max: 10485760
part_size_min: 5242880
part_size_max: 5242880
multipart_endpoint_pattern: [0, 0, 2, 0, 1]
`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for pattern endpoint idx >= len(endpoints), got nil")
	}
}

func TestLoadConfig_MultipartEndpointPattern_Valid(t *testing.T) {
	path := writeCfg(t, `endpoints: ["1.1.1.1:80", "2.2.2.2:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 1
object_size_min: 10485760
object_size_max: 10485760
part_size_min: 5242880
part_size_max: 5242880
multipart_endpoint_pattern: [0, 0, 1, 0, 1]
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("expected ok, got err: %v", err)
	}
	want := []int{0, 0, 1, 0, 1}
	if len(cfg.MultipartEndpointPattern) != len(want) {
		t.Fatalf("pattern len = %d, want %d", len(cfg.MultipartEndpointPattern), len(want))
	}
	for i, v := range cfg.MultipartEndpointPattern {
		if v != want[i] {
			t.Errorf("pattern[%d] = %d, want %d", i, v, want[i])
		}
	}
}

func TestLoadConfig_InvalidTreeParams(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
	}{
		{"depth_zero", `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 0
width: 2
files_per_dir: 1
object_size_min: 1
object_size_max: 1
part_size_min: 5242880
part_size_max: 5242880
`},
		{"width_one", `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 1
files_per_dir: 1
object_size_min: 1
object_size_max: 1
part_size_min: 5242880
part_size_max: 5242880
`},
		{"files_per_dir_zero", `endpoints: ["1.1.1.1:80"]
ak: a
sk: s
bucket: b
depth: 1
width: 2
files_per_dir: 0
object_size_min: 1
object_size_max: 1
part_size_min: 5242880
part_size_max: 5242880
`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCfg(t, tc.cfg)
			if _, err := LoadConfig(path); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
