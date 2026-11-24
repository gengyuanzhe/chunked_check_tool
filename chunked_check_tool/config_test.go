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
max_keys: 1000
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
max_keys: 1000
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
	content := []byte("endpoints:\n  - 1.2.3.4:9000\nscheme: https\nak: x\nsk: y\nmax_keys: 1000\n")
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

func TestLoadConfig_MultipartCheckMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
max_keys: 1000
multipart_check_mode: 2
multipart_segment_size: 5242880
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MultipartCheckMode != MultipartCheckModeSegment {
		t.Errorf("multipart_check_mode = %d, want %d (segment)", cfg.MultipartCheckMode, MultipartCheckModeSegment)
	}
	if cfg.MultipartSegmentSize != 5242880 {
		t.Errorf("multipart_segment_size = %d, want 5242880", cfg.MultipartSegmentSize)
	}
}

// TestLoadConfig_MultipartCheckModeDefault — an absent multipart_check_mode
// must default to 0 (off): the yaml zero value IS the off value, so no
// LoadConfig defaulting is needed (and none must be added).
func TestLoadConfig_MultipartCheckModeDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MultipartCheckMode != MultipartCheckModeOff {
		t.Errorf("multipart_check_mode = %d, want %d (off)", cfg.MultipartCheckMode, MultipartCheckModeOff)
	}
}

// TestLoadConfig_MultipartCheckModeInvalid — values outside 0/1/2 must fail
// at startup: silently treating a typo like 3 as "off" would skip multipart
// checking without the operator noticing.
func TestLoadConfig_MultipartCheckModeInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmultipart_check_mode: 3\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error when multipart_check_mode=3")
	}
}

// TestLoadConfig_NodeIsolateThreshold — absent → default 3; explicit values
// preserved; negative rejected.
func TestLoadConfig_NodeIsolateThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeIsolateThreshold != 3 {
		t.Errorf("default node_isolate_threshold = %d, want 3", cfg.NodeIsolateThreshold)
	}

	path2 := filepath.Join(dir, "cfg1.yaml")
	if err := os.WriteFile(path2, []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\nnode_isolate_threshold: 1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(path2)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeIsolateThreshold != 1 {
		t.Errorf("explicit node_isolate_threshold = %d, want 1 (legacy immediate isolation)", cfg.NodeIsolateThreshold)
	}

	path3 := filepath.Join(dir, "cfg2.yaml")
	if err := os.WriteFile(path3, []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\nnode_isolate_threshold: -1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path3); err == nil {
		t.Fatal("expected error for negative node_isolate_threshold")
	}
}

// TestLoadConfig_LegacySegmentCheckField — the retired is_multipart_segment_check
// field must not be silently ignored: an old config would then run with
// mode=0 (off) and report every multipart as unverified. Fail loudly with a
// migration hint instead.
func TestLoadConfig_LegacySegmentCheckField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
max_keys: 1000
is_multipart_segment_check: true
multipart_segment_size: 5242880
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for retired is_multipart_segment_check field")
	}
	if !strings.Contains(err.Error(), "multipart_check_mode") {
		t.Errorf("error should mention multipart_check_mode, got: %v", err)
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
max_keys: 1000
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\n")
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\noutput_dir: ./out\n")
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\noutput_dir: ./out\noutput_dir_timestamp: false\n")
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\noutput_dir: ./out/\n")
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\n")
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
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\noutput_dir: /tmp/e2e_out\noutput_dir_timestamp: true\n")
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

// TestLoadConfig_BackupOutputDir — a dedicated output directory for
// -backup-file mode, kept separate from output_dir so a single config can
// drive both list/check and backup runs without their result files mixing.
// Required when in backup mode (main.go enforces); ignored otherwise.
func TestLoadConfig_BackupOutputDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
max_keys: 1000
output_dir: ./out
backup_output_dir: ./backup_out
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	// output_dir_timestamp defaults to true, so backup_output_dir also gets
	// the stamp — same stamp as output_dir so a run lands in a matched pair.
	if !strings.HasPrefix(cfg.BackupOutputDir, "./backup_out_") {
		t.Errorf("backup_output_dir = %q, want prefix %q", cfg.BackupOutputDir, "./backup_out_")
	}
	outputStamp := strings.TrimPrefix(cfg.OutputDir, "./out_")
	backupStamp := strings.TrimPrefix(cfg.BackupOutputDir, "./backup_out_")
	if outputStamp != backupStamp {
		t.Errorf("output_dir stamp %q != backup_output_dir stamp %q (should share one timestamp)", outputStamp, backupStamp)
	}
}

// TestLoadConfig_BackupOutputDirTimestampOff — with output_dir_timestamp
// explicitly false, backup_output_dir is used verbatim (no stamp). Resume-
// friendly for backup retries that need to append into a fixed dir.
func TestLoadConfig_BackupOutputDirTimestampOff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
max_keys: 1000
output_dir: ./out
backup_output_dir: ./backup_out
output_dir_timestamp: false
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackupOutputDir != "./backup_out" {
		t.Errorf("backup_output_dir = %q, want %q verbatim", cfg.BackupOutputDir, "./backup_out")
	}
	if cfg.OutputDir != "./out" {
		t.Errorf("output_dir = %q, want %q verbatim", cfg.OutputDir, "./out")
	}
}

// TestLoadConfig_BackupOutputDirDefaultEmpty — no "reasonable" default for
// the backup output dir; it must be explicit in backup mode. LoadConfig is
// mode-agnostic (the mode comes from flags), so it stays empty here and
// main.go fatals when -backup-file is used without it.
func TestLoadConfig_BackupOutputDirDefaultEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 1000\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackupOutputDir != "" {
		t.Errorf("backup_output_dir default = %q, want empty (unset)", cfg.BackupOutputDir)
	}
}

// TestLoadConfig_SegmentCheckWithoutSize — multipart_check_mode=2 (segment)
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
max_keys: 1000
multipart_check_mode: 2
multipart_segment_size: 0
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error when multipart_check_mode=2 but multipart_segment_size=0")
	}
}

// TestLoadConfig_MaxKeysRequired — max_keys must be set explicitly (1..1000).
// It is both the LIST page size and the threshold for the short-response
// quirk detection ("IsTruncated but fewer entries than requested"), so a
// silent default would let a mis-sized page go unnoticed.
func TestLoadConfig_MaxKeysRequired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "max_keys") {
		t.Fatalf("expected max_keys required error, got %v", err)
	}
}

func TestLoadConfig_MaxKeysRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    string
		want string
	}{
		{"ok_min", "1", ""},
		{"ok_max", "1000", ""},
		{"over", "1001", "max_keys"},
		{"negative", "-5", "max_keys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "cfg.yaml")
			content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: " + tc.v + "\n")
			if err := os.WriteFile(path, content, 0644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("expected %q error, got %v", tc.want, err)
				}
				return
			}
			if cfg.MaxKeys < 1 || cfg.MaxKeys > 1000 {
				t.Errorf("MaxKeys = %d, want 1..1000", cfg.MaxKeys)
			}
		})
	}
}

// TestLoadConfig_LatencyWindowSec — absent falls back to 60s (windowed
// latency stats window); a positive explicit value is kept; negative is
// rejected (a negative window is meaningless).
func TestLoadConfig_LatencyWindowSec(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want int
		err  string
	}{
		{"default", "", 60, ""},
		{"explicit", "latency_window_sec: 120\n", 120, ""},
		{"negative", "latency_window_sec: -1\n", 0, "latency_window_sec"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "cfg.yaml")
			content := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 500\n" + tc.yaml)
			if err := os.WriteFile(path, content, 0644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("expected %q error, got %v", tc.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.LatencyWindowSec != tc.want {
				t.Errorf("LatencyWindowSec = %d, want %d", cfg.LatencyWindowSec, tc.want)
			}
		})
	}
}

// TestLoadConfig_EncodingTypeURLAndDebugLog — both are plain opt-in booleans
// (default false, no validation).
func TestLoadConfig_EncodingTypeURLAndDebugLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
ak: x
sk: y
max_keys: 500
encoding_type_url: true
debug_log: true
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EncodingTypeURL || !cfg.DebugLog {
		t.Errorf("EncodingTypeURL/DebugLog not parsed: %+v", cfg)
	}

	path2 := filepath.Join(dir, "cfg2.yaml")
	content2 := []byte("endpoints:\n  - 10.0.0.1:9000\nak: x\nsk: y\nmax_keys: 500\n")
	if err := os.WriteFile(path2, content2, 0644); err != nil {
		t.Fatal(err)
	}
	cfg2, err := LoadConfig(path2)
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.EncodingTypeURL || cfg2.DebugLog {
		t.Errorf("EncodingTypeURL/DebugLog should default false: %+v", cfg2)
	}
}
