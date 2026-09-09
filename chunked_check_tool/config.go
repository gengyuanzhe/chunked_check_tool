package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Endpoints               []string `yaml:"endpoints"`
	Scheme                  string   `yaml:"scheme"`
	AK                      string   `yaml:"ak"`
	SK                      string   `yaml:"sk"`
	ListType                int      `yaml:"list_type"`
	ListAPIVersion          int      `yaml:"list_api_version"`
	ListConcurrency         int      `yaml:"list_concurrency"`
	CheckConcurrency        int      `yaml:"check_concurrency"`
	OutputDir               string   `yaml:"output_dir"`
	OutputDirTimestamp      bool     `yaml:"output_dir_timestamp"`
	// BackupOutputDir is the output directory for -backup-file mode. Kept
	// separate from OutputDir so a single config can drive both list/check
	// runs and backup runs without their result files mixing. Required when
	// running in backup mode (main.go enforces); ignored by list/check modes.
	// When OutputDirTimestamp is true, a sibling-suffix stamp is applied to
	// BackupOutputDir the same way it is to OutputDir.
	BackupOutputDir         string   `yaml:"backup_output_dir"`
	IsCheck                 bool     `yaml:"is_check"`
	IsSuccessLog            bool     `yaml:"is_success_log"`
	IsMultipartSegmentCheck bool     `yaml:"is_multipart_segment_check"`
	MultipartSegmentSize    int64    `yaml:"multipart_segment_size"`
	IsMultipartSuccessLog   bool     `yaml:"is_multipart_success_log"`
	ProgressInterval        int      `yaml:"progress_interval"`
	ObjChCapacity           int      `yaml:"obj_ch_capacity"`
	OutputChCapacity        int      `yaml:"output_ch_capacity"`
	ResultLineFormat        string   `yaml:"result_line_format"`
	BackupBucket            string   `yaml:"backup_bucket"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	// Default ON: append a timestamp suffix to output_dir each run unless the
	// user explicitly sets output_dir_timestamp: false. yaml.Unmarshal only
	// overwrites fields present in the YAML doc, so pre-setting true here
	// means an absent key yields true while an explicit false still wins.
	cfg.OutputDirTimestamp = true
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("endpoints must not be empty")
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	if cfg.OutputDirTimestamp {
		// Sibling-suffix form: ./out → ./out_20260908_175201. Every run gets
		// its own directory so repeated runs never append into the same files
		// and mix results. Trailing separators are trimmed first — ./out/
		// must not become a subdir named "_<stamp>" inside ./out. The same
		// stamp is applied to BackupOutputDir when configured, so a run that
		// uses both directories lands in a matched pair
		// (./out_<stamp>, ./backup_<stamp>).
		stamp := time.Now().Format("20060102_150405")
		cfg.OutputDir = strings.TrimRight(cfg.OutputDir, "/\\") + "_" + stamp
		if cfg.BackupOutputDir != "" {
			cfg.BackupOutputDir = strings.TrimRight(cfg.BackupOutputDir, "/\\") + "_" + stamp
		}
	}
	if cfg.ListConcurrency <= 0 {
		cfg.ListConcurrency = 8
	}
	if cfg.CheckConcurrency <= 0 {
		cfg.CheckConcurrency = 16
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 5000
	}
	if cfg.ListType == 0 {
		cfg.ListType = 2
	}
	if cfg.ListAPIVersion == 0 {
		cfg.ListAPIVersion = 1
	}
	if cfg.ResultLineFormat == "" {
		cfg.ResultLineFormat = "<bucket>|<key>"
	}
	if cfg.IsMultipartSegmentCheck && cfg.MultipartSegmentSize <= 0 {
		return nil, fmt.Errorf("is_multipart_segment_check=true requires multipart_segment_size > 0 (got %d)", cfg.MultipartSegmentSize)
	}
	return &cfg, nil
}
