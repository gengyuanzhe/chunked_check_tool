package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是 delete_marker_cleanup_tool 的配置子集。可复用 delete_marker_list_tool
// 或 chunked_check_tool 的 config.yaml（未知字段会被 yaml.v3 忽略）。
type Config struct {
	Endpoints             []string `yaml:"endpoints"`
	Scheme                string   `yaml:"scheme"`
	AK                    string   `yaml:"ak"`
	SK                    string   `yaml:"sk"`
	OpConcurrency         int      `yaml:"op_concurrency"`
	OutputDir             string   `yaml:"output_dir"`
	OutputDirTimestamp    bool     `yaml:"output_dir_timestamp"`
	ProgressInterval      int      `yaml:"progress_interval"`
	DialTimeout           int64    `yaml:"dial_timeout"`
	ResponseHeaderTimeout int64    `yaml:"response_header_timeout"`
	LatencyWindowSec      int      `yaml:"latency_window_sec"`
}

// LoadConfig 读 YAML 配置并应用默认值 + 校验。timestamp 后缀逻辑对齐
// delete_marker_list_tool/config.go：trim trailing /，append _<stamp>
// 作为同级后缀（不是子目录嵌套）。
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.OpConcurrency == 0 {
		cfg.OpConcurrency = 8
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	// OutputDirTimestamp 默认 true：yaml.v3 对未提供的 bool 字段零值为 false，
	// 用 raw map 探测是否显式设置。
	raw := map[string]any{}
	_ = yaml.Unmarshal(b, &raw)
	if v, ok := raw["output_dir_timestamp"]; ok {
		if b, ok := v.(bool); ok {
			cfg.OutputDirTimestamp = b
		}
	} else {
		cfg.OutputDirTimestamp = true
	}
	if cfg.ProgressInterval == 0 {
		cfg.ProgressInterval = 5000
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10
	}
	// LatencyWindowSec 默认 60s（1min 窗口式时延统计）。0 视为默认。
	if cfg.LatencyWindowSec == 0 {
		cfg.LatencyWindowSec = 60
	}

	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("config: endpoints must have at least one entry")
	}
	if cfg.Scheme != "http" && cfg.Scheme != "https" {
		return nil, fmt.Errorf("config: scheme must be http or https, got %q", cfg.Scheme)
	}
	if cfg.AK == "" || cfg.SK == "" {
		return nil, fmt.Errorf("config: ak and sk are required")
	}
	if cfg.OpConcurrency < 1 {
		return nil, fmt.Errorf("config: op_concurrency must be >= 1, got %d", cfg.OpConcurrency)
	}
	if cfg.ProgressInterval < 1 {
		return nil, fmt.Errorf("config: progress_interval must be >= 1, got %d", cfg.ProgressInterval)
	}
	if cfg.LatencyWindowSec < 1 {
		return nil, fmt.Errorf("config: latency_window_sec must be >= 1, got %d", cfg.LatencyWindowSec)
	}

	if cfg.OutputDirTimestamp {
		cfg.OutputDir = applyTimestampSuffix(cfg.OutputDir)
	}

	return &cfg, nil
}

// applyTimestampSuffix 在 output_dir 末尾追加 _<YYYYMMDD_HHMMSS> 作为
// 同级后缀。先 trim trailing / 或 \。
func applyTimestampSuffix(dir string) string {
	d := strings.TrimRight(dir, "/\\")
	return d + "_" + time.Now().Format("20060102_150405")
}
