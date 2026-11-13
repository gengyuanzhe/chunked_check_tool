package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是 delete_marker_list_tool 的配置子集。可复用 chunked_check_tool
// 的 config.yaml（未知字段会被 yaml.v3 忽略），工具只读取自己认识的字段。
type Config struct {
	Endpoints              []string `yaml:"endpoints"`
	Scheme                 string   `yaml:"scheme"`
	AK                     string   `yaml:"ak"`
	SK                     string   `yaml:"sk"`
	ListType               int      `yaml:"list_type"`
	ListConcurrency         int      `yaml:"list_concurrency"`
	OutputDir               string   `yaml:"output_dir"`
	OutputDirTimestamp      bool     `yaml:"output_dir_timestamp"`
	ProgressInterval       int      `yaml:"progress_interval"`
	DialTimeout            int64    `yaml:"dial_timeout"`
	ResponseHeaderTimeout  int64    `yaml:"response_header_timeout"`
}

// LoadConfig 读 YAML 配置并应用默认值 + 校验。timestamp 后缀逻辑对齐
// chunked_check_tool/config.go:93-106：trim trailing /，append _<stamp>
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

	// 默认值（与 chunked_check_tool 对齐）。
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	// list_type 是模式选择（正确性关键），不给默认值，必须显式 1 或 2。
	if cfg.ListConcurrency == 0 {
		cfg.ListConcurrency = 8
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	// OutputDirTimestamp 默认 true：yaml.v3 对未提供的 bool 字段零值为 false，
	// 需在反序列化后用一个 sentinel 判断"是否被显式设置"。这里用先探测 YAML
	// raw map 的方式拿到显式标志。
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
	// ResponseHeaderTimeout 默认 0（不限），不在此强制。

	// 校验。
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("config: endpoints must have at least one entry")
	}
	if cfg.Scheme != "http" && cfg.Scheme != "https" {
		return nil, fmt.Errorf("config: scheme must be http or https, got %q", cfg.Scheme)
	}
	if cfg.AK == "" || cfg.SK == "" {
		return nil, fmt.Errorf("config: ak and sk are required")
	}
	if cfg.ListType != 1 && cfg.ListType != 2 {
		return nil, fmt.Errorf("config: list_type must be 1 or 2, got %d", cfg.ListType)
	}
	if cfg.ListConcurrency < 1 {
		return nil, fmt.Errorf("config: list_concurrency must be >= 1, got %d", cfg.ListConcurrency)
	}
	if cfg.ProgressInterval < 1 {
		return nil, fmt.Errorf("config: progress_interval must be >= 1, got %d", cfg.ProgressInterval)
	}

	// timestamp 后缀。
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

// helper 用于测试：返回不带时间戳的 base 名，校验同级后缀形式。
func outputDirBase(dir string) string {
	return filepath.Base(dir)
}
