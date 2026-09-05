package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Endpoints             []string `yaml:"endpoints"`
	Scheme                string   `yaml:"scheme"`
	AK                    string   `yaml:"ak"`
	SK                    string   `yaml:"sk"`
	ListType              int      `yaml:"list_type"`
	ListAPIVersion        int      `yaml:"list_api_version"`
	ListConcurrency       int      `yaml:"list_concurrency"`
	CheckConcurrency      int      `yaml:"check_concurrency"`
	OutputDir             string   `yaml:"output_dir"`
	IsCheck               bool     `yaml:"is_check"`
	IsSuccessLog          bool     `yaml:"is_success_log"`
	IsMultipartCheck      bool     `yaml:"is_multipart_check"`
	MultipartSegmentSize  int64    `yaml:"multipart_segment_size"`
	IsMultipartSuccessLog bool     `yaml:"is_multipart_success_log"`
	ProgressInterval      int      `yaml:"progress_interval"`
	ObjChCapacity         int      `yaml:"obj_ch_capacity"`
	OutputChCapacity      int      `yaml:"output_ch_capacity"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
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
	if cfg.ListConcurrency <= 0 {
		cfg.ListConcurrency = 8
	}
	if cfg.CheckConcurrency <= 0 {
		cfg.CheckConcurrency = 16
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 100000
	}
	if cfg.ListType == 0 {
		cfg.ListType = 1
	}
	if cfg.ListAPIVersion == 0 {
		cfg.ListAPIVersion = 2
	}
	return &cfg, nil
}
