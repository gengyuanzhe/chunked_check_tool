package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	minPartSize  int64 = 5 * 1024 * 1024
	maxObjectCap int64 = 100 * 1024 * 1024
)

type Config struct {
	Endpoints        []string `yaml:"endpoints"`
	Scheme           string   `yaml:"scheme"`
	AK               string   `yaml:"ak"`
	SK               string   `yaml:"sk"`
	Bucket           string   `yaml:"bucket"`
	Prefix           string   `yaml:"prefix"`
	Depth            int      `yaml:"depth"`
	Width            int      `yaml:"width"`
	FilesPerDir      int      `yaml:"files_per_dir"`
	ObjectSizeMin    int64    `yaml:"object_size_min"`
	ObjectSizeMax    int64    `yaml:"object_size_max"`
	ChunkSizeMin     int64    `yaml:"chunk_size_min"`
	ChunkSizeMax     int64    `yaml:"chunk_size_max"`
	OutputDir        string   `yaml:"output_dir"`
	Concurrency      int      `yaml:"concurrency"`
	ProgressInterval int      `yaml:"progress_interval"`
	MD5File          string   `yaml:"md5_file"`
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
	if cfg.AK == "" || cfg.SK == "" {
		return nil, fmt.Errorf("ak and sk must not be empty")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket must not be empty")
	}
	if cfg.Depth < 1 {
		return nil, fmt.Errorf("depth must be >= 1 (got %d)", cfg.Depth)
	}
	if cfg.Width < 2 {
		return nil, fmt.Errorf("width must be >= 2 (got %d)", cfg.Width)
	}
	if cfg.FilesPerDir < 1 {
		return nil, fmt.Errorf("files_per_dir must be >= 1 (got %d)", cfg.FilesPerDir)
	}
	if cfg.ObjectSizeMin < 1 {
		return nil, fmt.Errorf("object_size_min must be >= 1 (got %d)", cfg.ObjectSizeMin)
	}
	if cfg.ObjectSizeMax < cfg.ObjectSizeMin {
		return nil, fmt.Errorf("object_size_max (%d) must be >= object_size_min (%d)", cfg.ObjectSizeMax, cfg.ObjectSizeMin)
	}
	if cfg.ObjectSizeMax > maxObjectCap {
		return nil, fmt.Errorf("object_size_max (%d) exceeds %d (100MB per-object buffer cap)", cfg.ObjectSizeMax, maxObjectCap)
	}
	if cfg.ChunkSizeMin < minPartSize {
		return nil, fmt.Errorf("chunk_size_min (%d) must be >= %d (S3 minimum part size 5MiB)", cfg.ChunkSizeMin, minPartSize)
	}
	if cfg.ChunkSizeMax < cfg.ChunkSizeMin {
		return nil, fmt.Errorf("chunk_size_max (%d) must be >= chunk_size_min (%d)", cfg.ChunkSizeMax, cfg.ChunkSizeMin)
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 100
	}
	if cfg.MD5File == "" {
		cfg.MD5File = "md5.txt"
	}
	return &cfg, nil
}
