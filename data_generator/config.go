package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	minPartSize int64 = 5 * 1024 * 1024
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
	PartSizeMin     int64    `yaml:"part_size_min"`
	PartSizeMax     int64    `yaml:"part_size_max"`
	OutputDir        string   `yaml:"output_dir"`
	Concurrency      int      `yaml:"concurrency"`
	ProgressInterval int      `yaml:"progress_interval"`
	MD5File                  string   `yaml:"md5_file"`
	UseTrailer               bool     `yaml:"use_trailer"`
	MultipartEndpointPattern []int    `yaml:"multipart_endpoint_pattern"`
	LPrefix                  string   `yaml:"lprefix"`
	DPrefix                  string   `yaml:"dprefix"`
	FPrefix                  string   `yaml:"fprefix"`
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
	if cfg.PartSizeMin < minPartSize {
		return nil, fmt.Errorf("part_size_min (%d) must be >= %d (S3 minimum part size 5MiB)", cfg.PartSizeMin, minPartSize)
	}
	if cfg.PartSizeMax < cfg.PartSizeMin {
		return nil, fmt.Errorf("part_size_max (%d) must be >= part_size_min (%d)", cfg.PartSizeMax, cfg.PartSizeMin)
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
	if cfg.LPrefix == "" {
		cfg.LPrefix = "l"
	}
	if cfg.DPrefix == "" {
		cfg.DPrefix = "d"
	}
	if cfg.FPrefix == "" {
		cfg.FPrefix = "file_"
	}
	if len(cfg.MultipartEndpointPattern) > 0 {
		if len(cfg.MultipartEndpointPattern) < 3 {
			return nil, fmt.Errorf("multipart_endpoint_pattern must have at least 3 elements (init + 1 part + complete), got %d", len(cfg.MultipartEndpointPattern))
		}
		for i, v := range cfg.MultipartEndpointPattern {
			if v < 0 || v >= len(cfg.Endpoints) {
				return nil, fmt.Errorf("multipart_endpoint_pattern[%d]=%d out of range [0, %d)", i, v, len(cfg.Endpoints))
			}
		}
		if cfg.ObjectSizeMin <= cfg.PartSizeMax {
			return nil, fmt.Errorf("object_size_min (%d) must be > part_size_max (%d) when multipart_endpoint_pattern is set (multipart requires size > partSize for every object)", cfg.ObjectSizeMin, cfg.PartSizeMax)
		}
	}
	return &cfg, nil
}
