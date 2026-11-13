package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// main 解析 flag，调用 run。错误时 stderr + exit 1。
func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix (default empty = whole bucket)")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p]")
		os.Exit(2)
	}

	if err := run(*cfgPath, *bucket, *prefix); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run 是主流程，被 main 和测试调用。返回 nil 表示成功扫完。
func run(cfgPath, bucket, prefix string) error {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("mkdir output: %w", err)
	}

	// run.log 第一个 open（对齐 chunked_check_tool/main.go:102-110）。
	runLog, err := os.OpenFile(filepath.Join(cfg.OutputDir, "run.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open run.log: %w", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)

	// 配置快照（sk 掩码）。
	printConfigSnapshot(mwErr, cfg, bucket, prefix)

	start := time.Now()

	client, err := NewS3Client(cfg, cfg.Endpoints[0])
	if err != nil {
		return err
	}

	ctx := context.Background()

	// BucketExists 预检（防桶名手滑）。
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		fmt.Fprintf(mwErr, "bucket exists probe failed: %v\n", err)
		return err
	}
	if !exists {
		return fmt.Errorf("bucket %s does not exist", bucket)
	}

	out, err := NewOutput(cfg, bucket)
	if err != nil {
		return err
	}

	var stats Stats
	progress := NewProgressPrinter(mwOut)
	progress.SetInterval(cfg.ProgressInterval)
	stats.Latency.SetWindow(time.Duration(cfg.LatencyWindowSec) * time.Second)
	client.SetLatencyTracker(&stats.Latency)

	l := NewLister(cfg, client, out, &stats, progress)
	listErr := l.Run(ctx, bucket, prefix)

	stats.SetTotalDuration(time.Since(start))
	out.FlushAndWait()
	out.Close()

	stats.PrintSummary(mwOut)

	return listErr
}

// printConfigSnapshot 打印配置快照。sk 掩码为 ***（对齐 chunked_check_tool/main.go:174-210）。
func printConfigSnapshot(w io.Writer, cfg *Config, bucket, prefix string) {
	fmt.Fprintf(w, "=== config ===\n")
	fmt.Fprintf(w, "bucket=%s\n", bucket)
	fmt.Fprintf(w, "prefix=%q\n", prefix)
	fmt.Fprintf(w, "endpoints=[%s]\n", strings.Join(cfg.Endpoints, ", "))
	fmt.Fprintf(w, "scheme=%s\n", cfg.Scheme)
	fmt.Fprintf(w, "ak=%s\n", cfg.AK)
	fmt.Fprintf(w, "sk=***\n")
	fmt.Fprintf(w, "list_type=%d\n", cfg.ListType)
	fmt.Fprintf(w, "list_concurrency=%d\n", cfg.ListConcurrency)
	fmt.Fprintf(w, "output_dir=%s\n", cfg.OutputDir)
	fmt.Fprintf(w, "output_dir_timestamp=%v\n", cfg.OutputDirTimestamp)
	fmt.Fprintf(w, "progress_interval=%d\n", cfg.ProgressInterval)
	fmt.Fprintf(w, "dial_timeout=%d\n", cfg.DialTimeout)
	fmt.Fprintf(w, "response_header_timeout=%d\n", cfg.ResponseHeaderTimeout)
}
