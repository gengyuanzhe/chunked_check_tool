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
// -prefix 和 -retry 互斥：-retry 从文件读 prefix 列表（一行一个），-prefix
// 是单值的语法糖。两者都空 = 扫整个 bucket（root prefix）。
func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix (default empty = whole bucket; mutually exclusive with -retry)")
	retryPath := flag.String("retry", "", "retry prefix file, one prefix per line (mutually exclusive with -prefix)")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p | -retry file]")
		os.Exit(2)
	}

	prefixes, err := resolvePrefixes(*prefix, *retryPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if err := run(*cfgPath, *bucket, prefixes); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// resolvePrefixes 把 -prefix / -retry flag 解析成初始 prefix 列表。
// 互斥规则：两者同时给报错。-retry 文件必须存在（预检，防半启动后失败）。
// 两者都空 = [""]（root prefix，扫整个 bucket）。
func resolvePrefixes(prefix, retryPath string) ([]string, error) {
	if prefix != "" && retryPath != "" {
		return nil, fmt.Errorf("-prefix and -retry are mutually exclusive")
	}
	if retryPath != "" {
		return readRetryPrefixes(retryPath)
	}
	if prefix != "" {
		return []string{prefix}, nil
	}
	return []string{""}, nil
}

// run 是主流程，被 main 和测试调用。返回 nil 表示成功扫完。
// prefixes 是初始入队前缀列表（至少 1 个元素）；mode 1 只用 prefixes[0]，
// mode 2 全部入队。
func run(cfgPath, bucket string, prefixes []string) error {
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

	// 配置快照（sk 掩码）。prefix 显示：单 prefix 直显，多 prefix（retry 模式）
	// 显示数量避免 run.log 一行过长。
	prefixDisplay := prefixes[0]
	if len(prefixes) > 1 {
		prefixDisplay = fmt.Sprintf("(%d prefixes from -retry)", len(prefixes))
	}
	printConfigSnapshot(mwErr, cfg, bucket, prefixDisplay)

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
	listErr := l.Run(ctx, bucket, prefixes)

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
