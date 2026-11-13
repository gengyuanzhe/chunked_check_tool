package main

import (
	"bufio"
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
	mode := flag.String("mode", "", "check or delete")
	inputPath := flag.String("input", "", "input file path (delete_markers.txt or invalid_keys.txt)")
	flag.Parse()

	if *cfgPath == "" || *mode == "" || *inputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -mode <check|delete> -input <path>")
		os.Exit(2)
	}
	if *mode != "check" && *mode != "delete" {
		fmt.Fprintf(os.Stderr, "error: -mode must be check or delete, got %q\n", *mode)
		os.Exit(2)
	}
	if err := run(*cfgPath, *mode, *inputPath); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run 是主流程，被 main 和测试调用。返回 nil 表示成功跑完。
func run(cfgPath, mode, inputPath string) error {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("mkdir output: %w", err)
	}

	runLog, err := os.OpenFile(filepath.Join(cfg.OutputDir, "run.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open run.log: %w", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)

	printConfigSnapshot(mwErr, cfg, mode, inputPath)

	start := time.Now()

	// 预检：解析输入首行取 bkt，用 endpoints[0] 跑一次 BucketExists。
	// 传输错误硬退出；exists==false 只 warning + 继续（多桶输入保护）。
	firstBkt, ok := readFirstRowBucket(inputPath)
	if !ok {
		return fmt.Errorf("preflight: first input row unparseable (torn or empty file)")
	}
	preflightClient, err := NewS3Client(cfg, cfg.Endpoints[0])
	if err != nil {
		return err
	}
	exists, err := preflightClient.BucketExists(context.Background(), firstBkt)
	if err != nil {
		fmt.Fprintf(mwErr, "bucket exists probe failed: %v\n", err)
		return err
	}
	if !exists {
		fmt.Fprintf(mwErr, "warning: bucket %s not found on endpoints[0] (continuing)\n", firstBkt)
	}

	out, err := NewOutput(cfg, mode)
	if err != nil {
		return err
	}

	var stats Stats
	progress := NewProgressPrinter(mwOut)
	progress.SetInterval(cfg.ProgressInterval)
	stats.Latency.SetWindow(time.Duration(cfg.LatencyWindowSec) * time.Second)

	runErr := RunWorkers(context.Background(), cfg, mode, inputPath, out, &stats, progress)

	stats.SetTotalDuration(time.Since(start))
	out.FlushAndWait()
	out.Close()

	stats.PrintSummary(mwOut, mode)

	return runErr
}

// readFirstRowBucket 读输入文件第一个非空行，按 \x1f 分隔解析三段
// （basename == invalid_keys.txt 时 URL 解码 key），返回 bkt 字段。
// 返回 ok=false 表示文件空或首行格式错误。
func readFirstRowBucket(inputPath string) (string, bool) {
	f, err := os.Open(inputPath)
	if err != nil {
		return "", false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		row, ok := ParseRow(line, filepath.Base(inputPath))
		if !ok {
			return "", false
		}
		return row.Bkt, true
	}
	return "", false
}

// printConfigSnapshot 打印配置快照。sk 掩码为 ***。
func printConfigSnapshot(w io.Writer, cfg *Config, mode, inputPath string) {
	fmt.Fprintf(w, "=== config ===\n")
	fmt.Fprintf(w, "mode=%s\n", mode)
	fmt.Fprintf(w, "input=%s\n", inputPath)
	fmt.Fprintf(w, "endpoints=[%s]\n", strings.Join(cfg.Endpoints, ", "))
	fmt.Fprintf(w, "scheme=%s\n", cfg.Scheme)
	fmt.Fprintf(w, "ak=%s\n", cfg.AK)
	fmt.Fprintf(w, "sk=***\n")
	fmt.Fprintf(w, "op_concurrency=%d\n", cfg.OpConcurrency)
	fmt.Fprintf(w, "output_dir=%s\n", cfg.OutputDir)
	fmt.Fprintf(w, "output_dir_timestamp=%v\n", cfg.OutputDirTimestamp)
	fmt.Fprintf(w, "progress_interval=%d\n", cfg.ProgressInterval)
	fmt.Fprintf(w, "dial_timeout=%d\n", cfg.DialTimeout)
	fmt.Fprintf(w, "response_header_timeout=%d\n", cfg.ResponseHeaderTimeout)
}
