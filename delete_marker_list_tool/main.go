package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// main 解析 flag，调用 run。错误时 stderr + exit 1。
// -prefix 和 -retry 互斥：-retry 从文件读 prefix 列表（一行一个），-prefix
// 是单值的语法糖。两者都空 = 扫整个 bucket（root prefix）。
// -retry 文件每行格式（三字段均为 URL 编码态，读取时解码）：
// prefix\x1fkeyMarker\x1fversionIdMarker（空 marker = 从第一页扫）。
func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix (default empty = whole bucket; mutually exclusive with -retry)")
	retryPath := flag.String("retry", "", "retry prefix file (one entry per line: url-encoded prefix\\x1fkeyMarker\\x1fversionIdMarker; mutually exclusive with -prefix)")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p | -retry file]")
		os.Exit(2)
	}

	entries, err := resolveEntries(*prefix, *retryPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if err := run(*cfgPath, *bucket, entries); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// resolveEntries 把 -prefix / -retry flag 解析成初始 RetryEntry 列表。
// 互斥规则：两者同时给报错。-retry 文件必须存在（预检，防半启动后失败）。
// -prefix 单值 = [{Prefix: prefix}]（markers 空，从第一页扫）。
// 两者都空 = [{Prefix: ""}]（root prefix，扫整个 bucket）。
func resolveEntries(prefix, retryPath string) ([]RetryEntry, error) {
	if prefix != "" && retryPath != "" {
		return nil, fmt.Errorf("-prefix and -retry are mutually exclusive")
	}
	if retryPath != "" {
		return readRetryEntries(retryPath)
	}
	if prefix != "" {
		return []RetryEntry{{Prefix: prefix}}, nil
	}
	return []RetryEntry{{Prefix: ""}}, nil
}

// run 是主流程，被 main 和测试调用。返回 nil 表示成功扫完。
// entries 是初始入队条目列表（至少 1 个元素）；mode 1 只用 entries[0]，
// mode 2 全部入队。
func run(cfgPath, bucket string, entries []RetryEntry) error {
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
	prefixDisplay := entries[0].Prefix
	if len(entries) > 1 {
		prefixDisplay = fmt.Sprintf("(%d prefixes from -retry)", len(entries))
	}
	printConfigSnapshot(mwErr, cfg, bucket, prefixDisplay)

	start := time.Now()

	ctx := context.Background()

	// 启动期创建并预检所有需要的 client——fail-fast：
	// mode 1 持 1 个 client（Endpoints[0]）；mode 2 持 len(Endpoints) 个（每个 endpoint 一个）。
	// 任一 BucketExists 失败直接 return err，进程退出，不让 worker 半启动后才暴露死 endpoint。
	// 预检通过的 client 在 runMode1/runMode2 直接复用——runMode2 不再创建 client。
	nClients := 1
	if cfg.ListType == 2 {
		nClients = len(cfg.Endpoints)
	}
	clients := make([]*S3Client, 0, nClients)
	for i := 0; i < nClients; i++ {
		endpoint := cfg.Endpoints[i%len(cfg.Endpoints)]
		c, err := NewS3Client(cfg, endpoint)
		if err != nil {
			return fmt.Errorf("create client for endpoint %s: %w", endpoint, err)
		}
		exists, err := c.BucketExists(ctx, bucket)
		if err != nil {
			fmt.Fprintf(mwErr, "bucket exists probe failed for endpoint %s: %v\n", endpoint, err)
			return err
		}
		if !exists {
			return fmt.Errorf("bucket %s does not exist (probed via %s)", bucket, endpoint)
		}
		clients = append(clients, c)
	}

	out, err := NewOutput(cfg, bucket)
	if err != nil {
		return err
	}

	var stats Stats
	progress := NewProgressPrinter(mwOut)
	progress.SetInterval(cfg.ProgressInterval)
	stats.Latency.SetWindow(time.Duration(cfg.LatencyWindowSec) * time.Second)
	for _, c := range clients {
		c.SetLatencyTracker(&stats.Latency)
	}
	// debug_log: true 时建 Debug-level slog logger 路由到 run.log + 终端，
	// 注入所有 S3Client 打每次请求的 marker（req/resp/err）。false 时不注入（nil），零成本静默。
	if cfg.DebugLog {
		logger := slog.New(slog.NewTextHandler(mwErr, &slog.HandlerOptions{Level: slog.LevelDebug}))
		for _, c := range clients {
			c.SetLogger(logger)
		}
	}

	l := NewLister(cfg, clients, out, &stats, progress)
	listErr := l.Run(ctx, bucket, entries)

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
	fmt.Fprintf(w, "debug_log=%v\n", cfg.DebugLog)
	fmt.Fprintf(w, "max_keys=%d\n", cfg.MaxKeys)
	fmt.Fprintf(w, "encoding_type_url=%v\n", cfg.EncodingTypeURL)
	fmt.Fprintf(w, "repair_next_vid_marker_on_short_response=%v\n", cfg.RepairNextVIDMarkerOnShortResponse)
	fmt.Fprintf(w, "failure_threshold=%d\n", cfg.FailureThreshold)
	fmt.Fprintf(w, "slow_mode_interval_sec=%d\n", cfg.SlowModeIntervalSec)
}
