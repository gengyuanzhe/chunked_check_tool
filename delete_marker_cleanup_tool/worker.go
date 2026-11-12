package main

import (
	"bufio"
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

// Row 是输入文件解析后的一行——bkt / key / versionID 三段。
type Row struct {
	Bkt, Key, VersionID string
}

// ParseRow 把一行 \x1f 分隔的字符串解析为 Row。
// 输入 basename == "invalid_keys.txt" 时对 key 字段做 URL 解码（lister 的兜底
// 分流文件，key 被 url.QueryEscape 编码过）。返回 ok=false 表示字段数 != 3
// 或 URL 解码失败。
func ParseRow(line, inputBasename string) (Row, bool) {
	parts := strings.Split(line, "\x1f")
	if len(parts) != 3 {
		return Row{}, false
	}
	bkt, key, vid := parts[0], parts[1], parts[2]
	if filepath.Base(inputBasename) == "invalid_keys.txt" {
		dec, err := url.QueryUnescape(key)
		if err != nil {
			return Row{}, false
		}
		key = dec
	}
	return Row{Bkt: bkt, Key: key, VersionID: vid}, true
}

// RunWorkers 读输入文件逐行，N 个 worker 从 rowCh 抢任务。
// 每个 worker 绑 endpoints[i % len(endpoints)]，按 mode 调 handleCheck/handleDelete。
// 行解析失败（torn line）跳过并写到 mode 对应的 failed log。
func RunWorkers(ctx context.Context, cfg *Config, mode, inputPath string, out *Output, stats *Stats, progress *ProgressPrinter) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	inputBasename := filepath.Base(inputPath)
	rowCh := make(chan Row, 1024)

	var wg sync.WaitGroup
	var endpointIdx atomic.Int64
	n := cfg.OpConcurrency
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := int(endpointIdx.Add(1)-1) % len(cfg.Endpoints)
			endpoint := cfg.Endpoints[idx]
			client, err := NewS3Client(cfg, endpoint)
			if err != nil {
				return
			}
			var local int
			for row := range rowCh {
				switch mode {
				case "check":
					handleCheck(ctx, client, row, out, stats)
				case "delete":
					handleDelete(ctx, client, row, out, stats)
				}
				local++
				if mode == "check" {
					progress.MaybePrintCheck(stats, local)
				} else {
					progress.MaybePrintDelete(stats, local)
				}
			}
		}()
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		row, ok := ParseRow(line, inputBasename)
		if !ok {
			if mode == "check" {
				out.WriteHeadFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3"))
			} else {
				out.WriteDeleteFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3"))
			}
			continue
		}
		rowCh <- row
	}
	close(rowCh)
	wg.Wait()
	return scanner.Err()
}

// handleCheck HEAD 一行，三态分类写 check_results.txt；失败写 head_failed。
func handleCheck(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	info, err := c.StatObject(ctx, r.Bkt, r.Key, r.VersionID)
	state, ok := classifyHead(info, err)
	if !ok {
		stats.IncrHeadFailed()
		out.WriteHeadFailed(r.Bkt, r.Key, r.VersionID)
		var er minio.ErrorResponse
		if errors.As(err, &er) {
			out.WriteHeadFailedLog(r.Bkt, r.Key, r.VersionID, er.StatusCode, er.Code, er.RequestID, err)
		} else {
			out.WriteHeadFailedLog(r.Bkt, r.Key, r.VersionID, 0, "", "", err)
		}
		return
	}
	switch state {
	case "marker":
		stats.IncrMarker()
	case "not_marker":
		stats.IncrNotMarker()
	case "absent":
		stats.IncrAbsent()
	}
	out.WriteCheckResult(r.Bkt, r.Key, r.VersionID, state)
}

// handleDelete 删除一行。NoSuchVersion / NoSuchKey 算成功（幂等空操作）；
// 其他失败写 delete_failed + log。
func handleDelete(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	err := c.DeleteObject(ctx, r.Bkt, r.Key, r.VersionID)
	if err == nil {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.Key, r.VersionID)
		return
	}
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchVersion" || er.Code == "NoSuchKey") {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.Key, r.VersionID)
		return
	}
	stats.IncrDeleteFailed()
	out.WriteDeleteFailed(r.Bkt, r.Key, r.VersionID)
	if errors.As(err, &er) {
		out.WriteDeleteFailedLog(r.Bkt, r.Key, r.VersionID, er.StatusCode, er.Code, er.RequestID, err)
	} else {
		out.WriteDeleteFailedLog(r.Bkt, r.Key, r.VersionID, 0, "", "", err)
	}
}

// 保留 minio 的引用给后续任务用，避免 import-not-used 编译错。
var _ = minio.ErrorResponse{}
