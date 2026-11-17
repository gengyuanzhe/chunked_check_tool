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
	"time"
)

// Row 是输入文件解析后的一行——bkt / key / versionID / lastModified 四段。
// KeyEncoded 标记 key 是否从 URL 编码形式解码而来（需在输出时重新 QueryEscape
// 以保持行格式契约，避免解码后的控制字符 \x1f/\n/\r 撕裂行）。两种来源：
//   - delete_markers.txt 在 list 工具开了 encoding_type_url 时，key 是 URL 编码（list 不解码）
//   - invalid_keys.txt 永远是 URL 编码（list 兜底分流时 url.QueryEscape 编码过）
//
// LastModified 透传 lister 的输出，可能是 RFC3339 字符串或空串（旧 3 段输入）。
type Row struct {
	Bkt, Key, VersionID, LastModified string
	KeyEncoded                        bool
}

// ParseRow 把一行 \x1f 分隔的字符串解析为 Row。
// 解码时机：
//   - encodingTypeURL=true：delete_markers.txt 的 key 是 URL 编码，需 url.QueryUnescape。
//     解码失败 → ok=false（scanner 走 torn line 路径，写 failed.txt + slog 详情）。
//   - basename == "invalid_keys.txt"：key 永远 URL 编码（list 兜底路径），无视
//     encodingTypeURL 独立解码；失败 → ok=false（同上走 torn line）。
//   - encodingTypeURL=false 且 basename == "delete_markers.txt"：key 原样，不解码。
//
// 返回 ok=false 仅当字段数 != 3 且 != 4，或 URL 解码失败。
//
// 兼容 3 段（旧输入：bkt\x1fkey\x1fversionID）和 4 段（新输入：
// bkt\x1fkey\x1fversionID\x1flastModified）。3 段输入的 LastModified 为空串，
// 输出时第四列写空串，保持下游解析稳定。
func ParseRow(line, inputBasename string, encodingTypeURL bool) (Row, bool) {
	parts := strings.Split(line, "\x1f")
	if len(parts) != 3 && len(parts) != 4 {
		return Row{}, false
	}
	bkt, key, vid := parts[0], parts[1], parts[2]
	lastMod := ""
	if len(parts) == 4 {
		lastMod = parts[3]
	}
	encoded := false
	if filepath.Base(inputBasename) == "invalid_keys.txt" {
		// invalid_keys.txt 永远 URL 编码（list 兜底路径），与 encoding_type_url 独立。
		encoded = true
	} else if encodingTypeURL {
		// delete_markers.txt 在 list 工具开了 encoding_type_url 时 key 是 URL 编码。
		encoded = true
	}
	if encoded {
		dec, err := url.QueryUnescape(key)
		if err != nil {
			return Row{}, false
		}
		key = dec
	}
	return Row{Bkt: bkt, Key: key, VersionID: vid, LastModified: lastMod, KeyEncoded: encoded}, true
}

// outputKey 返回 key 的输出形态——KeyEncoded=true（key 是从 URL 编码形式解码的）
// 时重新 QueryEscape，保持 3/4 段行格式契约（解码后的 key 可能含 \x1f/\n/\r，撕裂行）。
// 输出文件（delete_failed.txt / deleted.txt / check_results.txt 等）下游仍由 cleanup
// 工具消费，KeyEncoded 标记需要通过文件名约定传递——目前 cleanup 工具自身读输入时
// 只对 invalid_keys.txt basename 做强制解码；delete_failed.txt 等中间文件的 key 若被
// 重新 QueryEscape，再次跑 cleanup 时不会自动解码，会按字面量百分号 key 处理。
// 现状：保留 QueryEscape 是为了一致性（与 lister 的 invalid_keys 输出格式一致）；
// 已知 tradeoff：delete_failed.txt 重跑 cleanup 时 key 不会被解码，可能找不到对象。
// 实务里 delete_failed.txt 一般人工核对后单独处理，不直接重跑 cleanup。
func (r Row) outputKey() string {
	if r.KeyEncoded {
		return url.QueryEscape(r.Key)
	}
	return r.Key
}

// RunWorkers 读输入文件逐行，N 个 worker 从 rowCh 抢任务。
// 每个 worker 绑 clients[idx % len(clients)]（clients 在 main 启动期预创并预检过
// BucketExists），按 mode 调 handleCheck/handleDelete。行解析失败（torn line）跳过
// 并写到 mode 对应的 failed log。
//
// 慢速模式：worker 连续失败 failureThreshold 次（StatObject/DeleteObject 失败，
// 不含 NoSuchVersion/NoSuchKey 幂等成功）后，每 slowModeInterval 消费 1 行 row，
// 直到拿到一次成功重置失败计数恢复正常消费。设计目的：绑死 endpoint 的 worker 不
// 持续打流量到死节点（撒满 failed.txt），但仍保留活过来的可能性。
// 等待用 select + ctx.Done() 让进程退出时能立即响应，不等满 slowModeInterval。
func RunWorkers(ctx context.Context, cfg *Config, mode, inputPath string, clients []*S3Client, out *Output, stats *Stats, progress *ProgressPrinter) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	inputBasename := filepath.Base(inputPath)
	rowCh := make(chan Row, 1024)

	threshold := cfg.FailureThreshold
	if threshold <= 0 {
		threshold = defaultFailureThreshold
	}
	slowInterval := time.Duration(cfg.SlowModeIntervalSec) * time.Second
	if cfg.SlowModeIntervalSec <= 0 {
		slowInterval = defaultSlowModeInterval
	}

	var wg sync.WaitGroup
	n := cfg.OpConcurrency
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// worker 按 idx % N 绑一个预创好的 client——endpoint 已在启动期预检。
			c := clients[idx%len(clients)]
			var local int
			var consecFails int
			interval := progress.Interval()
			for row := range rowCh {
				// 慢速模式：连续失败达阈值后，每行 row 前等待 slowInterval。
				if consecFails >= threshold {
					select {
					case <-time.After(slowInterval):
					case <-ctx.Done():
						return
					}
				}
				failed := handleRow(ctx, c, mode, row, out, stats)
				if failed {
					consecFails++
				} else {
					consecFails = 0
				}
				local++
				if local >= interval {
					if mode == "check" {
						progress.MaybePrintCheck(stats, local)
					} else {
						progress.MaybePrintDelete(stats, local)
					}
					local = 0
				}
			}
		}(i)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		row, ok := ParseRow(line, inputBasename, cfg.EncodingTypeURL)
		if !ok {
			if mode == "check" {
				out.WriteHeadFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3/4 or url decode failed"))
			} else {
				out.WriteDeleteFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3/4 or url decode failed"))
			}
			continue
		}
		rowCh <- row
	}
	close(rowCh)
	wg.Wait()
	return scanner.Err()
}

// handleRow 分发到 handleCheck/handleDelete，返回 failed=true 表示操作失败
// （worker 据此累加连续失败计数判断是否进慢速模式）。NoSuchVersion/NoSuchKey
// 算幂等成功——failed=false。
func handleRow(ctx context.Context, c *S3Client, mode string, r Row, out *Output, stats *Stats) (failed bool) {
	switch mode {
	case "check":
		return handleCheck(ctx, c, r, out, stats)
	case "delete":
		return handleDelete(ctx, c, r, out, stats)
	}
	return false
}

// handleCheck HEAD 一行，三态分类写 check_results.txt；失败写 head_failed。
// 返回 failed=true 表示 HEAD 失败（worker 据此累加连续失败计数）。
func handleCheck(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) (failed bool) {
	stats.IncrProcessed()
	result := c.StatObject(ctx, r.Bkt, r.Key, r.VersionID)
	state, ok := classifyHead(result)
	if !ok {
		stats.IncrHeadFailed()
		out.WriteHeadFailed(r.Bkt, r.outputKey(), r.VersionID, r.LastModified)
		out.WriteHeadFailedLog(r.Bkt, r.Key, r.VersionID, result.StatusCode, errorCode(result.Err), result.RequestID, result.Err)
		return true
	}
	switch state {
	case "marker":
		stats.IncrMarker()
	case "not_marker":
		stats.IncrNotMarker()
	case "absent":
		stats.IncrAbsent()
	}
	out.WriteCheckResult(r.Bkt, r.outputKey(), r.VersionID, r.LastModified, state)
	return false
}

// handleDelete 删除一行。NoSuchVersion / NoSuchKey 算成功（幂等空操作）；
// 其他失败写 delete_failed + log。返回 failed=true 表示删除失败（非幂等成功）。
func handleDelete(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) (failed bool) {
	stats.IncrProcessed()
	statusCode, requestID, err := c.DeleteObject(ctx, r.Bkt, r.Key, r.VersionID)
	if err == nil {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.outputKey(), r.VersionID, r.LastModified)
		return false
	}
	if code := errorCode(err); code == "NoSuchVersion" || code == "NoSuchKey" {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.outputKey(), r.VersionID, r.LastModified)
		return false
	}
	stats.IncrDeleteFailed()
	out.WriteDeleteFailed(r.Bkt, r.outputKey(), r.VersionID, r.LastModified)
	out.WriteDeleteFailedLog(r.Bkt, r.Key, r.VersionID, statusCode, errorCode(err), requestID, err)
	return true
}

// defaultFailureThreshold / defaultSlowModeInterval 与 lister 工具对齐（cfg=0 时用默认）。
const defaultFailureThreshold = 3
const defaultSlowModeInterval = time.Minute
