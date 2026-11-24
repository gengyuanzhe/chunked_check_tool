package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// outputFlushInterval 是定时 flush 间隔，对齐 chunked_check_tool/output.go:344。
// 保证 kill -9 时单文件尾部损失不超过一个 interval 的行。
const outputFlushInterval = 3 * time.Second

// rowSep 是结果行分隔符——ASCII Unit Separator (\x1f)。选 \x1f 而非 |：
// S3 key 规范上允许 |，实务里也偶有出现；\x1f 是控制字符，key 中基本不出现。
// 另有第二重保证：server 字段（key/versionID/markers）落盘前统一 url.QueryEscape
// （\x1f→%1F、\n→%0A、\r→%0D），撕裂类字符根本进不了文件。详见设计文档 §13。
const rowSep = "\x1f"

// Output 写结果/失败文件。一个结果 writer goroutine + 一个 lazy list_failed.txt。
// delete_markers.txt 在 NewOutput 时就 open；list_failed.txt 惰性 open（首次
// WriteListFailed 时），保证文件存在即有失败。
//
// 文件格式契约（边界 B）：server 字段（key/versionID、prefix/keyMarker/
// versionIdMarker）一律 url.QueryEscape 编码落盘，与 encoding_type_url 配置
// 无关；读取侧（retry/下游工具）一律先 url.QueryUnescape。bucket/lastModified
// 是工具生成值（字符集安全），原样落盘。
type Output struct {
	dir    string
	bucket string

	deleteMarkerCh  chan [3]string // {key, versionID, lastModified}——原始形态，writeRow 处 escape
	listFailedCh    chan [3]string // {prefix, keyMarker, versionIDMarker}——原始形态，写时 escape

	deleteMarkerFile *os.File
	listFailedFile     *os.File
	listFailedMu       sync.Mutex // 保护 listFailedFile 的 lazy open

	listLogger *slog.Logger
	listLogFile *os.File

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewOutput 创建输出目录 + 打开 delete_markers.txt + list_failed.log，
// 启动 writer goroutine。
func NewOutput(cfg *Config, bucket string) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	o := &Output{
		dir:            cfg.OutputDir,
		bucket:         bucket,
		deleteMarkerCh: make(chan [3]string, 1024),
		listFailedCh:   make(chan [3]string, 256),
	}

	var err error
	// delete_markers.txt: 主结果文件，启动即 open。
	o.deleteMarkerFile, err = os.OpenFile(filepath.Join(o.dir, "delete_markers.txt"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open delete_markers.txt: %w", err)
	}
	// list_failed.log: slog 详情文件，启动即 open。
	o.listLogFile, err = os.OpenFile(filepath.Join(o.dir, "list_failed.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		o.deleteMarkerFile.Close()
		return nil, fmt.Errorf("open list_failed.log: %w", err)
	}
	o.listLogger = slog.New(slog.NewTextHandler(o.listLogFile, nil))

	o.startWriter(o.deleteMarkerFile, o.deleteMarkerCh, false)
	o.startWriter(nil, nil, true) // list_failed writer 特殊处理
	return o, nil
}

// startWriter 启动一个 writer goroutine。isListFailed=true 时走 list_failed
// 特殊路径（lazy open 文件）；否则用预先 open 的 file。
func (o *Output) startWriter(file *os.File, ch chan [3]string, isListFailed bool) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		var w *bufio.Writer
		if !isListFailed && file != nil {
			w = bufio.NewWriterSize(file, 64*1024)
		}
		ticker := time.NewTicker(outputFlushInterval)
		defer ticker.Stop()
		for {
			if isListFailed {
				select {
				case line, ok := <-o.listFailedCh:
					if !ok {
						return
					}
					o.writeListFailedLine(line)
				case <-ticker.C:
					// list_failed 无 buffered writer，直接写文件，无需 flush。
				}
			} else {
				select {
				case row, ok := <-ch:
					if !ok {
						if w != nil {
							w.Flush()
						}
						return
					}
					writeRow(w, o.bucket, row[0], row[1], row[2])
				case <-ticker.C:
					if w != nil {
						w.Flush()
					}
				}
			}
		}
	}()
}

// writeListFailedLine 惰性 open list_failed.txt 并写入一行。
// 行格式：prefix\x1fkeyMarker\x1fversionIdMarker（三字段均 QueryEscape 编码，
// 读取侧解码恢复原始形态）。空 marker = 从第一页扫。
// 空前缀 + 空 marker = 空行 + \x1f + \x1f（root prefix 从第一页扫失败）。
// 与"无失败"（文件不存在）明确可区分——文件存在即有失败。
func (o *Output) writeListFailedLine(parts [3]string) {
	o.listFailedMu.Lock()
	defer o.listFailedMu.Unlock()
	if o.listFailedFile == nil {
		f, err := os.OpenFile(filepath.Join(o.dir, "list_failed.txt"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			// 写 log 文件记录错误，丢这一行。
			o.listLogger.Error("open list_failed.txt failed", "err", err, "prefix", parts[0])
			return
		}
		o.listFailedFile = f
	}
	o.listFailedFile.WriteString(url.QueryEscape(parts[0]))
	o.listFailedFile.WriteString("\x1f")
	o.listFailedFile.WriteString(url.QueryEscape(parts[1]))
	o.listFailedFile.WriteString("\x1f")
	o.listFailedFile.WriteString(url.QueryEscape(parts[2]))
	o.listFailedFile.WriteString("\n")
}

// writeRow 写一行 bkt<sep>key<sep>versionID<sep>lastModified\n 到 w。
// key/versionID 是 server 字段，QueryEscape 编码（边界 B，无条件）；
// bkt/lastModified 工具生成，原样。
func writeRow(w *bufio.Writer, bkt, key, versionID, lastModified string) {
	w.WriteString(bkt)
	w.WriteString(rowSep)
	w.WriteString(url.QueryEscape(key))
	w.WriteString(rowSep)
	w.WriteString(url.QueryEscape(versionID))
	w.WriteString(rowSep)
	w.WriteString(lastModified)
	w.WriteByte('\n')
}

// WriteDeleteMarker 把一条 marker 送进结果文件 writer。key/versionID 为原始
// 形态（s3client 出口已解码），writeRow 处 escape。lastModified 是 RFC3339
// 形式的 UTC 时间字符串（从 AWS SDK time.Time 格式化）。
func (o *Output) WriteDeleteMarker(key, versionID, lastModified string) {
	o.deleteMarkerCh <- [3]string{key, versionID, lastModified}
}

// WriteListFailed 写一行到 list_failed.txt（惰性 open）。
// prefix + (keyMarker, versionIDMarker) 三元组——非空 marker 表示页级续传点。
func (o *Output) WriteListFailed(prefix, keyMarker, versionIDMarker string) {
	o.listFailedCh <- [3]string{prefix, keyMarker, versionIDMarker}
}

// WriteNextVIDMarkerZeroRewriteLog 写一条 slog 警告到 list_failed.log。
// 触发条件：短响应触发时 s3client 修复了 NextVersionIdMarker（cfg.RepairNextVIDMarkerOnShortResponse=true）。
// 修复分两支：b. 原值=="0" 改写为 "null"；c. 原值为非 0 整数但与本页尾条目 key 对得上，
// 用本页尾条目的 VersionID 覆盖。rawVIDMarker 是修复前的原值，便于日志定位。
// 非失败，不写 list_failed.txt——但因为是服务端 quirk 信号（同 short-response），
// 保持 Warn 级别便于线上排查定位。
func (o *Output) WriteNextVIDMarkerZeroRewriteLog(prefix, keyMarker, rawVIDMarker string) {
	o.listLogger.Warn("next-version-id-marker rewritten",
		"bucket", o.bucket,
		"prefix", prefix,
		"key_marker", keyMarker,
		"raw_version_id_marker", rawVIDMarker)
}

// WriteWireDecodeFailedLog 写一条 slog 警告到 list_failed.log。
// 触发条件：encoding_type_url=true 时服务端返回值含非法 %XX（服务端 bug），s3client
// 出口 decodeWire 解码失败。best-effort 保留原值继续走——页请求本身 HTTP 200 成功，
// 非失败，不写 list_failed.txt。但翻页 marker / BFS prefix 用了未解码的 wire 值，
// 下一页/下一轮会位置错位导致静默漏扫——Warn 是兜底信号让运维定位。
// raw 是首个失败字段的原始 wire 值（一页可能多字段失败，只记首个避免刷屏）。
// 与 short-response / next-vid-rewrite 同级，不依赖 cfg.DebugLog——生产环境通常
// 不开 DebugLog，否则这条信号会被吞掉。
func (o *Output) WriteWireDecodeFailedLog(prefix, keyMarker, raw string) {
	o.listLogger.Warn("wire url decode failed",
		"bucket", o.bucket,
		"prefix", prefix,
		"key_marker", keyMarker,
		"raw", raw)
}

// WriteShortResponseWarning 写一条 slog 警告到 list_failed.log。
// 触发条件：服务端返回 IsTruncated=true 但本页条目数（Version+DeleteMarker+
// CommonPrefix）< 请求的 maxKeys——矛盾状态（说还有更多却又没返回满），
// 常预示服务端翻页 quirk（如返回异常 next-marker）。非失败，不写 list_failed.txt。
// 仅在 cfg.MaxKeys>0 时调（maxKeys=0 时客户端不知实际 max，无法判定）。
func (o *Output) WriteShortResponseWarning(prefix, keyMarker, versionIDMarker string, returnedCount, maxKeys int32) {
	o.listLogger.Warn("short response while truncated",
		"bucket", o.bucket,
		"prefix", prefix,
		"key_marker", keyMarker,
		"version_id_marker", versionIDMarker,
		"returned_count", returnedCount,
		"max_keys", maxKeys)
}

// WriteListFailedLog 写一条 slog 详情到 list_failed.log。
// keyMarker/versionIDMarker 是失败请求用的 marker（=续传点），便于定位翻页位置。
func (o *Output) WriteListFailedLog(prefix, keyMarker, versionIDMarker string, statusCode int, s3Code, reqID string, err error) {
	o.listLogger.Error("list failed",
		"bucket", o.bucket,
		"prefix", prefix,
		"key_marker", keyMarker,
		"version_id_marker", versionIDMarker,
		"http_code", statusCode,
		"s3_code", s3Code,
		"req_id", reqID,
		"err", err)
}

// FlushAndWait 关闭所有 channel（不再有新行），等 writer goroutine 排空 +
// flush 底层 bufio。必须在 Close() 前调用（writer goroutine 还活着才能 drain）。
func (o *Output) FlushAndWait() {
	close(o.deleteMarkerCh)
	close(o.listFailedCh)
	o.wg.Wait()
}

// Close 关闭底层文件。必须在 FlushAndWait 后调用。
func (o *Output) Close() error {
	var firstErr error
	if o.deleteMarkerFile != nil {
		if err := o.deleteMarkerFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	o.listFailedMu.Lock()
	if o.listFailedFile != nil {
		if err := o.listFailedFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	o.listFailedMu.Unlock()
	if o.listLogFile != nil {
		if err := o.listLogFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
