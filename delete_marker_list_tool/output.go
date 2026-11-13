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
// S3 key 规范上允许 |，实务里也偶有出现；\x1f 是控制字符，key 中基本不出现，
// 撞概率比 | 低一两个数量级。详见设计文档 §13。
const rowSep = "\x1f"

// Output 写结果/失败文件。三个 writer goroutine + 一个 lazy list_failed.txt。
// delete_markers.txt 和 invalid_keys.txt 在 NewOutput 时就 open；
// list_failed.txt 惰性 open（首次 WriteListFailed 时），保证文件存在即有失败。
type Output struct {
	dir    string
	bucket string

	deleteMarkerCh  chan [3]string // {key, versionID, lastModified}
	invalidKeysCh   chan [3]string // {key, versionID, lastModified}, key 已 URL 编码
	listFailedCh    chan string

	deleteMarkerFile *os.File
	invalidKeysFile   *os.File
	listFailedFile     *os.File
	listFailedMu       sync.Mutex // 保护 listFailedFile 的 lazy open

	listLogger *slog.Logger
	listLogFile *os.File

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewOutput 创建输出目录 + 打开 delete_markers.txt + invalid_keys.txt +
// list_failed.log，启动三个 writer goroutine。
func NewOutput(cfg *Config, bucket string) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	o := &Output{
		dir:            cfg.OutputDir,
		bucket:         bucket,
		deleteMarkerCh: make(chan [3]string, 1024),
		invalidKeysCh:  make(chan [3]string, 1024),
		listFailedCh:   make(chan string, 256),
	}

	var err error
	// delete_markers.txt: 主结果文件，启动即 open。
	o.deleteMarkerFile, err = os.OpenFile(filepath.Join(o.dir, "delete_markers.txt"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open delete_markers.txt: %w", err)
	}
	// invalid_keys.txt: 兜底文件，启动即 open（正常桶该文件为空）。
	o.invalidKeysFile, err = os.OpenFile(filepath.Join(o.dir, "invalid_keys.txt"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		o.deleteMarkerFile.Close()
		return nil, fmt.Errorf("open invalid_keys.txt: %w", err)
	}
	// list_failed.log: slog 详情文件，启动即 open。
	o.listLogFile, err = os.OpenFile(filepath.Join(o.dir, "list_failed.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		o.deleteMarkerFile.Close()
		o.invalidKeysFile.Close()
		return nil, fmt.Errorf("open list_failed.log: %w", err)
	}
	o.listLogger = slog.New(slog.NewTextHandler(o.listLogFile, nil))

	o.startWriter(o.deleteMarkerFile, o.deleteMarkerCh, false)
	o.startWriter(o.invalidKeysFile, o.invalidKeysCh, false)
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
// 空前缀 = 空行（root prefix 失败）；与"无失败"（文件不存在）明确可区分。
func (o *Output) writeListFailedLine(prefix string) {
	o.listFailedMu.Lock()
	defer o.listFailedMu.Unlock()
	if o.listFailedFile == nil {
		f, err := os.OpenFile(filepath.Join(o.dir, "list_failed.txt"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			// 写 log 文件记录错误，丢这一行。
			o.listLogger.Error("open list_failed.txt failed", "err", err, "prefix", prefix)
			return
		}
		o.listFailedFile = f
	}
	o.listFailedFile.WriteString(prefix)
	o.listFailedFile.WriteString("\n")
}

// writeRow 写一行 bkt<sep>key<sep>versionID<sep>lastModified\n 到 w。
func writeRow(w *bufio.Writer, bkt, key, versionID, lastModified string) {
	w.WriteString(bkt)
	w.WriteString(rowSep)
	w.WriteString(key)
	w.WriteString(rowSep)
	w.WriteString(versionID)
	w.WriteString(rowSep)
	w.WriteString(lastModified)
	w.WriteByte('\n')
}

// WriteDeleteMarker 把一条 marker 送进结果文件 writer。key 含 \x1f 时
// 自动路由到 invalid_keys.txt，key 经 URL 编码。lastModified 是 RFC3339
// 形式的 UTC 时间字符串（从 AWS SDK time.Time 格式化）。
func (o *Output) WriteDeleteMarker(key, versionID, lastModified string) {
	if containsRowSep(key) {
		o.invalidKeysCh <- [3]string{url.QueryEscape(key), versionID, lastModified}
	} else {
		o.deleteMarkerCh <- [3]string{key, versionID, lastModified}
	}
}

// WriteInvalidKey 显式路由到 invalid_keys.txt（key 已由调用方 URL 编码）。
func (o *Output) WriteInvalidKey(key, versionID, lastModified string) {
	o.invalidKeysCh <- [3]string{key, versionID, lastModified}
}

// WriteListFailed 写一行到 list_failed.txt（惰性 open）。
func (o *Output) WriteListFailed(prefix string) {
	o.listFailedCh <- prefix
}

// WriteListFailedLog 写一条 slog 详情到 list_failed.log。
func (o *Output) WriteListFailedLog(prefix string, statusCode int, s3Code, reqID string, err error) {
	o.listLogger.Error("list failed",
		"bucket", o.bucket,
		"prefix", prefix,
		"http_code", statusCode,
		"s3_code", s3Code,
		"req_id", reqID,
		"err", err)
}

// FlushAndWait 关闭所有 channel（不再有新行），等 writer goroutine 排空 +
// flush 底层 bufio。必须在 Close() 前调用（writer goroutine 还活着才能 drain）。
func (o *Output) FlushAndWait() {
	close(o.deleteMarkerCh)
	close(o.invalidKeysCh)
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
	if o.invalidKeysFile != nil {
		if err := o.invalidKeysFile.Close(); err != nil && firstErr == nil {
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

// containsRowSep 检查 key 是否含 \x1f（行分隔符）。
func containsRowSep(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1f {
			return true
		}
	}
	return false
}
