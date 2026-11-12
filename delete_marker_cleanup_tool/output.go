package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// outputFlushInterval 对齐 delete_marker_list_tool/output.go:16 的 3 秒 flush 间隔。
const outputFlushInterval = 3 * time.Second

// rowSep 是结果行分隔符——ASCII Unit Separator (\x1f)。
const rowSep = "\x1f"

// Output 写结果/失败文件。按 mode 切换打开哪些文件——mode check 开 check_results
// + head_failed（lazy）；mode delete 开 deleted + delete_failed（lazy）。
// 失败文件惰性 open（文件存在即有失败，与 lister 的 list_failed.txt 契约一致）。
type Output struct {
	dir   string
	mode  string
	bucket string // 不用——bkt 从每行读，写入时直接用调用方传入的 bkt

	checkResultCh chan [4]string // {bkt, key, versionID, state}
	deletedCh    chan [3]string // {bkt, key, versionID}
	headFailedCh chan [3]string
	deleteFailedCh chan [3]string

	checkResultFile *os.File
	deletedFile     *os.File

	headFailedFile   *os.File
	deleteFailedFile *os.File
	failedMu         sync.Mutex // 保护两个 failed 文件的 lazy open

	headFailedLogger   *slog.Logger
	deleteFailedLogger *slog.Logger
	headFailedLogFile   *os.File
	deleteFailedLogFile *os.File

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewOutput 创建输出目录 + 按 mode 打开相关文件 + 启动 writer goroutine。
func NewOutput(cfg *Config, mode string) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	o := &Output{
		dir:            cfg.OutputDir,
		mode:           mode,
		checkResultCh:  make(chan [4]string, 1024),
		deletedCh:      make(chan [3]string, 1024),
		headFailedCh:   make(chan [3]string, 256),
		deleteFailedCh: make(chan [3]string, 256),
	}

	var err error
	switch mode {
	case "check":
		o.checkResultFile, err = os.OpenFile(filepath.Join(o.dir, "check_results.txt"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open check_results.txt: %w", err)
		}
		o.headFailedLogFile, err = os.OpenFile(filepath.Join(o.dir, "head_failed.log"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			o.checkResultFile.Close()
			return nil, fmt.Errorf("open head_failed.log: %w", err)
		}
		o.headFailedLogger = slog.New(slog.NewTextHandler(o.headFailedLogFile, nil))
		o.startResultWriter(o.checkResultFile, o.checkResultCh, writeCheckResultRow)
		o.startFailedWriter("head")
	case "delete":
		o.deletedFile, err = os.OpenFile(filepath.Join(o.dir, "deleted.txt"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open deleted.txt: %w", err)
		}
		o.deleteFailedLogFile, err = os.OpenFile(filepath.Join(o.dir, "delete_failed.log"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			o.deletedFile.Close()
			return nil, fmt.Errorf("open delete_failed.log: %w", err)
		}
		o.deleteFailedLogger = slog.New(slog.NewTextHandler(o.deleteFailedLogFile, nil))
		o.startResultWriter(o.deletedFile, o.deletedCh, writeDeletedRow)
		o.startFailedWriter("delete")
	default:
		return nil, fmt.Errorf("NewOutput: unknown mode %q", mode)
	}
	return o, nil
}

// startResultWriter 启动一个 result writer goroutine。
func (o *Output) startResultWriter(file *os.File, ch interface{}, writeFn func(w *bufio.Writer, row interface{})) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		w := bufio.NewWriterSize(file, 64*1024)
		ticker := time.NewTicker(outputFlushInterval)
		defer ticker.Stop()
		switch c := ch.(type) {
		case chan [4]string:
			for {
				select {
				case row, ok := <-c:
					if !ok {
						w.Flush()
						return
					}
					writeFn(w, row)
				case <-ticker.C:
					w.Flush()
				}
			}
		case chan [3]string:
			for {
				select {
				case row, ok := <-c:
					if !ok {
						w.Flush()
						return
					}
					writeFn(w, row)
				case <-ticker.C:
					w.Flush()
				}
			}
		}
	}()
}

// startFailedWriter 启动一个 failed writer goroutine，惰性 open 对应的 txt 文件。
func (o *Output) startFailedWriter(kind string) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		ticker := time.NewTicker(outputFlushInterval)
		defer ticker.Stop()
		var ch interface{}
		if kind == "head" {
			ch = o.headFailedCh
		} else {
			ch = o.deleteFailedCh
		}
		switch c := ch.(type) {
		case chan [3]string:
			for {
				select {
				case row, ok := <-c:
					if !ok {
						return
					}
					o.writeFailedLine(kind, row)
				case <-ticker.C:
				}
			}
		}
	}()
}

// writeFailedLine 惰性 open 对应 failed.txt 并写入一行。
func (o *Output) writeFailedLine(kind string, row [3]string) {
	o.failedMu.Lock()
	defer o.failedMu.Unlock()
	var f **os.File
	var filename string
	if kind == "head" {
		f = &o.headFailedFile
		filename = "head_failed.txt"
	} else {
		f = &o.deleteFailedFile
		filename = "delete_failed.txt"
	}
	if *f == nil {
		file, err := os.OpenFile(filepath.Join(o.dir, filename),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			// 写到对应 log 文件
			if kind == "head" && o.headFailedLogger != nil {
				o.headFailedLogger.Error("open failed file", "err", err, "row", row)
			} else if o.deleteFailedLogger != nil {
				o.deleteFailedLogger.Error("open failed file", "err", err, "row", row)
			}
			return
		}
		*f = file
	}
	(*f).WriteString(row[0])
	(*f).WriteString(rowSep)
	(*f).WriteString(row[1])
	(*f).WriteString(rowSep)
	(*f).WriteString(row[2])
	(*f).WriteString("\n")
}

// writeCheckResultRow 写 bkt<sep>key<sep>versionID<sep>state\n 到 w。
func writeCheckResultRow(w *bufio.Writer, row interface{}) {
	r := row.([4]string)
	w.WriteString(r[0])
	w.WriteString(rowSep)
	w.WriteString(r[1])
	w.WriteString(rowSep)
	w.WriteString(r[2])
	w.WriteString(rowSep)
	w.WriteString(r[3])
	w.WriteByte('\n')
}

// writeDeletedRow 写 bkt<sep>key<sep>versionID\n 到 w。
func writeDeletedRow(w *bufio.Writer, row interface{}) {
	r := row.([3]string)
	w.WriteString(r[0])
	w.WriteString(rowSep)
	w.WriteString(r[1])
	w.WriteString(rowSep)
	w.WriteString(r[2])
	w.WriteByte('\n')
}

// WriteCheckResult 把一条 check 结果送进结果文件 writer。
func (o *Output) WriteCheckResult(bkt, key, versionID, state string) {
	o.checkResultCh <- [4]string{bkt, key, versionID, state}
}

// WriteDeleted 把一条 deleted 结果送进结果文件 writer。
func (o *Output) WriteDeleted(bkt, key, versionID string) {
	o.deletedCh <- [3]string{bkt, key, versionID}
}

// WriteHeadFailed 写一行到 head_failed.txt（惰性 open）。
func (o *Output) WriteHeadFailed(bkt, key, versionID string) {
	o.headFailedCh <- [3]string{bkt, key, versionID}
}

// WriteDeleteFailed 写一行到 delete_failed.txt（惰性 open）。
func (o *Output) WriteDeleteFailed(bkt, key, versionID string) {
	o.deleteFailedCh <- [3]string{bkt, key, versionID}
}

// WriteHeadFailedLog 写一条 slog 详情到 head_failed.log。
func (o *Output) WriteHeadFailedLog(bkt, key, versionID string, httpCode int, s3Code, reqID string, err error) {
	if o.headFailedLogger == nil {
		return
	}
	o.headFailedLogger.Error("head failed",
		"bucket", bkt, "key", key, "version_id", versionID,
		"http_code", httpCode, "s3_code", s3Code, "req_id", reqID, "err", err)
}

// WriteDeleteFailedLog 写一条 slog 详情到 delete_failed.log。
func (o *Output) WriteDeleteFailedLog(bkt, key, versionID string, httpCode int, s3Code, reqID string, err error) {
	if o.deleteFailedLogger == nil {
		return
	}
	o.deleteFailedLogger.Error("delete failed",
		"bucket", bkt, "key", key, "version_id", versionID,
		"http_code", httpCode, "s3_code", s3Code, "req_id", reqID, "err", err)
}

// FlushAndWait 关闭所有 channel，等 writer goroutine 排空 + flush bufio。
func (o *Output) FlushAndWait() {
	o.closeOnce.Do(func() {
		close(o.checkResultCh)
		close(o.deletedCh)
		close(o.headFailedCh)
		close(o.deleteFailedCh)
	})
	o.wg.Wait()
}

// Close 关闭底层文件。必须在 FlushAndWait 后调用。
func (o *Output) Close() error {
	var firstErr error
	if o.checkResultFile != nil {
		if err := o.checkResultFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if o.deletedFile != nil {
		if err := o.deletedFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	o.failedMu.Lock()
	if o.headFailedFile != nil {
		if err := o.headFailedFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if o.deleteFailedFile != nil {
		if err := o.deleteFailedFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	o.failedMu.Unlock()
	if o.headFailedLogFile != nil {
		if err := o.headFailedLogFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if o.deleteFailedLogFile != nil {
		if err := o.deleteFailedLogFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
