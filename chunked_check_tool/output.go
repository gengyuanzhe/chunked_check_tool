package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ownerLine pairs the ownerID (drives per-owner output routing) with the
// object key. The writer goroutine fans lines out by ownerID into
// <dir>/<ownerID>/<filename>.
type ownerLine struct {
	ownerID string
	key     string
}

type Output struct {
	dir    string
	bucket string

	// compiled result-line format (per-owner result files only). Process
	// files (list_failed/check_failed/multipart_check_failed) always write
	// the raw key/prefix — the format only applies to per-owner result files.
	lineFmt compiledLineFormat

	// per-owner channels (each carries ownerID + key)
	corruptedCh          chan ownerLine
	multipartAllCh       chan ownerLine
	corruptedMultipartCh chan ownerLine
	multipartOkCh        chan ownerLine
	successCh            chan ownerLine

	// root-level channels (global, no ownerID)
	listFailedCh           chan string
	checkFailedCh          chan string
	multipartCheckFailedCh chan string

	// slog loggers for the three .log files (root, concurrency-safe)
	listLogger                  *slog.Logger
	listLogFile                 *os.File
	checkLogger                 *slog.Logger
	checkLogFile                *os.File
	multipartCheckFailedLogger  *slog.Logger
	multipartCheckFailedLogFile *os.File

	// enable flags — each gates one writer goroutine + file
	corruptedEnabled            bool // is_check
	multipartAllEnabled         bool // is_check && !is_multipart_segment_check
	corruptedMultipartEnabled   bool // is_check && is_multipart_segment_check
	multipartOkEnabled          bool // is_check && is_multipart_segment_check && is_success_log
	multipartCheckFailedEnabled bool // is_check && is_multipart_segment_check
	checkEnabled                bool // is_check
	successEnabled              bool // is_check && is_success_log

	wg    sync.WaitGroup
	files []*os.File // root files only — per-owner files are owned by their goroutines
}

func NewOutput(cfg *Config, bucket string) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	chCap := cfg.OutputChCapacity
	if chCap <= 0 {
		chCap = 1024
	}
	isCheck := cfg.IsCheck
	isMP := cfg.IsMultipartSegmentCheck
	format := cfg.ResultLineFormat
	if format == "" {
		format = "<bucket>|<key>"
	}
	lineFmt, err := parseLineFormat(format)
	if err != nil {
		return nil, err
	}
	o := &Output{
		dir:                         cfg.OutputDir,
		bucket:                      bucket,
		lineFmt:                     lineFmt,
		corruptedCh:                 make(chan ownerLine, chCap),
		multipartAllCh:              make(chan ownerLine, chCap),
		corruptedMultipartCh:        make(chan ownerLine, chCap),
		multipartOkCh:               make(chan ownerLine, chCap),
		successCh:                   make(chan ownerLine, chCap),
		listFailedCh:                make(chan string, chCap),
		checkFailedCh:               make(chan string, chCap),
		multipartCheckFailedCh:      make(chan string, chCap),
		corruptedEnabled:            isCheck,
		multipartAllEnabled:         isCheck && !isMP,
		corruptedMultipartEnabled:   isCheck && isMP,
		multipartOkEnabled:          isCheck && isMP && cfg.IsMultipartSuccessLog,
		multipartCheckFailedEnabled: isCheck && isMP,
		checkEnabled:                isCheck,
		successEnabled:              isCheck && cfg.IsSuccessLog,
	}
	// list_failed is written by the lister in both check and list-only modes,
	// so it always opens. The object files are gated on is_check — in
	// list-only mode the checker never runs and no one writes to those
	// channels, so we don't create empty files.
	if err := o.openAndStartRoot("list_failed.txt", o.listFailedCh); err != nil {
		return nil, err
	}
	if err := o.openListFailedLog(); err != nil {
		return nil, err
	}
	if o.corruptedEnabled {
		if err := o.openAndStartOwner("corrupted_objects.txt", o.corruptedCh); err != nil {
			return nil, err
		}
	}
	if o.multipartAllEnabled {
		if err := o.openAndStartOwner("mp.txt", o.multipartAllCh); err != nil {
			return nil, err
		}
	}
	if o.corruptedMultipartEnabled {
		if err := o.openAndStartOwner("corrupted_mp.txt", o.corruptedMultipartCh); err != nil {
			return nil, err
		}
	}
	if o.multipartOkEnabled {
		if err := o.openAndStartOwner("ok_mp.txt", o.multipartOkCh); err != nil {
			return nil, err
		}
	}
	if o.successEnabled {
		if err := o.openAndStartOwner("ok_objects.txt", o.successCh); err != nil {
			return nil, err
		}
	}
	if o.checkEnabled {
		if err := o.openAndStartRoot("check_failed.txt", o.checkFailedCh); err != nil {
			return nil, err
		}
		if err := o.openCheckFailedLog(); err != nil {
			return nil, err
		}
	}
	if o.multipartCheckFailedEnabled {
		if err := o.openAndStartRoot("multipart_check_failed.txt", o.multipartCheckFailedCh); err != nil {
			return nil, err
		}
		if err := o.openMultipartCheckFailedLog(); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// ownerDirName normalizes an OwnerID into a safe single path component.
// Empty (when the LIST response omits owner) routes to "_unknown". Any
// ownerID containing a path separator or ".." is also folded to "_unknown"
// to prevent a malformed ownerID from escaping the output_dir.
func ownerDirName(ownerID string) string {
	if ownerID == "" || ownerID == "." || ownerID == ".." {
		return "_unknown"
	}
	if strings.ContainsAny(ownerID, `/\`) {
		return "_unknown"
	}
	return ownerID
}

// openAndStartRoot opens <dir>/<name> (append mode) and starts a writer
// goroutine that writes one channel value per line. Used for the global
// root .txt files (list_failed, check_failed, multipart_check_failed).
func (o *Output) openAndStartRoot(name string, ch chan string) error {
	f, err := os.OpenFile(filepath.Join(o.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	o.files = append(o.files, f)
	w := bufio.NewWriterSize(f, 64*1024)
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		for line := range ch {
			w.WriteString(line)
			w.WriteByte('\n')
		}
		w.Flush()
	}()
	return nil
}

// openAndStartOwner opens <dir>/<ownerID>/<name> lazily per ownerID and
// starts a writer goroutine that fans lines out by ownerID. The goroutine
// owns its own map[ownerID]*os.File + map[ownerID]*bufio.Writer; on Close
// (channel close) it flushes+ closes every opened file. This way we pay one
// goroutine per result file regardless of how many owners appear, instead
// of one goroutine per owner.
func (o *Output) openAndStartOwner(name string, ch chan ownerLine) error {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		files := make(map[string]*os.File)
		writers := make(map[string]*bufio.Writer)
		defer func() {
			for _, w := range writers {
				w.Flush()
			}
			for _, f := range files {
				f.Close()
			}
		}()
		for ol := range ch {
			dirName := ownerDirName(ol.ownerID)
			w, ok := writers[dirName]
			if !ok {
				subDir := filepath.Join(o.dir, dirName)
				if err := os.MkdirAll(subDir, 0755); err != nil {
					continue
				}
				f, err := os.OpenFile(filepath.Join(subDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					continue
				}
				files[dirName] = f
				w = bufio.NewWriterSize(f, 64*1024)
				writers[dirName] = w
			}
			o.lineFmt.writeTo(w, o.bucket, ol.ownerID, ol.key)
			w.WriteByte('\n')
		}
	}()
	return nil
}

// openListFailedLog opens list_failed.log at the root and wires it to a
// *slog.Logger with the stdlib text handler. Each WriteListFailedLog call
// becomes one structured record:
//
//	time=... level=ERROR msg="list failed" req_id=... prefix=... http_code=... s3_code=... err=...
//
// slog is concurrency-safe, so we drop the channel + writer goroutine that
// the .txt files still use.
func (o *Output) openListFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "list_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open list_failed.log: %w", err)
	}
	o.listLogFile = f
	o.files = append(o.files, f)
	o.listLogger = slog.New(slog.NewTextHandler(f, nil))
	return nil
}

// openCheckFailedLog opens check_failed.log at the root and wires it to a
// *slog.Logger with the stdlib text handler. Each WriteCheckFailedLog call
// becomes one structured log record:
//
//	time=... level=ERROR msg="check failed" req_id=... key=... http_code=... s3_code=... err=...
func (o *Output) openCheckFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "check_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open check_failed.log: %w", err)
	}
	o.checkLogFile = f
	o.files = append(o.files, f)
	o.checkLogger = slog.New(slog.NewTextHandler(f, nil))
	return nil
}

// openMultipartCheckFailedLog opens multipart_check_failed.log at the root
// and wires it to a *slog.Logger. Each WriteMultipartCheckFailedLog call
// becomes one structured log record:
//
//	time=... level=ERROR msg="multipart check failed" req_id=... key=... http_code=... s3_code=... err=...
func (o *Output) openMultipartCheckFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "multipart_check_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open multipart_check_failed.log: %w", err)
	}
	o.multipartCheckFailedLogFile = f
	o.files = append(o.files, f)
	o.multipartCheckFailedLogger = slog.New(slog.NewTextHandler(f, nil))
	return nil
}

func (o *Output) WriteCorrupted(ownerID, key string) {
	if o.corruptedEnabled {
		o.corruptedCh <- ownerLine{ownerID, key}
	}
}
func (o *Output) WriteCorruptedMultipart(ownerID, key string) {
	if o.corruptedMultipartEnabled {
		o.corruptedMultipartCh <- ownerLine{ownerID, key}
	}
}
func (o *Output) WriteMultipartAll(ownerID, key string) {
	if o.multipartAllEnabled {
		o.multipartAllCh <- ownerLine{ownerID, key}
	}
}
func (o *Output) WriteMultipartOk(ownerID, key string) {
	if o.multipartOkEnabled {
		o.multipartOkCh <- ownerLine{ownerID, key}
	}
}
func (o *Output) WriteSuccess(ownerID, key string) {
	if o.successEnabled {
		o.successCh <- ownerLine{ownerID, key}
	}
}

// orDash returns s, or "-" when s is empty. Used for slog fields that are
// optional (e.g. req_id on non-S3 errors) so the log line still carries a
// placeholder column instead of dropping the field entirely.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func (o *Output) WriteListFailed(prefix string) { o.listFailedCh <- prefix }
func (o *Output) WriteListFailedLog(prefix string, statusCode int, s3Code, reqID string, err error) {
	if o.listLogger == nil {
		return
	}
	attrs := []any{slog.String("req_id", orDash(reqID)), slog.String("bucket", orDash(o.bucket))}
	attrs = append(attrs, slog.String("prefix", prefix))
	if statusCode > 0 {
		attrs = append(attrs, slog.Int("http_code", statusCode))
	} else {
		attrs = append(attrs, slog.String("http_code", "N/A"))
	}
	if s3Code != "" {
		attrs = append(attrs, slog.String("s3_code", s3Code))
	}
	attrs = append(attrs, slog.Any("err", err))
	o.listLogger.Error("list failed", attrs...)
}
func (o *Output) WriteCheckFailed(key string) {
	if o.checkEnabled {
		o.checkFailedCh <- key
	}
}
func (o *Output) WriteCheckFailedLog(key string, statusCode int, s3Code, reqID string, err error) {
	if !o.checkEnabled || o.checkLogger == nil {
		return
	}
	attrs := []any{slog.String("req_id", orDash(reqID)), slog.String("bucket", orDash(o.bucket))}
	attrs = append(attrs, slog.String("key", key))
	if statusCode > 0 {
		attrs = append(attrs, slog.Int("http_code", statusCode))
	} else {
		attrs = append(attrs, slog.String("http_code", "N/A"))
	}
	if s3Code != "" {
		attrs = append(attrs, slog.String("s3_code", s3Code))
	}
	attrs = append(attrs, slog.Any("err", err))
	o.checkLogger.Error("check failed", attrs...)
}
func (o *Output) WriteMultipartCheckFailed(key string) {
	if o.multipartCheckFailedEnabled {
		o.multipartCheckFailedCh <- key
	}
}
func (o *Output) WriteMultipartCheckFailedLog(key string, statusCode int, s3Code, reqID string, err error) {
	if !o.multipartCheckFailedEnabled || o.multipartCheckFailedLogger == nil {
		return
	}
	attrs := []any{slog.String("req_id", orDash(reqID)), slog.String("bucket", orDash(o.bucket))}
	attrs = append(attrs, slog.String("key", key))
	if statusCode > 0 {
		attrs = append(attrs, slog.Int("http_code", statusCode))
	} else {
		attrs = append(attrs, slog.String("http_code", "N/A"))
	}
	if s3Code != "" {
		attrs = append(attrs, slog.String("s3_code", s3Code))
	}
	attrs = append(attrs, slog.Any("err", err))
	o.multipartCheckFailedLogger.Error("multipart check failed", attrs...)
}

// ChannelSnapshot returns the current length of each buffered writer
// channel. Channels whose writer goroutine was not started (because the
// corresponding mode is disabled) report 0. Called from
// ProgressPrinter's queueSnapshot provider once per progress line.
func (o *Output) ChannelSnapshot() (corrupted, multipartAll, corruptedMultipart, multipartOk, multipartCheckFailed, listFailed, checkFailed, success int) {
	if o.corruptedEnabled {
		corrupted = len(o.corruptedCh)
	}
	if o.multipartAllEnabled {
		multipartAll = len(o.multipartAllCh)
	}
	if o.corruptedMultipartEnabled {
		corruptedMultipart = len(o.corruptedMultipartCh)
	}
	if o.multipartOkEnabled {
		multipartOk = len(o.multipartOkCh)
	}
	if o.multipartCheckFailedEnabled {
		multipartCheckFailed = len(o.multipartCheckFailedCh)
	}
	listFailed = len(o.listFailedCh) // list_failed is always enabled
	if o.checkEnabled {
		checkFailed = len(o.checkFailedCh)
	}
	if o.successEnabled {
		success = len(o.successCh)
	}
	return
}

func (o *Output) Close() error {
	// list_failed always has a consumer. The other channels only have a
	// consumer when their file was opened (gated on the enable flags in
	// NewOutput). Closing a channel with no goroutine consuming it is safe.
	close(o.listFailedCh)
	if o.corruptedEnabled {
		close(o.corruptedCh)
	}
	if o.multipartAllEnabled {
		close(o.multipartAllCh)
	}
	if o.corruptedMultipartEnabled {
		close(o.corruptedMultipartCh)
	}
	if o.multipartOkEnabled {
		close(o.multipartOkCh)
	}
	if o.multipartCheckFailedEnabled {
		close(o.multipartCheckFailedCh)
	}
	if o.checkEnabled {
		close(o.checkFailedCh)
	}
	if o.successEnabled {
		close(o.successCh)
	}
	o.wg.Wait()
	var firstErr error
	for _, f := range o.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
