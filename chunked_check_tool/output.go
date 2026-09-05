package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type Output struct {
	dir                       string
	corruptedCh               chan string
	multipartCh               chan string
	corruptedMultipartCh      chan string
	listFailedCh              chan string
	checkFailedCh             chan string
	successCh                 chan string
	listLogger                *slog.Logger
	listLogFile               *os.File
	checkLogger               *slog.Logger
	checkLogFile              *os.File
	corruptedEnabled          bool
	multipartEnabled          bool
	corruptedMultipartEnabled bool
	checkEnabled              bool
	successEnabled            bool
	wg                        sync.WaitGroup
	files                     []*os.File
}

func NewOutput(cfg *Config) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	chCap := cfg.OutputChCapacity
	if chCap <= 0 {
		chCap = 1024
	}
	o := &Output{
		dir:                       cfg.OutputDir,
		corruptedCh:               make(chan string, chCap),
		multipartCh:               make(chan string, chCap),
		corruptedMultipartCh:      make(chan string, chCap),
		listFailedCh:              make(chan string, chCap),
		checkFailedCh:             make(chan string, chCap),
		successCh:                 make(chan string, chCap),
		corruptedEnabled:          cfg.IsCheck,
		multipartEnabled:          cfg.IsCheck,
		corruptedMultipartEnabled: cfg.IsCheck && cfg.IsMultipartCheck,
		checkEnabled:              cfg.IsCheck,
		successEnabled:            cfg.IsSuccessLog,
	}
	// list_failed is written by the lister in both check and list-only modes,
	// so it always opens. The object files (corrupted/multipart/check_failed)
	// are gated on is_check — in list-only mode the checker never runs and no
	// one writes to those channels, so we don't create empty files.
	if err := o.openAndStart("list_failed.txt", o.listFailedCh); err != nil {
		return nil, err
	}
	if err := o.openListFailedLog(); err != nil {
		return nil, err
	}
	if o.corruptedEnabled {
		if err := o.openAndStart("corrupted_objects.txt", o.corruptedCh); err != nil {
			return nil, err
		}
	}
	if o.multipartEnabled {
		if err := o.openAndStart("multipart_objects.txt", o.multipartCh); err != nil {
			return nil, err
		}
	}
	if o.corruptedMultipartEnabled {
		if err := o.openAndStart("corrupted_multipart_objects.txt", o.corruptedMultipartCh); err != nil {
			return nil, err
		}
	}
	if o.checkEnabled {
		if err := o.openAndStart("check_failed.txt", o.checkFailedCh); err != nil {
			return nil, err
		}
		if err := o.openCheckFailedLog(); err != nil {
			return nil, err
		}
	}
	if o.successEnabled {
		if err := o.openAndStart("success_objects.log", o.successCh); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// openAndStart opens the file (append mode) and starts a writer goroutine
// that writes one channel value per line. The .txt files carry only the
// key/prefix — structured error info lives in the .log files (see
// openListFailedLog/openCheckFailedLog).
func (o *Output) openAndStart(name string, ch chan string) error {
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

// openListFailedLog opens list_failed.log and wires it to a *slog.Logger
// with the stdlib text handler. Each WriteListFailedLog call becomes one
// structured record:
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

// openCheckFailedLog opens check_failed.log and wires it to a *slog.Logger
// with the stdlib text handler. Each WriteCheckFailedLog call becomes one
// structured log record:
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

func (o *Output) WriteCorrupted(key string) {
	if o.corruptedEnabled {
		o.corruptedCh <- key
	}
}
func (o *Output) WriteCorruptedMultipart(key string) {
	if o.corruptedMultipartEnabled {
		o.corruptedMultipartCh <- key
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
func (o *Output) WriteMultipart(etag, key string) {
	if o.multipartEnabled {
		o.multipartCh <- key + "|" + etag
	}
}
func (o *Output) WriteListFailed(prefix string) { o.listFailedCh <- prefix }
func (o *Output) WriteListFailedLog(prefix string, statusCode int, s3Code, reqID string, err error) {
	if o.listLogger == nil {
		return
	}
	attrs := []any{slog.String("req_id", orDash(reqID))}
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
	attrs := []any{slog.String("req_id", orDash(reqID))}
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
func (o *Output) WriteSuccess(key string) {
	if o.successEnabled {
		o.successCh <- key
	}
}

// ChannelSnapshot returns the current length of each buffered writer
// channel. Channels whose writer goroutine was not started (because the
// corresponding mode is disabled) report 0. Called from
// ProgressPrinter's queueSnapshot provider once per progress line.
func (o *Output) ChannelSnapshot() (corrupted, multipart, corruptedMultipart, listFailed, checkFailed, success int) {
	if o.corruptedEnabled {
		corrupted = len(o.corruptedCh)
	}
	if o.multipartEnabled {
		multipart = len(o.multipartCh)
	}
	if o.corruptedMultipartEnabled {
		corruptedMultipart = len(o.corruptedMultipartCh)
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
	// list_failed always has a consumer. The object channels only have a
	// consumer when their file was opened (gated on is_check / is_success_log
	// in NewOutput). Closing a channel with no goroutine consuming it is
	// safe — it just signals EOF to a range that isn't running.
	close(o.listFailedCh)
	if o.corruptedEnabled {
		close(o.corruptedCh)
	}
	if o.multipartEnabled {
		close(o.multipartCh)
	}
	if o.corruptedMultipartEnabled {
		close(o.corruptedMultipartCh)
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
