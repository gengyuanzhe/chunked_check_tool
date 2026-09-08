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
	// files (list_failed/check_failed/mp_check_failed) always write
	// the raw key/prefix — the format only applies to per-owner result files.
	lineFmt compiledLineFormat

	// per-owner channels (each carries ownerID + key)
	corruptedCh          chan ownerLine
	multipartAllCh       chan ownerLine
	corruptedMultipartCh chan ownerLine
	multipartOkCh        chan ownerLine
	successCh            chan ownerLine

	// root-level channels (global, no ownerID)
	listFailedCh         chan string
	checkFailedCh        chan string
	mpCheckFailedCh      chan string
	backupOkCh           chan string
	backupFailedCh       chan string
	mismatchCh           chan string
	backupSkippedCleanCh chan string

	// slog loggers for the three .log files (root, concurrency-safe)
	listLogger           *slog.Logger
	listLogFile          *os.File
	checkLogger          *slog.Logger
	checkLogFile         *os.File
	mpCheckFailedLogger  *slog.Logger
	mpCheckFailedLogFile *os.File
	backupFailedLogger   *slog.Logger
	backupFailedLogFile  *os.File
	mismatchLogger       *slog.Logger
	mismatchLogFile      *os.File

	// enable flags — each gates one writer goroutine + file
	corruptedEnabled          bool // is_check
	multipartAllEnabled       bool // is_check && !is_multipart_segment_check
	corruptedMultipartEnabled bool // is_check && is_multipart_segment_check
	multipartOkEnabled        bool // is_check && is_multipart_segment_check && is_success_log
	mpCheckFailedEnabled      bool // is_check && is_multipart_segment_check
	checkEnabled              bool // is_check
	successEnabled            bool // is_check && is_success_log
	backupEnabled             bool // backup mode: the four backup files

	wg    sync.WaitGroup
	files []*os.File // root files only — per-owner files are owned by their goroutines
}

// NewOutput builds the Output for list/check modes. listFileMode enables
// the multipart result routing (corrupted_mp/mp_check_failed/ok_mp) that is
// otherwise keyed on is_multipart_segment_check: list-file tasks always
// carry explicit offsets, so their verification results must land in those
// files even when the segment-check flag is off (it only governs offset
// synthesis in bucket mode and is meaningless for list files).
func NewOutput(cfg *Config, bucket string, listFileMode bool) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	chCap := cfg.OutputChCapacity
	if chCap <= 0 {
		chCap = 1024
	}
	isCheck := cfg.IsCheck
	mpOutputs := cfg.IsMultipartSegmentCheck || listFileMode
	format := cfg.ResultLineFormat
	if format == "" {
		format = "<bucket>|<key>"
	}
	lineFmt, err := parseLineFormat(format)
	if err != nil {
		return nil, err
	}
	o := &Output{
		dir:                       cfg.OutputDir,
		bucket:                    bucket,
		lineFmt:                   lineFmt,
		corruptedCh:               make(chan ownerLine, chCap),
		multipartAllCh:            make(chan ownerLine, chCap),
		corruptedMultipartCh:      make(chan ownerLine, chCap),
		multipartOkCh:             make(chan ownerLine, chCap),
		successCh:                 make(chan ownerLine, chCap),
		listFailedCh:              make(chan string, chCap),
		checkFailedCh:             make(chan string, chCap),
		mpCheckFailedCh:           make(chan string, chCap),
		corruptedEnabled:          isCheck,
		multipartAllEnabled:       isCheck && !mpOutputs,
		corruptedMultipartEnabled: isCheck && mpOutputs,
		multipartOkEnabled:        isCheck && mpOutputs && cfg.IsMultipartSuccessLog,
		mpCheckFailedEnabled:      isCheck && mpOutputs,
		checkEnabled:              isCheck,
		successEnabled:            isCheck && cfg.IsSuccessLog,
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
	if o.mpCheckFailedEnabled {
		if err := o.openAndStartRoot("mp_check_failed.txt", o.mpCheckFailedCh); err != nil {
			return nil, err
		}
		if err := o.openMpCheckFailedLog(); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewBackupOutput builds an Output for -backup-file mode: list_failed
// (malformed input lines) plus the four backup result files and
// backup_failed.log. The per-owner check-mode files (corrupted/ok/multipart)
// are not opened.
func NewBackupOutput(cfg *Config, bucket string) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	chCap := cfg.OutputChCapacity
	if chCap <= 0 {
		chCap = 1024
	}
	format := cfg.ResultLineFormat
	if format == "" {
		format = "<bucket>|<key>"
	}
	lineFmt, err := parseLineFormat(format)
	if err != nil {
		return nil, err
	}
	o := &Output{
		dir:                  cfg.OutputDir,
		bucket:               bucket,
		lineFmt:              lineFmt,
		listFailedCh:         make(chan string, chCap),
		backupOkCh:           make(chan string, chCap),
		backupFailedCh:       make(chan string, chCap),
		mismatchCh:           make(chan string, chCap),
		backupSkippedCleanCh: make(chan string, chCap),
		backupEnabled:        true,
	}
	if err := o.openAndStartRoot("list_failed.txt", o.listFailedCh); err != nil {
		return nil, err
	}
	if err := o.openListFailedLog(); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("backup_ok.txt", o.backupOkCh); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("backup_failed.txt", o.backupFailedCh); err != nil {
		return nil, err
	}
	if err := o.openBackupFailedLog(); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("mismatch.txt", o.mismatchCh); err != nil {
		return nil, err
	}
	if err := o.openMismatchLog(); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("backup_skipped_clean.txt", o.backupSkippedCleanCh); err != nil {
		return nil, err
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
// root .txt files (list_failed, check_failed, mp_check_failed).
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

// openMpCheckFailedLog opens mp_check_failed.log at the root
// and wires it to a *slog.Logger. Each WriteMpCheckFailedLog call
// becomes one structured log record:
//
//	time=... level=ERROR msg="multipart check failed" req_id=... key=... http_code=... s3_code=... err=...
func (o *Output) openMpCheckFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "mp_check_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open mp_check_failed.log: %w", err)
	}
	o.mpCheckFailedLogFile = f
	o.files = append(o.files, f)
	o.mpCheckFailedLogger = slog.New(slog.NewTextHandler(f, nil))
	return nil
}

// openBackupFailedLog opens backup_failed.log at the root and wires it to a
// *slog.Logger. Each WriteBackupFailedLog call becomes one structured
// record carrying the failure stage (head/verify/copy) and the error:
//
//	time=... level=ERROR msg="backup failed" req_id=... key=... stage=copy http_code=... s3_code=... err=...
func (o *Output) openBackupFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "backup_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open backup_failed.log: %w", err)
	}
	o.backupFailedLogFile = f
	o.files = append(o.files, f)
	o.backupFailedLogger = slog.New(slog.NewTextHandler(f, nil))
	return nil
}

// openMismatchLog opens mismatch.log at the root and wires it to a
// *slog.Logger. Each WriteMismatchLog call becomes one structured record
// explaining WHY the input line disagreed with the HEAD:
//
//	time=... level=WARN msg="backup mismatch" bucket=... key=... line_is_multipart=false head_etag=... head_size=... reason=...
func (o *Output) openMismatchLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "mismatch.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open mismatch.log: %w", err)
	}
	o.mismatchLogFile = f
	o.files = append(o.files, f)
	o.mismatchLogger = slog.New(slog.NewTextHandler(f, nil))
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
func (o *Output) WriteMpCheckFailed(key string) {
	if o.mpCheckFailedEnabled {
		o.mpCheckFailedCh <- key
	}
}

func (o *Output) WriteBackupOk(key string) {
	if o.backupEnabled {
		o.backupOkCh <- key
	}
}
func (o *Output) WriteBackupFailed(key string) {
	if o.backupEnabled {
		o.backupFailedCh <- key
	}
}

// WriteBackupFailedLog records the failure stage (head/verify/copy) and the
// error behind a backup_failed.txt entry.
func (o *Output) WriteBackupFailedLog(key, stage string, statusCode int, s3Code, reqID string, err error) {
	if !o.backupEnabled || o.backupFailedLogger == nil {
		return
	}
	attrs := []any{slog.String("req_id", orDash(reqID)), slog.String("bucket", orDash(o.bucket))}
	attrs = append(attrs, slog.String("key", key), slog.String("stage", stage))
	if statusCode > 0 {
		attrs = append(attrs, slog.Int("http_code", statusCode))
	} else {
		attrs = append(attrs, slog.String("http_code", "N/A"))
	}
	if s3Code != "" {
		attrs = append(attrs, slog.String("s3_code", s3Code))
	}
	attrs = append(attrs, slog.Any("err", err))
	o.backupFailedLogger.Error("backup failed", attrs...)
}

// WriteMismatch records the raw input line whose field shape (regular vs
// multipart) disagrees with the HEAD ETag.
func (o *Output) WriteMismatch(rawLine string) {
	if o.backupEnabled {
		o.mismatchCh <- rawLine
	}
}

// WriteMismatchLog records why an input line disagreed with the HEAD: the
// line's declared type, the HEAD ETag/size, and an actionable reason.
// mismatch.txt alone carries only the raw line, which made all-mismatch
// runs undiagnosable.
func (o *Output) WriteMismatchLog(key string, lineIsMultipart bool, headETag string, headSize int64) {
	if !o.backupEnabled || o.mismatchLogger == nil {
		return
	}
	headIsMultipart := !isNormalETag(headETag)
	reason := "input line and HEAD ETag disagree on object type"
	switch {
	case !lineIsMultipart && headIsMultipart:
		reason = "line is regular (bkt|key) but HEAD ETag is multipart-style: use bkt|key|partcnt|offset0|... lines for multipart objects"
	case lineIsMultipart && !headIsMultipart:
		reason = "line declares multipart (partcnt present) but HEAD ETag is a plain MD5: object type changed since the check run"
	}
	attrs := []any{
		slog.String("bucket", orDash(o.bucket)),
		slog.String("key", key),
		slog.Bool("line_is_multipart", lineIsMultipart),
		slog.String("head_etag", orDash(headETag)),
		slog.Int64("head_size", headSize),
		slog.String("reason", reason),
	}
	o.mismatchLogger.Warn("backup mismatch", attrs...)
}
func (o *Output) WriteBackupSkippedClean(key string) {
	if o.backupEnabled {
		o.backupSkippedCleanCh <- key
	}
}

func (o *Output) WriteMpCheckFailedLog(key string, statusCode int, s3Code, reqID string, err error) {
	if !o.mpCheckFailedEnabled || o.mpCheckFailedLogger == nil {
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
	o.mpCheckFailedLogger.Error("multipart check failed", attrs...)
}

// ChannelSnapshot returns the current length of each buffered writer
// channel. Channels whose writer goroutine was not started (because the
// corresponding mode is disabled) report 0. Called from
// ProgressPrinter's queueSnapshot provider once per progress line.
func (o *Output) ChannelSnapshot() (corrupted, multipartAll, corruptedMultipart, multipartOk, mpCheckFailed, listFailed, checkFailed, success int) {
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
	if o.mpCheckFailedEnabled {
		mpCheckFailed = len(o.mpCheckFailedCh)
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
	if o.mpCheckFailedEnabled {
		close(o.mpCheckFailedCh)
	}
	if o.checkEnabled {
		close(o.checkFailedCh)
	}
	if o.successEnabled {
		close(o.successCh)
	}
	if o.backupEnabled {
		close(o.backupOkCh)
		close(o.backupFailedCh)
		close(o.mismatchCh)
		close(o.backupSkippedCleanCh)
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
