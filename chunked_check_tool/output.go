package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ownerLine pairs the ownerID (drives per-owner output routing) with the
// object key and, for corrupted_mp lines in offset/list-file mode, the
// object's part offsets (nil → render via result_line_format).
type ownerLine struct {
	ownerID string
	key     string
	offsets []int64
}

// ownerRenderer renders one ownerLine to w. Per-owner result files pick
// either the configured result_line_format or, for corrupted_mp in
// offset/list-file mode, the offset-carrying bkt|key|partcnt|off0|... shape.
type ownerRenderer func(w io.Writer, bucket string, ol ownerLine)

// FileInputMode tells NewOutput which file-input mode (if any) is running,
// so it can gate the multipart result routing that is otherwise keyed on
// multipart_check_mode (the mode only governs offset synthesis during S3
// LIST and is meaningless for file inputs, whose lines carry their own
// offsets).
type FileInputMode int

const (
	// FileInputNone: S3 list/check or list-only mode.
	FileInputNone FileInputMode = iota
	// FileInputListFile: legacy -list-file (multipart-only lines). Tasks
	// always carry offsets, so mp.txt has no fallback role here.
	FileInputListFile
	// FileInputCheckFile: -check-file (mixed lines). mp.txt IS enabled: a
	// regular line whose HEAD reveals a multipart object (type drift) has no
	// offsets and must land unverified in mp.txt.
	FileInputCheckFile
)

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
	listFailedCh      chan string
	parseFailedCh     chan string
	invalidKeysCh     chan string
	checkFailedCh     chan string
	mpCheckFailedCh   chan string
	listParseFailedCh chan string
	backupOkCh        chan string
	backupFailedCh    chan string
	mismatchCh        chan string

	// slog loggers for the three .log files (root, concurrency-safe)
	listLogger             *slog.Logger
	listLogFile            *os.File
	checkLogger            *slog.Logger
	checkLogFile           *os.File
	mpCheckFailedLogger    *slog.Logger
	mpCheckFailedLogFile   *os.File
	listParseFailedLogger  *slog.Logger
	listParseFailedLogFile *os.File
	backupFailedLogger     *slog.Logger
	backupFailedLogFile    *os.File
	mismatchLogger         *slog.Logger
	mismatchLogFile        *os.File

	// enable flags — each gates one writer goroutine + file
	corruptedEnabled          bool // is_check
	multipartAllEnabled       bool // is_check && (mode=off || check-file type drift)
	corruptedMultipartEnabled bool // is_check && (mode!=off || list-file)
	multipartOkEnabled        bool // is_check && (mode!=off || list-file) && is_success_log
	mpCheckFailedEnabled      bool // is_check && (mode!=off || list-file)
	checkEnabled              bool // is_check
	successEnabled            bool // is_check && is_success_log
	backupEnabled             bool // backup mode: the three backup files
	listParseFailedEnabled    bool // is_check && mode=offset && S3-list (not file-input)

	wg    sync.WaitGroup
	files []*os.File // root files only — per-owner files are owned by their goroutines
}

// NewOutput builds the Output for list/check modes. fileInput selects the
// file-input mode (if any) and forces the multipart result routing
// (corrupted_mp/mp_check_failed/ok_mp) on regardless of multipart_check_mode:
// file-input tasks carry explicit offsets, so their verification results must
// land in those files even when the mode is off.
func NewOutput(cfg *Config, bucket string, fileInput FileInputMode) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	chCap := cfg.OutputChCapacity
	if chCap <= 0 {
		chCap = 1024
	}
	isCheck := cfg.IsCheck
	mpOutputs := cfg.MultipartCheckMode != MultipartCheckModeOff || fileInput != FileInputNone
	// corrupted_mp lines carry partcnt+offsets when the offsets are real
	// part boundaries: offset mode (etag-derived) and the file-input modes
	// (input file carries them). Segment mode offsets are synthetic [0, seg,
	// 2*seg, ...] guesses and must NOT be written as if they were part
	// boundaries.
	mpOffsetLines := cfg.MultipartCheckMode == MultipartCheckModeOffset || fileInput != FileInputNone
	// mp.txt gating:
	//   - mode=off writes every multipart here.
	//   - mode=offset routes ETag parse failures to list_parse_failed.txt
	//     (not mp.txt), so mp.txt is disabled.
	//   - mode=segment disables it (offsets always synthesized when Size>0;
	//     Size==0 has no offsets and is silently dropped).
	//   - -list-file tasks always carry offsets → no fallback role → off.
	//   - -check-file accepts regular lines, and a regular line whose HEAD
	//     reveals a multipart object (type drift) has no offsets → on.
	// list_parse_failed.txt gating:
	//   - mode=offset + S3 LIST only: the server returned a multipart ETag
	//     whose offset suffix did not parse. File-input modes carry their
	//     own offsets and never hit this path.
	var multipartAll bool
	var listParseFailed bool
	switch fileInput {
	case FileInputListFile:
		multipartAll = false
	case FileInputCheckFile:
		multipartAll = isCheck
	default:
		multipartAll = isCheck && cfg.MultipartCheckMode == MultipartCheckModeOff
		listParseFailed = isCheck && cfg.MultipartCheckMode == MultipartCheckModeOffset
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
		dir:                       cfg.OutputDir,
		bucket:                    bucket,
		lineFmt:                   lineFmt,
		corruptedCh:               make(chan ownerLine, chCap),
		multipartAllCh:            make(chan ownerLine, chCap),
		corruptedMultipartCh:      make(chan ownerLine, chCap),
		multipartOkCh:             make(chan ownerLine, chCap),
		successCh:                 make(chan ownerLine, chCap),
		listFailedCh:              make(chan string, chCap),
		parseFailedCh:             make(chan string, chCap),
		invalidKeysCh:             make(chan string, chCap),
		checkFailedCh:             make(chan string, chCap),
		mpCheckFailedCh:           make(chan string, chCap),
		listParseFailedCh:         make(chan string, chCap),
		corruptedEnabled:          isCheck,
		multipartAllEnabled:       multipartAll,
		corruptedMultipartEnabled: isCheck && mpOutputs,
		multipartOkEnabled:        isCheck && mpOutputs && cfg.IsMultipartSuccessLog,
		mpCheckFailedEnabled:      isCheck && mpOutputs,
		checkEnabled:              isCheck,
		successEnabled:            isCheck && cfg.IsSuccessLog,
		listParseFailedEnabled:    listParseFailed,
	}
	// list_failed is written by the lister in both check and list-only modes,
	// so it always opens. The object files are gated on is_check — in
	// list-only mode the checker never runs and no one writes to those
	// channels, so we don't create empty files.
	if err := o.openAndStartRoot("list_failed.txt", o.listFailedCh); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("parse_failed.txt", o.parseFailedCh); err != nil {
		return nil, err
	}
	if err := o.openListParseFailedLog(); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("invalid_keys.txt", o.invalidKeysCh); err != nil {
		return nil, err
	}
	if err := o.openListFailedLog(); err != nil {
		return nil, err
	}
	if o.corruptedEnabled {
		if err := o.openAndStartOwner("corrupted_objects.txt", o.corruptedCh, o.renderLineFmt); err != nil {
			return nil, err
		}
	}
	if o.multipartAllEnabled {
		if err := o.openAndStartOwner("mp.txt", o.multipartAllCh, o.renderLineFmt); err != nil {
			return nil, err
		}
	}
	if o.listParseFailedEnabled {
		if err := o.openAndStartRoot("list_parse_failed.txt", o.listParseFailedCh); err != nil {
			return nil, err
		}
	}
	if o.corruptedMultipartEnabled {
		render := o.renderLineFmt
		if mpOffsetLines {
			render = o.renderMultipartOffsets
		}
		if err := o.openAndStartOwner("corrupted_mp.txt", o.corruptedMultipartCh, render); err != nil {
			return nil, err
		}
	}
	if o.multipartOkEnabled {
		if err := o.openAndStartOwner("ok_mp.txt", o.multipartOkCh, o.renderLineFmt); err != nil {
			return nil, err
		}
	}
	if o.successEnabled {
		if err := o.openAndStartOwner("ok_objects.txt", o.successCh, o.renderLineFmt); err != nil {
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
// (malformed input lines) plus the three backup result files and
// backup_failed.log. The per-owner check-mode files (corrupted/ok/multipart)
// are not opened.
func NewBackupOutput(cfg *Config, bucket string) (*Output, error) {
	if err := os.MkdirAll(cfg.BackupOutputDir, 0755); err != nil {
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
		dir:            cfg.BackupOutputDir,
		bucket:         bucket,
		lineFmt:        lineFmt,
		listFailedCh:   make(chan string, chCap),
		parseFailedCh:  make(chan string, chCap),
		invalidKeysCh:  make(chan string, chCap),
		backupOkCh:     make(chan string, chCap),
		backupFailedCh: make(chan string, chCap),
		mismatchCh:     make(chan string, chCap),
		backupEnabled:  true,
	}
	if err := o.openAndStartRoot("list_failed.txt", o.listFailedCh); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("parse_failed.txt", o.parseFailedCh); err != nil {
		return nil, err
	}
	if err := o.openAndStartRoot("invalid_keys.txt", o.invalidKeysCh); err != nil {
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
// starts a writer goroutine that fans lines out by ownerID, rendering each
// line with the given renderer. The goroutine owns its own
// map[ownerID]*os.File + map[ownerID]*bufio.Writer; on Close (channel close)
// it flushes + closes every opened file. This way we pay one goroutine per
// result file regardless of how many owners appear, instead of one goroutine
// per owner.
func (o *Output) openAndStartOwner(name string, ch chan ownerLine, render ownerRenderer) error {
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
			render(w, o.bucket, ol)
			w.WriteByte('\n')
		}
	}()
	return nil
}

// renderLineFmt renders via the configured result_line_format.
func (o *Output) renderLineFmt(w io.Writer, bucket string, ol ownerLine) {
	o.lineFmt.writeTo(w, bucket, ol.ownerID, ol.key)
}

// renderMultipartOffsets writes bucket|key|partcnt|off0|off1|... — the exact
// -list-file / -backup-file input shape, so corrupted_mp.txt can be fed
// straight back for re-check or backup. strconv.AppendInt into a per-call
// stack buffer keeps it allocation-free apart from growth. Lines with no
// offsets (should not happen — corrupted implies probed) fall back to the
// plain format rather than emitting a malformed partcnt=0 line.
func (o *Output) renderMultipartOffsets(w io.Writer, bucket string, ol ownerLine) {
	if len(ol.offsets) == 0 {
		o.renderLineFmt(w, bucket, ol)
		return
	}
	sep := []byte{'|'}
	w.Write([]byte(bucket))
	w.Write(sep)
	w.Write([]byte(ol.key))
	var buf [20]byte
	w.Write(sep)
	w.Write(strconv.AppendInt(buf[:0], int64(len(ol.offsets)), 10))
	for _, off := range ol.offsets {
		w.Write(sep)
		w.Write(strconv.AppendInt(buf[:0], off, 10))
	}
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

// openListParseFailedLog opens list_parse_failed.log at the root and wires it
// to a *slog.Logger. Used for S3-side ETag parse failures in mode=offset —
// the server returned a multipart ETag whose offset suffix did not parse, so
// the object lands in list_parse_failed.txt unverified. Each entry becomes
// one structured log record:
//
//	time=... level=WARN msg="multipart etag parse failed" key=... owner=... size=... etag_len=... etag=...
func (o *Output) openListParseFailedLog() error {
	f, err := os.OpenFile(filepath.Join(o.dir, "list_parse_failed.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open list_parse_failed.log: %w", err)
	}
	o.listParseFailedLogFile = f
	o.files = append(o.files, f)
	o.listParseFailedLogger = slog.New(slog.NewTextHandler(f, nil))
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
		o.corruptedCh <- ownerLine{ownerID, key, nil}
	}
}

// WriteCorruptedMultipart records a corrupted multipart object. offsets are
// the part boundaries the probes ran at; the corrupted_mp renderer decides
// whether they appear in the line (offset/list-file mode) or are ignored in
// favor of result_line_format (segment mode — synthetic offsets).
func (o *Output) WriteCorruptedMultipart(ownerID, key string, offsets []int64) {
	if o.corruptedMultipartEnabled {
		o.corruptedMultipartCh <- ownerLine{ownerID, key, offsets}
	}
}
func (o *Output) WriteMultipartAll(ownerID, key string) {
	if o.multipartAllEnabled {
		o.multipartAllCh <- ownerLine{ownerID, key, nil}
	}
}
func (o *Output) WriteMultipartOk(ownerID, key string) {
	if o.multipartOkEnabled {
		o.multipartOkCh <- ownerLine{ownerID, key, nil}
	}
}
func (o *Output) WriteSuccess(ownerID, key string) {
	if o.successEnabled {
		o.successCh <- ownerLine{ownerID, key, nil}
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

// WriteListFailed records a failed listing of `prefix` starting from
// `continuationToken`. Token is the cursor of the FAILED page (i.e. the `next`
// returned by the previous successful page) — empty when the first page failed
// or when the caller has no resumption cursor (Mode 1 root pagination, legacy
// callers). Line format `prefix|token`; when token=="" the line is just
// `prefix`. -resume-list parses these lines and feeds (prefix, token) back
// into the BFS queue to continue enumeration from the failed page.
//
// If prefix contains '|', the contract (key/prefix contains no '|') is
// violated — the line would be unparseable by -resume-list. Such prefixes
// are written to invalid_keys.txt instead and the structured reason goes to
// list_failed.log; they cannot be auto-resumed and need manual handling.
func (o *Output) WriteListFailed(prefix, continuationToken string) {
	if strings.Contains(prefix, "|") {
		o.invalidKeysCh <- prefix
		if o.listLogger != nil {
			o.listLogger.Error("invalid prefix contains separator",
				slog.String("bucket", orDash(o.bucket)),
				slog.String("prefix", prefix),
				slog.String("reason", "prefix contains '|', cannot be written to list_failed.txt for -resume-list"))
		}
		return
	}
	if continuationToken == "" {
		o.listFailedCh <- prefix
	} else {
		o.listFailedCh <- prefix + "|" + continuationToken
	}
}
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

// WriteParseFailed records a malformed input line from -list-file / -backup-file
// (e.g. too few fields, bucket mismatch, non-integer partcnt). Lines go to
// parse_failed.txt — distinct from list_failed.txt (which is for S3 LIST
// failures and is -resume-list compatible). parse_failed lines are not
// auto-resumable; they signal a bad input file needing manual fixing.
func (o *Output) WriteParseFailed(line string) { o.parseFailedCh <- line }

// WriteListParseFailedLog records a structured log entry when an S3-listed
// multipart ETag's offset suffix did not parse in mode=offset. The object
// lands in list_parse_failed.txt unverified; this log explains why. The full
// ETag is logged — no truncation — so the operator can see the exact bytes
// the server returned, even for 10000-part objects (~145 KB).
func (o *Output) WriteListParseFailedLog(key, ownerID string, size int64, etag string) {
	if o.listParseFailedLogger == nil {
		return
	}
	attrs := []any{
		slog.String("bucket", orDash(o.bucket)),
		slog.String("key", key),
		slog.String("owner", orDash(ownerID)),
		slog.Int64("size", size),
		slog.Int("etag_len", len(etag)),
		slog.String("etag", etag),
	}
	o.listParseFailedLogger.Warn("multipart etag parse failed", attrs...)
}

// WriteListParseFailed records a multipart object whose S3-listed ETag did
// not parse in mode=offset. Routed to root-level list_parse_failed.txt —
// distinct from mp.txt (which is for mode=off / type drift) and from
// parse_failed.txt (which is for malformed -list-file / -backup-file input
// lines). Line format `bucket|key`, aligned with check_failed.txt /
// mp_check_failed.txt so the file can be fed back to -check-file for retry
// (the retry HEADs the object and re-derives offsets from the ETag).
func (o *Output) WriteListParseFailed(key string) {
	if o.listParseFailedEnabled {
		o.listParseFailedCh <- o.bucket + "|" + key
	}
}

// WriteInvalidKey records a key or prefix that violates the "no '|'"
// contract — the byte stream cannot be safely split into fields. Routed to
// invalid_keys.txt for manual handling. Called from the lister when a LIST
// response returns a key containing '|', and from WriteListFailed when the
// failed prefix contains '|'.
func (o *Output) WriteInvalidKey(key string) { o.invalidKeysCh <- key }

// WriteCheckFailed records a failed RangeGet on a regular object. Line format
// `bkt|key` — aligned with -backup-file's regular-object input, so the file
// can be fed straight back for backup retry. ETag/Size are NOT included:
// backup retry HEADs the object to re-obtain them, and including them would
// break -backup-file's 2-field regular-object shape.
func (o *Output) WriteCheckFailed(key string) {
	if o.checkEnabled {
		o.checkFailedCh <- o.bucket + "|" + key
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

// WriteMpCheckFailed records a failed RangeGet on a multipart object's probe.
// Line format `bkt|key|partcnt|off0|off1|...` — aligned with corrupted_mp.txt
// (mode=1 offset / list-file mode) and -backup-file/-list-file multipart
// input, so the file can be fed straight back for backup or re-check.
func (o *Output) WriteMpCheckFailed(key string, offsets []int64) {
	if !o.mpCheckFailedEnabled {
		return
	}
	var b []byte
	b = append(b, o.bucket...)
	b = append(b, '|')
	b = append(b, key...)
	b = append(b, '|')
	b = strconv.AppendInt(b, int64(len(offsets)), 10)
	for _, off := range offsets {
		b = append(b, '|')
		b = strconv.AppendInt(b, off, 10)
	}
	o.mpCheckFailedCh <- string(b)
}

// WriteBackupOk records the raw input line of a successfully backed-up
// object into backup_ok.txt (shape-compatible with -backup-file input).
func (o *Output) WriteBackupOk(rawLine string) {
	if o.backupEnabled {
		o.backupOkCh <- rawLine
	}
}

// WriteBackupFailed records the raw input line of a failed backup into
// backup_failed.txt — the line carries the offsets, so the file can be fed
// straight back into -backup-file for a retry.
func (o *Output) WriteBackupFailed(rawLine string) {
	if o.backupEnabled {
		o.backupFailedCh <- rawLine
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
func (o *Output) ChannelSnapshot() (corrupted, multipartAll, corruptedMultipart, multipartOk, mpCheckFailed, listFailed, checkFailed, success, listParseFailed int) {
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
	if o.listParseFailedEnabled {
		listParseFailed = len(o.listParseFailedCh)
	}
	return
}

// BackupChannelSnapshot returns the current length of the backup-mode
// writer channels. list_failed is shared with the other modes and always
// reports; the backup channels report 0 when backup mode is off.
// Called from ProgressPrinter's queueSnapshot provider once per progress
// line in -backup-file mode.
func (o *Output) BackupChannelSnapshot() (listFailed, backupOk, backupFailed, mismatch int) {
	listFailed = len(o.listFailedCh)
	if o.backupEnabled {
		backupOk = len(o.backupOkCh)
		backupFailed = len(o.backupFailedCh)
		mismatch = len(o.mismatchCh)
	}
	return
}

func (o *Output) Close() error {
	// list_failed / parse_failed / invalid_keys always have consumers (opened
	// unconditionally in both NewOutput and NewBackupOutput). The other
	// channels only have a consumer when their file was opened. Closing a
	// channel with no goroutine consuming it is safe.
	close(o.listFailedCh)
	close(o.parseFailedCh)
	close(o.invalidKeysCh)
	if o.corruptedEnabled {
		close(o.corruptedCh)
	}
	if o.multipartAllEnabled {
		close(o.multipartAllCh)
	}
	if o.listParseFailedEnabled {
		close(o.listParseFailedCh)
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
