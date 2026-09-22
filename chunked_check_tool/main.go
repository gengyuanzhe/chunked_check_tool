package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// errInterrupted is returned when the process received a signal and
// completed its graceful drain. The run directory is a complete checkpoint:
// every prefix is either fully listed (with every listed object probed or
// recorded in check_failed / mp_check_failed) or carries a resume cursor in
// list_failed.txt; the file-input modes resume by re-running the same input
// file. main logs this and exits non-zero so the operator knows to resume.
var errInterrupted = errors.New("interrupted: graceful drain complete, output is a resumable checkpoint")

func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix")
	startAfter := flag.String("nextmarker", "", "start-after key (Mode 1 root pagination only)")
	listFile := flag.String("list-file", "", "list file path: lines of bkt|key|partcnt|offset0|offset1|... (legacy multipart-only re-check; bypasses S3 listing; requires is_check=true)")
	checkFile := flag.String("check-file", "", "check file path: mixed lines of bkt|key (regular) or bkt|key|partcnt|offset0|... (multipart); re-checks the failure files of a prior run (check_failed.txt / mp_check_failed.txt / mismatch.txt); requires is_check=true")
	backupFile := flag.String("backup-file", "", "backup list file path: mixed lines of bkt|key (regular) or bkt|key|partcnt|offset0|... (multipart); heads each object, validates the type against the HEAD ETag, and relays it into backup_bucket without probing")
	resumeList := flag.String("resume-list", "", "resume file path: lines of prefix|token (or prefix) from a prior run's list_failed.txt; re-enumerates from each failed page's cursor. Mode 2 only; mutually exclusive with the other file flags")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p] [-nextmarker key]")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -list-file <path>    (legacy multipart-only re-check)")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -check-file <path>   (re-check failed objects from a prior run)")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -backup-file <path>  (requires backup_bucket in config)")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -resume-list <path>  (Mode 2 only; resumes from list_failed.txt)")
		os.Exit(2)
	}

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// The four file flags are mutually exclusive input sources.
	fileFlags := 0
	for _, s := range []string{*listFile, *checkFile, *backupFile, *resumeList} {
		if s != "" {
			fileFlags++
		}
	}
	if fileFlags > 1 {
		fmt.Fprintln(os.Stderr, "usage: -list-file / -check-file / -backup-file / -resume-list are mutually exclusive")
		os.Exit(2)
	}
	if (*listFile != "" || *checkFile != "") && !cfg.IsCheck {
		fmt.Fprintln(os.Stderr, "usage: -list-file / -check-file require -c config with is_check=true")
		os.Exit(2)
	}
	// -resume-list is list_type=2 only — Modes 1/3 have different pagination
	// shapes (Mode 1 mixes a delimiter'd root loop with flat sub-prefix
	// listings and the line format cannot say which loop a cursor belongs
	// to; Mode 3's recursive walk exposes no per-prefix cursor). Listing
	// failures under Mode 1/3 are retried by re-running the original scan
	// (idempotent; the cross-run union absorbs duplicates).
	if *resumeList != "" && cfg.ListType != 2 {
		fmt.Fprintln(os.Stderr, "usage: -resume-list requires list_type=2 (BFS)")
		os.Exit(2)
	}
	if *backupFile != "" && cfg.BackupBucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -backup-file requires -c config with backup_bucket=<name>")
		os.Exit(2)
	}
	if *backupFile != "" && cfg.BackupOutputDir == "" {
		fmt.Fprintln(os.Stderr, "usage: -backup-file requires -c config with backup_output_dir=<path>")
		os.Exit(2)
	}

	// Open run.log at the active output dir root and tee stdout+stderr into
	// it. Backup mode writes to BackupOutputDir; list/check modes write to
	// OutputDir. We open it before NewOutput so that NewOutput's mkdir error
	// (if any) is also captured. The dir may not exist yet, so mkdir here
	// first.
	outDir := cfg.OutputDir
	if *backupFile != "" {
		outDir = cfg.BackupOutputDir
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("mkdir output dir: %v", err)
	}
	runLogPath := filepath.Join(outDir, "run.log")
	runLog, err := os.OpenFile(runLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open run.log: %v", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)
	log.SetOutput(mwErr)

	printConfig(mwOut, *cfgPath, cfg, *bucket, *prefix, *startAfter, *listFile, *checkFile, *backupFile, *resumeList, runLogPath)

	listCtx, hardCtx, stopSignals := installSignalHandler()
	defer stopSignals()

	if err := run(listCtx, hardCtx, cfg, *bucket, *prefix, *startAfter, *listFile, *checkFile, *backupFile, *resumeList, mwOut); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// installSignalHandler wires the two-stage SIGINT/SIGTERM shutdown.
//
//	1st signal cancels listCtx: S3 listing and input-file reading stop.
//
// In-flight pages record their resume cursor into list_failed.txt; prefixes
// still queued are recorded by drainQueuedPrefixes; every task already
// emitted finishes through the workers. The run directory becomes a
// complete checkpoint — nothing that was listed is left without a terminal
// state, nothing unlisted is left without a resume cursor.
//
//	2nd signal cancels hardCtx: in-flight probes/relays abort (multipart
//
// uploads are aborted, no partial object lingers) and every task still
// queued fails fast into its failure file — check_failed / mp_check_failed
// (the -check-file retry input) or backup_failed (the -backup-file retry
// input). Nothing unprocessed is silently dropped; the process exits once
// the queues burn down.
//
// A hard crash (kill -9, OOM) is the only lossy path: nothing is recorded
// for the unprocessed remainder. Recovery is re-running the same scan scope
// — probes are idempotent reads and outputs are append-only unions.
func installSignalHandler() (listCtx, hardCtx context.Context, stop func()) {
	listCtx, listCancel := context.WithCancel(context.Background())
	hardCtx, hardCancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("signal received: stopping listing, draining in-flight work (second signal aborts immediately)")
		listCancel()
		<-sigCh
		log.Printf("second signal: aborting in-flight work, recording unprocessed tasks as failures")
		hardCancel()
	}()
	stop = func() {
		signal.Stop(sigCh)
		listCancel()
		hardCancel()
	}
	return listCtx, hardCtx, stop
}

// printConfig writes a config snapshot to w at startup. sk is masked so the
// run.log (which persists) doesn't leak the secret; ak is shown in clear.
// Each line is indented 4 spaces for readability against the progress/summary
// lines that surround it in run.log.
func printConfig(w io.Writer, cfgPath string, cfg *Config, bucket, prefix, startAfter, listFile, checkFile, backupFile, resumeList, runLogPath string) {
	fmt.Fprintf(w, "=== config ===\n")
	fmt.Fprintf(w, "    config: %s\n", cfgPath)
	fmt.Fprintf(w, "    bucket: %s\n", bucket)
	fmt.Fprintf(w, "    prefix: %s\n", orEmpty(prefix))
	fmt.Fprintf(w, "    nextmarker: %s\n", orEmpty(startAfter))
	fmt.Fprintf(w, "    list_file: %s\n", orEmpty(listFile))
	fmt.Fprintf(w, "    check_file: %s\n", orEmpty(checkFile))
	fmt.Fprintf(w, "    backup_file: %s\n", orEmpty(backupFile))
	fmt.Fprintf(w, "    resume_list: %s\n", orEmpty(resumeList))
	fmt.Fprintf(w, "    backup_bucket: %s\n", orEmpty(cfg.BackupBucket))
	fmt.Fprintf(w, "    endpoints: %s\n", strings.Join(cfg.Endpoints, ", "))
	fmt.Fprintf(w, "    scheme: %s\n", cfg.Scheme)
	fmt.Fprintf(w, "    ak: %s\n", cfg.AK)
	fmt.Fprintf(w, "    sk: ***\n")
	fmt.Fprintf(w, "    list_type: %d\n", cfg.ListType)
	fmt.Fprintf(w, "    list_api_version: %d\n", cfg.ListAPIVersion)
	fmt.Fprintf(w, "    list_concurrency: %d\n", cfg.ListConcurrency)
	fmt.Fprintf(w, "    check_concurrency: %d\n", cfg.CheckConcurrency)
	fmt.Fprintf(w, "    output_dir: %s\n", cfg.OutputDir)
	fmt.Fprintf(w, "    output_dir_timestamp: %t\n", cfg.OutputDirTimestamp)
	fmt.Fprintf(w, "    backup_output_dir: %s\n", orEmpty(cfg.BackupOutputDir))
	fmt.Fprintf(w, "    is_check: %t\n", cfg.IsCheck)
	fmt.Fprintf(w, "    is_success_log: %t\n", cfg.IsSuccessLog)
	fmt.Fprintf(w, "    multipart_check_mode: %d\n", cfg.MultipartCheckMode)
	fmt.Fprintf(w, "    multipart_segment_size: %d\n", cfg.MultipartSegmentSize)
	fmt.Fprintf(w, "    is_multipart_success_log: %t\n", cfg.IsMultipartSuccessLog)
	fmt.Fprintf(w, "    node_isolate_threshold: %d\n", cfg.NodeIsolateThreshold)
	fmt.Fprintf(w, "    node_recover_probe_interval: %d\n", cfg.NodeRecoverProbeInterval)
	fmt.Fprintf(w, "    progress_interval: %d\n", cfg.ProgressInterval)
	fmt.Fprintf(w, "    obj_ch_capacity: %d\n", cfg.ObjChCapacity)
	fmt.Fprintf(w, "    output_ch_capacity: %d\n", cfg.OutputChCapacity)
	fmt.Fprintf(w, "    result_line_format: %s\n", cfg.ResultLineFormat)
	fmt.Fprintf(w, "    run_log: %s\n", runLogPath)
	fmt.Fprintf(w, "=== end config ===\n")
}

func orEmpty(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

// drainQueuedPrefixes records every prefix still sitting in the BFS queue
// when listing was interrupted (listCtx cancelled): the workers exited via
// cancellation without popping them, so nothing under those prefixes was
// listed this run. Bare prefixes are written as-is (resume = full re-list);
// -resume-list seeds ("prefix\x00token") round-trip their cursor so the
// retry continues from where the previous run failed. No-op on normal
// completion (the queue is closed and empty by then).
func drainQueuedPrefixes(listCtx context.Context, q *Queue, out *Output, stats *Stats) {
	if listCtx.Err() == nil {
		return
	}
	err := fmt.Errorf("interrupted before listing started: %w", listCtx.Err())
	for _, item := range q.Drain() {
		prefix, token := item, ""
		if i := strings.IndexByte(item, '\x00'); i >= 0 {
			prefix, token = item[:i], item[i+1:]
		}
		out.WriteListFailed(prefix, token)
		out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
		stats.IncrListFailed()
	}
}

// run wires NodePool, Output, Stats, Lister, and Checker workers together.
//
// Seeding:
//   - Mode 1: main paginates the root prefix WITH delimiter in a loop. Each
//     page's direct objects are emitted to objCh (check mode) or classified
//     locally (list-only mode); each page's CommonPrefixes are seeded into
//     the queue. The root prefix itself is NEVER seeded — workers list
//     sub-prefixes flat (delim=false), so seeding root would flat-list
//     subdir objects and duplicate them.
//   - Mode 2: lister.Seed(prefix). The -nextmarker flag is ignored in Mode 2
//     (documented limitation — it is only meaningful for Mode 1 root
//     pagination).
//
// Termination: list workers exit when the shared Lister's inflight counter
// drops to zero (which closes the queue). listWg.Wait then drains any
// interrupted-but-unstarted prefixes into list_failed and closes objCh,
// which unblocks check workers. If nothing was seeded (e.g. Mode 1 root
// pagination returned no sub-prefixes), the queue is closed explicitly so
// list workers exit immediately.
//
// The two contexts come from installSignalHandler: listCtx stops listing
// (graceful drain begins), hardCtx aborts in-flight work (see Checker /
// BackupChecker / Lister comments).
func run(listCtx, hardCtx context.Context, cfg *Config, bucket, prefix, startAfter, listFile, checkFile, backupFile, resumeList string, stdout io.Writer) error {
	// Backup mode: an entirely separate pipeline (HEAD + type gate + relay);
	// see runBackup. Dispatched before any list/check wiring.
	if backupFile != "" {
		return runBackup(listCtx, hardCtx, cfg, bucket, backupFile, stdout)
	}

	pool := NewNodePool(cfg)
	startNodeRecovery(hardCtx, pool, cfg, bucket)

	fileInput := FileInputNone
	switch {
	case checkFile != "":
		fileInput = FileInputCheckFile
	case listFile != "":
		fileInput = FileInputListFile
	}
	out, err := NewOutput(cfg, bucket, fileInput)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	stats := NewStats()
	mode := ModeListCheck
	switch {
	case fileInput == FileInputCheckFile:
		mode = ModeCheckFile
	case fileInput == FileInputListFile:
		mode = ModeListFile
	case !cfg.IsCheck:
		mode = ModeListOnly
	}
	printer := NewProgressPrinter(stdout, mode)
	start := time.Now()

	objChCap := cfg.ObjChCapacity
	if objChCap <= 0 {
		// default: 4× check workers, floored at 2000
		objChCap = cfg.CheckConcurrency * 4
		if objChCap < 2000 {
			objChCap = 2000
		}
	}
	objCh := make(chan VerifyTask, objChCap)
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)
	printer.SetQueueSnapshotProvider(func() QueueSnapshot {
		cor, mpAll, cmp, mpOk, mcf, lf, cf, su, lpf := out.ChannelSnapshot()
		return QueueSnapshot{
			Prefix:           q.Len(),
			ObjCh:            len(objCh),
			CorruptedObjects: cor,
			OkMp:             mpAll + mpOk,
			CorruptedMp:      cmp,
			ListFailed:       lf,
			CheckFailed:      cf,
			MpCheckFailed:    mcf,
			ListParseFailed:  lpf,
			OkObjects:        su,
		}
	})

	// Spawn check workers first so they drain objCh while main paginates the
	// root prefix in Mode 1 (which writes directly to objCh).
	var checkWg sync.WaitGroup
	if cfg.IsCheck {
		for i := 0; i < cfg.CheckConcurrency; i++ {
			checkWg.Add(1)
			workerS3 := newWorker(pool, i+cfg.ListConcurrency, cfg, bucket, stats)
			lc := &localCounter{interval: cfg.ProgressInterval, printer: printer}
			go func(w S3API, lc *localCounter) {
				defer checkWg.Done()
				c := NewChecker(hardCtx, w, out, stats, cfg)
				for obj := range objCh {
					c.Handle(obj)
					lc.incr(stats, "checked")
				}
			}(workerS3, lc)
		}
	}

	// File-input source (-list-file legacy / -check-file): bypass S3 listing
	// entirely. Read the file line-by-line, push tasks to objCh. No list
	// workers, no queue, no Lister. is_check must be true (validated in
	// main). A signal stops the read (listCtx); queued tasks drain through
	// the check workers, and the retry is re-running the same input file.
	if fileInput != FileInputNone {
		srcPath := listFile
		parse := parseListFileLine
		if fileInput == FileInputCheckFile {
			srcPath = checkFile
			parse = parseCheckFileLine
		}
		listStart := time.Now()
		src := newFileSource[VerifyTask](srcPath, bucket, out, stats, parse)
		go func() {
			if err := src.Run(listCtx, objCh); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("file source: %v", err)
			}
			close(objCh)
			stats.SetListDuration(time.Since(listStart))
		}()
		checkWg.Wait()
		if err := out.Close(); err != nil {
			log.Printf("output close: %v", err)
		}
		stats.SetTotalDuration(time.Since(start))
		stats.PrintSummary(stdout, mode)
		if listCtx.Err() != nil {
			return errInterrupted
		}
		return nil
	}

	// Spawn list workers. They block on the queue until seeds arrive.
	// Mode 3 skips this pool — it drives enumeration via runRecursiveWalk
	// (see seeding section below), which caps concurrency with a semaphore
	// instead of a fixed worker count.
	var listWg sync.WaitGroup
	listStart := time.Now()
	if cfg.ListType != 3 {
		for i := 0; i < cfg.ListConcurrency; i++ {
			listWg.Add(1)
			workerS3 := newWorker(pool, i, cfg, bucket, stats)
			lc := &localCounter{interval: cfg.ProgressInterval, printer: printer}
			onObject := func() { lc.incr(stats, "listed") }
			go lister.Run(listCtx, hardCtx, &listWg, objCh, i, workerS3, onObject)
		}
	}

	// Seeding.
	seeded := false
	// seedErr captures the first error from the seed loop (only the
	// -resume-list read can set it now) so it can be returned AFTER shutdown
	// flushes output and stats.
	var seedErr error
	if cfg.ListType == 1 {
		// Mode 1: root pagination with delimiter. Use a worker index past
		// the list+check ranges so node assignment does not collide.
		seedWorker := newWorker(pool, cfg.ListConcurrency+cfg.CheckConcurrency, cfg, bucket, stats)
		sa := startAfter
		continuationToken := ""
		abort := false
		for !abort {
			objs, subprefixes, next, err := seedWorker.ListPage(listCtx, prefix, sa, continuationToken, true, 1000)
			if err != nil {
				out.WriteListFailed(prefix, continuationToken)
				out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				stats.IncrListFailed()
				break
			}
			sa = "" // continuation tokens take over after the first page
			for _, o := range objs {
				if strings.Contains(o.Key, "|") {
					out.WriteInvalidKey(o.Key)
					stats.IncrInvalidKeys()
					continue
				}
				if cfg.IsCheck {
					select {
					case objCh <- resolveOffsets(o, cfg):
					case <-hardCtx.Done():
						// Hard abort mid-page: re-record the current page's
						// cursor so the resume re-lists this page (already-sent
						// objects land in check_failed via the cancelled
						// probes).
						hardErr := hardCtx.Err()
						out.WriteListFailed(prefix, continuationToken)
						out.WriteListFailedLog(prefix, extractHTTPStatusCode(hardErr), extractS3Code(hardErr), extractRequestID(hardErr), hardErr)
						stats.IncrListFailed()
						abort = true
					}
					if abort {
						break
					}
				} else {
					// list-only: lister is the sole counter, so main counts
					// and classifies root direct objects here. No check is
					// performed, so we only bump listed counters (no ok_*).
					if !isNormalETag(o.ETag) {
						stats.IncrListedMp()
					} else {
						stats.IncrListedObject()
					}
				}
			}
			if abort {
				break
			}
			for _, sp := range subprefixes {
				lister.Seed(sp)
				seeded = true
			}
			if next == "" {
				break
			}
			continuationToken = next
		}
	} else if cfg.ListType == 3 {
		// Mode 3: recursive semaphore-bounded walk from the root. No queue,
		// no list worker pool — one goroutine drives the walk and closes
		// objCh when done. -nextmarker is ignored (same limitation as Mode 2).
		walkS3 := newWorker(pool, cfg.ListConcurrency, cfg, bucket, stats)
		go func() {
			runRecursiveWalk(listCtx, hardCtx, walkS3, prefix, objCh, out, stats, cfg, nil)
			close(objCh)
			stats.SetListDuration(time.Since(listStart))
		}()
		seeded = true
	} else if resumeList != "" {
		// Mode 2 + -resume-list: read list_failed.txt lines of "prefix|token"
		// (or "prefix" when the first page failed), seed each (prefix, token)
		// into the BFS queue. Lister.processPrefix resumes listing from the
		// token, so previously successful pages are not re-listed.
		entries, err := readResumeList(resumeList, out, stats)
		if err != nil {
			seedErr = err
		} else {
			for _, e := range entries {
				lister.SeedWithToken(e.prefix, e.token)
				seeded = true
			}
		}
	} else {
		// Mode 2: ignore -nextmarker (documented limitation).
		lister.Seed(prefix)
		seeded = true
	}

	// If nothing was seeded, close the queue so list workers exit instead of
	// blocking forever on Pop.
	if !seeded {
		q.Close()
	}

	// Close objCh once all list workers are done. On an interrupt, the
	// never-started prefixes still queued are recorded to list_failed first
	// (each with its resume cursor where one exists). Mode 3 closes objCh
	// itself in the walk goroutine above, so skip this branch.
	if cfg.ListType != 3 {
		go func() {
			listWg.Wait()
			drainQueuedPrefixes(listCtx, q, out, stats)
			close(objCh)
			stats.SetListDuration(time.Since(listStart))
		}()
	}

	if cfg.IsCheck {
		checkWg.Wait()
	} else {
		// No check workers: drain objCh (should be empty — lister classifies
		// locally in list-only mode and main does not send to objCh).
		for range objCh {
		}
	}

	if err := out.Close(); err != nil {
		log.Printf("output close: %v", err)
	}
	stats.SetTotalDuration(time.Since(start))
	stats.PrintSummary(stdout, mode)
	if seedErr != nil {
		return seedErr
	}
	// An interrupted run (signal) is reported after the flush: the output is
	// a valid checkpoint, but the scan did not complete.
	if listCtx.Err() != nil {
		return errInterrupted
	}
	return nil
}

// newWorker builds a per-worker S3API bound to a single node via NodePool
// round-robin assignment. If every node is failed, Assign returns -1 and we
// fatal — the tool cannot run without at least one reachable endpoint.
func newWorker(pool *NodePool, workerIdx int, cfg *Config, bucket string, stats *Stats) S3API {
	nodeIdx := pool.Assign(workerIdx)
	if nodeIdx < 0 {
		log.Fatalf("no available nodes for worker %d", workerIdx)
	}
	client, err := NewMinioClient(pool.Endpoint(nodeIdx), cfg.AK, cfg.SK, cfg.Scheme == "https", cfg.IsCheck && cfg.MultipartCheckMode == MultipartCheckModeOffset)
	if err != nil {
		log.Fatalf("minio client (worker %d): %v", workerIdx, err)
	}
	return NewS3Client(client, bucket, stats, pool, nodeIdx, cfg)
}

// runBackup drives the -backup-file pipeline: upload the input list to the
// backup bucket, then check_concurrency workers consume BackupTasks
// (HEAD → type validation → relay). See BackupChecker for the per-task
// routing.
func runBackup(listCtx, hardCtx context.Context, cfg *Config, bucket, backupFile string, stdout io.Writer) error {
	pool := NewNodePool(cfg)
	startNodeRecovery(hardCtx, pool, cfg, bucket)
	out, err := NewBackupOutput(cfg, bucket)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	stats := NewStats()
	printer := NewProgressPrinter(stdout, ModeBackup)
	start := time.Now()

	// The list archive is the record of what this run intended to back up,
	// so it goes up FIRST — a run that dies mid-way still leaves its input
	// archived. Abort before touching any object if this fails. It runs on
	// listCtx: a signal during the archive upload aborts the run cleanly
	// before any object is relayed.
	if err := uploadBackupList(listCtx, pool, cfg, bucket, backupFile); err != nil {
		out.Close()
		return err
	}

	objChCap := cfg.ObjChCapacity
	if objChCap <= 0 {
		objChCap = cfg.CheckConcurrency * 4
		if objChCap < 2000 {
			objChCap = 2000
		}
	}
	ch := make(chan BackupTask, objChCap)
	printer.SetQueueSnapshotProvider(func() QueueSnapshot {
		lf, bok, bfail, mm := out.BackupChannelSnapshot()
		return QueueSnapshot{
			ObjCh:        len(ch),
			ListFailed:   lf,
			BackupOk:     bok,
			BackupFailed: bfail,
			Mismatch:     mm,
		}
	})

	var wg sync.WaitGroup
	for i := 0; i < cfg.CheckConcurrency; i++ {
		wg.Add(1)
		workerS3 := newWorker(pool, i, cfg, bucket, stats)
		lc := &localCounter{interval: cfg.ProgressInterval, printer: printer}
		go func(w S3API, lc *localCounter) {
			defer wg.Done()
			c := NewBackupChecker(hardCtx, w, out, stats, cfg)
			for task := range ch {
				c.Handle(task)
				lc.incr(stats, "backed")
			}
		}(workerS3, lc)
	}

	src := newFileSource[BackupTask](backupFile, bucket, out, stats, parseMixedLine)
	go func() {
		if err := src.Run(listCtx, ch); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("backup source: %v", err)
		}
		close(ch)
	}()

	wg.Wait()
	if err := out.Close(); err != nil {
		log.Printf("output close: %v", err)
	}
	stats.SetTotalDuration(time.Since(start))
	stats.PrintSummary(stdout, ModeBackup)
	// An interrupted run is reported after the flush; backup_failed.txt is
	// the retry input (already-processed objects can be excluded with a
	// comm diff against backup_ok/mismatch since every output line is the
	// raw input line).
	if listCtx.Err() != nil {
		return errInterrupted
	}
	return nil
}

// uploadBackupList archives the input list file into the backup bucket under
// .backup_lists/<basename>_<YYYYMMDD_HHMMSS>.txt. The file is STREAMED from
// disk — the corrupted-objects list is the disaster artifact and can be
// gigabytes, so it must never be fully buffered in memory.
func uploadBackupList(ctx context.Context, pool *NodePool, cfg *Config, bucket, backupFile string) error {
	// Worker index past the backup worker pool (0..check_concurrency-1) so
	// node assignment does not collide.
	w := newWorker(pool, cfg.CheckConcurrency, cfg, bucket, NewStats())
	base := strings.TrimSuffix(filepath.Base(backupFile), ".txt")
	listKey := fmt.Sprintf(".backup_lists/%s_%s.txt", base, time.Now().Format("20060102_150405"))
	if _, err := w.PutObjectLocal(ctx, cfg.BackupBucket, listKey, backupFile); err != nil {
		return fmt.Errorf("upload backup list to %s/%s: %w", cfg.BackupBucket, listKey, err)
	}
	return nil
}
