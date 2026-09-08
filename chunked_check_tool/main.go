package main

import (
	"bytes"
	"context"
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

func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix")
	startAfter := flag.String("nextmarker", "", "start-after key (Mode 1 root pagination only)")
	listFile := flag.String("list-file", "", "list file path: lines of bkt|key|partcnt|offset0|offset1|... (bypasses S3 listing; requires is_check=true)")
	backupFile := flag.String("backup-file", "", "backup list file path: mixed lines of bkt|key (regular) or bkt|key|partcnt|offset0|... (multipart); heads each object, verifies multipart corruption, and copies corrupt objects into backup_bucket")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p] [-nextmarker key]")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -list-file <path>")
		fmt.Fprintln(os.Stderr, "       -c config.yaml -bkt <bucket> -backup-file <path>   (requires backup_bucket in config)")
		os.Exit(2)
	}

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if *listFile != "" && !cfg.IsCheck {
		fmt.Fprintln(os.Stderr, "usage: -list-file requires -c config with is_check=true")
		os.Exit(2)
	}
	if *listFile != "" && *backupFile != "" {
		fmt.Fprintln(os.Stderr, "usage: -list-file and -backup-file are mutually exclusive")
		os.Exit(2)
	}
	if *backupFile != "" && cfg.BackupBucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -backup-file requires -c config with backup_bucket=<name>")
		os.Exit(2)
	}

	// Open run.log at output_dir root and tee stdout+stderr into it. We
	// open it before NewOutput so that NewOutput's mkdir error (if any)
	// is also captured. The dir may not exist yet, so mkdir here first.
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		log.Fatalf("mkdir output dir: %v", err)
	}
	runLogPath := filepath.Join(cfg.OutputDir, "run.log")
	runLog, err := os.OpenFile(runLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open run.log: %v", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)
	log.SetOutput(mwErr)

	printConfig(mwOut, *cfgPath, cfg, *bucket, *prefix, *startAfter, *listFile, *backupFile, runLogPath)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, *bucket, *prefix, *startAfter, *listFile, *backupFile, mwOut); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// printConfig writes a config snapshot to w at startup. sk is masked so the
// run.log (which persists) doesn't leak the secret; ak is shown in clear.
// Each line is indented 4 spaces for readability against the progress/summary
// lines that surround it in run.log.
func printConfig(w io.Writer, cfgPath string, cfg *Config, bucket, prefix, startAfter, listFile, backupFile, runLogPath string) {
	fmt.Fprintf(w, "=== config ===\n")
	fmt.Fprintf(w, "    config: %s\n", cfgPath)
	fmt.Fprintf(w, "    bucket: %s\n", bucket)
	fmt.Fprintf(w, "    prefix: %s\n", orEmpty(prefix))
	fmt.Fprintf(w, "    nextmarker: %s\n", orEmpty(startAfter))
	fmt.Fprintf(w, "    list_file: %s\n", orEmpty(listFile))
	fmt.Fprintf(w, "    backup_file: %s\n", orEmpty(backupFile))
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
	fmt.Fprintf(w, "    is_check: %t\n", cfg.IsCheck)
	fmt.Fprintf(w, "    is_success_log: %t\n", cfg.IsSuccessLog)
	fmt.Fprintf(w, "    is_multipart_segment_check: %t\n", cfg.IsMultipartSegmentCheck)
	fmt.Fprintf(w, "    multipart_segment_size: %d\n", cfg.MultipartSegmentSize)
	fmt.Fprintf(w, "    is_multipart_success_log: %t\n", cfg.IsMultipartSuccessLog)
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
// drops to zero (which closes the queue). listWg.Wait then closes objCh,
// which unblocks check workers. If nothing was seeded (e.g. Mode 1 root
// pagination returned no sub-prefixes), the queue is closed explicitly so
// list workers exit immediately.
func run(ctx context.Context, cfg *Config, bucket, prefix, startAfter, listFile, backupFile string, stdout io.Writer) error {
	// Backup mode: an entirely separate pipeline (HEAD + verify + copy);
	// see runBackup. Dispatched before any list/check wiring.
	if backupFile != "" {
		return runBackup(ctx, cfg, bucket, backupFile, stdout)
	}

	pool := NewNodePool(cfg)
	out, err := NewOutput(cfg, bucket)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	stats := NewStats()
	printer := NewProgressPrinter(stdout)
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
		cor, mpAll, cmp, mpOk, mcf, lf, cf, su := out.ChannelSnapshot()
		return QueueSnapshot{
			Prefix:           q.Len(),
			ObjCh:            len(objCh),
			CorruptedObjects: cor,
			OkMp:             mpAll + mpOk,
			CorruptedMp:      cmp,
			ListFailed:       lf,
			CheckFailed:      cf,
			MpCheckFailed:    mcf,
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
				c := NewChecker(w, out, stats, cfg)
				for obj := range objCh {
					c.Handle(obj)
					lc.incr(stats, "checked")
				}
			}(workerS3, lc)
		}
	}

	// List-file source: bypass S3 listing entirely. Read the file line-by-line,
	// push VerifyTasks to objCh. No list workers, no queue, no Lister. is_check
	// must be true (validated in main).
	if listFile != "" {
		listStart := time.Now()
		src := newListFileSource(listFile, bucket, out, stats)
		go func() {
			if err := src.Run(ctx, objCh); err != nil {
				log.Printf("list-file source: %v", err)
			}
			close(objCh)
			stats.SetListDuration(time.Since(listStart))
		}()
		checkWg.Wait()
		if err := out.Close(); err != nil {
			log.Printf("output close: %v", err)
		}
		stats.SetTotalDuration(time.Since(start))
		stats.PrintSummary(stdout, cfg.IsCheck, false)
		// Return ctx.Err() (nil on happy path, context.Canceled on SIGINT)
		// so main logs the interrupt and exits non-zero, matching the S3
		// mode's seedErr path. Output and stats are flushed above first.
		return ctx.Err()
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
			go lister.Run(ctx, &listWg, objCh, i, workerS3, onObject)
		}
	}

	// Seeding.
	seeded := false
	// seedErr captures the first error from the seed loop (including
	// ctx.Err() on SIGINT) so it can be returned AFTER shutdown flushes
	// output and stats.
	var seedErr error
	if cfg.ListType == 1 {
		// Mode 1: root pagination with delimiter. Use a worker index past
		// the list+check ranges so node assignment does not collide.
		seedWorker := newWorker(pool, cfg.ListConcurrency+cfg.CheckConcurrency, cfg, bucket, stats)
		sa := startAfter
		continuationToken := ""
		for {
			objs, subprefixes, next, err := seedWorker.ListPage(ctx, prefix, sa, continuationToken, true, 1000)
			if err != nil {
				out.WriteListFailed(prefix)
				out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				stats.IncrListFailed()
				break
			}
			sa = "" // continuation tokens take over after the first page
			for _, o := range objs {
				if cfg.IsCheck {
					select {
					case objCh <- resolveOffsets(o, cfg):
					case <-ctx.Done():
						seedErr = ctx.Err()
						break
					}
					if seedErr != nil {
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
			if seedErr != nil {
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
			runRecursiveWalk(ctx, walkS3, prefix, objCh, out, stats, cfg, nil)
			close(objCh)
			stats.SetListDuration(time.Since(listStart))
		}()
		seeded = true
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

	// Close objCh once all list workers are done. Mode 3 closes objCh
	// itself in the walk goroutine above, so skip this branch.
	if cfg.ListType != 3 {
		go func() {
			listWg.Wait()
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
	stats.PrintSummary(stdout, cfg.IsCheck, false)
	// Return the seed-loop error (e.g. ctx.Err() on SIGINT) AFTER shutdown
	// has flushed buffered output and written stats. main logs the interrupt
	// and exits non-zero, but no data is lost.
	return seedErr
}

// newWorker builds a per-worker S3API bound to a single node via NodePool
// round-robin assignment. If every node is failed, Assign returns -1 and we
// fatal — the tool cannot run without at least one reachable endpoint.
func newWorker(pool *NodePool, workerIdx int, cfg *Config, bucket string, stats *Stats) S3API {
	nodeIdx := pool.Assign(workerIdx)
	if nodeIdx < 0 {
		log.Fatalf("no available nodes for worker %d", workerIdx)
	}
	client, err := NewMinioClient(pool.Endpoint(nodeIdx), cfg.AK, cfg.SK, cfg.Scheme == "https")
	if err != nil {
		log.Fatalf("minio client (worker %d): %v", workerIdx, err)
	}
	return NewS3Client(client, bucket, stats, pool, nodeIdx, cfg)
}

// runBackup drives the -backup-file pipeline: upload the input list to the
// backup bucket, then check_concurrency workers consume BackupTasks
// (HEAD → type validation → multipart verify → copy). See BackupChecker
// for the per-task routing.
func runBackup(ctx context.Context, cfg *Config, bucket, backupFile string, stdout io.Writer) error {
	pool := NewNodePool(cfg)
	out, err := NewBackupOutput(cfg, bucket)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	stats := NewStats()
	printer := NewProgressPrinter(stdout)
	start := time.Now()

	// The list archive is the record of what this run intended to back up,
	// so it goes up FIRST — a run that dies mid-way still leaves its input
	// archived. Abort before touching any object if this fails.
	if err := uploadBackupList(ctx, pool, cfg, bucket, backupFile); err != nil {
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

	var wg sync.WaitGroup
	for i := 0; i < cfg.CheckConcurrency; i++ {
		wg.Add(1)
		workerS3 := newWorker(pool, i, cfg, bucket, stats)
		lc := &localCounter{interval: cfg.ProgressInterval, printer: printer}
		go func(w S3API, lc *localCounter) {
			defer wg.Done()
			c := NewBackupChecker(w, out, stats, cfg)
			for task := range ch {
				c.Handle(task)
				lc.incr(stats, "backed")
			}
		}(workerS3, lc)
	}

	src := newBackupSource(backupFile, bucket, out, stats)
	go func() {
		if err := src.Run(ctx, ch); err != nil {
			log.Printf("backup source: %v", err)
		}
		close(ch)
	}()

	wg.Wait()
	if err := out.Close(); err != nil {
		log.Printf("output close: %v", err)
	}
	stats.SetTotalDuration(time.Since(start))
	stats.PrintSummary(stdout, true, true)
	// ctx.Err() (nil on happy path, context.Canceled on SIGINT) so main logs
	// the interrupt and exits non-zero, matching the other modes.
	return ctx.Err()
}

// uploadBackupList archives the input list file into the backup bucket under
// .backup_lists/<basename>_<YYYYMMDD_HHMMSS>.txt.
func uploadBackupList(ctx context.Context, pool *NodePool, cfg *Config, bucket, backupFile string) error {
	data, err := os.ReadFile(backupFile)
	if err != nil {
		return fmt.Errorf("read backup list %q: %w", backupFile, err)
	}
	// Worker index past the backup worker pool (0..check_concurrency-1) so
	// node assignment does not collide.
	w := newWorker(pool, cfg.CheckConcurrency, cfg, bucket, NewStats())
	base := strings.TrimSuffix(filepath.Base(backupFile), ".txt")
	listKey := fmt.Sprintf(".backup_lists/%s_%s.txt", base, time.Now().Format("20060102_150405"))
	if err := w.PutObject(ctx, cfg.BackupBucket, listKey, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("upload backup list to %s/%s: %w", cfg.BackupBucket, listKey, err)
	}
	return nil
}
