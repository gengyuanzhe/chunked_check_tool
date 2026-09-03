package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix")
	startAfter := flag.String("nextmarker", "", "start-after key (Mode 1 root pagination only)")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p] [-nextmarker key]")
		os.Exit(2)
	}

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, *bucket, *prefix, *startAfter); err != nil {
		log.Fatalf("run: %v", err)
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
// drops to zero (which closes the queue). listWg.Wait then closes objCh,
// which unblocks check workers. If nothing was seeded (e.g. Mode 1 root
// pagination returned no sub-prefixes), the queue is closed explicitly so
// list workers exit immediately.
func run(ctx context.Context, cfg *Config, bucket, prefix, startAfter string) error {
	pool := NewNodePool(cfg)
	out, err := NewOutput(cfg)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	stats := NewStats()
	printer := NewProgressPrinter(os.Stdout)
	start := time.Now()

	objChCap := cfg.CheckConcurrency * 4
	if objChCap < 2000 {
		objChCap = 2000
	}
	objCh := make(chan ObjectInfo, objChCap)
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

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
				c := NewChecker(w, out, stats, cfg.IsSuccessLog)
				for obj := range objCh {
					c.Handle(obj)
					lc.incr(stats, "checked")
				}
			}(workerS3, lc)
		}
	}

	// Spawn list workers. They block on the queue until seeds arrive.
	var listWg sync.WaitGroup
	listStart := time.Now()
	for i := 0; i < cfg.ListConcurrency; i++ {
		listWg.Add(1)
		workerS3 := newWorker(pool, i, cfg, bucket, stats)
		lc := &localCounter{interval: cfg.ProgressInterval, printer: printer}
		onObject := func() { lc.incr(stats, "listed") }
		go lister.Run(ctx, &listWg, objCh, i, workerS3, onObject)
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
				out.WriteListFailed(prefix, err.Error())
				stats.IncrListFailed()
				break
			}
			sa = "" // continuation tokens take over after the first page
			for _, o := range objs {
				if cfg.IsCheck {
					select {
					case objCh <- o:
					case <-ctx.Done():
						seedErr = ctx.Err()
						break
					}
					if seedErr != nil {
						break
					}
				} else {
					// list-only: lister is the sole counter, so main counts
					// and classifies root direct objects here too.
					stats.IncrListed()
					if !isNormalETag(o.ETag) {
						stats.IncrMultipart()
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

	// Close objCh once all list workers are done.
	go func() {
		listWg.Wait()
		close(objCh)
		stats.SetListDuration(time.Since(listStart))
	}()

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
	statsPath := filepath.Join(cfg.OutputDir, "stats.txt")
	if err := stats.WriteToFile(statsPath, cfg.IsCheck); err != nil {
		return fmt.Errorf("write stats: %w", err)
	}
	stats.PrintSummary(cfg.IsCheck)
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
