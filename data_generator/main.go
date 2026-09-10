package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfgPath := flag.String("c", "", "config file (required)")
	bktOverride := flag.String("bkt", "", "bucket override (optional)")
	flag.Parse()
	if *cfgPath == "" {
		log.Fatal("-c <config> is required")
	}
	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if *bktOverride != "" {
		cfg.Bucket = *bktOverride
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		log.Fatalf("mkdir output_dir: %v", err)
	}
	runLog, err := os.OpenFile(filepath.Join(cfg.OutputDir, "run.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open run.log: %v", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)
	log.SetOutput(mwErr)

	fmt.Fprintf(mwOut, "=== data_generator start ===\n")
	printConfigSnapshot(mwOut, cfg)

	pool := NewNodePool(cfg)
	uploader := NewS3Uploader(pool, cfg.AK, cfg.SK, cfg.Scheme == "https", cfg.UseTrailer)

	exists, err := uploader.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		log.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		log.Fatalf("bucket %q does not exist (create it first)", cfg.Bucket)
	}

	total := totalObjects(cfg)
	fmt.Fprintf(mwOut, "total_objects=%d (depth=%d width=%d files_per_dir=%d)\n",
		total, cfg.Depth, cfg.Width, cfg.FilesPerDir)

	stats := NewStats()
	progress := NewProgress(stats, cfg.ProgressInterval, mwOut)
	md5w, err := NewMD5Writer(filepath.Join(cfg.OutputDir, cfg.MD5File))
	if err != nil {
		log.Fatalf("open md5 file: %v", err)
	}

	start := time.Now()
	runErr := runWorkers(ctx, cfg, pool, uploader, stats, progress, md5w, mwErr)
	if cerr := md5w.Close(); cerr != nil && runErr == nil {
		runErr = cerr
	}
	elapsed := time.Since(start).Seconds()
	stats.PrintSummary(mwOut, elapsed)
	if runErr != nil {
		log.Fatalf("run: %v", runErr)
	}
	fmt.Fprintf(mwOut, "=== data_generator done ===\n")
}

func totalObjects(cfg *Config) int {
	leaves := (cfg.Width-1)*(cfg.Depth-1) + cfg.Width
	return leaves * cfg.FilesPerDir
}

func runWorkers(ctx context.Context, cfg *Config, pool *NodePool, uploader Uploader, stats *Stats, progress *Progress, md5w *MD5Writer, errW io.Writer) error {
	keyCh := WalkTree(ctx, cfg)
	var wg sync.WaitGroup
	baseSeed := time.Now().UnixNano()
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(baseSeed ^ int64(workerIdx)))
			for key := range keyCh {
				if ctx.Err() != nil {
					return
				}
				if err := processOne(ctx, cfg, pool, uploader, stats, progress, md5w, r, key); err != nil {
					fmt.Fprintf(errW, "[worker %d] key=%q err=%v\n", workerIdx, key.Key, err)
				}
			}
		}(w)
	}
	wg.Wait()
	return nil
}

func processOne(ctx context.Context, cfg *Config, pool *NodePool, uploader Uploader, stats *Stats, progress *Progress, md5w *MD5Writer, r *rand.Rand, key ObjectKey) error {
	sizeRange := cfg.ObjectSizeMax - cfg.ObjectSizeMin + 1
	size := cfg.ObjectSizeMin + r.Int63n(sizeRange)

	partRange := cfg.PartSizeMax - cfg.PartSizeMin + 1
	partSize := cfg.PartSizeMin + r.Int63n(partRange)

	// Stream random bytes through a TeeReader so the MD5 hasher sees the
	// exact bytes uploaded — no full-object buffer in memory. prngReader
	// draws from the same per-worker *rand.Rand sequentially.
	body := newPrngReader(r, size)
	hasher := md5.New()
	tee := io.TeeReader(body, hasher)

	endpointIdx := pool.Assign(key.Idx)
	var multipart bool
	if len(cfg.MultipartEndpointPattern) > 0 {
		if err := uploader.UploadObjectMultipart(ctx, cfg.Bucket, key.Key, tee, size, partSize, cfg.MultipartEndpointPattern); err != nil {
			stats.IncFailed()
			progress.Mark()
			return err
		}
		multipart = true
	} else {
		var err error
		multipart, err = uploader.UploadObject(ctx, endpointIdx, cfg.Bucket, key.Key, tee, size, partSize)
		if err != nil {
			stats.IncFailed()
			progress.Mark()
			return err
		}
	}
	md5hex := hex.EncodeToString(hasher.Sum(nil))

	stats.IncUploaded(size)
	if multipart {
		stats.IncMultipart()
	} else {
		stats.IncSingle()
	}
	if err := md5w.Write(MD5Record{Bucket: cfg.Bucket, Key: key.Key, MD5: md5hex}); err != nil {
		stats.IncFailed()
		progress.Mark()
		return fmt.Errorf("md5 write: %w", err)
	}
	progress.Mark()
	return nil
}

// prngReader streams exactly `size` bytes from a *rand.Rand. Reads beyond
// size return EOF. Used as the body source for streaming uploads so the
// object never needs to be fully materialized in memory.
type prngReader struct {
	r    *rand.Rand
	size int64
	read int64
}

func newPrngReader(r *rand.Rand, size int64) *prngReader {
	return &prngReader{r: r, size: size}
}

func (p *prngReader) Read(out []byte) (int, error) {
	remaining := p.size - p.read
	if remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(out))
	if n > remaining {
		n = remaining
	}
	nn, err := p.r.Read(out[:n])
	p.read += int64(nn)
	return nn, err
}

func printConfigSnapshot(w io.Writer, cfg *Config) {
	fmt.Fprintf(w, "config:\n")
	fmt.Fprintf(w, "  endpoints: %v\n", cfg.Endpoints)
	fmt.Fprintf(w, "  scheme: %s\n", cfg.Scheme)
	fmt.Fprintf(w, "  ak: %s\n", mask(cfg.AK))
	fmt.Fprintf(w, "  sk: ***\n")
	fmt.Fprintf(w, "  bucket: %s\n", cfg.Bucket)
	fmt.Fprintf(w, "  prefix: %q\n", cfg.Prefix)
	fmt.Fprintf(w, "  depth: %d  width: %d  files_per_dir: %d\n", cfg.Depth, cfg.Width, cfg.FilesPerDir)
	fmt.Fprintf(w, "  object_size_min: %d  max: %d\n", cfg.ObjectSizeMin, cfg.ObjectSizeMax)
	fmt.Fprintf(w, "  part_size_min: %d  max: %d\n", cfg.PartSizeMin, cfg.PartSizeMax)
	fmt.Fprintf(w, "  output_dir: %s  md5_file: %s\n", cfg.OutputDir, cfg.MD5File)
	fmt.Fprintf(w, "  concurrency: %d  progress_interval: %d\n", cfg.Concurrency, cfg.ProgressInterval)
	fmt.Fprintf(w, "  use_trailer: %v\n", cfg.UseTrailer)
	if len(cfg.MultipartEndpointPattern) > 0 {
		fmt.Fprintf(w, "  multipart_endpoint_pattern: %v\n", cfg.MultipartEndpointPattern)
	}
}

func mask(s string) string {
	if len(s) <= 4 {
		return "***"
	}
	return s[:2] + "***" + s[len(s)-2:]
}
