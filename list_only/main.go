package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v3"
)

// Config is loaded from config.yaml. The program is list-only: no RangeGet,
// no multipart classification, no ETag inspection — just enumerate and count.
type Config struct {
	Endpoints   []string `yaml:"endpoints"`
	AK          string   `yaml:"ak"`
	SK          string   `yaml:"sk"`
	Bucket      string   `yaml:"bucket"`
	Prefix      string   `yaml:"prefix"`
	Mode        string   `yaml:"mode"`        // "bfs" or "walk"
	Concurrency int      `yaml:"concurrency"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(c.Endpoints) == 0 {
		return nil, fmt.Errorf("endpoints must not be empty")
	}
	if c.AK == "" || c.SK == "" {
		return nil, fmt.Errorf("ak/sk must not be empty")
	}
	if c.Bucket == "" {
		return nil, fmt.Errorf("bucket must not be empty")
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.Mode == "" {
		c.Mode = "bfs"
	}
	if c.Mode != "bfs" && c.Mode != "walk" && c.Mode != "iter" {
		return nil, fmt.Errorf("mode must be bfs, walk, or iter, got %q", c.Mode)
	}
	return &c, nil
}

// Stats holds atomic counters. objects and listCalls are reported directly;
// listLatencyNsSum combined with listCalls yields average latency.
type Stats struct {
	objects         atomic.Int64
	listCalls       atomic.Int64
	listLatencyNsSum atomic.Int64
}

func (s *Stats) addObjects(n int)    { s.objects.Add(int64(n)) }
func (s *Stats) addListCall(durNs int64) {
	s.listCalls.Add(1)
	s.listLatencyNsSum.Add(durNs)
}

// lister holds the shared state for enumeration: a pool of minio clients
// (one per endpoint, HTTP), the bucket, and stats. Each ListObjects call
// picks a random client from the pool — that's the "random IP" selection.
type lister struct {
	clients []*minio.Client
	bucket  string
	stats   *Stats
	// r guards the rand source. rand.Intn is not goroutine-safe; a mutex
	// is simpler than a Generator and the contention is negligible relative
	// to a network round-trip.
	rMu sync.Mutex
	r   *rand.Rand
}

func (l *lister) pick() *minio.Client {
	l.rMu.Lock()
	defer l.rMu.Unlock()
	return l.clients[l.r.Intn(len(l.clients))]
}

// listPageV1 issues one V1 LIST call with the "/" delimiter and returns
// the object keys, common-prefixes, and the next marker. V1 marker
// pagination: S3 returns NextMarker when a delimiter is used; we fall back
// to the last Contents key when NextMarker is empty (the no-delimiter case,
// not used here but kept for safety).
func (l *lister) listPageV1(ctx context.Context, prefix, marker string) ([]minio.ObjectInfo, []string, string, error) {
	client := l.pick()
	core := &minio.Core{Client: client}
	start := time.Now()
	result, err := core.ListObjects(l.bucket, prefix, marker, "/", 1000)
	l.stats.addListCall(time.Since(start).Nanoseconds())
	if err != nil {
		return nil, nil, "", err
	}
	next := ""
	if result.IsTruncated {
		if result.NextMarker != "" {
			next = result.NextMarker
		} else if len(result.Contents) > 0 {
			next = result.Contents[len(result.Contents)-1].Key
		}
	}
	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, cp := range result.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	return result.Contents, prefixes, next, nil
}

// runBFS enumerates the tree with a queue and a fixed worker pool. Each
// worker pops a prefix, lists it page-by-page (delimiter "/"), counts
// objects, and enqueues discovered CommonPrefixes. inflight tracks pending
// prefixes; the worker that drives it to zero closes the queue.
func runBFS(ctx context.Context, li *lister, cfg *Config) {
	q := newQueue()
	var inflight atomic.Int64
	var wg sync.WaitGroup

	// ctx watcher: on SIGINT the cond.Wait inside pop must be unblocked.
	// Closing the queue broadcasts and makes all blocked workers return
	// ("", false), so wg.Wait can proceed and main can print stats.
	go func() {
		<-ctx.Done()
		q.close()
	}()

	inflight.Add(1)
	q.push(cfg.Prefix)

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				prefix, ok := q.pop()
				if !ok {
					return
				}
				listOnePrefixBFS(ctx, li, q, &inflight, prefix)
				if inflight.Add(-1) == 0 {
					q.close()
					return
				}
			}
		}()
	}
	wg.Wait()
}

func listOnePrefixBFS(ctx context.Context, li *lister, q *queue, inflight *atomic.Int64, prefix string) {
	marker := ""
	for {
		if ctx.Err() != nil {
			return
		}
		objs, prefixes, next, err := li.listPageV1(ctx, prefix, marker)
		if err != nil {
			log.Printf("list %q: %v", prefix, err)
			return
		}
		li.stats.addObjects(len(objs))
		for _, p := range prefixes {
			inflight.Add(1)
			q.push(p)
		}
		if next == "" {
			return
		}
		marker = next
	}
}

// runWalk enumerates the tree with recursive goroutines bounded by a
// semaphore. Each walk holds one slot, lists page-by-page (delimiter "/"),
// counts objects, and spawns a child goroutine per CommonPrefix. A
// WaitGroup tracks live walks; when it reaches zero the tree is done.
func runWalk(ctx context.Context, li *lister, cfg *Config) {
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	var walk func(prefix string)
	walk = func(prefix string) {
		defer wg.Done()
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-sem }()

		marker := ""
		for {
			if ctx.Err() != nil {
				return
			}
			objs, prefixes, next, err := li.listPageV1(ctx, prefix, marker)
			if err != nil {
				log.Printf("list %q: %v", prefix, err)
				return
			}
			li.stats.addObjects(len(objs))
			for _, p := range prefixes {
				wg.Add(1)
				go walk(p)
			}
			if next == "" {
				return
			}
			marker = next
		}
	}

	wg.Add(1)
	go walk(cfg.Prefix)
	wg.Wait()
}

// ensureTrailingSlash normalizes a prefix so delimiter-based listing
// behaves predictably: S3 treats "a/b" and "a/b/" differently when
// listing with delimiter. Mirrors test_list_sub.go's helper.
func ensureTrailingSlash(prefix string) string {
	if prefix == "" {
		return ""
	}
	if strings.HasSuffix(prefix, "/") {
		return prefix
	}
	return prefix + "/"
}

// runIter enumerates the tree using minio's high-level ListObjectsIter
// channel API (Recursive: false = delimiter "/"). Unlike listPageV1,
// this does NOT manually handle marker pagination — minio-go paginates
// internally and streams results until the prefix is exhausted.
//
// The channel yields both real objects (Key not ending with "/") and
// "directory" entries (Key ending with "/", which are CommonPrefixes
// surfaced as synthetic objects). We count the former in stats.objects
// and recurse into the latter.
//
// list_calls semantic in this mode = number of ListObjectsIter
// invocations = number of prefixes listed (each invocation may internally
// issue multiple HTTP requests that we do not count). This matches
// test_list_sub.go's accounting.
func runIter(ctx context.Context, li *lister, cfg *Config) {
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	var walk func(prefix string)
	walk = func(prefix string) {
		defer wg.Done()
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-sem }()

		prefix = ensureTrailingSlash(prefix)
		client := li.pick()
		start := time.Now()
		var subdirs []string
		for obj := range client.ListObjectsIter(ctx, li.bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false,
		}) {
			if obj.Err != nil {
				log.Printf("list %q: %v", prefix, obj.Err)
				return
			}
			if obj.Key == "" {
				continue
			}
			if strings.HasSuffix(obj.Key, "/") {
				subdirs = append(subdirs, obj.Key)
			} else {
				li.stats.addObjects(1)
			}
		}
		li.stats.addListCall(time.Since(start).Nanoseconds())
		for _, p := range subdirs {
			wg.Add(1)
			go walk(p)
		}
	}

	wg.Add(1)
	go walk(cfg.Prefix)
	wg.Wait()
}

// queue is an unbounded string queue with ctx-aware blocking pop. It is
// closed when the BFS inflight counter hits zero.
type queue struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []string
	done bool
}

func (q *queue) push(s string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.buf = append(q.buf, s)
	q.cond.Broadcast()
}

func (q *queue) pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.buf) == 0 && !q.done {
		q.cond.Wait()
	}
	if len(q.buf) == 0 {
		return "", false
	}
	s := q.buf[0]
	q.buf = q.buf[1:]
	return s, true
}

func (q *queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.cond.Broadcast()
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func main() {
	cfgPath := flag.String("c", "config.yaml", "config yaml path")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	clients := make([]*minio.Client, 0, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		c, err := minio.New(ep, &minio.Options{
			Creds:        credentials.NewStaticV4(cfg.AK, cfg.SK, ""),
			Secure:       false, // HTTP only
			Transport:    &http.Transport{MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			BucketLookup: minio.BucketLookupAuto,
		})
		if err != nil {
			log.Fatalf("minio client %s: %v", ep, err)
		}
		clients = append(clients, c)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	li := &lister{
		clients: clients,
		bucket:  cfg.Bucket,
		stats:   &Stats{},
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	start := time.Now()
	switch cfg.Mode {
	case "bfs":
		runBFS(ctx, li, cfg)
	case "walk":
		runWalk(ctx, li, cfg)
	case "iter":
		runIter(ctx, li, cfg)
	}
	total := time.Since(start)

	objects := li.stats.objects.Load()
	listCalls := li.stats.listCalls.Load()
	latSum := li.stats.listLatencyNsSum.Load()
	var avgMs float64
	if listCalls > 0 {
		avgMs = float64(latSum) / float64(listCalls) / 1e6
	}
	listTotalSec := float64(latSum) / 1e9

	fmt.Printf("objects: %d\n", objects)
	fmt.Printf("list_calls: %d\n", listCalls)
	fmt.Printf("list_avg_latency_ms: %.2f\n", avgMs)
	fmt.Printf("list_total_duration_sec: %.2f\n", listTotalSec)
	fmt.Printf("total_duration_sec: %.2f\n", total.Seconds())
}
