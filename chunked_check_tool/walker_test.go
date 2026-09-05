package main

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWalkerHappyPath drives a recursive tree through runRecursiveWalk and
// verifies every object in every visited prefix lands on objCh exactly once.
func TestWalkerHappyPath(t *testing.T) {
	tree := map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/file0", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: []string{"root/a/", "root/b/"}},
		},
		"root/a/": {
			{objs: []ObjectInfo{
				{Key: "root/a/1", ETag: "0123456789abcdef0123456789abcdef"},
				{Key: "root/a/2", ETag: "0123456789abcdef0123456789abcdef"},
			}, prefixes: []string{"root/a/sub/"}},
		},
		"root/a/sub/": {
			{objs: []ObjectInfo{{Key: "root/a/sub/1", ETag: "0123456789abcdef0123456789abcdef"}}},
		},
		"root/b/": {
			{objs: []ObjectInfo{{Key: "root/b/1", ETag: "0123456789abcdef0123456789abcdef"}}},
		},
	}
	fake := &scriptedS3{pages: tree}

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 3, ListConcurrency: 4, CheckConcurrency: 1, IsCheck: true}
	out, err := NewOutput(cfg)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()

	objCh := make(chan ObjectInfo, 16)
	runRecursiveWalk(context.Background(), fake, "root/", objCh, out, stats, cfg, nil)
	close(objCh)

	got := []string{}
	for o := range objCh {
		got = append(got, o.Key)
	}
	sort.Strings(got)
	want := []string{"root/a/1", "root/a/2", "root/a/sub/1", "root/b/1", "root/file0"}
	if len(got) != len(want) {
		t.Fatalf("got %d objects %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%q want %q", i, got[i], want[i])
		}
	}
	if stats.Snapshot().ListFailed != 0 {
		t.Errorf("ListFailed=%d want 0", stats.Snapshot().ListFailed)
	}
}

// TestWalkerConcurrencyCap verifies the semaphore bounds concurrent
// ListPage calls to cfg.ListConcurrency. The root call returns immediately
// (so it can spawn sub-walks); each sub-prefix call blocks on `release`
// until the test observes the peak and closes it.
func TestWalkerConcurrencyCap(t *testing.T) {
	const cap_ = 3
	var inflight atomic.Int32
	var maxInflight atomic.Int32

	release := make(chan struct{})
	tree := map[string][]pageResult{
		"root/": {
			{prefixes: []string{"root/a/", "root/b/", "root/c/", "root/d/"}},
		},
	}
	for _, p := range []string{"root/a/", "root/b/", "root/c/", "root/d/"} {
		tree[p] = []pageResult{{objs: []ObjectInfo{{Key: p + "x", ETag: "0123456789abcdef0123456789abcdef"}}}}
	}

	fake := &countingS3{
		pages:       tree,
		inflight:    &inflight,
		max:         &maxInflight,
		release:     release,
		blockPrefix: map[string]bool{"root/a/": true, "root/b/": true, "root/c/": true, "root/d/": true},
	}

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 3, ListConcurrency: cap_, CheckConcurrency: 1, IsCheck: true}
	out, err := NewOutput(cfg)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	objCh := make(chan ObjectInfo, 16)

	done := make(chan struct{})
	go func() {
		runRecursiveWalk(context.Background(), fake, "root/", objCh, out, stats, cfg, nil)
		close(done)
	}()

	// Wait until all cap_ sub-walks have entered ListPage simultaneously.
	waitFor(t, func() bool { return maxInflight.Load() >= int32(cap_) }, 2*time.Second)
	close(release)
	<-done
	close(objCh)
	for range objCh {
	}

	if max := maxInflight.Load(); max > int32(cap_) {
		t.Errorf("peak in-flight ListPage calls = %d, cap = %d", max, cap_)
	}
}

// TestWalkerSubtreeFailureIsolation verifies that when one sub-prefix's
// ListPage errors, the failure is recorded in list_failed and the list_sub
// branches are still enumerated.
func TestWalkerSubtreeFailureIsolation(t *testing.T) {
	tree := map[string][]pageResult{
		"root/": {
			{prefixes: []string{"root/ok/", "root/bad/"}},
		},
		"root/ok/": {
			{objs: []ObjectInfo{{Key: "root/ok/1", ETag: "0123456789abcdef0123456789abcdef"}}},
		},
	}
	fake := &scriptedS3{pages: tree}
	fake.errOn = map[string]error{"root/bad/": errInjected}

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 3, ListConcurrency: 2, CheckConcurrency: 1, IsCheck: true}
	out, err := NewOutput(cfg)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	objCh := make(chan ObjectInfo, 8)
	runRecursiveWalk(context.Background(), fake, "root/", objCh, out, stats, cfg, nil)
	close(objCh)

	got := []string{}
	for o := range objCh {
		got = append(got, o.Key)
	}
	if len(got) != 1 || got[0] != "root/ok/1" {
		t.Errorf("got %v, want [root/ok/1]", got)
	}
	if stats.Snapshot().ListFailed != 1 {
		t.Errorf("ListFailed=%d want 1", stats.Snapshot().ListFailed)
	}
}

var errInjected = newErr("injected list failure")

type errString string

func (e errString) Error() string { return string(e) }

func newErr(s string) error { return errString(s) }

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %v", timeout)
}

// countingS3 wraps scriptedS3 with in-flight instrumentation. On every
// ListPage call it increments inflight, records the peak, and — if the
// prefix is in blockPrefix — blocks on the release channel before
// decrementing and returning. Calls for prefixes not in blockPrefix
// return immediately, which lets the root call unblock so its parent
// walk can spawn sub-walks.
type countingS3 struct {
	pages       map[string][]pageResult
	inflight    *atomic.Int32
	max         *atomic.Int32
	release     <-chan struct{}
	blockPrefix map[string]bool
	mu          sync.Mutex
}

func (c *countingS3) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	cur := c.inflight.Add(1)
	for {
		old := c.max.Load()
		if cur <= old || c.max.CompareAndSwap(old, cur) {
			break
		}
	}
	if c.blockPrefix[prefix] {
		select {
		case <-c.release:
		case <-ctx.Done():
		}
	}
	c.inflight.Add(-1)

	c.mu.Lock()
	defer c.mu.Unlock()
	pages, ok := c.pages[prefix]
	if !ok || len(pages) == 0 {
		return nil, nil, "", nil
	}
	p := pages[0]
	c.pages[prefix] = pages[1:]
	return p.objs, p.prefixes, p.nextToken, nil
}

func (c *countingS3) RangeGet(ctx context.Context, key string) ([]byte, error) { return nil, nil }

var _ S3API = (*countingS3)(nil)
