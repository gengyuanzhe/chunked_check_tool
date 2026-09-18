// drain_test.go — tests for the two-stage shutdown / graceful-drain design:
// Queue.Drain, drainQueuedPrefixes seed round-tripping, the lister's
// graceful (next-page cursor) and hard (current-page cursor) abort
// recordings, the walker's never-started prefix recording, and the run-level
// integration (pre-cancelled listCtx → seeds land in list_failed, run
// returns errInterrupted).
package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// drainFake wraps scriptedS3 with the context behaviour the production
// client has but scriptedS3 lacks: once listCtx is done, every ListPage
// fails fast with the ctx error. blockToken (when non-empty) makes the
// ListPage call carrying that continuation token block until listCtx is
// done — used to deterministically cancel a worker between pages.
type drainFake struct {
	*scriptedS3
	listCtx    context.Context
	blockToken string
	once       sync.Once
}

func (d *drainFake) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	if d.blockToken != "" && continuationToken == d.blockToken {
		<-d.listCtx.Done()
		return nil, nil, "", d.listCtx.Err()
	}
	if d.listCtx.Err() != nil {
		return nil, nil, "", d.listCtx.Err()
	}
	return d.scriptedS3.ListPage(ctx, prefix, startAfter, continuationToken, delim, maxKeys)
}

var _ S3API = (*drainFake)(nil)

func TestQueueDrain(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b\x00tok1")
	if v, ok := q.Pop(context.Background()); !ok || v != "a" {
		t.Fatalf("Pop = %q,%v want a,true", v, ok)
	}
	items := q.Drain()
	if len(items) != 1 || items[0] != "b\x00tok1" {
		t.Fatalf("Drain = %v, want [b\x00tok1]", items)
	}
	if q.Len() != 0 {
		t.Errorf("Len after Drain = %d, want 0", q.Len())
	}
	// Drain on an already-empty (even closed) queue is a no-op.
	q.Close()
	if items := q.Drain(); len(items) != 0 {
		t.Errorf("Drain after close = %v, want none", items)
	}
}

// TestDrainQueuedPrefixesRecordsSeeds — never-started seeds round-trip into
// list_failed: bare prefixes as-is (resume = full re-list), -resume-list
// seeds with their cursor preserved (resume continues from where the
// previous run failed).
func TestDrainQueuedPrefixesRecordsSeeds(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	q := NewQueue()
	q.Push("plain/prefix")
	q.Push("resumed/\x00tok42")

	listCtx, cancel := context.WithCancel(context.Background())
	cancel()
	drainQueuedPrefixes(listCtx, q, out, stats)
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "plain/prefix\nresumed/|tok42\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q", got, want)
	}
	if got := stats.Snapshot().ListFailed; got != 2 {
		t.Errorf("ListFailed = %d, want 2", got)
	}
}

// TestDrainQueuedPrefixesNoOpWithoutCancel — normal completion (listCtx
// alive) must not record anything, even if the queue somehow still holds
// items.
func TestDrainQueuedPrefixesNoOpWithoutCancel(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	stats := NewStats()
	q := NewQueue()
	q.Push("leftover/")

	drainQueuedPrefixes(context.Background(), q, out, stats)
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	// list_failed.txt is pre-created (empty) by NewOutput; the drain must
	// not have written anything into it.
	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Errorf("list_failed.txt = %q, want empty (no interrupt happened)", data)
	}
	if got := stats.Snapshot().ListFailed; got != 0 {
		t.Errorf("ListFailed = %d, want 0", got)
	}
}

// TestListerGracefulCancelRecordsNextPageToken — first signal cancels
// listCtx while the worker holds a successful page's `next` cursor: the
// following ListPage fails and list_failed records (prefix, next) so the
// resume does NOT re-list the already-completed page.
func TestListerGracefulCancelRecordsNextPageToken(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	stats := NewStats()
	l := NewLister(NewQueue(), out, stats, cfg)

	const normal = "0123456789abcdef0123456789abcdef"
	listCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &drainFake{
		scriptedS3: &scriptedS3{pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/a", ETag: normal}}, nextToken: "tok2"},
				{objs: []ObjectInfo{{Key: "root/c", ETag: normal}}},
			},
		}},
		listCtx:    listCtx,
		blockToken: "tok2", // the second page blocks until the test cancels
	}

	objCh := make(chan VerifyTask, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(listCtx, context.Background(), "root/", "", objCh, fake, nil)
	}()
	<-objCh  // page 1 fetched and its object sent — worker now in ListPage #2
	cancel() // first signal
	<-done
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "root/|tok2\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q (resume cursor = next of last successful page)", got, want)
	}
	if got := stats.Snapshot().ListFailed; got != 1 {
		t.Errorf("ListFailed = %d, want 1", got)
	}
}

// TestListerHardAbortMidPageRecordsCurrentPageToken — second signal
// cancels hardCtx while a page's objects are still being sent: the failure
// records the CURRENT page's cursor (not the next one), so the resume
// re-lists the partially-emitted page and its unsent objects are recovered.
func TestListerHardAbortMidPageRecordsCurrentPageToken(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	stats := NewStats()
	l := NewLister(NewQueue(), out, stats, cfg)

	const normal = "0123456789abcdef0123456789abcdef"
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{
				{Key: "root/a", ETag: normal},
				{Key: "root/b", ETag: normal},
			}, nextToken: "tok2"},
		},
	}}

	objCh := make(chan VerifyTask, 1) // first send fills the buffer; second blocks
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(context.Background(), hardCtx, "root/", "", objCh, fake, nil)
	}()
	// Page 1 was fetched with cursor "" (first page). root/a sits in the
	// channel; the send of root/b blocks on the full buffer.
	waitFor(t, func() bool { return len(objCh) == 1 }, 2*time.Second)
	hardCancel()
	<-done
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "root/\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q (current page cursor re-lists the page; root/b is otherwise lost)", got, want)
	}
	if got := stats.Snapshot().ListFailed; got != 1 {
		t.Errorf("ListFailed = %d, want 1", got)
	}
}

// TestWalkerCancelledBeforeStartRecordsPrefix — a Mode 3 walk that never
// started (listCtx already cancelled) records the bare prefix: nothing under
// it was listed, so the resume must re-list it whole.
func TestWalkerCancelledBeforeStartRecordsPrefix(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 3, ListConcurrency: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	stats := NewStats()

	listCtx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &drainFake{
		scriptedS3: &scriptedS3{pages: map[string][]pageResult{
			"root/": {{objs: []ObjectInfo{{Key: "root/a", ETag: "0123456789abcdef0123456789abcdef"}}}},
		}},
		listCtx: listCtx,
	}

	objCh := make(chan VerifyTask, 1)
	runRecursiveWalk(listCtx, context.Background(), fake, "root/", objCh, out, stats, cfg, nil)
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "root/\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q", got, want)
	}
	if got := stats.Snapshot().ListFailed; got != 1 {
		t.Errorf("ListFailed = %d, want 1", got)
	}
	if len(objCh) != 0 {
		t.Errorf("objCh = %d tasks, want 0 (nothing listed)", len(objCh))
	}
}

// TestRunInterruptedDrainsQueue — run() with an already-cancelled listCtx:
// whichever way the race resolves (a worker pops the seed and its ListPage
// fails, or the seed stays queued and the closer goroutine drains it), the
// seed prefix lands in list_failed and run returns errInterrupted so the
// operator knows the scan is incomplete.
func TestRunInterruptedDrainsQueue(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - 127.0.0.1:1\n" +
		"scheme: http\nak: test\nsk: test\n" +
		"list_type: 2\nlist_api_version: 2\n" +
		"list_concurrency: 2\ncheck_concurrency: 2\n" +
		"output_dir: " + dir + "\nis_check: true\nprogress_interval: 1000\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	listCtx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf strings.Builder
	err = run(listCtx, context.Background(), cfg, "mybucket", "scanscope/", "", "", "", "", "", &buf)
	if !errors.Is(err, errInterrupted) {
		t.Fatalf("run err = %v, want errInterrupted", err)
	}
	// The seed prefix is recorded as a never-started (or first-page-failed)
	// listing: bare prefix, resumable via -resume-list.
	data, err := os.ReadFile(filepath.Join(cfg.OutputDir, "list_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "scanscope/\n"; got != want {
		t.Errorf("list_failed.txt = %q, want %q", got, want)
	}
	if !strings.Contains(buf.String(), "list_failed: 1") {
		t.Errorf("summary missing list_failed: 1\nfull:\n%s", buf.String())
	}
}
