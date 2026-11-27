package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestListerMode2BFSPrefixes(t *testing.T) {
	// fake: root prefix returns 1 object + 1 sub-prefix; sub-prefix returns 1 object.
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/file1", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: []string{"root/sub/"}},
			},
			"root/sub/": {
				{objs: []ObjectInfo{{Key: "root/sub/file2", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: nil},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 2, CheckConcurrency: 2, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan VerifyTask, 8)
	// Seed bumps inflight before pushing so the first worker's Add(-1) does
	// not race ahead to zero before the prefix is processed.
	lister.Seed("root/")

	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()

	got := []string{}
	for t := range objCh {
		got = append(got, t.Key)
	}
	if len(got) != 2 {
		t.Errorf("got %d objects: %v", len(got), got)
	}
	// Check mode → lister bumps listed_obj before pushing. 2 normal objects.
	if got := stats.Snapshot().ListedObjects; got != 2 {
		t.Errorf("list_obj=%d want 2 (lister bumps in check mode)", got)
	}
}

// TestListerMode2CommonPrefixesOnlyPage reproduces the C1 silent-data-loss
// bug: a truncated page that returns zero Contents but non-empty
// CommonPrefixes. The pre-fix last-key heuristic computed next="" (because
// len(objs)==0), so the remaining sub-prefixes were never fetched. With the
// continuation-token cursor the second page is fetched and all sub-prefixes
// are enumerated.
func TestListerMode2CommonPrefixesOnlyPage(t *testing.T) {
	// Page 1: no objects, two sub-prefixes, truncated -> continuation token "tok1".
	// Page 2: no objects, one sub-prefix, not truncated.
	// Each sub-prefix's own list returns a single object.
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: nil, prefixes: []string{"root/a/", "root/b/"}, nextToken: "tok1"},
				{objs: nil, prefixes: []string{"root/c/"}, nextToken: ""},
			},
			"root/a/": {{objs: []ObjectInfo{{Key: "root/a/1", ETag: "0123456789abcdef0123456789abcdef"}}}},
			"root/b/": {{objs: []ObjectInfo{{Key: "root/b/2", ETag: "0123456789abcdef0123456789abcdef"}}}},
			"root/c/": {{objs: []ObjectInfo{{Key: "root/c/3", ETag: "0123456789abcdef0123456789abcdef"}}}},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 1, CheckConcurrency: 1, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan VerifyTask, 16)
	lister.Seed("root/")

	var wg sync.WaitGroup
	wg.Add(1)
	go lister.Run(&wg, objCh, 0, fake, nil)
	go func() {
		wg.Wait()
		close(objCh)
	}()

	got := map[string]bool{}
	for t := range objCh {
		got[t.Key] = true
	}
	want := []string{"root/a/1", "root/b/2", "root/c/3"}
	for _, k := range want {
		if !got[k] {
			t.Errorf("missing object %q (got %v)", k, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d objects want %d: %v", len(got), len(want), got)
	}
	// Two LIST calls were made on "root/" (page1 + page2 via continuation token).
	if fake.calls < 2 {
		t.Errorf("root LIST calls = %d want >= 2", fake.calls)
	}
	// Check mode → lister bumps listed_obj before pushing. 3 normal objects.
	if got := stats.Snapshot().ListedObjects; got != 3 {
		t.Errorf("list_obj=%d want 3 (lister bumps in check mode)", got)
	}
}

// scriptedS3 serves canned page results keyed by prefix. Each call for a
// prefix pops the first page result. The nextToken field is the
// continuation token returned to drive the next page request.
type pageResult struct {
	objs      []ObjectInfo
	prefixes  []string
	nextToken string
}
type scriptedS3 struct {
	pages map[string][]pageResult
	calls int
	// lastMaxKeys records the maxKeys of the most recent ListPage call.
	lastMaxKeys int
	mu          sync.Mutex
	// errOn, when non-nil, makes ListPage return the mapped error for the
	// given prefix instead of consulting pages. Used to exercise subtree
	// failure isolation in walker tests.
	errOn map[string]error
}

func (s *scriptedS3) ListPage(ctx context.Context, prefix, startAfter, continuationToken string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.errOn != nil {
		if err, ok := s.errOn[prefix]; ok {
			return nil, nil, "", err
		}
	}
	pages, ok := s.pages[prefix]
	if !ok || len(pages) == 0 {
		return nil, nil, "", nil
	}
	p := pages[0]
	s.pages[prefix] = pages[1:]
	s.lastMaxKeys = maxKeys
	return p.objs, p.prefixes, p.nextToken, nil
}

func (s *scriptedS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (s *scriptedS3) RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return nil, nil
}

func (s *scriptedS3) HeadObject(ctx context.Context, key string) (string, int64, error) {
	return "", 0, nil
}

func (s *scriptedS3) DownloadRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return nil, nil
}

func (s *scriptedS3) PutObjectLocal(ctx context.Context, bucket, key, path string) (string, error) {
	return "", nil
}

func (s *scriptedS3) PutObjectStream(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	return "", nil
}

func (s *scriptedS3) CreateMultipart(ctx context.Context, bucket, key string) (string, error) {
	return "", nil
}

func (s *scriptedS3) UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error) {
	return "", nil
}

func (s *scriptedS3) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []UploadedPart) (string, error) {
	return "", nil
}

func (s *scriptedS3) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	return nil
}

var _ S3API = (*scriptedS3)(nil)

// TestListerShortResponseWarns — a truncated page that returns fewer entries
// (objects + common prefixes) than the requested max_keys is a contradictory
// server state and a classic pagination-quirk signal. It must Warn into
// list_failed.log (not list_failed.txt — the page itself succeeded) and not
// count as a failure. maxKeys=0 (unconfigured, direct Config construction)
// disables the detection.
func TestListerShortResponseWarns(t *testing.T) {
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/a", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: []string{"root/sub/"}, nextToken: "cursor-1"},
			},
			"root/sub/": {
				{objs: []ObjectInfo{{Key: "root/sub/b", ETag: "0123456789abcdef0123456789abcdef"}}, nextToken: ""},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 2, CheckConcurrency: 2, IsCheck: true, MaxKeys: 1000}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)
	objCh := make(chan VerifyTask, 8)
	lister.Seed("root/")
	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()
	for range objCh {
	}
	if err := out.Close(); err != nil {
		t.Fatalf("out.Close: %v", err)
	}
	logBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.log"))
	if err != nil {
		t.Fatalf("read list_failed.log: %v", err)
	}
	logStr := string(logBytes)
	if !strings.Contains(logStr, "short response while truncated") {
		t.Errorf("no short-response warn in list_failed.log:\n%s", logStr)
	}
	if !strings.Contains(logStr, "returned=2") || !strings.Contains(logStr, "max_keys=1000") {
		t.Errorf("warn missing counts (returned=2 max_keys=1000):\n%s", logStr)
	}
	if got := stats.Snapshot().ListFailed; got != 0 {
		t.Errorf("ListFailed = %d, want 0 (a warn is not a failure)", got)
	}
}

// TestListerShortResponseDisabledWhenUnconfigured — MaxKeys=0 (direct Config
// construction without LoadConfig) must not fire the detection: the client
// does not know the effective page size, so "fewer than requested" is
// undecidable.
func TestListerShortResponseDisabledWhenUnconfigured(t *testing.T) {
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/a", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: []string{"root/sub/"}, nextToken: "cursor-1"},
			},
			"root/sub/": {
				{objs: []ObjectInfo{{Key: "root/sub/b", ETag: "0123456789abcdef0123456789abcdef"}}, nextToken: ""},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 2, CheckConcurrency: 2, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)
	objCh := make(chan VerifyTask, 8)
	lister.Seed("root/")
	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()
	for range objCh {
	}
	if err := out.Close(); err != nil {
		t.Fatalf("out.Close: %v", err)
	}
	logBytes, err := os.ReadFile(filepath.Join(dir, "list_failed.log"))
	if err != nil {
		t.Fatalf("read list_failed.log: %v", err)
	}
	if strings.Contains(string(logBytes), "short response") {
		t.Errorf("short-response warn fired with MaxKeys=0:\n%s", logBytes)
	}
}

// TestListerPassesConfiguredMaxKeys — the ListPage call must carry the
// configured max_keys (both the request size and the detection baseline).
func TestListerPassesConfiguredMaxKeys(t *testing.T) {
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/a", ETag: "0123456789abcdef0123456789abcdef"}}, nextToken: ""},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 1, ListConcurrency: 2, CheckConcurrency: 2, IsCheck: true, MaxKeys: 500}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)
	objCh := make(chan VerifyTask, 8)
	lister.Seed("root/")
	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()
	for range objCh {
	}
	if fake.lastMaxKeys != 500 {
		t.Errorf("ListPage maxKeys = %d, want 500 (cfg.MaxKeys)", fake.lastMaxKeys)
	}
}

// TestListerSlashSuffixKeyFlowsAsObject pins the no-common-prefix-heuristic
// invariant: a real object whose key ends in '/' flows to objCh as a normal
// VerifyTask, the same as any other key. The lister MUST NOT classify keys
// by suffix (e.g. "ends in /" → treat as a common prefix). The wire protocol
// already separates Contents from CommonPrefixes via independent XML tags;
// any client-side heuristic would silently drop or mis-route real objects
// (delete_marker_list_tool pitfall 11).
func TestListerSlashSuffixKeyFlowsAsObject(t *testing.T) {
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{
					objs: []ObjectInfo{
						{Key: "root/sub/", ETag: "0123456789abcdef0123456789abcdef"}, // real object, ends in '/'
						{Key: "root/file", ETag: "0123456789abcdef0123456789abcdef"},
					},
					prefixes: []string{"root/realprefix/"}, // a true common prefix from the wire
				},
			},
			"root/realprefix/": {
				{objs: []ObjectInfo{{Key: "root/realprefix/inner", ETag: "0123456789abcdef0123456789abcdef"}}},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 1, CheckConcurrency: 1, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan VerifyTask, 8)
	lister.Seed("root/")

	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()

	got := map[string]bool{}
	for t := range objCh {
		got[t.Key] = true
	}
	// The slash-suffix object MUST appear as a regular task. If a heuristic
	// ever re-classifies it as a common prefix, this assertion fires.
	if !got["root/sub/"] {
		t.Errorf("slash-suffix key 'root/sub/' missing from tasks (got %v); the lister must not classify keys by suffix", got)
	}
	if !got["root/file"] {
		t.Errorf("plain key 'root/file' missing from tasks: %v", got)
	}
	if !got["root/realprefix/inner"] {
		t.Errorf("common-prefix-descended object missing from tasks: %v", got)
	}
	// 3 objects total (2 root + 1 under the real common prefix). All counted
	// as listed_obj (no ETag trickery — single-segment ETag → "normal object").
	if got := stats.Snapshot().ListedObjects; got != 3 {
		t.Errorf("ListedObjects = %d, want 3 (slash-suffix counted as object): snapshot=%+v", got, stats.Snapshot())
	}
}

// TestListerSlashSuffixKeyFlowsAsObject_ListOnly is the list-only mode (IsCheck=false)
// counterpart. processPrefix routes objects to local classification (no objCh push),
// but the SAME per-object loop runs — a suffix heuristic would silently drop the
// slash-suffix object from the listed counters. Pin the counter.
func TestListerSlashSuffixKeyFlowsAsObject_ListOnly(t *testing.T) {
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{
					objs: []ObjectInfo{
						{Key: "root/sub/", ETag: "0123456789abcdef0123456789abcdef"},
						{Key: "root/file", ETag: "0123456789abcdef0123456789abcdef"},
					},
					prefixes: []string{"root/realprefix/"},
				},
			},
			"root/realprefix/": {
				{objs: []ObjectInfo{{Key: "root/realprefix/inner", ETag: "0123456789abcdef0123456789abcdef"}}},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 1, CheckConcurrency: 1, IsCheck: false}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan VerifyTask, 8) // unused in list-only mode, but Run still closes it
	lister.Seed("root/")

	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(&wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()
	for range objCh {
	}
	if got := stats.Snapshot().ListedObjects; got != 3 {
		t.Errorf("ListedObjects = %d, want 3 in list-only mode (slash-suffix counted as object): snapshot=%+v", got, stats.Snapshot())
	}
}
