package main

import (
	"context"
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
	out, err := NewOutput(cfg, "test-bkt")
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan ObjectInfo, 8)
	// Seed bumps inflight before pushing so the first worker's Add(-1) does
	// not race ahead to zero before the prefix is processed.
	lister.Seed("root/")

	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(context.Background(), &wg, objCh, i, fake, nil)
	}
	go func() {
		wg.Wait()
		close(objCh)
	}()

	got := []string{}
	for o := range objCh {
		got = append(got, o.Key)
	}
	if len(got) != 2 {
		t.Errorf("got %d objects: %v", len(got), got)
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
	out, err := NewOutput(cfg, "test-bkt")
	if err != nil {
		t.Fatalf("NewOutput: %v", err)
	}
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(q, out, stats, cfg)

	objCh := make(chan ObjectInfo, 16)
	lister.Seed("root/")

	var wg sync.WaitGroup
	wg.Add(1)
	go lister.Run(context.Background(), &wg, objCh, 0, fake, nil)
	go func() {
		wg.Wait()
		close(objCh)
	}()

	got := map[string]bool{}
	for o := range objCh {
		got[o.Key] = true
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
	mu    sync.Mutex
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
	return p.objs, p.prefixes, p.nextToken, nil
}

func (s *scriptedS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (s *scriptedS3) RangeGetAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return nil, nil
}

var _ S3API = (*scriptedS3)(nil)
