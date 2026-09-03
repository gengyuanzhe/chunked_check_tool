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
	out, err := NewOutput(cfg)
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
		go lister.Run(context.Background(), &wg, objCh, i, fake)
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

// scriptedS3 serves canned page results keyed by prefix. Each call for a
// prefix pops the first page result; the nextAfter field is currently unused
// (each prefix is single-page in the test).
type pageResult struct {
	objs      []ObjectInfo
	prefixes  []string
	nextAfter string
}
type scriptedS3 struct {
	pages map[string][]pageResult
	calls int
	mu    sync.Mutex
}

func (s *scriptedS3) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	pages, ok := s.pages[prefix]
	if !ok || len(pages) == 0 {
		return nil, nil, "", nil
	}
	p := pages[0]
	s.pages[prefix] = pages[1:]
	return p.objs, p.prefixes, p.nextAfter, nil
}

func (s *scriptedS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

var _ S3API = (*scriptedS3)(nil)
