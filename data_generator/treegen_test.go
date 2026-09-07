package main

import (
	"context"
	"sort"
	"testing"
)

func collectKeys(ch <-chan ObjectKey) []ObjectKey {
	var out []ObjectKey
	for k := range ch {
		out = append(out, k)
	}
	return out
}

func TestWalkTree_Depth1_AllWidthLeaves(t *testing.T) {
	cfg := &Config{Prefix: "data/", Depth: 1, Width: 3, FilesPerDir: 2}
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantKeys := []string{
		"data/l1/d1/file_1", "data/l1/d1/file_2",
		"data/l1/d2/file_1", "data/l1/d2/file_2",
		"data/l1/d3/file_1", "data/l1/d3/file_2",
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d keys, want %d", len(got), len(wantKeys))
	}
	gotStrs := make([]string, len(got))
	for i, k := range got {
		gotStrs[i] = k.Key
		if k.Idx != i {
			t.Errorf("key %q Idx = %d, want %d", k.Key, k.Idx, i)
		}
	}
	sort.Strings(gotStrs)
	sort.Strings(wantKeys)
	for i := range wantKeys {
		if gotStrs[i] != wantKeys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotStrs[i], wantKeys[i])
		}
	}
}

func TestWalkTree_Depth2_BridgeAndLeaves(t *testing.T) {
	cfg := &Config{Prefix: "pfx", Depth: 2, Width: 3, FilesPerDir: 1}
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantKeys := []string{
		"pfx/l1/d1/file_1",
		"pfx/l1/d2/file_1",
		"pfx/l1/l2/d1/file_1",
		"pfx/l1/l2/d2/file_1",
		"pfx/l1/l2/d3/file_1",
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d keys, want %d; got=%v", len(got), len(wantKeys), keysToStrings(got))
	}
	gotSet := make(map[string]bool)
	for _, k := range got {
		gotSet[k.Key] = true
	}
	for _, w := range wantKeys {
		if !gotSet[w] {
			t.Errorf("missing key %q; got=%v", w, gotSet)
		}
	}
}

func TestWalkTree_TotalCount(t *testing.T) {
	cases := []struct {
		depth, width, files int
		wantLeaves          int
	}{
		{1, 2, 10, 2},  // max depth, all width leaves: 2
		{2, 2, 10, 3},  // (2-1)*(2-1) + 2 = 1 + 2 = 3
		{3, 4, 5, 10},  // (4-1)*(3-1) + 4 = 6 + 4 = 10
		{4, 4, 1, 13},  // (4-1)*(4-1) + 4 = 9 + 4 = 13
	}
	for _, tc := range cases {
		cfg := &Config{Prefix: "", Depth: tc.depth, Width: tc.width, FilesPerDir: tc.files}
		got := collectKeys(WalkTree(context.Background(), cfg))
		want := tc.wantLeaves * tc.files
		if len(got) != want {
			t.Errorf("depth=%d width=%d files=%d: got %d keys, want %d (leaves=%d)",
				tc.depth, tc.width, tc.files, len(got), want, tc.wantLeaves)
		}
	}
}

func TestWalkTree_EmptyPrefix(t *testing.T) {
	cfg := &Config{Prefix: "", Depth: 1, Width: 2, FilesPerDir: 1}
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantKeys := []string{"l1/d1/file_1", "l1/d2/file_1"}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d keys, want %d; got=%v", len(got), len(wantKeys), keysToStrings(got))
	}
	gotSet := make(map[string]bool)
	for _, k := range got {
		gotSet[k.Key] = true
	}
	for _, w := range wantKeys {
		if !gotSet[w] {
			t.Errorf("missing key %q", w)
		}
	}
}

func TestWalkTree_FileNumberWidth(t *testing.T) {
	cfg := &Config{Prefix: "p", Depth: 1, Width: 2, FilesPerDir: 100}
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantFirst := "p/l1/d1/file_001"
	if got[0].Key != wantFirst {
		t.Errorf("first key = %q, want %q (zero-padded to 3 digits)", got[0].Key, wantFirst)
	}
}

func TestWalkTree_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &Config{Prefix: "p", Depth: 5, Width: 5, FilesPerDir: 1000}
	ch := WalkTree(ctx, cfg)
	// read one then cancel
	<-ch
	cancel()
	// drain a few then expect channel close (or limited additional emissions)
	consumed := 0
	for range ch {
		consumed++
		if consumed > 2000 {
			t.Fatalf("channel did not close after cancel; consumed %d", consumed)
		}
	}
}

func keysToStrings(ks []ObjectKey) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = k.Key
	}
	return out
}
