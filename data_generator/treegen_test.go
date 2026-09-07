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

// newCfg builds a Config with the default segment prefixes that LoadConfig
// would apply, so tests construct Config literals without going through YAML.
func newCfg(prefix string, depth, width, files int) *Config {
	return &Config{
		Prefix:      prefix,
		Depth:       depth,
		Width:       width,
		FilesPerDir: files,
		LPrefix:     "l",
		DPrefix:     "d",
		FPrefix:     "file_",
	}
}

func TestWalkTree_Depth1_AllWidthLeaves(t *testing.T) {
	cfg := newCfg("data/", 1, 3, 2)
	got := collectKeys(WalkTree(context.Background(), cfg))
	// l1 (max depth) 同层 2 文件 + 3 个叶子 d 各 2 文件 = 8
	wantKeys := []string{
		"data/l1/file_1", "data/l1/file_2",
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
	cfg := newCfg("pfx", 2, 3, 1)
	got := collectKeys(WalkTree(context.Background(), cfg))
	// 每层 l 同层 1 文件 + 原 5 个 d 叶子 = 7
	wantKeys := []string{
		"pfx/l1/file_1",
		"pfx/l1/d1/file_1",
		"pfx/l1/d2/file_1",
		"pfx/l1/l2/file_1",
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
	// 总数公式 = (depth + (width-1)*(depth-1) + width) * files
	// 桥同层文件数 = depth*files，叶子目录文件数 = ((width-1)*(depth-1) + width) * files
	cases := []struct {
		depth, width, files int
		wantTotal           int
	}{
		{1, 2, 10, (1 + 1*0 + 2) * 10},   // 30
		{2, 2, 10, (2 + 1*1 + 2) * 10},   // 50
		{3, 4, 5, (3 + 3*2 + 4) * 5},     // 65
		{4, 4, 1, (4 + 3*3 + 4) * 1},     // 17
	}
	for _, tc := range cases {
		cfg := newCfg("", tc.depth, tc.width, tc.files)
		got := collectKeys(WalkTree(context.Background(), cfg))
		if len(got) != tc.wantTotal {
			t.Errorf("depth=%d width=%d files=%d: got %d keys, want %d",
				tc.depth, tc.width, tc.files, len(got), tc.wantTotal)
		}
	}
}

func TestWalkTree_EmptyPrefix(t *testing.T) {
	cfg := newCfg("", 1, 2, 1)
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantKeys := []string{"l1/file_1", "l1/d1/file_1", "l1/d2/file_1"}
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

func TestWalkTree_CustomSegmentPrefixes(t *testing.T) {
	cfg := &Config{
		Prefix:      "pfx",
		Depth:       2,
		Width:       3,
		FilesPerDir: 1,
		LPrefix:     "layer",
		DPrefix:     "dir",
		FPrefix:     "obj_",
	}
	got := collectKeys(WalkTree(context.Background(), cfg))
	wantKeys := []string{
		"pfx/layer1/obj_1",
		"pfx/layer1/dir1/obj_1",
		"pfx/layer1/dir2/obj_1",
		"pfx/layer1/layer2/obj_1",
		"pfx/layer1/layer2/dir1/obj_1",
		"pfx/layer1/layer2/dir2/obj_1",
		"pfx/layer1/layer2/dir3/obj_1",
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

func TestWalkTree_DefaultSegmentPrefixesUnchanged(t *testing.T) {
	cfg := newCfg("p", 1, 2, 1)
	got := collectKeys(WalkTree(context.Background(), cfg))
	// l1 同层文件先 emit
	if got[0].Key != "p/l1/file_1" {
		t.Errorf("default prefixes: first key = %q, want %q", got[0].Key, "p/l1/file_1")
	}
}

func TestWalkTree_FileNumberWidth(t *testing.T) {
	cfg := newCfg("p", 1, 2, 100)
	got := collectKeys(WalkTree(context.Background(), cfg))
	// l1 同层文件先 emit，零填充到 3 位
	wantFirst := "p/l1/file_001"
	if got[0].Key != wantFirst {
		t.Errorf("first key = %q, want %q (zero-padded to 3 digits)", got[0].Key, wantFirst)
	}
}

func TestWalkTree_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := newCfg("p", 5, 5, 1000)
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
