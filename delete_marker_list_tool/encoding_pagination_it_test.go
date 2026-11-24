//go:build integration

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestIntegration_EncodingTypeURL_PaginationRoundTrip 验证 encoding_type_url=true
// 时的翻页 marker 回传闭环。
//
// 服务端（minio）语义（cmd/api-resources.go getListBucketObjectVersionsArgs）：
// key-marker 请求参数只做一次标准 query 解码，直接按原始 key 使用——客户端必须
// 回传"解码态"的 marker。服务端响应侧 NextKeyMarker 用 s3EncodeName 编码
// （空格→%20、!→%21、~→%7E、非 ASCII 逐字节 %XX；'/' 与字母数字不编码）。
//
// 数据布局（每 key put+delete = 2 版本，delete marker 在前）：max_keys=3 强制分页，
// 页边界 key 刻意选含"会被编码字符"的 key：
//
//	a1 < "b c" < "b!x" < "b~" < "中" < "zz"   （字典序）
//
// 断言（两种 encoding 下都应成立）：6 个 delete marker 各出现恰好一次，
// delete_markers.txt 的 key 字段为 wire 编码态（encoding=true 时）。
//
// 若客户端把编码态 NextKeyMarker 原样回传（不解码），服务端按原始 key 用 marker：
//   - "b c" 页边界：编码 marker "b%20c" > "b!x"（'%'=0x25 > '!'=0x21）→ b!x 被跳过（漏）；
//   - "中" 页边界：编码 marker "%E4%B8%AD" < "中"（0xE4 > 0x25）→ 中 被重复列举（重）。
func TestIntegration_EncodingTypeURL_PaginationRoundTrip(t *testing.T) {
	seedKeys := []string{"a1", "b c", "b!x", "b~", "中", "zz"}
	// wire 编码态（minio s3URLEncode，QueryEscape 风格）：'/'、字母数字不编码，
	// 空格→'+'，其余 %XX。
	wantEncoded := []string{"a1", "b+c", "b%21x", "b%7E", "%E4%B8%AD", "zz"}

	t.Run("control_encoding_off", func(t *testing.T) {
		keys := seedAndScan(t, seedKeys, false)
		assertKeySet(t, keys, seedKeys, "encoding=false")
	})

	t.Run("encoding_on_roundtrip", func(t *testing.T) {
		keys := seedAndScan(t, seedKeys, true)
		assertKeySet(t, keys, wantEncoded, "encoding=true")
	})
}

// seedAndScan 建版本化桶、对每个 key put+delete 制造 delete marker，
// 用给定 encoding 配置跑完整扫描，返回 delete_markers.txt 的 key 字段列表。
func seedAndScan(t *testing.T, keys []string, encoding bool) []string {
	t.Helper()
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("list-tool-encpg-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	for _, k := range keys {
		putObject(t, admin, bucket, k, "v1")
		removeObject(t, admin, bucket, k)
	}

	dir := t.TempDir()
	outDir := filepath.Join(dir, "out")
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + endpoint,
		"scheme: http",
		"ak: " + ak,
		"sk: " + sk,
		"list_type: 1",
		"max_keys: 3",
		"list_concurrency: 1",
		"output_dir: " + outDir,
		"output_dir_timestamp: false",
		"progress_interval: 1000",
		"debug_log: true",
		fmt.Sprintf("encoding_type_url: %v", encoding),
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, bucket, []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatalf("run: %v", err)
	}

	dm, err := os.ReadFile(filepath.Join(outDir, "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var gotKeys []string
	for _, line := range strings.Split(strings.TrimRight(string(dm), "\n"), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\x1f")
		if len(parts) != 4 {
			t.Fatalf("malformed row %q: want 4 fields, got %d", line, len(parts))
		}
		gotKeys = append(gotKeys, parts[1])
	}
	return gotKeys
}

// assertKeySet 断言 key 集合精确等于 want（顺序无关）且无重复。
func assertKeySet(t *testing.T, got, want []string, mode string) {
	t.Helper()
	gotSorted := append([]string(nil), got...)
	sort.Strings(gotSorted)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)

	if len(got) != len(want) {
		t.Errorf("%s: got %d rows, want %d\ngot:  %v\nwant: %v",
			mode, len(got), len(want), gotSorted, wantSorted)
		return
	}
	for i := range wantSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Errorf("%s: row %d = %q, want %q\ngot:  %v\nwant: %v",
				mode, i, gotSorted[i], wantSorted[i], gotSorted, wantSorted)
			return
		}
	}
	// 重复检测（集合相等 + 数量相等已隐含无重复，这里单独报出便于定位）。
	seen := make(map[string]int)
	for _, k := range got {
		seen[k]++
	}
	for k, n := range seen {
		if n > 1 {
			t.Errorf("%s: key %q appears %d times (duplicate)", mode, k, n)
		}
	}
}
