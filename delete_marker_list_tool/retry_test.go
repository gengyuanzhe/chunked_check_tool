package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadRetryEntries_HappyPath(t *testing.T) {
	p := writeRetryFile(t, "a/\nb/\n")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryEntries_EmptyLineIsRootPrefix(t *testing.T) {
	// 空行=root prefix + 从第一页扫，保留不跳——list_failed.txt 里 root prefix
	// 失败记成空行（向后兼容）。
	p := writeRetryFile(t, "\na/\n")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{{Prefix: ""}, {Prefix: "a/"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryEntries_DedupPreservesFirstOrder(t *testing.T) {
	// 重复 (prefix, markers) 去重保序——多次失败入 list_failed.txt 重试时
	// 不应让多个 worker 重复扫同一 prefix+同一 marker。
	p := writeRetryFile(t, "a/\nb/\na/\n")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestReadRetryEntries_SamePrefixDifferentMarkersNotDedup 验证同一 prefix
// 不同 marker 不去重——这是页级续传的关键：失败时不同页可能记不同的 marker，
// retry 时各自从对应 marker 续扫。
func TestReadRetryEntries_SamePrefixDifferentMarkersNotDedup(t *testing.T) {
	p := writeRetryFile(t, "a/\x1f\x1f\na/\x1fk1\x1fv1\n")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{
		{Prefix: "a/"},
		{Prefix: "a/", KeyMarker: "k1", VersionIDMarker: "v1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryEntries_WithMarkers(t *testing.T) {
	// 标准格式：prefix\x1fkeyMarker\x1fversionIdMarker——页级续传场景。
	p := writeRetryFile(t, "a/\x1fk1\x1fv1\nb/\x1fk2\x1fv2\n")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{
		{Prefix: "a/", KeyMarker: "k1", VersionIDMarker: "v1"},
		{Prefix: "b/", KeyMarker: "k2", VersionIDMarker: "v2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestReadRetryEntries_LineWithUnitSeparatorInPrefix 验证 prefix 含 \x1f 时报错——
// \x1f 是字段分隔符，prefix 含 \x1f 无法在 3-field 格式里无歧义表示。S3 key/prefix
// 极少含 \x1f（与 delete_markers.txt 用 \x1f 分隔 key 同理的限制）。
func TestReadRetryEntries_LineWithUnitSeparatorInPrefix(t *testing.T) {
	// prefix "a\x1fb/" 本身含 1 个 \x1f → 整行 split 后 2 个 part → 非法。
	p := writeRetryFile(t, "a\x1fb/\n")
	_, err := readRetryEntries(p)
	if err == nil {
		t.Errorf("expected error for prefix containing \\x1f")
	}
}

// TestReadRetryEntries_MalformedSingleSeparator 验证 1 个 \x1f（marker
// 不成对）报错——格式非法。
func TestReadRetryEntries_MalformedSingleSeparator(t *testing.T) {
	p := writeRetryFile(t, "a/\x1fk1\n")
	_, err := readRetryEntries(p)
	if err == nil {
		t.Errorf("expected error for malformed line with single \\x1f")
	}
}

func TestReadRetryEntries_FileNotFound(t *testing.T) {
	if _, err := readRetryEntries(filepath.Join(t.TempDir(), "nope.txt")); err == nil {
		t.Errorf("expected error for missing file")
	}
}

func TestReadRetryEntries_EmptyFileIsRootPrefix(t *testing.T) {
	// 空文件=1 个空 prefix=root + 从第一页扫。文件存在但无内容表示只有 root prefix 失败。
	p := writeRetryFile(t, "")
	got, err := readRetryEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []RetryEntry{{Prefix: ""}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func writeRetryFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "retry.txt")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}
