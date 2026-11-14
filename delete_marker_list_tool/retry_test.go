package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadRetryPrefixes_HappyPath(t *testing.T) {
	p := writeRetryFile(t, "a/\nb/\n")
	got, err := readRetryPrefixes(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a/", "b/"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryPrefixes_EmptyLineIsRootPrefix(t *testing.T) {
	// 空行=root prefix，保留不跳——list_failed.txt 里 root prefix 失败记成空行。
	p := writeRetryFile(t, "\na/\n")
	got, err := readRetryPrefixes(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"", "a/"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryPrefixes_DedupPreservesFirstOrder(t *testing.T) {
	// 重复 prefix 去重保序——多次失败入 list_failed.txt 重试时不应让多个 worker
	// 重复扫同一 prefix。
	p := writeRetryFile(t, "a/\nb/\na/\n")
	got, err := readRetryPrefixes(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a/", "b/"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadRetryPrefixes_FileNotFound(t *testing.T) {
	if _, err := readRetryPrefixes(filepath.Join(t.TempDir(), "nope.txt")); err == nil {
		t.Errorf("expected error for missing file")
	}
}

func TestReadRetryPrefixes_EmptyFileIsRootPrefix(t *testing.T) {
	// 空文件=1 个空 prefix=root。文件存在但无内容表示只有 root prefix 失败。
	p := writeRetryFile(t, "")
	got, err := readRetryPrefixes(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{""}
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
