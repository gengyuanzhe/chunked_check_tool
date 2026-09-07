package main

import (
	"os"
	"strings"
	"sync"
	"testing"
)

func TestMD5Writer_LineFormat(t *testing.T) {
	f, err := os.CreateTemp("", "md5-*.txt")
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	f.Close()

	w, err := NewMD5Writer(f.Name())
	if err != nil {
		t.Fatalf("NewMD5Writer: %v", err)
	}
	if err := w.Write(MD5Record{Bucket: "bkt", Key: "k1", MD5: "abc123"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Write(MD5Record{Bucket: "bkt", Key: "k2", MD5: "def456"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := "bkt|k1|abc123\nbkt|k2|def456\n"
	if string(data) != want {
		t.Errorf("file content = %q, want %q", string(data), want)
	}
}

func TestMD5Writer_TruncatesExisting(t *testing.T) {
	f, err := os.CreateTemp("", "md5-*.txt")
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString("OLD CONTENT SHOULD BE GONE\n"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f.Close()

	w, err := NewMD5Writer(f.Name())
	if err != nil {
		t.Fatalf("NewMD5Writer: %v", err)
	}
	if err := w.Write(MD5Record{Bucket: "b", Key: "k", MD5: "m"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(string(data), "OLD CONTENT") {
		t.Errorf("file not truncated; content = %q", string(data))
	}
}

func TestMD5Writer_BufferFlushOnClose(t *testing.T) {
	f, err := os.CreateTemp("", "md5-*.txt")
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	f.Close()

	w, err := NewMD5Writer(f.Name())
	if err != nil {
		t.Fatalf("NewMD5Writer: %v", err)
	}
	// Write enough records that exceed 64KB buffer (each line ~30 bytes, need ~3000 lines)
	const n = 3000
	for i := 0; i < n; i++ {
		if err := w.Write(MD5Record{Bucket: "b", Key: "k", MD5: "0123456789abcdef0123456789abcdef"}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != n {
		t.Errorf("got %d lines, want %d", len(lines), n)
	}
}

func TestMD5Writer_ConcurrentWriters(t *testing.T) {
	f, err := os.CreateTemp("", "md5-*.txt")
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	f.Close()

	w, err := NewMD5Writer(f.Name())
	if err != nil {
		t.Fatalf("NewMD5Writer: %v", err)
	}

	const workers = 8
	const perWorker = 500
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if err := w.Write(MD5Record{Bucket: "b", Key: "k", MD5: "m"}); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	want := workers * perWorker
	if len(lines) != want {
		t.Errorf("got %d lines, want %d", len(lines), want)
	}
	for i, l := range lines {
		if l != "b|k|m" {
			t.Errorf("line[%d] = %q, want 'b|k|m'", i, l)
		}
	}
}
