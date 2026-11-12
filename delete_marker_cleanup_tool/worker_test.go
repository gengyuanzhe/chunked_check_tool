package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseRow_ThreeFields(t *testing.T) {
	r, ok := ParseRow("bkt\x1fkey/a\x1fv1", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Bkt != "bkt" || r.Key != "key/a" || r.VersionID != "v1" {
		t.Errorf("got %+v", r)
	}
}

func TestParseRow_InvalidKeysBasenameURLDecodesKey(t *testing.T) {
	r, ok := ParseRow("bkt\x1fa%1Fb\x1fv1", "invalid_keys.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Key != "a\x1fb" {
		t.Errorf("Key=%q want %q", r.Key, "a\x1fb")
	}
}

func TestParseRow_WrongFieldCount(t *testing.T) {
	if _, ok := ParseRow("only\x1ftwo", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 2-field line")
	}
	if _, ok := ParseRow("bkt\x1fk\x1fv\x1fextra", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 4-field line")
	}
}

func TestParseRow_NullVersionIdPassesThrough(t *testing.T) {
	r, ok := ParseRow("bkt\x1fk\x1fnull", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.VersionID != "null" {
		t.Errorf("VersionID=%q want %q", r.VersionID, "null")
	}
}

func TestRunWorkers_ModeCheck_ThreeStates(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fmarker.txt\x1fdm-1",
		"testbucket\x1fregular.txt\x1fv-1",
		"testbucket\x1fgone.txt\x1fv-1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out2 := string(b)
	if !strings.Contains(out2, "testbucket\x1fmarker.txt\x1fdm-1\x1fmarker") {
		t.Errorf("missing marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fregular.txt\x1fv-1\x1fnot_marker") {
		t.Errorf("missing not_marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fgone.txt\x1fv-1\x1fabsent") {
		t.Errorf("missing absent state: %q", out2)
	}
}

func TestRunWorkers_WorkerDiedOnBadEndpointReturnsError(t *testing.T) {
	// 回归：若 NewS3Client 失败（如 endpoints 中混入格式错的 endpoint），
	// worker 静默 return，scanner 在 rowCh <- row 上死锁。修复：worker 报错
	// 到 errCh，scanner 用 select 检测并返回 error。
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fk1\x1fv1",
		"testbucket\x1fk2\x1fv1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		// ftp 不是合法 S3 scheme——minio.New 直接报错
		Endpoints: []string{"ftp://bad-endpoint"}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 2, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress)
		if err == nil {
			t.Errorf("expected error from RunWorkers when all workers die on bad endpoint")
		}
		out.FlushAndWait()
		out.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunWorkers hung — worker death not detected (scanner deadlocked on rowCh <- row)")
	}
}

func TestRunWorkers_ProgressNotFloodedAfterInterval(t *testing.T) {
	// 回归：每 worker 的 local 计数器在打印后必须 reset，否则越过阈值后每行
	// 都打一行 progress——20 行 interval=5 会打 16 行而非 4 行。
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	const N = 20
	rows := make([]string, N)
	for i := range rows {
		rows[i] = "testbucket\x1fregular.txt\x1fv-1"
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 5,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	var progBuf strings.Builder
	progress := NewProgressPrinter(&progBuf)
	progress.SetInterval(5)
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	progLines := strings.Count(progBuf.String(), "[progress]")
	// 20 行 / interval 5 = 4 次（取等号时打）。容忍 ±1 漂移（不同 worker 抢任务边界）。
	if progLines > 5 {
		t.Errorf("progress flooded: got %d lines for %d rows @ interval 5 (want <=5):\n%s", progLines, N, progBuf.String())
	}
}

func TestRunWorkers_ModeDelete_NoSuchVersionCountsAsSuccess(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fany.txt\x1fv-1",
		"testbucket\x1fany.txt\x1falready-deleted",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d deleted lines, want 2: %q", len(lines), string(b))
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "delete_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("delete_failed.txt should not exist: %v", err)
	}
}

func TestRunWorkers_TornLineSkippedAndLogged(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fmarker.txt\x1fdm-1",
		"only\x1ftwo-fields",
		"testbucket\x1fregular.txt\x1fv-1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("got %d result lines, want 2: %q", len(lines), string(b))
	}
	lb, _ := os.ReadFile(filepath.Join(dir, "out", "head_failed.log"))
	if !strings.Contains(string(lb), "torn") {
		t.Errorf("head_failed.log missing torn note: %q", string(lb))
	}
}

func TestRunWorkers_InvalidKeysInputURLDecodesKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 2 || parts[1] == "" {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"abc"`)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2026 15:04:05 GMT")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "invalid_keys.txt")
	rows := []string{"testbucket\x1fa%1Fb\x1fv1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	// key 来自 invalid_keys.txt（URL 编码过）；输出行重新 QueryEscape key，
	// 保持 4 段行格式契约（解码后 key 含 \x1f，否则会撕裂成 5 段）。
	if !strings.Contains(string(b), "testbucket\x1fa%1Fb\x1fv1\x1fnot_marker") {
		t.Errorf("missing re-encoded-key row: %q", string(b))
	}
}

func TestRunWorkers_ModeDelete_InternalErrorRecordedAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 2 || parts[1] == "" {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		if r.Method == http.MethodDelete {
			http.Error(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><RequestId>req-x</RequestId></Error>`, http.StatusInternalServerError)
			return
		}
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fkey1\x1fv-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt")); strings.TrimSpace(string(b)) != "" {
		t.Errorf("deleted.txt should be empty: %q", string(b))
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.txt")); !strings.Contains(string(b), "testbucket\x1fkey1\x1fv-1") {
		t.Errorf("delete_failed.txt missing row: %q", string(b))
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.log")); !strings.Contains(string(b), "InternalError") {
		t.Errorf("delete_failed.log missing InternalError: %q", string(b))
	}
}
