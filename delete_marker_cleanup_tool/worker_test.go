package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRow_ThreeFields(t *testing.T) {
	r, ok := ParseRow("bkt\x1fkey/a\x1fv1")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Bkt != "bkt" || r.Key != "key/a" || r.VersionID != "v1" {
		t.Errorf("got %+v", r)
	}
}

// TestParseRow_URLDecodesKey 文件格式契约：key 恒为编码态，读取无条件解码。
// "a%1Fb" → "a\x1f b"（撕裂类控制字符恢复原始形态，发请求/再落盘都安全）。
func TestParseRow_URLDecodesKey(t *testing.T) {
	r, ok := ParseRow("bkt\x1fa%1Fb\x1fv1")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Key != "a\x1fb" {
		t.Errorf("Key=%q want %q", r.Key, "a\x1fb")
	}
}

// TestParseRow_URLDecodesVersionID versionID 同为 server 字段，无条件解码
// （AWS 风格 version-id 含 '/'：文件态 "3%2FL4" → 原始 "3/L4"）。
func TestParseRow_URLDecodesVersionID(t *testing.T) {
	r, ok := ParseRow("bkt\x1fk\x1f3%2FL4")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.VersionID != "3/L4" {
		t.Errorf("VersionID=%q want %q", r.VersionID, "3/L4")
	}
}

func TestParseRow_WrongFieldCount(t *testing.T) {
	if _, ok := ParseRow("only\x1ftwo"); ok {
		t.Errorf("expected ok=false for 2-field line")
	}
	if _, ok := ParseRow("bkt\x1fk\x1fv\x1flm\x1fextra"); ok {
		t.Errorf("expected ok=false for 5-field line")
	}
}

func TestParseRow_NullVersionIdPassesThrough(t *testing.T) {
	r, ok := ParseRow("bkt\x1fk\x1fnull")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.VersionID != "null" {
		t.Errorf("VersionID=%q want %q", r.VersionID, "null")
	}
}

// ParseRow 兼容 3 段（旧输入）和 4 段（新输入带 lastModified）。
func TestParseRow_Accepts3And4Fields(t *testing.T) {
	// 3 段：LastModified 为空
	r3, ok := ParseRow("bkt\x1fk\x1fv1")
	if !ok {
		t.Fatal("expected ok for 3-field line")
	}
	if r3.LastModified != "" {
		t.Errorf("LastModified=%q want empty for 3-field line", r3.LastModified)
	}
	// 4 段：LastModified 透传
	r4, ok := ParseRow("bkt\x1fk\x1fv1\x1f2026-11-13T15:30:00Z")
	if !ok {
		t.Fatal("expected ok for 4-field line")
	}
	if r4.LastModified != "2026-11-13T15:30:00Z" {
		t.Errorf("LastModified=%q want %q", r4.LastModified, "2026-11-13T15:30:00Z")
	}
}

// URL 解码失败（非法 %XX）→ ok=false（scanner 走 torn line 路径，
// 写 failed.txt + slog 详情，与 delete 失败一致）。
func TestParseRow_DecodeFailureReturnsFalse(t *testing.T) {
	// "%XX" 末尾不完整 → QueryUnescape 报错。
	if _, ok := ParseRow("bkt\x1fa%\x1fv1"); ok {
		t.Errorf("expected ok=false for invalid url-encoded key")
	}
	// versionID 解码失败同理。
	if _, ok := ParseRow("bkt\x1fk\x1fv%"); ok {
		t.Errorf("expected ok=false for invalid url-encoded versionID")
	}
}

// 字面量 % 不会被误解码为空格/加号之外的字段边界——'\x1f' 分隔后再解码，
// 含合法 %XX 的原始 key 在文件侧必然是二次编码态（QueryEscape 后 %→%25），
// 解码一次恰好恢复。
func TestParseRow_EncodedPercentRoundTrip(t *testing.T) {
	// 原始 key "100%done"（含字面 %）→ QueryEscape → "100%25done" → 解码恢复。
	r, ok := ParseRow("bkt\x1f100%25done\x1fv1")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Key != "100%done" {
		t.Errorf("Key=%q want %q", r.Key, "100%done")
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
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
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
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "check", inputPath, clients, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out2 := string(b)
	if !strings.Contains(out2, "testbucket\x1fmarker.txt\x1fdm-1\x1f\x1fmarker") {
		t.Errorf("missing marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fregular.txt\x1fv-1\x1f\x1fnot_marker") {
		t.Errorf("missing not_marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fgone.txt\x1fv-1\x1f\x1fabsent") {
		t.Errorf("missing absent state: %q", out2)
	}
}

func TestRunWorkers_WorkerDiedOnBadEndpointReturnsError(t *testing.T) {
	// 新架构下 NewS3Client 在 main 启动期预创并预检 BucketExists——bad endpoint
	// 在 RunWorkers 调用前就 fail-fast 退出。RunWorkers 自身不再处理 worker 死亡
	// （client 已预创好，worker 不会构造失败）。本测试改测 makeClientsForTest 在
	// bad endpoint 上返回 error——验证启动期 fail-fast 的等价路径。
	cfg := &Config{
		Endpoints: []string{"ftp://bad-endpoint"}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 2, OutputDir: t.TempDir(), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	if _, err := NewS3Client(cfg, cfg.Endpoints[0]); err == nil {
		t.Errorf("expected NewS3Client to fail on ftp:// endpoint")
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
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
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
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "check", inputPath, clients, out, &stats, progress); err != nil {
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
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
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
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, clients, out, &stats, progress); err != nil {
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
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "check", inputPath, clients, out, &stats, progress); err != nil {
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

func TestRunWorkers_EncodedInputURLDecodesKey(t *testing.T) {
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
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fa%1Fb\x1fv1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "check", inputPath, clients, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	// 输入 key 是编码态（文件格式契约）；读取解码得原始 key "a\x1fb"，
	// 输出时 Output 层重新 QueryEscape——行内仍是 "a%1Fb"，不会撕裂行。
	if !strings.Contains(string(b), "testbucket\x1fa%1Fb\x1fv1\x1f\x1fnot_marker") {
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
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	clients := makeClientsForTest(t, cfg)

	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, clients, out, &stats, progress); err != nil {
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
