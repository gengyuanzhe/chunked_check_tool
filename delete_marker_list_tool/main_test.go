package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_EndToEnd_HappyPath(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"max_keys: 500",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	// run.log exists with summary
	rl, err := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rl), "=== summary ===") {
		t.Errorf("run.log missing summary: %q", rl)
	}
	if !strings.Contains(string(rl), "sk=***") {
		t.Errorf("run.log should mask sk: %q", rl)
	}
	// delete_markers.txt has the two markers
	dm, err := os.ReadFile(filepath.Join(dir, "out", "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dm), "testbucket\x1fa.txt\x1fdm-a") {
		t.Errorf("missing a.txt marker: %q", dm)
	}
	if !strings.Contains(string(dm), "testbucket\x1fsub%2Fc.txt\x1fdm-c") {
		t.Errorf("missing sub/c.txt marker: %q", dm)
	}
}

func TestRun_RejectsInvalidListType(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - 1.2.3.4:80",
		"ak: a",
		"sk: s",
		"list_type: 5",
		"output_dir: " + filepath.Join(dir, "out"),
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "testbucket", []RetryEntry{{Prefix: ""}})
	if err == nil {
		t.Errorf("expected error for list_type=5")
	}
}

func TestRun_BucketDoesNotExist(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"max_keys: 500",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	// fake server returns 200 for HEAD on testbucket; use a different bucket name
	// so BucketExists returns false.
	err := run(cfgPath, "nonexistentbucket", []RetryEntry{{Prefix: ""}})
	if err == nil {
		t.Errorf("expected error for non-existent bucket")
	}
}

// TestResolveEntries_MutuallyExclusive 验证 -retry 和 -prefix 同时给时报错——
// 互斥语义防误用。直接测生产路径 resolveEntries（main.go 调用）。
func TestResolveEntries_MutuallyExclusive(t *testing.T) {
	_, err := resolveEntries("someprefix", "retry.txt")
	if err == nil {
		t.Errorf("expected error when both -prefix and -retry are set")
	}
}

// TestResolveEntries_RetryFileNotFound 验证 -retry 文件不存在时报错——
// resolveEntries 内部调 readRetryEntries，文件缺失在 BFS 启动前报出。
func TestResolveEntries_RetryFileNotFound(t *testing.T) {
	_, err := resolveEntries("", filepath.Join(t.TempDir(), "nope.txt"))
	if err == nil {
		t.Errorf("expected error when -retry file does not exist")
	}
}

// TestResolveEntries_OnlyRetryReturnsEntries 验证只给 -retry（存在文件）
// 返回文件里的 RetryEntry 列表，不报错。输入用 URL 编码态（文件格式契约），
// 验证读取侧解码恢复原始形态。
func TestResolveEntries_OnlyRetryReturnsEntries(t *testing.T) {
	dir := t.TempDir()
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte("a%2F\nb%2F\n"), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := resolveEntries("", retryPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}
	if len(out) != 2 || out[0] != want[0] || out[1] != want[1] {
		t.Errorf("got %v, want %v", out, want)
	}
}

// TestResolveEntries_OnlyPrefix 验证只给 -prefix 返回单元素 slice。
func TestResolveEntries_OnlyPrefix(t *testing.T) {
	out, err := resolveEntries("someprefix", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0].Prefix != "someprefix" {
		t.Errorf("got %v, want [{someprefix}]", out)
	}
}

// TestResolveEntries_BothEmptyIsRoot 验证两者都空时返回 root prefix——
// 扫整个 bucket。
func TestResolveEntries_BothEmptyIsRoot(t *testing.T) {
	out, err := resolveEntries("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0].Prefix != "" {
		t.Errorf("got %v, want [{Prefix:\"\"}]", out)
	}
}

// TestRun_ShortResponseWarning 验证服务端返回 IsTruncated=true 但本页条目数
// < max_keys 时，警告行被写入 list_failed.log（非失败，list_failed.txt 仍为空）。
// 这正是用户实际遇到的翻页 quirk 场景——服务端说截断了却没返回满页。
func TestRun_ShortResponseWarning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if !q.Has("versions") {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		// 第一页：1 条 + IsTruncated=true，但 max_keys=500 —— short response quirk。
		// 第二页（带 key-marker）：空页 + IsTruncated=false，正常收尾。
		if q.Get("key-marker") == "" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>k1</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`))
			return
		}
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 1",
		"max_keys: 500",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	// list_failed.log 含 short response 警告。
	logBytes, err := os.ReadFile(filepath.Join(dir, "out", "list_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	logStr := string(logBytes)
	if !strings.Contains(logStr, "short response while truncated") {
		t.Errorf("list_failed.log missing short-response warning: %q", logStr)
	}
	if !strings.Contains(logStr, "returned_count=1") || !strings.Contains(logStr, "max_keys=500") {
		t.Errorf("warning missing counts: %q", logStr)
	}
	// list_failed.txt 不应被创建——这是警告不是失败。
	if _, err := os.Stat(filepath.Join(dir, "out", "list_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("list_failed.txt should not exist for a warning: %v", err)
	}
	// delete_markers.txt 含第一页扫出的 marker。
	dm, err := os.ReadFile(filepath.Join(dir, "out", "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dm), "testbucket\x1fa.txt\x1fdm-a") {
		t.Errorf("delete_markers.txt missing a.txt marker: %q", dm)
	}
}
// TestRun_Mode2_StartupEndpointFailureExits 验证 mode 2 启动期 fail-fast：
// 配置含一个 dead endpoint，启动期对它做 BucketExists 预检失败 → run 直接 return err。
// 这取代了旧的"运行中 endpoint 故障 worker 继续"测试——新架构下 client 在启动期
// 全部预创好，启动时 endpoint 不可达就 fail-fast 退出，不存在"运行中挂"的早期检测路径。
// 运行中 endpoint 故障由 worker 慢速模式兜底（连续失败 3 次后降频），但那个路径
// 用 httptest 模拟"运行中变死"复杂度太高，不在集成测试覆盖。
func TestRun_Mode2_StartupEndpointFailureExits(t *testing.T) {
	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
	}))
	defer goodSrv.Close()

	// 死 endpoint：httptest server 立即 Close——连接被拒绝。
	deadSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadAddr := deadSrv.Listener.Addr().String()
	deadSrv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + goodSrv.Listener.Addr().String(),
		"  - " + deadAddr,
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"max_keys: 500",
		"list_concurrency: 2",
		"dial_timeout: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	err := run(cfgPath, "testbucket", []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}})
	if err == nil {
		t.Errorf("expected error: dead endpoint should fail BucketExists at startup")
	}
}

// 扫——run 接受 prefixes []string，多 prefix 入队。
func TestRun_RetryMode_ScansRetryPrefixes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if !q.Has("versions") {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		prefix := q.Get("prefix")
		var xml string
		switch prefix {
		case "a/":
			xml = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>a/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a/m.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`
		case "b/":
			xml = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>b/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>b/m.txt</Key><VersionId>dm-b</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-03T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`
		default:
			xml = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>` + prefix + `</Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`
		}
		w.Write([]byte(xml))
	}))
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"max_keys: 500",
		"list_concurrency: 2",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "testbucket", []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	dm, err := os.ReadFile(filepath.Join(dir, "out", "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dm), "testbucket\x1fa%2Fm.txt\x1fdm-a") {
		t.Errorf("missing a/m.txt marker: %q", dm)
	}
	if !strings.Contains(string(dm), "testbucket\x1fb%2Fm.txt\x1fdm-b") {
		t.Errorf("missing b/m.txt marker: %q", dm)
	}
}
