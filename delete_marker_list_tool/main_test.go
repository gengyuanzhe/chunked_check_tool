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
	if !strings.Contains(string(dm), "testbucket\x1fsub/c.txt\x1fdm-c") {
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
// 返回文件里的 RetryEntry 列表，不报错。
func TestResolveEntries_OnlyRetryReturnsEntries(t *testing.T) {
	dir := t.TempDir()
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte("a/\nb/\n"), 0644); err != nil {
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

// TestRun_RetryMode_ScansRetryPrefixes 验证 retry 模式从文件读 prefix 列表
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
	if !strings.Contains(string(dm), "testbucket\x1fa/m.txt\x1fdm-a") {
		t.Errorf("missing a/m.txt marker: %q", dm)
	}
	if !strings.Contains(string(dm), "testbucket\x1fb/m.txt\x1fdm-b") {
		t.Errorf("missing b/m.txt marker: %q", dm)
	}
}
