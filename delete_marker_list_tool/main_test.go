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
	if err := run(cfgPath, "testbucket", []string{""}); err != nil {
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
	err := run(cfgPath, "testbucket", []string{""})
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
	err := run(cfgPath, "nonexistentbucket", []string{""})
	if err == nil {
		t.Errorf("expected error for non-existent bucket")
	}
}

// TestValidateFlags_RetryAndPrefixMutuallyExclusive 验证 -retry 和 -prefix
// 同时给时报错——互斥语义防误用。
func TestValidateFlags_RetryAndPrefixMutuallyExclusive(t *testing.T) {
	err := validateFlags("cfg.yaml", "bkt", "someprefix", "retry.txt")
	if err == nil {
		t.Errorf("expected error when both -prefix and -retry are set")
	}
}

// TestValidateFlags_RetryFileMustExist 验证 -retry 文件不存在时报错——
// 预检阶段报错而非半启动后失败。
func TestValidateFlags_RetryFileMustExist(t *testing.T) {
	err := validateFlags("cfg.yaml", "bkt", "", filepath.Join(t.TempDir(), "nope.txt"))
	if err == nil {
		t.Errorf("expected error when -retry file does not exist")
	}
}

// TestValidateFlags_OnlyRetryOK 验证只给 -retry（存在文件）不报错。
func TestValidateFlags_OnlyRetryOK(t *testing.T) {
	dir := t.TempDir()
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte("a/\nb/\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := validateFlags("cfg.yaml", "bkt", "", retryPath); err != nil {
		t.Errorf("unexpected error for -retry only: %v", err)
	}
}

// TestValidateFlags_OnlyPrefixOK 验证只给 -prefix 不报错。
func TestValidateFlags_OnlyPrefixOK(t *testing.T) {
	if err := validateFlags("cfg.yaml", "bkt", "someprefix", ""); err != nil {
		t.Errorf("unexpected error for -prefix only: %v", err)
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
	if err := run(cfgPath, "testbucket", []string{"a/", "b/"}); err != nil {
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
