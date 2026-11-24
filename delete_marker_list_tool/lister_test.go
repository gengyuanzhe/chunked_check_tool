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

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// mode1XML: 一个 marker（a.txt 最新版本）+ 一个普通版本（a.txt 旧版本）+
// 另一个 key 的 marker（sub/c.txt）。mode 1 是 recursive，所有 key 的所有
// 版本在一条流里吐出，没有 common prefix。
const mode1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
<Version><Key>a.txt</Key><VersionId>reg-a</VersionId><IsLatest>false</IsLatest><ETag>"e"</ETag><Size>1</Size><LastModified>2026-01-01T15:04:05.000Z</LastModified></Version>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-03T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`

func TestLister_Mode1_HappyPath(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, mode1XML)
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 1, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	// mode 1 流里 3 个版本条目：dm-a（marker）+ reg-a（普通 version）+ dm-c（marker）。
	// versions 仅算普通版本（reg-a），delete_markers 算 marker，无 common prefix。
	if got := stats.Snapshot().Versions; got != 1 {
		t.Errorf("Versions=%d, want 1 (only regular versions)", got)
	}

	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d markers, want 2: %q", len(lines), b)
	}
	want := map[string]bool{"testbucket\x1fa.txt\x1fdm-a\x1f2026-01-02T15:04:05Z": false, "testbucket\x1fsub%2Fc.txt\x1fdm-c\x1f2026-01-03T15:04:05Z": false}
	for _, l := range lines {
		if _, ok := want[l]; !ok {
			t.Errorf("unexpected line %q", l)
		} else {
			want[l] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("missing line %q", k)
		}
	}
}

// TestLister_Mode2_RetryMultiplePrefixes 验证 Run 接受初始 prefix 列表——
// retry 工作流的前置：l.Run(ctx, bucket, []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}) 应让 BFS 从两个
// 前缀同时开始扫。fake S3 按 query prefix 返回不同 XML。
func TestLister_Mode2_RetryMultiplePrefixes(t *testing.T) {
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
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 2,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	// 两个 prefix 各 1 个 delete marker，无普通 version。
	if got, want := stats.Snapshot().Versions, int64(0); got != want {
		t.Errorf("Versions=%d, want %d (no regular versions, all delete markers)", got, want)
	}
	if got, want := stats.Snapshot().DeleteMarkers, int64(2); got != want {
		t.Errorf("DeleteMarkers=%d, want %d", got, want)
	}

	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	want := map[string]bool{
		"testbucket\x1fa%2Fm.txt\x1fdm-a\x1f2026-01-02T15:04:05Z": false,
		"testbucket\x1fb%2Fm.txt\x1fdm-b\x1f2026-01-03T15:04:05Z": false,
	}
	for _, line := range strings.Split(strings.TrimRight(b, "\n"), "\n") {
		if line == "" {
			continue
		}
		if _, ok := want[line]; !ok {
			t.Errorf("unexpected line %q", line)
		} else {
			want[line] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("missing line %q", k)
		}
	}
}
// TestLister_Mode2_KeyEndingInSlashWithNullVersion 验证以 "/" 结尾、
// VersionId="null" 的真实 key 不被 isCommonPrefix 误判成目录占位。
// 回归用例：自研 S3 上存在 `94-abc-0/` 这种 key，version=null，
// mode 2 原本会因 HasSuffix(Key,"/") 把它当 common prefix stub 跳过，
// 导致 versions 漏统计（mode 1 不走 delimited，没此问题）。
func TestLister_Mode2_KeyEndingInSlashWithNullVersion(t *testing.T) {
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
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>94-abc-0/</Key><VersionId>null</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	// 期望：1 个 delete marker（key 以 / 结尾、VersionId=null，不是 common prefix stub）。
	if got, want := stats.Snapshot().Versions, int64(0); got != want {
		t.Errorf("Versions=%d, want %d (entry is a delete marker, not a regular version)", got, want)
	}
	if got, want := stats.Snapshot().DeleteMarkers, int64(1); got != want {
		t.Errorf("DeleteMarkers=%d, want %d", got, want)
	}

	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	wantLine := "testbucket\x1f94-abc-0%2F\x1fnull"
	if !strings.Contains(b, wantLine) {
		t.Errorf("delete_markers.txt missing %q: got %q", wantLine, b)
	}
}

// TestLister_Mode2_KeyEndingInSlashAsVersion 验证服务端把以 "/" 结尾的 key
// 当 <Version>（非 <DeleteMarker>）返回、且无 VersionId 时，isCommonPrefix 的
// 四条件全成立（VersionID=="" && !IsDeleteMarker && ETag=="" && HasSuffix /），
// 会被误判成 common prefix stub 跳过 versions——这是 mode 2 漏统计的根因。
// mode 1 不走 delimited，不会触发 isCommonPrefix，所以不会漏。
func TestLister_Mode2_KeyEndingInSlashAsVersion(t *testing.T) {
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
		// key 以 / 结尾，作为 <Version> 返回（非 delete marker），无 VersionId/ETag。
		// 曾经 minio-go 把它误解析成 common prefix（四条件合取启发式），导致 mode 2 漏统计；
		// AWS SDK 按 XML 标签区分 Version 与 CommonPrefixes，本测试是回归保险。
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<Version><Key>94-abc-0/</Key><IsLatest>true</IsLatest></Version>
</ListVersionsResult>`))
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	// 加超时防死循环——bug 路径下 isCommonPrefix 误判 → 入队 94-abc-0/ →
	// worker 重列该前缀 → 服务端又返回同样的 key → 再入队…… 永不终止。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.Run(ctx, "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	// 期望：1 个普通版本被统计（<Version> 标签，非 delete marker）。
	// bug：isCommonPrefix 误判 → 死循环，versions=0。
	if got, want := stats.Snapshot().Versions, int64(1); got != want {
		t.Errorf("Versions=%d, want %d (key ending in / as <Version> is a real version, not a common prefix stub)", got, want)
	}
}

// newMode2BFSServer: 按 query prefix 返回不同 XML。
// prefix="" → DeleteMarker(a.txt, dm-a) + Version(a.txt, reg-a) + CommonPrefixes(sub/)
// prefix="sub/" → DeleteMarker(sub/c.txt, dm-c)
func newMode2BFSServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 检查桶名——path 第一段是桶名。
		bucket := strings.TrimPrefix(filepath.Dir(r.URL.Path), "/")
		if r.URL.Path == "/" {
			bucket = ""
		}
		// 提取 path 第一段。
		pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(pathParts) > 0 && pathParts[0] != "" {
			bucket = pathParts[0]
		}
		_ = bucket
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if !q.Has("versions") {
			if r.Method == http.MethodHead {
				// 只对 testbucket 返回 200，其他桶名 404。
				if bucket != "testbucket" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		prefix := q.Get("prefix")
		if prefix == "" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
<Version><Key>a.txt</Key><VersionId>reg-a</VersionId><IsLatest>false</IsLatest><ETag>"e"</ETag><Size>1</Size></Version>
<CommonPrefixes><Prefix>sub/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
		} else {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>sub/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-03T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`))
		}
	}))
}

func TestLister_Mode2_BFSWithCommonPrefix(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 2,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	// versions 仅算普通版本（reg-a = 1），不含 delete marker（dm-a, dm-c）也不含
	// common-prefix stub（sub/）。delete_markers 算 marker（2）。
	if got, want := stats.Snapshot().Versions, int64(1); got != want {
		t.Errorf("Versions=%d, want %d (only regular versions, no markers, no common-prefix stubs)", got, want)
	}
	if got, want := stats.Snapshot().DeleteMarkers, int64(2); got != want {
		t.Errorf("DeleteMarkers=%d, want %d", got, want)
	}

	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d markers, want 2: %q", len(lines), b)
	}
	want := map[string]bool{"testbucket\x1fa.txt\x1fdm-a\x1f2026-01-02T15:04:05Z": false, "testbucket\x1fsub%2Fc.txt\x1fdm-c\x1f2026-01-03T15:04:05Z": false}
	for _, l := range lines {
		if _, ok := want[l]; !ok {
			t.Errorf("unexpected line %q", l)
		} else {
			want[l] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("missing line %q", k)
		}
	}
}


// TestLister_Mode2_CommonPrefixTriggersProgress 验证只返回 CommonPrefixes
// （无 Version）的页面也能驱动 progress 打印——这是 10 层目录 + 1 对象桶
// 下行期的典型形态，原来只数 version 时会长时间无 progress 行。
// 服务端根页只返回 3 个 CommonPrefixes（d1/, d2/, d3/），子页各 0 条目。
// progress_interval=1 → 第 1 个 common prefix 触发打印。
func TestLister_Mode2_CommonPrefixTriggersProgress(t *testing.T) {
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
		if prefix == "" {
			// 根页：只有 CommonPrefixes，无任何 Version——下行期典型形态。
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<CommonPrefixes><Prefix>d1/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>d2/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>d3/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
			return
		}
		// 子页：空。
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>` + prefix + `</Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`))
	}))
	defer srv.Close()

	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 1, ProgressInterval: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	var progressBuf strings.Builder
	progress := NewProgressPrinter(&progressBuf)
	progress.SetInterval(1)
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: ""}}); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()

	snap := stats.Snapshot()
	if got, want := snap.CommonPrefixes, int64(3); got != want {
		t.Errorf("CommonPrefixes=%d, want %d", got, want)
	}
	if got, want := snap.Versions, int64(0); got != want {
		t.Errorf("Versions=%d, want %d (页内只有 common prefix，无 version)", got, want)
	}
	if got, want := snap.Scanned, int64(3); got != want {
		t.Errorf("Scanned=%d, want %d (versions + common_prefixes)", got, want)
	}

	// 关键断言：纯 common prefix 页也驱动了 progress 行。原来只数 version 时
	// 这个 case 会 0 行 progress——10 层目录下行期长时间静默的根因。
	pout := progressBuf.String()
	if !strings.Contains(pout, "[progress]") {
		t.Errorf("expected at least one progress line driven by common prefix, got empty buffer")
	}
	if !strings.Contains(pout, "scanned=") {
		t.Errorf("progress line missing scanned=: %q", pout)
	}
}


// TestLister_RetryRejectedByMode1 验证 mode 1 + 多 prefix 入口报错——
// mode 1 单流不支持 -retry（多 prefix），用户应用 -prefix 单值。
func TestLister_RetryRejectedByMode1(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, mode1XML)
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 1, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	client, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, []*S3Client{client}, out, &stats, progress)
	err = l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: "a/"}, {Prefix: "b/"}})
	if err == nil {
		out.FlushAndWait()
		out.Close()
		t.Fatalf("expected error for mode 1 with multiple prefixes")
	}
	out.FlushAndWait()
	out.Close()
}
