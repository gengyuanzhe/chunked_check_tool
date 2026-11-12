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
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<Version><Key>a.txt</Key><VersionId>reg-a</VersionId><IsLatest>false</IsLatest><ETag>"e"</ETag><Size>1</Size></Version>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`

func TestLister_Mode1_HappyPath(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, mode1XML)
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 1, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", ""); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d markers, want 2: %q", len(lines), b)
	}
	want := map[string]bool{"testbucket\x1fa.txt\x1fdm-a": false, "testbucket\x1fsub/c.txt\x1fdm-c": false}
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

// mode2BFSFakeServer: 按 query prefix 返回不同 XML。
// prefix="" → DeleteMarker(a.txt) + CommonPrefixes(sub/)
// prefix="sub/" → DeleteMarker(sub/c.txt)
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
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<CommonPrefixes><Prefix>sub/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
		} else {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>sub/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
		}
	}))
}

func TestLister_Mode2_BFSWithCommonPrefix(t *testing.T) {
	srv := newMode2BFSServer(t)
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 2,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(cfg, "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "testbucket", ""); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d markers, want 2: %q", len(lines), b)
	}
	want := map[string]bool{"testbucket\x1fa.txt\x1fdm-a": false, "testbucket\x1fsub/c.txt\x1fdm-c": false}
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
