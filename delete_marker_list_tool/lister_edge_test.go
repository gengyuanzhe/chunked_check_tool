package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestLister_Mode2_FailureRecordedOtherPrefixesContinue(t *testing.T) {
	// prefix="" → DeleteMarker(a.txt) + CommonPrefixes(sub/, bad/)
	// prefix="sub/" → DeleteMarker(sub/c.txt)
	// prefix="bad/" → HTTP 500
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
		switch prefix {
		case "":
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<CommonPrefixes><Prefix>sub/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>bad/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
		case "sub/":
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>sub/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
		case "bad/":
			http.Error(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>boom</Message><RequestId>req-bad</RequestId></Error>`, http.StatusInternalServerError)
		default:
			http.Error(w, "unexpected prefix "+prefix, http.StatusBadRequest)
		}
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 1, // conc=1 避免 bad/ 重试噪音
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

	// delete_markers.txt should have a.txt + sub/c.txt (bad/ marker missing — failed)
	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	if !strings.Contains(b, "testbucket\x1fa.txt\x1fdm-a") {
		t.Errorf("missing a.txt marker: %q", b)
	}
	if !strings.Contains(b, "testbucket\x1fsub%2Fc.txt\x1fdm-c") {
		t.Errorf("missing sub/c.txt marker: %q", b)
	}

	// list_failed.txt should contain "bad/"（QueryEscape 态）
	fb := readFile(t, filepath.Join(dir, "list_failed.txt"))
	if !strings.Contains(fb, "bad%2F") {
		t.Errorf("list_failed.txt missing bad/ encoded form: %q", fb)
	}

	// list_failed.log should exist and mention 500
	lb := readFile(t, filepath.Join(dir, "list_failed.log"))
	if !strings.Contains(lb, "500") {
		t.Errorf("list_failed.log missing 500: %q", lb)
	}
	if !strings.Contains(lb, "bad/") {
		t.Errorf("list_failed.log missing bad/ prefix: %q", lb)
	}
}

// TestLister_Mode1_MidPaginationFailureRecordsPage2Marker 验证"翻页中途失败记录
// 页级续传点"契约：页1成功（truncated，带 next-marker），页2请求（用页1的 marker）
// 失败 → list_failed.txt 记的必须是**页2请求所用的 marker**（不是空、也不是页1
// 请求的 marker）——retry 从该 marker 起续扫即只补页2之后的内容。
// marker 刻意含空格/斜杠，同时验证落盘 QueryEscape 态。
func TestLister_Mode1_MidPaginationFailureRecordsPage2Marker(t *testing.T) {
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
		if q.Get("key-marker") == "" {
			// 页1：1 条 + truncated，next-marker 指向页2。
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>k 1</NextKeyMarker><NextVersionIdMarker>v/1</NextVersionIdMarker>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`))
			return
		}
		// 页2：失败。
		http.Error(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>boom</Message><RequestId>req-p2</RequestId></Error>`, http.StatusInternalServerError)
	}))
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
	if err := l.Run(context.Background(), "testbucket", []RetryEntry{{Prefix: ""}}); err == nil {
		t.Fatal("expected error: page 2 fails")
	}
	out.FlushAndWait()
	out.Close()

	// 页1 的 marker 已写出结果；失败请求用的 marker = 页1 的 next-marker（k 1 / v/1），
	// 落盘为 QueryEscape 态：' '→'+'、'/'→%2F。
	fb := readFile(t, filepath.Join(dir, "list_failed.txt"))
	if want := "\x1fk+1\x1fv%2F1\n"; fb != want {
		t.Errorf("list_failed.txt=%q want %q（页2 marker 编码态）", fb, want)
	}
	// 页1 结果保留。
	b := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	if !strings.Contains(b, "testbucket\x1fa.txt\x1fdm-a") {
		t.Errorf("missing a.txt marker: %q", b)
	}
	lb := readFile(t, filepath.Join(dir, "list_failed.log"))
	if !strings.Contains(lb, "500") || !strings.Contains(lb, "req-p2") {
		t.Errorf("list_failed.log missing 500/req-p2: %q", lb)
	}
}
