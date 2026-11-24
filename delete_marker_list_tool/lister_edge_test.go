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
