package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newFakeVersionedS3(t *testing.T, versionsXML string) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Only the actual LIST versions request carries versions=.
		// Other requests (location lookup, HEAD bucket) get generic responses.
		// Use Has() not Get() — versions= has an empty value.
		if !q.Has("versions") {
			w.Header().Set("Content-Type", "application/xml")
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		if versionsXML == "" {
			http.Error(w, "InternalError", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(versionsXML))
	}))
	return srv, srv.Listener.Addr().String()
}

const versionsXMLOneMarker = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker>
<IsTruncated>false</IsTruncated>
<Version><Key>a.txt</Key><VersionId>regular-v1</VersionId><IsLatest>false</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>"abc"</ETag><Size>5</Size></Version>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-v2</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-02T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`

func TestListVersions_OneMarkerOneVersion(t *testing.T) {
	srv, url := newFakeVersionedS3(t, versionsXMLOneMarker)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{url}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, url)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page.Versions) != 2 {
		t.Fatalf("got %d entries, want 2", len(page.Versions))
	}
	var markerCount, regularCount int
	for _, o := range page.Versions {
		if o.IsDeleteMarker {
			markerCount++
		} else {
			regularCount++
		}
	}
	if markerCount != 1 || regularCount != 1 {
		t.Errorf("markerCount=%d regularCount=%d want 1/1", markerCount, regularCount)
	}
}

func TestListVersions_ServerError(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, "") // empty XML triggers 500 in handler
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

// TestListVersionsPage_MaxKeysPassedToRequest 验证 cfg.MaxKeys 配置确实把
// max-keys=N 传进了 ListObjectVersions 请求 query。用 fake server 抓 query。
func TestListVersionsPage_MaxKeysPassedToRequest(t *testing.T) {
	var sawMaxKeys string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if q.Has("versions") {
			sawMaxKeys = q.Get("max-keys")
		}
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`))
	}))
	defer srv.Close()

	// MaxKeys=500 → 应有 max-keys=500。
	// （MaxKeys 必填 1..1000，不再有 0 值，故仅测一个有效值。）
	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sawMaxKeys = ""
	if _, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", ""); err != nil {
		t.Fatal(err)
	}
	if sawMaxKeys != "500" {
		t.Errorf("MaxKeys=500: expected max-keys=500, got %q", sawMaxKeys)
	}
}

func TestBucketExists(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, versionsXMLOneMarker)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	exists, err := c.BucketExists(context.Background(), "testbucket")
	if err != nil {
		t.Fatalf("BucketExists err: %v", err)
	}
	if !exists {
		t.Errorf("exists=false want true")
	}
}

// TestListVersionsPage_EncodingTypeURLPassedToRequest 验证 cfg.EncodingTypeURL=true
// 时请求 URL 带 encoding-type=url，且 SDK 不自动解码返回的 URL 编码 key。
func TestListVersionsPage_EncodingTypeURLPassedToRequest(t *testing.T) {
	var sawEncodingType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if q.Has("versions") {
			sawEncodingType = q.Get("encoding-type")
		}
		// 服务端返回 URL 编码后的 key（a%1Fb），模拟 wire 上的编码形态。
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a%1Fb</Key><VersionId>dm-1</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`))
	}))
	defer srv.Close()

	// case 1: EncodingTypeURL=true → 请求带 encoding-type=url，key 不解码（保留 a%1Fb）
	cfgOn := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, EncodingTypeURL: true}
	cOn, err := NewS3Client(cfgOn, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sawEncodingType = ""
	page, err := cOn.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if sawEncodingType != "url" {
		t.Errorf("EncodingTypeURL=true: expected encoding-type=url in request, got %q", sawEncodingType)
	}
	if len(page.Versions) != 1 {
		t.Fatalf("got %d entries, want 1", len(page.Versions))
	}
	if page.Versions[0].Key != "a%1Fb" {
		t.Errorf("SDK should NOT auto-decode, got key=%q want %q", page.Versions[0].Key, "a%1Fb")
	}

	// case 2: EncodingTypeURL=false → 请求不带 encoding-type
	cfgOff := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, EncodingTypeURL: false}
	cOff, err := NewS3Client(cfgOff, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sawEncodingType = "__sentinel__"
	if _, err := cOff.ListVersionsPage(context.Background(), "testbucket", "", true, "", ""); err != nil {
		t.Fatal(err)
	}
	if sawEncodingType != "" {
		t.Errorf("EncodingTypeURL=false: expected no encoding-type query, got %q", sawEncodingType)
	}
}
