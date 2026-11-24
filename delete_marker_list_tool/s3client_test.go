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

// TestListVersionsPage_NextVIDMarkerZeroRewrittenToNull 验证 cfg.RepairNextVIDMarkerOnShortResponse=true
// 时，短响应触发下服务端返回 NextVersionIdMarker=="0" 被改写成 "null"（分支 b），下一页请求
// 带的 version-id-marker 是 "null" 而不是 "0"。
func TestListVersionsPage_NextVIDMarkerZeroRewrittenToNull(t *testing.T) {
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>k1</NextKeyMarker><NextVersionIdMarker>0</NextVersionIdMarker>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-1</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`
	const page2XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`

	var sawPage2VIDMarker string
	callCount := 0
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
		callCount++
		if callCount == 1 {
			w.Write([]byte(page1XML))
			return
		}
		sawPage2VIDMarker = q.Get("version-id-marker")
		w.Write([]byte(page2XML))
	}))
	defer srv.Close()

	// case 1: rewrite=true → page1 返回 NextVersionIdMarker=0 被改写成 null，
	//         page2 请求带的 version-id-marker 应是 "null"
	cfgOn := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: true}
	cOn, err := NewS3Client(cfgOn, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := cOn.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextVersionIDMarker != "null" {
		t.Errorf("rewrite=true: NextVersionIDMarker=%q want %q", page.NextVersionIDMarker, "null")
	}
	// 下一页请求带上改写后的 marker
	if _, err := cOn.ListVersionsPage(context.Background(), "testbucket", "", true, page.NextKeyMarker, page.NextVersionIDMarker); err != nil {
		t.Fatal(err)
	}
	if sawPage2VIDMarker != "null" {
		t.Errorf("rewrite=true: page2 version-id-marker=%q want %q", sawPage2VIDMarker, "null")
	}

	// case 2: rewrite=false → page1 返回 0 原样透传，page2 请求带 "0"
	callCount = 0
	sawPage2VIDMarker = ""
	cfgOff := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: false}
	cOff, err := NewS3Client(cfgOff, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err = cOff.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextVersionIDMarker != "0" {
		t.Errorf("rewrite=false: NextVersionIDMarker=%q want %q", page.NextVersionIDMarker, "0")
	}
	if _, err := cOff.ListVersionsPage(context.Background(), "testbucket", "", true, page.NextKeyMarker, page.NextVersionIDMarker); err != nil {
		t.Fatal(err)
	}
	if sawPage2VIDMarker != "0" {
		t.Errorf("rewrite=false: page2 version-id-marker=%q want %q", sawPage2VIDMarker, "0")
	}
}

// TestListVersionsPage_NextVIDMarkerShortResponseBranchC 验证短响应触发下的分支 c：
// 服务端 NextVersionIdMarker 是非 0 整数（明显坏值），但 NextKeyMarker 与本页尾条目
// 解码后 key 一致——s3client 用本页尾条目的真实 VersionID 覆盖坏值。
func TestListVersionsPage_NextVIDMarkerShortResponseBranchC(t *testing.T) {
	// 本页返回两条 Version：a.txt（vA）与 b.txt（vB）。NextKeyMarker="b.txt" 与本页
	// 尾条目解码态 key 一致；NextVersionIdMarker="999" 是坏的非 0 整数，应被覆盖为 vB。
	// maxKeys=500，count=2 → 短响应触发。
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>b.txt</NextKeyMarker><NextVersionIdMarker>999</NextVersionIdMarker>
<Version><Key>a.txt</Key><VersionId>vA</VersionId><IsLatest>false</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></Version>
<Version><Key>b.txt</Key><VersionId>vB</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-02T00:00:00.000Z</LastModified></Version>
</ListVersionsResult>`
	const page2XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
</ListVersionsResult>`

	var sawPage2VIDMarker string
	callCount := 0
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
		callCount++
		if callCount == 1 {
			w.Write([]byte(page1XML))
			return
		}
		sawPage2VIDMarker = q.Get("version-id-marker")
		w.Write([]byte(page2XML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !page.NextVIDMarkerRewritten {
		t.Errorf("NextVIDMarkerRewritten=false want true")
	}
	if page.RewrittenRawVIDMarker != "999" {
		t.Errorf("RewrittenRawVIDMarker=%q want %q", page.RewrittenRawVIDMarker, "999")
	}
	if page.NextVersionIDMarker != "vB" {
		t.Errorf("NextVersionIDMarker=%q want %q (本页尾条目 b.txt 的 VersionID)", page.NextVersionIDMarker, "vB")
	}
	if _, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, page.NextKeyMarker, page.NextVersionIDMarker); err != nil {
		t.Fatal(err)
	}
	if sawPage2VIDMarker != "vB" {
		t.Errorf("page2 version-id-marker=%q want %q", sawPage2VIDMarker, "vB")
	}
}

// TestListVersionsPage_NextVIDMarkerShortResponseBranchC_KeyMismatch 验证分支 c 的
// 不一致兜底：NextKeyMarker 不等于本页尾条目 key（指向本页之外），s3client 不动
// NextVersionIdMarker——服务端 marker 可能本就对。
func TestListVersionsPage_NextVIDMarkerShortResponseBranchC_KeyMismatch(t *testing.T) {
	// 本页返回 a.txt，NextKeyMarker="z.txt"（不在本页），NextVersionIdMarker="999" 是非 0 坏值。
	// 不一致——不动。
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>z.txt</NextKeyMarker><NextVersionIdMarker>999</NextVersionIdMarker>
<Version><Key>a.txt</Key><VersionId>vA</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></Version>
</ListVersionsResult>`

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
		w.Write([]byte(page1XML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextVIDMarkerRewritten {
		t.Errorf("NextVIDMarkerRewritten=true want false（key 不一致时不该修复）")
	}
	if page.NextVersionIDMarker != "999" {
		t.Errorf("NextVersionIDMarker=%q want %q（不一致时不动）", page.NextVersionIDMarker, "999")
	}
}

// TestListVersionsPage_NextVIDMarkerShortResponseBranchA_NonInteger 验证分支 a：
// NextVersionIdMarker 是非整数（如 "null" 或真实 version-id），短响应触发时不动。
func TestListVersionsPage_NextVIDMarkerShortResponseBranchA_NonInteger(t *testing.T) {
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>a.txt</NextKeyMarker><NextVersionIdMarker>3/L4kqtJlcpXro6omR98P.SnrKt8KQwg</NextVersionIdMarker>
<Version><Key>a.txt</Key><VersionId>3/L4kqtJlcpXro6omR98P.SnrKt8KQwg</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></Version>
</ListVersionsResult>`

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
		w.Write([]byte(page1XML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextVIDMarkerRewritten {
		t.Errorf("NextVIDMarkerRewritten=true want false（非整数分支 a 不动）")
	}
	want := "3/L4kqtJlcpXro6omR98P.SnrKt8KQwg"
	if page.NextVersionIDMarker != want {
		t.Errorf("NextVersionIDMarker=%q want %q", page.NextVersionIDMarker, want)
	}
}

// TestListVersionsPage_NextVIDMarkerNotShortResponse 验证非短响应（count==maxKeys）时
// 即便 NextVersionIdMarker="0" 也不动——修复只发生在短响应触发时。
func TestListVersionsPage_NextVIDMarkerNotShortResponse(t *testing.T) {
	// 本页返回 maxKeys=1 条目，count==maxKeys 不触发短响应，"0" 不该被改写。
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>a.txt</NextKeyMarker><NextVersionIdMarker>0</NextVersionIdMarker>
<Version><Key>a.txt</Key><VersionId>vA</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></Version>
</ListVersionsResult>`

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
		w.Write([]byte(page1XML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 1, RepairNextVIDMarkerOnShortResponse: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if page.NextVIDMarkerRewritten {
		t.Errorf("NextVIDMarkerRewritten=true want false（count==maxKeys 非短响应不该修复）")
	}
	if page.NextVersionIDMarker != "0" {
		t.Errorf("NextVersionIDMarker=%q want %q（非短响应不动）", page.NextVersionIDMarker, "0")
	}
}

// TestListVersionsPage_NextVIDMarkerShortResponseBranchC_SameKeyLastModifiedTiebreak
// 验证分支 c 的同 key LastModified 决胜：versions 与 deleteMarkers 尾元素 key 相同，
// 此时全局尾 = LastModified 更旧者（S3 同 key 内按 LastModified 倒序排，越旧越靠后）。
// 服务端按全局序返回时 01-03 (Version) 在 01-01 (DeleteMarker) 之前，故全局尾是 01-01。
func TestListVersionsPage_NextVIDMarkerShortResponseBranchC_SameKeyLastModifiedTiebreak(t *testing.T) {
	const page1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix>
<IsTruncated>true</IsTruncated>
<NextKeyMarker>b.txt</NextKeyMarker><NextVersionIdMarker>999</NextVersionIdMarker>
<Version><Key>b.txt</Key><VersionId>vNewer</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-03T00:00:00.000Z</LastModified></Version>
<DeleteMarker><Key>b.txt</Key><VersionId>dmOldest</VersionId><IsLatest>false</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`

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
		w.Write([]byte(page1XML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, RepairNextVIDMarkerOnShortResponse: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !page.NextVIDMarkerRewritten {
		t.Errorf("NextVIDMarkerRewritten=false want true")
	}
	// 全局尾是 LastModified 更旧的 DeleteMarker（dmOldest），不是 Version 的 vNewer。
	// 单比 key 不带 LastModified 决胜的旧实现会停在 versions 尾元素 vNewer，是 bug。
	if page.NextVersionIDMarker != "dmOldest" {
		t.Errorf("NextVersionIDMarker=%q want %q（同 key 取 LastModified 更旧者）", page.NextVersionIDMarker, "dmOldest")
	}
	if page.RewrittenRawVIDMarker != "999" {
		t.Errorf("RewrittenRawVIDMarker=%q want %q", page.RewrittenRawVIDMarker, "999")
	}
}


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

// TestListVersionsPage_EncodingTypeURL_DecodesCommonPrefix 验证 encoding_type_url=true 时
// CommonPrefix 在 s3client 出口被 url.QueryUnescape 解码——list 工具 BFS 用 CommonPrefix
// 作下一轮 prefix 入队，编码形式会让 SDK 二次编码（photos%2F → photos%252F），服务端解一次得到
// photos%2F 匹配不到真实目录。delete_markers.txt 的 key 仍保持 URL 编码（与 CommonPrefix 不同）。
func TestListVersionsPage_EncodingTypeURL_DecodesCommonPrefix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>
<CommonPrefixes><Prefix>photos%2F</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>photos%2F2024%2F</Prefix></CommonPrefixes>
</ListVersionsResult>`))
	}))
	defer srv.Close()

	// case 1: EncodingTypeURL=true → CommonPrefix 解码后返回（photos/、photos/2024/）
	cfgOn := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, EncodingTypeURL: true}
	cOn, err := NewS3Client(cfgOn, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := cOn.ListVersionsPage(context.Background(), "testbucket", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	wantCP := []string{"photos/", "photos/2024/"}
	if len(page.CommonPrefixes) != len(wantCP) {
		t.Fatalf("CommonPrefixes count=%d want %d", len(page.CommonPrefixes), len(wantCP))
	}
	for i, want := range wantCP {
		if page.CommonPrefixes[i] != want {
			t.Errorf("CommonPrefixes[%d]=%q want %q", i, page.CommonPrefixes[i], want)
		}
	}

	// case 2: EncodingTypeURL=false → CommonPrefix 原样返回（不解码）
	cfgOff := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, EncodingTypeURL: false}
	cOff, err := NewS3Client(cfgOff, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err = cOff.ListVersionsPage(context.Background(), "testbucket", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	wantRaw := []string{"photos%2F", "photos%2F2024%2F"}
	for i, want := range wantRaw {
		if page.CommonPrefixes[i] != want {
			t.Errorf("CommonPrefixes[%d]=%q want %q (no decode)", i, page.CommonPrefixes[i], want)
		}
	}
}

// TestListVersionsPage_EncodingTypeURL_DecodesNextKeyMarker 验证 encoding_type_url=true 时
// NextKeyMarker 在 s3client 出口被 url.QueryUnescape 解码——翻页时 key-marker 必须回传
// 原始形态：服务端对 key-marker 只做一次标准 query 解码后按原始 key 定位翻页位置，
// 编码态回传会让位置错位（minio 实测：边界 key 剩余版本与字典序缺口的 key 漏扫）。
// NextVersionIdMarker 不在 encoding-type=url 的编码字段列表，原样返回。
func TestListVersionsPage_EncodingTypeURL_DecodesNextKeyMarker(t *testing.T) {
	// 首请求（无 key-marker）返回截断页，NextKeyMarker 为编码态（含 minio 风格
	// 内部缓存标签）；后续请求捕获 key-marker/version-id-marker 参数。
	const pageXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>
<NextKeyMarker>b%20c%5Bminio_cache%3Av2%2Creturn%3A%5D</NextKeyMarker><NextVersionIdMarker>vid-1</NextVersionIdMarker>
<Version><Key>a.txt</Key><VersionId>vA</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified></Version>
</ListVersionsResult>`

	var sawKeyMarker, sawVIDMarker string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if q.Get("key-marker") != "" {
			sawKeyMarker = q.Get("key-marker")
			sawVIDMarker = q.Get("version-id-marker")
		}
		w.Write([]byte(pageXML))
	}))
	defer srv.Close()

	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s", MaxKeys: 500, EncodingTypeURL: true}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatal(err)
	}
	wantDecoded := "b c[minio_cache:v2,return:]"
	if page.NextKeyMarker != wantDecoded {
		t.Errorf("NextKeyMarker=%q want %q (decoded at exit)", page.NextKeyMarker, wantDecoded)
	}
	if page.NextVersionIDMarker != "vid-1" {
		t.Errorf("NextVersionIDMarker=%q want %q (not encoded, kept as-is)", page.NextVersionIDMarker, "vid-1")
	}

	// 翻页：出口解码后的 marker 传回下一页请求，服务端 query 解码后应看到原始形态。
	if _, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, page.NextKeyMarker, page.NextVersionIDMarker); err != nil {
		t.Fatal(err)
	}
	if sawKeyMarker != wantDecoded {
		t.Errorf("second request key-marker=%q want %q (raw form round-trip)", sawKeyMarker, wantDecoded)
	}
	if sawVIDMarker != "vid-1" {
		t.Errorf("second request version-id-marker=%q want %q", sawVIDMarker, "vid-1")
	}
}
