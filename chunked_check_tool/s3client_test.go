package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestFakeS3ListPage(t *testing.T) {
	f := &FakeS3{
		Objects: []ObjectInfo{
			{Key: "a", ETag: "0123456789abcdef0123456789abcdef"},
			{Key: "b", ETag: "0123456789abcdef0123456789abcdef-2"},
		},
		CommonPrefixes:        []string{"sub/"},
		NextContinuationToken: "tok-b",
	}
	objs, prefixes, next, err := f.ListPage(context.Background(), "p", "", "", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 || objs[1].Key != "b" {
		t.Errorf("objs = %v", objs)
	}
	if len(prefixes) != 1 || prefixes[0] != "sub/" {
		t.Errorf("prefixes = %v", prefixes)
	}
	if next != "tok-b" {
		t.Errorf("next = %q", next)
	}
}

func TestFakeS3RangeGet(t *testing.T) {
	f := &FakeS3{Body: []byte("1000;chunk-signature=abcdef")}
	body, err := f.RangeGet(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "1000;chunk-signature=abcdef" {
		t.Errorf("body = %q", body)
	}
}

func TestFakeS3ListError(t *testing.T) {
	f := &FakeS3{Err: errors.New("boom")}
	_, _, _, err := f.ListPage(context.Background(), "", "", "", false, 100)
	if err == nil || err.Error() != "boom" {
		t.Errorf("err = %v", err)
	}
}

// TestFakeS3FailNextRetry exercises the FailNext hook: the first call
// returns an error and clears the hook; the second call succeeds. This
// models the retry-once path a real S3Client takes on node faults.
func TestFakeS3FailNextRetry(t *testing.T) {
	f := &FakeS3{
		Objects:               []ObjectInfo{{Key: "k", ETag: "0123456789abcdef0123456789abcdef"}},
		NextContinuationToken: "tok",
		FailNext:              errors.New("connection refused"),
	}
	_, _, _, err := f.ListPage(context.Background(), "p", "", "", false, 100)
	if err == nil || err.Error() != "connection refused" {
		t.Errorf("first call err = %v", err)
	}
	objs, _, next, err := f.ListPage(context.Background(), "p", "", "", false, 100)
	if err != nil {
		t.Fatalf("second call err = %v", err)
	}
	if len(objs) != 1 || objs[0].Key != "k" {
		t.Errorf("objs = %v", objs)
	}
	if next != "tok" {
		t.Errorf("next = %q", next)
	}
	if f.Calls != 2 {
		t.Errorf("calls = %d want 2", f.Calls)
	}
}

func TestIsNodeFaultErr(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{context.DeadlineExceeded, true},
		{errors.New("Get \"http://1.2.3.4:9000/bucket\": dial tcp 1.2.3.4:9000: connect: connection refused"), true},
		{errors.New("read tcp 10.0.0.1:1->10.0.0.2:9000: i/o timeout"), true},
		{errors.New("no such host"), true},
		{minio.ErrorResponse{Code: "InternalError", StatusCode: 500}, true},
		{minio.ErrorResponse{Code: "SlowDown", StatusCode: 503}, true},
		{minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}, false},
		{minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403}, false},
		{errors.New("random business error"), false},
	}
	for _, c := range cases {
		got := isNodeFaultErr(c.err)
		if got != c.want {
			t.Errorf("isNodeFaultErr(%v)=%v want %v", c.err, got, c.want)
		}
	}
}

func TestTrimETagQuotes(t *testing.T) {
	cases := []struct{ in, want string }{
		{`"0123456789abcdef0123456789abcdef"`, "0123456789abcdef0123456789abcdef"},
		{`"abc-2"`, "abc-2"},
		{`abc`, "abc"},
		{`""`, ""},
		{`"`, "\""},
		{`"abc`, `"abc`},
	}
	for _, c := range cases {
		if got := trimETagQuotes(c.in); got != c.want {
			t.Errorf("trimETagQuotes(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

// TestListPagePopulatesOwnerID verifies S3Client.ListPage extracts Owner.ID
// from the minio.ObjectInfo returned by the LIST response into ObjectInfo.
// OwnerID drives per-owner output partitioning, so a missing or wrong OwnerID
// would route writes to the wrong subfolder.
func TestListPagePopulatesOwnerID(t *testing.T) {
	core := &fakeCore{
		v2Result: minio.ListBucketV2Result{
			Contents: []minio.ObjectInfo{
				{Key: "a", ETag: `"0123456789abcdef0123456789abcdef"`, Owner: minio.Owner{ID: "owner-1234"}},
				{Key: "b", ETag: `"abc-2"`, Owner: minio.Owner{ID: "owner-5678"}},
				{Key: "c", ETag: `"def-3"`}, // no owner → empty OwnerID
			},
		},
	}
	c := &S3Client{core: core, bucket: "bk", stats: NewStats(), cfg: &Config{ListAPIVersion: 2}}
	objs, _, _, err := c.ListPage(context.Background(), "p", "", "", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 {
		t.Fatalf("got %d objs, want 3", len(objs))
	}
	want := []string{"owner-1234", "owner-5678", ""}
	for i, w := range want {
		if objs[i].OwnerID != w {
			t.Errorf("objs[%d].OwnerID = %q, want %q", i, objs[i].OwnerID, w)
		}
	}
}

// --- HeadObject / CopyObject / PutObject (backup-mode S3 ops) ---

// newOpsTestClient builds an S3Client bound to an httptest server that
// speaks just enough S3 for HeadObject/CopyObject/PutObject. The server
// host is 127.0.0.1:<port>, so BucketLookupAuto resolves to path style and
// the handler sees /<bucket>/<key>. The ?location= probe that minio-go
// issues before the first API call is answered with us-east-1 so it does
// not leak into the user handler.
func newOpsTestClient(t *testing.T, h http.HandlerFunc) *S3Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "location=" {
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")
	client, err := NewMinioClient(host, "ak", "sk", false)
	if err != nil {
		t.Fatalf("NewMinioClient: %v", err)
	}
	return NewS3Client(client, "srcbucket", NewStats(), nil, -1, &Config{})
}

func TestS3ClientHeadObject(t *testing.T) {
	var gotPath string
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.WriteHeader(200)
	})
	etag, err := c.HeadObject(context.Background(), "k1")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/srcbucket/k1" {
		t.Errorf("HEAD path = %q, want /srcbucket/k1", gotPath)
	}
	if etag != "0123456789abcdef0123456789abcdef" {
		t.Errorf("etag = %q, want quotes stripped", etag)
	}
}

func TestS3ClientHeadObjectMultipartETag(t *testing.T) {
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"abc-3"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.WriteHeader(200)
	})
	etag, err := c.HeadObject(context.Background(), "k1")
	if err != nil {
		t.Fatal(err)
	}
	if etag != "abc-3" {
		t.Errorf("etag = %q, want abc-3", etag)
	}
}

func TestS3ClientHeadObjectNotFound(t *testing.T) {
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	_, err := c.HeadObject(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for 404 HEAD")
	}
	if extractHTTPStatusCode(err) != 404 {
		t.Errorf("status = %d, want 404", extractHTTPStatusCode(err))
	}
}

func TestS3ClientCopyObject(t *testing.T) {
	var gotMethod, gotCopySource, gotPath string
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotCopySource = r.Header.Get("x-amz-copy-source")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><LastModified>2026-09-08T00:00:00Z</LastModified><ETag>"abc"</ETag></CopyObjectResult>`))
	})
	if err := c.CopyObject(context.Background(), "src/key1", "dstbucket", "src/key1"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotPath != "/dstbucket/src/key1" {
		t.Errorf("path = %q, want /dstbucket/src/key1", gotPath)
	}
	if gotCopySource != "srcbucket/src/key1" {
		t.Errorf("x-amz-copy-source = %q, want srcbucket/src/key1", gotCopySource)
	}
}

func TestS3ClientCopyObjectError(t *testing.T) {
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	err := c.CopyObject(context.Background(), "src/key1", "dstbucket", "src/key1")
	if err == nil {
		t.Fatal("expected error for 404 copy")
	}
}

func TestS3ClientPutObject(t *testing.T) {
	var gotPath string
	var gotBody bytes.Buffer
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		io.Copy(&gotBody, r.Body)
		w.WriteHeader(200)
	})
	content := "mybucket|k1\nmybucket|k2|1|0\n"
	if err := c.PutObject(context.Background(), "dstbucket", ".backup_lists/list.txt", strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/dstbucket/.backup_lists/list.txt" {
		t.Errorf("path = %q, want /dstbucket/.backup_lists/list.txt", gotPath)
	}
	if decodeAwsChunked(gotBody.Bytes()) != content {
		t.Errorf("body = %q, want %q", gotBody.String(), content)
	}
}

// decodeAwsChunked strips the streaming-chunked framing that minio-go's
// SigV4 signer wraps around PUT bodies:
//
//	<hex-size>;chunk-signature=<64hex>\r\n<content>\r\n...0;chunk-signature=...\r\n\r\n
//
// A real S3 server decodes this transparently; the test fake must do it
// itself to see the plain content.
func decodeAwsChunked(b []byte) string {
	var out bytes.Buffer
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			break
		}
		header := string(b[:i])
		b = b[i+1:]
		semi := strings.IndexByte(header, ';')
		if semi < 0 {
			continue
		}
		n, err := strconv.ParseUint(header[:semi], 16, 64)
		if err != nil || n == 0 {
			break
		}
		if int64(len(b)) < int64(n)+2 {
			break // truncated
		}
		out.Write(b[:n])
		b = b[n+2:] // content + trailing \r\n
	}
	return out.String()
}

func TestS3ClientPutObjectError(t *testing.T) {
	c := newOpsTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
	})
	err := c.PutObject(context.Background(), "dstbucket", "k", strings.NewReader("x"), 1)
	if err == nil {
		t.Fatal("expected error for 403 put")
	}
}
