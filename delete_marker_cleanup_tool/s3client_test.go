package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newFakeHeadDeleteS3 routes by method.
// HEAD /<bkt>                  → 200 (bucket exists)
// HEAD /<bkt>/<key>?versionId= → 405+marker / 200+ETag / 404 NoSuchVersion / 500
// DELETE /<bkt>/<key>?versionId= → 204 / 404 NoSuchVersion (if vid=="already-deleted")
func newFakeHeadDeleteS3(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 2 || parts[1] == "" {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		bkt, key := parts[0], parts[1]
		vid := r.URL.Query().Get("versionId")
		switch r.Method {
		case http.MethodHead:
			switch {
			case bkt == "testbucket" && key == "marker.txt" && vid == "dm-1":
				w.Header().Set("x-amz-delete-marker", "true")
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			case bkt == "testbucket" && key == "regular.txt" && vid == "v-1":
				w.Header().Set("ETag", `"abc"`)
				w.Header().Set("Content-Length", "5")
				w.Header().Set("Last-Modified", "Mon, 02 Jan 2026 15:04:05 GMT")
				w.WriteHeader(http.StatusOK)
				return
			case bkt == "testbucket" && key == "gone.txt" && vid == "v-1":
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchVersion</Code></Error>`))
				return
			}
			http.Error(w, "boom", http.StatusInternalServerError)
		case http.MethodDelete:
			if vid == "already-deleted" {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchVersion</Code></Error>`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method", http.StatusBadRequest)
		}
	}))
}

func TestStatObject_MarkerReturns405(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := c.StatObject(context.Background(), "testbucket", "marker.txt", "dm-1")
	if !r.IsDeleteMarker {
		t.Errorf("IsDeleteMarker=false want true (405 + x-amz-delete-marker: true)")
	}
	if r.Err != nil {
		t.Errorf("unexpected err: %v", r.Err)
	}
}

func TestStatObject_RegularObjectReturnsInfo(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := c.StatObject(context.Background(), "testbucket", "regular.txt", "v-1")
	if r.Err != nil {
		t.Fatalf("err: %v", r.Err)
	}
	if r.IsDeleteMarker {
		t.Errorf("IsDeleteMarker=true want false")
	}
}

func TestStatObject_AbsentReturnsNoSuchVersion(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := c.StatObject(context.Background(), "testbucket", "gone.txt", "v-1")
	if r.Err == nil {
		t.Fatal("expected error")
	}
	// aws sdk 对 HEAD 的 404 返回 code="NotFound"（HEAD 无 body，SDK 用状态码推断），
	// 不是 "NoSuchVersion"——classifier 按 StatusCode==404 兜底判定为 absent。
	if r.StatusCode != 404 {
		t.Errorf("StatusCode=%d want 404", r.StatusCode)
	}
}

func TestDeleteObject_Success(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.DeleteObject(context.Background(), "testbucket", "any.txt", "v-1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestDeleteObject_NoSuchVersion(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.DeleteObject(context.Background(), "testbucket", "any.txt", "already-deleted")
	if err == nil {
		return
	}
	if code := errorCode(err); code != "NoSuchVersion" {
		t.Errorf("code=%q want NoSuchVersion", code)
	}
}

func TestBucketExists(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
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
