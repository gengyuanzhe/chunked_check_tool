package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// newFakeHeadDeleteS3 routes by method. minio-go sends a GET location lookup
// before HEAD/DELETE — that must return LocationConstraint XML, otherwise the
// client fails before the real request.
// HEAD /<bkt>                  → 200 (bucket exists)
// GET  /<bkt> or /             → LocationConstraint XML
// HEAD /<bkt>/<key>?versionId= → 405+marker / 200+ETag / 404 NoSuchVersion / 500
// DELETE /<bkt>/<key>?versionId= → 204 / 404 NoSuchVersion (if vid=="already-deleted")
func newFakeHeadDeleteS3(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Bucket-level request (no key segment): location lookup or HEAD bucket.
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
	info, err := c.StatObject(context.Background(), "testbucket", "marker.txt", "dm-1")
	if info.IsDeleteMarker {
		return
	}
	if err == nil {
		t.Fatalf("expected IsDeleteMarker=true or MethodNotAllowed error; got info=%+v err=nil", info)
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T: %v", err, err)
	}
	if er.Code != "MethodNotAllowed" {
		t.Errorf("err code=%q want MethodNotAllowed", er.Code)
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
	info, err := c.StatObject(context.Background(), "testbucket", "regular.txt", "v-1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if info.IsDeleteMarker {
		t.Errorf("IsDeleteMarker=true want false")
	}
	if info.ETag != `abc` {
		t.Errorf("ETag=%q want %q", info.ETag, `abc`)
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
	_, err = c.StatObject(context.Background(), "testbucket", "gone.txt", "v-1")
	if err == nil {
		t.Fatal("expected error")
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T", err)
	}
	// minio-go may surface this as NoSuchKey or NoSuchVersion — both map to
	// "absent" in classifier.go, so accept either.
	if er.Code != "NoSuchVersion" && er.Code != "NoSuchKey" {
		t.Errorf("code=%q want NoSuchVersion or NoSuchKey", er.Code)
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
	if err := c.DeleteObject(context.Background(), "testbucket", "any.txt", "v-1"); err != nil {
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
	err = c.DeleteObject(context.Background(), "testbucket", "any.txt", "already-deleted")
	if err == nil {
		return
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T", err)
	}
	if er.Code != "NoSuchVersion" {
		t.Errorf("code=%q want NoSuchVersion", er.Code)
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
