package main

import (
	"context"
	"errors"
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
