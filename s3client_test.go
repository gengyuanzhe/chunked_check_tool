package main

import (
	"context"
	"errors"
	"testing"
)

func TestFakeS3ListPage(t *testing.T) {
	f := &FakeS3{
		Objects: []ObjectInfo{
			{Key: "a", ETag: "0123456789abcdef0123456789abcdef"},
			{Key: "b", ETag: "0123456789abcdef0123456789abcdef-2"},
		},
		CommonPrefixes: []string{"sub/"},
		NextStartAfter: "b",
	}
	objs, prefixes, next, err := f.ListPage(context.Background(), "p", "", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 || objs[1].Key != "b" {
		t.Errorf("objs = %v", objs)
	}
	if len(prefixes) != 1 || prefixes[0] != "sub/" {
		t.Errorf("prefixes = %v", prefixes)
	}
	if next != "b" {
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
	_, _, _, err := f.ListPage(context.Background(), "", "", false, 100)
	if err == nil || err.Error() != "boom" {
		t.Errorf("err = %v", err)
	}
}
