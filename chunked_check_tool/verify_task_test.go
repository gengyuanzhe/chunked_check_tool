package main

import (
	"reflect"
	"testing"
)

func TestResolveOffsets(t *testing.T) {
	const seg = int64(5 * 1024 * 1024)
	cases := []struct {
		name string
		obj  ObjectInfo
		cfg  *Config
		want VerifyTask
	}{
		{
			name: "normal etag nil offsets (verify probes offset 0 implicitly)",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 100, OwnerID: "o"},
			cfg:  &Config{},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef", Size: 100, IsMultipart: false, Offsets: nil},
		},
		{
			name: "multipart segcheck on builds ceil size/seg offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024, IsMultipart: true, Offsets: []int64{0, seg}},
		},
		{
			name: "multipart segcheck on size not multiple of seg",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-3", Size: 12*1024*1024 + 1, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-3", Size: 12*1024*1024 + 1, IsMultipart: true, Offsets: []int64{0, seg, 2 * seg}},
		},
		{
			name: "multipart segcheck off nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: false, MultipartSegmentSize: 0},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, IsMultipart: true, Offsets: nil},
		},
		{
			name: "multipart segcheck on but size zero nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 0, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 0, IsMultipart: true, Offsets: nil},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveOffsets(c.obj, c.cfg)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("resolveOffsets mismatch\ngot:  %+v\nwant: %+v", got, c.want)
			}
		})
	}
}
