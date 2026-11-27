package main

import (
	"reflect"
	"testing"
)

func TestResolveOffsets(t *testing.T) {
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
			name: "multipart check off nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, OwnerID: "o"},
			cfg:  &Config{MultipartCheckMode: MultipartCheckModeOff},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, IsMultipart: true, Offsets: nil},
		},
		{
			name: "offset mode parses etag offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-3-0|5242880|10485760", Size: 15 * 1024 * 1024, OwnerID: "o"},
			cfg:  &Config{MultipartCheckMode: MultipartCheckModeOffset},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-3-0|5242880|10485760", Size: 15 * 1024 * 1024, IsMultipart: true, Offsets: []int64{0, 5242880, 10485760}},
		},
		{
			name: "offset mode unparseable etag falls back nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-3", Size: 100, OwnerID: "o"},
			cfg:  &Config{MultipartCheckMode: MultipartCheckModeOffset},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-3", Size: 100, IsMultipart: true, Offsets: nil},
		},
		{
			name: "offset mode normal etag unaffected",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 100, OwnerID: "o"},
			cfg:  &Config{MultipartCheckMode: MultipartCheckModeOffset},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef", Size: 100, IsMultipart: false, Offsets: nil},
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
