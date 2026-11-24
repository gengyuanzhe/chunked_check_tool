package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseLineFormat(t *testing.T) {
	cases := []struct {
		format  string
		wantErr bool
		segs    []fmtSegment
	}{
		{`<bucket>|<key>`, false, []fmtSegment{
			{field: "bucket"}, {literal: "|"}, {field: "key"},
		}},
		{`<bucket>`, false, []fmtSegment{{field: "bucket"}}},
		{`<key>`, false, []fmtSegment{{field: "key"}}},
		{`<owner>/<bucket>/<key>`, false, []fmtSegment{
			{field: "owner"}, {literal: "/"}, {field: "bucket"}, {literal: "/"}, {field: "key"},
		}},
		{`<bucket>|<key>|<etag>`, true, nil}, // unknown field etag
		{`<bucket`, true, nil},               // unterminated
		{``, true, nil},                      // empty
	}
	for _, c := range cases {
		got, err := parseLineFormat(c.format)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseLineFormat(%q): want error, got nil (%+v)", c.format, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseLineFormat(%q): unexpected err: %v", c.format, err)
			continue
		}
		if len(got) != len(c.segs) {
			t.Errorf("parseLineFormat(%q): got %d segments, want %d (%+v)", c.format, len(got), len(c.segs), got)
			continue
		}
		for i, s := range got {
			if s.field != c.segs[i].field || s.literal != c.segs[i].literal {
				t.Errorf("parseLineFormat(%q) seg %d: got %+v, want %+v", c.format, i, s, c.segs[i])
			}
		}
	}
}

func TestLineFormatWriteTo(t *testing.T) {
	cf, err := parseLineFormat(`<bucket>|<key>`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	cf.writeTo(&buf, "mybucket", "ownerX", "path/obj")
	if got := buf.String(); got != "mybucket|path%2Fobj" {
		t.Errorf("got %q, want %q", got, "mybucket|path%2Fobj")
	}
}

func TestLineFormatOwnerField(t *testing.T) {
	cf, _ := parseLineFormat(`<owner>:<bucket>:<key>`)
	var buf bytes.Buffer
	cf.writeTo(&buf, "bkt", "own1", "k1")
	if got := buf.String(); got != "own1:bkt:k1" {
		t.Errorf("got %q, want %q", got, "own1:bkt:k1")
	}
	// key containing '|' — the value is escaped at render, so the literal
	// '|' in the key cannot tear the line's separator structure.
	buf.Reset()
	cf.writeTo(&buf, "bkt", "own1", "a|b")
	if got := buf.String(); !strings.HasSuffix(got, ":bkt:a%7Cb") || !strings.HasPrefix(got, "own1:bkt:") {
		t.Errorf("key with | produced %q", got)
	}
}
