// multipart_line_test.go
package main

import (
	"errors"
	"strings"
	"testing"
)

func TestParseMultipartLine(t *testing.T) {
	const bkt = "mybucket"
	cases := []struct {
		name     string
		line     string
		wantKey  string
		wantOffs []int64
		wantErr  string // empty means no error; substring match on err.Error()
	}{
		{
			name:     "single part",
			line:     "mybucket\x1fk1\x1f1\x1f0",
			wantKey:  "k1",
			wantOffs: []int64{0},
		},
		{
			name:     "three parts increasing",
			line:     "mybucket\x1fpath/obj\x1f3\x1f0\x1f5242880\x1f10485760",
			wantKey:  "path/obj",
			wantOffs: []int64{0, 5242880, 10485760},
		},
		{
			name:    "bucket mismatch",
			line:    "other\x1fk\x1f1\x1f0",
			wantErr: "bucket mismatch",
		},
		{
			name:    "empty key",
			line:    "mybucket\x1f\x1f1\x1f0",
			wantErr: "empty key",
		},
		{
			// Whitespace-only keys are rejected at parse time with a
			// resolvable line number instead of a per-call failure entry
			// (minio-go client-side rejection used to catch them; the aws SDK
			// does not client-validate, so this is our own input hygiene).
			name:    "whitespace-only key",
			line:    "mybucket\x1f \x1f1\x1f0",
			wantErr: "empty key",
		},
		{
			name:    "too few fields",
			line:    "mybucket\x1fk",
			wantErr: "too few fields",
		},
		{
			name:    "partcnt not integer",
			line:    "mybucket\x1fk\x1fx\x1f0",
			wantErr: "partcnt not an integer",
		},
		{
			name:    "partcnt zero",
			line:    "mybucket\x1fk\x1f0",
			wantErr: "partcnt must be >= 1",
		},
		{
			name:    "offset count mismatch",
			line:    "mybucket\x1fk\x1f3\x1f0\x1f5242880",
			wantErr: "offset count != partcnt",
		},
		{
			name:    "offset0 not zero",
			line:    "mybucket\x1fk\x1f1\x1f100",
			wantErr: "offset0 must be 0",
		},
		{
			name:    "offset not integer",
			line:    "mybucket\x1fk\x1f2\x1f0\x1fabc",
			wantErr: "offset not an integer",
		},
		{
			name:    "offsets not strictly increasing",
			line:    "mybucket\x1fk\x1f3\x1f0\x1f5242880\x1f5242880",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "offsets decreasing",
			line:    "mybucket\x1fk\x1f3\x1f0\x1f5242880\x1f100",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "negative offset",
			line:    "mybucket\x1fk\x1f2\x1f0\x1f-5",
			wantErr: "offset negative",
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: "empty line",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseMultipartLine(c.line, bkt, 1)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			var mle *MalformedLineError
			if !errors.As(err, &mle) {
				t.Fatalf("expected *MalformedLineError, got %T: %v", err, err)
			}
			if !strings.Contains(mle.Reason, c.wantErr) {
				t.Errorf("err reason = %q, want substring %q", mle.Reason, c.wantErr)
			}
		})
	}
}

func TestParseMultipartLineSuccessFields(t *testing.T) {
	task, err := parseMultipartLine("mybucket\x1fpath%2Fobj\x1f3\x1f0\x1f5242880\x1f10485760", "mybucket", 42)
	if err != nil {
		t.Fatal(err)
	}
	if task.Key != "path/obj" {
		t.Errorf("Key=%q want path/obj", task.Key)
	}
	if !task.IsMultipart {
		t.Errorf("IsMultipart=false, want true (a multipart line always yields a multipart task)")
	}
	if len(task.Offsets) != 3 {
		t.Errorf("len(Offsets)=%d want 3", len(task.Offsets))
	}
	if task.Offsets[2] != 10485760 {
		t.Errorf("Offsets[2]=%d want 10485760", task.Offsets[2])
	}
	// ETag/Size empty for manifest tasks (the line does not carry them);
	// the checker HEADs the object to fill them in (HeadFirst=true).
	if task.ETag != "" || task.Size != 0 {
		t.Errorf("ETag=%q Size=%d, want empty/0", task.ETag, task.Size)
	}
	if !task.HeadFirst {
		t.Errorf("HeadFirst=false, want true (the checker HEADs in Checker.Handle)")
	}
}

// TestParseMultipartLineErrorFields asserts MalformedLineError.Line and
// .LineNum are populated so the caller can write the original line to
// parse_failed for manual fixing.
func TestParseMultipartLineErrorFields(t *testing.T) {
	_, err := parseMultipartLine("mybucket\x1fbad\x1f1\x1f100", "mybucket", 42)
	var mle *MalformedLineError
	if !errors.As(err, &mle) {
		t.Fatalf("expected *MalformedLineError, got %T: %v", err, err)
	}
	if mle.Line != "mybucket\x1fbad\x1f1\x1f100" {
		t.Errorf("Line = %q, want %q", mle.Line, "mybucket\x1fbad\x1f1\x1f100")
	}
	if mle.LineNum != 42 {
		t.Errorf("LineNum = %d, want 42", mle.LineNum)
	}
}
