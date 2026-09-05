package main

import (
	"fmt"
	"io"
)

// compiledLineFormat is a pre-parsed result-line template: a sequence of
// literal segments and field references. The writer goroutine walks this
// sequence per line and writes the literal or the field value, avoiding
// any per-object allocation (no fmt.Sprintf, no strings.ReplaceAll).
//
// Supported field tokens in the format string: <bucket>, <key>, <owner>.
// Anything else inside <...> is a parse error. Literal text (including
// the "|" separator) is written verbatim.
type compiledLineFormat []fmtSegment

type fmtSegment struct {
	literal string
	field   string // "" means literal segment; otherwise one of bucket/key/owner
}

func parseLineFormat(format string) (compiledLineFormat, error) {
	var out compiledLineFormat
	i := 0
	for i < len(format) {
		// find next '<'
		j := i
		for j < len(format) && format[j] != '<' {
			j++
		}
		if j > i {
			out = append(out, fmtSegment{literal: format[i:j]})
		}
		if j == len(format) {
			break
		}
		// j points at '<'. Find closing '>'.
		k := j + 1
		for k < len(format) && format[k] != '>' {
			k++
		}
		if k == len(format) {
			return nil, fmt.Errorf("result_line_format: unterminated <%s> (missing '>')", format[j+1:])
		}
		field := format[j+1 : k]
		switch field {
		case "bucket", "key", "owner":
		default:
			return nil, fmt.Errorf("result_line_format: unknown field <%s> (want <bucket>, <key>, or <owner>)", field)
		}
		out = append(out, fmtSegment{field: field})
		i = k + 1
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("result_line_format: empty format")
	}
	return out, nil
}

// writeTo writes the formatted line for one (bucket, owner, key) triple to w.
// Called once per result line in the per-owner writer goroutines.
func (cf compiledLineFormat) writeTo(w io.Writer, bucket, owner, key string) {
	for _, s := range cf {
		if s.field == "" {
			w.Write([]byte(s.literal))
			continue
		}
		switch s.field {
		case "bucket":
			w.Write([]byte(bucket))
		case "key":
			w.Write([]byte(key))
		case "owner":
			w.Write([]byte(owner))
		}
	}
}
