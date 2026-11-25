package main

import (
	"fmt"
	"io"
	"net/url"
)

// compiledLineFormat is a pre-parsed result-line template: a sequence of
// literal segments and field references. The writer goroutine walks this
// sequence per line and writes the literal or the field value, avoiding
// any per-object allocation (no fmt.Sprintf, no strings.ReplaceAll).
//
// Supported field tokens in the format string: <bucket>, <key>, <owner>.
// Anything else inside <...> is a parse error. Literal text (including
// the default "|" separator) is written verbatim.
//
// Server-field VALUES are QueryEscape'd at render time (boundary B): <key>
// comes from the LIST response and may contain the format's own separator,
// newlines, or \x1f; the escaped value cannot, so the line structure survives
// hostile keys no matter what separator the format uses. <bucket> comes from
// the CLI flag and stays raw. <owner> is [a-z0-9] when non-empty (server
// convention; empty means the LIST response omitted it and routes to the
// _unknown/ directory), so it's written verbatim with no escape. Result files
// are terminal outputs (human/script consumers), not tool round-trip inputs.
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
			w.Write([]byte(url.QueryEscape(key)))
		case "owner":
			w.Write([]byte(owner))
		}
	}
}
