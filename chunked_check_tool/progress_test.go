package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestProgressLocalCounter(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	lc := &localCounter{interval: 3, printer: pp}
	for i := 0; i < 7; i++ {
		lc.incr(s, "checked")
	}
	out := buf.String()
	// Should print at n=3 and n=6, then reset — 2 lines total.
	if strings.Count(out, "[progress]") != 2 {
		t.Errorf("expected 2 progress lines, got:\n%s", out)
	}
}

func TestProgressNoPrintBelowInterval(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	lc := &localCounter{interval: 1000, printer: pp}
	for i := 0; i < 100; i++ {
		lc.incr(s, "listed")
	}
	if buf.Len() != 0 {
		t.Errorf("should not print, got %q", buf.String())
	}
}
