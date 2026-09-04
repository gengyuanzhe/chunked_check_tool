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

func TestProgressPrintsQueueLengths(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	pp.SetQueueLenProviders(
		func() int { return 7 },  // prefix queue length
		func() int { return 42 }, // objCh length
	)
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	for _, want := range []string{`prefix_queue_len=7`, `obj_ch_len=42`} {
		if !strings.Contains(out, want) {
			t.Errorf("progress line missing %q\nfull line:\n%s", want, out)
		}
	}
}

func TestProgressNoQueueLengthsWhenProvidersNil(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	// no SetQueueLenProviders call
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	if strings.Contains(out, "prefix_queue_len") || strings.Contains(out, "obj_ch_len") {
		t.Errorf("progress line should not contain queue fields when providers are nil\nfull line:\n%s", out)
	}
}
