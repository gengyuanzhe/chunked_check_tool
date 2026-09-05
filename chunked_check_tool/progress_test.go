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
	// Seed some calls so list_calls/get_calls show non-zero values.
	s.AddListCall(1_000_000) // 1ms
	s.AddGetCall(2_000_000)  // 2ms
	s.IncrCorruptedMultipart()
	s.IncrMultipartCheckFailed()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	pp.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{
			Prefix: 7, ObjCh: 42,
			CorruptedObjects: 1, MultipartOk: 2,
			CorruptedMultipart:   6,
			ListFailed:           3, CheckFailed: 4, Success: 5,
			MultipartCheckFailed: 8,
		}
	})
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	for _, want := range []string{
		`corrupted_mp=1`,
		`multipart_check_failed=1`,
		`list_calls=1`,
		`get_calls=1`,
		`q=pfx:7`,
		`obj:42`,
		`cor_obj:1`,
		`mp_ok:2`,
		`cmp:6`,
		`mcf:8`,
		`lf:3`,
		`cf:4`,
		`su:5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("progress line missing %q\nfull line:\n%s", want, out)
		}
	}
}

func TestProgressNoQueueLengthsWhenProvidersNil(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	// no SetQueueSnapshotProvider call
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	if strings.Contains(out, "q=") {
		t.Errorf("progress line should not contain queue fields when provider is nil\nfull line:\n%s", out)
	}
}
