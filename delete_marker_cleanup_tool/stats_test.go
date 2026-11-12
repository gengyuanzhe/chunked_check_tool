package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestStatsCheckSnapshotAndSummary(t *testing.T) {
	var s Stats
	for i := 0; i < 100; i++ {
		s.IncrProcessed()
	}
	for i := 0; i < 50; i++ {
		s.IncrMarker()
	}
	for i := 0; i < 10; i++ {
		s.IncrNotMarker()
	}
	for i := 0; i < 38; i++ {
		s.IncrAbsent()
	}
	for i := 0; i < 2; i++ {
		s.IncrHeadFailed()
	}
	var buf bytes.Buffer
	s.PrintSummary(&buf, "check")
	out := buf.String()
	for _, want := range []string{"processed=100", "marker=50", "not_marker=10", "absent=38", "head_failed=2", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("check summary missing %q: %s", want, out)
		}
	}
}

func TestStatsDeleteSnapshotAndSummary(t *testing.T) {
	var s Stats
	for i := 0; i < 100; i++ {
		s.IncrProcessed()
	}
	for i := 0; i < 98; i++ {
		s.IncrDeleted()
	}
	for i := 0; i < 2; i++ {
		s.IncrDeleteFailed()
	}
	var buf bytes.Buffer
	s.PrintSummary(&buf, "delete")
	out := buf.String()
	for _, want := range []string{"processed=100", "deleted=98", "delete_failed=2", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("delete summary missing %q: %s", want, out)
		}
	}
}

func TestProgressPrinterCheck(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetInterval(10)
	var s Stats
	for i := 0; i < 5; i++ {
		s.IncrMarker()
	}
	p.MaybePrintCheck(&s, 10)
	out := buf.String()
	if !strings.Contains(out, "marker=5") {
		t.Errorf("missing marker=5: %q", out)
	}
	if !strings.Contains(out, "not_marker=0") {
		t.Errorf("missing not_marker=0: %q", out)
	}
}

func TestProgressPrinterDelete(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetInterval(10)
	var s Stats
	for i := 0; i < 7; i++ {
		s.IncrDeleted()
	}
	p.MaybePrintDelete(&s, 10)
	out := buf.String()
	if !strings.Contains(out, "deleted=7") {
		t.Errorf("missing deleted=7: %q", out)
	}
}
