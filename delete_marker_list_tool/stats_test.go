package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestStatsSnapshot(t *testing.T) {
	var s Stats
	for i := 0; i < 100; i++ {
		s.IncrVersions()
	}
	for i := 0; i < 30; i++ {
		s.IncrCommonPrefixes()
	}
	for i := 0; i < 7; i++ {
		s.IncrDeleteMarkers()
	}
	for i := 0; i < 2; i++ {
		s.IncrListFailed()
	}
	snap := s.Snapshot()
	if snap.Versions != 100 {
		t.Errorf("Versions=%d want 100", snap.Versions)
	}
	if snap.CommonPrefixes != 30 {
		t.Errorf("CommonPrefixes=%d want 30", snap.CommonPrefixes)
	}
	if snap.DeleteMarkers != 7 {
		t.Errorf("DeleteMarkers=%d want 7", snap.DeleteMarkers)
	}
	if snap.Scanned != 137 {
		t.Errorf("Scanned=%d want 137 (versions + delete_markers + common_prefixes)", snap.Scanned)
	}
	if snap.ListFailed != 2 {
		t.Errorf("ListFailed=%d want 2", snap.ListFailed)
	}
}

func TestPrintSummaryContainsAllFields(t *testing.T) {
	var s Stats
	s.IncrDeleteMarkers()
	for i := 0; i < 42; i++ {
		s.IncrDeleteMarkers()
	}
	for i := 0; i < 1000; i++ {
		s.IncrVersions()
	}
	s.IncrCommonPrefixes()
	s.IncrListFailed()
	var buf bytes.Buffer
	s.PrintSummary(&buf)
	out := buf.String()
	for _, want := range []string{"scanned", "versions", "common_prefixes", "delete_markers", "list_failed", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q: %s", want, out)
		}
	}
}

func TestProgressPrinterMaybePrintThreshold(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	var s Stats
	// First call at count=1 with default interval 5000 should NOT print.
	p.MaybePrint(&s, "scanned", 1, 0)
	if buf.Len() != 0 {
		t.Errorf("expected no print below threshold, got %q", buf.String())
	}
	p.MaybePrint(&s, "scanned", 5000, 0)
	if buf.Len() == 0 {
		t.Errorf("expected progress line at threshold")
	}
}

func TestProgressPrinterWithQueueSnapshot(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{Prefix: 3, Inflight: 2}
	})
	var s Stats
	s.IncrVersions()
	p.MaybePrint(&s, "scanned", 5000, 0)
	out := buf.String()
	if !strings.Contains(out, "q=3") {
		t.Errorf("missing q=3: %q", out)
	}
	if strings.Contains(out, "inflight=") {
		t.Errorf("inflight should not be in progress line: %q", out)
	}
	if !strings.Contains(out, "heap=") {
		t.Errorf("missing heap= field: %q", out)
	}
	if !strings.Contains(out, "versions=") {
		t.Errorf("missing versions= field: %q", out)
	}
	if !strings.Contains(out, "worker=0") {
		t.Errorf("missing worker=0 field: %q", out)
	}
}

func TestProgressPrinterCustomInterval(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetInterval(10)
	var s Stats
	p.MaybePrint(&s, "scanned", 9, 0)
	if buf.Len() != 0 {
		t.Errorf("expected no print at 9 with interval 10")
	}
	p.MaybePrint(&s, "scanned", 10, 0)
	if buf.Len() == 0 {
		t.Errorf("expected print at 10 with interval 10")
	}
}
