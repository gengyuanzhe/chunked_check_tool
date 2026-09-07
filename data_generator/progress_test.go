package main

import (
	"strings"
	"testing"
)

func TestProgress_PrintsAtInterval(t *testing.T) {
	s := NewStats()
	var sb strings.Builder
	p := NewProgress(s, 10, &sb)
	for i := 0; i < 10; i++ {
		p.Mark()
	}
	out := sb.String()
	if !strings.Contains(out, "progress") {
		t.Errorf("expected 1 progress print at 10 marks; got %q", out)
	}
}

func TestProgress_PrintsMultipleIntervals(t *testing.T) {
	s := NewStats()
	var sb strings.Builder
	p := NewProgress(s, 10, &sb)
	for i := 0; i < 25; i++ {
		p.Mark()
	}
	out := sb.String()
	count := strings.Count(out, "progress")
	if count != 2 {
		t.Errorf("expected 2 progress prints (at 10 and 20), got %d; out=%q", count, out)
	}
}

func TestProgress_ZeroIntervalNoPrint(t *testing.T) {
	s := NewStats()
	var sb strings.Builder
	p := NewProgress(s, 0, &sb)
	for i := 0; i < 100; i++ {
		p.Mark()
	}
	if sb.Len() != 0 {
		t.Errorf("expected no prints with interval=0; got %q", sb.String())
	}
}

func TestProgress_PrintFormat(t *testing.T) {
	s := NewStats()
	s.IncUploaded(1024)
	s.IncUploaded(2048)
	s.IncFailed()
	var sb strings.Builder
	p := NewProgress(s, 1, &sb)
	p.Mark()
	out := sb.String()
	for _, want := range []string{"uploaded", "failed", "bytes", "elapsed", "rate"} {
		if !strings.Contains(out, want) {
			t.Errorf("progress missing %q; out=%q", want, out)
		}
	}
}
