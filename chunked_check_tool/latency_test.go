package main

import (
	"sync"
	"testing"
	"time"
)

// TestLatencyTrackerWindowedStats — Record/Snapshot report the CURRENT
// window's avg/max, not a global average: a long run's global average gets
// diluted by early spikes and hides current-batch health.
func TestLatencyTrackerWindowedStats(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Minute)
	tr.Record(10 * time.Millisecond)
	tr.Record(30 * time.Millisecond)
	snap := tr.Snapshot()
	if snap.Count != 2 {
		t.Errorf("Count = %d, want 2", snap.Count)
	}
	if snap.AvgMs != 20 {
		t.Errorf("AvgMs = %f, want 20", snap.AvgMs)
	}
	if snap.MaxMs != 30 {
		t.Errorf("MaxMs = %f, want 30", snap.MaxMs)
	}
}

// TestLatencyTrackerMonotonicMax — a smaller sample after a larger one must
// not lower the window's max. Pins latency.go:45 `if ns > l.maxNs`.
func TestLatencyTrackerMonotonicMax(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Minute)
	tr.Record(50 * time.Millisecond)
	tr.Record(10 * time.Millisecond)
	tr.Record(20 * time.Millisecond)
	snap := tr.Snapshot()
	if snap.MaxMs != 50 {
		t.Errorf("MaxMs = %f, want 50 (max must be monotonic)", snap.MaxMs)
	}
}

// TestLatencyTrackerSetWindowMidRun — a mid-run SetWindow takes effect at the
// next rotation: after the old window expires, the new (shorter) window
// rotates sooner than the old one would have. Pins the doc claim.
func TestLatencyTrackerSetWindowMidRun(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Minute)
	tr.Record(10 * time.Millisecond)
	tr.SetWindow(time.Millisecond)
	time.Sleep(3 * time.Millisecond)
	// Old window is now long-expired; Snapshot must rotate to the new 1ms
	// window and report an empty one.
	snap := tr.Snapshot()
	if snap.Count != 0 {
		t.Errorf("after mid-run SetWindow + expiry, Count = %d, want 0 (rotated under new window)", snap.Count)
	}
	tr.Record(7 * time.Millisecond)
	if snap = tr.Snapshot(); snap.Count != 1 || snap.AvgMs != 7 {
		t.Errorf("after fresh record under new window = %+v, want {1, 7, 7}", snap)
	}
}

// TestLatencyTrackerSnapshotIsIdempotent — Snapshot is a read, not a consume:
// calling it twice on the same non-expired window returns the same count.
// Pins that Snapshot does not reset/rotate a live window.
func TestLatencyTrackerSnapshotIsIdempotent(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Minute)
	tr.Record(10 * time.Millisecond)
	tr.Record(20 * time.Millisecond)
	a := tr.Snapshot()
	b := tr.Snapshot()
	if a != b {
		t.Errorf("Snapshot not idempotent: a=%+v b=%+v", a, b)
	}
}

// TestLatencyTrackerConcurrentRecord — concurrent Record/Snapshot/SetWindow
// must be race-free under -race. Exercises the mutex on real contention.
func TestLatencyTrackerConcurrentRecord(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				tr.Record(time.Duration(n*j+1) * time.Millisecond)
				_ = tr.Snapshot()
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			tr.SetWindow(time.Minute)
		}
	}()
	wg.Wait()
	snap := tr.Snapshot()
	if snap.Count == 0 {
		t.Errorf("expected samples from concurrent records, got Count=0")
	}
}

// TestLatencyTrackerRotatesOnWindowExpiry — once the window has elapsed,
// Snapshot returns an empty window (the old samples aged out) and new
// Records accumulate fresh.
func TestLatencyTrackerRotatesOnWindowExpiry(t *testing.T) {
	tr := &LatencyTracker{}
	tr.SetWindow(time.Millisecond)
	tr.Record(10 * time.Millisecond)
	time.Sleep(3 * time.Millisecond)
	snap := tr.Snapshot()
	if snap.Count != 0 || snap.AvgMs != 0 || snap.MaxMs != 0 {
		t.Errorf("after window expiry = %+v, want zeroed", snap)
	}
	tr.Record(5 * time.Millisecond)
	snap = tr.Snapshot()
	if snap.Count != 1 || snap.AvgMs != 5 {
		t.Errorf("after new record = %+v, want {1, 5, 5}", snap)
	}
}

// TestLatencyTrackerZeroValueUsable — the zero-value tracker locks its
// window on first use (1 minute default); SetWindow(<=0) keeps the default.
func TestLatencyTrackerZeroValueUsable(t *testing.T) {
	tr := &LatencyTracker{}
	tr.Record(2 * time.Millisecond)
	if snap := tr.Snapshot(); snap.Count != 1 || snap.AvgMs != 2 {
		t.Errorf("zero-value tracker = %+v, want {1, 2, 2}", snap)
	}
	tr.SetWindow(0)
	tr.Record(2 * time.Millisecond)
	if snap := tr.Snapshot(); snap.Count != 2 {
		t.Errorf("SetWindow(0) should keep default, count = %d want 2", snap.Count)
	}
}

// TestStatsWindowedListLatency — AddListCall feeds both the global counters
// (kept for the summary) and the windowed tracker (progress lines); the
// window length is configurable via the stats handle so main can pass
// cfg.latency_window_sec through.
func TestStatsWindowedListLatency(t *testing.T) {
	s := NewStats()
	s.SetListLatencyWindow(time.Minute)
	s.AddListCall(4 * time.Millisecond)
	s.AddListCall(8 * time.Millisecond)
	snap := s.Snapshot()
	if snap.ListCalls != 2 || snap.ListAvgLatencyMs != 6 {
		t.Errorf("global = calls %d avg %f, want 2 / 6", snap.ListCalls, snap.ListAvgLatencyMs)
	}
	if snap.ListLatencyAvgMs != 6 || snap.ListLatencyMaxMs != 8 {
		t.Errorf("windowed = %+v, want avg 6 max 8", snap)
	}
}
