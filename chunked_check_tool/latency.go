package main

import (
	"sync"
	"time"
)

// LatencyTracker is a windowed request-latency gauge: every window it
// resets, so Snapshot reports the CURRENT batch's count/avg/max instead of
// a global average. A long run's global average gets diluted by early
// spikes and hides current-batch health — the windowed form surfaces it.
//
// Zero value is usable: the first Record/Snapshot locks windowStart and
// defaults the window to one minute; SetWindow overrides (before first use
// — a mid-run change takes effect at the next rotation).
type LatencyTracker struct {
	mu          sync.Mutex
	window      time.Duration
	windowStart time.Time
	sumNs       int64
	count       int64
	maxNs       int64
}

// LatencySnapshot is one point-in-time read of the current window. Count==0
// means the window has no samples yet (or just rotated).
type LatencySnapshot struct {
	Count int64
	AvgMs float64
	MaxMs float64
}

// Record adds one request latency. An expired window rotates first.
func (l *LatencyTracker) Record(d time.Duration) {
	ns := int64(d)
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.ensureWindowLocked(now)
	if now.Sub(l.windowStart) >= l.window {
		l.rotateLocked(now)
	}
	l.sumNs += ns
	l.count++
	if ns > l.maxNs {
		l.maxNs = ns
	}
}

// Snapshot reads the current window. An expired window rotates first, so a
// quiet window reports zeroed stats rather than stale samples.
func (l *LatencyTracker) Snapshot() LatencySnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.ensureWindowLocked(now)
	if now.Sub(l.windowStart) >= l.window {
		l.rotateLocked(now)
	}
	if l.count == 0 {
		return LatencySnapshot{}
	}
	return LatencySnapshot{
		Count: l.count,
		AvgMs: float64(l.sumNs) / float64(l.count) / 1e6,
		MaxMs: float64(l.maxNs) / 1e6,
	}
}

// SetWindow sets the window length; d <= 0 keeps the default (1 minute).
func (l *LatencyTracker) SetWindow(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if d > 0 {
		l.window = d
	}
}

// ensureWindowLocked backstops the zero value: window 0 means "unset".
func (l *LatencyTracker) ensureWindowLocked(now time.Time) {
	if l.window <= 0 {
		l.window = time.Minute
	}
	if l.windowStart.IsZero() {
		l.windowStart = now
	}
}

func (l *LatencyTracker) rotateLocked(now time.Time) {
	l.sumNs = 0
	l.count = 0
	l.maxNs = 0
	l.windowStart = now
}
