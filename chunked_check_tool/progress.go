package main

import (
	"fmt"
	"io"
	"sync"
)

// ProgressPrinter serializes progress line writes to a shared io.Writer.
// It does NOT maintain per-object counters — each worker keeps a local
// counter and calls MaybePrint only when its local count crosses the
// threshold, so the hot path stays atomic-free.
type ProgressPrinter struct {
	w  io.Writer
	mu sync.Mutex // guards write only, not counting
}

func NewProgressPrinter(w io.Writer) *ProgressPrinter {
	return &ProgressPrinter{w: w}
}

// MaybePrint reads a global stats snapshot and prints one progress line.
// label is "checked" or "listed"; count is the worker's local accumulated
// value since the last print. The snapshot fields are read atomically here
// (once per threshold crossing), never per-object.
func (p *ProgressPrinter) MaybePrint(stats *Stats, label string, count int) {
	snap := stats.Snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w,
		"[progress] listed=%d checked=%d multipart=%d corrupted=%d list_failed=%d check_failed=%d (%s=%d)\n",
		snap.ListedTotal, snap.ListedTotal,
		snap.Multipart, snap.Corrupted,
		snap.ListFailed, snap.CheckFailed,
		label, count,
	)
}

// localCounter is a per-worker progress counter. Workers call incr() on every
// object processed; when the local count reaches the interval, MaybePrint fires
// and the counter resets. No atomic op per object — only the snapshot read
// inside MaybePrint touches atomics, and only once per `interval` objects.
type localCounter struct {
	n        int
	interval int
	printer  *ProgressPrinter
}

func (lc *localCounter) incr(stats *Stats, label string) {
	lc.n++
	if lc.n >= lc.interval {
		lc.printer.MaybePrint(stats, label, lc.n)
		lc.n = 0
	}
}
