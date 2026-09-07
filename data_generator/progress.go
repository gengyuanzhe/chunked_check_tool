package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Progress struct {
	stats    *Stats
	interval int64
	w        io.Writer
	start    time.Time
	counter  atomic.Int64
}

func NewProgress(stats *Stats, interval int, w io.Writer) *Progress {
	return &Progress{
		stats:    stats,
		interval: int64(interval),
		w:        w,
		start:    time.Now(),
	}
}

func (p *Progress) Mark() {
	v := p.counter.Add(1)
	if p.interval <= 0 || v%p.interval != 0 {
		return
	}
	snap := p.stats.Snapshot()
	elapsed := time.Since(p.start).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(snap.Uploaded) / elapsed
	}
	fmt.Fprintf(p.w, "progress: uploaded=%d failed=%d bytes=%d elapsed=%.1fs rate=%.2f obj/sec\n",
		snap.Uploaded, snap.Failed, snap.Bytes, elapsed, rate)
}
