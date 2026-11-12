package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Stats 是 delete_marker_list_tool 的进程级计数器，全 atomic。
type Stats struct {
	scannedVersions  atomic.Int64
	deleteMarkers    atomic.Int64
	listFailed       atomic.Int64
	listCalls        atomic.Int64
	listLatencySumNs atomic.Int64
	totalDurationNs  atomic.Int64
}

// StatsSnapshot 是 Stats 的值快照。
type StatsSnapshot struct {
	ScannedVersions  int64
	DeleteMarkers    int64
	ListFailed       int64
	ListCalls        int64
	ListAvgLatencyMs float64
	TotalSec         float64
}

func (s *Stats) IncrScannedVersions()   { s.scannedVersions.Add(1) }
func (s *Stats) IncrDeleteMarkers()     { s.deleteMarkers.Add(1) }
func (s *Stats) IncrListFailed()        { s.listFailed.Add(1) }
func (s *Stats) IncListCalls()          { s.listCalls.Add(1) }
func (s *Stats) AddListLatency(ns int64) { s.listLatencySumNs.Add(ns) }

func (s *Stats) SetTotalDuration(d time.Duration) {
	s.totalDurationNs.Store(int64(d))
}

func (s *Stats) Snapshot() StatsSnapshot {
	calls := s.listCalls.Load()
	var avgMs float64
	if calls > 0 {
		avgMs = float64(s.listLatencySumNs.Load()) / float64(calls) / 1e6
	}
	return StatsSnapshot{
		ScannedVersions:  s.scannedVersions.Load(),
		DeleteMarkers:    s.deleteMarkers.Load(),
		ListFailed:       s.listFailed.Load(),
		ListCalls:        calls,
		ListAvgLatencyMs: avgMs,
		TotalSec:         float64(s.totalDurationNs.Load()) / 1e9,
	}
}

// PrintSummary 写 === summary === 块到 w。
func (s *Stats) PrintSummary(w io.Writer) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "scanned_versions=%d\n", snap.ScannedVersions)
	fmt.Fprintf(w, "delete_markers=%d\n", snap.DeleteMarkers)
	fmt.Fprintf(w, "list_failed=%d\n", snap.ListFailed)
	fmt.Fprintf(w, "list_calls=%d\n", snap.ListCalls)
	fmt.Fprintf(w, "list_avg_latency_ms=%.2f\n", snap.ListAvgLatencyMs)
	fmt.Fprintf(w, "total_sec=%.2f\n", snap.TotalSec)
}
