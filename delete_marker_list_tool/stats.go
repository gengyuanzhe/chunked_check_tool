package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Stats 是 delete_marker_list_tool 的进程级计数器，全 atomic。
// Latency 是 1min 窗口的 ListObjectVersions 时延统计——progress 打印窗口
// avg/max，非全局平均（长跑中全局平均被早期毛刺稀释，看不出当前批的健康度）。
type Stats struct {
	scannedVersions atomic.Int64
	deleteMarkers   atomic.Int64
	listFailed      atomic.Int64
	totalDurationNs atomic.Int64
	Latency         LatencyTracker
}

// StatsSnapshot 是 Stats 的值快照。
type StatsSnapshot struct {
	ScannedVersions int64
	DeleteMarkers   int64
	ListFailed      int64
	TotalSec        float64
}

func (s *Stats) IncrScannedVersions() { s.scannedVersions.Add(1) }
func (s *Stats) IncrDeleteMarkers()   { s.deleteMarkers.Add(1) }
func (s *Stats) IncrListFailed()      { s.listFailed.Add(1) }

func (s *Stats) SetTotalDuration(d time.Duration) {
	s.totalDurationNs.Store(int64(d))
}

func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ScannedVersions: s.scannedVersions.Load(),
		DeleteMarkers:   s.deleteMarkers.Load(),
		ListFailed:      s.listFailed.Load(),
		TotalSec:        float64(s.totalDurationNs.Load()) / 1e9,
	}
}

// PrintSummary 写 === summary === 块到 w。
// 时延统计不进 summary——它是窗口式（1min reset），整体结束时的"最后一窗"
// 没有代表性。用户看 progress 的窗口 avg/max 即可。
func (s *Stats) PrintSummary(w io.Writer) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "scanned_versions=%d\n", snap.ScannedVersions)
	fmt.Fprintf(w, "delete_markers=%d\n", snap.DeleteMarkers)
	fmt.Fprintf(w, "list_failed=%d\n", snap.ListFailed)
	fmt.Fprintf(w, "total_sec=%.2f\n", snap.TotalSec)
}
