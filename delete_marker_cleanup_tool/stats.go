package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Stats 是 delete_marker_cleanup_tool 的进程级计数器，全 atomic。
// mode check 用 marker / notMarker / absent / headFailed；
// mode delete 用 deleted / deleteFailed；processed / totalSecNs 共用。
// Latency 是 1min 窗口的请求时延统计——progress 打印窗口 avg/max，非全局平均。
type Stats struct {
	processed     atomic.Int64
	marker         atomic.Int64
	notMarker      atomic.Int64
	absent         atomic.Int64
	headFailed     atomic.Int64
	deleted        atomic.Int64
	deleteFailed   atomic.Int64
	totalMicrosec  atomic.Int64
	Latency        LatencyTracker
}

func (s *Stats) IncrProcessed()   { s.processed.Add(1) }
func (s *Stats) IncrMarker()      { s.marker.Add(1) }
func (s *Stats) IncrNotMarker()    { s.notMarker.Add(1) }
func (s *Stats) IncrAbsent()       { s.absent.Add(1) }
func (s *Stats) IncrHeadFailed()   { s.headFailed.Add(1) }
func (s *Stats) IncrDeleted()      { s.deleted.Add(1) }
func (s *Stats) IncrDeleteFailed() { s.deleteFailed.Add(1) }

func (s *Stats) SetTotalDuration(d time.Duration) {
	s.totalMicrosec.Store(int64(d.Microseconds()))
}

// PrintSummary 按 mode 写 === summary === 块到 w。
// mode check：processed / marker / not_marker / absent / head_failed / total_sec
// mode delete：processed / deleted / delete_failed / total_sec
func (s *Stats) PrintSummary(w io.Writer, mode string) {
	processed := s.processed.Load()
	totalSec := float64(s.totalMicrosec.Load()) / 1e6
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "processed=%d\n", processed)
	switch mode {
	case "check":
		fmt.Fprintf(w, "marker=%d\n", s.marker.Load())
		fmt.Fprintf(w, "not_marker=%d\n", s.notMarker.Load())
		fmt.Fprintf(w, "absent=%d\n", s.absent.Load())
		fmt.Fprintf(w, "head_failed=%d\n", s.headFailed.Load())
	case "delete":
		fmt.Fprintf(w, "deleted=%d\n", s.deleted.Load())
		fmt.Fprintf(w, "delete_failed=%d\n", s.deleteFailed.Load())
	}
	fmt.Fprintf(w, "total_sec=%.2f\n", totalSec)
}
