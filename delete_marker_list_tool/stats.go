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
//
// 字段语义（严格按 S3 XML 标签划分，互斥）：
//
//	versions       — 仅 <Version>（普通版本，有数据），不含 delete marker
//	deleteMarkers  — 仅 <DeleteMarker>
//	commonPrefixes — delimiter=/ 列举到的子目录数（mode 1 用 recursive 不带 delimiter，恒为 0）
//	listFailed     — ListVersionsPage 失败次数
//
// Scanned = versions + deleteMarkers + commonPrefixes，用作 progress 打印阈值与 summary 展示，
// 让 mode 2 下行期（大量 common prefix、几乎无 version）也能稳定推进打印。
type Stats struct {
	versions        atomic.Int64
	commonPrefixes  atomic.Int64
	deleteMarkers   atomic.Int64
	listFailed      atomic.Int64
	totalDurationNs atomic.Int64
	Latency         LatencyTracker
}

// StatsSnapshot 是 Stats 的值快照。
type StatsSnapshot struct {
	Scanned        int64 // Versions + DeleteMarkers + CommonPrefixes
	Versions       int64 // 仅普通版本
	CommonPrefixes int64
	DeleteMarkers  int64
	ListFailed     int64
	TotalSec       float64
}

func (s *Stats) IncrVersions()       { s.versions.Add(1) }
func (s *Stats) IncrCommonPrefixes() { s.commonPrefixes.Add(1) }
func (s *Stats) IncrDeleteMarkers()  { s.deleteMarkers.Add(1) }
func (s *Stats) IncrListFailed()     { s.listFailed.Add(1) }

func (s *Stats) SetTotalDuration(d time.Duration) {
	s.totalDurationNs.Store(int64(d))
}

func (s *Stats) Snapshot() StatsSnapshot {
	v := s.versions.Load()
	dm := s.deleteMarkers.Load()
	cp := s.commonPrefixes.Load()
	return StatsSnapshot{
		Scanned:        v + dm + cp,
		Versions:       v,
		CommonPrefixes: cp,
		DeleteMarkers:  dm,
		ListFailed:     s.listFailed.Load(),
		TotalSec:       float64(s.totalDurationNs.Load()) / 1e9,
	}
}

// PrintSummary 写 === summary === 块到 w。
// 时延统计不进 summary——它是窗口式（1min reset），整体结束时的"最后一窗"
// 没有代表性。用户看 progress 的窗口 avg/max 即可。
func (s *Stats) PrintSummary(w io.Writer) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "scanned=%d\n", snap.Scanned)
	fmt.Fprintf(w, "versions=%d\n", snap.Versions)
	fmt.Fprintf(w, "common_prefixes=%d\n", snap.CommonPrefixes)
	fmt.Fprintf(w, "delete_markers=%d\n", snap.DeleteMarkers)
	fmt.Fprintf(w, "list_failed=%d\n", snap.ListFailed)
	fmt.Fprintf(w, "total_sec=%.2f\n", snap.TotalSec)
}
