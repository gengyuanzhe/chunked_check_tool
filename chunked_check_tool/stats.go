package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Stats struct {
	listedTotal           atomic.Int64
	okObjectsCount        atomic.Int64
	okMpCount             atomic.Int64
	corruptedObjectsCount atomic.Int64
	corruptedMpCount      atomic.Int64
	listFailedCount       atomic.Int64
	checkFailedCount      atomic.Int64
	mpCheckFailedCount    atomic.Int64
	listCalls             atomic.Int64
	listLatencySumNs      atomic.Int64
	getCalls              atomic.Int64
	getLatencySumNs       atomic.Int64

	listTotalDuration time.Duration
	totalDuration     time.Duration
}

type StatsSnapshot struct {
	ListedTotal      int64
	OkObjects        int64
	OkMp             int64
	CorruptedObjects int64
	CorruptedMp      int64
	ListFailed       int64
	CheckFailed      int64
	MpCheckFailed    int64
	ListCalls        int64
	ListAvgLatencyMs float64
	ListTotalSec     float64
	GetCalls         int64
	GetAvgLatencyMs  float64
	GetTotalSec      float64
	TotalSec         float64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncrListed()           { s.listedTotal.Add(1) }
func (s *Stats) IncrOkObjects()        { s.okObjectsCount.Add(1) }
func (s *Stats) IncrOkMp()             { s.okMpCount.Add(1) }
func (s *Stats) IncrCorruptedObjects() { s.corruptedObjectsCount.Add(1) }
func (s *Stats) IncrCorruptedMp()      { s.corruptedMpCount.Add(1) }
func (s *Stats) IncrListFailed()       { s.listFailedCount.Add(1) }
func (s *Stats) IncrCheckFailed()      { s.checkFailedCount.Add(1) }
func (s *Stats) IncrMpCheckFailed()    { s.mpCheckFailedCount.Add(1) }

func (s *Stats) AddListCall(latency time.Duration) {
	s.listCalls.Add(1)
	s.listLatencySumNs.Add(int64(latency))
}

// AddGetCall records one RangeGet invocation's latency. Called once per
// public RangeGet call (covering both the initial attempt and any node-
// fault retry), so getCalls == successful RangeGets + RangeGets that
// eventually failed.
func (s *Stats) AddGetCall(latency time.Duration) {
	s.getCalls.Add(1)
	s.getLatencySumNs.Add(int64(latency))
}

func (s *Stats) SetListDuration(d time.Duration)  { s.listTotalDuration = d }
func (s *Stats) SetTotalDuration(d time.Duration) { s.totalDuration = d }

func (s *Stats) Snapshot() StatsSnapshot {
	calls := s.listCalls.Load()
	var avgMs float64
	if calls > 0 {
		avgMs = float64(s.listLatencySumNs.Load()) / float64(calls) / 1e6
	}
	getCalls := s.getCalls.Load()
	var getAvgMs float64
	var getTotalSec float64
	if getCalls > 0 {
		getAvgMs = float64(s.getLatencySumNs.Load()) / float64(getCalls) / 1e6
		getTotalSec = float64(s.getLatencySumNs.Load()) / 1e9
	}
	return StatsSnapshot{
		ListedTotal:      s.listedTotal.Load(),
		OkObjects:        s.okObjectsCount.Load(),
		OkMp:             s.okMpCount.Load(),
		CorruptedObjects: s.corruptedObjectsCount.Load(),
		CorruptedMp:      s.corruptedMpCount.Load(),
		ListFailed:       s.listFailedCount.Load(),
		CheckFailed:      s.checkFailedCount.Load(),
		MpCheckFailed:    s.mpCheckFailedCount.Load(),
		ListCalls:        calls,
		ListAvgLatencyMs: avgMs,
		ListTotalSec:     s.listTotalDuration.Seconds(),
		GetCalls:         getCalls,
		GetAvgLatencyMs:  getAvgMs,
		GetTotalSec:      getTotalSec,
		TotalSec:         s.totalDuration.Seconds(),
	}
}

func (s *Stats) PrintSummary(w io.Writer, isCheck bool) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "total_objects: %d total_sec: %.2f\n", snap.ListedTotal, snap.TotalSec)
	fmt.Fprintf(w, "list_calls: %d avg_latency_ms: %.2f list_total_sec: %.2f\n",
		snap.ListCalls, snap.ListAvgLatencyMs, snap.ListTotalSec)
	if isCheck {
		fmt.Fprintf(w, "get_calls: %d avg_latency_ms: %.2f get_total_sec: %.2f\n", snap.GetCalls, snap.GetAvgLatencyMs, snap.GetTotalSec)
		fmt.Fprintf(w, "ok_objects: %d corrupted_objects: %d ok_mp: %d corrupted_mp: %d list_failed: %d check_failed: %d mp_check_failed: %d\n",
			snap.OkObjects, snap.CorruptedObjects, snap.OkMp, snap.CorruptedMp, snap.ListFailed, snap.CheckFailed, snap.MpCheckFailed)
	} else {
		fmt.Fprintf(w, "list_failed: %d\n", snap.ListFailed)
	}
}
