package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Stats struct {
	listedTotal      atomic.Int64
	multipartCount   atomic.Int64
	corruptedCount   atomic.Int64
	listFailedCount  atomic.Int64
	checkFailedCount atomic.Int64
	listCalls        atomic.Int64
	listLatencySumNs atomic.Int64
	getCalls         atomic.Int64
	getLatencySumNs  atomic.Int64

	listTotalDuration time.Duration
	totalDuration     time.Duration
}

type StatsSnapshot struct {
	ListedTotal          int64
	Multipart            int64
	Corrupted            int64
	ListFailed           int64
	CheckFailed          int64
	ListCalls            int64
	ListAvgLatencyMs     float64
	ListTotalDurationSec float64
	GetCalls             int64
	GetAvgLatencyMs      float64
	TotalDurationSec     float64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncrListed()     { s.listedTotal.Add(1) }
func (s *Stats) IncrMultipart()  { s.multipartCount.Add(1) }
func (s *Stats) IncrCorrupted()  { s.corruptedCount.Add(1) }
func (s *Stats) IncrListFailed() { s.listFailedCount.Add(1) }
func (s *Stats) IncrCheckFailed() {
	s.checkFailedCount.Add(1)
}

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
	if getCalls > 0 {
		getAvgMs = float64(s.getLatencySumNs.Load()) / float64(getCalls) / 1e6
	}
	return StatsSnapshot{
		ListedTotal:          s.listedTotal.Load(),
		Multipart:            s.multipartCount.Load(),
		Corrupted:            s.corruptedCount.Load(),
		ListFailed:           s.listFailedCount.Load(),
		CheckFailed:          s.checkFailedCount.Load(),
		ListCalls:            calls,
		ListAvgLatencyMs:     avgMs,
		ListTotalDurationSec: s.listTotalDuration.Seconds(),
		GetCalls:             getCalls,
		GetAvgLatencyMs:      getAvgMs,
		TotalDurationSec:     s.totalDuration.Seconds(),
	}
}

func (s *Stats) WriteToFile(path string, isCheck bool) error {
	snap := s.Snapshot()
	var b []byte
	b = append(b, fmt.Sprintf("total_objects: %d\n", snap.ListedTotal)...)
	b = append(b, fmt.Sprintf("list_calls: %d\n", snap.ListCalls)...)
	b = append(b, fmt.Sprintf("list_avg_latency_ms: %.2f\n", snap.ListAvgLatencyMs)...)
	b = append(b, fmt.Sprintf("list_total_duration_sec: %.2f\n", snap.ListTotalDurationSec)...)
	if isCheck {
		b = append(b, fmt.Sprintf("get_calls: %d\n", snap.GetCalls)...)
		b = append(b, fmt.Sprintf("get_avg_latency_ms: %.2f\n", snap.GetAvgLatencyMs)...)
	}
	b = append(b, fmt.Sprintf("total_duration_sec: %.2f\n", snap.TotalDurationSec)...)
	if isCheck {
		b = append(b, fmt.Sprintf("multipart: %d\n", snap.Multipart)...)
		b = append(b, fmt.Sprintf("corrupted: %d\n", snap.Corrupted)...)
	}
	b = append(b, fmt.Sprintf("list_failed: %d\n", snap.ListFailed)...)
	if isCheck {
		b = append(b, fmt.Sprintf("check_failed: %d\n", snap.CheckFailed)...)
	}
	return os.WriteFile(path, b, 0644)
}

func (s *Stats) PrintSummary(isCheck bool) {
	snap := s.Snapshot()
	fmt.Printf("=== summary ===\n")
	fmt.Printf("total_objects: %d\n", snap.ListedTotal)
	fmt.Printf("list_calls: %d avg_latency_ms: %.2f list_total_sec: %.2f total_sec: %.2f\n",
		snap.ListCalls, snap.ListAvgLatencyMs, snap.ListTotalDurationSec, snap.TotalDurationSec)
	if isCheck {
		fmt.Printf("get_calls: %d avg_latency_ms: %.2f\n", snap.GetCalls, snap.GetAvgLatencyMs)
		fmt.Printf("multipart: %d corrupted: %d list_failed: %d check_failed: %d\n",
			snap.Multipart, snap.Corrupted, snap.ListFailed, snap.CheckFailed)
	} else {
		fmt.Printf("list_failed: %d\n", snap.ListFailed)
	}
}
