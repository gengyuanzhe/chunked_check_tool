package main

import (
	"fmt"
	"io"
	"sync/atomic"
)

type Stats struct {
	Uploaded      atomic.Int64
	Failed        atomic.Int64
	Bytes         atomic.Int64
	SingleObjs    atomic.Int64
	MultipartObjs atomic.Int64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncUploaded(bytes int64) {
	s.Uploaded.Add(1)
	s.Bytes.Add(bytes)
}

func (s *Stats) IncFailed() {
	s.Failed.Add(1)
}

func (s *Stats) IncSingle() {
	s.SingleObjs.Add(1)
}

func (s *Stats) IncMultipart() {
	s.MultipartObjs.Add(1)
}

type StatsSnapshot struct {
	Uploaded      int64
	Failed        int64
	Bytes         int64
	SingleObjs    int64
	MultipartObjs int64
}

func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Uploaded:      s.Uploaded.Load(),
		Failed:        s.Failed.Load(),
		Bytes:        s.Bytes.Load(),
		SingleObjs:    s.SingleObjs.Load(),
		MultipartObjs: s.MultipartObjs.Load(),
	}
}

func (s *Stats) PrintSummary(w io.Writer, elapsedSec float64) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	fmt.Fprintf(w, "uploaded: %d\n", snap.Uploaded)
	fmt.Fprintf(w, "failed: %d\n", snap.Failed)
	fmt.Fprintf(w, "bytes: %d\n", snap.Bytes)
	fmt.Fprintf(w, "single_objs: %d\n", snap.SingleObjs)
	fmt.Fprintf(w, "multipart_objs: %d\n", snap.MultipartObjs)
	fmt.Fprintf(w, "elapsed: %.2f sec\n", elapsedSec)
	if elapsedSec > 0 {
		fmt.Fprintf(w, "rate: %.2f obj/sec\n", float64(snap.Uploaded)/elapsedSec)
	}
}
