package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type RunMode int

const (
	ModeListCheck RunMode = iota // bucket LIST + check (is_check=true)
	ModeListOnly                 // bucket LIST only (is_check=false)
	ModeListFile                 // -list-file: verify tasks read from a local file
	ModeBackup                   // -backup-file: HEAD + verify + relay
)

// readTotal renders the read/total progress fraction: "12/5000", or just
// "12" when the total is unknown (0 — input line counting failed or the
// file was empty).
func readTotal(read, total int64) string {
	if total > 0 {
		return fmt.Sprintf("%d/%d", read, total)
	}
	return fmt.Sprintf("%d", read)
}

type Stats struct {
	listedObjectsCount    atomic.Int64
	listedMpCount         atomic.Int64
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
	backupOkCount         atomic.Int64
	backupFailedCount     atomic.Int64
	backupMismatchCount   atomic.Int64
	backupSkippedCleanCnt atomic.Int64

	// Input-file consumption for -list-file / -backup-file: every line read
	// (valid or malformed) bumps readLines; totalLines is set once at
	// startup from a line count of the input file. read=X/Y is the progress
	// denominator for the file modes, replacing the bucket modes' list_all.
	readLines  atomic.Int64
	totalLines atomic.Int64

	// Durations are atomics because SetListDuration is called from the
	// goroutine that closes objCh while the main goroutine may already be
	// reading Snapshot for the summary — there is no happens-before edge
	// between the two.
	listTotalDurationNs atomic.Int64
	totalDurationNs     atomic.Int64
}

type StatsSnapshot struct {
	ListedObjects      int64
	ListedMp           int64
	ListedAll          int64
	OkObjects          int64
	OkMp               int64
	CorruptedObjects   int64
	CorruptedMp        int64
	ListFailed         int64
	CheckFailed        int64
	MpCheckFailed      int64
	ListCalls          int64
	ListAvgLatencyMs   float64
	ListTotalSec       float64
	GetCalls           int64
	GetAvgLatencyMs    float64
	GetTotalSec        float64
	BackupOk           int64
	BackupFailed       int64
	BackupMismatch     int64
	BackupSkippedClean int64
	ReadLines          int64
	TotalLines         int64
	TotalSec           float64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncrListedObject()       { s.listedObjectsCount.Add(1) }
func (s *Stats) IncrListedMp()           { s.listedMpCount.Add(1) }
func (s *Stats) IncrOkObjects()          { s.okObjectsCount.Add(1) }
func (s *Stats) IncrOkMp()               { s.okMpCount.Add(1) }
func (s *Stats) IncrCorruptedObjects()   { s.corruptedObjectsCount.Add(1) }
func (s *Stats) IncrCorruptedMp()        { s.corruptedMpCount.Add(1) }
func (s *Stats) IncrListFailed()         { s.listFailedCount.Add(1) }
func (s *Stats) IncrCheckFailed()        { s.checkFailedCount.Add(1) }
func (s *Stats) IncrMpCheckFailed()      { s.mpCheckFailedCount.Add(1) }
func (s *Stats) IncrBackupOk()           { s.backupOkCount.Add(1) }
func (s *Stats) IncrBackupFailed()       { s.backupFailedCount.Add(1) }
func (s *Stats) IncrBackupMismatch()     { s.backupMismatchCount.Add(1) }
func (s *Stats) IncrBackupSkippedClean() { s.backupSkippedCleanCnt.Add(1) }
func (s *Stats) IncrReadLine()           { s.readLines.Add(1) }
func (s *Stats) SetTotalLines(n int64)   { s.totalLines.Store(n) }

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

func (s *Stats) SetListDuration(d time.Duration)  { s.listTotalDurationNs.Store(int64(d)) }
func (s *Stats) SetTotalDuration(d time.Duration) { s.totalDurationNs.Store(int64(d)) }

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
	listedObj := s.listedObjectsCount.Load()
	listedMp := s.listedMpCount.Load()
	return StatsSnapshot{
		ListedObjects:      listedObj,
		ListedMp:           listedMp,
		ListedAll:          listedObj + listedMp,
		OkObjects:          s.okObjectsCount.Load(),
		OkMp:               s.okMpCount.Load(),
		CorruptedObjects:   s.corruptedObjectsCount.Load(),
		CorruptedMp:        s.corruptedMpCount.Load(),
		ListFailed:         s.listFailedCount.Load(),
		CheckFailed:        s.checkFailedCount.Load(),
		MpCheckFailed:      s.mpCheckFailedCount.Load(),
		ListCalls:          calls,
		ListAvgLatencyMs:   avgMs,
		ListTotalSec:       time.Duration(s.listTotalDurationNs.Load()).Seconds(),
		GetCalls:           getCalls,
		GetAvgLatencyMs:    getAvgMs,
		GetTotalSec:        getTotalSec,
		BackupOk:           s.backupOkCount.Load(),
		BackupFailed:       s.backupFailedCount.Load(),
		BackupMismatch:     s.backupMismatchCount.Load(),
		BackupSkippedClean: s.backupSkippedCleanCnt.Load(),
		ReadLines:          s.readLines.Load(),
		TotalLines:         s.totalLines.Load(),
		TotalSec:           time.Duration(s.totalDurationNs.Load()).Seconds(),
	}
}

// PrintSummary writes the final === summary === block. The metric set is
// mode-specific: the bucket modes print list_* + check outcomes, while
// -list-file and -backup-file print input consumption (read=X/Y) plus only
// the outcomes those modes can produce — no S3 LIST happens, so list_all/
// list_calls would be all-zero noise.
func (s *Stats) PrintSummary(w io.Writer, mode RunMode) {
	snap := s.Snapshot()
	fmt.Fprintf(w, "=== summary ===\n")
	switch mode {
	case ModeListFile, ModeBackup:
		fmt.Fprintf(w, "read: %s total_sec: %.2f\n",
			readTotal(snap.ReadLines, snap.TotalLines), snap.TotalSec)
		fmt.Fprintf(w, "list_failed: %d\n", snap.ListFailed)
		// get_calls stays: multipart verification goes through RangeGetAt.
		fmt.Fprintf(w, "get_calls: %d avg_latency_ms: %.2f get_total_sec: %.2f\n",
			snap.GetCalls, snap.GetAvgLatencyMs, snap.GetTotalSec)
		if mode == ModeListFile {
			fmt.Fprintf(w, "ok_mp: %d corrupt_mp: %d mp_check_failed: %d\n",
				snap.OkMp, snap.CorruptedMp, snap.MpCheckFailed)
			return
		}
		fmt.Fprintf(w, "backup_ok: %d backup_failed: %d backup_mismatch: %d backup_skipped_clean: %d\n",
			snap.BackupOk, snap.BackupFailed, snap.BackupMismatch, snap.BackupSkippedClean)
	case ModeListCheck:
		fmt.Fprintf(w, "list_all: %d (list_obj: %d list_mp: %d) total_sec: %.2f\n",
			snap.ListedAll, snap.ListedObjects, snap.ListedMp, snap.TotalSec)
		fmt.Fprintf(w, "list_calls: %d avg_latency_ms: %.2f list_total_sec: %.2f\n",
			snap.ListCalls, snap.ListAvgLatencyMs, snap.ListTotalSec)
		fmt.Fprintf(w, "get_calls: %d avg_latency_ms: %.2f get_total_sec: %.2f\n",
			snap.GetCalls, snap.GetAvgLatencyMs, snap.GetTotalSec)
		fmt.Fprintf(w, "ok_obj: %d corrupt_obj: %d ok_mp: %d corrupt_mp: %d list_failed: %d check_failed: %d mp_check_failed: %d\n",
			snap.OkObjects, snap.CorruptedObjects, snap.OkMp, snap.CorruptedMp,
			snap.ListFailed, snap.CheckFailed, snap.MpCheckFailed)
	default: // ModeListOnly
		fmt.Fprintf(w, "list_all: %d (list_obj: %d list_mp: %d) total_sec: %.2f\n",
			snap.ListedAll, snap.ListedObjects, snap.ListedMp, snap.TotalSec)
		fmt.Fprintf(w, "list_calls: %d avg_latency_ms: %.2f list_total_sec: %.2f\n",
			snap.ListCalls, snap.ListAvgLatencyMs, snap.ListTotalSec)
		fmt.Fprintf(w, "list_failed: %d\n", snap.ListFailed)
	}
}
