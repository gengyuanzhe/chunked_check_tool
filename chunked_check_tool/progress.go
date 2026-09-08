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
	w             io.Writer
	mu            sync.Mutex // guards write only, not counting
	mode          RunMode
	queueSnapshot func() QueueSnapshot
}

// QueueSnapshot is a live snapshot of all in-flight queues: the BFS
// prefix queue, the objCh channel between lister and checker, and the
// buffered channels inside Output that feed the various .txt / .log
// writer goroutines. Reported as a group on every progress line so
// backpressure is visible at a glance. Which fields actually print is
// mode-specific (see MaybePrint).
type QueueSnapshot struct {
	Prefix           int // BFS queue length (Queue.Len)
	ObjCh            int // lister→checker channel (len(objCh))
	CorruptedObjects int // → corrupted_objects.txt
	OkMp             int // → mp.txt OR ok_mp.txt
	CorruptedMp      int // → corrupted_mp.txt
	ListFailed       int // → list_failed.txt
	CheckFailed      int // → check_failed.txt
	MpCheckFailed    int // → mp_check_failed.txt
	OkObjects        int // → ok_objects.txt

	// Backup-mode channels (-backup-file). ObjCh above carries the
	// source→worker task channel in that mode.
	BackupOk           int // → backup_ok.txt
	BackupFailed       int // → backup_failed.txt
	Mismatch           int // → mismatch.txt
	BackupSkippedClean int // → backup_skipped_clean.txt
}

func NewProgressPrinter(w io.Writer, mode RunMode) *ProgressPrinter {
	return &ProgressPrinter{w: w, mode: mode}
}

// SetQueueSnapshotProvider injects a callback returning the current
// QueueSnapshot. Called once per progress line. nil = omit the q= field.
func (p *ProgressPrinter) SetQueueSnapshotProvider(fn func() QueueSnapshot) {
	p.queueSnapshot = fn
}

// MaybePrint reads a global stats snapshot and prints one progress line.
// The field set is mode-specific: bucket modes print the list/check
// metrics, -list-file prints input consumption (read) plus the multipart
// outcomes it can produce, -backup-file prints input consumption plus the
// relay outcomes. label is "listed"/"checked"/"backed"; count is the
// worker's local accumulated value since the last print. The snapshot
// fields are read atomically here (once per threshold crossing), never
// per-object.
func (p *ProgressPrinter) MaybePrint(stats *Stats, label string, count int) {
	snap := stats.Snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.mode {
	case ModeListFile:
		fmt.Fprintf(p.w,
			"[progress] read=%d ok_mp=%d corrupt_mp=%d list_failed=%d mp_check_failed=%d get_calls=%d get_avg_ms=%.2f (%s=%d)",
			snap.ReadLines,
			snap.OkMp, snap.CorruptedMp, snap.ListFailed, snap.MpCheckFailed,
			snap.GetCalls, snap.GetAvgLatencyMs, label, count)
	case ModeBackup:
		fmt.Fprintf(p.w,
			"[progress] read=%d list_failed=%d backup_ok=%d backup_failed=%d backup_mismatch=%d backup_skipped_clean=%d get_calls=%d get_avg_ms=%.2f (%s=%d)",
			snap.ReadLines,
			snap.ListFailed, snap.BackupOk, snap.BackupFailed,
			snap.BackupMismatch, snap.BackupSkippedClean,
			snap.GetCalls, snap.GetAvgLatencyMs, label, count)
	default:
		fmt.Fprintf(p.w,
			"[progress] list_all=%d list_obj=%d list_mp=%d ok_obj=%d corrupt_obj=%d ok_mp=%d corrupt_mp=%d list_failed=%d check_failed=%d mp_check_failed=%d list_calls=%d list_avg_ms=%.2f get_calls=%d get_avg_ms=%.2f (%s=%d)",
			snap.ListedAll, snap.ListedObjects, snap.ListedMp,
			snap.OkObjects, snap.CorruptedObjects,
			snap.OkMp, snap.CorruptedMp,
			snap.ListFailed, snap.CheckFailed,
			snap.MpCheckFailed,
			snap.ListCalls, snap.ListAvgLatencyMs,
			snap.GetCalls, snap.GetAvgLatencyMs,
			label, count,
		)
	}
	if p.queueSnapshot != nil {
		q := p.queueSnapshot()
		switch p.mode {
		case ModeListFile:
			fmt.Fprintf(p.w, " q=obj:%d cor_mp:%d ok_mp:%d mcf:%d lf:%d",
				q.ObjCh, q.CorruptedMp, q.OkMp, q.MpCheckFailed, q.ListFailed)
		case ModeBackup:
			fmt.Fprintf(p.w, " q=obj:%d lf:%d bok:%d bfail:%d mm:%d bsc:%d",
				q.ObjCh, q.ListFailed, q.BackupOk, q.BackupFailed, q.Mismatch, q.BackupSkippedClean)
		default:
			fmt.Fprintf(p.w, " q=pfx:%d obj:%d cor_obj:%d ok_o:%d ok_mp:%d cor_mp:%d lf:%d cf:%d mcf:%d",
				q.Prefix, q.ObjCh, q.CorruptedObjects, q.OkObjects, q.OkMp, q.CorruptedMp, q.ListFailed, q.CheckFailed, q.MpCheckFailed)
		}
	}
	fmt.Fprintln(p.w)
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
