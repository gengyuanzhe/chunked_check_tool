package main

import (
	"fmt"
	"io"
	"sync"
)

// QueueSnapshot 是进度行的队列快照。mode 2 填 Prefix+Inflight；
// mode 1 不填（无队列）。
type QueueSnapshot struct {
	Prefix   int // BFS 队列长度
	Inflight int // inflight WaitGroup 计数
}

// ProgressPrinter 串行化进度行写到一个共享 io.Writer。
// 不维护 per-object 计数——每 worker 本地计数，跨阈值时调 MaybePrint。
type ProgressPrinter struct {
	w       io.Writer
	mu      sync.Mutex
	interval int
	queueSnapshot func() QueueSnapshot
}

// NewProgressPrinter 构造 ProgressPrinter。interval 是阈值（处理条目数），
// 0 视为默认 5000。
func NewProgressPrinter(w io.Writer) *ProgressPrinter {
	return &ProgressPrinter{w: w, interval: 5000}
}

// SetInterval 覆盖阈值。
func (p *ProgressPrinter) SetInterval(n int) {
	if n > 0 {
		p.interval = n
	}
}

// Interval 返回当前阈值。caller 用它在本地累积到阈值后调 MaybePrint，
// 打印后 reset 本地计数器——否则越过阈值后每行都打一行 progress。
func (p *ProgressPrinter) Interval() int {
	return p.interval
}

// SetQueueSnapshotProvider 注入队列快照回调（mode 2 用）。
func (p *ProgressPrinter) SetQueueSnapshotProvider(fn func() QueueSnapshot) {
	p.queueSnapshot = fn
}

// MaybePrint 在 count >= interval 时打印一行进度。label 是字段名
// （"scanned"），count 是 worker 自上次打印以来的本地累积值。
func (p *ProgressPrinter) MaybePrint(s *Stats, label string, count int) {
	if count < p.interval {
		return
	}
	snap := s.Snapshot()
	var qs QueueSnapshot
	if p.queueSnapshot != nil {
		qs = p.queueSnapshot()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queueSnapshot != nil {
		fmt.Fprintf(p.w,
			"[progress] scanned=%d markers=%d list_failed=%d (q=%d inflight=%d)\n",
			snap.ScannedVersions, snap.DeleteMarkers, snap.ListFailed,
			qs.Prefix, qs.Inflight)
	} else {
		fmt.Fprintf(p.w,
			"[progress] scanned=%d markers=%d list_failed=%d\n",
			snap.ScannedVersions, snap.DeleteMarkers, snap.ListFailed)
	}
}
