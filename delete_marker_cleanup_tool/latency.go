package main

import (
	"sync"
	"time"
)

// LatencyTracker 是窗口式请求时延统计——每 window 重置一次。
// 打印时返回当前窗口的 count/avg/max，让用户看到"最近一批"的时延，
// 而不是全局平均（长跑中全局平均被早期毛刺稀释，看不出当前批的健康度）。
//
// 零值可用：第一次 Record/Snapshot 时 windowStart 为 zero time，触发 rotate
// 锁定 windowStart=now，之后正常工作。默认 window=1min。
type LatencyTracker struct {
	mu          sync.Mutex
	window      time.Duration
	windowStart time.Time
	sumNs       int64
	count       int64
	maxNs       int64
}

// LatencySnapshot 是 LatencyTracker 的值快照。Count==0 表示窗口内尚无样本。
type LatencySnapshot struct {
	Count int64
	AvgMs float64
	MaxMs float64
}

// Record 记录一次请求时延。窗口过期先 rotate 再记。
func (l *LatencyTracker) Record(d time.Duration) {
	ns := int64(d)
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.ensureWindowLocked(now)
	if now.Sub(l.windowStart) >= l.window {
		l.rotateLocked(now)
	}
	l.sumNs += ns
	l.count++
	if ns > l.maxNs {
		l.maxNs = ns
	}
}

// Snapshot 返回当前窗口的 count/avg/max。窗口过期先 rotate——
// 打印时若窗口已过期则返回空窗口（count=0），用户看到当前分钟已积累的样本。
func (l *LatencyTracker) Snapshot() LatencySnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.ensureWindowLocked(now)
	if now.Sub(l.windowStart) >= l.window {
		l.rotateLocked(now)
	}
	if l.count == 0 {
		return LatencySnapshot{}
	}
	return LatencySnapshot{
		Count: l.count,
		AvgMs: float64(l.sumNs) / float64(l.count) / 1e6,
		MaxMs: float64(l.maxNs) / 1e6,
	}
}

// SetWindow 设置窗口长度。<=0 视为默认 1min。必须在第一次 Record/Snapshot 前
// 调——运行中改窗口不影响当前窗口（rotate 时才生效）。
func (l *LatencyTracker) SetWindow(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if d > 0 {
		l.window = d
	}
}

// ensureWindowLocked 兜底零值 LatencyTracker：window 为 0 设默认 1min。
func (l *LatencyTracker) ensureWindowLocked(now time.Time) {
	if l.window <= 0 {
		l.window = time.Minute
	}
}

func (l *LatencyTracker) rotateLocked(now time.Time) {
	l.sumNs = 0
	l.count = 0
	l.maxNs = 0
	l.windowStart = now
}
