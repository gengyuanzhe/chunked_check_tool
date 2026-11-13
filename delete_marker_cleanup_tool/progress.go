package main

import (
	"fmt"
	"sync"
)

// ProgressPrinter 串行化进度行写到一个共享 io.Writer。
// 不维护 per-object 计数——每 worker 本地计数，跨阈值时调 MaybePrint*。
type ProgressPrinter struct {
	w interface {
		Write(p []byte) (int, error)
	}
	mu       sync.Mutex
	interval int
}

// NewProgressPrinter 构造 ProgressPrinter。interval 是阈值（处理条目数），
// 0 视为默认 5000。
func NewProgressPrinter(w interface {
	Write(p []byte) (int, error)
}) *ProgressPrinter {
	return &ProgressPrinter{w: w, interval: 5000}
}

// SetInterval 覆盖阈值。
func (p *ProgressPrinter) SetInterval(n int) {
	if n > 0 {
		p.interval = n
	}
}

// Interval 返回当前阈值。worker 用它在本地累积到阈值后调 MaybePrint*，
// 打印后 reset 本地计数器——否则越过阈值后每行都打一行 progress。
func (p *ProgressPrinter) Interval() int {
	return p.interval
}

// MaybePrintCheck 在 count >= interval 时打印一行 check 模式进度。
// not_marker 字段必须可见——用户工作流"跑 check 看 not_marker，非 0 就停手"。
// latency 是 1min 窗口的请求时延（avg/max），非全局平均。
func (p *ProgressPrinter) MaybePrintCheck(s *Stats, count int) {
	if count < p.interval {
		return
	}
	lat := s.Latency.Snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "[progress] processed=%d marker=%d not_marker=%d absent=%d failed=%d latency_avg_ms=%.2f latency_max_ms=%.2f\n",
		s.processed.Load(), s.marker.Load(), s.notMarker.Load(), s.absent.Load(), s.headFailed.Load(),
		lat.AvgMs, lat.MaxMs)
}

// MaybePrintDelete 在 count >= interval 时打印一行 delete 模式进度。
// latency 是 1min 窗口的请求时延（avg/max），非全局平均。
func (p *ProgressPrinter) MaybePrintDelete(s *Stats, count int) {
	if count < p.interval {
		return
	}
	lat := s.Latency.Snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "[progress] processed=%d deleted=%d failed=%d latency_avg_ms=%.2f latency_max_ms=%.2f\n",
		s.processed.Load(), s.deleted.Load(), s.deleteFailed.Load(),
		lat.AvgMs, lat.MaxMs)
}
