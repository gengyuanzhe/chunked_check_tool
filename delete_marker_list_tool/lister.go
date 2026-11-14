package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// formatLastModified 把 AWS SDK 返回的 time.Time 格式化成 RFC3339 UTC 字符串。
// zero time（列举响应缺 LastModified 时）返回空串——下游可据此判断"无时间戳"。
// 用 RFC3339 而非 ISO8601 因为 Go time.RFC3339 是标准库，且 RFC3339 是 ISO8601
// 的严格子集，S3 / MinIO / 各家 S3 客户端都能解析。
func formatLastModified(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// Lister 驱动两种 mode 的列举。
// mode 1: 单 goroutine 消费一个 recursive 版本化流。
// mode 2: BFS，list_concurrency 个 worker，每 worker 取前缀做
//         delimited 版本化列举，common prefix 入队继续扫。
type Lister struct {
	cfg      *Config
	client   *S3Client
	out      *Output
	stats    *Stats
	progress *ProgressPrinter
}

func NewLister(cfg *Config, client *S3Client, out *Output, stats *Stats, progress *ProgressPrinter) *Lister {
	return &Lister{cfg: cfg, client: client, out: out, stats: stats, progress: progress}
}

// Run 按 cfg.ListType 驱动。
// mode 1 流中断返回 error；mode 2 单前缀失败只记录不返回，全部扫完后返回 nil。
// entries 是初始入队条目列表（至少 1 个元素）；mode 1 只用 entries[0]，
// mode 2 全部入队。retry 工作流传入 list_failed.txt 读出的多个 (prefix, markers) 三元组。
// mode 1 不支持 -retry 多入口（单流语义与多入口冲突），报错让用户改用 -prefix 单值；
// mode 1 + 单 entry retry 支持——从该 entry 的 marker 续扫该 prefix。
func (l *Lister) Run(ctx context.Context, bucket string, entries []RetryEntry) error {
	if l.cfg.ListType == 1 && len(entries) > 1 {
		return fmt.Errorf("mode 1 does not support -retry (multiple prefixes); use -prefix instead")
	}
	if l.cfg.ListType == 1 {
		return l.runMode1(ctx, bucket, entries[0])
	}
	return l.runMode2(ctx, bucket, entries)
}

// runMode1: 单流递归。流中断记 list_failed（含 marker）+ 返回 error。
// entry.KeyMarker/VersionIDMarker 非空 = 从该 marker 起续扫（retry 场景）。
func (l *Lister) runMode1(ctx context.Context, bucket string, entry RetryEntry) error {
	vc, pc, bc := l.client.ListVersions(ctx, bucket, entry.Prefix, true, entry.KeyMarker, entry.VersionIDMarker)
	var localCount int
	interval := l.progress.Interval()
	// recursive=true 不会有 common prefix；pc 单独排空防 goroutine 泄漏。
	pcClosed := make(chan struct{})
	go func() {
		for range pc {
		}
		close(pcClosed)
	}()
	// bc 由独立 goroutine 接收，更新当前 marker——vc.Err 出现时用当前 marker
	// 写 list_failed（页级续传点）。
	var curKeyMarker, curVersionIDMarker string
	bcDone := make(chan struct{})
	go func() {
		defer close(bcDone)
		for pb := range bc {
			curKeyMarker = pb.NextKeyMarker
			curVersionIDMarker = pb.NextVersionIDMarker
		}
	}()
	for obj := range vc {
		if obj.Err != nil {
			<-bcDone
			l.recordListFailed(entry.Prefix, curKeyMarker, curVersionIDMarker, obj.Err)
			l.stats.IncrListFailed()
			<-pcClosed
			return obj.Err
		}
		l.stats.IncrScannedVersions()
		localCount++
		if obj.IsDeleteMarker {
			l.out.WriteDeleteMarker(obj.Key, obj.VersionID, formatLastModified(obj.LastModified))
			l.stats.IncrDeleteMarkers()
		}
		if localCount >= interval {
			l.progress.MaybePrint(l.stats, "scanned", localCount)
			localCount = 0
		}
	}
	l.progress.MaybePrint(l.stats, "scanned", localCount)
	<-bcDone
	<-pcClosed
	return nil
}

// runMode2: BFS delimiter 并发。entries 是初始入队条目列表（至少 1 个）。
// BFS 发现的新 common prefix 入队为 RetryEntry{Prefix: p}（markers 空，从第一页扫）。
func (l *Lister) runMode2(ctx context.Context, bucket string, entries []RetryEntry) error {
	q := newQueue()
	var inflight atomic.Int64

	l.progress.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{Prefix: q.Len(), Inflight: int(inflight.Load())}
	})

	for _, e := range entries {
		inflight.Add(1)
		q.Push(e)
	}

	conc := l.cfg.ListConcurrency
	if conc < 1 {
		conc = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lc := &localCounter{}
			for {
				e, ok := q.Pop(ctx)
				if !ok {
					return
				}
				l.processPrefix(ctx, bucket, e, q, &inflight, lc)
			}
		}()
	}

	wgDone := make(chan struct{})
	go func() {
		defer close(wgDone)
		for {
			if inflight.Load() == 0 {
				q.SignalDone()
				return
			}
			select {
			case <-ctx.Done():
				q.SignalDone()
				return
			default:
			}
		}
	}()
	<-wgDone
	wg.Wait()
	return nil
}

// processPrefix 处理一个条目：消费版本化列举流，分类条目。
// AWS SDK 把 <Version>/<DeleteMarker>/<CommonPrefixes> 解析到独立 slice，
// ListVersions 返回三个 channel：vc（版本，含 marker）+ pc（common prefix）+ bc（页边界）。
// 不再有 isCommonPrefix 启发式——key 以 / 结尾的真实版本不会被误判。
// entry.KeyMarker/VersionIDMarker 非空 = 从该 marker 起续扫（retry 场景）。
// bc goroutine 实时更新 curKeyMarker/curVersionIDMarker——vc.Err 出现时用其写
// list_failed（页级续传点，下页起始 marker）。bc 与 vc 都是无缓冲 channel，
// ListVersions 发 PageBoundary 时本页所有 entry 已被消费——严格同步点。
func (l *Lister) processPrefix(ctx context.Context, bucket string, entry RetryEntry, q *queue, inflight *atomic.Int64, lc *localCounter) {
	defer inflight.Add(-1)
	interval := l.progress.Interval()
	vc, pc, bc := l.client.ListVersions(ctx, bucket, entry.Prefix, false, entry.KeyMarker, entry.VersionIDMarker)
	// pc 由独立 goroutine 排空并把 common prefix 入队——vc、pc、bc 独立消费，
	// 不互相阻塞。vc 排空即该前缀完成。
	pcDone := make(chan struct{})
	go func() {
		for p := range pc {
			inflight.Add(1)
			// 新发现的 common prefix 从第一页扫（markers 空）。
			q.Push(RetryEntry{Prefix: p})
		}
		close(pcDone)
	}()
	// bc goroutine 接收 PageBoundary，更新 curKeyMarker/curVersionIDMarker。
	// 出现 Err 后等 bcDone 确保最新 marker 已写入本地变量（无 race）。
	var curKeyMarker, curVersionIDMarker string
	bcDone := make(chan struct{})
	go func() {
		defer close(bcDone)
		for pb := range bc {
			curKeyMarker = pb.NextKeyMarker
			curVersionIDMarker = pb.NextVersionIDMarker
		}
	}()
	for obj := range vc {
		if obj.Err != nil {
			<-bcDone
			l.recordListFailed(entry.Prefix, curKeyMarker, curVersionIDMarker, obj.Err)
			l.stats.IncrListFailed()
			<-pcDone
			return
		}
		l.stats.IncrScannedVersions()
		lc.count++
		if obj.IsDeleteMarker {
			l.out.WriteDeleteMarker(obj.Key, obj.VersionID, formatLastModified(obj.LastModified))
			l.stats.IncrDeleteMarkers()
		}
		if lc.count >= interval {
			l.progress.MaybePrint(l.stats, "scanned", lc.count)
			lc.count = 0
		}
	}
	<-bcDone
	<-pcDone
}

// recordListFailed 从 err 提取 HTTP/S3 code + reqID，写 list_failed.txt + log。
// keyMarker/versionIDMarker 是失败时已扫完的最后一页的 next-marker；
// 同 prefix 重扫时从该 marker 起续扫（页级续传）。
func (l *Lister) recordListFailed(prefix, keyMarker, versionIDMarker string, err error) {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		l.out.WriteListFailed(prefix, keyMarker, versionIDMarker)
		l.out.WriteListFailedLog(prefix, re.Response.StatusCode, "", re.Response.Header.Get("x-amz-request-id"), err)
		return
	}
	l.out.WriteListFailed(prefix, keyMarker, versionIDMarker)
	l.out.WriteListFailedLog(prefix, 0, "", "", err)
}

// localCounter 是 worker 本地进度计数（无 per-obj atomic）。
type localCounter struct {
	count int
}
