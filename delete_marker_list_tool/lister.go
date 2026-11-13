package main

import (
	"context"
	"errors"
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
func (l *Lister) Run(ctx context.Context, bucket, prefix string) error {
	if l.cfg.ListType == 1 {
		return l.runMode1(ctx, bucket, prefix)
	}
	return l.runMode2(ctx, bucket, prefix)
}

// runMode1: 单流递归。流中断记 list_failed + 返回 error。
func (l *Lister) runMode1(ctx context.Context, bucket, prefix string) error {
	vc, pc := l.client.ListVersions(ctx, bucket, prefix, true)
	var localCount int
	interval := l.progress.Interval()
	// recursive=true 不会有 common prefix；pc 单独排空防 goroutine 泄漏。
	pcClosed := make(chan struct{})
	go func() {
		for range pc {
		}
		close(pcClosed)
	}()
	for obj := range vc {
		if obj.Err != nil {
			l.recordListFailed(prefix, obj.Err)
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
	<-pcClosed
	return nil
}

// runMode2: BFS delimiter 并发。
func (l *Lister) runMode2(ctx context.Context, bucket, prefix string) error {
	q := newQueue()
	var inflight atomic.Int64

	l.progress.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{Prefix: q.Len(), Inflight: int(inflight.Load())}
	})

	inflight.Add(1)
	q.Push(prefix)

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
				p, ok := q.Pop(ctx)
				if !ok {
					return
				}
				l.processPrefix(ctx, bucket, p, q, &inflight, lc)
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

// processPrefix 处理一个前缀：消费版本化列举流，分类条目。
// AWS SDK 把 <Version>/<DeleteMarker>/<CommonPrefixes> 解析到独立 slice，
// ListVersions 返回两个 channel：vc（版本，含 marker）+ pc（common prefix）。
// 不再有 isCommonPrefix 启发式——key 以 / 结尾的真实版本不会被误判。
func (l *Lister) processPrefix(ctx context.Context, bucket, prefix string, q *queue, inflight *atomic.Int64, lc *localCounter) {
	defer inflight.Add(-1)
	interval := l.progress.Interval()
	vc, pc := l.client.ListVersions(ctx, bucket, prefix, false)
	// pc 由独立 goroutine 排空并把 common prefix 入队——vc、pc 独立消费，
	// 不互相阻塞。vc 排空即该前缀完成。
	pcDone := make(chan struct{})
	go func() {
		for p := range pc {
			inflight.Add(1)
			q.Push(p)
		}
		close(pcDone)
	}()
	for obj := range vc {
		if obj.Err != nil {
			l.recordListFailed(prefix, obj.Err)
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
	<-pcDone
}

// recordListFailed 从 err 提取 HTTP/S3 code + reqID，写 list_failed.txt + log。
func (l *Lister) recordListFailed(prefix string, err error) {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		l.out.WriteListFailed(prefix)
		l.out.WriteListFailedLog(prefix, re.Response.StatusCode, "", re.Response.Header.Get("x-amz-request-id"), err)
		return
	}
	l.out.WriteListFailed(prefix)
	l.out.WriteListFailedLog(prefix, 0, "", "", err)
}

// localCounter 是 worker 本地进度计数（无 per-obj atomic）。
type localCounter struct {
	count int
}
