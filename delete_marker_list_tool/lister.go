package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

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
	ch := l.client.ListVersions(ctx, bucket, prefix, true)
	var localCount int
	interval := l.progress.Interval()
	for obj := range ch {
		if obj.Err != nil {
			l.recordListFailed(prefix, obj.Err)
			l.stats.IncrListFailed()
			return obj.Err
		}
		l.stats.IncrScannedVersions()
		localCount++
		if l.isCommonPrefix(obj) {
			// recursive=true 不会吐 common prefix，但兜底跳过。
			continue
		}
		if obj.IsDeleteMarker {
			l.out.WriteDeleteMarker(obj.Key, obj.VersionID)
			l.stats.IncrDeleteMarkers()
		}
		if localCount >= interval {
			l.progress.MaybePrint(l.stats, "scanned", localCount)
			localCount = 0
		}
	}
	l.progress.MaybePrint(l.stats, "scanned", localCount)
	return nil
}

// runMode2: BFS delimiter 并发。
func (l *Lister) runMode2(ctx context.Context, bucket, prefix string) error {
	q := newQueue()
	var inflight atomic.Int64 // 替代 WaitGroup——需要能读计数做进度 + 终止判断

	// 进度队列快照回调。
	l.progress.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{Prefix: q.Len(), Inflight: int(inflight.Load())}
	})

	// seed: 入队前先 Add，保持 Add-before-Push 终止不变量。
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

	// inflight 归零后 SignalDone，唤醒阻塞在 Pop 的 worker。
	// 用一个 watcher goroutine 轮询——inflight 在 ms 级变化，100ms 轮询够快。
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
func (l *Lister) processPrefix(ctx context.Context, bucket, prefix string, q *queue, inflight *atomic.Int64, lc *localCounter) {
	defer inflight.Add(-1)
	interval := l.progress.Interval()
	ch := l.client.ListVersions(ctx, bucket, prefix, false) // delimited
	for obj := range ch {
		if obj.Err != nil {
			l.recordListFailed(prefix, obj.Err)
			l.stats.IncrListFailed()
			return // 该前缀失败，继续扫其他前缀
		}
		l.stats.IncrScannedVersions()
		lc.count++
		if l.isCommonPrefix(obj) {
			// common prefix 入队，Add-before-Push。
			inflight.Add(1)
			q.Push(obj.Key)
			continue
		}
		if obj.IsDeleteMarker {
			l.out.WriteDeleteMarker(obj.Key, obj.VersionID)
			l.stats.IncrDeleteMarkers()
		}
		if lc.count >= interval {
			l.progress.MaybePrint(l.stats, "scanned", lc.count)
			lc.count = 0
		}
	}
}

// isCommonPrefix 判别 common prefix 条目。
// minio channel API 把 common prefix 作为 ObjectInfo{Key: ".../"} 吐出，
// 其余字段全空。四条件合取——误判前缀为版本会漏扫子目录，误判版本为
// 前缀会死循环。详见设计文档 §4.2。
func (l *Lister) isCommonPrefix(o minio.ObjectInfo) bool {
	return o.VersionID == "" && !o.IsDeleteMarker && o.ETag == "" && strings.HasSuffix(o.Key, "/")
}

// recordListFailed 从 err 提取 HTTP/S3 code + reqID，写 list_failed.txt + log。
func (l *Lister) recordListFailed(prefix string, err error) {
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		l.out.WriteListFailed(prefix)
		l.out.WriteListFailedLog(prefix, er.StatusCode, er.Code, er.RequestID, err)
		return
	}
	// 非 S3 ErrorResponse（传输层错误等）。
	l.out.WriteListFailed(prefix)
	l.out.WriteListFailedLog(prefix, 0, "", "", err)
}

// localCounter 是 worker 本地进度计数（无 per-obj atomic）。
type localCounter struct {
	count int
}
