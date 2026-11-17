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
//
// 所有 S3Client 在 main 启动期创建并完成 BucketExists 预检——任一失败
// 进程直接退出（fail-fast）。mode 1 持 1 个 client（Endpoints[0]）；
// mode 2 持 len(Endpoints) 个，worker 按 idx % N 取。
type Lister struct {
	cfg      *Config
	clients  []*S3Client
	out      *Output
	stats    *Stats
	progress *ProgressPrinter
}

func NewLister(cfg *Config, clients []*S3Client, out *Output, stats *Stats, progress *ProgressPrinter) *Lister {
	return &Lister{cfg: cfg, clients: clients, out: out, stats: stats, progress: progress}
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
	var localCount int
	interval := l.progress.Interval()
	keyMarker, versionIDMarker := entry.KeyMarker, entry.VersionIDMarker
	c := l.clients[0]
	for {
		page, err := c.ListVersionsPage(ctx, bucket, entry.Prefix, true, keyMarker, versionIDMarker)
		if err != nil {
			// keyMarker/versionIDMarker 就是这次失败请求用的 marker = 续传点。
			l.recordListFailed(entry.Prefix, keyMarker, versionIDMarker, err)
			l.stats.IncrListFailed()
			return err
		}
		l.checkShortResponse(entry.Prefix, keyMarker, versionIDMarker, page)
		l.checkNextVIDMarkerRewrite(entry.Prefix, keyMarker, page)
		for _, obj := range page.Versions {
			l.stats.IncrScannedVersions()
			localCount++
			if obj.IsDeleteMarker {
				if l.out.WriteDeleteMarker(obj.Key, obj.VersionID, formatLastModified(obj.LastModified)) {
					l.stats.IncrInvalidKeys()
				} else {
					l.stats.IncrDeleteMarkers()
				}
			}
			if localCount >= interval {
				l.progress.MaybePrint(l.stats, "scanned", localCount)
				localCount = 0
			}
		}
		if !page.IsTruncated {
			break
		}
		keyMarker, versionIDMarker = page.NextKeyMarker, page.NextVersionIDMarker
	}
	l.progress.MaybePrint(l.stats, "scanned", localCount)
	return nil
}

// failureThreshold 默认值——cfg.FailureThreshold=0 视为默认 3（config.go 填充）。
// 连续 N 次 ListVersionsPage 失败后，worker 进入慢速模式：每 slowModeInterval 消费 1 条 entry，
// 直到拿到一次成功后重置失败计数恢复正常消费。设计目的是绑死 endpoint 的 worker
// 不持续打流量到死节点（撒满 list_failed.txt），但仍保留活过来的可能性。
const defaultFailureThreshold = 3

// slowModeInterval 默认值——cfg.SlowModeIntervalSec=0 视为默认 60s（config.go 填充）。
// 1 分钟——足够 S3 节点重启/恢复，又不会让队列堆积太久。
const defaultSlowModeInterval = time.Minute

// failureThreshold 返回配置的连续失败阈值。
func (l *Lister) failureThreshold() int {
	if l.cfg.FailureThreshold > 0 {
		return l.cfg.FailureThreshold
	}
	return defaultFailureThreshold
}

// slowModeInterval 返回配置的慢速模式间隔。
func (l *Lister) slowModeInterval() time.Duration {
	if l.cfg.SlowModeIntervalSec > 0 {
		return time.Duration(l.cfg.SlowModeIntervalSec) * time.Second
	}
	return defaultSlowModeInterval
}

// runMode2: BFS delimiter 并发。entries 是初始入队条目列表（至少 1 个）。
// BFS 发现的新 common prefix 入队为 RetryEntry{Prefix: p}（markers 空，从第一页扫）。
// inflight 计数 = 队列里待处理 + worker 正在处理的条目数；归零 = 全扫完。
// 终止由 worker 事件触发：处理完一个条目后若发现 inflight 归零，该 worker
// 调 q.SignalDone() 唤醒其他阻塞在 Pop 的 worker（无需轮询 goroutine）。
//
// endpoint 绑定：所有 S3Client 在 main 启动期创建并完成 BucketExists 预检
// （任一失败进程退出），worker 按 idx % len(clients) 取一个直接用——runMode2
// 不再创建 client。运行中 endpoint 故障由 worker 慢速模式兜底（见下）。
//
// 慢速模式：worker 连续失败 failureThreshold 次后，每 slowModeInterval 消费 1 条 entry，
// 直到拿到一次成功重置失败计数恢复正常消费。设计目的：绑死 endpoint 的 worker 不持续
// 打流量到死节点（撒满 list_failed.txt），但仍保留活过来的可能性。
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
		go func(idx int) {
			defer wg.Done()
			// worker 按 idx % N 绑一个预创建好的 client——endpoint 已在启动期预检。
			wc := l.clients[idx%len(l.clients)]
			lc := &localCounter{}
			var consecFails int
			threshold := l.failureThreshold()
			slowInterval := l.slowModeInterval()
			for {
				// 慢速模式：连续失败达阈值后，每条 entry 前等待 slowInterval。
				// 用 select + ctx.Done() 让进程退出时能立即响应，不等满 slowInterval。
				if consecFails >= threshold {
					select {
					case <-time.After(slowInterval):
					case <-ctx.Done():
						return
					}
				}
				e, ok := q.Pop(ctx)
				if !ok {
					return
				}
				failed := l.processPrefixWithClient(ctx, wc, bucket, e, q, &inflight, lc)
				if failed {
					consecFails++
				} else {
					consecFails = 0
				}
				// inflight == 0 表示无待处理也无在处理 = 全扫完。
				// 该 worker 发完成信号唤醒其他阻塞 Pop 的 worker 退出。
				// 安全性：inflight 统计入队+在处理，归零时不可能有并发 push
				// （push 来自在处理条目，归零时无在处理），故 SignalDone 不会
				// 误杀未处理条目。
				if inflight.Load() == 0 {
					q.SignalDone()
					return
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// processPrefixWithClient 处理一个 entry：逐页列举，分类条目。
// 用传入的 wc 替代 l.client（mode 2 worker 各自绑 endpoint 时调此版本）。
// 返回 failed=true 表示该 entry 任一页 ListVersionsPage 失败（worker 据此累加
// 连续失败计数判断是否进慢速模式）。
//
// inflight.Add(-1) 在 defer 里——无论失败/成功都把"在处理"计数减掉。
// 第一页和后续页用同一个 processPage helper，避免逻辑重复。
func (l *Lister) processPrefixWithClient(ctx context.Context, wc *S3Client, bucket string, entry RetryEntry, q *queue, inflight *atomic.Int64, lc *localCounter) (failed bool) {
	defer inflight.Add(-1)
	interval := l.progress.Interval()
	keyMarker, versionIDMarker := entry.KeyMarker, entry.VersionIDMarker
	for {
		page, err := wc.ListVersionsPage(ctx, bucket, entry.Prefix, false, keyMarker, versionIDMarker)
		if err != nil {
			// keyMarker/versionIDMarker 就是这次失败请求用的 marker = 续传点。
			l.recordListFailed(entry.Prefix, keyMarker, versionIDMarker, err)
			l.stats.IncrListFailed()
			return true
		}
		l.checkShortResponse(entry.Prefix, keyMarker, versionIDMarker, page)
		l.checkNextVIDMarkerRewrite(entry.Prefix, keyMarker, page)
		for _, p := range page.CommonPrefixes {
			inflight.Add(1)
			// 新发现的 common prefix 从第一页扫（markers 空）。
			q.Push(RetryEntry{Prefix: p})
		}
		for _, obj := range page.Versions {
			l.stats.IncrScannedVersions()
			lc.count++
			if obj.IsDeleteMarker {
				if l.out.WriteDeleteMarker(obj.Key, obj.VersionID, formatLastModified(obj.LastModified)) {
					l.stats.IncrInvalidKeys()
				} else {
					l.stats.IncrDeleteMarkers()
				}
			}
			if lc.count >= interval {
				l.progress.MaybePrint(l.stats, "scanned", lc.count)
				lc.count = 0
			}
		}
		if !page.IsTruncated {
			return false
		}
		keyMarker, versionIDMarker = page.NextKeyMarker, page.NextVersionIDMarker
	}
}

// checkNextVIDMarkerRewrite 在 s3client 短响应触发时修复了 NextVersionIdMarker
// （分支 b 改写为 "null"，或分支 c 用本页尾条目推导覆盖）命中时打 Warn 日志到
// list_failed.log。与 short-response 警告一致，非失败不写 list_failed.txt——
// 服务端 quirk 信号便于线上排查。
func (l *Lister) checkNextVIDMarkerRewrite(prefix, keyMarker string, page *VersionPage) {
	if page.NextVIDMarkerRewritten {
		l.out.WriteNextVIDMarkerZeroRewriteLog(prefix, keyMarker, page.RewrittenRawVIDMarker)
	}
}

// checkShortResponse 检测"截断了却没返回满"的矛盾响应并写警告到 list_failed.log。
// 条件：IsTruncated=true 且本页条目数（Version+DeleteMarker+CommonPrefix）< maxKeys。
// 服务端在正常翻页下 IsTruncated=true 的页一定返回满 maxKeys；不满预示服务端
// 翻页 quirk（如返回异常 next-marker）。非失败，不写 list_failed.txt。
func (l *Lister) checkShortResponse(prefix, keyMarker, versionIDMarker string, page *VersionPage) {
	if !page.IsTruncated {
		return
	}
	count := int32(len(page.Versions) + len(page.CommonPrefixes))
	if count < l.cfg.MaxKeys {
		l.out.WriteShortResponseWarning(prefix, keyMarker, versionIDMarker, count, l.cfg.MaxKeys)
	}
}

// recordListFailed 从 err 提取 HTTP/S3 code + reqID，写 list_failed.txt + log。
// keyMarker/versionIDMarker 是失败时已扫完的最后一页的 next-marker；
// 同 prefix 重扫时从该 marker 起续扫（页级续传）。
func (l *Lister) recordListFailed(prefix, keyMarker, versionIDMarker string, err error) {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		l.out.WriteListFailed(prefix, keyMarker, versionIDMarker)
		l.out.WriteListFailedLog(prefix, keyMarker, versionIDMarker, re.Response.StatusCode, "", re.Response.Header.Get("x-amz-request-id"), err)
		return
	}
	l.out.WriteListFailed(prefix, keyMarker, versionIDMarker)
	l.out.WriteListFailedLog(prefix, keyMarker, versionIDMarker, 0, "", "", err)
}

// localCounter 是 worker 本地进度计数（无 per-obj atomic）。
type localCounter struct {
	count int
}
