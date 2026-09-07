package main

import (
	"context"
	"sync"
	"sync/atomic"
)

// Lister enumerates S3 objects under a set of prefixes. It supports two
// modes selected by cfg.ListType:
//
//   - Mode 1 (flat): each queued prefix is listed without a delimiter, so
//     every object key under it is returned in full (BFS collapses to a
//     single level — sub-prefixes are NOT enqueued because delim()=false
//     means the S3 ListPage returns no CommonPrefixes).
//   - Mode 2 (BFS): each queued prefix is listed WITH a delimiter, so
//     immediate object keys are returned and sub-prefixes are enqueued for
//     later processing by any worker.
//
// Termination: inflight counts prefixes that have been pushed onto the
// queue but not yet fully processed (Seed/Add in processPrefix balance the
// Add(-1) in Run). When a worker's Add(-1) returns 0, every prefix has
// been drained — that worker closes the queue so blocked Pop calls in
// list_sub workers return (ok=false) and they exit too.
//
// Controller ruling (deviates from the original brief): s3 is NOT a field.
// inflight must be shared across all workers — if each worker owned its
// own Lister (as Task 10's main.go does), per-instance counters would break
// BFS termination. Run and processPrefix therefore take s3 as a parameter.
type Lister struct {
	queue    *Queue
	out      *Output
	stats    *Stats
	cfg      *Config
	inflight atomic.Int64
}

func NewLister(queue *Queue, out *Output, stats *Stats, cfg *Config) *Lister {
	return &Lister{queue: queue, out: out, stats: stats, cfg: cfg}
}

// Seed pushes a starting prefix and bumps inflight so the first worker does
// not observe inflight==0 (and spuriously close the queue) before the
// prefix has been processed.
func (l *Lister) Seed(prefix string) {
	l.inflight.Add(1)
	l.queue.Push(prefix)
}

// Run is the worker loop. It pops prefixes off the queue and lists them
// until either the context is cancelled or inflight drops to zero. The
// worker that drives inflight to zero closes the queue, which unblocks any
// list_sub workers waiting in Queue.Pop.
//
// onObject is invoked after each object is processed (sent to objCh in
// check mode, classified locally in list-only mode). It is used by main to
// drive per-worker progress printing. Pass nil to disable.
func (l *Lister) Run(ctx context.Context, wg *sync.WaitGroup, objCh chan<- VerifyTask, workerIdx int, s3 S3API, onObject func()) {
	defer wg.Done()
	_ = workerIdx
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		prefix, ok := l.queue.Pop(ctx)
		if !ok {
			return
		}
		l.processPrefix(ctx, prefix, objCh, s3, onObject)
		// Every prefix that is popped accounts for one Add(-1). Seed and the
		// Mode-2 sub-prefix enqueue path do the matching Add(+1).
		if l.inflight.Add(-1) == 0 {
			l.queue.Close()
			return
		}
	}
}

// processPrefix lists all pages under prefix, sending objects to objCh
// (when IsCheck) or classifying them locally (when list-only). In Mode 2
// (delim=true) it also enqueues discovered sub-prefixes, bumping inflight
// for each so they are accounted for in the termination counter.
//
// Counting rule: the lister bumps listed counters (IncrListedMp /
// IncrListedObject) exactly once per S3-listed object in both modes —
// in check mode the bump happens before pushing the VerifyTask to objCh
// (moved from Checker.Handle in Task 2), in list-only mode the bump is
// the only effect since no check is performed.
func (l *Lister) processPrefix(ctx context.Context, prefix string, objCh chan<- VerifyTask, s3 S3API, onObject func()) {
	continuationToken := ""
	for {
		objs, prefixes, next, err := s3.ListPage(ctx, prefix, "", continuationToken, l.delim(), 1000)
		if err != nil {
			l.out.WriteListFailed(prefix)
			l.out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
			l.stats.IncrListFailed()
			return
		}
		for _, o := range objs {
			task := resolveOffsets(o, l.cfg)
			if l.cfg.IsCheck {
				// Check mode: lister bumps listed counters (moved from Checker.Handle).
				// list-file source does not go through this path, so its summary
				// shows list_all: 0 — see listFileSource.
				if task.IsMultipart {
					l.stats.IncrListedMp()
				} else {
					l.stats.IncrListedObject()
				}
				select {
				case objCh <- task:
				case <-ctx.Done():
					return
				}
			} else {
				// list-only mode: same classification, no check performed.
				if task.IsMultipart {
					l.stats.IncrListedMp()
				} else {
					l.stats.IncrListedObject()
				}
			}
			if onObject != nil {
				onObject()
			}
		}
		if l.cfg.ListType == 2 {
			// BFS: enqueue discovered sub-prefixes. Each Add must happen
			// before Push so a racing worker cannot observe inflight==0
			// between the Push and the accounting Add.
			for _, p := range prefixes {
				l.inflight.Add(1)
				l.queue.Push(p)
			}
		}
		if next == "" {
			return
		}
		continuationToken = next
	}
}

// delim reports whether ListPage should be called with the "/" delimiter.
// Mode 1 (flat): false — every key under the prefix is returned in full.
// Mode 2 (BFS): true — CommonPrefixes are returned and enqueued.
func (l *Lister) delim() bool {
	return l.cfg.ListType == 2
}
