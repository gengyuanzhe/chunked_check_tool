package main

import (
	"context"
	"strings"
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
//
// Contexts (two-stage shutdown, see main.go installSignalHandler):
//   - listCtx is cancelled on the FIRST signal: workers stop popping new
//     prefixes and in-flight ListPage calls fail, which records the
//     remaining pages of the in-flight prefix into list_failed with a
//     resume cursor.
//   - hardCtx is cancelled on the SECOND signal: the objCh send aborts
//     mid-page, which records the CURRENT page's cursor (so the resume
//     re-lists that page — already-sent objects land in check_failed via
//     the cancelled probes) and the worker returns.
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

// SeedWithToken pushes a starting prefix with an initial continuation token
// (the cursor to resume listing from — i.e. the `next` returned by the last
// successful page of a prior run). Used by -resume-list: list_failed.txt
// carries (prefix, token) pairs, and re-listing from the token skips pages
// that previously succeeded. The queue carries them as a combined value
// "prefix\x00token" — NUL is safe because S3 keys/prefixes cannot contain
// NUL (S3 keys are arbitrary UTF-8 bytes but not NUL). processPrefix splits
// the queue value back into prefix+token.
func (l *Lister) SeedWithToken(prefix, token string) {
	l.inflight.Add(1)
	l.queue.Push(prefix + "\x00" + token)
}

// Run is the worker loop. It pops prefixes off the queue and lists them
// until either the context is cancelled or inflight drops to zero. The
// worker that drives inflight to zero closes the queue, which unblocks any
// list_sub workers waiting in Queue.Pop.
//
// onObject is invoked after each object is processed (sent to objCh in
// check mode, classified locally in list-only mode). It is used by main to
// drive per-worker progress printing. Pass nil to disable.
func (l *Lister) Run(listCtx, hardCtx context.Context, wg *sync.WaitGroup, objCh chan<- VerifyTask, workerIdx int, s3 S3API, onObject func()) {
	defer wg.Done()
	_ = workerIdx
	for {
		select {
		case <-listCtx.Done():
			return
		default:
		}
		prefix, ok := l.queue.Pop(listCtx)
		if !ok {
			return
		}
		// Queue values are "prefix" (plain Seed) or "prefix\x00token"
		// (SeedWithToken from -resume-list). NUL cannot appear in S3 keys, so
		// this split is unambiguous.
		initialToken := ""
		if i := strings.IndexByte(prefix, '\x00'); i >= 0 {
			initialToken = prefix[i+1:]
			prefix = prefix[:i]
		}
		l.processPrefix(listCtx, hardCtx, prefix, initialToken, objCh, s3, onObject)
		// Every prefix that is popped accounts for one Add(-1). Seed and the
		// Mode-2 sub-prefix enqueue path do the matching Add(+1).
		if l.inflight.Add(-1) == 0 {
			l.queue.Close()
			return
		}
	}
}

// recordListFailure writes one prefix failure (with the resume cursor of the
// page that did not complete) to list_failed.txt + list_failed.log.
func (l *Lister) recordListFailure(prefix, token string, err error) {
	l.out.WriteListFailed(prefix, token)
	l.out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
	l.stats.IncrListFailed()
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
//
// Failure recording: the continuationToken recorded is always the cursor of
// the page that did NOT complete — the `next` of the last successful page
// (empty when the first page failed). On a graceful interrupt the failing
// ListPage is the first page after the signal, so previously completed
// pages are not re-listed on resume; on a hard abort mid-page the same
// cursor makes the resume re-list the partially-emitted page (its unsent
// objects are otherwise lost — the already-sent ones land in check_failed
// via the cancelled probes).
func (l *Lister) processPrefix(listCtx, hardCtx context.Context, prefix, initialToken string, objCh chan<- VerifyTask, s3 S3API, onObject func()) {
	continuationToken := initialToken
	for {
		objs, prefixes, next, err := s3.ListPage(listCtx, prefix, "", continuationToken, l.delim(), 1000)
		if err != nil {
			l.recordListFailure(prefix, continuationToken, err)
			return
		}
		for _, o := range objs {
			// Reject keys containing '|' — they violate the field-separator
			// contract and would corrupt corrupted_objects/corrupted_mp output
			// lines. Such keys are not enumerable by -resume-list; route to
			// invalid_keys.txt for manual handling and skip the object.
			if strings.Contains(o.Key, "|") {
				l.out.WriteInvalidKey(o.Key)
				l.stats.IncrInvalidKeys()
				continue
			}
			if l.cfg.IsCheck {
				// Check mode: lister bumps listed counters (moved from Checker.Handle).
				// file-input sources do not go through this path, so their summary
				// shows list_all: 0 — see fileSource.
				task := resolveOffsets(o, l.cfg)
				if task.IsMultipart {
					l.stats.IncrListedMp()
				} else {
					l.stats.IncrListedObject()
				}
				select {
				case objCh <- task:
				case <-hardCtx.Done():
					l.recordListFailure(prefix, continuationToken, hardCtx.Err())
					return
				}
			} else {
				// list-only mode: classify via ETag directly so no per-object
				// []int64 offset slice is allocated (resolveOffsets would
				// allocate ceil(Size/seg) entries that are immediately discarded
				// in list-only mode — only IsMultipart is read here).
				if !isNormalETag(o.ETag) {
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
