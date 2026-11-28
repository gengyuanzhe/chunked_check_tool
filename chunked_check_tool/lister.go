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
type Lister struct {
	queue    *Queue
	out      *Output
	stats    *Stats
	cfg      *Config
	inflight atomic.Int64
	// journal tracks scan progress for kill -9 resume (Mode 2 S3 listing
	// only; nil = off, every bookkeeping call is guarded by a nil check).
	journal *JournalTracker
	// resumed holds the prefix table replayed from the journal of the killed
	// run (-resume-journal only; nil otherwise). Every prefix it covers was
	// seeded from that journal when the round started, or skipped as done,
	// so a prefix re-listing the page its cursor points at must neither
	// record nor enqueue those sub-prefixes again: see processPrefix.
	resumed map[string]journalState
}

func NewLister(queue *Queue, out *Output, stats *Stats, cfg *Config) *Lister {
	return &Lister{queue: queue, out: out, stats: stats, cfg: cfg}
}

// SetJournal wires the progress tracker. Setter (not a constructor param)
// to keep the dozens of existing test constructions untouched.
func (l *Lister) SetJournal(j *JournalTracker) {
	l.journal = j
}

// SetResumed hands the lister the prefix table replayed from the journal of
// the killed run, for the rest of a -resume-journal round. See the resumed
// field.
func (l *Lister) SetResumed(states map[string]journalState) {
	l.resumed = states
}

// Seed pushes a starting prefix and bumps inflight so the first worker does
// not observe inflight==0 (and spuriously close the queue) before the
// prefix has been processed.
func (l *Lister) Seed(prefix string) {
	if l.journal != nil {
		l.journal.RecordSeed(prefix, "")
	}
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
	if l.journal != nil {
		l.journal.RecordSeed(prefix, token)
	}
	l.inflight.Add(1)
	l.queue.Push(prefix + "\x00" + token)
}

// Run is the worker loop. It pops prefixes off the queue and lists them
// until inflight drops to zero. The worker that drives inflight to zero
// closes the queue, which unblocks any workers waiting in Queue.Pop.
//
// onObject is invoked after each object is processed (sent to objCh in
// check mode, classified locally in list-only mode). It is used by main to
// drive per-worker progress printing. Pass nil to disable.
func (l *Lister) Run(wg *sync.WaitGroup, objCh chan<- VerifyTask, workerIdx int, s3 S3API, onObject func()) {
	defer wg.Done()
	_ = workerIdx
	for {
		prefix, ok := l.queue.Pop()
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
		l.processPrefix(prefix, initialToken, objCh, s3, onObject)
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
// (empty when the first page failed) — so a -resume-list round skips the
// pages that already succeeded.
func (l *Lister) processPrefix(prefix, initialToken string, objCh chan<- VerifyTask, s3 S3API, onObject func()) {
	continuationToken := initialToken
	for {
		objs, prefixes, next, err := s3.ListPage(context.Background(), prefix, "", continuationToken, l.delim(), int(l.cfg.MaxKeys))
		if err != nil {
			l.recordListFailure(prefix, continuationToken, err)
			return
		}
		checkShortResponse(l.out, prefix, continuationToken, len(objs)+len(prefixes), next != "", l.cfg.MaxKeys)
		// Sub-prefixes the killed run's journal already covers were seeded
		// from it, or skipped as done, when this round started (seedFromJournal).
		// This prefix is re-listing the page its cursor pointed at, and that
		// page's sub-prefixes reached the journal before the cursor could
		// advance. Taking them again would list each one twice in this round,
		// everything under it included, and would write a late discovered
		// line behind a done one in this round's journal, turning a finished
		// sub-prefix back into an unfinished one for the next resume.
		newPrefixes := prefixes
		if l.resumed != nil {
			newPrefixes = prefixes[:0]
			for _, p := range prefixes {
				if _, covered := l.resumed[p]; !covered {
					newPrefixes = append(newPrefixes, p)
				}
			}
		}
		// Register the page's FULL object count and record this page's
		// discoveries BEFORE any object of the page can reach a terminal
		// state (the first objCh send). Two load-bearing orderings:
		//   - full count: the page counts as complete only once all of its
		//     objects have reported, so the journal cursor never crosses a
		//     page whose results may not be on disk yet;
		//   - discoveries first: the parent's cursor may only cross this
		//     page once the sub-prefixes it revealed are visible to the
		//     tracker, or a checkpoint could drop a subtree.
		var pageSeq int32 = -1
		if l.journal != nil {
			pageSeq = l.journal.RegisterPage(prefix, len(objs), next)
			for _, p := range newPrefixes {
				l.journal.AddDiscovered(p)
			}
		}
		for _, o := range objs {
			if l.cfg.IsCheck {
				// Check mode: lister bumps listed counters (moved from Checker.Handle).
				// file-input sources do not go through this path, so their summary
				// shows list_all: 0 — see fileSource.
				task := resolveOffsets(o, l.cfg)
				if l.journal != nil {
					task.JournalPrefix = prefix
					task.PageSeq = pageSeq
				}
				if task.IsMultipart {
					l.stats.IncrListedMp()
				} else {
					l.stats.IncrListedObject()
				}
				objCh <- task
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
				if l.journal != nil {
					// No checker in list-only mode: the classification IS
					// the terminal event.
					l.journal.Terminal(prefix, pageSeq)
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
			for _, p := range newPrefixes {
				l.inflight.Add(1)
				l.queue.Push(p)
			}
		}
		if next == "" {
			if l.journal != nil {
				l.journal.MarkPaginationDone(prefix)
			}
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

// checkShortResponse warns when a truncated page returned fewer entries
// (objects + common prefixes) than the requested max_keys — a contradictory
// state that usually precedes a server pagination quirk. maxKeys <= 0
// (config constructed without LoadConfig, e.g. tests) disables the check:
// without knowing the requested page size, "fewer than requested" is
// undecidable. Shared by the lister, the walker, and Mode 1's root loop —
// every ListPage call site.
func checkShortResponse(out *Output, prefix, token string, entries int, truncated bool, maxKeys int32) {
	if maxKeys <= 0 || !truncated || entries >= int(maxKeys) {
		return
	}
	out.WriteShortResponseWarning(prefix, token, entries, int(maxKeys))
}
