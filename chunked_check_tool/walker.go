package main

import (
	"context"
	"sync"
)

// runRecursiveWalk enumerates the object tree under prefix using a
// semaphore-bounded recursive walk. It is the Mode 3 counterpart to the
// Lister-based worker pool used by Modes 1 and 2.
//
// Concurrency model: a buffered channel `sem` of size cfg.ListConcurrency
// caps the number of in-flight ListPage calls. Each walk goroutine holds
// one slot for the lifetime of its pagination loop; sub-prefixes discovered
// during listing are walked in new goroutines that block on sem until a
// slot frees. The cap therefore bounds concurrent LIST requests regardless
// of tree depth or fan-out, which is the property Modes 1/2's fixed worker
// count cannot give you when the tree is deep or irregular.
//
// Termination: a sync.WaitGroup tracks live walk goroutines. Each goroutine
// does Add(1) before being spawned so Wait never races ahead of a pending
// spawn. When all walks exit, Wait returns and the caller (main) closes
// objCh so check workers drain and terminate.
//
// Counting rule mirrors Lister.processPrefix: the walker bumps listed
// counters (IncrListedMp / IncrListedObject) exactly once per S3-listed
// object in both modes — in check mode the bump happens before pushing
// the VerifyTask to objCh (moved from Checker.Handle in Task 2), in
// list-only mode the bump is the only effect since no check is performed.
//
// Failure handling: a ListPage error writes the prefix to list_failed,
// bumps ListFailed, and returns — the subtree under that prefix is
// abandoned, but sibling branches continue. Matches the per-prefix failure
// semantics of Mode 2.
func runRecursiveWalk(ctx context.Context, s3 S3API, prefix string, objCh chan<- VerifyTask, out *Output, stats *Stats, cfg *Config, onObject func()) {
	sem := make(chan struct{}, cfg.ListConcurrency)
	var wg sync.WaitGroup

	var walk func(string)
	walk = func(prefix string) {
		defer wg.Done()
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-sem }()

		continuationToken := ""
		for {
			objs, prefixes, next, err := s3.ListPage(ctx, prefix, "", continuationToken, true, 1000)
			if err != nil {
				out.WriteListFailed(prefix)
				out.WriteListFailedLog(prefix, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				stats.IncrListFailed()
				return
			}
			for _, o := range objs {
				if cfg.IsCheck {
					task := resolveOffsets(o, cfg)
					if task.IsMultipart {
						stats.IncrListedMp()
					} else {
						stats.IncrListedObject()
					}
					select {
					case objCh <- task:
					case <-ctx.Done():
						return
					}
				} else {
					// list-only mode: classify via ETag directly so no
					// per-object offset slice is allocated.
					if !isNormalETag(o.ETag) {
						stats.IncrListedMp()
					} else {
						stats.IncrListedObject()
					}
				}
				if onObject != nil {
					onObject()
				}
			}
			for _, p := range prefixes {
				wg.Add(1)
				go walk(p)
			}
			if next == "" {
				return
			}
			continuationToken = next
		}
	}

	wg.Add(1)
	go walk(prefix)
	wg.Wait()
}
