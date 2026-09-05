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
// Counting rule mirrors Lister.processPrefix: in check mode the walker
// does NOT IncrListed — the checker bumps listedTotal once per consumed
// object. In list-only mode the walker is the sole counter and bumps
// IncrListed (and IncrMultipart for non-32-hex ETags) per object.
//
// Failure handling: a ListPage error writes the prefix to list_failed,
// bumps ListFailed, and returns — the subtree under that prefix is
// abandoned, but sibling branches continue. Matches the per-prefix failure
// semantics of Mode 2.
func runRecursiveWalk(ctx context.Context, s3 S3API, prefix string, objCh chan<- ObjectInfo, out *Output, stats *Stats, cfg *Config, onObject func()) {
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
					select {
					case objCh <- o:
					case <-ctx.Done():
						return
					}
				} else {
					stats.IncrListed()
					if !isNormalETag(o.ETag) {
						stats.IncrMultipart()
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
