package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// NodePool tracks the S3 endpoints and isolates nodes that accumulate enough
// node-fault-classified failures (isNodeFaultErr). Isolation is
// process-wide: any worker's fault counts toward the shared per-node
// counter, and once a node crosses the threshold no new/failed-over client
// binds to it. Fault counts never decay — a node that keeps producing
// transient faults (EOF blips, resets) eventually crosses the threshold
// too, which is the intended semantics: persistent flakiness deserves
// isolation as much as a hard outage.
type NodePool struct {
	endpoints []string
	scheme    string
	failed    map[int]struct{}

	// faultCounts[i] is the process-wide count of node-fault errors against
	// endpoints[i]. Atomics so RecordFault needs no lock on the hot path.
	faultCounts []atomic.Int64
	// isolateThreshold is the fault count at which a node is marked failed.
	isolateThreshold int64

	mu sync.RWMutex
}

func NewNodePool(cfg *Config) *NodePool {
	threshold := cfg.NodeIsolateThreshold
	if threshold <= 0 {
		// LoadConfig defaults the yaml layer to 3; this guard covers direct
		// programmatic construction (tests) with the same default.
		threshold = 3
	}
	return &NodePool{
		endpoints:        cfg.Endpoints,
		scheme:           cfg.Scheme,
		failed:           make(map[int]struct{}),
		faultCounts:      make([]atomic.Int64, len(cfg.Endpoints)),
		isolateThreshold: threshold,
	}
}

// Assign returns the index of the first alive node in the rotation starting
// at workerIdx, or -1 when every node is isolated.
func (p *NodePool) Assign(workerIdx int) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := len(p.endpoints)
	for i := 0; i < n; i++ {
		idx := (workerIdx + i) % n
		if _, fail := p.failed[idx]; !fail {
			return idx
		}
	}
	return -1
}

// RecordFault notes one node-fault-classified failure against the node and
// isolates it once the process-wide count reaches the configured threshold.
// Returns true when the node is (or just became) isolated — the signal for
// callers to rebind their client; false while the node is still considered
// merely flaky, in which case callers retry on the same node.
func (p *NodePool) RecordFault(idx int) bool {
	if p.faultCounts[idx].Add(1) >= p.isolateThreshold {
		p.MarkFailed(idx)
		return true
	}
	return false
}

func (p *NodePool) MarkFailed(idx int) {
	p.mu.Lock()
	if _, ok := p.failed[idx]; !ok {
		p.failed[idx] = struct{}{}
		log.Printf("[nodepool] node %d (%s) isolated after %d node faults (threshold %d)",
			idx, p.endpoints[idx], p.faultCounts[idx].Load(), p.isolateThreshold)
	}
	p.mu.Unlock()
}

func (p *NodePool) URL(idx int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return fmt.Sprintf("%s://%s", p.scheme, p.endpoints[idx])
}

// Endpoint returns the raw host:port of node idx. Used by worker factory
// to bind a minio client to a specific node without touching private fields.
func (p *NodePool) Endpoint(idx int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.endpoints[idx]
}

func (p *NodePool) IsFailed(idx int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, fail := p.failed[idx]
	return fail
}
