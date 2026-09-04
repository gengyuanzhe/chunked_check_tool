package main

import (
	"fmt"
	"sync"
)

type NodePool struct {
	endpoints []string
	scheme    string
	failed    map[int]struct{}
	mu        sync.RWMutex
}

func NewNodePool(cfg *Config) *NodePool {
	return &NodePool{
		endpoints: cfg.Endpoints,
		scheme:    cfg.Scheme,
		failed:    make(map[int]struct{}),
	}
}

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

func (p *NodePool) MarkFailed(idx int) {
	p.mu.Lock()
	if _, ok := p.failed[idx]; !ok {
		p.failed[idx] = struct{}{}
		fmt.Printf("[nodepool] node %d (%s) marked failed\n", idx, p.endpoints[idx])
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
