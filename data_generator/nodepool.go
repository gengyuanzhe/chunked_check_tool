package main

import "fmt"

type NodePool struct {
	endpoints []string
	scheme    string
}

func NewNodePool(cfg *Config) *NodePool {
	return &NodePool{
		endpoints: cfg.Endpoints,
		scheme:    cfg.Scheme,
	}
}

func (p *NodePool) Assign(i int) int {
	n := len(p.endpoints)
	r := i % n
	if r < 0 {
		r += n
	}
	return r
}

func (p *NodePool) Endpoint(idx int) string {
	return p.endpoints[idx]
}

func (p *NodePool) URL(idx int) string {
	return fmt.Sprintf("%s://%s", p.scheme, p.endpoints[idx])
}
