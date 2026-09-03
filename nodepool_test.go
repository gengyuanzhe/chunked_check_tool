package main

import (
	"testing"
)

func TestNodePoolAssignRoundRobin(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000", "c:9000"}, Scheme: "http"}
	pool := NewNodePool(cfg)
	if got := pool.Assign(0); got != 0 {
		t.Errorf("Assign(0)=%d want 0", got)
	}
	if got := pool.Assign(1); got != 1 {
		t.Errorf("Assign(1)=%d want 1", got)
	}
	if got := pool.Assign(2); got != 2 {
		t.Errorf("Assign(2)=%d want 2", got)
	}
	if got := pool.Assign(3); got != 0 {
		t.Errorf("Assign(3)=%d want 0 (wrap)", got)
	}
}

func TestNodePoolMarkFailedSkips(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000", "c:9000"}, Scheme: "http"}
	pool := NewNodePool(cfg)
	pool.MarkFailed(1)
	if got := pool.Assign(0); got != 0 {
		t.Errorf("Assign(0)=%d want 0", got)
	}
	if got := pool.Assign(1); got != 2 {
		t.Errorf("Assign(1)=%d want 2 (skip failed 1)", got)
	}
	if pool.IsFailed(1) != true {
		t.Error("node 1 should be failed")
	}
}

func TestNodePoolAllFailed(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000"}, Scheme: "http"}
	pool := NewNodePool(cfg)
	pool.MarkFailed(0)
	pool.MarkFailed(1)
	if got := pool.Assign(0); got != -1 {
		t.Errorf("Assign when all failed = %d, want -1", got)
	}
}

func TestNodePoolURL(t *testing.T) {
	cfg := &Config{Endpoints: []string{"1.2.3.4:9000"}, Scheme: "https"}
	pool := NewNodePool(cfg)
	if got := pool.URL(0); got != "https://1.2.3.4:9000" {
		t.Errorf("URL(0)=%q want https://1.2.3.4:9000", got)
	}
}

func TestNodePoolEndpoint(t *testing.T) {
	cfg := &Config{Endpoints: []string{"1.2.3.4:9000", "5.6.7.8:9000"}, Scheme: "http"}
	pool := NewNodePool(cfg)
	if got := pool.Endpoint(0); got != "1.2.3.4:9000" {
		t.Errorf("Endpoint(0)=%q want 1.2.3.4:9000", got)
	}
	if got := pool.Endpoint(1); got != "5.6.7.8:9000" {
		t.Errorf("Endpoint(1)=%q want 5.6.7.8:9000", got)
	}
}
