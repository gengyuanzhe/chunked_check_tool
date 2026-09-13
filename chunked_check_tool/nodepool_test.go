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

func TestNodePoolRecordFaultThreshold(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000"}, Scheme: "http", NodeIsolateThreshold: 3}
	pool := NewNodePool(cfg)

	if pool.RecordFault(0) {
		t.Error("first fault must not isolate (threshold 3)")
	}
	if pool.RecordFault(0) {
		t.Error("second fault must not isolate (threshold 3)")
	}
	if pool.IsFailed(0) {
		t.Error("node 0 isolated below threshold")
	}
	if !pool.RecordFault(0) {
		t.Error("third fault must isolate")
	}
	if !pool.IsFailed(0) {
		t.Error("node 0 should be failed after 3 faults")
	}
	// Faults past the threshold keep reporting isolated so callers rebind.
	if !pool.RecordFault(0) {
		t.Error("post-threshold fault must still report isolated")
	}
	// Other nodes are unaffected.
	if pool.IsFailed(1) {
		t.Error("node 1 must not be isolated by node 0's faults")
	}
}

// TestNodePoolRecordFaultThresholdOne — threshold=1 restores the legacy
// isolate-on-first-fault behavior.
func TestNodePoolRecordFaultThresholdOne(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000"}, Scheme: "http", NodeIsolateThreshold: 1}
	pool := NewNodePool(cfg)
	if !pool.RecordFault(0) {
		t.Error("threshold 1 must isolate on first fault")
	}
	if !pool.IsFailed(0) {
		t.Error("node 0 should be failed")
	}
}

func TestNodePoolAssignOther(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000", "c:9000"}, Scheme: "http"}
	pool := NewNodePool(cfg)
	if got := pool.AssignOther(0); got != 1 {
		t.Errorf("AssignOther(0)=%d want 1", got)
	}
	if got := pool.AssignOther(1); got != 2 {
		t.Errorf("AssignOther(1)=%d want 2", got)
	}
	if got := pool.AssignOther(2); got != 0 {
		t.Errorf("AssignOther(2)=%d want 0 (wrap)", got)
	}

	pool.MarkFailed(1)
	if got := pool.AssignOther(0); got != 2 {
		t.Errorf("AssignOther(0) with 1 failed = %d want 2", got)
	}
	if got := pool.AssignOther(2); got != 0 {
		t.Errorf("AssignOther(2) with 1 failed = %d want 0", got)
	}

	// Sole alive node: degrades to the excluded node itself.
	pool.MarkFailed(0)
	if got := pool.AssignOther(2); got != 2 {
		t.Errorf("AssignOther(2) as sole survivor = %d want 2 (itself)", got)
	}

	// All failed: -1.
	pool.MarkFailed(2)
	if got := pool.AssignOther(0); got != -1 {
		t.Errorf("AssignOther with all failed = %d want -1", got)
	}
}
