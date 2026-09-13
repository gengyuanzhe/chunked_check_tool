package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProbeNodeHealthy(t *testing.T) {
	var headCalls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == "location=":
			w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
		case r.Method == http.MethodHead:
			atomic.AddInt64(&headCalls, 1)
			w.WriteHeader(200)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	cfg := &Config{Scheme: "http", AK: "ak", SK: "sk"}
	if !probeNode(cfg, "bkt", strings.TrimPrefix(srv.URL, "http://")) {
		t.Error("healthy node should probe true")
	}
	if atomic.LoadInt64(&headCalls) == 0 {
		t.Error("probe should issue a HEAD bucket request")
	}
}

func TestProbeNodeDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // port now closed → connection refused
	cfg := &Config{Scheme: "http", AK: "ak", SK: "sk"}
	if probeNode(cfg, "bkt", strings.TrimPrefix(srv.URL, "http://")) {
		t.Error("dead node should probe false")
	}
}

// TestProbeNode5xx — a node still answering 5xx was isolated for exactly
// that; recovery must require the faults to have stopped.
func TestProbeNode5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "location=" {
			w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
			return
		}
		w.WriteHeader(500)
	}))
	defer srv.Close()
	cfg := &Config{Scheme: "http", AK: "ak", SK: "sk"}
	if probeNode(cfg, "bkt", strings.TrimPrefix(srv.URL, "http://")) {
		t.Error("5xx-answering node should probe false")
	}
}

// TestRecoveryRound — a node is readmitted only after the required number
// of CONSECUTIVE successful probes; a failed probe resets the streak.
func TestRecoveryRound(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000", "b:9000"}, Scheme: "http", NodeIsolateThreshold: 1}
	pool := NewNodePool(cfg)
	pool.RecordFault(0) // isolate node 0
	pool.RecordFault(1) // isolate node 1
	if got := len(pool.FailedNodes()); got != 2 {
		t.Fatalf("failed nodes = %d, want 2", got)
	}

	answers := map[int]bool{0: true, 1: false}
	probe := func(idx int) bool { return answers[idx] }
	successes := make(map[int]int)

	// Round 1: node 0 succeeds once (below the consecutive threshold),
	// node 1 fails. Both stay isolated.
	recoveryRound(pool, probe, successes)
	if !pool.IsFailed(0) || !pool.IsFailed(1) {
		t.Fatal("one success must not readmit yet")
	}

	// Round 2: node 0 hits the consecutive threshold → readmitted.
	recoveryRound(pool, probe, successes)
	if pool.IsFailed(0) {
		t.Error("node 0 should be readmitted after 2 consecutive successes")
	}
	if !pool.IsFailed(1) {
		t.Error("node 1 must stay isolated while probes fail")
	}

	// Node 1 recovers for one round, then fails again: streak resets —
	// the readmission needs the FULL consecutive run again.
	answers[1] = true
	recoveryRound(pool, probe, successes)
	if !pool.IsFailed(1) {
		t.Fatal("node 1 must stay isolated after only one success")
	}
	answers[1] = false
	recoveryRound(pool, probe, successes)
	answers[1] = true
	recoveryRound(pool, probe, successes)
	if !pool.IsFailed(1) {
		t.Error("broken streak must not count: node 1 needs 2 fresh consecutive successes")
	}
	recoveryRound(pool, probe, successes)
	if pool.IsFailed(1) {
		t.Error("node 1 should be readmitted after a fresh 2-success streak")
	}
}

// TestUnmarkResetsFaultCount — a recovered node starts from zero faults;
// pre-recovery counts must not make the re-isolation threshold effectively 1.
func TestUnmarkResetsFaultCount(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000"}, Scheme: "http", NodeIsolateThreshold: 3}
	pool := NewNodePool(cfg)
	for i := 0; i < 3; i++ {
		pool.RecordFault(0)
	}
	if !pool.IsFailed(0) {
		t.Fatal("node 0 should be isolated at threshold")
	}
	pool.Unmark(0)
	if pool.IsFailed(0) {
		t.Fatal("node 0 should be readmitted")
	}
	// The old count must be gone: one new fault is below threshold again.
	if pool.RecordFault(0) {
		t.Error("post-recovery fault must start from a zero count")
	}
	if pool.IsFailed(0) {
		t.Error("one post-recovery fault must not re-isolate")
	}
}

// TestStartNodeRecoveryDisabled — interval 0 keeps the legacy behavior: the
// prober never starts, isolated nodes stay out for the process lifetime.
func TestStartNodeRecoveryDisabled(t *testing.T) {
	cfg := &Config{Endpoints: []string{"a:9000"}, Scheme: "http", NodeIsolateThreshold: 1, NodeRecoverProbeInterval: 0}
	pool := NewNodePool(cfg)
	pool.RecordFault(0)
	startNodeRecovery(context.Background(), pool, cfg, "bkt")
	if !pool.IsFailed(0) {
		t.Error("disabled recovery must leave the node isolated")
	}
}
