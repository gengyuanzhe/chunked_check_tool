// noderecovery.go — background recovery of isolated nodes.
//
// Isolation is permanent for the process lifetime by default (AGENTS §4.10)
// — too harsh for hour-long runs where an ops restart of one node's S3
// process would cost its capacity until the run ends. When
// node_recover_probe_interval > 0 a background prober re-admits an isolated
// node after nodeRecoverSuccesses consecutive healthy probes, resetting its
// fault count so the node must accumulate a fresh threshold of faults to be
// isolated again.
package main

import (
	"context"
	"time"
)

// nodeRecoverSuccesses is how many consecutive healthy probe rounds an
// isolated node needs before it is readmitted. Two rounds at the default
// 60s interval means a node must look healthy for ~2 minutes straight.
const nodeRecoverSuccesses = 2

// probeNode reports whether the S3 process on endpoint is answering. The
// probe is a HEAD bucket through a throwaway client (one fresh connection
// per round — isolated nodes are rare, so this is a connection per minute
// at most). The health criterion mirrors how nodes get isolated: any
// coherent S3 answer (2xx, 404, 403, ...) counts as healthy, while
// transport faults and 5xx (!isNodeFaultErr) do not — recovery requires the
// faults that caused the isolation to have stopped.
func probeNode(cfg *Config, bucket, endpoint string) bool {
	client, err := NewMinioClient(endpoint, cfg.AK, cfg.SK, cfg.Scheme == "https", false,
		time.Duration(cfg.DialTimeout)*time.Second, time.Duration(cfg.ResponseHeaderTimeout)*time.Second)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.BucketExists(ctx, bucket)
	return !isNodeFaultErr(err)
}

// recoveryRound probes every isolated node once. A node failing its probe
// has its consecutive-success streak reset; a node reaching
// nodeRecoverSuccesses is readmitted (Unmark resets its fault count).
func recoveryRound(pool *NodePool, probe func(idx int) bool, successes map[int]int) {
	for _, idx := range pool.FailedNodes() {
		if !probe(idx) {
			delete(successes, idx)
			continue
		}
		successes[idx]++
		if successes[idx] >= nodeRecoverSuccesses {
			pool.Unmark(idx)
			delete(successes, idx)
		}
	}
}

// startNodeRecovery launches the background prober goroutine. Interval <= 0
// (node_recover_probe_interval: 0) keeps the legacy behavior: no prober,
// isolation is permanent for the process lifetime. The goroutine exits on
// ctx cancellation (SIGINT/SIGTERM or run end).
func startNodeRecovery(ctx context.Context, pool *NodePool, cfg *Config, bucket string) {
	if cfg.NodeRecoverProbeInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.NodeRecoverProbeInterval) * time.Second)
		defer ticker.Stop()
		successes := make(map[int]int)
		probe := func(idx int) bool {
			return probeNode(cfg, bucket, pool.Endpoint(idx))
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recoveryRound(pool, probe, successes)
			}
		}
	}()
}
