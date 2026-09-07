package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type ObjectKey struct {
	Key string
	Idx int
}

// WalkTree produces object keys for a fan-chain directory tree.
//
// Structure (depth=N, width=W, files_per_dir=F, prefix=P):
//
//	P/l1/d1/file_*..F   P/l1/d2/file_*..F ... P/l1/d(W-1)/file_*..F   (W-1 leaves at layer 1)
//	P/l1/l2/d1/...      (W-1 leaves at layer 2)
//	...
//	P/l1/l2/.../lN/d1..dW/file_*..F   (all W leaves at max-depth layer N)
//
// Total leaf dirs = (W-1)*(N-1) + W. Total objects = leaves * F.
// Idx is the 0-based object index across the whole walk — used by workers
// for round-robin endpoint assignment.
func WalkTree(ctx context.Context, cfg *Config) <-chan ObjectKey {
	ch := make(chan ObjectKey, 1024)
	go func() {
		defer close(ch)
		prefix := strings.TrimSuffix(cfg.Prefix, "/")
		fileWidth := len(strconv.Itoa(cfg.FilesPerDir))
		var idx int
		walkLayer(ctx, cfg, ch, prefix, 1, fileWidth, &idx)
	}()
	return ch
}

func walkLayer(ctx context.Context, cfg *Config, ch chan<- ObjectKey, parentPath string, layer int, fileWidth int, idx *int) {
	var layerPath string
	layerSeg := fmt.Sprintf("%s%d", cfg.LPrefix, layer)
	if parentPath == "" {
		layerPath = layerSeg
	} else {
		layerPath = parentPath + "/" + layerSeg
	}

	isMaxDepth := layer == cfg.Depth
	leafCount := cfg.Width - 1
	if isMaxDepth {
		leafCount = cfg.Width
	}

	for d := 1; d <= leafCount; d++ {
		leafPath := layerPath + "/" + fmt.Sprintf("%s%d", cfg.DPrefix, d)
		for f := 1; f <= cfg.FilesPerDir; f++ {
			key := leafPath + "/" + cfg.FPrefix + fmt.Sprintf("%0*d", fileWidth, f)
			select {
			case <-ctx.Done():
				return
			case ch <- ObjectKey{Key: key, Idx: *idx}:
				*idx++
			}
		}
	}

	if !isMaxDepth {
		walkLayer(ctx, cfg, ch, layerPath, layer+1, fileWidth, idx)
	}
}
