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
//	P/l1/file_*..F                (l1 同层 F 文件)
//	P/l1/d1/file_*..F   P/l1/d2/file_*..F ... P/l1/d(W-1)/file_*..F   (W-1 leaves at layer 1)
//	P/l1/l2/file_*..F            (l2 同层 F 文件)
//	P/l1/l2/d1/...                (W-1 leaves at layer 2)
//	...
//	P/l1/l2/.../lN/file_*..F     (lN 同层 F 文件)
//	P/l1/l2/.../lN/d1..dW/file_*..F   (all W leaves at max-depth layer N)
//
// 每层 l* 桥目录下同层放 F 个文件 + (W-1 或 W) 个叶子目录 d* 各 F 文件 + 桥嵌套下一层（非最深层）。
// 总文件数 = (depth + (width-1)*(depth-1) + width) * files_per_dir。
// Idx 是 0-based 对象序号，跨整次遍历——worker 用它做 round-robin endpoint 分配。
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

	// 桥目录同层放 files_per_dir 个文件——与 d* 叶子目录及下一层桥同级
	for f := 1; f <= cfg.FilesPerDir; f++ {
		key := layerPath + "/" + cfg.FPrefix + fmt.Sprintf("%0*d", fileWidth, f)
		select {
		case <-ctx.Done():
			return
		case ch <- ObjectKey{Key: key, Idx: *idx}:
			*idx++
		}
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
