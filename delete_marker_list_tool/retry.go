package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// RetryEntry 是 retry 文件里的一条重扫条目。
// Prefix 是 BFS 入队前缀；KeyMarker/VersionIDMarker 非空 = 从该 marker
// 起续扫（页级续传），都空 = 从第一页扫。
type RetryEntry struct {
	Prefix          string
	KeyMarker       string
	VersionIDMarker string
}

// readRetryEntries 读 retry 文件，每行一条 RetryEntry。
// 行格式：
//   - 无 \x1f：纯 prefix（向后兼容旧 list_failed.txt），markers 空 = 从第一页扫
//   - 2 个 \x1f：prefix\x1fkeyMarker\x1fversionIdMarker（标准格式）
//   - 1 个 \x1f：格式非法（marker 必须成对），报错
//   - 其他 \x1f 数量：格式非法，报错
//
// 空行 = root prefix + 从第一页扫（对齐旧 list_failed.txt 语义）。
// 去重保序——按 (prefix, keyMarker, versionIDMarker) 三元组去重。
// 空文件（无行）返回单元素 [{Prefix:""}]（root prefix），保证 retry 空文件仍能扫 root。
func readRetryEntries(path string) ([]RetryEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read retry file %s: %w", path, err)
	}
	defer f.Close()

	var out []RetryEntry
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		entry, err := parseRetryLine(line)
		if err != nil {
			return nil, fmt.Errorf("read retry file %s: %w", path, err)
		}
		key := entry.Prefix + "\x1f" + entry.KeyMarker + "\x1f" + entry.VersionIDMarker
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan retry file %s: %w", path, err)
	}
	if len(out) == 0 {
		out = []RetryEntry{{Prefix: ""}}
	}
	return out, nil
}

// parseRetryLine 解析一行 retry 条目。空行 = root prefix + 从第一页扫。
// 见 readRetryEntries 的格式约定。
func parseRetryLine(line string) (RetryEntry, error) {
	parts := strings.Split(line, "\x1f")
	switch len(parts) {
	case 1:
		// 无 \x1f：纯 prefix，markers 空
		return RetryEntry{Prefix: parts[0]}, nil
	case 3:
		// 标准格式：prefix\x1fkeyMarker\x1fversionIdMarker
		return RetryEntry{
			Prefix:          parts[0],
			KeyMarker:       parts[1],
			VersionIDMarker: parts[2],
		}, nil
	default:
		return RetryEntry{}, fmt.Errorf("invalid retry line: expected 0 or 2 \\x1f separators, got %d in %q", len(parts)-1, line)
	}
}
