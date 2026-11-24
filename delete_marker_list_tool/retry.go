package main

import (
	"bufio"
	"fmt"
	"net/url"
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
// 文件格式契约：三字段（prefix/keyMarker/versionIdMarker）均为 URL 编码态
// （list_failed.txt 由 Output 层 QueryEscape 落盘），这里逐字段 url.QueryUnescape
// 恢复原始形态——发请求用的就是原始形态。行结构：
//   - 无 \x1f：纯 prefix（向后兼容旧 list_failed.txt），markers 空 = 从第一页扫
//   - 2 个 \x1f：prefix\x1fkeyMarker\x1fversionIdMarker（标准格式）
//   - 1 个 \x1f：格式非法（marker 必须成对），报错
//   - 其他 \x1f 数量：格式非法，报错
//   - 任一字段解码失败（非法 %XX）：格式非法，报错（工具生成的文件不应出现）
//
// 空行 = root prefix + 从第一页扫（对齐旧 list_failed.txt 语义）。
// 去重保序——按 (prefix, keyMarker, versionIDMarker) 三元组去重。
// 空文件（无行）返回单元素 [{Prefix:""}]（root prefix），保证 retry 空文件仍能扫 root。
//
// 兼容性：改动前 encoding_type_url=false 写出的旧文件是原始态，不含 '+'/%XX 的
// 值解码恒等无损；含字面 '+' 或合法 %XX 的旧值会被误解码——旧批次文件不要混入，
// 整批重跑。
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
// 见 readRetryEntries 的格式约定——三字段是 URL 编码态，此处解码恢复原始形态。
func parseRetryLine(line string) (RetryEntry, error) {
	parts := strings.Split(line, "\x1f")
	switch len(parts) {
	case 1:
		// 无 \x1f：纯 prefix，markers 空
		prefix, err := url.QueryUnescape(parts[0])
		if err != nil {
			return RetryEntry{}, fmt.Errorf("invalid retry line: prefix decode failed: %w", err)
		}
		return RetryEntry{Prefix: prefix}, nil
	case 3:
		// 标准格式：prefix\x1fkeyMarker\x1fversionIdMarker（均为编码态）
		prefix, err := url.QueryUnescape(parts[0])
		if err != nil {
			return RetryEntry{}, fmt.Errorf("invalid retry line: prefix decode failed: %w", err)
		}
		km, err := url.QueryUnescape(parts[1])
		if err != nil {
			return RetryEntry{}, fmt.Errorf("invalid retry line: keyMarker decode failed: %w", err)
		}
		vidm, err := url.QueryUnescape(parts[2])
		if err != nil {
			return RetryEntry{}, fmt.Errorf("invalid retry line: versionIdMarker decode failed: %w", err)
		}
		return RetryEntry{
			Prefix:          prefix,
			KeyMarker:       km,
			VersionIDMarker: vidm,
		}, nil
	default:
		return RetryEntry{}, fmt.Errorf("invalid retry line: expected 0 or 2 \\x1f separators, got %d in %q", len(parts)-1, line)
	}
}
