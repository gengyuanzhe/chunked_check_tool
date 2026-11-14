package main

import (
	"bufio"
	"fmt"
	"os"
)

// readRetryPrefixes 读 retry 文件，每行一个 prefix，去重保序后返回。
// 空行=root prefix（""），保留不跳——list_failed.txt 里 root prefix 失败
// 记成空行。返回 slice 至少含 1 个元素：空文件（无行）返回 [""]（root prefix），
// 保证 retry 空文件仍能扫 root。
func readRetryPrefixes(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read retry file %s: %w", path, err)
	}
	defer f.Close()

	var out []string
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		p := scanner.Text()
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan retry file %s: %w", path, err)
	}
	if len(out) == 0 {
		out = []string{""}
	}
	return out, nil
}
