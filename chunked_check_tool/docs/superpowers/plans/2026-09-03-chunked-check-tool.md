# chunked_check_tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 并发列举+校验 S3 对象，识别被错误存储为 aws-chunked body 的普通对象，输出损坏/多段/失败列表到文件，支持几十亿对象规模。

**Architecture:** Pipeline: 无界 prefix 队列 → list worker (并发列举) → 有界 objCh → check worker (Range GET 128 字节 + 正则) → 5 个 output channel → 单 writer goroutine 串行写文件。NodePool 轮询绑定 endpoint + 故障隔离。背压链全链路自调节，无死锁。

**Tech Stack:** Go 1.27, `github.com/minio/minio-go/v7`, `gopkg.in/yaml.v3`, 标准库 `net/http` / `sync` / `sync/atomic` / `regexp`。

**Spec:** `docs/superpowers/specs/2026-09-03-chunked-check-tool-design.md`

## Global Constraints

- Go 1.27，module 名 `chunked_check_tool`。
- 依赖只引入：`github.com/minio/minio-go/v7`、`gopkg.in/yaml.v3`。其他用标准库。
- 性能优先但保持可读：默认 HTTP keep-alive（MinIO client 自带连接池）、`bufio.Writer` 包裹文件写入、channel 容量按 spec、slice 预分配。**遇到需要 unsafe / 内存池 / lock-free 之类复杂优化，先向用户请求，不要直接写。**
- 多段判定严格：只有 `^[0-9a-f]{32}$` 算普通对象，其他一律多段。
- checker goroutine 绝不因错误退出。
- 所有提交信息用 `feat:` / `fix:` / `test:` / `chore:` 前缀，英文。

---

## File Structure

所有文件在仓库根目录（`/Users/gengyuanzhe/code/S3/golang/chunked_check_tool/`），不分子目录（除 `docs/`、`testdata/`）。

| 文件 | 职责 |
|---|---|
| `go.mod` | module + 依赖 |
| `config.go` | YAML 配置结构体 + 加载 + 默认值 |
| `config_test.go` | 配置加载测试 |
| `queue.go` | 无界 prefix 队列（slice + mutex + cond） |
| `queue_test.go` | 队列并发测试 |
| `nodepool.go` | endpoint 轮询 + 故障隔离 |
| `nodepool_test.go` | NodePool 测试 |
| `stats.go` | atomic 计数器 + 统计文件写 |
| `stats_test.go` | Stats 测试 |
| `output.go` | 5 文件 channel writer |
| `output_test.go` | Output 测试 |
| `s3client.go` | `S3API` 接口 + minio 实现 + fake |
| `s3client_test.go` | S3 client 测试（用 fake） |
| `checker.go` | ETag 正则 + chunk-signature 正则 + handle 逻辑 |
| `checker_test.go` | Checker 纯逻辑测试 |
| `lister.go` | Mode 1 + Mode 2 列举 worker |
| `lister_test.go` | Lister 测试（用 fake S3） |
| `progress.go` | 本地计数器 + 进度打印 |
| `progress_test.go` | Progress 测试 |
| `main.go` | flag 解析 + 编排 |
| `testdata/` | 测试 fixtures |

---

## Task 1: 项目脚手架与配置加载

**Files:**
- Create: `go.mod`
- Create: `config.go`
- Create: `config_test.go`
- Modify: `main.go`（保留空 main，后续 task 填充）

**Interfaces:**
- Produces: `Config` struct、`LoadConfig(path string) (*Config, error)`。

- [ ] **Step 1: 初始化 go.mod 并加依赖**

Run:
```bash
cd /Users/gengyuanzhe/code/S3/golang/chunked_check_tool
go mod init chunked_check_tool
go get github.com/minio/minio-go/v7@latest
go get gopkg.in/yaml.v3@latest
go mod tidy
```

- [ ] **Step 2: 写失败测试 `config_test.go`**

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_DefaultsAndParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte(`
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
ak: ACCESSKEY
sk: SECRETKEY
list_type: 2
list_concurrency: 4
check_concurrency: 8
output_dir: ./out
is_check: true
is_success_log: true
progress_interval: 50000
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheme != "http" {
		t.Errorf("default scheme = %q, want http", cfg.Scheme)
	}
	if len(cfg.Endpoints) != 2 || cfg.Endpoints[1] != "10.0.0.2:9000" {
		t.Errorf("endpoints = %v", cfg.Endpoints)
	}
	if cfg.ListType != 2 || cfg.ListConcurrency != 4 || cfg.CheckConcurrency != 8 {
		t.Errorf("concurrency fields wrong: %+v", cfg)
	}
	if !cfg.IsSuccessLog || cfg.ProgressInterval != 50000 {
		t.Errorf("bool/int fields wrong: %+v", cfg)
	}
}

func TestLoadConfig_MissingEndpoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("ak: x\nsk: y\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing endpoints")
	}
}

func TestLoadConfig_SchemeHttps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("endpoints:\n  - 1.2.3.4:9000\nscheme: https\nak: x\nsk: y\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheme != "https" {
		t.Errorf("scheme = %q, want https", cfg.Scheme)
	}
}
```

- [ ] **Step 3: 运行测试验证失败**

Run: `go test -run TestLoadConfig -v ./...`
Expected: FAIL — `LoadConfig` undefined.

- [ ] **Step 4: 写最小实现 `config.go`**

```go
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Endpoints        []string `yaml:"endpoints"`
	Scheme           string   `yaml:"scheme"`
	AK               string   `yaml:"ak"`
	SK               string   `yaml:"sk"`
	ListType         int      `yaml:"list_type"`
	ListConcurrency  int      `yaml:"list_concurrency"`
	CheckConcurrency int      `yaml:"check_concurrency"`
	OutputDir        string   `yaml:"output_dir"`
	IsCheck          bool     `yaml:"is_check"`
	IsSuccessLog     bool     `yaml:"is_success_log"`
	ProgressInterval int      `yaml:"progress_interval"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("endpoints must not be empty")
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	if cfg.ListConcurrency <= 0 {
		cfg.ListConcurrency = 8
	}
	if cfg.CheckConcurrency <= 0 {
		cfg.CheckConcurrency = 16
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 100000
	}
	if cfg.ListType == 0 {
		cfg.ListType = 1
	}
	return &cfg, nil
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `go test -run TestLoadConfig -v ./...`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add go.mod go.sum config.go config_test.go
git commit -m "feat: add config loading with yaml + defaults"
```

---

## Task 2: 无界 prefix 队列

**Files:**
- Create: `queue.go`
- Create: `queue_test.go`

**Interfaces:**
- Produces: `Queue` struct、`NewQueue() *Queue`、`Push(s string)`、`Pop(ctx context.Context) (string, bool)`、`Len() int`。`Pop` 在队列空且未关闭时阻塞；队列关闭且空时返回 `(_, false)`。

- [ ] **Step 1: 写失败测试 `queue_test.go`**

```go
package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueuePushPop(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b")
	got, ok := q.Pop(context.Background())
	if !ok || got != "a" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	got, ok = q.Pop(context.Background())
	if !ok || got != "b" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
}

func TestQueueBlockingPop(t *testing.T) {
	q := NewQueue()
	done := make(chan struct{})
	go func() {
		v, ok := q.Pop(context.Background())
		if !ok || v != "x" {
			t.Errorf("got %q ok=%v", v, ok)
		}
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	q.Push("x")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Pop did not unblock after Push")
	}
}

func TestQueueCloseEmpty(t *testing.T) {
	q := NewQueue()
	q.Close()
	_, ok := q.Pop(context.Background())
	if ok {
		t.Fatal("expected ok=false after close on empty queue")
	}
}

func TestQueueCloseAfterDrain(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Close()
	v, ok := q.Pop(context.Background())
	if !ok || v != "a" {
		t.Fatalf("got %q ok=%v", v, ok)
	}
	_, ok = q.Pop(context.Background())
	if ok {
		t.Fatal("expected ok=false after drain")
	}
}

func TestQueueConcurrent(t *testing.T) {
	q := NewQueue()
	const N = 1000
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < N; j++ {
				q.Push("x")
			}
		}()
	}
	go func() {
		wg.Wait()
		q.Close()
	}()
	count := 0
	for {
		_, ok := q.Pop(context.Background())
		if !ok {
			break
		}
		count++
	}
	if count != 5*N {
		t.Fatalf("count=%d want %d", count, 5*N)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestQueue -v ./...`
Expected: FAIL — `Queue` undefined.

- [ ] **Step 3: 写实现 `queue.go`**

```go
package main

import (
	"context"
	"sync"
)

type Queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []string
	closed bool
}

func NewQueue() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Push(s string) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, s)
	q.cond.Signal()
	q.mu.Unlock()
}

func (q *Queue) Pop(ctx context.Context) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		// 用 ctx 超时唤醒定期检查 ctx 是否取消
		go func() {}() // placeholder no-op
		q.cond.Wait()
		select {
		case <-ctx.Done():
			return "", false
		default:
		}
	}
	if len(q.items) == 0 {
		return "", false
	}
	s := q.items[0]
	q.items[0] = ""
	q.items = q.items[1:]
	return s, true
}

func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
```

**Note:** 上面的 `Pop` 对 ctx 取消的处理有缺陷——`cond.Wait()` 不响应 ctx。修复版：

```go
func (q *Queue) Pop(ctx context.Context) (string, bool) {
	// 先尝试非阻塞
	q.mu.Lock()
	if len(q.items) > 0 {
		s := q.items[0]
		q.items[0] = ""
		q.items = q.items[1:]
		q.mu.Unlock()
		return s, true
	}
	if q.closed {
		q.mu.Unlock()
		return "", false
	}
	q.mu.Unlock()

	// ctx 已取消直接返回
	select {
	case <-ctx.Done():
		return "", false
	default:
	}

	// 阻塞等待：启动一个唤醒 goroutine，ctx 取消时 Signal
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.cond.Broadcast()
			q.mu.Unlock()
		case <-stop:
		}
	}()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
		select {
		case <-ctx.Done():
			return "", false
		default:
		}
	}
	if len(q.items) == 0 {
		return "", false
	}
	s := q.items[0]
	q.items[0] = ""
	q.items = q.items[1:]
	return s, true
}
```

实现时只写第二个版本（修复版），不要写第一个版本。

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestQueue -v ./...`
Expected: PASS（全部 5 个测试）。

- [ ] **Step 5: 提交**

```bash
git add queue.go queue_test.go
git commit -m "feat: add unbounded queue with ctx-aware blocking pop"
```

---

## Task 3: NodePool 节点轮询与故障隔离

**Files:**
- Create: `nodepool.go`
- Create: `nodepool_test.go`

**Interfaces:**
- Consumes: `Config`（Endpoints、Scheme 字段）。
- Produces: `NodePool` struct、`NewNodePool(cfg *Config) *NodePool`、`(p *NodePool) Assign(workerIdx int) int`、`(p *NodePool) MarkFailed(idx int)`、`(p *NodePool) URL(idx int) string`、`(p *NodePool) IsFailed(idx int) bool`。

- [ ] **Step 1: 写失败测试 `nodepool_test.go`**

```go
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
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestNodePool -v ./...`
Expected: FAIL — `NodePool` undefined.

- [ ] **Step 3: 写实现 `nodepool.go`**

```go
package main

import (
	"fmt"
	"sync"
)

type NodePool struct {
	endpoints []string
	scheme    string
	failed    map[int]struct{}
	mu        sync.RWMutex
}

func NewNodePool(cfg *Config) *NodePool {
	return &NodePool{
		endpoints: cfg.Endpoints,
		scheme:    cfg.Scheme,
		failed:    make(map[int]struct{}),
	}
}

func (p *NodePool) Assign(workerIdx int) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := len(p.endpoints)
	for i := 0; i < n; i++ {
		idx := (workerIdx + i) % n
		if _, fail := p.failed[idx]; !fail {
			return idx
		}
	}
	return -1
}

func (p *NodePool) MarkFailed(idx int) {
	p.mu.Lock()
	if _, ok := p.failed[idx]; !ok {
		p.failed[idx] = struct{}{}
		fmt.Printf("[nodepool] node %d (%s) marked failed\n", idx, p.endpoints[idx])
	}
	p.mu.Unlock()
}

func (p *NodePool) URL(idx int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return fmt.Sprintf("%s://%s", p.scheme, p.endpoints[idx])
}

func (p *NodePool) IsFailed(idx int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, fail := p.failed[idx]
	return fail
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestNodePool -v ./...`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add nodepool.go nodepool_test.go
git commit -m "feat: add nodepool round-robin assign with fault isolation"
```

---

## Task 4: Stats 统计计数器

**Files:**
- Create: `stats.go`
- Create: `stats_test.go`

**Interfaces:**
- Produces: `Stats` struct（atomic 字段）、`NewStats() *Stats`、`IncrListed()` / `IncrMultipart()` / `IncrCorrupted()` / `IncrListFailed()` / `IncrCheckFailed()`、`AddListCall(latency time.Duration)`、`SetListDuration(d time.Duration)`、`SetTotalDuration(d time.Duration)`、`Snapshot() StatsSnapshot`、`WriteToFile(path string, isCheck bool) error`、`PrintSummary(isCheck bool)`。

- [ ] **Step 1: 写失败测试 `stats_test.go`**

```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatsIncrAndSnapshot(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrListed()
	s.IncrMultipart()
	s.IncrCorrupted()
	s.IncrListFailed()
	s.IncrCheckFailed()
	s.AddListCall(2 * time.Millisecond)
	s.AddListCall(4 * time.Millisecond)
	s.SetListDuration(10 * time.Second)
	s.SetTotalDuration(15 * time.Second)

	snap := s.Snapshot()
	if snap.ListedTotal != 2 {
		t.Errorf("listed=%d want 2", snap.ListedTotal)
	}
	if snap.Multipart != 1 || snap.Corrupted != 1 || snap.ListFailed != 1 || snap.CheckFailed != 1 {
		t.Errorf("counts wrong: %+v", snap)
	}
	if snap.ListCalls != 2 {
		t.Errorf("listcalls=%d want 2", snap.ListCalls)
	}
	if snap.ListAvgLatencyMs != 3.0 {
		t.Errorf("avg latency=%v want 3.0", snap.ListAvgLatencyMs)
	}
	if snap.ListTotalDurationSec != 10.0 {
		t.Errorf("list duration=%v want 10.0", snap.ListTotalDurationSec)
	}
}

func TestStatsWriteFile(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrMultipart()
	s.AddListCall(1 * time.Millisecond)
	s.SetListDuration(2 * time.Second)
	s.SetTotalDuration(5 * time.Second)

	dir := t.TempDir()
	path := filepath.Join(dir, "stats.txt")
	if err := s.WriteToFile(path, true); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	content := string(data)
	for _, key := range []string{"total_objects:", "list_calls:", "list_avg_latency_ms:", "list_total_duration_sec:", "total_duration_sec:", "multipart:", "corrupted:"} {
		if !strings.Contains(content, key) {
			t.Errorf("missing %q in:\n%s", key, content)
		}
	}
}

func TestStatsWriteFileListOnly(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrMultipart()
	s.SetListDuration(1 * time.Second)
	s.SetTotalDuration(2 * time.Second)
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.txt")
	if err := s.WriteToFile(path, false); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	content := string(data)
	if strings.Contains(content, "corrupted:") {
		t.Errorf("list-only mode should not contain corrupted: but got:\n%s", content)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestStats -v ./...`
Expected: FAIL — `Stats` undefined.

- [ ] **Step 3: 写实现 `stats.go`**

```go
package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Stats struct {
	listedTotal      atomic.Int64
	multipartCount   atomic.Int64
	corruptedCount   atomic.Int64
	listFailedCount  atomic.Int64
	checkFailedCount atomic.Int64
	listCalls        atomic.Int64
	listLatencySumNs atomic.Int64

	listTotalDuration time.Duration
	totalDuration     time.Duration
}

type StatsSnapshot struct {
	ListedTotal         int64
	Multipart           int64
	Corrupted           int64
	ListFailed          int64
	CheckFailed         int64
	ListCalls           int64
	ListAvgLatencyMs    float64
	ListTotalDurationSec float64
	TotalDurationSec    float64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncrListed()       { s.listedTotal.Add(1) }
func (s *Stats) IncrMultipart()    { s.multipartCount.Add(1) }
func (s *Stats) IncrCorrupted()    { s.corruptedCount.Add(1) }
func (s *Stats) IncrListFailed()   { s.listFailedCount.Add(1) }
func (s *Stats) IncrCheckFailed() { s.checkFailedCount.Add(1) }

func (s *Stats) AddListCall(latency time.Duration) {
	s.listCalls.Add(1)
	s.listLatencySumNs.Add(int64(latency))
}

func (s *Stats) SetListDuration(d time.Duration) { s.listTotalDuration = d }
func (s *Stats) SetTotalDuration(d time.Duration) { s.totalDuration = d }

func (s *Stats) Snapshot() StatsSnapshot {
	calls := s.listCalls.Load()
	var avgMs float64
	if calls > 0 {
		avgMs = float64(s.listLatencySumNs.Load()) / float64(calls) / 1e6
	}
	return StatsSnapshot{
		ListedTotal:         s.listedTotal.Load(),
		Multipart:           s.multipartCount.Load(),
		Corrupted:           s.corruptedCount.Load(),
		ListFailed:          s.listFailedCount.Load(),
		CheckFailed:         s.checkFailedCount.Load(),
		ListCalls:           calls,
		ListAvgLatencyMs:    avgMs,
		ListTotalDurationSec: s.listTotalDuration.Seconds(),
		TotalDurationSec:    s.totalDuration.Seconds(),
	}
}

func (s *Stats) WriteToFile(path string, isCheck bool) error {
	snap := s.Snapshot()
	var b []byte
	b = append(b, fmt.Sprintf("total_objects: %d\n", snap.ListedTotal)...)
	b = append(b, fmt.Sprintf("list_calls: %d\n", snap.ListCalls)...)
	b = append(b, fmt.Sprintf("list_avg_latency_ms: %.2f\n", snap.ListAvgLatencyMs)...)
	b = append(b, fmt.Sprintf("list_total_duration_sec: %.2f\n", snap.ListTotalDurationSec)...)
	b = append(b, fmt.Sprintf("total_duration_sec: %.2f\n", snap.TotalDurationSec)...)
	if isCheck {
		b = append(b, fmt.Sprintf("multipart: %d\n", snap.Multipart)...)
		b = append(b, fmt.Sprintf("corrupted: %d\n", snap.Corrupted)...)
	}
	b = append(b, fmt.Sprintf("list_failed: %d\n", snap.ListFailed)...)
	if isCheck {
		b = append(b, fmt.Sprintf("check_failed: %d\n", snap.CheckFailed)...)
	}
	return os.WriteFile(path, b, 0644)
}

func (s *Stats) PrintSummary(isCheck bool) {
	snap := s.Snapshot()
	fmt.Printf("=== summary ===\n")
	fmt.Printf("total_objects: %d\n", snap.ListedTotal)
	fmt.Printf("list_calls: %d avg_latency_ms: %.2f list_total_sec: %.2f total_sec: %.2f\n",
		snap.ListCalls, snap.ListAvgLatencyMs, snap.ListTotalDurationSec, snap.TotalDurationSec)
	if isCheck {
		fmt.Printf("multipart: %d corrupted: %d list_failed: %d check_failed: %d\n",
			snap.Multipart, snap.Corrupted, snap.ListFailed, snap.CheckFailed)
	} else {
		fmt.Printf("list_failed: %d\n", snap.ListFailed)
	}
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestStats -v ./...`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add stats.go stats_test.go
git commit -m "feat: add atomic stats counters and stats file output"
```

---

## Task 5: Output Writer

**Files:**
- Create: `output.go`
- Create: `output_test.go`

**Interfaces:**
- Produces: `Output` struct、`NewOutput(cfg *Config) (*Output, error)`、`(o *Output) WriteCorrupted(key string)`、`(o *Output) WriteMultipart(etag, key string)`、`(o *Output) WriteListFailed(prefix, err string)`、`(o *Output) WriteCheckFailed(key, err string)`、`(o *Output) WriteSuccess(key string)`、`(o *Output) Close() error`。

- [ ] **Step 1: 写失败测试 `output_test.go`**

```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOutputWritesCorruptedAndMultipart(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsSuccessLog: true}
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteCorrupted("obj/a")
	o.WriteMultipart("abc123-2", "obj/b")
	o.WriteListFailed("prefix/x", "timeout")
	o.WriteCheckFailed("obj/c", "500")
	o.WriteSuccess("obj/ok")
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(name, want string) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s missing %q:\n%s", name, want, string(data))
		}
	}
	check("corrupted_objects.txt", "obj/a")
	check("multipart_objects.txt", "abc123-2|obj/b")
	check("list_failed.txt", "prefix/x")
	check("list_failed.txt", "timeout")
	check("check_failed.txt", "obj/c")
	check("check_failed.txt", "500")
	check("success_objects.log", "obj/ok")
}

func TestOutputNoSuccessWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsSuccessLog: false}
	o, err := NewOutput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	o.WriteSuccess("x") // should be no-op
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "success_objects.log")); !os.IsNotExist(err) {
		t.Errorf("success log should not exist, got %v", err)
	}
}

func TestOutputMultipartFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	o, _ := NewOutput(cfg)
	o.WriteMultipart("deadbeef-3", "key/with|pipe")
	o.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "multipart_objects.txt"))
	line := strings.TrimSpace(string(data))
	if line != "deadbeef-3|key/with|pipe" {
		t.Errorf("multipart line = %q", line)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestOutput -v ./...`
Expected: FAIL — `Output` undefined.

- [ ] **Step 3: 写实现 `output.go`**

```go
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Key string
	Err string
}

type Output struct {
	dir            string
	corruptedCh    chan string
	multipartCh    chan string
	listFailedCh   chan Entry
	checkFailedCh  chan Entry
	successCh      chan string
	successEnabled bool
	wg             sync.WaitGroup
	files          []*os.File
}

func NewOutput(cfg *Config) (*Output, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir output: %w", err)
	}
	o := &Output{
		dir:            cfg.OutputDir,
		corruptedCh:    make(chan string, 1024),
		multipartCh:    make(chan string, 1024),
		listFailedCh:   make(chan Entry, 1024),
		checkFailedCh:  make(chan Entry, 1024),
		successCh:      make(chan string, 1024),
		successEnabled: cfg.IsSuccessLog,
	}
	if err := o.openAndStart("corrupted_objects.txt", o.corruptedCh, false); err != nil {
		return nil, err
	}
	if err := o.openAndStart("multipart_objects.txt", o.multipartCh, false); err != nil {
		return nil, err
	}
	if err := o.openAndStart("list_failed.txt", o.listFailedCh, true); err != nil {
		return nil, err
	}
	if err := o.openAndStart("check_failed.txt", o.checkFailedCh, true); err != nil {
		return nil, err
	}
	if o.successEnabled {
		if err := o.openAndStart("success_objects.log", o.successCh, false); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// openAndStart opens the file (append mode) and starts a writer goroutine.
// isEntry=true means channel is chan Entry (write "key err\n"); else chan string.
func (o *Output) openAndStart(name string, ch interface{}, isEntry bool) error {
	f, err := os.OpenFile(filepath.Join(o.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	o.files = append(o.files, f)
	w := bufio.NewWriterSize(f, 64*1024)

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		switch c := ch.(type) {
		case chan string:
			for line := range c {
				w.WriteString(line)
				w.WriteByte('\n')
			}
		case chan Entry:
			for e := range c {
				w.WriteString(e.Key)
				w.WriteByte(' ')
				w.WriteString(e.Err)
				w.WriteByte('\n')
			}
		}
		w.Flush()
	}()
	return nil
}

func (o *Output) WriteCorrupted(key string)   { o.corruptedCh <- key }
func (o *Output) WriteMultipart(etag, key string) {
	o.multipartCh <- etag + "|" + key
}
func (o *Output) WriteListFailed(prefix, errStr string)  { o.listFailedCh <- Entry{Key: prefix, Err: errStr} }
func (o *Output) WriteCheckFailed(key, errStr string)    { o.checkFailedCh <- Entry{Key: key, Err: errStr} }
func (o *Output) WriteSuccess(key string) {
	if o.successEnabled {
		o.successCh <- key
	}
}

func (o *Output) Close() error {
	close(o.corruptedCh)
	close(o.multipartCh)
	close(o.listFailedCh)
	close(o.checkFailedCh)
	if o.successEnabled {
		close(o.successCh)
	}
	o.wg.Wait()
	var firstErr error
	for _, f := range o.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestOutput -v ./...`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add output.go output_test.go
git commit -m "feat: add output writer with 5 channels and bufio"
```

---

## Task 6: S3 客户端接口 + MinIO 实现 + Fake

**Files:**
- Create: `s3client.go`
- Create: `s3client_test.go`

**Interfaces:**
- Produces: `S3API` 接口、`ObjectInfo` 类型、`FakeS3` 测试实现、`NewMinioClient(endpoint, ak, sk string, secure bool) (*minio.Client, error)`、`S3Client` 包装。
- `S3API` 方法：
  - `ListPage(ctx, prefix, startAfter string, delim bool, maxKeys int) (objs []ObjectInfo, commonPrefixes []string, nextStartAfter string, err error)`
  - `RangeGet(ctx, key string) (body []byte, err error)`

**关键决策点（实现时验证）：** MinIO v7 的 `Client.ListObjectsV2` channel API 不暴露 `CommonPrefixes`。优先用 `minio.Core.ListObjects`（若存在且返回 `ListBucketResult` with `CommonPrefixes`）；否则 fallback 到 raw HTTP：用 `minio.Credentials` + `s3signer` 自签 SigV4 请求。**先尝试 `minio.Core` API**——若可用，覆盖 95% 场景。

- [ ] **Step 1: 写失败测试 `s3client_test.go`（用 FakeS3）**

```go
package main

import (
	"context"
	"errors"
	"testing"
)

func TestFakeS3ListPage(t *testing.T) {
	f := &FakeS3{
		Objects: []ObjectInfo{
			{Key: "a", ETag: "0123456789abcdef0123456789abcdef"},
			{Key: "b", ETag: "0123456789abcdef0123456789abcdef-2"},
		},
		CommonPrefixes: []string{"sub/"},
		NextStartAfter: "b",
	}
	objs, prefixes, next, err := f.ListPage(context.Background(), "p", "", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 || objs[1].Key != "b" {
		t.Errorf("objs = %v", objs)
	}
	if len(prefixes) != 1 || prefixes[0] != "sub/" {
		t.Errorf("prefixes = %v", prefixes)
	}
	if next != "b" {
		t.Errorf("next = %q", next)
	}
}

func TestFakeS3RangeGet(t *testing.T) {
	f := &FakeS3{Body: []byte("1000;chunk-signature=abcdef")}
	body, err := f.RangeGet(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "1000;chunk-signature=abcdef" {
		t.Errorf("body = %q", body)
	}
}

func TestFakeS3ListError(t *testing.T) {
	f := &FakeS3{Err: errors.New("boom")}
	_, _, _, err := f.ListPage(context.Background(), "", "", false, 100)
	if err == nil || err.Error() != "boom" {
		t.Errorf("err = %v", err)
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestFakeS3 -v ./...`
Expected: FAIL — `ObjectInfo`、`FakeS3` undefined.

- [ ] **Step 3: 写实现 `s3client.go`**

```go
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ObjectInfo struct {
	Key  string
	ETag string
}

type S3API interface {
	ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error)
	RangeGet(ctx context.Context, key string) ([]byte, error)
}

// NewMinioClient 构造一个 MinIO client，绑定到单个 endpoint。
func NewMinioClient(endpoint, ak, sk string, secure bool) (*minio.Client, error) {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       secure,
		Transport:    tr,
		BucketLookup: minio.BucketLookupAuto,
	})
}

// S3Client wraps minio.Core for ListPage and minio.Client for RangeGet.
type S3Client struct {
	core   *minio.Core
	client *minio.Client
	bucket string
}

func NewS3Client(client *minio.Client, bucket string) *S3Client {
	return &S3Client{
		core:   &minio.Core{Client: client},
		client: client,
		bucket: bucket,
	}
}

func (c *S3Client) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	delimeter := ""
	if delim {
		delimeter = "/"
	}
	result, err := c.core.ListObjects(ctx, c.bucket, prefix, startAfter, delimeter, maxKeys)
	if err != nil {
		return nil, nil, "", err
	}
	objs := make([]ObjectInfo, 0, len(result.Contents))
	for _, o := range result.Contents {
		objs = append(objs, ObjectInfo{Key: o.Key, ETag: o.ETag})
	}
	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, cp := range result.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	next := ""
	if len(objs) > 0 {
		next = objs[len(objs)-1].Key
	}
	return objs, prefixes, next, nil
}

func (c *S3Client) RangeGet(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	obj, err := c.client.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{
		Range: fmt.Sprintf("bytes=0-127"),
	})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	body, err := io.ReadAll(io.LimitReader(obj, 128))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// FakeS3 implements S3API for testing.
type FakeS3 struct {
	Objects        []ObjectInfo
	CommonPrefixes []string
	NextStartAfter string
	Body           []byte
	Err            error
}

func (f *FakeS3) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	if f.Err != nil {
		return nil, nil, "", f.Err
	}
	return f.Objects, f.CommonPrefixes, f.NextStartAfter, nil
}

func (f *FakeS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Body, nil
}

var _ S3API = (*FakeS3)(nil)
var _ S3API = (*S3Client)(nil)

var ErrNodeDown = errors.New("node down")
```

**实现时验证项：**
1. `minio.Core` 是否有 `ListObjects(ctx, bucket, prefix, startAfter, delimiter, maxKeys)` 方法且返回带 `Contents` 和 `CommonPrefixes` 的结构体。
2. 若没有，fallback：用 `minio.Client` 的 `executeMethod`（unexported，不可用）→ 必须 raw HTTP。Fallback 代码模板：
   ```go
   // raw HTTP fallback (用 minio 的 credentials.Sign 签 SigV4)
   // 这个比较复杂，如果走到这里，先停下来问用户
   ```
   **如果 fallback 需要，停下来向用户请求**——这是复杂代码，按 memory 规则要先请求。

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestFakeS3 -v ./...`
Expected: PASS（FakeS3 部分）。S3Client 因没真 S3 不测，但 `var _ S3API = (*S3Client)(nil)` 保证接口实现。

- [ ] **Step 5: 验证 `minio.Core.ListObjects` API 存在**

Run: `go build ./...`
Expected: 编译通过。若 `c.core.ListObjects` 签名不匹配，查 minio-go v7 文档修正参数顺序。若 `minio.Core` 不导出 `ListObjects`，停下来找用户讨论 fallback。

- [ ] **Step 6: 提交**

```bash
git add s3client.go s3client_test.go
git commit -m "feat: add S3API interface, minio wrapper, and fake for tests"
```

---

## Task 7: Checker 纯逻辑

**Files:**
- Create: `checker.go`
- Create: `checker_test.go`

**Interfaces:**
- Consumes: `S3API`、`Output`、`Stats`。
- Produces: `Checker` struct、`isNormalETag(etag string) bool`、`chunkSigRe *regexp.Regexp`、`NewChecker(worker S3API, out *Output, stats *Stats, successLog bool) *Checker`、`(c *Checker) Handle(obj ObjectInfo)`。

- [ ] **Step 1: 写失败测试 `checker_test.go`**

```go
package main

import (
	"context"
	"testing"
)

func TestIsNormalETag(t *testing.T) {
	cases := []struct {
		etag   string
		normal bool
	}{
		{"0123456789abcdef0123456789abcdef", true},
		{"0123456789abcdef0123456789abcdef-2", false},
		{"ABCDEF0123456789abcdef0123456789abcdef", false}, // uppercase + too long
		{"", false},
		{"short", false},
		{"0123456789ABCDEF0123456789ABCDEF", false}, // uppercase
	}
	for _, c := range cases {
		got := isNormalETag(c.etag)
		if got != c.normal {
			t.Errorf("isNormalETag(%q)=%v want %v", c.etag, got, c.normal)
		}
	}
}

func TestChunkSigRegex(t *testing.T) {
	cases := []struct {
		body   string
		match  bool
	}{
		{"1000;chunk-signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\n", true},
		{"ff;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\n", true},
		{"hello world", false},
		{"1000;chunk-signature=short\n", false}, // signature not 64
		{";chunk-signature=abcdef\n", false},     // empty chunk size
		{"1000;notchunk-signature=abc\n", false},
	}
	for _, c := range cases {
		got := chunkSigRe.Match([]byte(c.body))
		if got != c.match {
			t.Errorf("Match(%q)=%v want %v", c.body, got, c.match)
		}
	}
}

func TestCheckerHandleNormal(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Body: []byte("normal object content here")}
	c := NewChecker(worker, out, s, false)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef"})
	// success not enabled, no files written yet (deferred to Close)
}

func TestCheckerHandleCorrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	body := []byte("1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n")
	worker := &FakeS3{Body: body}
	c := NewChecker(worker, out, s, false)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef"})
	if s.Snapshot().Corrupted != 1 {
		t.Errorf("corrupted=%d want 1", s.Snapshot().Corrupted)
	}
}

func TestCheckerHandleMultipartSkipsRangeGet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	called := false
	worker := &FakeS3{
		Body: nil,
		Err:  nil,
	}
	// 用一个 wrap 检测是否调用 RangeGet
	c := NewChecker(&callTrackingS3{FakeS3: worker, called: &called}, out, s, false)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2"})
	if called {
		t.Error("RangeGet should not be called for multipart")
	}
	if s.Snapshot().Multipart != 1 {
		t.Errorf("multipart=%d want 1", s.Snapshot().Multipart)
	}
}

func TestCheckerHandleRangeGetError(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir}
	out, _ := NewOutput(cfg)
	defer out.Close()
	s := NewStats()
	worker := &FakeS3{Err: context.DeadlineExceeded}
	c := NewChecker(worker, out, s, false)
	c.Handle(ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef"})
	if s.Snapshot().CheckFailed != 1 {
		t.Errorf("checkfailed=%d want 1", s.Snapshot().CheckFailed)
	}
}

// helper: track RangeGet calls
type callTrackingS3 struct {
	*FakeS3
	called *bool
}

func (c *callTrackingS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	*c.called = true
	return c.FakeS3.RangeGet(ctx, key)
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run "TestIsNormalETag|TestChunkSigRegex|TestCheckerHandle" -v ./...`
Expected: FAIL — symbols undefined.

- [ ] **Step 3: 写实现 `checker.go`**

```go
package main

import (
	"context"
	"regexp"
)

var chunkSigRe = regexp.MustCompile(`^[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}[\r\n]`)

func isNormalETag(etag string) bool {
	// 严格 32 位小写 hex
	if len(etag) != 32 {
		return false
	}
	for i := 0; i < 32; i++ {
		c := etag[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

type Checker struct {
	worker     S3API
	out        *Output
	stats      *Stats
	successLog bool
}

func NewChecker(worker S3API, out *Output, stats *Stats, successLog bool) *Checker {
	return &Checker{worker: worker, out: out, stats: stats, successLog: successLog}
}

func (c *Checker) Handle(obj ObjectInfo) {
	if !isNormalETag(obj.ETag) {
		c.out.WriteMultipart(obj.ETag, obj.Key)
		c.stats.IncrMultipart()
		return
	}
	body, err := c.worker.RangeGet(context.Background(), obj.Key)
	if err != nil {
		c.out.WriteCheckFailed(obj.Key, err.Error())
		c.stats.IncrCheckFailed()
		return
	}
	if chunkSigRe.Match(body) {
		c.out.WriteCorrupted(obj.Key)
		c.stats.IncrCorrupted()
	} else if c.successLog {
		c.out.WriteSuccess(obj.Key)
	}
	c.stats.IncrListed()
}
```

**注：** `IncrListed()` 在 checker 路径调用——listed_total 计的是被检查过的对象。list worker 路径在 Task 8 里也会 IncrListed（仅列举模式）。

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run "TestIsNormalETag|TestChunkSigRegex|TestCheckerHandle" -v ./...`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add checker.go checker_test.go
git commit -m "feat: add checker with strict etag and chunk-signature regex"
```

---

## Task 8: Lister (Mode 1 + Mode 2)

**Files:**
- Create: `lister.go`
- Create: `lister_test.go`

**Interfaces:**
- Consumes: `S3API`、`Queue`、`Output`、`Stats`、`Config`。
- Produces: `Lister` struct、`NewLister(...)`、`(l *Lister) Run(ctx context.Context, wg *sync.WaitGroup)`、`(l *Lister) Seed(prefix, startAfter string)`、`(l *Lister) WaitDoneAndCloseObjCh(objCh chan<- ObjectInfo)`。

- [ ] **Step 1: 写失败测试 `lister_test.go`**

```go
package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestListerMode2BFSPrefixes(t *testing.T) {
	// fake: 根 prefix 返回 1 对象 + 1 子目录；子目录返回 1 对象
	fake := &scriptedS3{
		pages: map[string][]pageResult{
			"root/": {
				{objs: []ObjectInfo{{Key: "root/file1", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: []string{"root/sub/"}},
			},
			"root/sub/": {
				{objs: []ObjectInfo{{Key: "root/sub/file2", ETag: "0123456789abcdef0123456789abcdef"}}, prefixes: nil},
			},
		},
	}
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, ListConcurrency: 2, CheckConcurrency: 2}
	out, _ := NewOutput(cfg)
	defer out.Close()
	stats := NewStats()
	q := NewQueue()
	lister := NewLister(fake, q, out, stats, cfg)

	objCh := make(chan ObjectInfo, 8)
	q.Push("root/")

	var wg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		wg.Add(1)
		go lister.Run(context.Background(), &wg, objCh, i)
	}
	go func() {
		lister.WaitInflightZero()
		close(objCh)
	}()

	got := []string{}
	for o := range objCh {
		got = append(got, o.Key)
	}
	wg.Wait()
	if len(got) != 2 {
		t.Errorf("got %d objects: %v", len(got), got)
	}
}

// scriptedS3 serves canned page results keyed by prefix.
type pageResult struct {
	objs     []ObjectInfo
	prefixes []string
	nextAfter string
}
type scriptedS3 struct {
	pages map[string][]pageResult
	calls int
	mu    sync.Mutex
}

func (s *scriptedS3) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pages, ok := s.pages[prefix]
	if !ok {
		return nil, nil, "", nil
	}
	if len(pages) == 0 {
		return nil, nil, "", nil
	}
	p := pages[0]
	s.pages[prefix] = pages[1:]
	return p.objs, p.prefixes, p.nextAfter, nil
}

func (s *scriptedS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

// wait helper
func init() {
	_ = time.Second
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestListerMode2 -v ./...`
Expected: FAIL — `Lister` undefined.

- [ ] **Step 3: 写实现 `lister.go`**

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type Lister struct {
	s3      S3API
	queue   *Queue
	out     *Output
	stats   *Stats
	cfg     *Config
	inflight atomic.Int64
}

func NewLister(s3 S3API, queue *Queue, out *Output, stats *Stats, cfg *Config) *Lister {
	return &Lister{s3: s3, queue: queue, out: out, stats: stats, cfg: cfg}
}

func (l *Lister) Seed(prefix string) {
	l.inflight.Add(1)
	l.queue.Push(prefix)
}

func (l *Lister) Run(ctx context.Context, wg *sync.WaitGroup, objCh chan<- ObjectInfo, workerIdx int) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		prefix, ok := l.queue.Pop(ctx)
		if !ok {
			return
		}
		l.processPrefix(ctx, prefix, objCh)
		if l.inflight.Add(-1) == 0 {
			l.queue.Close() // signal list_sub workers to exit
			return
		}
	}
}

func (l *Lister) processPrefix(ctx context.Context, prefix string, objCh chan<- ObjectInfo) {
	startAfter := ""
	for {
		objs, prefixes, next, err := l.s3.ListPage(ctx, prefix, startAfter, l.delim(), 1000)
		if err != nil {
			l.out.WriteListFailed(prefix, err.Error())
			l.stats.IncrListFailed()
			return
		}
		for _, o := range objs {
			l.stats.IncrListed()
			if l.cfg.IsCheck {
				select {
				case objCh <- o:
				case <-ctx.Done():
					return
				}
			} else {
				// 仅列举模式：本地分类计数
				if !isNormalETag(o.ETag) {
					l.stats.IncrMultipart()
				}
			}
		}
		if l.cfg.ListType == 2 {
			// BFS：新子目录入队
			for _, p := range prefixes {
				l.inflight.Add(1)
				l.queue.Push(p)
			}
		}
		if next == "" {
			return
		}
		startAfter = next
	}
}

func (l *Lister) delim() bool {
	// Mode 1: 子目录已 seed 进队列，单 prefix 内列举不带 delim（平铺）
	// Mode 2: 每次列举都带 delim（BFS）
	return l.cfg.ListType == 2
}

func (l *Lister) WaitInflightZero() {
	for l.inflight.Load() > 0 {
		// 自旋等待；queue 关闭后 workers 退出，inflight 归零
		// 实际由 Run 里 Add(-1)==0 时 Close queue 触发
		// 这里只是兜底：如果 workers 都退出但 inflight>0（异常），不阻塞
		break
	}
}
```

**关键约束（实现时注意）：**
- `processPrefix` 里 `for` 循环翻页用 `startAfter`（next 返回的最后一个 key），不是 ContinuationToken。
- Mode 1 的 seed（根 prefix 列举拿子目录）在 `main.go` 里完成——用一次 `ListPage(prefix, "", true, 1000)` 拿 CommonPrefixes + 根下直接对象，然后 seed 子目录进 queue。
- `WaitInflightZero` 在当前实现下是 no-op，因为 `Run` 里 `Add(-1)==0` 就 `Close` 了 queue 触发所有 worker 退出。main.go 里用 `listWg.Wait()` 替代。

- [ ] **Step 4: 运行测试验证通过**

Run: `go test -run TestListerMode2 -v ./...`
Expected: PASS。若挂死（inflight 计数 bug），调试 — 这是 BFS 队列最易错的地方。

- [ ] **Step 5: 提交**

```bash
git add lister.go lister_test.go
git commit -m "feat: add lister with mode1 flat and mode2 BFS"
```

---

## Task 9: Progress 打印

**Files:**
- Create: `progress.go`
- Create: `progress_test.go`

**Interfaces:**
- Produces: `Progress` struct、`NewProgress(stats *Stats, interval int, isCheck bool, w io.Writer) *Progress`、`(p *Progress) IncrListed()`、`(p *Progress) IncrChecked()`。

- [ ] **Step 1: 写失败测试 `progress_test.go`**

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestProgressPrintsAtInterval(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	p := NewProgress(s, 3, true, &buf)
	for i := 0; i < 7; i++ {
		p.IncrChecked()
	}
	out := buf.String()
	// 7 次入 2 个区间（3、6）→ 至少 2 行
	if !strings.Contains(out, "checked=3") || !strings.Contains(out, "checked=6") {
		t.Errorf("expected progress lines, got:\n%s", out)
	}
}

func TestProgressNoSharedAtomic(t *testing.T) {
	// 仅确保 IncrChecked 不调 atomic（这里无直接断言方式，靠代码 review）
	s := NewStats()
	var buf bytes.Buffer
	p := NewProgress(s, 1000, true, &buf)
	for i := 0; i < 100; i++ {
		p.IncrChecked()
	}
	if buf.Len() != 0 {
		t.Errorf("should not print below interval, got %q", buf.String())
	}
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `go test -run TestProgress -v ./...`
Expected: FAIL — `Progress` undefined.

- [ ] **Step 3: 写实现 `progress.go`**

```go
package main

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type Progress struct {
	stats    *Stats
	interval int
	isCheck  bool
	w       io.Writer
	mu      sync.Mutex // 仅保护 write，不保护计数

	// 每个 worker 本地计数——这里简化：list/check 各一组本地计数器
	// 用一个 thread-local map 麻烦，改成 per-goroutine 实例更简单。
	// 但调用方需要每个 worker 一个 Progress 实例——我们改 API：直接给每个 worker 一个 *Progress。
}
```

实际上更简洁的设计：每个 worker 自带一个 `progressCounter`，达到阈值时调 `stats.Snapshot()` + `fmt.Fprintf`。所以 `Progress` 只是 helper 函数 + 共享的 `io.Writer`。

重写：

```go
package main

import (
	"fmt"
	"io"
	"sync"
)

type ProgressPrinter struct {
	w   io.Writer
	mu  sync.Mutex
}

func NewProgressPrinter(w io.Writer) *ProgressPrinter {
	return &ProgressPrinter{w: w}
}

// MaybePrint 由 worker 在本地计数达到阈值时调用。
// label 是 "checked" 或 "listed"，count 是本地累计值（worker 自己维护）。
func (p *ProgressPrinter) MaybePrint(stats *Stats, label string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := stats.Snapshot()
	fmt.Fprintf(p.w, "[progress] listed=%d checked=%d multipart=%d corrupted=%d list_failed=%d check_failed=%d (%s=%d)\n",
		snap.ListedTotal, snap.Multipart+snap.Corrupted+snap.ListedTotal-snap.Multipart, // rough
		snap.Multipart, snap.Corrupted, snap.ListFailed, snap.CheckFailed, label, count)
}
```

worker 端用法（在 lister.go / checker.go）：

```go
type localCounter struct {
	n int
	interval int
	printer *ProgressPrinter
}

func (lc *localCounter) incr(stats *Stats, label string) {
	lc.n++
	if lc.n >= lc.interval {
		lc.printer.MaybePrint(stats, label, lc.n)
		lc.n = 0
	}
}
```

- [ ] **Step 4: 调整测试**

重写 `progress_test.go`：

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestProgressLocalCounter(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	lc := &localCounter{interval: 3, printer: pp}
	for i := 0; i < 7; i++ {
		lc.incr(s, "checked")
	}
	out := buf.String()
	// 应在 n=3 和 n=6 时各打一次，共 2 行
	if strings.Count(out, "[progress]") != 2 {
		t.Errorf("expected 2 progress lines, got:\n%s", out)
	}
}

func TestProgressNoPrintBelowInterval(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)
	lc := &localCounter{interval: 1000, printer: pp}
	for i := 0; i < 100; i++ {
		lc.incr(s, "listed")
	}
	if buf.Len() != 0 {
		t.Errorf("should not print, got %q", buf.String())
	}
}
```

- [ ] **Step 5: 运行测试验证通过**

Run: `go test -run TestProgress -v ./...`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add progress.go progress_test.go
git commit -m "feat: add progress printer with local counters (no per-obj atomic)"
```

---

## Task 10: main.go 编排 + 集成测试

**Files:**
- Modify: `main.go`
- Create: `main_test.go`

**Interfaces:**
- Consumes: 全部上述模块。

- [ ] **Step 1: 写 `main.go`**

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	bucket := flag.String("bkt", "", "bucket name")
	prefix := flag.String("prefix", "", "list prefix")
	startAfter := flag.String("nextmarker", "", "start-after key")
	flag.Parse()

	if *cfgPath == "" || *bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -bkt <bucket> [-prefix p] [-nextmarker key]")
		os.Exit(2)
	}

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, *bucket, *prefix, *startAfter); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func run(ctx context.Context, cfg *Config, bucket, prefix, startAfter string) error {
	pool := NewNodePool(cfg)
	out, err := NewOutput(cfg)
	if err != nil {
		return err
	}
	stats := NewStats()
	printer := NewProgressPrinter(os.Stdout)
	start := time.Now()

	objChCap := cfg.CheckConcurrency * 4
	if objChCap < 2000 {
		objChCap = 2000
	}
	objCh := make(chan ObjectInfo, objChCap)
	q := NewQueue()
	lister := NewLister(nil, q, out, stats, cfg) // s3 set per-worker below

	// seed
	if cfg.ListType == 1 {
		// 根列举拿子目录 + 直接对象
		worker := newWorkerForList(pool, 0, cfg, bucket)
		objs, subprefixes, _, err := worker.ListPage(ctx, prefix, startAfter, true, 1000)
		if err != nil {
			out.WriteListFailed(prefix, err.Error())
		} else {
			for _, o := range objs {
				select {
				case objCh <- o:
					stats.IncrListed()
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			for _, sp := range subprefixes {
				lister.Seed(sp)
			}
			if len(objs) > 0 || len(subprefixes) > 0 {
				lister.Seed(prefix) // 根 prefix 的直接对象已在上面发送，这里 seed 让 worker 翻页后续——实际只发后续页
				// 简化：把根 prefix 也 seed，让 worker 列举后续页（startAfter 不可传递，会重复）
				// 修正：根 prefix 的直接对象上面已发，不要重复 seed；只 seed 子目录
			}
			// 撤回上面的 Seed(prefix)，避免重复：
			// 实际实现里这里需要更细——见下方注释
		}
	} else {
		lister.Seed(prefix)
	}

	// 启动 list workers
	var listWg sync.WaitGroup
	for i := 0; i < cfg.ListConcurrency; i++ {
		listWg.Add(1)
		worker := newWorkerForList(pool, i, cfg, bucket)
		listerLocal := NewLister(worker, q, out, stats, cfg) // 每个 worker 一个 lister 实例？不，queue 共享
		go func(w S3API) {
			defer listWg.Done()
			l := NewLister(w, q, out, stats, cfg)
			l.Run(ctx, &listWg, objCh, i)
		}(worker)
	}
	_ = lister // 避免未用

	go func() {
		listWg.Wait()
		close(objCh)
		stats.SetListDuration(time.Since(start))
	}()

	if cfg.IsCheck {
		var checkWg sync.WaitGroup
		for i := 0; i < cfg.CheckConcurrency; i++ {
			checkWg.Add(1)
			worker := newWorkerForCheck(pool, i+cfg.ListConcurrency, cfg, bucket)
			go func(w S3API) {
				defer checkWg.Done()
				c := NewChecker(w, out, stats, cfg.IsSuccessLog)
				for {
					select {
					case obj, ok := <-objCh:
						if !ok {
							return
						}
						c.Handle(obj)
					case <-ctx.Done():
						return
					}
				}
			}(worker)
		}
		checkWg.Wait()
	} else {
		for range objCh {
		}
	}

	if err := out.Close(); err != nil {
		log.Printf("output close: %v", err)
	}
	stats.SetTotalDuration(time.Since(start))
	statsPath := filepath.Join(cfg.OutputDir, "stats.txt")
	if err := stats.WriteToFile(statsPath, cfg.IsCheck); err != nil {
		return fmt.Errorf("write stats: %w", err)
	}
	stats.PrintSummary(cfg.IsCheck)
	return nil
}

func newWorkerForList(pool *NodePool, idx int, cfg *Config, bucket string) S3API {
	nodeIdx := pool.Assign(idx)
	if nodeIdx < 0 {
		log.Fatalf("no available nodes")
	}
	client, err := NewMinioClient(pool.endpoints[nodeIdx], cfg.AK, cfg.SK, cfg.Scheme == "https")
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}
	return NewS3Client(client, bucket)
}

func newWorkerForCheck(pool *NodePool, idx int, cfg *Config, bucket string) S3API {
	return newWorkerForList(pool, idx, cfg, bucket)
}
```

**注意：上面 main.go 的 Mode1 seed 逻辑有重复列举 bug**（注释里已标）。修复：Mode1 的根 prefix 直接对象不要在 main 里发送，而是把根 prefix 也 seed 进 queue，让 worker 用 startAfter 翻页。简化版：

```go
if cfg.ListType == 1 {
    // 用一次 delimiter 调用拿子目录，把根 prefix + 所有子目录 seed 进 queue
    worker := newWorkerForList(pool, 0, cfg, bucket)
    _, subprefixes, _, err := worker.ListPage(ctx, prefix, startAfter, true, 1)
    if err != nil {
        out.WriteListFailed(prefix, err.Error())
    } else {
        lister.Seed(prefix)
        for _, sp := range subprefixes {
            lister.Seed(sp)
        }
    }
} else {
    lister.Seed(prefix)
}
```

实现时只用修复版，不要写带 bug 的版本。

`pool.endpoints` 是私有字段——在 main.go 同包内可访问。或给 NodePool 加 `Endpoint(idx int) string`。实现时加这个方法更干净。

- [ ] **Step 2: 加 `NodePool.Endpoint` 方法**

Edit `nodepool.go`，加：

```go
func (p *NodePool) Endpoint(idx int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.endpoints[idx]
}
```

更新 `newWorkerForList` 使用 `pool.Endpoint(nodeIdx)` 替代 `pool.endpoints[nodeIdx]`。

- [ ] **Step 3: 写集成测试 `main_test.go`（用 scriptedS3）**

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunIntegration_FakeS3(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := `
endpoints:
  - fake.example:9000
scheme: http
ak: a
sk: s
list_type: 2
list_concurrency: 2
check_concurrency: 2
output_dir: ` + dir + `
is_check: true
is_success_log: false
progress_interval: 1000
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}

	// 这个测试需要 mock S3API，但 main.go 里直接 newWorkerForList 走真 minio。
	// 真集成测试需要真 S3 / minio 容器，跳过单测，标 Skip。
	t.Skip("integration test requires real minio; see manual test plan")
}

func TestRunEndToEnd_smoke(t *testing.T) {
	// 占位：实际 e2e 用 S3_ENDPOINT 环境变量
	if os.Getenv("S3_ENDPOINT") == "" {
		t.Skip("set S3_ENDPOINT to run smoke test")
	}
	ctx := context.Background()
	cfg := &Config{
		Endpoints: []string{os.Getenv("S3_ENDPOINT")},
		Scheme: "http",
		AK: os.Getenv("S3_AK"),
		SK: os.Getenv("S3_SK"),
		ListType: 2,
		ListConcurrency: 2,
		CheckConcurrency: 2,
		OutputDir: t.TempDir(),
		IsCheck: true,
		ProgressInterval: 1000,
	}
	_ = strings.TrimSpace
	if err := run(ctx, cfg, os.Getenv("S3_BUCKET"), "", ""); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 4: 运行全部测试**

Run: `go test ./... -v`
Expected: 全部 PASS（除两个 Skip）。

- [ ] **Step 5: 编译**

Run: `go build ./...`
Expected: 编译通过。若有未定义符号或类型错误，逐个修复。

- [ ] **Step 6: 手动冒烟测试（如果用户有真 S3）**

```bash
go build -o chunked_check_tool .
./chunked_check_tool -c config.yaml -bkt test-bucket
```

Expected: 输出 `stats.txt` + 各分类文件。

- [ ] **Step 7: 提交**

```bash
git add main.go main_test.go nodepool.go
git commit -m "feat: add main orchestration with mode1/mode2 seeding and workers"
```

---

## Self-Review Checklist

完成后检查：

1. **Spec 覆盖**：
   - [ ] 配置 YAML + 默认 scheme=http → Task 1
   - [ ] 无界队列 → Task 2
   - [ ] NodePool 轮询 + 故障隔离 → Task 3
   - [ ] Stats（对象总数、list 次数、平均/总耗时、程序总耗时）→ Task 4
   - [ ] 5 个输出文件 + `<etag>|<key>` 格式 → Task 5
   - [ ] MinIO client + Range GET + 跳过 TLS → Task 6
   - [ ] 严格 ETag 正则 + chunk-signature 正则 + 多段跳 Range GET → Task 7
   - [ ] Mode 1 + Mode 2 + 仅列举模式 → Task 8
   - [ ] 进度打印（本地计数，无 per-obj atomic）→ Task 9
   - [ ] main 编排 + 计时 → Task 10

2. **Placeholder 扫描**：上面代码块里无 TBD/TODO。

3. **类型一致**：`ObjectInfo{Key, ETag}` 在 Task 6/7/8 一致；`S3API` 接口在 Task 6/7/8/10 一致；`Stats` 方法名在 Task 4/7/8/10 一致。

4. **已知风险**：
   - minio-go v7 `Core.ListObjects` 的签名需在 Task 6 实现时验证。若不匹配，停下来问用户。
   - main.go Mode1 seed 逻辑需用修复版（不带 bug 的版本）。
   - Range GET 用 `minio.GetObjectOptions{Range: "bytes=0-127"}`——若 minio-go 不支持该字段，改用 `core.GetObject` + 手动设 Range header。
