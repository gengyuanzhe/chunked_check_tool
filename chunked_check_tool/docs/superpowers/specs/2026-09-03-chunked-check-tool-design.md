# chunked_check_tool 设计文档

- 日期: 2026-09-03
- 状态: 设计完成，待评审

## 1. 背景与目标

自研 S3 存储系统无法正确识别 `X-Amz-Content-Sha256` header，导致 `aws-chunked` 类型的 PUT 请求未按 chunked 编码解析，把 `length;chunk-signature=xxx` / `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式化内容作为原始 body 写入存储。

需要编写一个 Go 工具，**并发**列举并检查对象，把损坏的普通对象、多段对象、以及处理失败的对象分别写入不同文件，支持几十亿对象规模。

## 2. 范围

- 输入: 命令行 `-c config.yaml -bkt <bucket> [-prefix <p>] [-nextmarker <key>]`
- 输出: 损坏对象、多段对象、列举失败、校验失败、可选的成功对象日志、统计文件
- 不在本工具范围: 修复损坏对象（另行处理）

## 3. 技术选型

| 项 | 选择 | 理由 |
|---|---|---|
| 语言 | Go 1.27 | 需求指定 |
| S3 客户端 | `github.com/minio/minio-go/v7` | 用户指定，API 简洁，支持自定义 endpoint + SigV4 + Range GET |
| 配置格式 | YAML (`gopkg.in/yaml.v3`) | 可读性好，适合手改 |
| TLS | https 时 `InsecureSkipVerify=true` | 用户指定忽略证书；scheme 默认 http |

## 4. 模块拆分

```
chunked_check_tool/
  main.go         # flag 解析、配置加载、编排、计时、进度打印
  config.go       # YAML 配置结构体 + 加载器
  nodepool.go     # 轮询 endpoint 分配 + 故障隔离
  s3client.go     # MinIO client 包装，worker 绑定 endpoint
  lister.go       # 两种列举模式，输出对象到 channel
  checker.go       # Range GET 128 字节 + chunk-signature 正则
  output.go       # 5 个输出文件的串行 writer
  queue.go        # 无界 prefix 队列（slice + mutex + cond）
  stats.go        # atomic 计数器 + 最终统计
  go.mod
```

## 5. 配置与 CLI

### CLI flags

```
-c <path>          # 配置文件，必填
-bkt <bucket>      # 桶名，必填
-prefix <prefix>   # 列举前缀，可选，默认空
-nextmarker <key>  # start-after key，可选；作为 ListObjectsV2 的 start-after 参数，跳过该 key 之前的对象
```

### config.yaml

```yaml
endpoints:              # ip:port 列表，至少 1 个
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http            # 或 https（跳过证书校验）；默认 http，缺省时用 http
ak: <access-key>
sk: <secret-key>
list_type: 1            # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter
list_concurrency: 8     # 列举并发度
check_concurrency: 16   # 校验并发度
output_dir: ./out        # 默认当前目录
is_check: true           # true=列举+校验, false=仅列举
is_success_log: false    # 是否记录正常对象
progress_interval: 100000  # 进度打印阈值（约）
```

## 6. 输出文件

全部写到 `output_dir`：

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏的普通对象 key | 校验发现 chunk-signature 前缀 |
| `multipart_objects.txt` | 多段对象 `<etag>\|<key>` | ETag 不匹配 `^[0-9a-f]{32}$` |
| `list_failed.txt` | 列举失败的 prefix + 原因 | list worker 调用失败 |
| `check_failed.txt` | 校验失败的对象 key + 原因 | checker 调用失败 / 非预期 ETag |
| `success_objects.log` | 正常对象 key | 仅 `is_success_log=true` |
| `stats.txt` | 计时与计数 | 程序结束 |

`is_check=false` 时不写任何对象文件，仅写 `stats.txt`。

## 7. NodePool 与 S3 Client

### NodePool (`nodepool.go`)

```go
type NodePool struct {
    endpoints []string
    scheme    string
    failed    map[int]struct{}
    mu        sync.RWMutex
}

func (p *NodePool) Assign(workerIdx int) int    // 启动时轮询绑定，跳过已故障节点
func (p *NodePool) MarkFailed(idx int)          // 标记故障 + 打印告警
func (p *NodePool) URL(idx int) string          // scheme://endpoint
```

- 启动时 `Assign(i)` 返回 `endpoints[i % N]`，若该下标已故障则向后找下一个存活。
- 请求失败且错误类型为节点故障（连接拒绝、超时、5xx）→ `MarkFailed`，worker 重新 `Assign` + 重建 MinIO client，**重试一次**；再失败则记 `list_failed` / `check_failed`，不阻塞流程。
- 普通 S3 业务错误（404、403 等）不触发重绑。
- 故障集合全局共享，所有 worker 避开。

### Worker (`s3client.go`)

每个 worker 持有一个 `*minio.Client`，绑定到自己的 endpoint。节点重绑时重建 client。

```go
type Worker struct {
    pool    *NodePool
    nodeIdx int
    client  *minio.Client
    bucket  string
}

func (w *Worker) callList(prefix, startAfter string, delim bool) (objs []Object, prefixes []string, nextMarker string, err error)
func (w *Worker) callRangeGet(key string) (body []byte, err error)
```

MinIO client 构造参数：
- `Endpoint`: `pool.URL(nodeIdx)`
- `Creds`: `credentials.NewStaticV4(ak, sk, "")`
- `Secure`: `scheme == "https"`
- `Transport`: 自定义 `*http.Transport`，https 时 `TLSClientConfig.InsecureSkipVerify = true`

Range GET 用 `GetObject` + `Range` 选项（`bytes=0-127`），只返回 body。ETag 由 list 响应提供，不从 Range GET header 取。

## 8. Lister

### 共用结构

- `prefixQueue`：**无界**队列（`queue.go`，slice + mutex + cond），承载待列举 prefix 任务。
- `objCh`：**有界** channel，cap = `check_concurrency * 4`（最少 2000），承载 list → check 的对象。
- list worker 数 = `list_concurrency`，`sync.WaitGroup` 跟踪。

### Mode 1（子目录 + 平铺 nextmarker）

1. 主线程先用一次 delimiter 列举根 prefix，拿到 `CommonPrefixes`（子目录）。
2. 把这些子目录 + 根 prefix（代表根下直接对象）全部塞进 `prefixQueue`。
3. 每个 list worker 从 `prefixQueue` 取一个 prefix，用 `ContinuationToken` 翻页列举该 prefix 下**所有对象**（**不**带 delimiter，纯 nextmarker 分页），对象全部发到 `objCh`。
4. `nextmarker` flag 作为根列举的 start-after 参数传入第 1 步（跳过该 key 之前的对象）。
5. 所有 prefix 处理完 → 关闭 `objCh`。

### Mode 2（递归 BFS delimiter）

1. `prefixQueue` 预置根 prefix。若指定 `nextmarker`，第一次列举带 start-after 跳过该 key 之前的对象。
2. 每个 list worker 取一个 prefix，带 delimiter 列举：对象发到 `objCh`，新 `CommonPrefixes` append 回 `prefixQueue`。
3. ContinuationToken 分页在 worker 内部循环完成，不回队列。
4. 用 `inflight` atomic 计数器跟踪队列中待处理任务数：预置 1，push 新 prefix 时 +1，处理完一个 prefix -1。`inflight == 0` → 关闭 `objCh`。

### 仅列举模式（`is_check=false`）

- list worker 仍正常工作，但不发 `objCh`。
- 对象在 list worker 内部按 list 返回的 ETag 直接分类计数（多段 / 普通 / 总数）。
- checker 不启动。
- 只写 `stats.txt`，不写任何对象文件。

### 失败处理

- 单个 prefix 列举失败 → 写 `list_failed.txt`（prefix + 错误），worker 继续取下一个 prefix。
- 不让列举失败中断整体。

## 9. Checker

checker worker 数 = `check_concurrency`，从 `objCh` 消费对象。

```go
func (c *Checker) handle(obj Object) {
    // obj 带 Key + ETag（来自 list 响应）
    if !isNormal(obj.ETag) {             // 严格：仅 ^[0-9a-f]{32}$ 为普通对象
        output.writeMultipart(obj.ETag, obj.Key)   // 写 "<etag>|<key>"
        return
    }
    body, err := worker.callRangeGet(obj.Key)  // bytes=0-127
    if err != nil {
        output.writeCheckFailed(obj.Key, err.Error())
        return
    }
    if chunkSigRe.Match(body) {
        output.writeCorrupted(obj.Key)
    } else if is_success_log {
        output.writeSuccess(obj.Key)
    }
}
```

### 关键约束

- **ETag 来源**：list 响应（统一），不依赖 Range GET header。
- **多段判定严格**：只有 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象，**任何其他格式（`<hex>-N`、大写、长度不对、空值）一律按多段处理**。原则：绝不把多段误判为普通对象，宁可多段多计。
- **多段对象跳过 Range GET**：直接入 `multipart_objects.txt`，格式 `<etag>|<key>\n`。
- **Range GET 超时 30s**：超时计入 `check_failed`，防止网络挂死拖垮 pipeline。
- **checker goroutine 绝不退出**：任何错误都写 `check_failed` 后继续，否则 `objCh` 无人消费、list worker 永久阻塞。

### chunk-signature 正则（严格）

```
^[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}[\r\n]
```

- chunk size：十六进制（AWS 规范）
- signature：64 hex（SHA256 hexdigest）
- 行尾：`\r\n` 或 `\n`
- body 不足 128 字节时用 `body[:len(body)]` 匹配

## 10. Output Writer (`output.go`)

单 goroutine 串行化每个文件，5 个结果 channel：

```go
type Output struct {
    corruptedCh   chan string
    multipartCh   chan string
    listFailedCh  chan Entry    // {Prefix, Err}
    checkFailedCh chan Entry    // {Key, Err}
    successCh     chan string    // nil if !is_success_log
}
```

- 所有 channel cap = 1024，背压传回 producer。
- 启动 5 个 writer goroutine，各消费一个 channel，`Fprintln` 写文件。
- `Close()` 关闭所有 channel，等 goroutine 结束，flush 文件。
- 文件以 append 模式打开，支持断点续跑（不覆盖）。

## 11. 队列容量与背压

| 队列 | 类型 | 容量 | 内存预算（几十亿对象） |
|---|---|---|---|
| `prefixQueue` | 无界 slice+mutex+cond | BFS frontier | 数十万 × 100B ≈ 数十 MB |
| `objCh` | 有界 channel | `max(check_concurrency*4, 2000)` | 2000 × 100B = 200KB |
| 5 个输出 channel | 有界 channel | 1024 | 5 × 1024 × 100B = 500KB |
| 合计 | | | < 100MB |

背压链：输出 channel 满 → writer 阻塞 → checker 阻塞 → objCh 满 → list worker 阻塞 → 整体自调节。无死锁（见第 8 节 Mode 2 死锁分析）。

### Mode 2 无界 prefixQueue 的必要性

若 `prefixQueue` 有界，会死锁：8 个 worker、cap=16、每个 list 返回 10 个新 subprefix，前 16 次 push 后队列满，所有 worker 阻塞在 push、无人消费 → 死锁。无界队列消除此风险；BFS frontier 规模可控（受限于 key 层级宽度，不是对象总数）。

## 12. 统计与进度

### 统计（`stats.go`）

atomic 计数器：
- `listedTotal` — 列举到的对象总数
- `multipartCount`
- `corruptedCount`
- `listFailedCount`
- `checkFailedCount`
- `listCalls` — ListObjectsV2 调用次数
- `listLatencySum` — list 调用累计纳秒

计时：
- `listTotalDuration` — list 阶段 wall clock（从启动到 objCh 关闭）
- `totalDuration` — 程序总 wall clock

`stats.txt`：
```
total_objects: <listedTotal>
list_calls: <listCalls>
list_avg_latency_ms: <listLatencySum / listCalls / 1e6>
list_total_duration_sec: <listTotalDuration / 1e9>
total_duration_sec: <totalDuration / 1e9>
multipart: <multipartCount>
corrupted: <corruptedCount>
list_failed: <listFailedCount>
check_failed: <checkFailedCount>
```

`is_check=false` 时只写前 5 项 + `list_failed`。

### 进度打印（stdout）

- 每个 worker（list/check）维护本地 `localCount`，每处理一个对象 `localCount++`。
- `localCount >= progress_interval` → 读全局 atomic 快照，打印一行，`localCount = 0`。
- **每对象无 atomic 操作**，本地自增是寄存器级开销，零竞争。
- 打印节奏不精确（约 `progress_interval`），优先性能。

进度行示例：
```
[progress] listed=1000000 checked=995000 multipart=5000 corrupted=30 elapsed=120s
```

`is_check=false` 时只打 listed。程序结束时打印汇总。

## 13. 主流程 (`main.go`)

```go
func main() {
    parseFlags()                    // -c -bkt -prefix -nextmarker
    cfg := loadConfig()
    pool := NewNodePool(cfg)
    output := NewOutput(cfg)
    stats := NewStats()
    start := time.Now()

    // list 阶段
    listStart := time.Now()
    objCh := make(chan Object, max(cfg.CheckConcurrency*4, 2000))
    prefixQueue := NewUnboundedQueue()
    var listWg sync.WaitGroup
    seedListTasks(prefixQueue, cfg, bkt, prefix, nextmarker, listType)  // Mode1 或 Mode2 预置

    for i := 0; i < cfg.ListConcurrency; i++ {
        listWg.Add(1)
        go func(idx int) {
            defer listWg.Done()
            worker := NewWorker(pool, idx, cfg, bkt)
            runListWorker(worker, prefixQueue, objCh, output, stats, cfg, listType)
        }(i)
    }

    // close objCh when list done (Mode2 用 inflight 计数器；Mode1 用 listWg)
    go func() {
        waitListDone(prefixQueue, listType, &listWg)
        close(objCh)
        stats.listTotalDuration = time.Since(listStart)
    }()

    // check 阶段
    if cfg.IsCheck {
        var checkWg sync.WaitGroup
        for i := 0; i < cfg.CheckConcurrency; i++ {
            checkWg.Add(1)
            go func(idx int) {
                defer checkWg.Done()
                worker := NewWorker(pool, idx+cfg.ListConcurrency, cfg, bkt)
                runCheckWorker(worker, objCh, output, stats, cfg)
            }(i+cfg.ListConcurrency)
        }
        checkWg.Wait()
    } else {
        // drain objCh（不应有数据，但保险）
        for range objCh {
            stats.listedTotal++  // 仅列举模式下 objCh 不发数据
        }
    }

    output.Close()
    stats.totalDuration = time.Since(start)
    stats.WriteToFile(filepath.Join(cfg.OutputDir, "stats.txt"))
    stats.PrintSummary()
}
```

## 14. 错误处理边界

- list worker 失败 → 写 `list_failed`，继续。
- checker 失败 → 写 `check_failed`，继续。
- 节点故障 → `MarkFailed`，重绑重试一次；再失败按业务错误处理。
- checker goroutine 绝不因错误退出。
- objCh 永远会被关闭（list 阶段保证）。

## 15. 测试策略

- 单元测试：ETag 分类正则、chunk-signature 正则、无界队列、NodePool 故障重绑。
- 集成测试：用 minio testcontainer 起一个 S3，构造少量损坏对象 + 多段对象 + 正常对象，跑全流程，验证 4 个输出文件内容。
- 性能测试：大 prefix 下 list worker 吞吐与背压是否生效。
