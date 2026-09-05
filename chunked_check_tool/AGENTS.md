# AGENTS.md — chunked_check_tool

本文件面向在本仓库工作的 AI 代理（和人类工程师），说明项目意图、模块边界、关键不变量与已知遗留项。改动前请先读完本文件对应章节。

## 1. 项目意图

自研 S3 存储系统曾因未正确解析 `X-Amz-Content-Sha256` header，把 `aws-chunked` PUT 请求的 `length;chunk-signature=…` / `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式化内容当作原始 body 写入存储。

本工具**并发**列举并校验对象：
- 普通对象（ETag 为 32 位小写 MD5 hex）→ Range GET 前 128 字节，匹配 chunk-signature 正则 → 命中即视为损坏。
- 多段对象（任何非 `^[0-9a-f]{32}$` 的 ETag）→ 直接入 `multipart_objects.txt`，**不做 Range GET**。
- 列举失败、校验失败分别落不同文件。
- 支持几十亿对象规模，内存有界，背压自调节。

**不在本工具范围**：修复损坏对象（另行处理）。

## 2. 技术栈

- Go 1.27（`go.mod` module `chunked_check_tool`）
- `github.com/minio/minio-go/v7`（MinIO SDK，非 AWS SDK；用 `Core.ListObjectsV2` 拿 Contents + CommonPrefixes，用 `Client.GetObject` + `GetObjectOptions.SetRange(0, 127)` 做 Range GET）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（用户明确要求忽略证书）

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析、`signal.NotifyContext`（SIGINT/SIGTERM）、`run` 编排、Mode 1 根列举分页、worker 启停、stats 写盘 |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，带默认值） |
| `nodepool.go` | `NodePool`：轮询 `Assign`、`MarkFailed`、`URL`、`Endpoint`（故障隔离，全局共享 failed 集） |
| `s3client.go` | `S3API` 接口、`S3Client`（`minioListAPI` 接口包装 minio.Core + minio.Client，按 `cfg.ListAPIVersion` 分派 V1/V2）、`FakeS3`/`scriptedS3`（测试用）、节点故障重试一次 |
| `lister.go` | `Lister`（无 `s3` 字段；`Run`/`processPrefix` 接 `s3` 参数）、无界队列 BFS、`inflight` atomic 计数 |
| `walker.go` | `runRecursiveWalk`：Mode 3 信号量递归列举，`sync.WaitGroup` 终止，不用 queue/inflight |
| `checker.go` | `Checker`、`isNormalETag`（严格 32 位小写 hex）、`chunkSigRe` |
| `output.go` | 8 channel + writer goroutine（5 个按 OwnerID 分目录 fan-out，3 个根目录全局），`bufio.Writer` 64KB，append 模式，per-owner 文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`） |
| `queue.go` | 无界队列（slice + mutex + cond），ctx-aware 阻塞 Pop |
| `stats.go` | atomic.Int64 计数器 + `StatsSnapshot` + `WriteToFile` + `PrintSummary` |
| `progress.go` | `ProgressPrinter` + `localCounter`（每 worker 本地 int，无 per-obj atomic） |

## 4. 关键不变量（改动前必须守住）

1. **多段判定严格**：仅 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象。大写、长度不对、`<hex>-N`、空值一律按多段处理。**绝不把多段误判为普通对象**。`isNormalETag` 用逐字节循环实现（非正则），不要改成宽松匹配。

2. **多段对象跳过 Range GET**：直接写 `<ownerID>/multipart_objects.txt`（仅 key，不带 ETag）。不要给多段对象发 Range GET（浪费请求 + 可能误判）。

3. **ETag 来源**：list 响应（统一），**不从 Range GET response header 取**。OwnerID 同样来自 list 响应（minio-go v7.3.0 默认 `fetchOwner=true`，无额外请求开销）。

4. **结果文件按 OwnerID 分目录，处理文件全局**：`corrupted`/`multipart`/`corrupted_multipart`/`ok_multipart`/`ok` 这五类结果文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`）；`list_failed`/`check_failed`/`multipart_check_failed`/`stats` 留根目录全局。理由：结果文件数量大且天然按 owner 分桶有意义；处理文件全局方便运维统一排查；stats 全局一份避免 owner 分桶后还要汇总。ownerDirName 折叠空/`.`/`..`/含路径分隔符的 OwnerID 到 `_unknown`，防止路径穿越。

5. **checker goroutine 绝不退出**：任何错误写 `check_failed`（普通对象）/ `multipart_check_failed`（多段分段）后继续。若 checker 退出，`objCh` 无人消费，list worker 永久阻塞。

6. **`objCh` 永远会被关闭**：list 阶段保证（`listWg.Wait` → `close(objCh)`；或 Mode 1 无 seed 时显式 `q.Close()`）。

7. **Add-before-Push 顺序**（BFS 终止性）：Mode 2 在 push 新 subprefix 前 `inflight++`，否则 push 后 worker 消费完 inflight 已归零，新 subprefix 无人处理 → 永久挂起。改动 `lister.go` 的 `processPrefix` 时务必保留此顺序。

8. **Mode 1 不 seed 根 prefix**：根的直接对象由 `main` 用 delimiter 分页拿（Contents → objCh）；只把 CommonPrefixes（子目录）seed 进队列。若 seed 根，worker 会用 `delim=false` 平铺列出根，与 main 的 delimiter 分页重复，根下子目录对象被计两次。

9. **`-nextmarker` 仅 Mode 1 生效**：作为根列举的 start-after 参数。Mode 2/3 忽略（文档化限制）。

10. **节点故障重试一次**：`S3Client.ListPage`/`RangeGet`/`RangeGetAt` 在 `isNodeFaultErr`（连接拒绝、超时、5xx，**不含 4xx**）时 `pool.MarkFailed` → `pool.Assign` 找下一个存活节点 → 重建 client → 重试一次。再失败按业务错误处理（写 `list_failed`/`check_failed`/`multipart_check_failed`）。普通 S3 业务错误（404/403）不触发重绑。

11. **性能优先但可读**：HTTP keep-alive（minio-go 自带连接池，不要自建）、`bufio.Writer` 64KB、合理 channel 容量、避免 per-obj 分配。**但**任何"复杂难读"的优化（手写内存池、unsafe、lock-free 结构）需先向用户请求确认，不要直接写。见 `memory/performance-vs-readability.md`。

12. **Mode 3 用 WaitGroup 终止**：`walker.go` 的 `runRecursiveWalk` 不用 queue、不用 inflight 计数；终止性靠 `sync.WaitGroup`。每个 `go walk(subprefix)` **前**必须 `wg.Add(1)`，否则 Wait 可能在 spawn 前归零、过早关闭 objCh。root 首个 `wg.Add(1)` 同理在 `go walk(prefix)` 前。objCh 由 main 中的 walk goroutine 在 `wg.Wait()` 返回后显式关闭。子树列举失败只写 `list_failed` 跳过该子树，不中止其他分支。

13. **V1/V2 分页协议对 caller 透明**：`S3Client.listPageOnce` 按 `cfg.ListAPIVersion` 分派 `Core.ListObjects`（V1，marker 游标）或 `Core.ListObjectsV2`（V2，continuation token）。两条路径都归一化进 `listResult{contents, commonPrefixes, next}`，`next` 作为下一次 `ListPage` 的 `continuationToken` 参数回传。V1 无 delimiter 且 `IsTruncated=true` 但 `NextMarker` 为空时，回退到最后一个 Contents key 作 marker；有 delimiter 时 S3 返回 `NextMarker`。caller（lister/walker/main 根分页）只需把 `next` 喂回 `continuationToken`，不感知 V1/V2 差异。`S3Client.core` 是 `minioListAPI` 接口（非 `*minio.Core`）以支持测试注入。

14. **多段分段检查的失败分流**：分段 RangeGet 报错走 `multipart_check_failed` 路径（`WriteMultipartCheckFailed` + `IncrMultipartCheckFailed`），**不走** `check_failed`。任一段命中 chunk-signature 即视为整段对象损坏，写 `<ownerID>/corrupted_multipart_objects.txt` 并 `IncrCorruptedMultipart`（同时**不** `IncrMultipart`）。干净的多段对象 `IncrMultipart`，仅 `is_success_log=true` 时写 `<ownerID>/ok_multipart_objects.txt`。

## 5. CLI 与配置

### CLI flags
```
-c <path>          # 配置文件，必填
-bkt <bucket>      # 桶名，必填
-prefix <prefix>   # 列举前缀，可选，默认空
-nextmarker <key>  # start-after key，可选；仅 Mode 1 根分页使用
```

### config.yaml 字段
| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | ip:port 列表，至少 1 个 |
| `scheme` | `http` | `http` 或 `https`（后者跳过证书校验） |
| `ak` / `sk` | 必填 | SigV4 静态凭证 |
| `list_type` | `1` | 1=子目录+平铺 nextmarker；2=递归 BFS delimiter；3=递归+信号量 |
| `list_api_version` | `2` | 1=ListObjects V1（marker 分页）；2=ListObjectsV2（continuation token，默认） |
| `list_concurrency` | `8` | 列举并发度 |
| `check_concurrency` | `16` | 校验并发度 |
| `output_dir` | `.` | 输出目录 |
| `is_check` | `true` | true=列举+校验；false=仅列举（只写 stats.txt + list_failed.*） |
| `is_success_log` | `false` | 是否记录正常对象到 `<ownerID>/ok_objects.txt` + 干净多段到 `<ownerID>/ok_multipart_objects.txt` |
| `is_multipart_check` | `false` | 是否对多段对象做分段损坏检查 |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `progress_interval` | `100000` | 进度打印阈值（约） |

## 6. 输出文件（append 模式）

### 结果文件（按 OwnerID 分目录，`<output_dir>/<ownerID>/<filename>`；OwnerID 为空 → `_unknown/`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏普通对象 key | Range GET 命中 chunk-signature（`is_check=true`） |
| `multipart_objects.txt` | 多段对象 key（仅 key） | `is_multipart_check=false` 时所有多段对象 |
| `corrupted_multipart_objects.txt` | 损坏多段对象 key | `is_multipart_check=true` 时分段检查命中 |
| `ok_multipart_objects.txt` | 干净多段对象 key | `is_multipart_check=true` 且 `is_success_log=true` |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true` |

### 处理文件（全局，根目录 `<output_dir>/<filename>`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `list_failed.txt` | 列举失败 prefix | list worker 调用失败（list-only 模式也写） |
| `list_failed.log` | 列举失败结构化错误（slog text，req_id/prefix/http_code/s3_code/err） | 同上 |
| `check_failed.txt` | 普通对象校验失败 key | checker 普通对象 RangeGet 失败 |
| `check_failed.log` | 校验失败结构化错误（slog text，req_id/key/http_code/s3_code/err） | 同上 |
| `multipart_check_failed.txt` | 多段分段检查失败 key | `is_multipart_check=true` 时分段 RangeGet 失败 |
| `multipart_check_failed.log` | 多段分段检查失败结构化错误（slog text） | 同上 |
| `stats.txt` | 计时与计数（全局一份） | 程序结束 |

`is_check=false` 时只写 `list_failed.*` + `stats.txt`，不创建 owner 目录。`is_check=true && is_multipart_check=false` 时 `corrupted_multipart_objects.txt` / `ok_multipart_objects.txt` / `multipart_check_failed.*` 不创建。

## 7. 编译与测试

```bash
# 默认（当前平台）
go build -o chunked_check_tool .

# Linux 二进制（交叉编译，目标机上不需要 Go）
GOOS=linux GOARCH=amd64 go build -o chunked_check_tool-linux-amd64 .
# 或 arm64
GOOS=linux GOARCH=arm64 go build -o chunked_check_tool-linux-arm64 .

# 测试（含 race 检测）
go test -race ./...

# 单文件聚焦
go test -run TestChecker -v ./...
```

Go 1.27 二进制路径：`/Users/gengyuanzhe/sdk/go1.27.1/bin/go`（若不在 PATH，`export PATH=$PATH:/Users/gengyuanzhe/sdk/go1.27.1/bin`）。

## 8. 已知遗留项（改动时留意，非阻塞）

来源：SDD ledger 的 deferred minors（见 `.superpowers/sdd/2026-09-03-chunked-check-tool/progress.md`）。

- **Queue.Pop 阻塞路径的 lost-signal 窗口**：fast-path unlock 与 re-lock+Wait 之间存在理论上的丢失唤醒窗口。当前 pipeline 关闭队列时用 `Broadcast`，不会触发。若未来改为单消费者关闭场景需重新评估。
- **NodePool.Assign 空端点 panic**：`n=0` 时除零。`Config` 上游校验 `endpoints` 非空，故不会触发。无 `idx` 越界保护（调用方只用 `Assign` 返回的索引）。
- **NewOutput 部分初始化失败的 goroutine/file 泄漏**：brief 继承的设计，仅 `os.OpenFile` 失败时触发（罕见启动期磁盘错误）。修复需偏离 brief。
- **bufio flush 错误静默丢弃**：writer goroutine 不检查 `Flush` 错误，无 Write/Close race guard（标准使用契约）。
- **Core.ListObjectsV2 无 ctx 参数**：minio-go 限制， cancellation 在更高层（放弃 goroutine on `ctx.Done`），靠 socket 超时兜底。
- **Checker.Handle 用 `context.Background()`**：非父 ctx（brief 逐字；改签名会级联到 Task 8）。
- **MaybePrint 在 stats.Snapshot() 之后再取写锁**：快照原子安全，仅进度行时间戳可能略偏。
- **Mode 1 根直接对象不调 `onObject`**：进度计数偏少（仅外观，stats 正确）。
- **`workerIdx` 参数在 `Lister.Run` 未用**：保留给未来 NodePool 分配，当前是死重量。
- **`list_type:3` 等非法值静默落到 Mode 2 分支**：可加校验。
- **per-owner writer goroutine 的 MkdirAll/OpenFile 失败静默丢弃该行**：罕见启动期磁盘错误，第 N 个 owner 目录建不出来时该 owner 的结果行会丢，但其他 owner 不受影响。

## 9. 工作流约定

- 实现性改动遵循 TDD：先写失败测试，再实现，再跑测试，再 commit。
- 每个 commit 聚焦一个职责（feat/fix/refactor 前缀）。
- `chunked_check_tool` 二进制已 `.gitignore`，不要提交。
- `.superpowers/` 目录是 SDD 工作区（ledger、brief、review package），已 gitignore，不要提交。
- 改动涉及多文件接口（`S3API`、`Lister.Run` 签名、`Output` 方法、`Stats` 字段）时，先在 ledger 或 PR 描述记录决策，再改。
