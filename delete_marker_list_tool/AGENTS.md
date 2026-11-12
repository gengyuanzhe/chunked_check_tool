# AGENTS.md — delete_marker_list_tool

本文件面向在本仓库工作的 AI 代理（和人类工程师），说明项目意图、模块边界、关键不变量与已知遗留项。改动前请先读完本文件对应章节。

## 1. 项目意图

本工具是删除清理的**前置侦察**：扫全桶多版本列举，把所有 delete marker 落盘成 `delete_markers.txt`。下游工具（另行实现）读取该文件，逐条执行 `DeleteObject(bucket, key, VersionId)` 完成真正删除。

**本工具只读不删**。不做内容校验、不做备份、不做多段探测——这些是 chunked_check_tool 的职责。

设计文档：`../docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md`

## 2. 技术栈

- Go 1.27（`go.mod` module `delete_marker_list_tool`）
- `github.com/minio/minio-go/v7 v7.3.0`（**Pin 不升级**——channel API 的 `WithVersions` + CommonPrefixes 行为按 v7.3.0 校准；升级需回归验证）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（对齐 chunked_check_tool）

**用 `Client.ListObjects` 不用 `Core.ListObjects`**：minio-go v7.3.0 的 Core 不支持版本化列举（`WithVersions`）；版本化列举只在 Client 的 channel API 上暴露。

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析（`-c`/`-bkt`/`-prefix`）、`run` 编排、run.log 打开（第一个 open）、配置快照打印（sk 掩码）、BucketExists 预检、summary 打印。无信号处理器 |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，带默认值 + 校验 + 时间戳后缀重写） |
| `s3client.go` | `S3Client`（minio.Client 包装）、`ListVersions`（channel API 封装）、`BucketExists`。transport 调优对齐 chunked_check_tool |
| `lister.go` | `Lister`：mode 1 单流 + mode 2 BFS 两种驱动；`isCommonPrefix` 四条件判别；`recordListFailed` 从 `minio.ErrorResponse` 提 HTTP/S3 code + reqID |
| `queue.go` | 无界前缀队列（slice + mutex + cond），ctx-aware Pop，`SignalDone` 终止。对齐 chunked_check_tool/queue.go 形态，砍掉 Drain（无优雅退出） |
| `output.go` | `Output`：三个 writer goroutine（delete_markers / invalid_keys / list_failed）+ slog listLogger；`\x1f` 行分隔符；key 含 `\x1f` 自动路由到 invalid_keys + URL 编码；list_failed.txt 惰性 open；3 秒定时 flush |
| `stats.go` | atomic.Int64 计数器 + `StatsSnapshot` + `PrintSummary` |
| `progress.go` | `ProgressPrinter`（per-worker 本地计数 + 阈值打印）+ `QueueSnapshot`（mode 2 队列/inflight 快照） |

## 4. 关键不变量（改动前必须守住）

1. **行分隔符 `\x1f`**：行格式 `bkt\x1fkey\x1fversion_id`。**绝不**改回 `|`——chunked_check_tool 用 `|`，但本工具因 key 含 `|` 会导致漏删（marker 永远删不掉），刻意换 `\x1f`。`output.go` 的 `rowSep` 常量不可改。

2. **`version_id` 字面量 `"null"` 是合法值**：S3 开版本控制之前写入的旧版本对象返回字面量字符串 `"null"` 作为 VersionId。下游删除工具必须原样透传，不要当空值。本工具原样写入结果文件，不做特殊处理。

3. **共同前缀判别四条件合取**：`VersionID == "" && !IsDeleteMarker && ETag == "" && strings.HasSuffix(Key, "/")`。**不可放宽任一条件**——误判前缀为版本会漏扫子目录，误判版本为前缀会死循环。详见设计文档 §4.2。
   - minio channel API 把 common prefix 作为 `ObjectInfo{Key: ".../", 其余字段空}` 吐出
   - 真版本条目 VersionID 非空（含 `"null"` 字面量）；null 版本对象的 VersionID 在 minio-go v7.3.0 里是 `"null"` 字符串，非空
   - 后三个条件兜底：真对象有 ETag，marker 有 IsDeleteMarker

4. **mode 2 BFS 终止不变量——Add-before-Push**：每入队一个新前缀**先** `inflight.Add(1)` **再** `queue.Push`。违反会导致 inflight 提前归零 → 提前终止 → 漏扫子目录。对齐 chunked_check_tool/AGENTS.md §4.7。

5. **`list_failed.txt` 惰性创建**：文件不存在 = 无失败，文件存在 = 至少一次失败，空行 = 空前缀失败。**不可**在 NewOutput 时创建空文件（会破坏"文件存在即有失败"契约）。`output.go` 的 `writeListFailedLine` 在首次调用时 lazy open。

6. **无重试无断点无优雅退出**：失败靠整轮重跑。**不可**加 retry/journal/signal handler（与 chunked_check_tool 不同；spec §9 非目标）。

7. **无 NodePool 无节点恢复**：mode 2 worker 用原子轮询绑 endpoint，失败即记 `list_failed` 继续。**不可**引入节点隔离/重绑/恢复探测（spec §9 非目标）。

8. **`output_dir_timestamp` 同级后缀规则**：trim trailing `/` 或 `\`，append `_<YYYYMMDD_HHMMSS>`。**不是**子目录嵌套（`./out/` → `./out_<stamp>` 而非 `./out/_<stamp>`）。对齐 chunked_check_tool/config.go:93-106。

9. **run.log 第一个 open**：先于任何输出文件。`mwOut = io.MultiWriter(os.Stdout, runLog)`，`mwErr = io.MultiWriter(os.Stderr, runLog)`。sk 在配置快照里掩码为 `***`。对齐 chunked_check_tool/main.go:102-110。

10. **`\x1f`-in-key 路由**：key 含 `\x1f` 的 marker 走 `invalid_keys.txt`，key 经 `url.QueryEscape` 编码。**不可**直接写入 `delete_markers.txt`（会撕裂行字段）。`output.go` 的 `WriteDeleteMarker` 自动检测 + 路由。

11. **minio-go v7.3.0 channel API**：`Client.ListObjects(WithVersions: true)` 吐出的 `ObjectInfo` 带 `IsDeleteMarker` / `VersionID` / `IsLatest` 字段；带 delimiter 时也吐 common prefix 作为 `ObjectInfo{Key: ".../"}` 条目。**不要**改用 `Core.ListObjects`（Core 不支持 WithVersions）；升级 minio-go 需重验 channel API 是否仍吐 CommonPrefixes（spec §4.1）。

## 5. 约束

- 单 `package main`，独立 `go.mod`，与 chunked_check_tool / list_only 平级
- HTTP transport 调优对齐 chunked_check_tool：`MaxIdleConnsPerHost: 32`、`IdleConnTimeout: 90s`、`dial_timeout` 默认 10s 兜底黑洞 IP、optional `ResponseHeaderTimeout`
- `BucketLookup: minio.BucketLookupPath`（ip:port 节点更安全，避免 DNS 子域）
- 无 `-resume` / `-nextmarker` / `-check-file` / `-list-file` / `-backup-file` flag
- `list_type` **必须显式配置**（1 或 2），不给默认值——模式选择是正确性关键，按"校验类参数默认零值+强制显式配置"原则不设默认（与 chunked_check_tool 不同，那里 list_type 默认 2）
- 未知 YAML 字段被忽略——可复用 chunked_check_tool 的 config.yaml

## 6. 遗留任务（已知未决/未来工作）

1. **key 含 `\n` 的 marker 仍会撕裂行**：chunked_check_tool 同样不防护，S3 key 含换行极罕见。未来如需可加 `\n` 检测 + URL 编码路由（同 `\x1f` 路由模式）。

2. **同秒内连续启动两次**（`output_dir_timestamp: true`）会撞目录并发 append 撕裂尾部行：靠下游解析时丢弃格式非法行兜底。如需严格保证可加"目录已存在即拒绝"——但需同时支持 `output_dir_timestamp: false` 的补齐追加语义，两者冲突。

3. **无优雅退出**：Ctrl-C / kill -9 丢尾部整行，run.log 里 summary 不会打出来。未来如需可加 chunked_check_tool 的两段式 SIGINT handler + journal——但 spec §9 明确非目标，加之前需重新评估。

4. **下游删除工具未实现**：需读 `delete_markers.txt` + `invalid_keys.txt`（后者 key 需 `url.QueryUnescape` 解码），逐条 `DeleteObject(bucket, key, VersionId)`。删除按 version-id 幂等——重复行无害。

5. **mode 2 终止检测用轮询**：`runMode2` 用一个 watcher goroutine 100ms 级轮询 `inflight.Load() == 0` 后 `SignalDone`。未来如需更精确可用 channel + WaitGroup 替代，但当前实现已足够快（inflight 在 ms 级变化）。

## 7. 相关文档

- 设计文档：`../docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md`
- 实现计划：`../docs/superpowers/plans/2026-11-12-delete-marker-list-tool.md`
- chunked_check_tool AGENTS.md（约定参考）：`../chunked_check_tool/AGENTS.md`
