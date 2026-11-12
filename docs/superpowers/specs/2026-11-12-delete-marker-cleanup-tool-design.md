# delete_marker_cleanup_tool 设计文档（2026-11-12）

## 1. 意图与背景

`delete_marker_list_tool` 把全桶所有 delete marker 落盘成 `delete_markers.txt`（行格式 `bkt\x1fkey\x1fversion_id`）。本工具消费该文件，提供两种操作模式：

- **check**：对每行 `(bkt, key, version_id)` 发 `HeadObject(bucket, key, VersionId=version_id)`，三态分类落盘，用于人工/自动审计 lister 的输出"是否真的都是 delete marker"。
- **delete**：对每行发 `DeleteObject(bucket, key, VersionId=version_id)`，物理删除该版本（对 delete marker 即消除该 marker）。`NoSuchVersion` 算成功（用户决策：删两次同一版本第二次返回 NoSuchVersion 是空操作，不噪音化）。

典型工作流（用户已确认）：
```
lister → delete_markers.txt
       → handle -mode check  → check_results.txt   (审计 lister 输出)
       → handle -mode delete  → deleted.txt          (实际删除)
       → handle -mode check  → check_results.txt   (复查：应全 absent)
```

mode 2（delete）**直接信任 lister 输出**，不做二次校验——用户工作流已经在前后各跑一次 mode check 包夹。

本工具**只做这两个操作**：不做列举、不做内容校验、不做备份、不支持断点续跑、不支持优雅退出。

## 2. 用法与形态

- 位置：仓库根目录下的兄弟模块 `delete_marker_cleanup_tool/`（与 chunked_check_tool、delete_marker_list_tool 平级），独立 `go.mod`（module `delete_marker_cleanup_tool`）。
- 依赖：`github.com/minio/minio-go/v7 v7.3.0`，与 lister 同版本，**锁定不升级**（HEAD/DELETE 带 VersionId 的行为按 v7.3.0 校准；升级需回归验证）。
- 命令行：

```
./delete_marker_cleanup_tool -c config.yaml -mode check -input path/to/delete_markers.txt
./delete_marker_cleanup_tool -c config.yaml -mode delete -input path/to/delete_markers.txt
```

| flag | 必填 | 说明 |
|---|---|---|
| `-c` | 是 | 配置文件（YAML）路径 |
| `-mode` | 是 | `check` 或 `delete`，**不给默认值**——模式选择是正确性关键，按"校验类参数默认零值+强制显式配置"原则不设默认；不传或传别的值启动即报错退出 |
| `-input` | 是 | 输入文件路径。通常是 lister 的 `delete_markers.txt` 或 `invalid_keys.txt`。文件名 basename 是 `invalid_keys.txt` 时自动对 key 字段做 `url.QueryUnescape`（兼容 lister 的兜底分流） |

**桶名从每行第一字段读取**——lister 已把 `bkt` 写进每行，再加 `-bkt` flag 做一致性校验只是冗余，不加。

worker 数从配置文件 `op_concurrency` 读，**不设命令行 flag**——对齐 lister 的 `list_concurrency`（lister 也是只配 config，无 flag 覆盖）。未配置时默认 8。

配置文件可直接复用 lister 或 chunked_check_tool 的 config.yaml：本工具只读自己认识的字段，未知字段被 yaml.v3 忽略。

无其他 flag：无 `-bkt`、无 `-concurrency`、无 `-resume`、无 `-yes`、无 `-dry-run`。

## 3. 两种 mode 详解

### mode check（HEAD 校验）

N 个 worker 从输入文件行池抢任务，每个 worker 各有一个 `*S3Client`（绑 `endpoints[i % len(endpoints)]`）。

对每行 `(bkt, key, version_id)`：

1. 调 `minio.Client.StatObject(ctx, bkt, key, minio.StatObjectOptions{VersionID: version_id})`。
2. 按返回分类三态：

   | 状态码 | 状态 | 含义 |
   |---|---|---|
   | `(ObjectInfo{IsDeleteMarker: true}, nil)` 或 minio-go 把 405 + `x-amz-delete-marker: true` 包装成 `errorResponse.Code == "MethodNotAllowed"` 且 ObjectInfo.IsDeleteMarker=true | `marker` | 存在且是 delete marker（lister 正确） |
   | `(ObjectInfo{IsDeleteMarker: false}, nil)` | `not_marker` | 存在但不是 marker（lister 误判——这是审计信号） |
   | `error` 且 `errorResponse.Code ∈ {"NoSuchKey", "NoSuchVersion"}` | `absent` | 该版本不存在 |
   | 其他 `error`（5xx / 传输层 / 未识别 4xx） | **失败**，不分类，走 `head_failed.txt` + `head_failed.log` | HEAD 调用本身失败 |

   > **已知实现未知**：自研 S3 对 HEAD 一个 delete marker 版本返回什么——S3 规范是 `405 Method Not Allowed + x-amz-delete-marker: true` 头，minio-go v7.3.0 应转译成 `ObjectInfo{IsDeleteMarker: true}`（可能伴随 `MethodNotAllowed` error，也可能 nil）。**实现时必须先用 fakes3 + 真实桶分别验证**：若 minio-go 把它当 `MethodNotAllowed` error 返回，需要从 `minio.ErrorResponse` 里取 `StatusCode == 405` 判定为 `marker`。设计层不假定具体形态。

3. 成功三态写入 `check_results.txt`，行格式 `bkt\x1fkey\x1fversion_id\x1f<state>`（四段，`\x1f` 分隔）。
4. 失败写入 `head_failed.txt`（裸原行 `bkt\x1fkey\x1fversion_id`，惰性 open）+ `head_failed.log`（slog：bucket、key、version_id、http_code、s3_code、req_id、err）。

### mode delete（删除 delete marker）

N 个 worker 同上。

对每行 `(bkt, key, version_id)`：

1. 调 `minio.Client.RemoveObject(ctx, bkt, key, minio.RemoveObjectOptions{VersionID: version_id})`。
2. 按返回分类：

   | 返回 | 状态 | 写入 |
   |---|---|---|
   | `nil` | 成功 | `deleted.txt` 一行 `bkt\x1fkey\x1fversion_id` |
   | `error` 且 `errorResponse.Code == "NoSuchVersion"` | **成功**（用户决策） | `deleted.txt` 一行 |
   | 其他 `error`（5xx / 传输层 / 其他 4xx） | 失败 | `delete_failed.txt`（裸原行，惰性 open） + `delete_failed.log`（slog：bucket、key、version_id、http_code、s3_code、req_id、err） |

> **delete marker 上 DeleteObject 的语义**：带 VersionId 删除 delete marker = 物理删除该 marker 版本。若该 marker 是当前 latest 版本，删除后前一版本变为 latest（对象"恢复"——正是清理 delete marker 的预期效果）。若 lister 误判（version 实际是普通版本），mode delete 会删掉一个普通版本——这正是用户工作流要在 mode delete 前先跑 mode check 的原因。**mode delete 不做任何二次校验**。

## 4. 正确性要点

### 4.1 直接信任 lister 输出

mode 2 不调用 mode 1、不重新 HEAD、不验证 `IsDeleteMarker`——按用户工作流，校验在前后两次 mode check 完成。理由：
- lister 已经过 `isCommonPrefix` 四条件合取判别（详见 lister 设计 §4.2），把普通版本误判为 marker 概率极低；
- 真正的兜底是 mode check：用户在 mode delete 前跑 mode check 看 `not_marker` 一类是否为 0。

### 4.2 version_id 字面量 `"null"` 原样透传

lister 把 S3 开版本控制前的旧版本对象的 VersionId（字面量 `"null"`）原样写入结果文件。本工具**原样透传**给 minio-go 的 `VersionID` option，不做"是否为 null 字符串"特殊处理——对 S3 而言，`VersionId: "null"` 是合法值，指代那个 null 版本。

### 4.3 桶名从每行读，不做一致性校验

`bkt` 字段直接从输入行解析。不强制所有行 bkt 一致——支持把多个桶的 `delete_markers.txt` `cat` 起来一次跑。每行的 bkt 决定该次 HEAD/DELETE 的 bucket。

### 4.4 输入文件按 basename 自动识别 URL 编码

- basename == `invalid_keys.txt` → key 字段做 `url.QueryUnescape` 还原（lister 的兜底分流文件，key 被编码过）；
- 其他 basename（含 `delete_markers.txt`）→ key 原样使用。

不做 `-input-format` flag——文件名约定足够，flag 是冗余。

### 4.5 行格式契约：四段 `\x1f` 分隔

mode check 的 `check_results.txt` 行格式 `bkt\x1fkey\x1fversion_id\x1f<state>`（四段，比 lister 的三段多一段 state）。
mode delete 的 `deleted.txt` / `delete_failed.txt` 行格式 `bkt\x1fkey\x1fversion_id`（三段，与 lister 完全一致，便于 `diff` 比对）。

`<state>` 取值固定为 `marker` / `not_marker` / `absent`，禁止扩展——下游解析按 `strings.Split(line, "\x1f")` 取第四段，固定三个字面量。

### 4.6 key 含 `\x1f` 的兜底分流（继承 lister）

若从 `invalid_keys.txt` URL 解码后的 key 仍含 `\x1f`（理论不可能，URL 编码会消除），或在 `delete_markers.txt` 中本就含 `\x1f`（lister 的兜底分流保证不会出现），跳过该行写入 `head_failed.txt` / `delete_failed.txt` 并在 log 里标 `err="key contains row separator"`。不做二次 URL 编码——输入侧已编码过，再编码无意义。

## 5. 失败处理：无重试、无断点

与 lister 完全一致：
- 单行 HEAD/DELETE 失败（5xx / 传输层 / 未识别 4xx）→ 写 `head_failed.txt` / `delete_failed.txt` + slog 详情，继续处理其他行，**不自动重试**。
- mode check 的 `absent` 状态不算失败（404 NoSuchKey 是预期响应）。
- mode delete 的 `NoSuchVersion` 不算失败（用户决策）。
- 补齐方式：整轮重跑。`head_failed.txt` / `delete_failed.txt` 文件不存在即无失败。
- 跨运行并集：设 `output_dir_timestamp: false` 同目录追加，结果文件跨运行合并。mode check 的 `check_results.txt` 跨运行追加同一行可能因状态变化（如先 `marker` 后 `absent`）产生重复行——下游解析以最后一行为准（或人工 `sort -u`）；mode delete 的 `deleted.txt` 重复行无害（删除按 version-id 幂等）。
- 无 journal、无 `-resume-*`、无优雅退出：Ctrl-C / kill -9 直接终止进程。已 flush 的结果以整行为单位存续，丢的是尾部若干整行。

## 6. 输出文件

目录 = `output_dir`（经时间戳后缀规则改写，同级后缀 `<dir>_<YYYYMMDD_HHMMSS>`，与 lister 完全一致）。

### mode check 输出

| 文件 | 内容 | 行格式 |
|---|---|---|
| `check_results.txt` | **结果文件**，每行一个三态分类 | `bkt\x1fkey\x1fversion_id\x1f<state>` 四段，`<state>` ∈ {`marker`, `not_marker`, `absent`} |
| `head_failed.txt` | HEAD 调用失败的行 | `bkt\x1fkey\x1fversion_id` 三段，惰性创建 |
| `head_failed.log` | 失败详情 | slog text：bucket、key、version_id、http_code、s3_code、req_id、err |
| `run.log` | 进程日志：stdout/stderr tee、配置快照（sk 掩码）、`[progress]` 行、`=== summary ===` | 纯文本 |

### mode delete 输出

| 文件 | 内容 | 行格式 |
|---|---|---|
| `deleted.txt` | **结果文件**，删除成功（含 NoSuchVersion）的行 | `bkt\x1fkey\x1fversion_id` 三段 |
| `delete_failed.txt` | 删除失败的行 | `bkt\x1fkey\x1fversion_id` 三段，惰性创建 |
| `delete_failed.log` | 失败详情 | slog text：bucket、key、version_id、http_code、s3_code、req_id、err |
| `run.log` | 同上 | 纯文本 |

**失败文件惰性创建**：文件不存在 = 无失败，文件存在 = 至少一次失败。继承 lister 的 `list_failed.txt` 契约（AGENTS.md §4.5）。

写入路径：worker → channel（容量 1024）→ 单 writer goroutine + 64KB bufio，每 3 秒定时 flush 一次、结束时 FlushAndWait（保证 kill -9 时断尾损失有界）。继承 lister 的 `Output` 结构（详见 §10）。

## 7. 配置字段

| 字段 | 类型 | 默认 | 必填 | 说明 |
|---|---|---|---|---|
| `endpoints` | []string | — | 是（≥1） | ip:port 列表；worker 轮询绑 |
| `scheme` | string | `http` | 否 | `https` 时 InsecureSkipVerify |
| `ak` / `sk` | string | — | 是 | 静态签名凭证；sk 在 run.log 掩码 |
| `op_concurrency` | int | `8` | 否 | worker 数；mode check / mode delete 共用 |
| `output_dir` | string | `.` | 否 | |
| `output_dir_timestamp` | bool | `true` | 否 | 同级后缀 `_YYYYMMDD_HHMMSS` |
| `progress_interval` | int | `5000` | 否 | 进度行间隔（处理条目数） |
| `dial_timeout` | int64 | `10` | 否 | 秒 |
| `response_header_timeout` | int64 | `0` | 否 | 秒，0 = 不限 |

HTTP transport 沿用 lister 的调优：`MaxIdleConnsPerHost: 32`、`IdleConnTimeout: 90s`、可选 dial/response-header 超时、https 跳过证书校验、`BucketLookup: minio.BucketLookupPath`。

被砍掉的字段（出现即被忽略）：`list_type`、`list_concurrency`、check/multipart/backup 全家、`is_check`、`is_success_log`、节点隔离/恢复参数、channel 容量参数。

命令行无 `-concurrency` flag——worker 数从 config 的 `op_concurrency` 读，未配置时默认 8（对齐 lister 的 `list_concurrency` 行为）。

## 8. 统计与进度

- 进度行：每处理 `progress_interval` 行输出一行：
  - mode check：`[progress] processed=<n> marker=<n> not_marker=<n> absent=<n> failed=<n>`——三态各列一字段，**`not_marker` 在进度行里可见**（用户工作流"跑 mode check 看 not_marker，非 0 就停手"必须能在运行中看到，不能等 summary）；
  - mode delete：`[progress] processed=<n> deleted=<n> failed=<n>`。
- 汇总（stdout tee 进 run.log）：
  - mode check：`processed`、`marker`、`not_marker`、`absent`、`head_failed`、`total_sec`；
  - mode delete：`processed`、`deleted`（含 NoSuchVersion）、`delete_failed`、`total_sec`。
- **预检契约**（统一 §8 与 §10.7，消除原稿自相矛盾）：
  1. 启动时解析输入文件首行，取 `bkt` 字段。首行解析失败（撕裂/空文件）→ 启动报错退出。
  2. 用 `endpoints[0]` 对首行 bkt 跑一次 `BucketExists`。
     - 传输层错误（dial 失败、5xx、超时）→ **硬退出**：配置或网络炸了，避免空跑整批 all-failures。
     - `exists == false` → log warning 到 run.log，**继续启动**：首行 bucket 可能存在于其他 endpoint，不硬阻塞多桶输入。
  3. 此预检只覆盖首行 bkt；其他行的 bucket 存在性靠 worker HEAD/DELETE 自己暴露（失败走 `head_failed.txt` / `delete_failed.txt`）。

## 9. 非目标（明确不做）

- 不做列举（list 是 lister 的职责）；
- 不做内容校验、不做备份（chunked_check_tool 的职责）；
- mode delete 不做二次校验（不调 HEAD 确认 IsDeleteMarker）；
- 不支持优雅退出（无信号处理器）；
- 不支持断点续跑（无 journal、无 resume flag）；
- 无自动重试、无节点隔离/恢复/重绑；
- 无 `-yes` / `-dry-run` flag（删除是不可逆操作，但用户工作流 mode check → mode delete → mode check 已有多次人工断点；与 lister 的"无重试无断点"哲学一致）；
- 不输出 ok 列表（只关心 marker 处理结果）；
- 不做 per-owner 分目录、不做行格式模板。

## 10. 设计决定（我拍板的，用户可否决）

1. **工具名 `delete_marker_cleanup_tool`**：cleanup = 清理（含 check 审计 + delete 清理两种方式）。备选 `handle_tool`、`op_tool`。选 `cleanup` 因为最贴近"清理 delete marker"的语义；
2. **模式走 flag 不走 config**：用户决策（用户原话："两种模式通过命令的参数来控制"）。`-mode check|delete` 必填，无默认值——模式选择是正确性关键，按"校验类参数默认零值+强制显式配置"原则不设默认；
3. **桶名从每行读**，不加 `-bkt` flag——lister 已把 bkt 写进每行，再加强校验只是冗余；
4. **输入文件按 basename 自动识别 URL 编码**——约定优于 flag，少一个 `-input-format` flag；
5. **mode delete 的 `NoSuchVersion` 算成功**——用户决策（幂等空操作不噪音化）；
6. **mode check 三态输出**：`marker` / `not_marker` / `absent`——三态能直接暴露 lister 的 `isCommonPrefix` 误判（`not_marker` 一类），二态（ok/bad）会掩盖"lister 把真版本误判成 marker"这种问题；
7. **`BucketExists` 预检契约**：解析输入文件首行取 `bkt`，用 `endpoints[0]` 跑一次 `BucketExists`——传输错误硬退出（配置/网络炸了），`exists==false` 只 log warning + 继续（首行 bucket 可能存在于其他 endpoint，保多桶输入能用）。详见 §8 末尾；
8. **3 秒定时 flush** 保留（数据落盘节奏，不属于优雅退出范畴）；
9. **Output 结构复用 lister 的设计**：writer goroutine + channel + bufio + 惰性失败文件——同 lister `Output`（output.go）的形态，只是文件名和行格式按 mode 切换。具体复用方式见 §11；
10. **不重试、不断点、不优雅退出**：完全继承 lister §5 的哲学。

## 11. 与 lister 的复用关系

| 模块 | 复用方式 |
|---|---|
| `config.go` | 复制改字段：砍 `list_type` / `list_concurrency`，加 `op_concurrency`；timestamp 后缀逻辑、YAML 解析、校验模式照搬 |
| `s3client.go` | 复制改方法：保留 `NewS3Client` / `BucketExists`，加 `StatObject(ctx, bkt, key, versionID) (ObjectInfo, error)` 和 `DeleteObject(ctx, bkt, key, versionID) error`；transport 调优照搬 |
| `output.go` | 复制改 channel 类型 + 文件名：从 `[2]string` 改为承载完整行内容（state / 错误信息）；writer goroutine + 惰性失败文件 + 3 秒 flush 全照搬；按 mode 在 NewOutput 时切换打开哪些文件 |
| `stats.go` | 复制改字段：mode check = `marker` / `not_marker` / `absent` / `head_failed`；mode delete = `deleted` / `delete_failed`；`total_sec` 照搬 |
| `progress.go` | 复制不改：`ProgressPrinter` + per-worker 本地计数 + 阈值打印；`QueueSnapshot` 砍掉（无 BFS 队列） |
| `queue.go` | **不复用**：本工具无 BFS 队列，单生产者（读输入文件）多消费者（worker pool）用 buffered channel 即可 |
| `lister.go` | **不复用**：换成 `worker.go`（worker pool + per-worker S3Client + input row channel） |
| `main.go` | 复制改 flag：`-c` / `-mode` / `-input` / `-concurrency`；run.log 第一个 open、配置快照（sk 掩码）、BucketExists 预检、summary 照搬 |

## 12. 已知限制

- **HEAD-on-delete-marker 行为未验证**：S3 规范是 405 + `x-amz-delete-marker: true` 头，minio-go v7.3.0 的转译形态需在实现时用 fakes3 + 真实桶验证。若 minio-go 把它当 `MethodNotAllowed` error 返回，需从 `minio.ErrorResponse.StatusCode == 405` 判定为 `marker` 状态。详见 §3 mode check；
- **key 含 `\n`** 会撕裂输入行解析（chunked_check_tool / lister 同样不防护，S3 key 含换行极罕见）；
- **kill -9** 丢尾部整行无提示（run.log 里 summary 不会打出来）；靠 `head_failed.txt` / `delete_failed.txt` 为空 + 行数稳定来人工确认完整性；
- **同秒内连续启动两次**（`output_dir_timestamp: true`）会撞目录并发 append 撕裂尾部行；靠下游解析时丢弃格式非法行兜底；
- **mode delete 误删普通版本**：若 lister 把普通版本误判为 marker，mode delete 会删掉一个普通版本——用户工作流 mode check 在前可暴露 `not_marker` 一类，避免误删。**本工具不保护这种情况**；
- **mode check 跨运行同 bkt/key/version_id 行追加**：状态可能变化（先 `marker` 后 `absent`），下游解析以最后一行为准，或人工 `tac | sort -u` 去重。

## 13. 建议目录结构（留给实现者微调）

单包 `main`：
- `main.go`：flag 解析（`-c` / `-mode` / `-input` / `-concurrency`）、`run` 编排、run.log 打开、配置快照、BucketExists 预检、summary 打印。无信号处理器。
- `config.go`：`Config` + `LoadConfig`（YAML + 默认 + 校验 + 时间戳后缀）。
- `s3client.go`：`S3Client`（minio.Client 包装）、`StatObject` / `DeleteObject` / `BucketExists`。transport 调优照搬 lister。
- `worker.go`：worker pool，每个 worker 一个 `*S3Client`，从 `rowCh` 读行、调对应 op、把结果送进 `Output`。
- `output.go`：`Output`（writer goroutine + channel + bufio + 惰性失败文件 + 3 秒 flush），按 mode 切换打开的文件。
- `stats.go`：atomic.Int64 计数器 + snapshot + summary。
- `progress.go`：`ProgressPrinter`（per-worker 本地计数 + 阈值打印）。

测试参照 lister 的 fakes3 模式：mode check 三态各一个端到端用例、mode delete（含 NoSuchVersion → 成功）一个端到端用例、HEAD/DELETE 失败路径用例、`\x1f`-in-key 路由用例、行格式撕裂用例。
