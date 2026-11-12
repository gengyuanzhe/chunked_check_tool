# delete_marker_list_tool 设计文档（2026-11-12）

## 1. 意图与背景

多版本桶里，每次对对象的"无版本号删除"都会留下一个 delete marker（删除标记，S3 里的一种特殊版本，记录"某时刻谁删除了这个 key"）。长期运行后桶里会积累大量 marker，需要全量清理。

本工具是清理的第一步——侦察：把桶内**所有** delete marker 枚举出来，落盘成机器可读的列表文件。后续由另一个独立工具读取该列表，逐条执行 `DeleteObject(bucket, key, VersionId)` 完成真正的删除。本工具**只读不删**。

用法、配置、输出约定继承自 chunked_check_tool，但刻意做小：不做内容校验、不做备份、不支持断点续跑、不支持优雅退出，只保留列举一个功能。

## 2. 用法与形态

- 位置：仓库根目录下的兄弟模块 `delete_marker_list_tool/`（与 chunked_check_tool、list_only 平级），独立 go.mod。
- 依赖：`github.com/minio/minio-go/v7 v7.3.0`，与 chunked_check_tool 同版本，**锁定不升级**（channel API 的行为契约按 v7.3.0 校准；升级需回归验证）。
- 命令行（与 chunked_check_tool 一致的最小集）：

```
./delete_marker_list_tool -c config.yaml -bkt mybucket [-prefix photos/]
```

| flag | 必填 | 说明 |
|---|---|---|
| `-c` | 是 | 配置文件（YAML）路径 |
| `-bkt` | 是 | 桶名 |
| `-prefix` | 否 | 列举起点前缀，默认空 = 全桶 |

- 配置文件可直接复用现有 chunked_check_tool 的 config.yaml：本工具只读取自己认识的字段，未知字段忽略（yaml.v3 默认行为）。
- 无其他 flag：没有五种文件输入模式、没有 -nextmarker、没有 resume。

## 3. 两种列举 mode（配置 `list_type`）

### mode 1：单并发多版本列举（`list_type: 1`）

单个 goroutine 消费一个 channel：

```
minio Client.ListObjects(ctx, bkt, ListObjectsOptions{
    Prefix: <-prefix 值>, Recursive: true, WithVersions: true,
})
```

`Recursive: true` 表示不带 delimiter（分隔符，S3 用它把 key 按 `/` 分组），流里会按字典序吐出**所有 key 的所有版本**。逐条过滤 `IsDeleteMarker == true` 写结果文件。`list_concurrency` 在此模式下忽略。

### mode 2：基于 delimiter 并发多版本列举（`list_type: 2`，默认值）

广度优先扫描（BFS），结构与 chunked_check_tool 的 mode 2 一致，只是列举调用换成带版本的：

1. 队列种子 = `-prefix` 值（默认空串）。
2. `list_concurrency` 个 worker 从队列取前缀，每个 worker 绑定一个 endpoint（`endpoints` 轮询分配，用原子计数器取模，不引入 NodePool）。
3. 每个前缀列举时 `Recursive: false`（即 delimiter = `/`），流里得到两类条目：
   - 该前缀**直接子 key 的所有版本**（含 delete marker）；
   - **更深一层的公共前缀**（common prefix，形如 `photos/2024/`），入队继续扫。
4. 终止不变量（照搬 chunked_check_tool 的教训）：每入队一个新前缀先 `inflight.Add(1)` 再 Push；worker 处理完一个前缀（channel 耗尽或出错）后 `Done(1)`；`inflight.Wait()` 归零即全桶扫完。

## 4. 正确性要点

### 4.1 子前缀的发现必须用版本化列举本身

不能用普通 ListObjects（不带 versions）来发现子前缀：普通列举看不到"所有版本都已被删除、只剩 marker"的对象——这类 key 整体从普通列举里消失，其上层前缀也不会出现在 CommonPrefixes 里。用普通列举发现前缀会**漏掉整片只剩 marker 的目录**，对"枚举所有 marker"的工具是不可接受的正确性漏洞。

带 delimiter 的版本化列举没有这个问题：marker 本身也是版本，参与 CommonPrefixes 计算。

### 4.2 公共前缀的判别规则

channel API 把公共前缀混在版本条目流里吐出，形如 `ObjectInfo{Key: "photos/2024/"}`（其余字段全空）。判别规则：

```
VersionID == "" && !IsDeleteMarker && ETag == "" && strings.HasSuffix(Key, "/")
```

满足即公共前缀，否则按版本条目处理。理由与失败模式分析：

- 真版本条目的 VersionID 非空——包括"null 版本"（开版本控制之前写入的旧版本），S3 规定它返回字面量字符串 `"null"`；
- 若自研服务器对 null 版本返回**空** VersionId，后三个条件仍能兜住（真对象有 ETag，marker 有 IsDeleteMarker）；
- 判别错误的代价不对称：把版本条目误判为前缀会把它入队重扫，极端情况（VersionID 空 + ETag 空 + IsDeleteMarker 假 + key 以 `/` 结尾）甚至死循环；把前缀误判为版本条目则只是**漏扫一个子目录**。四条件合取下前者基本不可能，用表驱动测试钉死。

### 4.3 结果唯一性

同一 (key, version_id) 可能重复出现（同一前缀流被重跑、或列举本身无重试导致用户整轮重跑）。结果文件是**追加并集**模型：重复行无害，因为下游删除按 version-id 幂等（删两次同一版本，第二次空操作）。

### 4.4 list_failed.txt 文件契约

- 文件**惰性创建**：仅在第一次失败发生时 open（`O_CREATE|O_APPEND`），文件存在即至少一次失败。`wc -l` = 失败数。
- 每行 = 一个失败前缀，作为下次重跑的 `-prefix` 值直接透传。
- 空前缀失败 → 文件里有且仅有一个空行；与"无失败"（文件根本不存在）明确可区分。
- mode 1 / mode 2 同一份契约：mode 1 失败时写入的"前缀"就是用户传入的 `-prefix`（可能为空）。

### 4.5 同秒 output_dir_timestamp 冲突

- 默认 `output_dir_timestamp: true` 下，时间戳精度到秒；同秒内连续启动两次会落到同一目录，两个进程并发 append 同一 `delete_markers.txt`，可能撕裂尾部行。
- 处理方式：**不做检测，接受极小概率**。理由：(a) 用户手动运行，同秒连发两次的场景几乎不存在；(b) timestamp=false 的補齊场景**需要** append，做"目录已存在即拒绝"会破坏该用法；(c) 撕裂行的下游影响有界——下游删除工具读取时按 `bkt|key|version_id` 三段解析，撕裂行格式非法会被丢进自身的 `invalid_keys.txt`，不会误删。
- 已知限制写入 §11。

## 5. 失败处理：无重试、无断点

- mode 2：某前缀的 channel 吐出 `ObjectInfo{Err}`（传输层/HTTP 错误）→ 该前缀记入 `list_failed.txt`（一行 = 一个失败前缀，裸前缀，空前缀=空行）+ `list_failed.log`（slog：bucket、prefix、HTTP code、S3 error code、错误详情，取自 `minio.ErrorResponse`）。该前缀已吐出的部分结果保留。继续扫其他前缀，**不自动重试**。
- mode 1：单流中断 → 整个 `-prefix` 范围记一条失败。
- 补齐方式：整轮重跑。结果文件跨运行合并有两种用法：
  - 默认 `output_dir_timestamp: true`：每次运行输出到新目录 `<dir>_<时间戳>`，运行之间互相隔离；删除工具需要吃多个目录；
  - 补齐场景建议设 `output_dir_timestamp: false`：固定路径，同目录追加，新旧结果自然并集。
- 无 journal、无 `-resume-*`、无优雅退出：Ctrl-C / kill -9 直接终止进程。已 flush 的结果以**整行**为单位存续（单 writer 串行写行，flush 边界即行边界，不会出现半行）；进程被杀时丢的是尾部若干整行。

## 6. 输出文件

目录 = `output_dir`（经时间戳后缀规则改写，同级后缀 `<dir>_<YYYYMMDD_HHMMSS>`，与 chunked_check_tool 完全一致）。

| 文件 | 内容 | 行格式 |
|---|---|---|
| `delete_markers.txt` | **结果文件**，全桶 delete marker | `bkt\x1fkey\x1fversion_id` 固定三段，机器契约，`\x1f` 分隔 |
| `run.log` | 进程日志：stdout/stderr tee、配置快照（sk 掩码为 `***`）、`[progress]` 行、`=== summary ===` | 纯文本 |
| `list_failed.txt` | 列举失败的前缀 | 裸前缀一行；空前缀 = 空行（详见 §4.4） |
| `list_failed.log` | 失败详情 | slog text |
| `invalid_keys.txt` | key 含 `\x1f` 的 marker（对该 key 做 URL 编码后写入） | `bkt\x1f<urlencoded-key>\x1fversion_id`（详见 §13） |

写入路径：worker → channel（容量 1024）→ 单 writer goroutine + 64KB bufio，每 3 秒定时 flush 一次、结束时 FlushAndWait（保证 kill -9 时断尾损失有界）。

version_id 可能是字面量 `"null"`（旧版本对象）；下游删除工具必须**原样透传**，不要当空值处理。

## 7. 配置字段（继承子集）

| 字段 | 类型 | 默认 | 必填 | 说明 |
|---|---|---|---|---|
| `endpoints` | []string | — | 是（≥1） | ip:port 列表 |
| `scheme` | string | `http` | 否 | `https` 时 InsecureSkipVerify |
| `ak` / `sk` | string | — | 是 | 静态签名凭证；sk 在 run.log 掩码 |
| `list_type` | int | `2` | 否 | 只接受 1 或 2，其他值启动时报错退出 |
| `list_concurrency` | int | `8` | 否 | mode 2 的 worker 数；mode 1 忽略 |
| `output_dir` | string | `.` | 否 | |
| `output_dir_timestamp` | bool | `true` | 否 | |
| `progress_interval` | int | `5000` | 否 | 进度行间隔（处理条目数） |
| `dial_timeout` | int64 | `10` | 否 | 秒 |
| `response_header_timeout` | int64 | `0` | 否 | 秒，0 = 不限 |

HTTP transport 沿用 chunked_check_tool 的调优：`MaxIdleConnsPerHost: 32`、`IdleConnTimeout: 90s`、可选 dial/response-header 超时、https 跳过证书校验。

被砍掉的字段（出现即被忽略）：check/multipart/backup 全家、`check_concurrency`、`is_check`、`is_success_log`、`list_api_version`（版本列举无 V1/V2 之分）、`result_line_format`、节点隔离/恢复参数、channel 容量参数。

## 8. 统计与进度

- 进度行：每处理 `progress_interval` 个版本条目输出一行 `[progress] scanned=<n> markers=<n> [q=<队列长度> inflight=<n>]`（q 字段仅 mode 2）。
- 汇总（stdout tee 进 run.log）：`scanned_versions`、`delete_markers`、`list_failed`、`total_sec`。
- 启动时做一次 `BucketExists`（用 endpoints[0]），桶不存在/不可达直接报错退出。

## 9. 非目标（明确不做）

- 不做任何删除、校验、备份、内容探测；
- 不支持优雅退出（无信号处理器）；
- 不支持断点续跑（无 journal、无 resume flag、无 NextMarker 接口）；
- 无自动重试、无节点隔离/恢复/重绑（与 chunked_check_tool 的行为差异点：靠整轮重跑补齐）；
- 结果文件不做 per-owner 分目录、不做行格式模板；
- 不输出 ok 列表（只关心 marker）。

## 10. 设计决定（我拍板的，用户可否决）

1. 工具名 `delete_marker_list_tool`，兄弟模块独立 go.mod——目录里名字即二进制名，与 list_only 等一致；
2. 行格式固定 `bkt\x1fkey\x1fversion_id`，不引入 result_line_format（下游是机器，固定契约最稳；`\x1f` 分隔避免 key 含 `|` 的漏删问题，详见 §13）；
3. `list_type` 默认 2（与 chunked_check_tool 默认一致，mode 2 是通用场景）；
4. mode 2 worker 绑 endpoint 用原子轮询而非 NodePool（砍掉整套节点管理，失败靠重跑）；
5. 3 秒定时 flush 保留（数据落盘节奏，不属于优雅退出范畴）；
6. 启动 BucketExists 预检（一次调用，防桶名手滑）。

## 11. 已知限制

- key 含换行符会把结果行切断（chunked_check_tool 同样不防护，S3 key 含 `\n` 极罕见）；
- kill -9 时丢尾部整行、无任何提示（run.log 里 summary 不会打出来）；靠 list_failed 为空 + 行数稳定来人工确认完整性；
- mode 1 无并发，大桶会慢——这正是该模式的语义（用户显式选择）；
- 同秒内连续启动两次（timestamp=true）会撞目录，详见 §4.5；
- key 含 `\x1f` 的 marker 分流到 `invalid_keys.txt`，key 经 URL 编码（详见 §13）；正常桶该文件为空。

## 12. 建议目录结构（留给实现者微调）

单包 `main`：`main.go`（flag/编排）、`config.go`、`s3client.go`（client 构造 + ListObjects 封装 + 判别规则）、`lister.go`（两种 mode 的驱动）、`output.go`（结果/失败文件写入）、`stats.go` + `progress.go`。测试参照 chunked_check_tool 的 fakes3 模式：两种 mode 各一个端到端用例、判别规则表驱动用例、`|` 守卫用例。

## 13. 行分隔符决定

行格式用 `\x1f`（ASCII Unit Separator）作分隔符：`bkt\x1fkey\x1fversion_id`。

理由：
- S3 key 规范允许 `|`、tab、`/` 等字符；`|` 和 tab 在实务 key 中都有出现概率，会撕裂行。
- `\x1f` 是 ASCII 专为字段分隔设计的控制字符，实务 key 中基本不出现，碰撞概率比 `|`/tab 低一两个数量级。
- 下游删除工具解析简单：`strings.Split(line, "\x1f")` 一行解决，无需 URL 编解码。
- 不可见性是调试一次性学习成本——`less` 看着字段黏在一起，但 `awk -F'\x1f' '{print $1,$2,$3}'` 一行查看，或在结果文件同目录写一个 README 说明格式即可。
- URL 编码方案（C）会让 `/` 变 `%2F`，结果文件可读性大幅下降；自定义只编码 `|` 的变体（C'）下游需要配套实现编解码，比 `Split("\x1f")` 复杂。

仍保留 `invalid_keys.txt` 作为兜底：key 含 `\x1f` 时（极罕见，控制字符），对 key 做 `url.QueryEscape` 后写入 `invalid_keys.txt`，行格式 `bkt\x1f<urlencoded-key>\x1fversion_id`。下游删除工具同时读 `delete_markers.txt` + `invalid_keys.txt`，对后者的 key 字段调 `url.QueryUnescape` 还原。

理由：
- `\x1f` 在 S3 key 里基本不出现，概率比 chunked_check_tool 用 `|` 低一两个数量级；
- 极罕见 case 走分流 + URL 编码兜底，确保这种 marker 也下游可删；
- `delete_markers.txt` 主文件保持 `\x1f` 分隔 + 无 URL 编码，主路径下游解析最简单（`strings.Split(line, "\x1f")`）；URL 编码只对 invalid_keys.txt 这一份兜底文件生效。

注意：URL 编码会让 `/` 变 `%2F`、空格变 `+` 等，invalid_keys.txt 这一份文件人眼看着会比较丑，但这是兜底文件，正常桶该文件为空。
