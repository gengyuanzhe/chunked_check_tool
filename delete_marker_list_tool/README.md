# delete_marker_list_tool

枚举多版本桶内所有 delete marker（删除标记），输出机器可读列表供下游删除工具消费。

## 作用

多版本桶每次"无版本号删除"（即 `DeleteObject` 不带 `VersionId`）都会留下一个 delete marker——S3 里的一种特殊版本，记录"某时刻某 key 被删除了"。长期运行后桶里会积累大量 marker，需要全量清理。

本工具扫全桶多版本列举，把所有 delete marker 落盘成 `delete_markers.txt`，后续由另一个独立工具读取该文件，逐条执行 `DeleteObject(bucket, key, VersionId)` 完成真正删除。**本工具只读不删**。

## 编译

```bash
# 本机
go build -o delete_marker_list_tool .

# Linux amd64（交叉编译，目标机不需要 Go）
GOOS=linux GOARCH=amd64 go build -o delete_marker_list_amd64 .

# Linux arm64
GOOS=linux GOARCH=arm64 go build -o delete_marker_list_arm64 .
```

生成的二进制可直接 scp 到 Linux 服务器运行，不需要目标机安装 Go 或运行时依赖（静态编译）。

## 运行命令

```bash
# 基本用法（默认 mode 2 BFS 并发扫描整个桶）
./delete_marker_list_tool -c config.yaml -bkt mybucket

# 限定前缀
./delete_marker_list_tool -c config.yaml -bkt mybucket -prefix data/2026/

# mode 1 单流递归（小桶/调试用，config 里设 list_type: 1）
./delete_marker_list_tool -c config.yaml -bkt mybucket

# 补齐场景：同目录追加跑多轮，结果自动并集
# config 里设 output_dir_timestamp: false
./delete_marker_list_tool -c config.yaml -bkt mybucket
# （上一轮的 list_failed.txt 手动清理后重跑，结果文件追加）
```

## 命令参数

| flag | 必填 | 说明 |
|---|---|---|
| `-c` | 是 | 配置文件路径（YAML） |
| `-bkt` | 是 | 桶名 |
| `-prefix` | 否 | 列举起点前缀，默认空（整个桶） |

无其他 flag。不支持 `-nextmarker`、`-check-file`、`-list-file`、`-backup-file`、`-resume-list`、`-resume-journal`（这些是 chunked_check_tool 的文件输入/断点续跑 flag，本工具刻意砍掉）。

## 配置

`config.yaml` 示例：

```yaml
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http                        # http 或 https（后者忽略证书校验）
ak: <access-key>
sk: <secret-key>
list_type: 2                        # 1=单流递归, 2=BFS delimiter 并发（必须显式设，不给默认值）
list_concurrency: 8                 # mode 2 的 worker 数；mode 1 忽略
output_dir: ./out                   # 输出目录
output_dir_timestamp: true         # true=output_dir 追加 _YYYYMMDD_HHMMSS 同级后缀（./out → ./out_20260908_175201）隔离每次运行; false=固定用 output_dir
progress_interval: 5000             # 进度行间隔（处理条目数）
dial_timeout: 10                    # TCP 建连超时秒数（黑洞 IP 默认 10s 兜底）
response_header_timeout: 0           # 等服务端响应头超时秒数（0=不限）
```

| 字段 | 类型 | 默认 | 必填 | 说明 |
|---|---|---|---|---|
| `endpoints` | []string | — | 是（≥1） | ip:port 列表（mode 2 worker 轮询绑） |
| `scheme` | string | `http` | 否 | `https` 时 `InsecureSkipVerify=true` |
| `ak` / `sk` | string | — | 是 | 静态签名凭证；sk 在 run.log 掩码为 `***` |
| `list_type` | int | — | 是 | **必须显式设**：1=单流递归，2=BFS delimiter 并发；不给默认值（正确性关键） |
| `list_concurrency` | int | `8` | 否 | mode 2 的 worker 数；mode 1 忽略 |
| `output_dir` | string | `.` | 否 | 输出目录 |
| `output_dir_timestamp` | bool | `true` | 否 | true=追加同级时间戳后缀隔离运行 |
| `progress_interval` | int | `5000` | 否 | 进度行间隔（处理条目数） |
| `dial_timeout` | int64 | `10` | 否 | TCP 建连超时秒数 |
| `response_header_timeout` | int64 | `0` | 否 | 等响应头超时秒数；0=不限 |

**可直接复用 chunked_check_tool 的 config.yaml**——本工具只读自己认识的字段，未识别字段（`is_check`、`multipart_*`、`backup_*`、`check_concurrency` 等）被 YAML 解析器忽略，不报错。

## 输出文件

| 文件 | 内容 | 行格式 |
|---|---|---|
| `delete_markers.txt` | **结果文件**，全桶 delete marker | `bkt\x1fkey\x1fversion_id`（`\x1f` 分隔，三段） |
| `invalid_keys.txt` | key 含 `\x1f` 的 marker（兜底，正常桶该文件为空） | `bkt\x1f<urlencoded-key>\x1fversion_id`（key 经 `url.QueryEscape` 编码） |
| `run.log` | 进程日志：stdout/stderr tee、配置快照（sk 掩码）、进度行、summary | 纯文本 |
| `list_failed.txt` | 列举失败的前缀（惰性创建：文件不存在=无失败） | 裸前缀一行；空前缀=空行 |
| `list_failed.log` | 失败详情（slog text） | `bucket`/`prefix`/`http_code`/`s3_code`/`req_id`/`err` |

**行分隔符 `\x1f`**（ASCII Unit Separator，控制字符）：S3 key 规范上允许 `|`，实务里偶有出现；`\x1f` 是控制字符，key 中基本不出现，撞概率比 `|` 低一两个数量级。`\x1f` 在终端不可见，查看用：

```bash
awk -F'\x1f' '{print $1, $2, $3}' delete_markers.txt
# 或
less delete_markers.txt   # 看着字段黏在一起，但数据完整
```

**version_id 可能是字面量 `"null"`**（旧版本对象，S3 开版本控制之前写入的）；下游删除工具必须**原样透传**，不要当空值处理。

## 两种 mode 详解

### mode 1：单并发多版本列举（`list_type: 1`）

单个 goroutine 调 `ListObjectVersions(Prefix: bucket, Recursive: true)`，AWS SDK 把 `<Version>` / `<DeleteMarker>` 解析到独立 slice，逐条过滤 `IsDeleteMarker == true` 写结果文件。

- 适合小桶或调试
- 无并发，大桶会慢
- 流中断直接返回 error（单流，失败即终止）

### mode 2：基于 delimiter 并发多版本列举（`list_type: 2`，默认推荐）

广度优先扫描（BFS）：

1. 队列种子 = `-prefix` 值（默认空串=全桶）
2. `list_concurrency` 个 worker 从队列取前缀
3. 每个前缀列举时带 `/` delimiter，流里得到两类条目：
   - 该前缀直接子 key 的所有版本（含 delete marker）
   - 更深一层的公共前缀（common prefix，形如 `photos/2024/`），入队继续扫
4. 终止：队列空 + 所有 in-flight 前缀处理完

- 适合大桶并发扫描
- 单前缀失败只记录不终止（写 `list_failed.txt` + `list_failed.log`），其他前缀继续扫
- 已扫出的 marker 保留，失败靠整轮重跑补齐

## 失败处理

**无重试、无断点、无优雅退出**：

- 某前缀列举失败（HTTP 5xx / 传输层错误）→ 该前缀记入 `list_failed.txt`（一行=一个失败前缀）+ `list_failed.log`（slog 详情含 HTTP code / S3 error code / req_id），其他前缀继续扫，已扫出的 marker 保留
- mode 1 单流中断 → 整个 `-prefix` 范围记一条失败，进程返回 error
- 补齐方式：整轮重跑。`list_failed.txt` 为空（或文件不存在）即全桶扫完
- 多轮结果并集：设 `output_dir_timestamp: false` 同目录追加跑，结果文件跨运行自动合并（重复行无害——下游删除按 version-id 幂等，删两次同一版本第二次空操作）
- Ctrl-C / kill -9 直接终止进程；已 flush 的结果以整行为单位存续，丢的是尾部若干整行

## 已知限制

- **key 含 `\n`** 会撕裂结果行（chunked_check_tool 同样不防护，S3 key 含换行极罕见）
- **kill -9** 丢尾部整行无提示（run.log 里 summary 不会打出来）；靠 `list_failed.txt` 为空 + 行数稳定来人工确认完整性
- **mode 1 无并发**，大桶会慢——这正是该模式的语义
- **同秒内连续启动两次**（`output_dir_timestamp: true`）会撞目录并发 append 撕裂尾部行；靠下游解析时丢弃格式非法行兜底
- **version_id 可能是字面量 `"null"`**（旧版本对象）；下游透传不要当空值
- **`scanned_versions` 在 mode 2 对"以 / 结尾的目录 key"少统计**——已知偏差，不修。自研 S3 有 quirk：`delimiter=/` 列举时以 / 结尾的 key（如 `97-abc-0/`）被折叠到 CommonPrefixes，但 `prefix=97-abc-0/` + `delimiter=/` 再列举时不返回该 key 本身作为 Version。mode 2 的 BFS re-list 踩在这个 quirk 上，目录 key 本身的 Version 计数被丢；mode 1 走 recursive 不分 shard，无此问题。
  - **delete_markers.txt 完整正确**，不受影响——`<DeleteMarker>` 是独立 XML 标签，不走 CommonPrefix 路径。`scanned_versions` 只是诊断指标，不是产出。
  - **不修的原因**：修复需 CommonPrefix re-list 改用 recursive，会失去 shard 内部多级细分（并发度下降）。诊断指标偏差不值得牺牲并发。两个模式统计值有差异时以 mode 1 为准，或只看 delete_markers.txt 完整性。

## 约束

- **endpoint 是纯 `host:port`**（如 `10.0.0.1:9000`），**不带 scheme**——scheme 由 config 的 `scheme` 字段（http/https）单一决定，endpoint 不允许自带 scheme 覆盖
- **scheme 只能是 `http` 或 `https`**，其他值启动即报错
- **`list_type` 必须显式配置**（1 或 2），不给默认值——模式选择是正确性关键
- **op_concurrency / list_concurrency** 默认 8，最小 1，0 取默认，负数报错
- **无重试无断点无优雅退出**：失败靠整轮重跑，靠 `list_failed.txt` 为空确认完整性

## 相关文档

- 设计文档：`docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md`
- 实现计划：`docs/superpowers/plans/2026-11-12-delete-marker-list-tool.md`
- AGENTS.md：`AGENTS.md`（设计约束、不变量、遗留任务）
- chunked_check_tool（约定参考）：`../chunked_check_tool/`
