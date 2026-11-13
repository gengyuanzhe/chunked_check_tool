# delete_marker_cleanup_tool

消费 `delete_marker_list_tool` 的输出（`delete_markers.txt`），对每行做两种操作之一：HEAD 验证或 DELETE 删除。两种模式由必填 flag `-mode` 控制。

## 作用

`delete_marker_list_tool` 扫全桶多版本列举出所有 delete marker，落盘成 `bkt\x1fkey\x1fversion_id` 三段行。本工具读这个文件，按 mode 对每行做一次 S3 调用：

- **mode `check`**：`HEAD /<bucket>/<key>?versionId=<vid>`，把每行分类成三态之一：
  - `marker` — 该版本是 delete marker
  - `not_marker` — 该版本存在但不是 delete marker（普通版本）
  - `absent` — 该版本不存在（NoSuchKey / NoSuchVersion）
- **mode `delete`**：`DELETE /<bucket>/<key>?versionId=<vid>`，直接按行里的 version-id 删除；`NoSuchKey` / `NoSuchVersion` 算成功（幂等空操作），其他失败落 `delete_failed.txt`。

**典型工作流**：list → mode check 审计 → mode delete 删除 → mode check 复核全 absent。

## 编译

```bash
# 本机
go build -o delete_marker_cleanup_tool .

# Linux amd64
GOOS=linux GOARCH=amd64 go build -o delete_marker_cleanup_amd64 .

# Linux arm64
GOOS=linux GOARCH=arm64 go build -o delete_marker_cleanup_arm64 .
```

静态编译，可 scp 到无 Go 环境的服务器运行。

## 运行命令

```bash
# 模式 1：HEAD 校验三态
./delete_marker_cleanup_tool -c config.yaml -mode check -input delete_markers.txt

# 模式 2：按 version-id 删除
./delete_marker_cleanup_tool -c config.yaml -mode delete -input delete_markers.txt

# input 也支持 invalid_keys.txt（key 含 \x1f 的兜底文件，
# delete_marker_list_tool 产出，本工具按 basename 自动 URL 解码 key）
./delete_marker_cleanup_tool -c config.yaml -mode check -input invalid_keys.txt
```

## 命令参数

| flag | 必填 | 说明 |
|---|---|---|
| `-c` | 是 | 配置文件路径（YAML） |
| `-mode` | 是 | `check` 或 `delete`（必填，无默认值——正确性关键） |
| `-input` | 是 | 输入文件路径，行格式 `bkt\x1fkey\x1fversion_id`（三段，`\x1f` 分隔） |

并发度**不在 flag 里**——从配置文件 `op_concurrency` 读，与其他参数同样强约束显式配置。

## 配置

`config.yaml` 示例：

```yaml
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http                        # http 或 https（后者忽略证书校验）
ak: <access-key>
sk: <secret-key>
op_concurrency: 8                   # 并发 worker 数
output_dir: ./out                   # 输出目录
output_dir_timestamp: true          # true=output_dir 追加 _YYYYMMDD_HHMMSS 同级后缀
progress_interval: 5000             # 进度行间隔（处理条目数）
dial_timeout: 10                    # TCP 建连超时秒数
response_header_timeout: 0          # 等服务端响应头超时秒数（0=不限）
```

| 字段 | 类型 | 默认 | 必填 | 说明 |
|---|---|---|---|---|
| `endpoints` | []string | — | 是（≥1） | ip:port 列表，worker 按行号 `i % N` 轮询绑 |
| `scheme` | string | `http` | 否 | `https` 时 `InsecureSkipVerify=true` |
| `ak` / `sk` | string | — | 是 | 静态签名凭证；sk 在 run.log 掩码为 `***` |
| `op_concurrency` | int | `8` | 否 | 并发 worker 数（0=未设→8；负数报错） |
| `output_dir` | string | `.` | 否 | 输出目录 |
| `output_dir_timestamp` | bool | `true` | 否 | true=追加同级时间戳后缀隔离运行 |
| `progress_interval` | int | `5000` | 否 | 进度行间隔（处理条目数） |
| `dial_timeout` | int64 | `10` | 否 | TCP 建连超时秒数 |
| `response_header_timeout` | int64 | `0` | 否 | 等响应头超时秒数；0=不限 |

**可直接复用 `delete_marker_list_tool` / `chunked_check_tool` 的 config.yaml**——本工具只读自己认识的字段，未识别字段（`list_type`、`list_concurrency`、`is_check`、`multipart_*`、`backup_*` 等）被 YAML 解析器忽略，不报错。

## 输入文件

行格式：`bkt\x1fkey\x1fversion_id`（三段，`\x1f` ASCII Unit Separator 分隔）。

- 由 `delete_marker_list_tool` 产出的 `delete_markers.txt` 直接消费
- 若输入 basename 是 `invalid_keys.txt`（key 含 `\x1f` 的兜底文件），本工具按 `url.QueryUnescape` 解码 key 段用于 HEAD/DELETE 调用；**输出行重新 `url.QueryEscape` 编码 key**，保持 3/4 段行格式契约（解码后 key 含 `\x1f` 会撕裂行字段）
- `version_id` 字面量 `"null"` 原样透传（S3 开版本控制前的旧对象，合法值）

## 输出文件

**mode check 产出：**

| 文件 | 内容 | 行格式 |
|---|---|---|
| `check_results.txt` | **结果文件**，每行一个版本的三态分类 | `bkt\x1fkey\x1fversion_id\x1f<state>`（四段，state ∈ `marker`/`not_marker`/`absent`） |
| `head_failed.txt` | HEAD 失败的行（惰性创建：文件不存在=无失败） | `bkt\x1fkey\x1fversion_id`（原样回写） |
| `head_failed.log` | HEAD 失败详情（含 HTTP code / S3 code / req_id / err） | 纯文本多行 |
| `run.log` | 进程日志：stdout/stderr tee、配置快照（sk 掩码）、进度行、summary | 纯文本 |

**mode delete 产出：**

| 文件 | 内容 | 行格式 |
|---|---|---|
| `deleted.txt` | **结果文件**，成功删除的行 | `bkt\x1fkey\x1fversion_id`（原样回写） |
| `delete_failed.txt` | DELETE 失败的行（惰性创建：文件不存在=无失败） | `bkt\x1fkey\x1fversion_id`（原样回写） |
| `delete_failed.log` | DELETE 失败详情 | 纯文本多行 |
| `run.log` | 同 mode check | 纯文本 |

**行分隔符 `\x1f`**（ASCII Unit Separator）：与 `delete_marker_list_tool` 对齐，比 `|` 撞 key 中字符的概率低一两个数量级。终端不可见，查看用：

```bash
awk -F'\x1f' '{print $1, $2, $3, $4}' check_results.txt
```

## mode check 三态判别（classifier.go）

| HEAD 响应 | state | 说明 |
|---|---|---|
| 200 OK + `IsDeleteMarker=false` | `not_marker` | 该版本存在且不是 marker |
| 405 + `x-amz-delete-marker: true` | `marker` | AWS SDK 把 405 错误路径 + `x-amz-delete-marker: true` 头推断为 `IsDeleteMarker=true` |
| 404 + code `NoSuchKey`/`NoSuchVersion` | `absent` | 该版本不存在 |
| 404 + code `NotFound`（HEAD 无 body） | `absent` | AWS SDK 对 HEAD 404 用状态码推断 code="NotFound"，按 `StatusCode==404` 兜底判 absent |
| 其他错误 | (失败) | 记入 `head_failed.txt` + `head_failed.log`，不计入 `check_results.txt` |

`classifier.classifyHead(r HeadResult)` 走 `r.Err == nil` 和 `r.Err != nil` 两条路径——aws-sdk-go-v2 v1.114.10 对 HEAD delete marker 返回 `*awshttp.ResponseError{StatusCode:405}` + `x-amz-delete-marker: true` 头，`headResultFromErr` 据此把 `IsDeleteMarker=true, Err=nil`；HEAD 404 返回 code="NotFound"（HEAD 无 body），按 `StatusCode==404` 兜底判 absent。

## mode delete 失败处理

| DELETE 响应 | 计入 | 说明 |
|---|---|---|
| 204 | `deleted.txt` | 成功 |
| 404 NoSuchKey / NoSuchVersion | `deleted.txt` | **算成功**（幂等空操作——版本不存在即目标状态达成） |
| 其他错误 | `delete_failed.txt` + `delete_failed.log` | 失败 |

**mode delete 不重新校验 marker 状态**：直接信任 lister 输出的 version-id，按行调用 `DeleteObject(bucket, key, versionId)`。这是 spec 设计：用户工作流是 list → check 审计 → delete → check 复核，delete 阶段不重复 check。

## 失败处理

**无重试、无断点、无优雅退出**（对齐 `delete_marker_list_tool`）：

- 单行失败不影响其他行——失败的行落 `head_failed.txt` / `delete_failed.txt`，其他行继续
- Ctrl-C / kill -9 直接终止进程；已 flush 的结果以整行为单位存续
- 补齐方式：把 `*_failed.txt` 当作下一轮 `-input` 重跑（行格式与输入一致，可直接复用）

## 预检

`run` 启动时读输入首行解析 bkt，用 `endpoints[0]` 跑一次 `BucketExists`：

- 传输错误（dial 失败 / 5xx 等）→ 硬退出（避免对着不可达节点跑全量）
- `exists=false` → warning + 继续（多桶输入保护：只校验首行 bkt，其他桶可能存在）

## 已知限制

- **key 含 `\n`** 会撕裂输入行（lister 同样不防护，S3 key 含换行极罕见）
- **kill -9** 丢尾部整行无提示；靠 `*_failed.txt` 与行数稳定人工确认完整性
- **mode delete 不重新校验**：若 input 是手写或被篡改，可能删错版本——用户工作流保证 input 来自 lister
- **同秒内连续启动两次**（`output_dir_timestamp: true`）会撞目录并发 append 撕裂尾部行

## 约束

- **endpoint 是纯 `host:port`**（如 `10.0.0.1:9000`），**不带 scheme**——scheme 由 config 的 `scheme` 字段（http/https）单一决定，endpoint 不允许自带 scheme 覆盖
- **scheme 只能是 `http` 或 `https`**，其他值启动即报错
- **`op_concurrency`** 默认 8，最小 1，0 取默认，负数报错
- **`version_id` 字面量 `"null"` 是合法值**：版本控制前的旧对象返回字面量 `"null"`，原样透传到 HEAD / DELETE，不要当空值
- **无重试无断点无优雅退出**：失败靠把 `*_failed.txt` 当下一轮 input 重跑

## 集成测试

`integration_test.go` 用 `//go:build integration` tag 隔离，跑前需启本地 MinIO：

```bash
# 启 MinIO（一次性，长期后台）
mkdir -p /tmp/minio-cleanup-it-data
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  nohup minio server /tmp/minio-cleanup-it-data --address 127.0.0.1:9100 \
  >/tmp/minio-cleanup-it.log 2>&1 &

# 跑集成测试
MINIO_ENDPOINT=127.0.0.1:9100 MINIO_AK=minioadmin MINIO_SK=minioadmin \
  go test -tags=integration -run TestIntegration_ -v ./...
```

未设这三个 env 的环境会自动 `t.Skip`，不阻塞 `go test ./...`。

## 相关文档

- 设计文档：`../docs/superpowers/specs/2026-11-12-delete-marker-cleanup-tool-design.md`
- 实现计划：`../docs/superpowers/plans/2026-11-12-delete-marker-cleanup-tool.md`
- AGENTS.md：`AGENTS.md`（设计约束、不变量、遗留任务）
- `delete_marker_list_tool`（输入来源）：`../delete_marker_list_tool/`
