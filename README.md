# chunked_check_tool

并发列举并校验 S3 对象，识别因 `aws-chunked` 编码未被正确解析而产生的损坏对象。

## 背景

自研 S3 存储系统曾因未正确解析 `X-Amz-Content-Sha256` header，把 `aws-chunked` PUT 请求的 `length;chunk-signature=…` / `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式化内容当作原始 body 写入存储。本工具遍历整个桶，把损坏的普通对象、多段对象、处理失败的对象分别写入不同文件，支持几十亿对象规模。

## 编译

```bash
# 当前平台（macOS）
go build -o chunked_check_tool .

# Linux amd64（交叉编译，目标机不需要 Go）
GOOS=linux GOARCH=amd64 go build -o chunked_check_tool-linux-amd64 .

# Linux arm64
GOOS=linux GOARCH=arm64 go build -o chunked_check_tool-linux-arm64 .
```

生成的二进制可直接 scp 到 Linux 服务器运行，不需要目标机安装 Go 或任何运行时依赖（静态编译）。

## 配置

`config.yaml` 示例：

```yaml
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http            # http 或 https（后者忽略证书校验）
ak: <access-key>
sk: <secret-key>
list_type: 1            # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter
list_concurrency: 8
check_concurrency: 16
output_dir: ./out
is_check: true          # true=列举+校验, false=仅列举
is_success_log: false   # 是否记录正常对象
progress_interval: 100000
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | S3 节点 ip:port 列表，至少 1 个 |
| `scheme` | `http` | `https` 时跳过 TLS 证书校验 |
| `ak` / `sk` | 必填 | 访问凭证（SigV4 静态凭证） |
| `list_type` | `1` | 1 或 2，见下方"列举模式" |
| `list_concurrency` | `8` | 列举 worker 数 |
| `check_concurrency` | `16` | 校验 worker 数 |
| `output_dir` | `.` | 输出目录（自动创建） |
| `is_check` | `true` | `false` 时只列举不校验，仅写 `stats.txt` |
| `is_success_log` | `false` | `true` 时把正常对象 key 写入 `success_objects.log` |
| `progress_interval` | `100000` | stdout 进度打印阈值（约） |

## 运行

```bash
./chunked_check_tool -c config.yaml -bkt mybucket
./chunked_check_tool -c config.yaml -bkt mybucket -prefix data/2026/
./chunked_check_tool -c config.yaml -bkt mybucket -prefix data/2026/ -nextmarker data/2026/file_005
```

| flag | 必填 | 说明 |
|---|---|---|
| `-c` | 是 | 配置文件路径 |
| `-bkt` | 是 | 桶名 |
| `-prefix` | 否 | 列举前缀，默认空（整个桶） |
| `-nextmarker` | 否 | start-after key，跳过该 key 之前的对象；**仅 Mode 1 生效** |

按 `Ctrl+C`（SIGINT）或 `SIGTERM` 会触发优雅关闭：种子循环中断，但已缓冲的输出和统计仍会落盘。

## 列举模式

### Mode 1（`list_type: 1`，子目录 + 平铺 nextmarker）

1. 主线程用 delimiter 列举根 prefix，拿 `CommonPrefixes`（子目录）。
2. 子目录塞进队列；根下直接对象由主线程的分页循环直接处理。
3. 每个 list worker 从队列取一个子目录 prefix，**不带** delimiter 翻页列出该子目录下所有对象。
4. `-nextmarker` 作为根列举的 start-after 参数（断点续跑）。

适用：按目录层级组织、需要按子目录并行列举的场景。

### Mode 2（`list_type: 2`，递归 BFS delimiter）

1. 队列预置根 prefix。
2. 每个 list worker 取一个 prefix，带 delimiter 列举：对象发到 objCh，新 `CommonPrefixes` 回队列。
3. `inflight` atomic 计数器跟踪待处理任务；归零时关闭 objCh。

适用：层级深、需要自动发现所有子前缀的场景。`-nextmarker` 在此模式被忽略。

## 输出文件

全部写到 `output_dir`，以 append 模式打开（支持断点续跑，不覆盖）：

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏的普通对象 key | Range GET 前 128 字节命中 chunk-signature 正则 |
| `multipart_objects.txt` | `<key>\|<etag>` | ETag 不是 32 位小写 MD5 hex |
| `list_failed.txt` | prefix + 错误原因 | list worker 调用失败 |
| `check_failed.txt` | key + 错误原因 | checker 调用失败 |
| `success_objects.log` | 正常对象 key | 仅 `is_success_log=true` |
| `stats.txt` | 计时与计数 | 程序结束 |

`is_check=false` 时不写任何对象文件，仅写 `stats.txt`。

### multipart_objects.txt 格式

每行 `<key>|<etag>`，例如：
```
a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4-5|data/2026/01/file.bin
-|data/2026/02/no-etag.bin
```

多段判定**严格**：只有 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象，任何其他格式（`<hex>-N`、大写、长度不对、空值）一律按多段处理。原则：绝不把多段误判为普通对象。

## 统计（stats.txt 示例）

```
total_objects: 12345678
list_calls: 12350
list_avg_latency_ms: 82.15
list_total_duration_sec: 642.31
total_duration_sec: 780.45
multipart: 5230
corrupted: 42
list_failed: 3
check_failed: 7
```

`is_check=false` 时只写前 5 行 + `list_failed`。

## 节点故障处理

- 启动时 worker 轮询绑定 endpoint（`endpoints[i % N]`）。
- 请求失败且错误类型为节点故障（连接拒绝、超时、5xx）→ 标记该节点故障，worker 重新分配到下一个存活节点，重建 client，**重试一次**；再失败记 `list_failed`/`check_failed`，不阻塞流程。
- 普通 S3 业务错误（404、403）不触发重绑。
- 故障集合全局共享，所有 worker 避开故障节点。

## 进度打印

stdout 约每 `progress_interval` 个对象打印一行：
```
[progress] listed=1000000 checked=995000 multipart=5000 corrupted=30 elapsed=120s
```

`is_check=false` 时只打 `listed`。程序结束时打印汇总。

## 测试

```bash
go test -race ./...
```