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
list_type: 1            # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter, 3=递归+信号量
list_api_version: 2     # 1=ListObjects V1 (marker 分页), 2=ListObjectsV2 (continuation token, 默认)
list_concurrency: 8
check_concurrency: 16
output_dir: ./out
is_check: true          # true=列举+校验, false=仅列举
is_success_log: false   # 是否记录正常对象
is_multipart_check: false   # 是否对多段对象做分段损坏检查
multipart_segment_size: 0    # 多段分段检查的段长度(字节)，需与上传 part size 一致
progress_interval: 100000
obj_ch_capacity: 0           # lister→checker channel 容量；0=max(check_concurrency*4, 2000)
output_ch_capacity: 0        # output writer channel 容量；0=1024
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | S3 节点 ip:port 列表，至少 1 个 |
| `scheme` | `http` | `https` 时跳过 TLS 证书校验 |
| `ak` / `sk` | 必填 | 访问凭证（SigV4 静态凭证） |
| `list_type` | `1` | 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter, 3=递归+信号量 |
| `list_api_version` | `2` | 1=ListObjects V1（marker 分页），2=ListObjectsV2（continuation token，默认） |
| `list_concurrency` | `8` | 列举 worker 数（Mode 3 为信号量容量） |
| `check_concurrency` | `16` | 校验 worker 数 |
| `output_dir` | `.` | 输出目录（自动创建） |
| `is_check` | `true` | `false` 时只列举不校验，仅写 `stats.txt` + `list_failed.*` |
| `is_success_log` | `false` | `true` 时把正常对象 key 写入 `<ownerID>/ok_objects.txt`，干净的多段写入 `<ownerID>/ok_multipart_objects.txt` |
| `is_multipart_check` | `false` | `true` 时对多段对象做分段损坏检查 |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `progress_interval` | `100000` | stdout 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；0 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；0 走默认 |

### 配置示例

#### 启用 Mode 3（递归 + 信号量）

```yaml
list_type: 3
list_concurrency: 32      # 信号量容量，同时进行的 ListPage 调用数上限
```

适用：对象树深或不规则、希望并发度严格受控于信号量而非固定 worker 数的场景。Mode 2 的固定 worker 池在树形不规则时可能饿死（树宽 < worker 数时部分 worker 空闲）或过载（子目录集中爆发时），Mode 3 用递归 + 信号量自动随树形调节并发，每棵子树按需抢占 slot。`-nextmarker` 在此模式被忽略。

#### 切换到 V1 ListObjects API

```yaml
list_api_version: 1
```

适用：目标 S3 实现不支持 ListObjectsV2（某些旧版 MinIO 或自研存储），或 V2 行为异常时。V1 用 marker（最后一个返回的 key，delimited 时由 S3 返回 `NextMarker`）分页；V2 用 continuation token（服务器返回的不透明游标）。两条路径对 caller 透明，切换只需改这一个字段，Mode 1/2/3 均可搭配 V1 或 V2。

#### 组合：Mode 3 + V1

```yaml
list_type: 3
list_api_version: 1
list_concurrency: 32
```

#### 启用多段分段损坏检查

```yaml
is_multipart_check: true
multipart_segment_size: 5242880   # 5 MiB，需与上传 multipart part size 一致
```

适用：怀疑多段对象也写入了 chunked 签名（如客户端对每个 part 单独走 aws-chunked 编码）。开启后对每个多段对象按 `ceil(Size/segment_size)` 分段，对每段开头 128 字节做 Range GET，任一段命中 `length;chunk-signature=…` 正则即视为损坏，写入 `<ownerID>/corrupted_multipart_objects.txt`。

注意：`multipart_segment_size` **必须**与上传时的 part size 一致——chunk-signature 出现在每个 part body 的开头，分段边界错位会漏检。Size=0 的多段对象跳过分段检查（按普通多段记录）。某段 Range GET 报错走 `multipart_check_failed.txt`/`multipart_check_failed.log`（根目录），停止后续段检查。

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

### Mode 3（`list_type: 3`，递归 + 信号量）

1. 从根 prefix 开始，每个进行中的 walk goroutine 持有一个信号量 slot（容量 = `list_concurrency`）。
2. 对当前 prefix 带 delimiter 翻页列举：对象发到 objCh，新 `CommonPrefixes` 各起一个 goroutine 递归 walk。
3. 信号量保证同时进行的 ListPage 调用数 ≤ `list_concurrency`，无论树多深多宽。
4. 所有 walk goroutine 退出时（`sync.WaitGroup` 归零）→ 关闭 objCh。`-nextmarker` 在此模式被忽略。

适用：树深或不规则、希望并发度严格受控于信号量而非固定 worker 数的场景。Mode 2 的 worker 池在树形不规则时容易饿死或过载，Mode 3 用递归 + 信号量规避此问题。

## 输出文件

全部以 append 模式打开（支持断点续跑，不覆盖）。

### 结果文件（按 OwnerID 分目录，路径 `<output_dir>/<ownerID>/<filename>`；OwnerID 为空时落到 `_unknown/`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏的普通对象 key | Range GET 前 128 字节命中 chunk-signature 正则 |
| `multipart_objects.txt` | 多段对象 key（仅 key） | `is_multipart_check=false` 时所有多段对象 |
| `corrupted_multipart_objects.txt` | 损坏的多段对象 key | `is_multipart_check=true` 时分段检查命中 |
| `ok_multipart_objects.txt` | 干净的多段对象 key | `is_multipart_check=true` 且 `is_success_log=true` |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true` |

### 处理文件（全局，根目录 `<output_dir>/<filename>`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `list_failed.txt` | 列举失败的 prefix | list worker 调用失败（list-only 模式也写） |
| `list_failed.log` | 列举失败结构化错误信息（slog text，含 req_id/http_code/s3_code/err） | 同上 |
| `check_failed.txt` | 校验失败的普通对象 key | checker 普通对象 RangeGet 失败 |
| `check_failed.log` | 校验失败结构化错误信息（slog text） | 同上 |
| `multipart_check_failed.txt` | 多段分段检查失败的对象 key | `is_multipart_check=true` 时分段 RangeGet 失败 |
| `multipart_check_failed.log` | 多段分段检查失败结构化错误信息（slog text） | 同上 |
| `stats.txt` | 计时与计数（全局一份） | 程序结束 |

`is_check=false` 时只写 `list_failed.*` + `stats.txt`，不写任何对象文件，不创建 owner 目录。

### multipart_objects.txt 格式

每行只存 key，不带 ETag：
```
data/2026/01/file.bin
data/2026/02/no-etag.bin
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
corrupted_multipart: 7
list_failed: 3
check_failed: 7
multipart_check_failed: 2
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
[progress] listed=1000000 multipart=5000 corrupted=30 corrupted_mp=7 list_failed=3 check_failed=7 multipart_check_failed=2 list_calls=12350 list_avg_ms=82.15 get_calls=995000 get_avg_ms=4.21 (checked=1000000) q=pfx:12 obj:48 cor:0 mp:0 cmp:0 lf:1 cf:0 mcf:0 su:0
```

`is_check=false` 时只打 `listed`/`list_calls`/`list_failed`。程序结束时打印汇总。

## 测试

```bash
go test -race ./...
```