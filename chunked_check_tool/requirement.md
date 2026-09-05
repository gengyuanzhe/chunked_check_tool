### 背景

我开发了一个S3存储系统，这个存储系统由于无法正确识别 `X-Amz-Content-Sha256` 这个header，导致对于aws-chunked类型的请求，没有解析请求body体里 `length; chunk-signature=xxxx`/ `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式，把整个内容写入到body体里，存储到系统中。


### 需求

现在我需要编写一个go语言的工具，能够并发地列举+检查对象，把结果分类输出到不同文件里。

- 输入: 命令参数 `-c <config.yaml> -bkt <bucket> [-prefix <p>] [-nextmarker <key>]`（并发度从配置文件读，不再是命令行参数）
- 输出: 把损坏的普通对象、多段对象、列举失败、校验失败分别写入到不同文件中；可选地记录正常对象；最后写一个统计文件

**校验逻辑：**

仅对普通对象进行强制校验；多段对象可选分段校验。ETag 来自 list 响应（统一来源，**不从 Range GET response header 取**），根据 ETag 判断是普通对象还是多段：

- 如果是多段对象：
  - 若 `is_multipart_check=true` 且 `multipart_segment_size>0`：按 `ceil(Size/segment_size)` 分段，对每段开头 128 字节做 Range GET，任一段命中 `length;chunk-signature=xxx` 格式即视为损坏，写入 `<ownerID>/corrupted_mp.txt`。某段 Range GET 报错走 `multipart_check_failed.txt`（根目录）路径并停止后续段检查；全部段均不匹配则按普通多段记入 `<ownerID>/ok_mp.txt`（仅 `is_multipart_success_log=true` 时落盘，否则只计数不写文件）。
  - 否则直接写入 `<ownerID>/mp.txt`（仅 key，不带 ETag），**不做 Range GET**。
- 如果是普通对象，通过 Range GET 读前 128 字节，检查是否以 `length;chunk-signature=xxx\n` 格式开头；命中则视为损坏，写入 `<ownerID>/corrupted_objects.txt`。

`multipart_segment_size` 必须与上传时的 multipart part size 一致，否则 chunk-signature 不在段边界上会漏检。Size=0 的多段对象跳过分段检查，按普通多段记录。

**ETag 区分（严格）**

- 普通对象 ETag：`^[0-9a-f]{32}$`（32 位小写 MD5 hex）
- 多段对象 ETag：`<32 char>-N`（N 为段数）

**判定原则**：只有 `^[0-9a-f]{32}$` 算普通对象，**任何其他格式（`<hex>-N`、大写、长度不对、空值）一律按多段处理**。绝不把多段误判为普通对象，宁可多段多计。

**多段输出格式**

所有 .txt 文件（包括 `mp.txt`）**每行只存 key**，不带 ETag。

**列举模式**

提供两种列举方式，但都需要保证列举的并发上限。

1. **Mode 1（子目录 + 平铺 nextmarker）**：先用 delimiter 列举根 prefix 拿子目录（`CommonPrefixes`），子目录塞进队列；根下直接对象由主线程的分页循环处理。每个 list worker 从队列取一个子目录 prefix，**不带** delimiter 翻页列出该子目录下所有对象。`-nextmarker` 作为根列举的 start-after 参数（断点续跑）。
2. **Mode 2（递归 BFS delimiter）**：队列预置根 prefix。每个 list worker 取一个 prefix，带 delimiter 列举：对象发到 objCh，新 `CommonPrefixes` 回队列。用 atomic 计数器跟踪待处理任务，归零时关闭 objCh。`-nextmarker` 在此模式被忽略。

列举出的结果放到 objCh（有界 channel）里，再用另外一组并发线程来进行校验。背压链：输出 channel 满 → writer 阻塞 → checker 阻塞 → objCh 满 → list worker 阻塞 → 整体自调节。

**配置文件** (`config.yaml`) 包含如下信息：

```yaml
endpoints:              # ip:port 列表，至少 1 个
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http            # http 或 https（https 时忽略证书校验）；默认 http
ak: <access-key>
sk: <secret-key>
list_type: 1            # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter
list_concurrency: 8     # 列举并发度
check_concurrency: 16   # 校验并发度
output_dir: ./out       # 默认当前目录
is_check: true          # true=列举+校验, false=仅列举
is_success_log: false   # 是否记录正常普通对象
is_multipart_check: false   # 是否对多段对象做分段损坏检查
multipart_segment_size: 0    # 多段分段检查的段长度(字节)，需与上传 part size 一致
is_multipart_success_log: false  # 是否记录干净的多段对象
progress_interval: 100000  # 进度打印阈值（约，性能优先）
```

`is_check=false` 时只统计不校验，**不写任何对象文件**，仅写 `stats.txt` + `list_failed.txt` + `list_failed.log`。

**输出文件**（全部 append 模式，支持断点续跑）：

**结果文件**（按 OwnerID 分目录，路径 `<output_dir>/<ownerID>/<filename>`；OwnerID 为空时落到 `_unknown/`）：

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏的普通对象 key | Range GET 命中 chunk-signature（`is_check=true`） |
| `mp.txt` | 多段对象 key（仅 key，不带 ETag） | `is_multipart_check=false` 时所有多段对象 |
| `corrupted_mp.txt` | 损坏的多段对象 key | `is_multipart_check=true` 时分段检查命中 |
| `ok_mp.txt` | 干净的多段对象 key | `is_multipart_check=true` 且 `is_multipart_success_log=true` |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true` |

**处理文件**（全局，根目录 `<output_dir>/<filename>`）：

| 文件 | 内容 |
|---|---|
| `list_failed.txt` | 列举失败的 prefix（仅 prefix） |
| `list_failed.log` | 列举失败的结构化错误信息（slog text，含 req_id/http_code/s3_code/err） |
| `check_failed.txt` | 校验失败的普通对象 key（仅 key） |
| `check_failed.log` | 校验失败的结构化错误信息（slog text，含 req_id/http_code/s3_code/err） |
| `multipart_check_failed.txt` | 多段分段检查失败的对象 key（仅 key） |
| `multipart_check_failed.log` | 多段分段检查失败的结构化错误信息（slog text，含 req_id/http_code/s3_code/err） |
| `stats.txt` | 计时与计数（全局一份） |

`.txt` 与对应 `.log` 通过对象名/prefix 关联：`.txt` 只存 key/prefix 作关联键，错误原因在 `.log` 里。`.log` 字段顺序：`time level msg req_id key/prefix http_code s3_code err`（slog text handler，key=value 形式）。

**统计**需要包含：对象总数、程序执行总耗时（与对象总数同一行）、list 总次数、list 平均耗时、list 总耗时、get 总次数、get 平均耗时、get 总耗时、ok_objects 数（干净普通对象）、ok_mp 数（干净多段，switch off 时为全部多段、switch on 时为通过分段检查的）、corrupted_objects 数（损坏普通对象）、corrupted_mp 数（损坏多段）、list_failed 数、check_failed 数（普通对象 RangeGet 失败）、multipart_check_failed 数。

### 其他：

1. **节点轮询与故障隔离**：启动时各 worker 轮询绑定一个 endpoint（`endpoints[i % N]`），一旦确定，worker 发送请求的节点保持固定。若某个节点故障（连接拒绝、超时、5xx），则打印告警，把这个节点先忽略，worker 重新分配到下一个存活节点，重建 client，**重试一次**；再失败记 `list_failed`/`check_failed`，不阻塞流程。普通 S3 业务错误（404/403）不触发重绑。故障集合全局共享，所有 worker 避开故障节点。
2. **统计 list 的时延和程序运行的总时延**（见上方统计字段）。
3. **列举/校验失败分流**：列举失败和校验失败**分别**写入 `list_failed.txt` 和 `check_failed.txt`，支持后续另行处理。checker goroutine 绝不因错误退出（否则 `objCh` 无人消费、list worker 永久阻塞）。
4. **进度打印**：stdout 每处理约 `progress_interval` 个对象打印一行，不要过多但得有。打印节奏不要求精确到 `progress_interval`，大约在这个数字就好，**以性能优先**（每 worker 本地计数器，避免 per-obj atomic 开销）。
5. **S3 客户端**：用 `github.com/minio/minio-go/v7`（MinIO SDK v7，非 AWS SDK）。配置文件里指定 `scheme`（http/https，https 时 `InsecureSkipVerify=true` 忽略证书）。
6. **性能优先但保持可读性**：关注长连接复用（minio-go 自带连接池，不要自建）、文件写入性能（`bufio.Writer` 包裹）、合理的 channel 容量、避免每对象分配。但**不要为了性能把代码变得过于复杂**——如果要写一段很复杂难读的代码（如手写内存池、unsafe、复杂 lock-free 结构），**需要提前向我请求确认**，不要直接写。可读性 > 微优化。
