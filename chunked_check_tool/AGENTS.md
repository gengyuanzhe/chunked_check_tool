# AGENTS.md — chunked_check_tool

本文件面向在本仓库工作的 AI 代理（和人类工程师），说明项目意图、模块边界、关键不变量与已知遗留项。改动前请先读完本文件对应章节。

## 1. 项目意图

### 1.1 故障背景（最重要的前提，所有后续设计都基于此）

自研 S3 存储系统存在 BUG：PUT 请求处理未正确解析 `X-Amz-Content-Sha256` header，因此**未识别** aws-chunked transfer-encoding，把以下三类 streaming 上传的**分帧格式 + trailer + 签名**全部当作原始 body 字节流落盘：

- `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` —— 每段格式 `<hexlen>;chunk-signature=<64hex>\r\n<data>\r\n`
- `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` —— 段格式同上，末尾追加 `0\r\n<x-amz-checksum-**:...>\r\nx-amz-trailer-signature:...\r\n\r\n`
- `STREAMING-UNSIGNED-PAYLOAD-TRAILER` —— 段格式简化为 `<hexlen>\r\n<data>\r\n`（无 chunk-signature），末尾追加 `0\r\nx-amz-checksum-**:...\r\n\r\n`

例如 `"hello world"` 走 unsigned-trailer 上传，落盘的实际字节是：
```
b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:<base64>\r\n\r\n
```
而非 11 字节的 `hello world`。

**关键推论**（直接决定校验逻辑）：

1. **ETag 是物理字节的 MD5**：存储侧根本没意识到是 aws-chunked，按普通对象计算 MD5。因此 LIST 返回的 ETag 反映**实际落盘字节**，而非用户上传的逻辑 payload。
2. **Size 是物理存储大小**：LIST 返回的 Size 含分帧/trailer 字节数，大于逻辑 payload。
3. **多段对象的 ETag 在 mode=1 下携带物理 part 边界**：`internal-list-mp-offset: true` 让服务端把多段 ETag 改写为 `<md5>-<partcnt>-<off0>|<off1>|...`，其中 `off_i` 是**物理字节偏移**（每段 part 的起始物理偏移），不是逻辑 payload 偏移。配合 Size（物理大小）可推出每段 `[off_i, off_{i+1})` 或末段 `[off_{N-1}, Size)` 的物理边界。
4. **损坏的概率性**：BUG 不是确定性触发——同一个客户端既上传正常对象、也上传损坏对象；同一个多段对象的**每个 part 独立**可能损坏或正常。因此：
   - 普通对象（ETag 32 位 hex）也可能是损坏的（含 chunked 帧残留）。
   - 多段对象的某些 part 可能损坏、其它 part 正常——必须逐段探测，不能整体跳过。
   - **多段 trailer part 不落盘**：trailer 类编码（`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`、`STREAMING-UNSIGNED-PAYLOAD-TRAILER`）作为多段 part 上传时私有存储报错、对象不落盘（见根 `AGENTS.md §1`）。因此多段对象落盘的 part 只可能是普通 PUT 或 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`（非 trailer 签名 streaming）——多段探测只需抓段首 `chunkSigRe`，不会遇到真实 trailer marker。trailer 损坏检测（`trailerRe`）只对单次 PUT 场景有效。
5. **三类损坏特征分布在 part 的不同位置**：
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 和 `-TRAILER`：段首是 `<hexlen>;chunk-signature=<64hex>\r\n`（当前 `chunkSigRe` 抓这个，强特征）。注：`-TRAILER` 变体作为多段 part 不落盘（私有存储报错），但作为单次 PUT 仍落盘——单次 PUT 场景下 `chunkSigRe` 对它冗余覆盖（每段首有 `;chunk-signature=`）。
   - `STREAMING-UNSIGNED-PAYLOAD-TRAILER`：段首是 `<hexlen>\r\n`（弱特征，误报风险高，**不能**单独作判据）；段尾/对象尾是 `0\r\nx-amz-checksum-<algo>:<value>\r\n\r\n`（`trailerRe` 抓这个强特征）。这个弱特征只当 trailerRe 的前提：尾部的 trailer 标记，只有在对象以 `<hexlen>\r\n` 开头时才算损坏；不以它开头的对象，大对象也不再读尾部的 128 字节。**多段 part 不落盘**，故 `trailerRe` 只在单次 PUT 的 whole-object / tail 探测路径上有效。
   - trailer 类的段首特征太弱，必须靠段尾 trailer marker 抓。

### 1.2 工具职责

本工具**并发**列举并校验对象：
- 普通对象（ETag 为 32 位小写 MD5 hex）→ 判损坏的条件不分大小：开头是签名头（`chunkSigRe`），或者以 chunk 长度行 `<hexlen>\r\n` 开头且尾部有 x-amz-checksum trailer 标记（`trailerRe`）。小对象（`Size <= whole_object_probe_threshold`）一次 Range GET 读全对象（1 次请求）；大对象先读 head@0 的 128 字节：命中 `chunkSigRe` 即损坏，不以 chunk 长度行开头即判干净（不再读 tail，共 1 次请求），否则再读 tail@Size-128 的 128 字节查 `trailerRe`（共 2 次请求）。
- 多段对象（任何非 `^[0-9a-f]{32}$` 的 ETag）→ 按 `multipart_check_mode` 处理：
  - `0`（关闭）→ 直接入 `mp.txt`，不做 Range GET。
  - `1`（offset 检查）→ LIST 带 `internal-list-mp-offset: true` header，服务端把多段 ETag 改写为 `<md5>-<partcnt>-<off0>|<off1>|...`（物理字节偏移）；按**每个 part offset 一次探测 `[off_i, off_i+128]`** = **N 次请求，无 tail probe**（多段只跑 `chunkSigRe`，它匹配段首 `;chunk-signature=`；每个 part 段首由自己的探测覆盖，tail probe 读末段段尾永不命中 → 死请求，已删）；ETag 解析不出 offsets（格式不符或 offset > Size）的对象回落 `list_parse_failed.txt`（根目录，行格式 `bucket\x1fkey`）。`parseMultipartOffsetETag` 始终校验每个 offset ≤ Size（Size=0 是合法空对象：唯一合法 ETag 是 `<md5>-1-0`）。这种空的多段对象不探测，直接记 `ok_mp`（见不变量 16 的 (a)）。
- 列举失败、校验失败分别落不同文件。
- 支持几十亿对象规模，内存有界，背压自调节。探测矩阵与双 regex 细则见 §4.17。

除 list+check 主模式外还有两个文件输入模式（互斥，组成补跑流水线，见 §4.20）：`-check-file`（mixed 格式重查 check_failed/mp_check_failed/mismatch，HEAD 权威判型）、`-backup-file`（mixed 格式，HEAD 判型 + 中转 + ETag 终验，**不探测损坏**）。

**不在本工具范围**：修复损坏对象（另行处理）。

## 2. 技术栈

- Go 1.27（`go.mod` module `chunked_check_tool`）
- `github.com/aws/aws-sdk-go-v2`（`aws`、`aws/transport/http`、`aws/signer/v4`）+ `service/s3` + `credentials` + `smithy-go`：**AWS SDK v2，path-style 寻址**（迁移自 minio-go——minio-go 在客户端按 S3 3-63 命名规则拒绝短桶名，自研 S3 接受它们；aws client 无此校验）。用 `s3.ListObjectsV2`/`s3.ListObjects` 拿 Contents + CommonPrefixes，用 `s3.GetObject` + `Range: "bytes=a-b"` 做 Range GET。client 用 `s3.New` + 全显式 options 构造（**不用 LoadDefaultConfig**——环境变量和 ~/.aws/config 不得泄漏进数据完整性工具的凭证/重试行为）。中转 PUT 走 `withUnsignedPayload`（见不变量 22）。LIST 请求按 `encoding_type_url` 配置带 `EncodingType=url`，wire 上 server 字段（key/prefix/marker）URL 编码，出口由 `wireDecoder` 统一 `QueryUnescape` 解码（见不变量 23）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（用户明确要求忽略证书）

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析、**不拦截任何信号**（Ctrl-C / kill 直接终止进程，见不变量 19）、`run`/`runBackup` 编排、Mode 1 根列举分页（用 `cfg.MaxKeys`，调用 `checkShortResponse`）、worker 启停、stats 写盘；`run()` 注入 `debugLogger`（cfg.DebugLog=true 时 Debug-level slog 路由 stdout→run.log）和 `listLogger`（`list_failed.log` 的 Warn-level slog）到 S3Client，调 `stats.SetListLatencyWindow(cfg.LatencyWindowSec)` |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，带默认值）；`max_keys` 默认 1000、超出 1..1000 报错；`latency_window_sec` 默认 60 拒负值 |
| `nodepool.go` | `NodePool`：轮询 `Assign`/`AssignOther`、`RecordFault`（累积故障计数，达 `node_isolate_threshold` 才 `MarkFailed` 隔离）、`Unmark`/`FailedNodes`（恢复探测用）、`URL`/`Endpoint`（隔离全局共享、进程内单向——除非开了恢复探测） |
| `noderecovery.go` | 后台节点恢复：`startNodeRecovery`（`node_recover_probe_interval`>0 时每轮对隔离节点做 HEAD bucket 探测，连续 2 次健康 → `Unmark` 重返轮询池并清零故障计数；探测健康标准 = `!isNodeFaultErr`，即 2xx/404/403 都算活、5xx/传输错误不算）、`recoveryRound`/`probeNode`（可单测的轮次逻辑） |
| `s3client.go` | `S3API` 接口、`S3Client`（`awsS3API` 接口包装 `*s3.Client`，按 `cfg.ListAPIVersion` 分派 V1/V2）、`PutObjectLocal`（归档流式上传，>16MiB 自动分段 multipart）/`PutObjectStream`（中转流式上传，`withUnsignedPayload`）的双流式设计（**任何路径不整文件缓冲**）、节点故障 failover。`NewAWSClient` 在 `http.Transport` 上绑 `DialContext`（`dial_timeout`，默认 10s 兜底黑洞 IP）和 `ResponseHeaderTimeout`（`response_header_timeout`，默认 0 不兜底）；所有 s3 client 构造（worker client、`rebuildOn`、`noderecovery.probeNode`）共用同一对超时。**wire 层**：`wireDecoder`（`encoding_type_url` 开时 `QueryUnescape` 出口解码，失败 warn 一次到 `listLogger`，保留原值 best-effort）、`encodingTypeURL()`/`SetLoggers()`（debug=list-page req/resp/err，list=warn 信号，两者 nil 安全）。V1 NextMarker 与 Contents key 回退两个 next 源都出口解码；V2 NextContinuationToken 不透明 base64 **不**解码（字面 `+` 必须原样往返）。`listPageOnce` 三个 Debug 点：req（marker/token/delim/maxKeys）、err（req_id + 完整 err）、resp（next/truncated/nContents/nCP）。测试替身在各自 _test.go：`FakeS3`（fakes3_test.go，S3API 级）、`fakeCore`（s3client_v1_test.go，awsS3API 级 V1/V2 分派）、`scriptedS3`（lister_test.go） |
| `lister.go` | `Lister`（无 `s3` 字段；`Run`/`processPrefix` 接 `s3` 参数）、无界队列 BFS、`inflight` atomic 计数、`recordListFailure`（记录未完成页的游标，见不变量 20）、`resumed`（`-resume-journal` 轮留用的重放前缀表：重列页上已在 journal 里的子前缀不再登记、不再入队，见不变量 20）；`checkShortResponse` 包级函数（IsTruncated 且 entries<max_keys 告警，见不变量 24） |
| `resume_list.go` | `-resume-list` 模式：`parseResumeListLine` 解析 `\x1f` 分隔行（`splitEscaped2` 在第一个 `\x1f` 切分，每段 `QueryUnescape`）、`readResumeList` 逐行解析+违约分流（whitespace-only 行/无效 %XX → parse_failed；正常 → entries）。**空行 `""` 是合法条目**（根 prefix 第一页失败，prefix=="", token==""），不丢——否则用户没配 `-prefix` 时第一页 LIST 失败的 run 无法 resume |
| `checker.go` | `Checker`（不持 ctx；探测用 `context.Background()`，单次调用的 30s 超时在 `S3Client` 里兜底）、`isNormalETag`（严格 32 位小写 hex）、`chunkSigRe`/`trailerRe`、`runProbes`/`buildProbesStatic`（探测矩阵）；`extractHTTPStatusCode`/`extractS3Code`/`extractRequestID` 三个错误字段提取器（`errors.As` 到 `*awshttp.ResponseError`，nil 安全） |
| `backup.go` | `BackupChecker`：HEAD → 判型门 → 中转 → ETag 终验（**不探测**，见不变量 19）；relayRegular/relayMultipart 流式中转 |
| `file_source.go` | 泛型 `fileSource[T]`：两个文件输入模式（-check-file/-backup-file）共用的逐行读取骨架（读行→parse→parse_failed 记录→push），`MalformedLineError`。文件打不开或读到一半出错时返回带路径和行号的错误（单行超过 1 MiB 上限算读错）；`run` / `runBackup` 把它返回给 `main`：已读入的行照常处理、汇总照常打印，然后进程以退出码 1 结束 |
| `check_file_source.go` | `parseCheckFileLine`：mixed 格式 → VerifyTask（委托 `parseMixedLine`，HeadFirst=true） |
| `multipart_line.go` | `parseMultipartLine`：mixed 格式的多段行（`bkt\x1fkey\x1fpartcnt\x1foff...`）→ VerifyTask；`parseMixedLine` 的多段分支委托于此（规则：partcnt>=1、offset 个数=partcnt、offset0=0、严格递增） |
| `backup_source.go` | `parseMixedLine`：mixed 格式（`bkt\x1fkey` / `bkt\x1fkey\x1fpartcnt\x1foff...`）→ BackupTask（-check-file 与 -backup-file 共用的解析器）；`unescapeKey` helper 在 `\x1f` 处分割并对键字段 `QueryUnescape` |
| `mpoffset.go` | offset-etag 检查的两个原语：`withMpOffsetListHeader`（per-call `s3.Options` mutator，经 Build middleware 给**单次** LIST 请求注入 `internal-list-mp-offset: true`；只被 `listPageOnce` 在 LIST 调用点传入——"仅 LIST"是结构保证，无请求形态嗅探。Build 在签名前运行，非 x-amz 名不参与 SigV4 签名）、`parseMultipartOffsetETag(etag, size)`（服务端 ETag 格式 `<md5>-<partcnt>-<off0>|<off1>|...`，**ETag 内部分隔符仍是 `|`**——这是服务端 wire 格式不能改；解析按 `strings.Split(parts[2], "|")`，规则对齐 parseMultipartLine；始终校验每个 offset ≤ Size，Size=0 是合法空对象） |
| `output.go` | channel + writer goroutine（per-owner 分目录 fan-out + 根目录全局），`FileInputMode` 门控（FileInputCheckFile 强制 mp 输出开启、开 mp.txt 漂移回落、恒开 ok_objects/ok_mp；mode=offset 开 list_parse_failed.txt/log），`bufio.Writer` 64KB，append 模式（OwnerID 为空 → `_unknown/`）。**行分隔与转义**：`const rowSep = "\x1f"`（ASCII Unit Separator，S3 key 可含 `|` 故弃用 pipe），server 字段（key/prefix/token）写入时 `url.QueryEscape`，bucket 来自 CLI 不转义，数字字段（partcnt/offsets）不转义，owner 非空时为 `[a-z0-9]+`，不转义（`renderLineFmt` 原样写 bucket，key `QueryEscape`）。`ListLogger()` 访问器返回 `list_failed.log` 的 slog 给 s3client wire 层用。`WriteShortResponseWarning` 写到 listLogger（非 debug_log 门控——生产信号不能被吞） |
| `queue.go` | 无界队列（slice + mutex + cond），阻塞 Pop，`Close` 广播唤醒所有等待者 |
| `stats.go` | atomic.Int64 计数器 + `StatsSnapshot` + `PrintSummary`（按 `RunMode` 分模式输出指标集；写入 stdout，被 `mwOut` tee 进 run.log）；`listLatency *LatencyTracker`（窗口化 LIST 延迟，由 `AddListCall` 双写全局+窗口，`SetListLatencyWindow(cfg.LatencyWindowSec)` 配置窗口长度），快照字段 `ListLatencyAvgMs`/`ListLatencyMaxMs`（仅 progress 行使用，summary 仍用全局 `ListAvgLatencyMs`） |
| `progress.go` | `ProgressPrinter`（按 `RunMode` 输出进度行字段集）+ `localCounter`（每 worker 本地 int，无 per-obj atomic）。默认分支打印 `latency_avg_ms`/`latency_max_ms`（窗口化，反映当前批次健康，全局 `list_avg_ms` 会被早期 spike 稀释） |
| `latency.go` | `LatencyTracker`：滑动窗口延迟计量，每窗口重置，`Record(d)`/`Snapshot()`/`SetWindow(d)`，零值可用（1 分钟默认窗口首用锁定），用于 progress 行的当前批次 avg/max |

## 4. 关键不变量（改动前必须守住）

1. **多段判定严格**：仅 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象。大写、长度不对、`<hex>-N`、`<hex>-N-<offsets>`、空值一律按多段处理。**绝不把多段误判为普通对象**。`isNormalETag` 用逐字节循环实现（非正则），不要改成宽松匹配。offset-etag 的 md5 前缀校验复用同款严格性（大写 → 解析失败 → 回落 list_parse_failed.txt，不静默接受）。

1a. **offset 模式的 header 注入与回落**：`multipart_check_mode=1` 且 `is_check=true` 时，LIST 请求（V1/V2、failover 重建的 client）经 `withMpOffsetListHeader`（per-call API option，`listPageOnce` 的 LIST 调用点传入）注入 `internal-list-mp-offset: true`——"仅 LIST、绝不 HEAD/GET/PUT"由调用点结构保证。该 header 名**不得**改成 x-amz-/x-obs- 开头（服务端签名计算覆盖这些前缀，注入会被拒；非 x-amz 名在 SigV2/V4 下均可不签）。ETag 解析不出 offsets（服务端未实现/未升级、格式不符、或 offset > Size）→ Offsets=nil → 根目录 `list_parse_failed.txt` + `list_parse_failed.log` 回落（**不再**写 mp.txt）。`parseMultipartOffsetETag(etag, size)` 始终校验每个 offset ≤ Size；Size=0 是合法空对象，唯一合法 ETag 是 `<md5>-1-0`，多段 ETag 的非零 offset > 0 即非法。log 是根目录 slog，字段 `bucket/key/owner/size/etag_len/etag`（完整 ETag 无截断）。**假设服务端只改多段对象的 ETag**——普通对象若也被改写会被判为多段（不漏检损坏，但 list_obj/list_mp 计数失真）。`TestMpOffsetHeaderListOnly` 是回归钉（LIST 带 header、HEAD 不带）。

2. **mode=0 时多段对象跳过 Range GET**：直接写 `<ownerID>/mp.txt`。每行 `bucket\x1f<escaped-key>`（见不变量 23），不带 ETag。mode=1 的多段对象按 offsets 逐段探测（见 14），**不是**无条件跳过 Range GET。

3. **ETag 来源**：list 响应（统一），**不从 Range GET response header 取**。OwnerID 同样来自 list 响应（V2 请求必须显式 `FetchOwner=true`——minio-go 默认带上，aws SDK **不**默认；漏设则所有 OwnerID 为空、全部结果塌进 `_unknown/`。`TestListPageV2_SetsFetchOwner` 是回归钉）。ETag 落地在 `ListPage`/`HeadObject` 出口统一 `trimETagQuotes`（S3 wire 上 ETag 带引号，aws SDK 不剥）。

4. **结果文件按 OwnerID 分目录，处理文件全局**：`corrupted_objects`/`mp`/`corrupted_mp`/`ok_mp`/`ok_objects` 这五类结果文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`）；`list_failed`/`parse_failed`/`check_failed`/`mp_check_failed`/`list_parse_failed`/`stats` 留根目录全局。理由：结果文件数量大且天然按 owner 分桶有意义；处理文件全局方便运维统一排查；stats 全局一份避免 owner 分桶后还要汇总。ownerDirName 折叠空/`.`/`..`/含路径分隔符的 OwnerID 到 `_unknown`，防止路径穿越。

4a. **失败分类三文件**（list_failed / parse_failed / list_parse_failed 职责分离）：
  - `list_failed.txt` — S3 LIST 调用失败（节点故障/5xx/超时）。行格式 `prefix\x1ftoken`：token 是失败页的 continuationToken（V1=上一页最后 key，V2=服务端不透明 token），第一页失败时为单字段 `prefix`。**可自动补跑**：`-resume-list <list_failed.txt>` 读这些行，把 `(prefix, token)` seed 进 BFS 队列，`processPrefix` 从 token 续页。
  - `parse_failed.txt` — `-check-file`/`-backup-file`/`-resume-list` 输入解析失败（坏行/bkt 不匹配/whitespace-only 行/无效 `%XX` 转义）。写入整行原始内容。**不可自动补跑**，需人工修输入文件。
  - `list_parse_failed.txt` — **仅 mode=offset + S3 LIST**：multipart ETag 解析失败（服务端未实现 header / ETag 格式不符 / offset > Size）。根目录全局，行格式 `bucket\x1fkey`（对齐 `check_failed.txt`）。**喂回 `-check-file` 不会真正检查**：HEAD 不带分段边界，这些行会落到 `mp.txt`（任何 `multipart_check_mode` 下都是），要检查需人工补 part 边界。配套 `list_parse_failed.log`（根目录 slog，字段 `bucket/key/owner/size/etag_len/etag`，完整 ETag 无截断）。**与 mp.txt 的区别**：mp.txt 是 mode=0 主动关闭 / check-file 类型漂移的未校验 multipart；list_parse_failed 是 mode=offset 下 ETag 本应解析但失败的对象。
  - **契约**：整个工具的行格式都基于 `\x1f`（ASCII Unit Separator）分隔（`bkt\x1fkey\x1fpartcnt\x1foff...` / `prefix\x1ftoken` / `bkt\x1fkey`），server 字段（key/prefix/token）写入时 `url.QueryEscape`、读取时 `QueryUnescape`，含 `\x1f`、`%`、`+`、`/` 的 key 都能逐字节往返。bucket 字段来自 CLI 不转义，数字字段（partcnt/offsets）不转义。**为什么弃用 pipe `|`**：S3 key 可合法包含 `|`，旧 pipe 分隔会把含 `|` 的 key 静默错切。旧的 `invalid_keys.txt` 机制（拦截含 `|` 的 key）已整体移除——`\x1f`+转义让任何 key 都能往返，拦截成死代码。
  - **stats 字段**：`ParseFailed`（输入解析失败计数）、`ListParseFailed`（mode=offset ETag 解析失败计数）。summary 中 ModeListCheck 打 `list_parse_failed`，ModeCheckFile/ModeBackup 打 `parse_failed`。**无 `InvalidKeys`**——机制已移除，含 `|` 的 key 走正常路径。

5. **checker goroutine 绝不退出**：任何错误写 `check_failed`（普通对象）/ `mp_check_failed`（多段分段）后继续。若 checker 退出，`objCh` 无人消费，list worker 永久阻塞。

6. **`objCh` 永远会被关闭**：list 阶段保证（`listWg.Wait` → `close(objCh)`；或 Mode 1 无 seed 时显式 `q.Close()`）。

7. **Add-before-Push 顺序**（BFS 终止性）：Mode 2 在 push 新 subprefix 前 `inflight++`，否则 push 后 worker 消费完 inflight 已归零，新 subprefix 无人处理 → 永久挂起。改动 `lister.go` 的 `processPrefix` 时务必保留此顺序。

8. **Mode 1 不 seed 根 prefix**：根的直接对象由 `main` 用 delimiter 分页拿（Contents → objCh）；只把 CommonPrefixes（子目录）seed 进队列。若 seed 根，worker 会用 `delim=false` 平铺列出根，与 main 的 delimiter 分页重复，根下子目录对象被计两次。

9. **`-nextmarker` 仅 Mode 1 生效**：作为根列举的 start-after 参数。Mode 2 忽略（文档化限制）。

10. **节点故障分类与阈值隔离**：`isNodeFaultErr` 的判定顺序**必须**是 ① `errors.As(err, &*awshttp.ResponseError)` 且**有真实 HTTP 响应**（`StatusCode != 0`——aws SDK 也用 ResponseError 包传输层发送失败，此时 StatusCode=0，必须放行到 ②③ 否则死节点拨号被判业务错误、failover/恢复静默失效）按状态码裁决（5xx=节点故障；4xx=业务错误；**501/505 除外**——能力/协议缺口换节点无意义）→ ② `errors.Is(context.DeadlineExceeded)` → ③ 字符串特征串（`nodeFaultSigs`，兜底传输层错误——含被 SDK 包在 ResponseError(StatusCode=0)/OperationError 里的 net 栈错误）。顺序不可颠倒：4xx 的 Message 是服务端文案，可能含 "timeout"/"EOF" 等传输层词汇，字符串匹配放在前面会把业务拒绝误判成节点故障、错误隔离健康节点。隔离是**阈值化**的：每次 `isNodeFaultErr` 命中调 `pool.RecordFault`（进程级 per-node 累积计数，无衰减），达到 `node_isolate_threshold`（默认 3，1=旧即时隔离行为）才 `MarkFailed`。重试恰好一次，且**重试一律换节点**：已隔离（本 worker 触达阈值或他人已隔离）→ `Assign` 轮询跳过隔离节点；未达阈值 → `AssignOther` 跳过当前节点（仅剩自己时退化为同节点重试，全隔离返回 -1 不重试）——**绝不**在刚故障的节点上同节点重试：节点真死时同节点重试必失败，会把本可在健康节点完成的工作项写进 `list_failed`/`check_failed` 丢覆盖。再失败按业务错误处理。注意 aws SDK 的 standard retryer 对 500/502/503/504 和连接层错误有**内部重试**（默认 3 次尝试）——工具侧的一次计数在 SDK 内部已是多轮失败；507 不在其重试集（failover 测试依赖这一点测量单次故障）。流式调用（`PutObjectStream`/`UploadPart`）不可重放、不参与 failover。**隔离默认可通过后台探测恢复**（`node_recover_probe_interval`，默认 60s，0=禁用恢复）：每轮对隔离节点 HEAD bucket，健康标准是 `!isNodeFaultErr`（与隔离标准互为镜像），**连续 2 次**健康才 `Unmark` 重返轮询池，同时**清零该节点故障计数**（否则恢复后再 1 次故障即再隔离，阈值名存实亡）；探测中失败即打断连续计数。探测走独立的一次性 client，不经过 `S3Client` 故障计数路径。

11. **性能优先但可读**：HTTP keep-alive（注入 aws client 的 `http.Transport` 自带连接池，不要自建）、`bufio.Writer` 64KB、合理 channel 容量、避免 per-obj 分配。**但**任何"复杂难读"的优化（手写内存池、unsafe、lock-free 结构）需先向用户请求确认，不要直接写。见 `memory/performance-vs-readability.md`。

12. **V1/V2 分页协议对 caller 透明**：`S3Client.listPageOnce` 按 `cfg.ListAPIVersion` 分派 `s3.ListObjects`（V1，marker 游标）或 `s3.ListObjectsV2`（V2，continuation token）。两条路径都归一化进 `listResult{contents, commonPrefixes, next}`，`next` 作为下一次 `ListPage` 的 `continuationToken` 参数回传。V1 无 delimiter 且 `IsTruncated=true` 但 `NextMarker` 为空时，回退到最后一个 Contents key 作 marker；有 delimiter 时 S3 返回 `NextMarker`。caller（lister/main 根分页）只需把 `next` 喂回 `continuationToken`，不感知 V1/V2 差异。`S3Client.api` 是 `awsS3API` 接口（非 `*s3.Client`）以支持测试注入。aws SDK 所有 API 带首参 ctx——本工具没有任何路径取消它，调用方一律传 `context.Background()`；单次调用的超时由各方法自己的 `WithTimeout` 兜底，LIST 没有单次超时，靠 `dial_timeout` / `response_header_timeout` 配置兜底）。

13. **多段分段检查的失败分流**（mode=1 与 -check-file 共用路径）：分段 RangeGet 报错走 `mp_check_failed` 路径（`WriteMpCheckFailed` + `IncrMpCheckFailed`），**不走** `check_failed`。任一段命中 chunk-signature 即视为整段对象损坏，写 `<ownerID>/corrupted_mp.txt` 并 `IncrCorruptedMp`（同时**不** `IncrOkMp`）。干净的多段对象 `IncrOkMp`，主扫描仅 `is_multipart_success_log=true` 时写 `<ownerID>/ok_mp.txt`（与普通对象的 `is_success_log` 独立，互不影响）；`-check-file` 恒写（见不变量 17）。**ok_mp.txt 行格式同 corrupted_mp.txt**（`WriteMultipartOk(ownerID, key, offsets)` 接收 task.Offsets）：`bkt\x1fkey\x1fpartcnt\x1foff...`。**corrupted_mp.txt 行格式**：`bkt\x1fkey\x1fpartcnt\x1foff0\x1foff1\x1f...`（offsets 是真实 part 边界，文件可直喂 -backup-file/-check-file）。**mp_check_failed.txt 行格式对齐 corrupted_mp.txt**：`bkt\x1fkey\x1fpartcnt\x1foff0\x1f...`（`WriteMpCheckFailed(key, offsets)` 接收 task.Offsets），同样可直喂 -backup-file/-check-file 补跑。**check_failed.txt 行格式 `bkt\x1fkey`**（`WriteCheckFailed(key)` 内部拼 `o.bucket + rowSep + url.QueryEscape(key)`，rowSep=`\x1f`），对齐 -check-file/-backup-file 普通对象 2 字段输入，可直喂补跑（不需要 ETag/Size：check-file 会 HEAD 重取，backup 中转也会 HEAD）。**owner 不写入** check_failed/mp_check_failed（文件输入模式不读 owner，输出走全局 root 不按 owner 分桶）。

14. **统计字段命名**（display name / Go 字段）：`list_obj` (`ListedObjects`) / `list_mp` (`ListedMp`) / `list_all` (`ListedAll=list_obj+list_mp`) / `ok_obj` (`OkObjects`) / `corrupt_obj` (`CorruptedObjects`) / `ok_mp` (`OkMp`) / `corrupt_mp` (`CorruptedMp`) / `list_failed` (`ListFailed`) / `list_parse_failed` (`ListParseFailed`，mode=offset ETag 解析失败计数) / `check_failed` (`CheckFailed`) / `mp_check_failed` (`MpCheckFailed`) / `read` (`ReadLines`，文件输入模式的累计已读输入行数，每读一行 +1 含坏行；**不统计总行数**——预扫描整个输入文件不值得)。**关键语义**：`ok_mp` 只在 `multipart_check_mode!=0` 且通过分段检查时 +1；mode=0 时多段对象只计 `list_mp`，**不**计 `ok_mp`——未校验不能谎称干净（mode=1 的 list_parse_failed 回落对象同理只计 `list_mp`）。`is_check=false`（list-only）时 `ok_obj`/`corrupt_obj`/`ok_mp`/`corrupt_mp`/`check_failed`/`mp_check_failed`/`list_parse_failed` 全部为 0，summary 不输出这些字段。**无 `invalid_keys` 字段**——`invalid_keys.txt`/`InvalidKeys` 已整体移除（`\x1f`+转义让任何 key 都能往返，拦截成死代码）。**窗口化延迟**：`stats.listLatency *LatencyTracker`（latency.go）按 `latency_window_sec`（默认 60s）维护滑动窗口，每窗口重置；`AddListCall` 双写全局计数（`ListCalls`/`ListAvgLatencyMs`）与窗口（`ListLatencyAvgMs`/`ListLatencyMaxMs`）。summary 用全局 `list_avg_ms`，progress 默认分支用窗口 `latency_avg_ms`/`latency_max_ms`——长跑全局平均会被早期 spike 稀释、掩盖当前批次健康，窗口化暴露当前批次。

15. **进度行与 summary 按 `RunMode` 输出指标集**：`ModeListCheck`/`ModeListOnly` 用 list/check 全量字段；`ModeCheckFile`（-check-file）打 `read=X` + `ok_obj/corrupt_obj/ok_mp/corrupt_mp/check_failed/mp_check_failed`（该模式重查普通+多段两种对象）；`ModeBackup`（-backup-file）打 `read=X` + `backup_ok/backup_failed/backup_mismatch`。文件输入模式**不**输出 `list_all`/`list_calls`/`list_obj` 等 list 指标——无 S3 LIST，全是 0 噪声。`q=` 队列快照同理按模式裁剪（文件模式无 BFS 队列，backup 用 `BackupChannelSnapshot`）。

16. **探测矩阵与 regex 分流**（见 §1.1 故障背景的三类 streaming 类型）：`chunkSigRe` = `^[0-9a-fA-F]+;chunk-signature=[0-9a-fA-F]{64}\r\n`（`^` 锚定：分片头只在对象或 part 的第 0 字节才算命中，探测窗口中间出现同样的文本是对象数据、不算。探测窗口都从对象或 part 的起点开始读，真实的分片头一定落在第 0 字节；行尾 `\r\n` 是 RFC 7230 chunked 强制的 CRLF，AWS SDK 全程用 `\r\n`）；`trailerRe` = `0\r\nx-amz-checksum-[a-z0-9]+:[^\r\n]*\r\n\r\n` 抓 STREAMING-UNSIGNED-PAYLOAD-TRAILER 的段尾/对象尾 trailer marker（unsigned 变体段首是弱特征 `<hexlen>\r\n`，不能当判据，判损坏**只能**靠 trailerRe；`chunkSizeLineRe` = `^[0-9a-fA-F]+\r\n` 不作判据，只作 trailerRe 的前提：尾部标记只有在对象以 chunk 长度行开头时才算损坏；algo 用 `[a-z0-9]+` 而非枚举，防未来 AWS 新算法漏判；不要求前导 `\r\n`——空对象 payloadSize==0 无前导 `\r\n`，要求会漏判空对象）。**regex 按 task 形状分流**：单次 PUT（`IsMultipart=false`）由 `probeSinglePut` 判定，规则是开头命中 `chunkSigRe`，或者对象以 chunk 长度行开头且尾部命中 `trailerRe`（单次 PUT trailer 上传能落盘，trailerRe 必须守护）；多段（`IsMultipart=true`）→ **仅 `chunkSigRe`**（多段 trailer part 不落盘，私有存储报错，见根 `AGENTS.md §1`；多段 part 只可能是普通 PUT 或 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`，段首都有 `;chunk-signature=`）。多段的 `matchFunc(task, probes)` 负责把锚点对到每个 part 的起点：每 part 一次探测的计划里，探测从该 part 的段首开始读，锚点就是 body 的第 0 字节；多段对象小到走 (a) 的一次全读时，所有 part 的段首都在同一个 body 里，按 `task.Offsets` 逐个起点匹配。**list-check 与文件检查模式（-check-file）共用 `runProbes`**（返回 `probeClean` / `probeCorrupted` / `probeFailed` 三态，调用方各自 routing：checker.go verify 写 corrupted/check_failed/ok_*；backup.go **不再探测**——见不变量 19）。`runProbes(ctx, ...)` 的 ctx 由调用方传入——`Checker` 传 `context.Background()`，进程内没有任何路径取消 in-flight 探测。`buildProbesStatic(task, threshold)` 是包级函数（threshold 从 cfg 传入，runProbes 无 cfg 依赖），按 task 形状产出探测计划：(a) `Size==0` 在 `Handle` 短路（不请求）：空对象没有字节，不可能有分帧残留，判干净——普通对象记 `ok_objects`，带 offsets 的多段对象记 `ok_mp`（行带 partcnt+offsets）；没有 offsets 的多段对象不走这条短路，仍由 `verify()` 记为未校验（`mp.txt` / `list_parse_failed.txt`）。对空对象发 Range 请求会得到 416（起点 0 已超出对象大小；终点超出不报错，所以非空的小对象照常探测），修复前空的多段对象因此每一轮都落 `mp_check_failed`、重试也清不掉（2026-11-27 在本地 minio 上复现并验证）；`0<Size<=whole_object_probe_threshold` → 单次 `RangeGetAt(0, thr)` 全读，1 请求（单次 PUT 按 `probeSinglePut` 判定，多段按 `matchFunc`）；(b) 单次 PUT `Size>thr` → head@0(128B)：命中 chunkSigRe 即损坏；head 不以 chunk 长度行开头 → 判干净，tail 不读（1 请求）；以 chunk 长度行开头 → 再读 tail@Size-128(128B) 抓 trailer marker（2 请求）。(a) 与 (b) 是同一条规则，依据：服务端把请求体原样落盘，unsigned-trailer 的 body 一定以 chunk 长度行开头，其他 head 的对象尾部不可能是它的 trailer；(c) 多段 N part `Size>thr` → **每个 part offset 一次探测 `[off_i, off_i+128]`(128B) = N 请求**（多段只跑 chunkSigRe，它匹配段首 `;chunk-signature=`；每个 part 段首由自己的探测覆盖，无 tail probe——读末段段尾永不命中）。探测 length 按 Size clamp（`min(Size, off_i+128)`）。任一探测命中即 early-exit 判 corrupted（普通→`corrupted_objects.txt`，多段→`corrupted_mp.txt` 带 offsets），不变量 14 的失败分流与行格式不受影响。`whole_object_probe_threshold=0` 禁用快路径恒走多探测。

17. **文件检查模式（-check-file）的 HeadFirst 与 HEAD 权威判型**：输入行不带 ETag/Size（`parseCheckFileLine` 返回 `HeadFirst=true`），`Checker.Handle` 检测 `HeadFirst` 先 `HeadObject(c.ctx)` 填充 ETag/Size，**并以 HEAD ETag 为权威判型**（输入行描述的是上一轮检查时的对象，期间可能被覆盖）：HEAD 普通 + 行多段 → 丢弃 stale offsets 按 head+tail 探测（结果路由到普通对象文件）；HEAD 多段 + 行普通（无 offsets）→ `verify()` 路由 `mp.txt` 不认干净（无边界无法逐段验证），任何 `multipart_check_mode` 下都如此——`verify()` 按 `Output` 是否开了 `list_parse_failed.txt` 分流，该文件只在主扫描的 offset 模式开启。**HEAD 失败按行声明的类型分流**（HEAD 失败时行是唯一信息源）：普通行 → `check_failed`，多段行 → `mp_check_failed`（保留 offsets）。**若不 HEAD**：`buildProbesStatic` 的 `if task.Size > 0` 门会跳掉 tail 探测，段尾 trailer marker 静默漏判（历史 bug）。list-check 模式不设 `HeadFirst`：S3 LIST 已带 ETag/Size，无需再 HEAD。-check-file 是 mixed 格式（`parseCheckFileLine` 委托 `parseMixedLine`，与 -backup-file 同一解析器）。**成功与失败都记录**：`ok_objects.txt` / `ok_mp.txt` 恒写，不受 `is_success_log` / `is_multipart_success_log` 控制（两个开关只管主扫描）——每一行输入都要留下结果行，与失败文件、备份模式一致。门控在 `Output`：`Checker` 无条件调用 `WriteSuccess` / `WriteMultipartOk`，由 Output 按模式和开关决定是否落盘。**输入读不出来即失败**（`-backup-file` 同）：输入文件打不开或读取中途出错不是坏行，不进 `parse_failed`；已读入的行照常处理、汇总照常打印，然后 `run` 返回该错误，进程退出码为 1。没有这一条时，一轮 `read: 0`、失败文件全空的运行会被「失败文件为空即收敛」读成已完成。

18. **backup 模式不探测损坏**（`-backup-file`）：`BackupChecker.Handle` = HEAD → 判型门（mismatch）→ 中转 → ETag 终验。**不调用 runProbes**——输入列表是检查阶段的产物（corrupted_objects/corrupted_mp 跨轮并集），是否损坏已判定，备份只保字节（不因"干净"跳过；`backup_skipped_clean` 全链路不存在，勿加回）。mismatch 判型门保留（HEAD 顺手可得、零成本；拦"检查后对象被改写"——stale offsets 中转必然 ETag 终验失败），mismatch.txt 行可直喂 `-check-file` 重查。HEAD 保留是必须的：中转需要 Size，ETag 终验需要源 ETag。`BackupChecker` 不持 ctx，中转不可中止：进程被杀时（Ctrl-C / kill 与 kill -9 相同），未完成的 multipart upload 会遗留在备份桶，见 §7.4。

19. **失败文件即断点，不拦截信号**（断点续传的总体设计；设计文档 `docs/superpowers/specs/2026-11-27-remove-signal-handling-design.md`）：
  - **架构原则**：不做 per-object done-set（十亿级对象的逐对象记账是写放大灾难；probe 幂等；LIST 天然带 continuation token；append-only 文件 + 每轮新时间戳目录构成检查点。例外：Mode 2 的**页级** journal——1 行/1000 对象，见不变量 21）。**每个失败文件有且仅有一个补跑入口**：`list_failed.txt` → `-resume-list`（仅 Mode 2；Mode 1 根/子前缀游标语义在行内不可区分——其列举失败重试 = 原命令重跑，union 吸收重复）；`check_failed.txt`/`mp_check_failed.txt`/`mismatch.txt` → `-check-file`（循环喂回至收敛）；`list_parse_failed.txt` → 人工补 offsets（行格式 `bucket\x1fkey` 对齐 check_failed.txt，但喂回 `-check-file` 只会落到 `mp.txt`——HEAD 不带分段边界）；`backup_failed.txt` → `-backup-file` 自喂；`parse_failed.txt` → 人工；`mp.txt` → 人工补 offsets。跨轮逻辑结果 = 各 run 目录并集（`sort -u` 去重），收敛判据 = 最新一轮失败文件为空。
  - **进程不拦截任何信号**：没有 `signal.Notify`，Ctrl-C / kill / kill -9 效果相同——立即终止，不执行任何收尾代码（不刷输出缓冲、不写 journal 的最终检查点、不打印汇总）。没有“优雅停机”这一档；续跑能力只来自两样：Mode 2 的 `journal.txt` 快照，和文件输入模式的结果文件。
  - **被杀之后怎么续跑**：Mode 2 扫描由 journal 兜底（不变量 21）——损失 ≤ 一个 checkpoint 间隔（`journal_checkpoint_interval_sec`，默认 600 秒的工作量），恢复入口 `-resume-journal`；Mode 1 与文件输入模式是"未处理部分无记录"：Mode 1 的补救 = 原命令重跑（幂等 + union）；文件输入模式（`-check-file` / `-backup-file`）用结果文件按 key 求差集得到重跑输入（§7.4）。已写管线的尾部丢失由 writer 周期 Flush 兜底（`outputFlushInterval`=3s，见 §6）：被杀时每文件最多丢最后 ≤3s 的行，末行可能是半行（含失败文件即补跑输入本身）。无 fsync：write() 落 page cache 即扛进程死，fsync 只防整机断电（另一级耐久性，不需要）。

20. **Mode 2 journal：kill -9 断点续跑**（`journal.go`，spec `docs/superpowers/specs/2026-11-05-resume-journal-design.md`）：
  - **启用条件**：`list_type=2 ∧ 文件输入=无`——全新扫描、`-resume-list` 恢复轮、`-resume-journal` 恢复轮都记账（`-resume-list` 轮同样长跑、同样会被 kill）。`journal.txt` 落输出目录根，append-only；恢复后写新时间戳目录的新 journal。
  - **三行格式**（`\x1f` 分隔，server 字段 `QueryEscape` 转义——单版本策略，旧 `|` 格式不可读）：`discovered\x1f<prefix>` / `cursor\x1f<prefix>\x1f<token>` / `done\x1f<prefix>`。解析规则：discovered/done 的 prefix = `QueryUnescape` 余量；cursor 在余量按 `\x1f` 切分两个字段各 `QueryUnescape`（含 `|`/`\n` 的 token 也能逐字节往返）；重放取每 prefix 最后一条事件；末尾无换行的残行丢弃（kill 撕裂批次，安全方向）；其他坏行报错（机器写的文件，中间坏 = 真损坏）。
  - **"完成"的语义**（正确性核心）：prefix 的某页"完成" = 该页**全部**对象的结果行已推入输出 channel——不是"列举过"、不是"发射过"。页的待终态计数在 `ListPage` 成功时按 `len(objs)` **整页一次性登记**（只上报了一部分对象时该页永远不算完成 → 水位停在它前面）；`Terminal` 由 checker `Handle` 的 **defer** 上报（体 = 结果行已推 channel）；list-only 分类即终态。
  - **顺序不变量**（违反任意一条 = 覆盖漏洞）：① `AddDiscovered` 必须先于该页任何对象发射（否则"父页已终态、子前缀未记账"可同框快照，子树蒸发）；② checkpoint 固定顺序 = **快照 tracker → `Output.FlushAndWait` → 写批次（discovered 行在 cursor/done 行前；写短了回滚到批次前偏移，防撕裂残行被下批嫁接）→ Commit**——先快照后 flush 的因果：Terminal 在行推入 channel 之后才计数，快照里的每个终态的行必在该次 flush 覆盖内；③ `journalRun.Stop()`（同步等最终 checkpoint 完成）必须在 `out.Close()` **之前**——FlushAndWait 依赖 writer goroutine 存活。
  - **checkpoint 时机**：种子落盘靠 **seeding 完成后立即触发的同步 checkpoint**（`JournalRunner.CheckpointNow`，main 在 seeding 分支链结束后调用）——checkpoint 循环先于 seeding 启动，其启动首检时刻 tracker 必为空；种子必须靠显式触发而非等首个 tick（否则头一个间隔内 kill = 空 journal 不可恢复）。此后每 `journal_checkpoint_interval_sec` 秒一次（配置项，默认 600；负数启动报错），ctx 取消时终检一次。间隔作为参数传给 `StartJournalCheckpoints`，没有包级变量：`run()` 取配置值（不经 `LoadConfig` 构造的 Config 里它是 0，回退到默认值），测试直接传入自己要的间隔。游标行按 `\x1f` 切分两个 `QueryEscape` 字段，V1 的 NextMarker 是真实 key 可能含 `|`/`\n`——转义让它们也能逐字节往返。恢复入口 `-resume-journal`：done 跳过、cursor 用 `SeedWithToken` 续、仅 discovered 裸列；重放得到的前缀表由 `seedFromJournal` 交给 `Lister.resumed` 留用整轮——续列的前缀重列游标那一页时，页上已在 journal 里的子前缀既不再登记也不再次入队（否则它们会在续跑轮内被列两遍，本轮 journal 里还会把已 done 的子前缀写回成未完成）；**与 `-prefix` 互斥**（journal 即 scope）；空 journal 报错提示用原命令重跑（CheckpointNow 之后仍空 = 路径错/手工清空）。
  - **内存**：Commit 后 done 前缀从内存 map 删除、水位以下簿记丢弃（journal 文件是权威全量）——活跃内存 = 一个周期的增量 + in-flight，不随桶规模增长。
  - **验收标准**：任何出错/竞态的后果只能是"恢复时多做工作"，不允许"有对象漏检"——单测钉死：`TestTrackerFrozenOnPartialPage`、`TestCheckpointOrderAndDurability`、`TestJournalResumeRoundTrip`、`TestSnapshotEmitsCursorWithPipeOrNewlineToken`（含 `|`/`\n` 的 token 往返）；端到端冒烟见 §7.5（kill -9 → 三态 journal → 续跑 → 并集完备性，2026-11-05 实跑通过）。

21. **中转 PUT 的 UNSIGNED-PAYLOAD**（`PutObjectStream`/`UploadPart`，`withUnsignedPayload`）：aws SDK v2 **无法**在纯 HTTP 上签名不可 seek 的流——payload-hash middleware 需要读完回卷，aws-chunked trailer 逃逸仅在 TLS 下启用，裸跑必报 "request stream is not seekable"。因此中转 PUT 一律换 `x-amz-content-sha256: UNSIGNED-PAYLOAD`（裸 body 直发、无分帧、无缓冲）。这同时是**特性**：中转的字节保真由 ETag 终验端到端把关，不依赖传输层签名；且 wire 上永远不出现 aws-chunked 帧形态（对落盘了分帧的那台故障服务器而言，任何新上传都不可能再产生同类损坏）。**不要**移除该选项"恢复签名"——会回到不可 seek 报错。seekable 源（`PutObjectLocal` 的文件、测试里的 `bytes.Reader`）走默认 full-payload-hash 裸发。伪服务器解码 PUT body 必须按 `Content-Encoding: aws-chunked` 条件分支（`decodePutBody`），无条件解码会把裸 body 解成空串。

22. **wire 层双重边界编码**（`s3client.go` `wireDecoder` + `output.go` `rowSep`）：server 字段（key/prefix/marker）跨两个边界，每个边界都做一次 `url.QueryEscape`/`QueryUnescape`，且**仅做一次**：
  - **边界 A（wire 出口）**：`encoding_type_url=true` 时 LIST 请求带 `EncodingType=url`，wire 上 server 字段 URL 编码（保护 XML 反序列化免受 `\x1f` 等 XML 1.0 非法控制字符撕裂）。SDK 不解码，出口由 `wireDecoder.decode(raw)` 统一 `QueryUnescape`——`ListPage` 对 `Contents.Key`、`CommonPrefixes.Prefix`、V1 `NextMarker`/Contents-key-fallback 都调 `decode`；**V2 `NextContinuationToken` 不解码**——它是 opaque base64，可能含字面 `+`（probe-verified minio 语义），`QueryUnescape` 会把 `+` 变空格撕裂 token。wire decode 失败（stray `%XX`——server bug）保留原值 best-effort，`listLogger.Warn` 一次/页（非 debug_log 门控——生产信号不能被吞）。
  - **边界 B（文件写入）**：`output.go` 的 `WriteListFailed`/`WriteCheckFailed`/`WriteMpCheckFailed`/`WriteListParseFailed`/`renderMultipartOffsets`/`renderLineFmt` 对 server 字段（key/prefix/token）`url.QueryEscape`，读取侧（`resume_list.go`/`backup_source.go`/`multipart_line.go`/`journal.go`）在 `\x1f` 分割后对每段 `QueryUnescape`。bucket 字段来自 CLI、不转义；partcnt/offsets 是纯数字不转义；owner 非空时为 `[a-z0-9]+`（server 约定），`QueryEscape` 对它是 no-op，作目录名时走 `ownerDirName` 过滤（空/`.`/`..` 精确匹配或含 `/`/`\` → `_unknown`）是防越狱防御，不是编码。
  - **行分隔符 `rowSep = "\x1f"`**（ASCII Unit Separator）：S3 key 可合法包含 `|`，旧 pipe 分隔会把含 `|` 的 key 静默错切。`\x1f` 是 ASCII 控制字符，不可能出现在 S3 key 里。
  - **`invalid_keys.txt` 已整体移除**：`\x1f`+转义让任何 key 都能往返，旧"拦截含 `|` 的 key 到 invalid_keys"成死代码。`InvalidKeys` stats 字段、`invalidKeysCh`、`WriteInvalidKey`、`strings.Contains(o.Key, "|")` 短路检查全部删除。
  - **单版本策略**：旧格式（`|` 分隔、未转义）的文件不可读——记录文件只用最新版本，不做旧格式兼容。例外：`-resume-list` 手写纯 prefix 行仍是活功能（空 token 时单字段 prefix 是合法条目）。
  - **probe-verified minio 语义**（RELEASE.2025-09-06T17-38-46Z）：V1 `NextMarker` 出口编码、V2 `NextContinuationToken` 不透明 base64 不解码、`url.QueryUnescape` 对字面 `+` 的处理（wire 上 `+` 恒为空格，字面 `+` key 在 wire 上已转义为 `%2B`，无歧义）。

23. **short-response quirk 检测**（`lister.go` `checkShortResponse`）：S3 LIST 返回 `IsTruncated=true` 但 `entries < max_keys` 是矛盾信号——服务端分页错误。`checkShortResponse(out, prefix, token, entries, truncated, maxKeys)` 在每个 LIST 页（lister / Mode 1 根循环）调用：`maxKeys <= 0`（无 LoadConfig 的直接 Config）禁用检测；`!truncated` 直接返回（非末页无需检查）；`entries >= maxKeys` 直接返回（正常情况）；否则 `out.WriteShortResponseWarning(prefix, token, returned, maxKeys)` 写到 `listLogger`（非 debug_log 门控）——生产信号不能被吞。`max_keys` 不配时默认 1000（`LoadConfig` 填入）；为 0 的只有不经 `LoadConfig` 直接构造的 Config。

24. **无 common-prefix 启发式**（坑 11）：Contents 与 CommonPrefixes 由 wire XML 标签权威区分，**绝不**引入客户端 `HasSuffix(key, "/")` 启发式——`delete_marker_list_tool` 曾因此静默丢对象。任何 key（包括以 `/` 结尾的真实对象）都按 wire XML 标签路由到 objCh 作普通对象；common prefix 只来自 `CommonPrefixes.Prefix`。回归钉：`TestListerSlashSuffixKeyFlowsAsObject`（check 模式）、`TestListerSlashSuffixKeyFlowsAsObject_ListOnly`（list-only 模式）。

25. **窗口化 LIST 延迟统计**（`latency.go` `LatencyTracker`）：长跑的全局平均延迟会被早期 spike 稀释、掩盖当前批次健康。`LatencyTracker` 按滑动窗口（`latency_window_sec`，默认 60s，负值启动报错）维护 `count`/`sumNs`/`maxNs`，每窗口重置；`Record(d)` 与 `Snapshot()` 在窗口过期时先 rotate。零值可用（1 分钟默认窗口首用锁定，`SetWindow(<=0)` 保留默认）。`Stats.AddListCall` 双写全局计数（`ListCalls`/`ListAvgLatencyMs`，summary 用）与窗口（`ListLatencyAvgMs`/`ListLatencyMaxMs`，progress 默认分支用）。窗口化 `latency_avg_ms`/`latency_max_ms` 让操作员能在长跑中看到当前批次的延迟趋势，而非被早期 spike 主导。回归钉：`TestLatencyTrackerWindowedStats`、`TestLatencyTrackerRotatesOnWindowExpiry`、`TestLatencyTrackerZeroValueUsable`、`TestLatencyTrackerSetWindowMidRun`、`TestLatencyTrackerMonotonicMax`、`TestLatencyTrackerSnapshotIsIdempotent`、`TestLatencyTrackerConcurrentRecord`、`TestStatsWindowedListLatency`。

26. **debug_log 每次 LIST 请求诊断**（`s3client.go` `listPageOnce`）：`cfg.debug_log=true` 时每个 LIST 调用打三行 Debug 级 slog（路由 stdout → run.log）——`list-page req`（marker/token/delim/maxKeys，调用前）、`list-page err`（req_id + 完整 err，失败时）、`list-page resp`（next/truncated/nContents/nCP，成功时）。V1 日志 `marker`（V1 cursor），V2 日志 `startAfter` + `token` 独立字段；V1 resp 的 `next` 是出口解码后的原始 key 形态，V2 resp 的 `next` 是不透明 base64 token 原样。所有 6 处 `Debug` 调用 nil 守卫（`if c.debugLogger != nil`），无 logger 时不打。`main` 在 `cfg.DebugLog=true` 时构造 `slog.New(slog.NewTextHandler(stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))` 注入到所有 worker。回归钉：`TestListPage_DebugLogEmitsReqRespLines`、`TestListPage_DebugLogEmitsErrLine`。`extractRequestID(err)`（`errors.As` 到 `*awshttp.ResponseError`）nil 安全，已在 backup/checker 多处使用。

## 5. CLI 与配置

### CLI flags
```
-c <path>          # 配置文件，必填
-bkt <bucket>      # 桶名，必填
-prefix <prefix>   # 列举前缀，可选，默认空
-nextmarker <key>  # start-after key，可选；仅 Mode 1 根分页使用
-check-file <path> # 重查文件，可选；mixed 格式（bkt|key / bkt|key|partcnt|off...），check_failed/mp_check_failed/mismatch 的统一重试入口（与其他文件 flag 互斥，需 is_check=true）
-backup-file <path># 备份列表文件，可选；HEAD 判型+中转+ETag 终验，不探测损坏（与其他文件 flag 互斥，需配置 backup_bucket + backup_output_dir）
-resume-list <path># 断点续跑文件，可选；读 list_failed.txt 格式 `prefix|token` 把 (prefix, token) seed 进 BFS（与其他文件 flag 互斥，仅 list_type=2 生效；Mode 1 的列举失败重试 = 原命令重跑）
-resume-journal <path> # kill -9 断点续跑文件，可选；读被 kill 轮的 journal.txt 重放扫描现场（done 前缀跳过、在途前缀从游标续列、未启动前缀从头列，恢复零重走）；与其他文件 flag 互斥，仅 list_type=2，-prefix 必须为空（journal 即扫描范围）
```

### config.yaml 字段
| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | ip:port 列表，至少 1 个 |
| `scheme` | `http` | `http` 或 `https`（后者跳过证书校验） |
| `ak` / `sk` | 必填 | SigV4 静态凭证 |
| `list_type` | `2` | 1=子目录+平铺 nextmarker；2=递归 BFS delimiter |
| `list_api_version` | `1` | 1=ListObjects V1（marker 分页）；2=ListObjectsV2（continuation token） |
| `list_concurrency` | `8` | 列举并发度 |
| `check_concurrency` | `16` | 校验并发度 |
| `output_dir` | `.` | 输出目录 |
| `output_dir_timestamp` | `true` | true=目录名追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`）隔离每次运行；false=固定使用 output_dir 原样路径（断点续跑需显式 false）。后缀在 `LoadConfig` 内追加（尾部 `/` 先裁剪），下游全部用改写后的 `cfg.OutputDir`；同一 `stamp` 同时作用于 `backup_output_dir` |
| `backup_output_dir` | （无，必填） | `-backup-file` 模式专用输出目录；与 `output_dir` 分离以免 backup 结果与 list/check 结果混写。`NewBackupOutput` 用 `cfg.BackupOutputDir` 建 `MkdirAll` 并写所有 backup 文件；list/check 模式忽略该字段。`main` 启动时若 `-backup-file` 而 `BackupOutputDir==""` 直接 `os.Exit(2)`，同样受 `output_dir_timestamp` 控制并与 `output_dir` 共享同一时间戳成对生成 |
| `is_check` | `true` | true=列举+校验；false=仅列举（不校验普通对象，不写对象文件，不创建 owner 目录，仅写 list_failed.*） |
| `is_success_log` | `false` | 主扫描是否记录正常普通对象到 `<ownerID>/ok_objects.txt`。`-check-file` 模式恒记录，不受此项控制 |
| `max_keys` | `1000` | 每次 LIST 请求的最大键数。不配时默认 1000（S3 单页上限）；配了就必须在 1..1000 内，否则启动报错。决定分页粒度；服务端单页响应上限。`checkShortResponse` 用它判定短响应 quirk（IsTruncated && entries<max_keys）。无 LoadConfig 的直接 Config（maxKeys<=0）禁用短响应检测 |
| `encoding_type_url` | `false` | true 时 LIST 请求带 `EncodingType=url`（V1/V2 都带）。wire 上 server 字段（key/prefix/marker）URL 编码，保护 XML 反序列化免受 `\x1f` 等 XML 1.0 非法控制字符撕裂。出口由 `wireDecoder` 统一 `QueryUnescape` 解码（边界 A，见不变量 23）；V2 `NextContinuationToken` 是不透明 base64 不解码。Wire decode 失败保留原值 best-effort，warn 一次/页到 `list_failed.log`。**在目标服务器上启用前必须先探测**——某些 S3 实现不支持该参数或解码行为不同（见 `memory/encoding-type-marker-semantics.md`） |
| `debug_log` | `false` | true 时每次 LIST 请求打三行 Debug 级诊断（`list-page req`/`list-page err`/`list-page resp`）到 run.log，记录 marker/token/delim/maxKeys/req_id/next/truncated/nContents/nCP。便于追 marker 游标漂移与短响应 quirk。nil 守卫——无 logger 时不打。详见不变量 27 |
| `latency_window_sec` | `60` | progress 行 `latency_avg_ms`/`latency_max_ms` 的滑动窗口秒数。每窗口重置，反映**当前批次**的 LIST 延迟健康（全局 `list_avg_ms` 会被早期 spike 稀释、掩盖当前批次问题）。0=用默认 60s；负值启动报错。详见不变量 26 |
| `multipart_check_mode` | `0` | 多段对象损坏检查模式：`0`=关闭（全部写 mp.txt 不检查）；`1`=offset 检查（LIST 带 `internal-list-mp-offset: true` header，多段 ETag 返回 `<md5>-<partcnt>-<off0>|<off1>|...`——服务端 ETag 内部分隔符仍是 `|`，由服务端生成不可改，按真实 part 边界逐段检查；解析不出 offsets 回落 list_parse_failed.txt，**不再**写 mp.txt）。非法值启动报错 |
| `whole_object_probe_threshold` | `1024` | 小对象全读阈值（字节）。`0<Size<=该值` 时走快路径：单次 `RangeGetAt(0, thr)` 读全对象：开头命中 `chunkSigRe`（`;chunk-signature=` 强特征）即损坏，或以 chunk 长度行开头且命中 `trailerRe`（`0\r\nx-amz-checksum-<algo>:<value>\r\n\r\n`）才损坏，1 请求覆盖段首+段尾两种损坏；超过该值走多探测：普通对象先读 head@0，head 以 chunk 长度行 `<hexlen>\r\n` 开头才再读 tail@Size-128（否则 1 请求即判干净）；多段每个 part offset 一次探测（无 tail probe）。`0`=禁用快路径恒走多探测。负值启动报错 |
| `is_multipart_success_log` | `false` | 主扫描是否记录干净的多段对象到 `<ownerID>/ok_mp.txt`。`-check-file` 模式恒记录，不受此项控制 |
| `node_isolate_threshold` | `3` | 节点隔离阈值：进程级累积的节点故障数（连接错误/超时/5xx，4xx 业务错误不计）达到该值才隔离节点；`1`=旧的首次故障即隔离。计数无时间衰减，跨 worker 共享 |
| `node_recover_probe_interval` | `60` | 隔离节点恢复探测间隔（秒）。后台 goroutine 每隔该值对隔离节点 HEAD bucket，连续 2 次健康（`!isNodeFaultErr`，即任何正常 S3 应答）→ 恢复进轮询池并清零故障计数；`0`=禁用恢复（隔离在进程内永久，旧行为） |
| `dial_timeout` | `10` | 单次 TCP 建连超时（秒）。建连阶段（SYN→SYN-ACK→ACK）上限。黑洞 IP（不路由、不回 RST）OS 默认 SYN 重试 ~75s（macOS）/ ~127s（Linux），默认 10s 兜底；`0`=Go net/http 默认（无超时，走 OS 默认）。负值启动报错。只控制建连阶段，不影响已建立连接上的请求/响应传输。所有 s3 client（含 `noderecovery.probeNode`）共用 |
| `response_header_timeout` | `0` | 等服务端响应头超时（秒）。请求发出后等 HTTP 响应头（status line + headers）的上限，不含响应体传输。服务端 accept 了 TCP 但进程挂死不响应时，这是唯一能检测的兜底。默认 `0` 不兜底（Go net/http 默认 = 无超时），用户遇到挂死服务端时显式配。负值启动报错 |
| `journal_checkpoint_interval_sec` | `600` | journal 快照间隔（秒）。`list_type=2` 扫描每隔该值把扫描进度写一次 `journal.txt`，被 kill -9 后最多重做一个间隔的进度（不变量 20）。调小更安全，写盘更频繁。负值启动报错。烟囱测试要缩短间隔，直接在 cfg 里改这一项（§7.5） |
| `progress_interval` | `5000` | 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；`0` 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；`0` 走默认 |

## 6. 输出文件（append 模式）

所有 `.txt` 结果/处理文件走 channel(1024) → bufio(64KB) → fd 的 writer goroutine；除 64KB 攒满与 Close 时 Flush 外，每 `outputFlushInterval`（3s，`output.go`）周期 Flush 一次——kill -9 的每文件损失上界从 64KB 缓冲压到 ≤3s 的行（失败文件即补跑输入，不能整段蒸发）。`.log` 文件走 slog 逐条直写（每条一次 write()），天然无此窗口。

### 结果文件（按 OwnerID 分目录，`<output_dir>/<ownerID>/<filename>`；OwnerID 为空 → `_unknown/`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏普通对象 key | Range GET 命中 chunk-signature/trailer（`is_check=true`） |
| `mp.txt` | 多段对象 key（仅 key） | `multipart_check_mode=0` 时所有多段对象；`-check-file` 时普通行 HEAD 判多段的漂移对象（无 offsets 不认干净）。`mode=offset` 的 ETag 解析失败**不**写 mp.txt（改写 list_parse_failed.txt） |
| `corrupted_mp.txt` | 损坏多段对象 key | `multipart_check_mode=1` 或文件检查模式（-check-file）分段检查命中。行带 `bkt\|key\|partcnt\|off0\|off1\|...`（真实 part 边界，可直喂 -backup-file/-check-file） |
| `ok_mp.txt` | 干净多段对象 key | 主扫描：`multipart_check_mode=1` 且 `is_multipart_success_log=true`；`-check-file`：恒写。行格式同 `corrupted_mp.txt`：`bkt\x1fkey\x1fpartcnt\x1foff0\x1f...` |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true`；`-check-file` 模式恒写 |

### 处理文件（全局，根目录 `<output_dir>/<filename>`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `list_failed.txt` | 列举失败 `prefix\x1ftoken`（token 为失败页 continuationToken，第一页失败时为单字段 prefix；未配 `-prefix` 时根 prefix 失败写空行，合法条目） | list worker 调用失败（list-only 模式也写）；可直喂 `-resume-list` 补跑（空行 = 根 prefix） |
| `list_failed.log` | 列举失败结构化错误（slog text，req_id/prefix/http_code/s3_code/err） | 同上 |
| `parse_failed.txt` | 输入解析失败的原始行（-check-file / -backup-file / -resume-list 坏行、bkt 不匹配、whitespace-only 行、无效 `%XX` 转义） | `fileSource`/`readResumeList` 解析失败时写；不可自动补跑，需人工修输入 |
| `list_parse_failed.txt` | mode=offset 下 ETag 解析失败的 multipart 对象 `bucket\x1fkey` | **仅 mode=offset + S3 LIST**：服务端未实现 header / ETag 格式不符 / offset > Size。行格式对齐 check_failed.txt；喂回 `-check-file` 只会落到 `mp.txt`（HEAD 不带分段边界），要检查需人工补 part 边界。配套 `list_parse_failed.log`（slog，bucket/key/owner/size/etag_len/etag，完整 ETag 无截断） |
| `check_failed.txt` | 普通对象校验失败 `bkt\x1fkey` | checker 普通对象 RangeGet/HEAD 失败；可直喂 `-check-file`/`-backup-file` 普通对象输入补跑 |
| `check_failed.log` | 校验失败结构化错误（slog text，req_id/key/http_code/s3_code/err） | 同上 |
| `mp_check_failed.txt` | 多段分段检查失败 `bkt\x1fkey\x1fpartcnt\x1foff0\x1f...` | `multipart_check_mode=1`（或文件检查模式）时分段 RangeGet/HEAD 失败；可直喂 -check-file/-backup-file 补跑 |
| `mp_check_failed.log` | 多段分段检查失败结构化错误（slog text） | 同上 |
| `backup_ok.txt` | 备份成功的原始输入行 | `-backup-file`：中转 + ETag 终验通过 |
| `backup_failed.txt` | 备份失败的原始输入行（可自喂重试） | `-backup-file`：HEAD 失败（stage=head）/中转失败（stage=upload）/ETag 终验失败（stage=etag，坏副本保留） |
| `backup_failed.log` | 备份失败结构化错误（stage/head→upload→etag） | 同上 |
| `mismatch.txt` | 输入类型校验失败的原始行（对象在检查后被改写） | `-backup-file`：行声明类型与 HEAD 判型不一致；可直喂 `-check-file` 重查当前状态 |
| `journal.txt` | Mode 2 扫描进度三行格式：`discovered\x1f<prefix>` / `cursor\x1f<prefix>\x1f<token>` / `done\x1f<prefix>`（append-only，每 `journal_checkpoint_interval_sec` 秒一批 checkpoint（默认 600），批内 discovered 行先于 cursor/done；server 字段 `QueryEscape` 转义） | `list_type=2 ∧ 非文件输入`的运行（含 -resume-list/-resume-journal 恢复轮）；恢复 = `-resume-journal` 直喂，重放取每 prefix 最后一条；详见 §4.21 |

`is_check=false` 时不校验普通对象，不写任何对象文件，不创建 owner 目录，仅写 `list_failed.*`。`is_check=true && multipart_check_mode=0`（非文件检查模式）时 `corrupted_mp.txt` / `ok_mp.txt` / `mp_check_failed.*` 不创建。文件输入模式的 owner 恒为空 → 结果全落 `_unknown/`。

### 结果文件行格式

所有结果/处理文件统一用 `\x1f` 分隔、server 字段（key/prefix/token）`url.QueryEscape` 转义（见不变量 23）。per-owner 结果文件行格式 `bucket\x1f<escaped-key>`（bucket 来自 CLI 不转义）；`corrupted_mp.txt`、`ok_mp.txt` 带真实 offsets（`bkt\x1fkey\x1fpartcnt\x1foff0\x1f...`）。处理文件（`list_failed`/`parse_failed`/`check_failed`/`mp_check_failed` 的 .txt 与 .log）按各自补跑链路需要的 `\x1f` 分隔格式写入：
- `list_failed.txt` = `prefix\x1ftoken`（token 为空时单字段 prefix）— `-resume-list` 解析此格式
- `parse_failed.txt` = 原始输入行（整行，不切分）
- `check_failed.txt` = `bkt\x1fkey` — 对齐 `-backup-file` 普通对象 2 字段输入
- `mp_check_failed.txt` = `bkt\x1fkey\x1fpartcnt\x1foff0\x1f...` — 对齐 `-backup-file`/`-check-file` 多段输入
- `list_parse_failed.txt` = `bkt\x1fkey` — 对齐 check_failed.txt
- `.log` 文件用 slog text handler，结构化字段（req_id/bucket/key/http_code/s3_code/err）
- `journal.txt` = `discovered\x1f<prefix>` / `cursor\x1f<prefix>\x1f<token>` / `done\x1f<prefix>` — server 字段 `QueryEscape`

### 启动输出 / run.log

run.log 是进程运行日志，路径为`<output_dir>/run.log`。`output_dir_timestamp=true`（默认）时 `LoadConfig` 已先把 `output_dir` 改写为 `<output_dir>_<YYYYMMDD_HHMMSS>`，下述路径均落在该带后缀目录内。`main` 启动时：先 `MkdirAll(output_dir)`，再 append 打开 `<output_dir>/run.log`，构造 `mwOut=MultiWriter(os.Stdout, runLog)` 与 `mwErr=MultiWriter(os.Stderr, runLog)`，`log.SetOutput(mwErr)`（nodepool 告警 / `log.Fatalf` 进 run.log），进度行与 summary 走 `mwOut`。随后打印完整配置快照（ak/sk 屏蔽为 `***`，含 `output_dir_timestamp`）到 `mwOut`。run.log 全程 append，断点续跑不覆盖。

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

### 7.1 端到端冒烟（本地 minio + 真实多段对象）

用于改动后回归：验证 list/check、多段对象的逐 part 检查、owner 分桶、per-owner 行格式、run.log 全链路。整个流程在 `/tmp/chunked-e2e/` 下，**非仓库内容**，可随改随丢。2026-11-27 按本节步骤实跑通过（脚本执行，期望值取自那次运行）。

本地 minio 不识别 `internal-list-mp-offset`，主扫描在 minio 上拿不到 part 边界：多段对象要么进 `mp.txt`（`multipart_check_mode=0`），要么进 `list_parse_failed.txt`（`multipart_check_mode=1`）。逐 part 检查用 `-check-file` 喂入边界来验证（步骤 7）。

**前置**：`/opt/homebrew/bin/{minio,mc}` 已装。minio data dir 与 seed 脚本都放 `/tmp/chunked-e2e/`。

```bash
# 1. 启动 minio（后台，127.0.0.1:9100）
mkdir -p /tmp/chunked-e2e/data
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin123 \
  /opt/homebrew/bin/minio server /tmp/chunked-e2e/data \
  --address 127.0.0.1:9100 > /tmp/chunked-e2e/minio.log 2>&1 &

# 2. 配 mc alias + 建 bucket
/opt/homebrew/bin/mc alias set local http://127.0.0.1:9100 minioadmin minioadmin123
/opt/homebrew/bin/mc mb local/testbucket

# 3. seed 5 个正常普通对象 + 1 个损坏普通对象（body 开头是 chunk-sig 头）
mkdir -p /tmp/chunked-e2e/seed
for i in 01 02 03 04 05; do
  head -c 65536 /dev/urandom > /tmp/chunked-e2e/seed/file_${i}.bin
done
/opt/homebrew/bin/mc cp /tmp/chunked-e2e/seed/file_*.bin local/testbucket/data/2026/01/
printf '1000;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n' > /tmp/chunked-e2e/seed/corrupted.bin
head -c 65536 /dev/urandom >> /tmp/chunked-e2e/seed/corrupted.bin
/opt/homebrew/bin/mc cp /tmp/chunked-e2e/seed/corrupted.bin local/testbucket/corrupted/corrupted.bin

# 4. seed 2 个多段对象，各 2 个 part（5MiB + 1MiB，共 6MB）
#    mp/clean.bin   — 正常 body（ETag <hex>-2）
#    mp/corrupt.bin — 第 1 个 part 的开头是 chunk-sig 头（ETag <hex>-2）
#    helper 代码见本节末尾，存成 /tmp/chunked-e2e/mp-seed/main.go；
#    从仓库目录运行，借用仓库的 SDK 依赖
cd /Users/gengyuanzhe/code/S3/golang/chunked_check_tool/chunked_check_tool
go run /tmp/chunked-e2e/mp-seed/main.go 127.0.0.1:9100 testbucket

# 5. 写 cfg（is_check=true；多段检查关闭，主扫描把多段对象记进 mp.txt）
cat > /tmp/chunked-e2e/cfg.yaml <<'EOF'
endpoints:
  - 127.0.0.1:9100
scheme: http
ak: minioadmin
sk: minioadmin123
list_type: 2
list_concurrency: 2
check_concurrency: 4
output_dir: /tmp/chunked-e2e/out
output_dir_timestamp: false
is_check: true
is_success_log: true
multipart_check_mode: 0
max_keys: 1000
progress_interval: 2
EOF

# 6. 编译并跑主扫描
go build -o /tmp/chunked_check_tool .
/tmp/chunked_check_tool -c /tmp/chunked-e2e/cfg.yaml -bkt testbucket
```

**期望结果**（testbucket 8 对象）。`<ownerID>` 是 LIST 响应里的 Owner.ID，随 minio 版本和登录用户而不同（2026-11-27 本机是一串 64 位十六进制）；为空时落 `_unknown/`。

| 输出 | 内容 |
|---|---|
| `out/run.log`（=== summary === 段） | `list_all: 8 (list_obj: 6 list_mp: 2)` `ok_obj: 5 corrupt_obj: 1 ok_mp: 0 corrupt_mp: 0 list_failed: 0 list_parse_failed: 0 check_failed: 0 mp_check_failed: 0` |
| `out/<ownerID>/corrupted_objects.txt` | `testbucket\x1fcorrupted%2Fcorrupted.bin` |
| `out/<ownerID>/ok_objects.txt` | 5 行 `testbucket\x1fdata%2F2026%2F01%2Ffile_0N.bin` |
| `out/<ownerID>/mp.txt` | 2 行：`testbucket\x1fmp%2Fclean.bin`、`testbucket\x1fmp%2Fcorrupt.bin` |
| `out/{list_failed,check_failed,mp_check_failed}.txt` | 空 |
| `out/run.log` | 含配置快照（ak/sk `***`）+ 进度行 + summary |

```bash
# 7. 多段对象的逐 part 检查：-check-file 喂入两个多段对象的 part 边界（\x1f 分隔）
printf 'testbucket\x1fmp/clean.bin\x1f2\x1f0\x1f5242880\ntestbucket\x1fmp/corrupt.bin\x1f2\x1f0\x1f5242880\n' \
  > /tmp/chunked-e2e/mp-check.txt
/tmp/chunked_check_tool -c /tmp/chunked-e2e/cfg.yaml -bkt testbucket -check-file /tmp/chunked-e2e/mp-check.txt
```

期望 summary：`read: 2` + `ok_mp: 1 corrupt_mp: 1 check_failed: 0 mp_check_failed: 0`。结果落在 `out/_unknown/`（输入行不带 owner）：

| 文件 | 内容 |
|---|---|
| `out/_unknown/corrupted_mp.txt` | `testbucket\x1fmp%2Fcorrupt.bin\x1f2\x1f0\x1f5242880` |
| `out/_unknown/ok_mp.txt` | `testbucket\x1fmp%2Fclean.bin\x1f2\x1f0\x1f5242880` |

**multipart_check_mode=1 负向验证**（本地 minio 不识别 `internal-list-mp-offset`，正好验证回落路径）：cfg 改 `multipart_check_mode: 1` 重跑主扫描——minio 返回普通 `<md5>-<N>` ETag → 解析失败 → 两个多段对象都落 `out/list_parse_failed.txt`（根目录），summary `list_parse_failed: 2`；不创建 `mp.txt`、`corrupted_mp.txt`、`ok_mp.txt`。header 实际已发出（`TestMpOffsetHeaderListOnly` 单测覆盖）。**正向路径**（offset 检查全链路）需服务端实现该 header 后在真实集群验证：corrupted_mp.txt 行 `bkt\x1fkey\x1fpartcnt\x1foff0\x1f...` 可直喂 `-backup-file`。

**断点续跑**：`output_dir` 是 append 模式，重跑会累加。想干净跑就换 `output_dir`（`sed 's#out#out2#'`）。

**清场**（minio 后台进程 + /tmp 数据）：
```bash
pkill -f 'minio server.*127.0.0.1:9100'
# /tmp/chunked-e2e 视情况删；权限系统可能拒绝 rm -rf，必要时用 rm 逐文件
```

**mp-seed helper**（`/tmp/chunked-e2e/mp-seed/main.go`，非仓库代码）：用仓库的 aws-sdk-go-v2 依赖直接调 CreateMultipartUpload / UploadPart / CompleteMultipartUpload。minio 要求非末段 ≥5MiB，所以两个 part 取 5MiB + 1MiB。`mp/corrupt.bin` 的第 1 个 part 以 `1000;chunk-signature=...` 头开头，其余是 filler——逐 part 检查在 part 1 的 offset 0 命中。helper 没有独立 go.mod，从仓库目录 `go run`。

```go
// Seeds the two multipart objects of the AGENTS §7.1 smoke test on a local
// minio:
//
//	mp/clean.bin   — 6MB of plain filler, in two parts (5MiB + 1MiB)
//	mp/corrupt.bin — the same, but the first part opens with a chunk-signature
//	                 header, as a corrupted upload leaves it on the private
//	                 storage
//
// Run it from the repo directory, which supplies the SDK:
//
//	go run /tmp/chunked-e2e/mp-seed/main.go 127.0.0.1:9100 testbucket
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	endpoint, bucket := os.Args[1], os.Args[2]
	c := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://" + endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin123", ""),
	})
	// minio rejects a non-last part smaller than 5MiB.
	partSizes := []int{5 * 1024 * 1024, 1024 * 1024}
	sig := "1000;chunk-signature=" + strings.Repeat("0", 64) + "\r\n"
	for _, o := range []struct{ key, head string }{
		{"mp/clean.bin", ""},
		{"mp/corrupt.bin", sig},
	} {
		ctx := context.Background()
		created, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
		if err != nil {
			fmt.Fprintln(os.Stderr, o.key, "CreateMultipartUpload:", err)
			os.Exit(1)
		}
		var parts []types.CompletedPart
		for i, n := range partSizes {
			head := ""
			if i == 0 {
				head = o.head
			}
			part, err := c.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(o.key), UploadId: created.UploadId,
				PartNumber: aws.Int32(int32(i + 1)),
				Body:       bytes.NewReader([]byte(head + strings.Repeat("x", n-len(head)))),
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, o.key, "UploadPart:", err)
				os.Exit(1)
			}
			parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: part.ETag})
		}
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(o.key), UploadId: created.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}); err != nil {
			fmt.Fprintln(os.Stderr, o.key, "CompleteMultipartUpload:", err)
			os.Exit(1)
		}
		fmt.Println(o.key, "ok")
	}
}
```

### 7.2 -check-file / -backup-file 冒烟（复用 7.1 环境）

复用 7.1 的 8 对象数据（5 干净普通 + 1 损坏普通 + 2 个 6MB 两段对象，多段 offsets = `[0, 5242880]`）。要确定性对象数就用全新 data 目录重跑 7.1 步骤 1-4。

**-check-file**（HEAD 权威判型 + 失败分流 + 漂移回落）：

```bash
# 字段分隔符是 \x1f（ASCII Unit Separator）；下面用 <US> 标注。实际文件由工具产出
# 或用 printf '\x1f' 手写。
cat > /tmp/chunked-e2e/check-input.txt <<'EOF'
testbucket<US>corrupted/corrupted.bin
testbucket<US>data/2026/01/file_01.bin
testbucket<US>mp/corrupt.bin<US>2<US>0<US>5242880
testbucket<US>mp/clean.bin<US>2<US>0<US>5242880
testbucket<US>mp/clean.bin
testbucket<US>no-such-key
testbucket<US>bad<US>1<US>100
EOF
# cfg 关键项：is_check，multipart_check_mode: 0（-check-file 恒写 ok_objects/ok_mp，无需开两个 success_log）
# （0 验证文件模式无视该配置——mp 输出照常开启，offsets 来自输入行）
/tmp/chunked_check_tool -c cfg.yaml -bkt testbucket -check-file /tmp/chunked-e2e/check-input.txt
```

期望 summary：`read: 7` + `ok_obj: 1 corrupt_obj: 1 ok_mp: 1 corrupt_mp: 1 check_failed: 1 mp_check_failed: 0 parse_failed: 1`。文件（全落 `_unknown/`）：

| 文件 | 内容 |
|---|---|
| `corrupted_objects.txt` | `testbucket\x1fcorrupted/corrupted.bin` |
| `corrupted_mp.txt` | `testbucket\x1fmp/corrupt.bin\x1f2\x1f0\x1f5242880`（带 offsets，可直喂 -backup-file） |
| `ok_mp.txt` / `ok_objects.txt` | `testbucket\x1fmp/clean.bin\x1f2\x1f0\x1f5242880`（带 offsets）/ `file_01.bin` |
| `mp.txt` | `testbucket\x1fmp/clean.bin`（漂移行：普通行 + HEAD 多段 → 无 offsets 不认干净） |
| `check_failed.txt` | `testbucket\x1fno-such-key`（普通行 HEAD 404 按行型分流） |
| `parse_failed.txt` | `testbucket\x1fbad\x1f1\x1f100` |

**-backup-file**（不探测直接中转）：

```bash
# 字段分隔符是 \x1f（ASCII Unit Separator）；下面用 <US> 标注。
cat > /tmp/chunked-e2e/backup-input.txt <<'EOF'
testbucket<US>corrupted/corrupted.bin
testbucket<US>mp/corrupt.bin<US>2<US>0<US>5242880
testbucket<US>mp/clean.bin<US>2<US>0<US>5242880
testbucket<US>mp/clean.bin
EOF
# cfg 关键项：backup_bucket: backupbucket + backup_output_dir（先 mc mb local/backupbucket）
/tmp/chunked_check_tool -c cfg.yaml -bkt testbucket -backup-file /tmp/chunked-e2e/backup-input.txt
```

期望 summary：`read: 4` + `backup_ok: 3 backup_failed: 0 backup_mismatch: 1`，**`get_calls: 0`**（零探测的证据——HEAD/下载不经过 RangeGetAt 计数路径）。文件与远端：

- `backup_ok.txt` 3 行——**含干净多段**（旧行为是 backup_skipped_clean 跳过）；`backup_skipped_clean.txt` 不存在
- `mismatch.txt` = `testbucket\x1fmp/clean.bin`（第 4 行普通行 vs HEAD 多段），可喂回 -check-file 重查
- 目标桶三对象 `mc stat` ETag 与源一致（多段 `-2` 形式 = 分段边界保真）；`mc cat local/testbucket/<k> | cmp -s - <(mc cat local/backupbucket/<k>)` 字节级相同
- `.backup_lists/backup-input_<时间戳>.txt` 归档存在

### 7.3 LIST 失败后的 -resume-list 闭环

工具不拦截信号（不变量 19），所以没有“中断轮”：`-resume-list` 的输入只来自运行中真正失败的页（节点故障、5xx、超时）。复用 7.1 的环境，让一部分 LIST 失败就能验证闭环。下面的步骤由早先的信号中断版本改写而来，**改写后没有重新实跑**；闭环断言本身曾在信号中断场景实跑通过。

```bash
# seed 2000 前缀 × 3 对象（拉长运行，留出停 minio 的时间）
mkdir -p /tmp/chunked-e2e/bulk2 && cd /tmp/chunked-e2e/bulk2
for d in $(seq -w 0 1999); do mkdir -p $d; for f in 1 2 3; do head -c 4096 /dev/urandom > $d/obj_$f.bin; done; done
mc cp --recursive . local/testbucket/bulk2/

# 后台全量扫描；1 秒后停掉 minio，3 秒后重启（check_concurrency 调低可拉长运行便于掐点）
/tmp/chunked_check_tool -c cfg.yaml -bkt testbucket > /tmp/chunked-e2e/listfail.log 2>&1 &
PID=$!; sleep 1; pkill -f 'minio server.*127.0.0.1:9100'; sleep 3
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin123 \
  /opt/homebrew/bin/minio server /tmp/chunked-e2e/data \
  --address 127.0.0.1:9100 > /tmp/chunked-e2e/minio.log 2>&1 &
wait $PID; echo "exit=$?"
```

期望：

- summary 里 `list_failed` 非 0；`list_failed.txt` 每行 `prefix\x1ftoken`，token 是失败页的 continuationToken（第一页失败的 prefix 为单字段）
- **闭环验证**：`/tmp/chunked_check_tool -c cfg -bkt testbucket -resume-list <该轮目录>/list_failed.txt` 跑完 `list_failed: 0`，且两轮 `list_all` 之和 == `mc ls --recursive local/testbucket | wc -l`（无丢失无重复的完备性证明）

### 7.4 kill -9 后 `-check-file` / `-backup-file` 的续跑：按 key 求差集

场景：这两个文件输入模式被 kill -9 / OOM 打断。它们没有快照，也不记录输入读到哪一行，没处理的行不在任何文件里。恢复入口是**原输入重跑**；本 runbook 用差集剔掉已经有定论的对象，只重跑剩下的。

**规则（两个模式统一）**

- **按 key 过滤**：只比每行的第 2 个字段。同一轮只有一个桶（桶名不符的行工具判为坏行），所以 key 就能唯一确定对象。
- **只有有定论的结果算完成**，其余的行留在重跑输入里。

| 模式 | 算完成 | 不算完成（留在重跑输入里） |
|---|---|---|
| `-backup-file` | `backup_ok.txt` | `backup_failed.txt`、`mismatch.txt`、`parse_failed.txt` |
| `-check-file` | `_unknown/` 下的 `ok_objects.txt`、`ok_mp.txt`、`corrupted_objects.txt`、`corrupted_mp.txt` | `mp.txt`、`check_failed.txt`、`mp_check_failed.txt`、`parse_failed.txt` |

**为什么按 key，不按整行**

- `-check-file` 的结果行是按字段重新拼的，不是抄输入行。对象在两轮之间被改写成普通对象时，结果行没有分段边界，按整行比永远对不上，这个对象会被反复重查。
- `-backup-file` 的结果行是原始输入行，按整行比本来可行。统一成按 key 后，两个模式一套做法。

**为什么 `mismatch.txt` 和 `mp.txt` 不算完成**

它们表示"这一行处理不了"，不是对对象的定论。同一个 key 可能在输入里有两行（来自不同轮次，一行写普通对象，一行写多段对象）。以备份为例：对象现在是多段，普通那行得到 mismatch，多段那行能正常备份。如果中断发生在只处理完普通那行之后，mismatch 算完成就会跳过多段那行，**对象没有备份**。不算完成的代价很小：那一行每次重跑只花一次 HEAD，结果不变。

**命令：`-backup-file`**

```bash
# <kill 轮目录> = backup_output_dir（output_dir_timestamp=true 时为 backup_output_dir_<时间戳>）
D=<kill 轮目录>
IN=<那一轮的原始输入文件>
OUT=retry.txt                         # 生成的重跑输入
W=$(mktemp -d)                        # 中间文件目录
export LC_ALL=C                      # sort 和 join 必须用同一套字典序
US=$(printf '\037')                  # 字段分隔符 \x1f

# 结果文件末行没有换行符 = 被 kill 时写了一半，去掉这一行；文件不存在则不输出
drop_torn() { { if [ -s "$1" ] && [ -n "$(tail -c 1 "$1")" ]; then sed '$d' "$1"; else cat "$1"; fi; } 2>/dev/null; }

# 1) 有定论的对象（只有 backup_ok.txt），按 key 排序
drop_torn "$D/backup_ok.txt" | sort -t "$US" -k2,2 > "$W/done.bykey"
# 2) 原输入按 key 排序
sort -t "$US" -k2,2 "$IN" > "$W/input.bykey"
# 3) 关联，只留没有定论的行。关联字段 key 会被放到行首，所以把前两个字段换回来
join -t "$US" -1 2 -2 2 -v 1 "$W/input.bykey" "$W/done.bykey" \
  | awk -F "$US" -v OFS="$US" '{ k = $1; $1 = $2; $2 = k; print }' > "$OUT"

# 多轮的结果都算数时，把各轮的 backup_ok.txt 都接到第 1 步：
# { drop_torn <轮1>/backup_ok.txt; drop_torn <轮2>/backup_ok.txt; } | sort -t "$US" -k2,2 > "$W/done.bykey"
# 续跑（落新的时间戳目录）：
# /tmp/chunked_check_tool -c cfg.yaml -bkt <bucket> -backup-file retry.txt
```

**命令：`-check-file`**

```bash
# <kill 轮目录> = output_dir（output_dir_timestamp=true 时为 output_dir_<时间戳>）
D=<kill 轮目录>
IN=<那一轮的原始输入文件>
OUT=retry.txt                         # 生成的重跑输入
W=$(mktemp -d)                        # 中间文件目录
export LC_ALL=C                      # sort 和 join 必须用同一套字典序
US=$(printf '\037')                  # 字段分隔符 \x1f

# 结果文件末行没有换行符 = 被 kill 时写了一半，去掉这一行；文件不存在则不输出
drop_torn() { { if [ -s "$1" ] && [ -n "$(tail -c 1 "$1")" ]; then sed '$d' "$1"; else cat "$1"; fi; } 2>/dev/null; }

# 1) 有定论的对象，按 key 排序（mp.txt 不算：它表示"没法验证"，不是定论）
for f in ok_objects ok_mp corrupted_objects corrupted_mp; do
  drop_torn "$D/_unknown/$f.txt"
done | sort -t "$US" -k2,2 > "$W/done.bykey"
# 2) 原输入按 key 排序
sort -t "$US" -k2,2 "$IN" > "$W/input.bykey"
# 3) 关联，只留没有定论的行。关联字段 key 会被放到行首，所以把前两个字段换回来
join -t "$US" -1 2 -2 2 -v 1 "$W/input.bykey" "$W/done.bykey" \
  | awk -F "$US" -v OFS="$US" '{ k = $1; $1 = $2; $2 = k; print }' > "$OUT"

# 续跑（落新的时间戳目录）：
# /tmp/chunked_check_tool -c cfg.yaml -bkt <bucket> -check-file retry.txt
```

**依据的代码事实**

- `backup_ok.txt` 的行在 ETag 终验通过后才写，是"真完成集合"的子集——周期 Flush（≤3s）内丢的行只会让对象多备一次（幂等覆盖），不会漏备。`ok_*` / `corrupted_*` 的行同理，探测出结果后才写。
- 结果行的 key 和输入行的 key 都是 `url.QueryEscape` 后的形态，逐字节相同。工具生成的文件天然满足；手写的行要保证转义形式一致。
- 关联（join）输出的行总是把关联字段放在最前，所以第 3 步要把前两个字段换回来。这一步只处理留在重跑输入里的行。

**注意事项**

- `LC_ALL=C` 必须设：sort 和 join 要用同一套字典序，macOS/Linux 默认 locale 不同，不设会静默出错。
- 结果文件末行可能只写了一半（被 kill 时），`drop_torn` 把它去掉。这个对象因此留在重跑输入里，方向安全。
- 输入里的重复行原样保留（同一个 key 的多行都会留下）。要先合并就对原输入先 `sort -u`。
- 性能（2026-11-27 在 macOS 上实测，BSD 工具）：全程只用外部排序和顺序关联，内存不随输入增长。500 万行（180 MB）、90% 已完成时，整行比 2.3 秒，按 key 比 3.9 秒；1500 万行（545 MB）时 7.1 秒对 11.9 秒。
- 收敛判据 = 最新一轮失败文件为空（§4.20 闭环）。
- kill -9 还会在备份桶遗留**未完成的 multipart upload**（kill 时 `AbortMultipart` 不会执行）——普通 LIST 不可见，需 ListMultipartUploads 排查清理，当前无工具入口。
- 命令经构造数据验证（2026-11-27）：覆盖有定论、无定论、未处理、坏行、末行残行、同一个 key 多行、前缀相近的 key、转义后含 `%` 的 key、多轮结果合并；在 500 万 / 1500 万行的数据集上与整行比结果一致。kill 场景的端到端复验待 §7.1 环境。

### 7.5 kill -9 + -resume-journal 冒烟（复用 7.1/7.3 环境）

验证 Mode 2 扫描被 SIGKILL 后从 journal 断点续跑的闭环（进程不拦截信号，Ctrl-C / kill / kill -9 效果相同：都不执行收尾代码；journal 是唯一的续跑依据）。**实跑记录 2026-11-05（17,006 对象，17k 对象本地 minio 全程 ~2.4s）**，步骤与断言均为实跑验证过的版本：

```bash
# 数据准备（在 7.1 基础 6 对象 + 7.3 bulk2 6000 之上）：
#   data/2026/02/ 单前缀 3000 对象（3 页，练分页）+ 000multi/ 单前缀 8000 对象（8 页，练中途 cursor）
#   —— mc cp --recursive . 会把 "." 目录本身嵌一层，实际键为 000multi/000multi/obj_*，无碍
# cfg 需 is_check: true + is_success_log: true（完备性并集要 ok_objects.txt 的逐对象行）

# 本地 minio 太快，默认 600 秒的首个 tick 永远等不到：cfg-kill.yaml 里把
# journal_checkpoint_interval_sec 设为 1（负值启动报错；不配 = 默认 600 秒）
/tmp/chunked_check_tool -c cfg-kill.yaml -bkt testbucket > /tmp/chunked-e2e/kill.log 2>&1 &
PID=$!; sleep 3; kill -9 $PID; wait $PID; echo "exit=$?"    # 期望 137

KILL_DIR=<被 kill 那轮的输出目录>       # 时间戳最新的 out_*
awk -F'\x1f' '{print $1}' $KILL_DIR/journal.txt | sort | uniq -c
#   期望三类行齐备：done（扫完前缀）+ cursor（分页中途）+ discovered（种子/未启动）
#   （journal.txt 字段分隔符是 \x1f，不是 |——见不变量 21/23）

# 从 journal 续跑（-prefix 必须为空，journal 即扫描范围）
/tmp/chunked_check_tool -c cfg-kill.yaml -bkt testbucket -resume-journal $KILL_DIR/journal.txt

# 完备性并集（按文件处理撕裂尾行：被 kill 轮输出文件最后一行可能没有换行——
# P0 周期 flush ≤3s 尾部损失的设计内行为；该对象由恢复轮重列补回）
for f in $KILL_DIR/minio/ok_objects.txt $KILL_DIR/minio/corrupted_objects.txt \
         <RESUME_DIR>/minio/ok_objects.txt <RESUME_DIR>/minio/corrupted_objects.txt; do
  if [ -n "$(tail -c 1 "$f")" ]; then sed '$d' "$f" > /tmp/f.tmp; else cp "$f" /tmp/f.tmp; fi
  cut -d'|' -f2 /tmp/f.tmp
done | sort -u > union.txt
mc ls --recursive local/testbucket | awk '{print $NF}' | sort > mckeys.txt
diff union.txt mckeys.txt      # 期望空（有 mp.txt 的配置把它也并进 union）
```

期望（实跑值）：

- kill -9 退出码 137；journal 非空三态混合（实测 5 done + 8 discovered + 2 cursor；cursor 同前缀每 tick 重发一行，重放取最后一条）；间隔保持默认 600 秒时 kill 落在首 tick 前则 journal 只有种子行（`CheckpointNow` 保证非空）——恢复退化为整轮重扫，损失 ≤ 一个间隔，设计内
- 续跑轮 summary `list_failed: 0`；**完备性判据 = 两轮对象结果行（撕裂尾行丢弃后）去重并集 == `mc ls` 键集**（实测 17,006 == 17,006，diff 空）。**不要用"两轮 `list_all` 求和 == mc 数"**：恢复轮重列在途页会双计（实测两轮结果行 19,396 行，重叠 2,390）——这是 journal"冻结在半页前、恢复重列整页"设计的必然副作用
- 数学自洽断言：续跑 `list_all` = 总对象数 −（cursor 前已终态页的对象数 + done 前缀的对象数）（实测 13,005 = 17,006 − 4,000 − 1）
- done 跳过抽验：done 前缀的对象不出现在续跑轮任何输出文件（实测 corrupted/corrupted.bin）；cursor 续列抽验：撕裂行所属对象（如 obj_6391）出现在续跑轮 ok_objects.txt
- 续跑轮 run.log 的 config 快照含 `resume_journal:` 行；续跑轮自己写新 journal（终态全 done；中途 tick 的 cursor 行被更晚的 done 行覆盖，行序保证重放 last-wins 正确）
- 恢复轮可再次被 kill / 再次恢复（用同样的命令指向新目录的 journal.txt）

## 8. 已知遗留项（改动时留意，非阻塞）

来源：SDD ledger 的 deferred minors（见 `.superpowers/sdd/2026-09-03-chunked-check-tool/progress.md` 与 `.superpowers/sdd/2026-11-05-resume-journal/progress.md`）。

- **NodePool.Assign 空端点 panic**：`n=0` 时除零。`Config` 上游校验 `endpoints` 非空，故不会触发。无 `idx` 越界保护（调用方只用 `Assign` 返回的索引）。
- **NewOutput 部分初始化失败的 goroutine/file 泄漏**：brief 继承的设计，仅 `os.OpenFile` 失败时触发（罕见启动期磁盘错误）。修复需偏离 brief。
- **bufio flush 错误静默丢弃**：writer goroutine 不检查 `Flush` 错误，无 Write/Close race guard（标准使用契约）。
- **MaybePrint 在 stats.Snapshot() 之后再取写锁**：快照原子安全，仅进度行时间戳可能略偏。
- **Mode 1 根直接对象不调 `onObject`**：进度计数偏少（仅外观，stats 正确）。
- **`workerIdx` 参数在 `Lister.Run` 未用**：保留给未来 NodePool 分配，当前是死重量。
- **`list_type` 非法值（非 1/2）静默落到 Mode 2 分支**：可加校验。
- **per-owner writer goroutine 的 MkdirAll/OpenFile 失败静默丢弃该行**：罕见启动期磁盘错误，第 N 个 owner 目录建不出来时该 owner 的结果行会丢，但其他 owner 不受影响。

### 8.1 journal 遗留项（2026-11-06 review 轮 deferred，M=Minor / N=Nit）

- **M1（已解决 2026-11-24）：key 含 `\n` 或 `|` 破坏 journal 行格式**：已用 `\x1f` 分隔 + `QueryEscape`/`QueryUnescape` 转义统一解决（全工具规则，见不变量 23），`TestSnapshotEmitsCursorWithPipeOrNewlineToken` 是回归钉。
- **N1：`FlushAndWait` 在 `out.Close()` 之后调用会永久阻塞**：写手 goroutine 已退出、flush 请求无人应答。现有接线不可达：不变量 ③（`journalRun.Stop()` 先于 `out.Close()`）锁死时序。**修复触发条件**：未来有人改动关停顺序或新增 FlushAndWait 调用点时，顺手加"Close 后返回而非阻塞"守卫。
- **N2：journal.go 内存注释措辞过头**："不随桶规模增长"的精确说法是上限 = 一个 checkpoint 间隔内新发现的前缀量（发现速率 × 间隔），极端发现速率下也会不小。**修复触发条件**：下次改 journal.go 时顺带改一行注释，无需单独动。
- **N3（已解决 2026-11-27）：测试直接改写包级 `journalCheckpointInterval` 的恢复纪律**：快照间隔改为配置项后，作为参数传给 checkpoint 循环，测试直接传入自己要的间隔。包级变量已删除，不再妨碍 `t.Parallel()`。

## 9. 工作流约定

- 实现性改动遵循 TDD：先写失败测试，再实现，再跑测试，再 commit。
- 每个 commit 聚焦一个职责（feat/fix/refactor 前缀）。
- `chunked_check_tool` 二进制已 `.gitignore`，不要提交。
- `.superpowers/` 目录是 SDD 工作区（ledger、brief、review package），已 gitignore，不要提交。
- 改动涉及多文件接口（`S3API`、`Lister.Run` 签名、`Output` 方法、`Stats` 字段）时，先在 ledger 或 PR 描述记录决策，再改。
- **工具自身不做历史版本兼容**：本工具是单机运维工具，不是给外部长期消费的库。改配置字段/文件格式/接口时，**不要**加 retired-field guard、旧格式兼容、迁移提示等回溯代码——旧配置/旧文件直接报错或失效即可，由使用者重新生成。代码里不保留对已删配置项/已删代码路径的检测守卫。
