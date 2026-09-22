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
5. **三类损坏特征分布在 part 的不同位置**：
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 和 `-TRAILER`：段首是 `<hexlen>;chunk-signature=<64hex>\r\n`（当前 `chunkSigRe` 抓这个，强特征）。
   - `STREAMING-UNSIGNED-PAYLOAD-TRAILER`：段首是 `<hexlen>\r\n`（弱特征，误报风险高，**不能**单独作判据）；段尾/对象尾是 `0\r\nx-amz-checksum-(sha256|crc32|crc32c|sha1):...\r\n\r\n`（强特征）。
   - trailer 类的段首特征太弱，必须靠段尾 trailer marker 抓。

### 1.2 工具职责

本工具**并发**列举并校验对象：
- 普通对象（ETag 为 32 位小写 MD5 hex）→ 小对象（`Size <= whole_object_probe_threshold`）单次 Range GET 全读；大对象 head@0 + tail@Size-128 两次。body 同时匹配 `chunkSigRe`（段首 chunk-signature）OR `trailerRe`（段尾/对象尾 x-amz-checksum marker）→ 任一命中即视为损坏。
- 多段对象（任何非 `^[0-9a-f]{32}$` 的 ETag）→ 按 `multipart_check_mode` 处理：
  - `0`（关闭）→ 直接入 `mp.txt`，不做 Range GET。
  - `1`（offset 检查）→ LIST 带 `internal-list-mp-offset: true` header，服务端把多段 ETag 改写为 `<md5>-<partcnt>-<off0>|<off1>|...`（物理字节偏移）；按 head@0 + (N-1) 边界探测 + tail@Size-128 探测，边界 256B 窗口同时覆盖上一段尾 trailer 与下一段首 chunk-signature；ETag 解析不出 offsets（格式不符或 offset > Size）的对象回落 `list_parse_failed.txt`（根目录，行格式 `bucket|key`）。`parseMultipartOffsetETag` 始终校验每个 offset ≤ Size（Size=0 是合法空对象：唯一合法 ETag 是 `<md5>-1-0`）。
  - `2`（固定分段，旧模式待废弃）→ 按 `multipart_segment_size` 合成 `[0, seg, 2*seg, ...]` 边界，探测矩阵同 mode=1。
- 列举失败、校验失败分别落不同文件。
- 支持几十亿对象规模，内存有界，背压自调节。探测矩阵与双 regex 细则见 §4.17。

除 list+check 主模式外还有三个文件输入模式（互斥，组成补跑流水线，见 §4.20）：`-check-file`（mixed 格式重查 check_failed/mp_check_failed/mismatch，HEAD 权威判型）、`-list-file`（旧多段-only 格式）、`-backup-file`（mixed 格式，HEAD 判型 + 中转 + ETag 终验，**不探测损坏**）。

**不在本工具范围**：修复损坏对象（另行处理）。

## 2. 技术栈

- Go 1.27（`go.mod` module `chunked_check_tool`）
- `github.com/minio/minio-go/v7`（MinIO SDK，非 AWS SDK；用 `Core.ListObjectsV2` 拿 Contents + CommonPrefixes，用 `Client.GetObject` + `GetObjectOptions.SetRange(0, 127)` 做 Range GET）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（用户明确要求忽略证书）

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析、`installSignalHandler`（两段式 SIGINT/SIGTERM，见不变量 20）、`run`/`runBackup` 编排（双 ctx：listCtx/hardCtx）、Mode 1 根列举分页、`drainQueuedPrefixes`（中断时把未启动 prefix 写 list_failed）、worker 启停、stats 写盘 |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，带默认值） |
| `nodepool.go` | `NodePool`：轮询 `Assign`/`AssignOther`、`RecordFault`（累积故障计数，达 `node_isolate_threshold` 才 `MarkFailed` 隔离）、`Unmark`/`FailedNodes`（恢复探测用）、`URL`/`Endpoint`（隔离全局共享、进程内单向——除非开了恢复探测） |
| `noderecovery.go` | 后台节点恢复：`startNodeRecovery`（`node_recover_probe_interval`>0 时每轮对隔离节点做 HEAD bucket 探测，连续 2 次健康 → `Unmark` 重返轮询池并清零故障计数；探测健康标准 = `!isNodeFaultErr`，即 2xx/404/403 都算活、5xx/传输错误不算）、`recoveryRound`/`probeNode`（可单测的轮次逻辑） |
| `s3client.go` | `S3API` 接口、`S3Client`（`minioCoreAPI` 接口包装 minio.Core + minio.Client，按 `cfg.ListAPIVersion` 分派 V1/V2）、`PutObjectLocal`（归档流式上传）/`PutObjectStream`（中转流式上传）的双流式设计（**任何路径不整文件缓冲**）、节点故障 failover。测试替身在各自 _test.go：`FakeS3`（fakes3_test.go）、`scriptedS3`（lister_test.go）、`countingS3`（walker_test.go）、`drainFake`（drain_test.go，ctx 感知） |
| `lister.go` | `Lister`（无 `s3` 字段；`Run`/`processPrefix` 接 `s3` 与双 ctx 参数）、无界队列 BFS、`inflight` atomic 计数、`recordListFailure`（graceful=下一页游标 / hard=当前页游标，见不变量 20） |
| `walker.go` | `runRecursiveWalk`：Mode 3 信号量递归列举，`sync.WaitGroup` 终止，不用 queue/inflight；中断时未启动 walk 记录裸 prefix |
| `resume_list.go` | `-resume-list` 模式：`parseResumeListLine` 解析 `prefix\|token` 行（SplitN，契约是 prefix 不含 `\|`）、`readResumeList` 逐行解析+违约分流（空行→parse_failed，prefix 含 `\|`→invalid_keys，正常→entries） |
| `checker.go` | `Checker`（ctx=hardCtx）、`isNormalETag`（严格 32 位小写 hex）、`chunkSigRe`/`trailerRe`、`runProbes`/`buildProbesStatic`（探测矩阵） |
| `backup.go` | `BackupChecker`：HEAD → 判型门 → 中转 → ETag 终验（**不探测**，见不变量 19）；relayRegular/relayMultipart 流式中转 |
| `file_source.go` | 泛型 `fileSource[T]`：三个文件输入模式（-check-file/-list-file/-backup-file）共用的逐行读取骨架（读行→parse→parse_failed 记录→push），`MalformedLineError` |
| `check_file_source.go` | `parseCheckFileLine`：mixed 格式 → VerifyTask（委托 `parseMixedLine`，HeadFirst=true） |
| `list_file_source.go` | `parseListFileLine`：旧 -list-file 多段-only 格式 → VerifyTask |
| `backup_source.go` | `parseMixedLine`：mixed 格式（`bkt\|key` / `bkt\|key\|partcnt\|off...`）→ BackupTask（-check-file 与 -backup-file 共用的解析器） |
| `mpoffset.go` | offset-etag 检查的三个原语：`mpOffsetTransport`（仅对 LIST 请求注入 `internal-list-mp-offset: true`，签名后 transport 层注入，非 x-amz 名不参与 SigV2/V4 签名计算）、`isListRequest`（钉死 minio-go v7.3.0 的 V1/V2 LIST 请求形态）、`parseMultipartOffsetETag(etag, size)`（`<md5>-<partcnt>-<off0>\|...` 解析，规则对齐 parseListFileLine；始终校验每个 offset ≤ Size，Size=0 是合法空对象） |
| `output.go` | channel + writer goroutine（per-owner 分目录 fan-out + 根目录全局），`FileInputMode` 门控（FileInputListFile/FileInputCheckFile 强制 mp 输出开启；FileInputCheckFile 开 mp.txt 漂移回落；mode=offset 开 list_parse_failed.txt/log），`bufio.Writer` 64KB，append 模式（OwnerID 为空 → `_unknown/`） |
| `queue.go` | 无界队列（slice + mutex + cond），ctx-aware 阻塞 Pop，`Drain`（中断排空：返回并清空残余项） |
| `stats.go` | atomic.Int64 计数器 + `StatsSnapshot` + `PrintSummary`（按 `RunMode` 分模式输出指标集；写入 stdout，被 `mwOut` tee 进 run.log） |
| `progress.go` | `ProgressPrinter`（按 `RunMode` 输出进度行字段集）+ `localCounter`（每 worker 本地 int，无 per-obj atomic） |

## 4. 关键不变量（改动前必须守住）

1. **多段判定严格**：仅 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象。大写、长度不对、`<hex>-N`、`<hex>-N-<offsets>`、空值一律按多段处理。**绝不把多段误判为普通对象**。`isNormalETag` 用逐字节循环实现（非正则），不要改成宽松匹配。offset-etag 的 md5 前缀校验复用同款严格性（大写 → 解析失败 → 回落 list_parse_failed.txt，不静默接受）。

1a. **offset 模式的 header 注入与回落**：`multipart_check_mode=1` 且 `is_check=true` 时，所有 LIST 请求（V1/V2、failover 重建的 client）经 `mpOffsetTransport` 注入 `internal-list-mp-offset: true`。该 header 名**不得**改成 x-amz-/x-obs- 开头（服务端签名计算覆盖这些前缀，签名后注入会被拒；非 x-amz 名在 SigV2/V4 下均可不签）。ETag 解析不出 offsets（服务端未实现/未升级、格式不符、或 offset > Size）→ Offsets=nil → 根目录 `list_parse_failed.txt` + `list_parse_failed.log` 回落（**不再**写 mp.txt）。`parseMultipartOffsetETag(etag, size)` 始终校验每个 offset ≤ Size；Size=0 是合法空对象，唯一合法 ETag 是 `<md5>-1-0`，多段 ETag 的非零 offset > 0 即非法。log 是根目录 slog，字段 `bucket/key/owner/size/etag_len/etag`（完整 ETag 无截断）。**假设服务端只改多段对象的 ETag**——普通对象若也被改写会被判为多段（不漏检损坏，但 list_obj/list_mp 计数失真）。`isListRequest` 钉死 minio-go v7.3.0 请求形态（`TestMpOffsetTransportMinioList` 是回归钉），升级 minio-go 必须重验。

2. **mode=0 时多段对象跳过 Range GET**：直接写 `<ownerID>/mp.txt`。每行按 `result_line_format` 渲染（默认 `<bucket>|<key>`），不带 ETag。mode=1/2 的多段对象按 offsets 逐段探测（见 14），**不是**无条件跳过 Range GET。

3. **ETag 来源**：list 响应（统一），**不从 Range GET response header 取**。OwnerID 同样来自 list 响应（minio-go v7.3.0 默认 `fetchOwner=true`，无额外请求开销）。

4. **结果文件按 OwnerID 分目录，处理文件全局**：`corrupted_objects`/`mp`/`corrupted_mp`/`ok_mp`/`ok_objects` 这五类结果文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`）；`list_failed`/`parse_failed`/`invalid_keys`/`check_failed`/`mp_check_failed`/`list_parse_failed`/`stats` 留根目录全局。理由：结果文件数量大且天然按 owner 分桶有意义；处理文件全局方便运维统一排查；stats 全局一份避免 owner 分桶后还要汇总。ownerDirName 折叠空/`.`/`..`/含路径分隔符的 OwnerID 到 `_unknown`，防止路径穿越。

4a. **失败分类四文件**（list_failed / parse_failed / invalid_keys / list_parse_failed 职责分离）：
  - `list_failed.txt` — S3 LIST 调用失败（节点故障/5xx/超时）。行格式 `prefix|token`：token 是失败页的 continuationToken（V1=上一页最后 key，V2=服务端不透明 token），第一页失败时为单字段 `prefix`。**可自动补跑**：`-resume-list <list_failed.txt>` 读这些行，把 `(prefix, token)` seed 进 BFS 队列，`processPrefix` 从 token 续页。
  - `parse_failed.txt` — `-list-file`/`-backup-file`/`-resume-list` 输入解析失败（坏行/bkt 不匹配/空行）。写入整行原始内容。**不可自动补跑**，需人工修输入文件。
  - `invalid_keys.txt` — S3 key/prefix 含 `|`（违反字段分隔契约）。lister 在 LIST 拿到对象后检查 key，违规则跳过该对象（不进 objCh）落此文件；WriteListFailed 检查 prefix，违规则不写 list_failed 改写此文件。**不可自动补跑**，需人工修数据。
  - `list_parse_failed.txt` — **仅 mode=offset + S3 LIST**：multipart ETag 解析失败（服务端未实现 header / ETag 格式不符 / offset > Size）。根目录全局，行格式 `bucket|key`（对齐 `check_failed.txt`，可直喂 `-check-file` 补跑：retry 会 HEAD 对象重新取 ETag/Size）。配套 `list_parse_failed.log`（根目录 slog，字段 `bucket/key/owner/size/etag_len/etag`，完整 ETag 无截断）。**与 mp.txt 的区别**：mp.txt 是 mode=0 主动关闭 / check-file 类型漂移 / mode=segment+Size==0 的未校验 multipart；list_parse_failed 是 mode=offset 下 ETag 本应解析但失败的对象。
  - **契约**：整个工具的行格式都基于 `|` 分隔（`bkt|key|partcnt|off...` / `prefix|token` / `bkt|key`），契约是 key/prefix **不含** `|`。违约的 key/prefix 在源头拒绝（落 invalid_keys.txt），不静默错切。`parse_failed.txt` 和 `invalid_keys.txt` 都写原始字节（不套任何格式），因为它们本身就是"无法被工具格式化的字节"。
  - **stats 新增**：`ParseFailed`（输入解析失败计数）、`InvalidKeys`（key 含 `|` 计数）、`ListParseFailed`（mode=offset ETag 解析失败计数）。summary 中 ModeListCheck 打 `invalid_keys` + `list_parse_failed`，ModeListFile/ModeCheckFile/ModeBackup 打 `parse_failed` + `invalid_keys`，ModeListOnly 打 `invalid_keys`。

5. **checker goroutine 绝不退出**：任何错误写 `check_failed`（普通对象）/ `mp_check_failed`（多段分段）后继续。若 checker 退出，`objCh` 无人消费，list worker 永久阻塞。

6. **`objCh` 永远会被关闭**：list 阶段保证（`listWg.Wait` → `close(objCh)`；或 Mode 1 无 seed 时显式 `q.Close()`）。

7. **Add-before-Push 顺序**（BFS 终止性）：Mode 2 在 push 新 subprefix 前 `inflight++`，否则 push 后 worker 消费完 inflight 已归零，新 subprefix 无人处理 → 永久挂起。改动 `lister.go` 的 `processPrefix` 时务必保留此顺序。

8. **Mode 1 不 seed 根 prefix**：根的直接对象由 `main` 用 delimiter 分页拿（Contents → objCh）；只把 CommonPrefixes（子目录）seed 进队列。若 seed 根，worker 会用 `delim=false` 平铺列出根，与 main 的 delimiter 分页重复，根下子目录对象被计两次。

9. **`-nextmarker` 仅 Mode 1 生效**：作为根列举的 start-after 参数。Mode 2/3 忽略（文档化限制）。

10. **节点故障分类与阈值隔离**：`isNodeFaultErr` 的判定顺序**必须**是 ① `errors.As(err, &minio.ErrorResponse)` 按状态码裁决（5xx=节点故障；4xx=业务错误；**501/505 除外**——能力/协议缺口换节点无意义）→ ② `errors.Is(context.DeadlineExceeded)` → ③ 字符串特征串（`nodeFaultSigs`，仅兜底非 S3 协议的传输层错误）。顺序不可颠倒：4xx 的 Message 是服务端文案，可能含 "timeout"/"EOF" 等传输层词汇，字符串匹配放在前面会把业务拒绝误判成节点故障、错误隔离健康节点。隔离是**阈值化**的：每次 `isNodeFaultErr` 命中调 `pool.RecordFault`（进程级 per-node 累积计数，无衰减），达到 `node_isolate_threshold`（默认 3，1=旧即时隔离行为）才 `MarkFailed`。重试恰好一次，且**重试一律换节点**：已隔离（本 worker 触达阈值或他人已隔离）→ `Assign` 轮询跳过隔离节点；未达阈值 → `AssignOther` 跳过当前节点（仅剩自己时退化为同节点重试，全隔离返回 -1 不重试）——**绝不**在刚故障的节点上同节点重试：节点真死时同节点重试必失败，会把本可在健康节点完成的工作项写进 `list_failed`/`check_failed` 丢覆盖。再失败按业务错误处理。注意 minio-go 对 408/429/499/500/502/503/504/520 有**内部重试**（最多 10 次带退避）——工具侧的一次计数在 minio-go 内部已是多轮失败。流式调用（`PutObjectStream`/`UploadPart`）不可重放、不参与 failover。**隔离默认可通过后台探测恢复**（`node_recover_probe_interval`，默认 60s，0=禁用恢复）：每轮对隔离节点 HEAD bucket，健康标准是 `!isNodeFaultErr`（与隔离标准互为镜像），**连续 2 次**健康才 `Unmark` 重返轮询池，同时**清零该节点故障计数**（否则恢复后再 1 次故障即再隔离，阈值名存实亡）；探测中失败即打断连续计数。探测走独立的一次性 client，不经过 `S3Client` 故障计数路径。

11. **性能优先但可读**：HTTP keep-alive（minio-go 自带连接池，不要自建）、`bufio.Writer` 64KB、合理 channel 容量、避免 per-obj 分配。**但**任何"复杂难读"的优化（手写内存池、unsafe、lock-free 结构）需先向用户请求确认，不要直接写。见 `memory/performance-vs-readability.md`。

12. **Mode 3 用 WaitGroup 终止**：`walker.go` 的 `runRecursiveWalk` 不用 queue、不用 inflight 计数；终止性靠 `sync.WaitGroup`。每个 `go walk(subprefix)` **前**必须 `wg.Add(1)`，否则 Wait 可能在 spawn 前归零、过早关闭 objCh。root 首个 `wg.Add(1)` 同理在 `go walk(prefix)` 前。objCh 由 main 中的 walk goroutine 在 `wg.Wait()` 返回后显式关闭。子树列举失败只写 `list_failed` 跳过该子树，不中止其他分支。

13. **V1/V2 分页协议对 caller 透明**：`S3Client.listPageOnce` 按 `cfg.ListAPIVersion` 分派 `Core.ListObjects`（V1，marker 游标）或 `Core.ListObjectsV2`（V2，continuation token）。两条路径都归一化进 `listResult{contents, commonPrefixes, next}`，`next` 作为下一次 `ListPage` 的 `continuationToken` 参数回传。V1 无 delimiter 且 `IsTruncated=true` 但 `NextMarker` 为空时，回退到最后一个 Contents key 作 marker；有 delimiter 时 S3 返回 `NextMarker`。caller（lister/walker/main 根分页）只需把 `next` 喂回 `continuationToken`，不感知 V1/V2 差异。`S3Client.core` 是 `minioListAPI` 接口（非 `*minio.Core`）以支持测试注入。

14. **多段分段检查的失败分流**（mode=1/2 与 -check-file/-list-file 共用路径）：分段 RangeGet 报错走 `mp_check_failed` 路径（`WriteMpCheckFailed` + `IncrMpCheckFailed`），**不走** `check_failed`。任一段命中 chunk-signature 即视为整段对象损坏，写 `<ownerID>/corrupted_mp.txt` 并 `IncrCorruptedMp`（同时**不** `IncrOkMp`）。干净的多段对象 `IncrOkMp`，仅 `is_multipart_success_log=true` 时写 `<ownerID>/ok_mp.txt`（与普通对象的 `is_success_log` 独立，互不影响）。**corrupted_mp.txt 行格式按模式**：mode=1 与文件输入模式写 `bkt|key|partcnt|off0|off1|...`（offsets 是真实 part 边界，文件可直喂 -backup-file/-check-file，**不套 result_line_format**）；mode=2 写 result_line_format 渲染行——固定分段的 [0,seg,2*seg,...] 是合成边界，**绝不能**当 part 边界写出（否则备份中转按错误边界分段，ETag 必 mismatch）。**mp_check_failed.txt 行格式对齐 corrupted_mp.txt**：mode=1 与文件输入模式写 `bkt|key|partcnt|off0|...`（`WriteMpCheckFailed(key, offsets)` 接收 task.Offsets），同样可直喂 -backup-file/-check-file 补跑。**check_failed.txt 行格式 `bkt|key`**（`WriteCheckFailed(key)` 内部拼 `o.bucket + "|" + key`），对齐 -check-file/-backup-file 普通对象 2 字段输入，可直喂补跑（不需要 ETag/Size：check-file 会 HEAD 重取，backup 中转也会 HEAD）。**owner 不写入** check_failed/mp_check_failed（文件输入模式不读 owner，输出走全局 root 不按 owner 分桶）。

15. **统计字段命名**（display name / Go 字段）：`list_obj` (`ListedObjects`) / `list_mp` (`ListedMp`) / `list_all` (`ListedAll=list_obj+list_mp`) / `ok_obj` (`OkObjects`) / `corrupt_obj` (`CorruptedObjects`) / `ok_mp` (`OkMp`) / `corrupt_mp` (`CorruptedMp`) / `list_failed` (`ListFailed`) / `list_parse_failed` (`ListParseFailed`，mode=offset ETag 解析失败计数) / `check_failed` (`CheckFailed`) / `mp_check_failed` (`MpCheckFailed`) / `read` (`ReadLines`，文件输入模式的累计已读输入行数，每读一行 +1 含坏行；**不统计总行数**——预扫描整个输入文件不值得)。**关键语义**：`ok_mp` 只在 `multipart_check_mode!=0` 且通过分段检查时 +1；mode=0 时多段对象只计 `list_mp`，**不**计 `ok_mp`——未校验不能谎称干净（mode=1 的 list_parse_failed 回落对象同理只计 `list_mp`）。`is_check=false`（list-only）时 `ok_obj`/`corrupt_obj`/`ok_mp`/`corrupt_mp`/`check_failed`/`mp_check_failed`/`list_parse_failed` 全部为 0，summary 不输出这些字段。

16. **进度行与 summary 按 `RunMode` 输出指标集**：`ModeListCheck`/`ModeListOnly` 用 list/check 全量字段；`ModeListFile`（-list-file，旧）打 `read=X` + `ok_mp/corrupt_mp/mp_check_failed`；`ModeCheckFile`（-check-file）打 `read=X` + `ok_obj/corrupt_obj/ok_mp/corrupt_mp/check_failed/mp_check_failed`（该模式重查普通+多段两种对象）；`ModeBackup`（-backup-file）打 `read=X` + `backup_ok/backup_failed/backup_mismatch`。文件输入模式**不**输出 `list_all`/`list_calls`/`list_obj` 等 list 指标——无 S3 LIST，全是 0 噪声。`q=` 队列快照同理按模式裁剪（文件模式无 BFS 队列，backup 用 `BackupChannelSnapshot`）。

17. **探测矩阵与双 regex**（见 §1.1 故障背景的三类 streaming 类型）：`chunkSigRe` 已去 `^` 锚（边界探测中下一段 chunk-signature 落在 256B slice 的 byte 128，锚定会漏）；新增 `trailerRe` = `x-amz-checksum-(sha256|crc32|crc32c|sha1|crc64):` 抓 STREAMING-UNSIGNED-PAYLOAD-TRAILER 的段尾/对象尾 trailer marker（unsigned 变体段首是弱特征 `<hexlen>\r\n`，不可用，**只能**靠 trailerRe）。**list-check 与文件检查模式（-check-file/-list-file）共用 `runProbes`**（返回 `probeClean` / `probeCorrupted` / `probeFailed` 三态，调用方各自 routing：checker.go verify 写 corrupted/check_failed/ok_*；backup.go **不再探测**——见不变量 19）。`runProbes(ctx, ...)` 接收 hard ctx（第二次信号取消 in-flight 探测，失败任务落 check_failed/mp_check_failed 成为补跑输入）。`runProbes` 对同一 body 用 `chunkSigRe.Match(body) || trailerRe.Match(body)` 一次判定，不算两次 probe。`buildProbesStatic(task, threshold)` 是包级函数（threshold 从 cfg 传入，runProbes 无 cfg 依赖），按 task 形状产出探测计划：(a) `Size==0` 在 `Handle` 短路（不请求）；`0<Size<=whole_object_probe_threshold` → 单次 `RangeGetAt(0, thr)` 全读，匹配双 regex，1 请求；(b) 普通对象 `Size>thr` → head@0(128B) + tail@Size-128(128B) = 2 请求；(c) 多段 N part `Size>thr` → head@0 + (N-1) 个边界探测 `[off_i-128, off_i+128]`(256B，覆盖上一段尾 trailer + 下一段首 chunk-sig) + tail@Size-128 = N+1 请求。边界/tail 的 start/length 按 Size clamp（`max(0, off_i-128)`、`min(Size, off_i+128)`、Size<128 时 tail 读全对象）。任一探测命中即 early-exit 判 corrupted（普通→`corrupted_objects.txt`，多段→`corrupted_mp.txt` 带 offsets），不变量 14 的失败分流与行格式不受影响。`whole_object_probe_threshold=0` 禁用快路径恒走多探测。

18. **文件检查模式（-check-file / -list-file）的 HeadFirst 与 HEAD 权威判型**：输入行不带 ETag/Size（`parseCheckFileLine`/`parseListFileLine` 返回 `HeadFirst=true`），`Checker.Handle` 检测 `HeadFirst` 先 `HeadObject(c.ctx)` 填充 ETag/Size，**并以 HEAD ETag 为权威判型**（输入行描述的是上一轮检查时的对象，期间可能被覆盖）：HEAD 普通 + 行多段 → 丢弃 stale offsets 按 head+tail 探测（结果路由到普通对象文件）；HEAD 多段 + 行普通（无 offsets）→ `verify()` 路由 `mp.txt` 不认干净（无边界无法逐段验证）。**HEAD 失败按行声明的类型分流**（HEAD 失败时行是唯一信息源）：普通行 → `check_failed`，多段行 → `mp_check_failed`（保留 offsets）。**若不 HEAD**：`buildProbesStatic` 的 `if task.Size > 0` 门会跳掉 tail 探测，段尾 trailer marker 静默漏判（历史 bug）。list-check 模式不设 `HeadFirst`：S3 LIST 已带 ETag/Size，无需再 HEAD。-check-file 是 mixed 格式（`parseCheckFileLine` 委托 `parseMixedLine`，与 -backup-file 同一解析器）；-list-file 是多段-only 旧格式（`parseListFileLine`），保留仅为兼容。

19. **backup 模式不探测损坏**（`-backup-file`）：`BackupChecker.Handle` = HEAD → 判型门（mismatch）→ 中转 → ETag 终验。**不调用 runProbes**——输入列表是检查阶段的产物（corrupted_objects/corrupted_mp 跨轮并集），是否损坏已判定，备份只保字节。历史上 backup 曾在多段行上先探测"确认损坏才中转、干净跳过（backup_skipped_clean）"，该路径已整体删除：它会静默跳过检查阶段已标记的对象（backup 侧探测矩阵不如检查侧完整时即漏备份）。`backup_skipped_clean` 文件/channel/stats/progress 字段全链路不存在。mismatch 判型门保留（HEAD 顺手可得、零成本；拦"检查后对象被改写"——stale offsets 中转必然 ETag 终验失败），mismatch.txt 行可直喂 `-check-file` 重查。HEAD 保留是必须的：中转需要 Size，ETag 终验需要源 ETag。`BackupChecker` 的 ctx 是 hard ctx：第二次信号中止 in-flight 中转（多段 AbortMultipart 清理），失败任务落 backup_failed（本模式自己的补跑输入）。

20. **两段式信号与失败文件即断点**（断点续传的总体设计）：
  - **架构原则**：不做进程内 checkpoint/journal（per-object done-set 对十亿级对象是写放大灾难；probe 幂等；LIST 天然带 continuation token；append-only 文件 + 每轮新时间戳目录已构成检查点）。**每个失败文件有且仅有一个补跑入口**：`list_failed.txt` → `-resume-list`（仅 Mode 2；Mode 1 根/子前缀游标语义在行内不可区分、Mode 3 递归无游标——这两类列举失败重试 = 原命令重跑，union 吸收重复）；`check_failed.txt`/`mp_check_failed.txt`/`list_parse_failed.txt`/`mismatch.txt` → `-check-file`（循环喂回至收敛；list_parse_failed.txt 行格式 `bucket|key` 对齐 check_failed.txt，retry HEAD 重新取 ETag/Size）；`backup_failed.txt` → `-backup-file` 自喂；`parse_failed.txt`/`invalid_keys.txt` → 人工；`mp.txt` → 人工补 offsets。跨轮逻辑结果 = 各 run 目录并集（`sort -u` 去重），收敛判据 = 最新一轮失败文件为空。
  - **第一次 SIGINT/SIGTERM**（`installSignalHandler` cancel listCtx）：停止 S3 LIST 与输入文件读取；in-flight 页的 ListPage 返回 ctx 错误 → 现有失败路径写 `list_failed(prefix, 本页游标)`——记录的是**未完成页**的 cursor，已成功的页不重列；队列里未弹出的 prefix 由 `drainQueuedPrefixes`（在 `listWg.Wait` 后、`close(objCh)` 前）写入 list_failed（裸 prefix = 从头列；`-resume-list` 种子 `prefix\x00token` 原样往返其游标，`Queue.Drain()` 取残余）；objCh 中已入队任务继续被 check worker 消费至完成。**不变量：run 目录是完备检查点**——每个 prefix 要么列完且每个对象都有终态，要么带可续游标。run 返回 `errInterrupted`（非零退出，数据无损失）。
  - **第二次信号**（cancel hardCtx）：in-flight 探测/中转中止（`Checker`/`BackupChecker`/relay 全走 hard ctx），objCh/ch 中剩余任务快速失败落 `check_failed`/`mp_check_failed`/`backup_failed`（即各自的补跑输入）——**硬停也不静默丢对象**。多段中转 Abort 清理。
  - **硬停 mid-page 的游标语义**：lister/walker/Mode-1 根循环的 objCh 发送 select 在 hardCtx.Done 分支记录**当前页**的 continuationToken（不是下一页）——resume 重列本页即可找回未发送的对象；已发送对象在 check worker 侧因 ctx 取消落 check_failed，同样可补跑。
  - **kill -9 是唯一有损路径**：未处理部分无记录，补救 = 同 prefix 重跑全量（幂等 + union）。

## 5. CLI 与配置

### CLI flags
```
-c <path>          # 配置文件，必填
-bkt <bucket>      # 桶名，必填
-prefix <prefix>   # 列举前缀，可选，默认空
-nextmarker <key>  # start-after key，可选；仅 Mode 1 根分页使用
-check-file <path> # 重查文件，可选；mixed 格式（bkt|key / bkt|key|partcnt|off...），check_failed/mp_check_failed/mismatch 的统一重试入口（与其他文件 flag 互斥，需 is_check=true）
-list-file <path>  # 列表文件，可选；旧模式，多段-only 格式跳过 S3 LIST 按行校验（与其他文件 flag 互斥，需 is_check=true）
-backup-file <path># 备份列表文件，可选；HEAD 判型+中转+ETag 终验，不探测损坏（与其他文件 flag 互斥，需配置 backup_bucket + backup_output_dir）
-resume-list <path># 断点续跑文件，可选；读 list_failed.txt 格式 `prefix|token` 把 (prefix, token) seed 进 BFS（与其他文件 flag 互斥，仅 list_type=2 生效；Mode 1/3 的列举失败重试 = 原命令重跑）
```

### config.yaml 字段
| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | ip:port 列表，至少 1 个 |
| `scheme` | `http` | `http` 或 `https`（后者跳过证书校验） |
| `ak` / `sk` | 必填 | SigV4 静态凭证 |
| `list_type` | `2` | 1=子目录+平铺 nextmarker；2=递归 BFS delimiter；3=递归+信号量 |
| `list_api_version` | `1` | 1=ListObjects V1（marker 分页）；2=ListObjectsV2（continuation token） |
| `list_concurrency` | `8` | 列举并发度 |
| `check_concurrency` | `16` | 校验并发度 |
| `output_dir` | `.` | 输出目录 |
| `output_dir_timestamp` | `true` | true=目录名追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`）隔离每次运行；false=固定使用 output_dir 原样路径（断点续跑需显式 false）。后缀在 `LoadConfig` 内追加（尾部 `/` 先裁剪），下游全部用改写后的 `cfg.OutputDir`；同一 `stamp` 同时作用于 `backup_output_dir` |
| `backup_output_dir` | （无，必填） | `-backup-file` 模式专用输出目录；与 `output_dir` 分离以免 backup 结果与 list/check 结果混写。`NewBackupOutput` 用 `cfg.BackupOutputDir` 建 `MkdirAll` 并写所有 backup 文件；list/check 模式忽略该字段。`main` 启动时若 `-backup-file` 而 `BackupOutputDir==""` 直接 `os.Exit(2)`，同样受 `output_dir_timestamp` 控制并与 `output_dir` 共享同一时间戳成对生成 |
| `is_check` | `true` | true=列举+校验；false=仅列举（不校验普通对象，不写对象文件，不创建 owner 目录，仅写 list_failed.*） |
| `is_success_log` | `false` | 是否记录正常普通对象到 `<ownerID>/ok_objects.txt` |
| `multipart_check_mode` | `0` | 多段对象损坏检查模式：`0`=关闭（全部写 mp.txt 不检查）；`1`=offset 检查（LIST 带 `internal-list-mp-offset: true` header，多段 ETag 返回 `<md5>-<partcnt>-<off0>\|<off1>\|...`，按真实 part 边界逐段检查；解析不出 offsets 回落 list_parse_failed.txt，**不再**写 mp.txt。**优先级高于 mode=2**）；`2`=固定分段检查（旧模式待废弃，必须配 `multipart_segment_size > 0`）。非法值启动报错；配置里出现已废弃的 `is_multipart_segment_check` 字段也报错并提示迁移（防旧配置被静默当作 mode=0） |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `whole_object_probe_threshold` | `1024` | 小对象全读阈值（字节）。`0<Size<=该值` 时走快路径：单次 `RangeGetAt(0, thr)` 读全对象，body 同时匹配 `chunkSigRe`（去 `^` 锚）OR `trailerRe`（`x-amz-checksum-(sha256\|crc32\|crc32c\|sha1\|crc64):`），1 请求覆盖段首+段尾两种损坏；超过该值走 head@0+tail@Size-128（普通）/head@0+(N-1)边界+tail@Size-128（多段）多探测。`0`=禁用快路径恒走多探测。负值启动报错 |
| `is_multipart_success_log` | `false` | 是否记录干净的多段对象到 `<ownerID>/ok_mp.txt` |
| `node_isolate_threshold` | `3` | 节点隔离阈值：进程级累积的节点故障数（连接错误/超时/5xx，4xx 业务错误不计）达到该值才隔离节点；`1`=旧的首次故障即隔离。计数无时间衰减，跨 worker 共享 |
| `node_recover_probe_interval` | `60` | 隔离节点恢复探测间隔（秒）。后台 goroutine 每隔该值对隔离节点 HEAD bucket，连续 2 次健康（`!isNodeFaultErr`，即任何正常 S3 应答）→ 恢复进轮询池并清零故障计数；`0`=禁用恢复（隔离在进程内永久，旧行为） |
| `progress_interval` | `5000` | 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；`0` 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；`0` 走默认 |
| `result_line_format` | `<bucket>\|<key>` | 结果文件每行格式，支持 `<bucket>`/`<key>`/`<owner>` 占位符；只影响 per-owner 结果文件，处理文件始终只存 key/prefix |

## 6. 输出文件（append 模式）

### 结果文件（按 OwnerID 分目录，`<output_dir>/<ownerID>/<filename>`；OwnerID 为空 → `_unknown/`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏普通对象 key | Range GET 命中 chunk-signature/trailer（`is_check=true`） |
| `mp.txt` | 多段对象 key（仅 key） | `multipart_check_mode=0` 时所有多段对象；`-check-file` 时普通行 HEAD 判多段的漂移对象（无 offsets 不认干净）；`mode=segment + Size==0`（无法合成 offset）。`mode=offset` 的 ETag 解析失败**不**写 mp.txt（改写 list_parse_failed.txt）。`-list-file` 不创建（任务恒带 offsets，无回落路径） |
| `corrupted_mp.txt` | 损坏多段对象 key | `multipart_check_mode=1/2` 或文件检查模式（-check-file/-list-file）分段检查命中。mode=1 与文件输入模式行带 `bkt\|key\|partcnt\|off0\|off1\|...`（真实 part 边界，可直喂 -backup-file/-check-file，不套 result_line_format）；mode=2 行按 result_line_format |
| `ok_mp.txt` | 干净多段对象 key | `multipart_check_mode=1/2`（或文件检查模式）且 `is_multipart_success_log=true`，行按 result_line_format |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true` |

### 处理文件（全局，根目录 `<output_dir>/<filename>`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `list_failed.txt` | 列举失败 `prefix\|token`（token 为失败页 continuationToken，第一页失败/未启动时为单字段 prefix） | list worker 调用失败（list-only 模式也写）；优雅中断时未启动 prefix 亦写入；可直喂 `-resume-list` 补跑 |
| `list_failed.log` | 列举失败结构化错误（slog text，req_id/prefix/http_code/s3_code/err） | 同上 |
| `parse_failed.txt` | 输入解析失败的原始行（-check-file / -list-file / -backup-file / -resume-list 坏行、bkt 不匹配、空行） | `fileSource`/`readResumeList` 解析失败时写；不可自动补跑，需人工修输入 |
| `list_parse_failed.txt` | mode=offset 下 ETag 解析失败的 multipart 对象 `bucket\|key` | **仅 mode=offset + S3 LIST**：服务端未实现 header / ETag 格式不符 / offset > Size。行格式对齐 check_failed.txt，可直喂 `-check-file` 补跑（retry HEAD 重新取 ETag/Size）。配套 `list_parse_failed.log`（slog，bucket/key/owner/size/etag_len/etag，完整 ETag 无截断） |
| `invalid_keys.txt` | 违反 `\|` 字段分隔契约的 key/prefix（原始字节，不套格式） | lister LIST 拿到 key 含 `\|`（跳过对象不进 objCh）；`WriteListFailed` prefix 含 `\|`（不写 list_failed 改写此文件） |
| `check_failed.txt` | 普通对象校验失败 `bkt\|key` | checker 普通对象 RangeGet/HEAD 失败；硬停时队列中未探测对象亦快速失败落此文件；可直喂 `-check-file`/`-backup-file` 普通对象输入补跑 |
| `check_failed.log` | 校验失败结构化错误（slog text，req_id/key/http_code/s3_code/err） | 同上 |
| `mp_check_failed.txt` | 多段分段检查失败 `bkt\|key\|partcnt\|off0\|...` | `multipart_check_mode=1/2`（或文件检查模式）时分段 RangeGet/HEAD 失败；可直喂 -check-file/-backup-file 补跑 |
| `mp_check_failed.log` | 多段分段检查失败结构化错误（slog text） | 同上 |
| `backup_ok.txt` | 备份成功的原始输入行 | `-backup-file`：中转 + ETag 终验通过 |
| `backup_failed.txt` | 备份失败的原始输入行（可自喂重试） | `-backup-file`：HEAD 失败（stage=head）/中转失败（stage=upload）/ETag 终验失败（stage=etag，坏副本保留）；硬停时队列中未处理对象快速失败落此文件 |
| `backup_failed.log` | 备份失败结构化错误（stage/head→upload→etag） | 同上 |
| `mismatch.txt` | 输入类型校验失败的原始行（对象在检查后被改写） | `-backup-file`：行声明类型与 HEAD 判型不一致；可直喂 `-check-file` 重查当前状态 |

`is_check=false` 时不校验普通对象，不写任何对象文件，不创建 owner 目录，仅写 `list_failed.*`。`is_check=true && multipart_check_mode=0`（非文件检查模式）时 `corrupted_mp.txt` / `ok_mp.txt` / `mp_check_failed.*` 不创建。文件输入模式的 owner 恒为空 → 结果全落 `_unknown/`。

### 结果文件行格式

per-owner 结果文件每行按 `result_line_format` 配置渲染（默认 `<bucket>|<key>`），启动时在配置快照里打印实际生效值。解析在 `NewOutput` 完成（`parseLineFormat`），未知占位符 / 未闭合 `<` 报错并中止启动。处理文件（`list_failed`/`parse_failed`/`invalid_keys`/`check_failed`/`mp_check_failed` 的 .txt 与 .log）**不**套用此格式，而是按各自补跑链路需要的格式写入：
- `list_failed.txt` = `prefix|token`（token 为空时单字段 prefix）— `-resume-list` 解析此格式
- `parse_failed.txt` = 原始输入行（整行，不切分）
- `invalid_keys.txt` = 原始 key/prefix（整行，不切分）
- `check_failed.txt` = `bkt|key` — 对齐 `-backup-file` 普通对象 2 字段输入
- `mp_check_failed.txt` = `bkt|key|partcnt|off0|...` — 对齐 `-backup-file`/`-list-file` 多段输入
- `.log` 文件用 slog text handler，结构化字段（req_id/bucket/key/http_code/s3_code/err）

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

用于改动后回归：验证 list/check/分段检查/owner 分桶/`result_line_format`/run.log 全链路。整个流程在 `/tmp/chunked-e2e/` 下，**非仓库内容**，可随改随丢。

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

# 4. seed 2 个多段对象（用 minio-go，partSize=5MiB 触发 multipart）
#    mp/clean.bin   — 6MB 正常 body（ETag <hex>-2）
#    mp/corrupt.bin — 首段开头 128 字节是 chunk-sig 头（ETag <hex>-2，分段检查命中）
mkdir -p /tmp/chunked-e2e/mp-seed
#    见 /tmp/chunked-e2e/mp-seed/main.go（仓库外 helper，用 minio-go v7）
cd /tmp/chunked-e2e/mp-seed && go run .

# 5. 写 cfg（is_check=true, multipart_check_mode=2, segment_size=5242880）
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
is_check: true
is_success_log: true
multipart_check_mode: 2
multipart_segment_size: 5242880
is_multipart_success_log: true
progress_interval: 2
result_line_format: <bucket>|<key>
EOF

# 6. 编译并跑
cd /Users/gengyuanzhe/code/S3/golang/chunked_check_tool/chunked_check_tool
go build -o /tmp/chunked_check_tool .
/tmp/chunked_check_tool -c /tmp/chunked-e2e/cfg.yaml -bkt testbucket
```

**期望结果**（testbucket 8 对象）：

| 输出 | 内容 |
|---|---|
| `out/run.log`（=== summary === 段） | `list_all: 8 (list_obj: 6 list_mp: 2)` `ok_obj: 5 corrupt_obj: 1 ok_mp: 1 corrupt_mp: 1 list_failed: 0 check_failed: 0 mp_check_failed: 0` |
| `out/minio/corrupted_objects.txt` | `testbucket\|corrupted/corrupted.bin` |
| `out/minio/ok_objects.txt` | 5 行 `testbucket\|data/2026/01/file_0N.bin` |
| `out/minio/corrupted_mp.txt` | `testbucket\|mp/corrupt.bin` |
| `out/minio/ok_mp.txt` | `testbucket\|mp/clean.bin` |
| `out/{list_failed,check_failed,mp_check_failed}.txt` | 空 |
| `out/run.log` | 含配置快照（ak/sk `***`）+ 进度行 + summary |

`out/minio/` 路径名里的 `minio` 是 LIST 响应 Owner 字段（root 用户 → OwnerID=`minio`）；空 OwnerID 会落到 `_unknown/`。

**multipart_check_mode=1 负向验证**（本地 minio 不识别 `internal-list-mp-offset`，正好验证回落路径）：cfg 改 `multipart_check_mode: 1`（删 segment_size）重跑——minio 返回普通 `<md5>-<N>` ETag → 解析失败 → 两个多段对象都落 `out/minio/mp.txt`，summary `ok_mp: 0 corrupt_mp: 0`，corrupted_mp/ok_mp 文件为空或不创建。header 实际已发出（`TestMpOffsetTransportMinioList` 单测覆盖）。**正向路径**（offset 检查全链路）需服务端实现该 header 后在真实集群验证：corrupted_mp.txt 行 `bkt|key|partcnt|off0|...` 可直喂 `-backup-file`。

**断点续跑**：`output_dir` 是 append 模式，重跑会累加。想干净跑就换 `output_dir`（`sed 's#out#out2#'`）。

**清场**（minio 后台进程 + /tmp 数据）：
```bash
pkill -f 'minio server.*127.0.0.1:9100'
# /tmp/chunked-e2e 视情况删；权限系统可能拒绝 rm -rf，必要时用 rm 逐文件
```

**mp-seed helper**（`/tmp/chunked-e2e/mp-seed/main.go`，非仓库代码）：用 `github.com/minio/minio-go/v7` 上传两个 5MiB+ 对象触发 multipart，partSize 必须是 `5*1024*1024`（minio 最小 part size）。`mp/corrupt.bin` 的首段前 128 字节是 `1000;chunk-signature=...` 头，其余是 filler——分段检查在段 0 offset 0 命中。helper 无独立 go.mod，从仓库目录 `go run /tmp/chunked-e2e/mp-seed2/main.go`（借仓库模块上下文解析 minio-go）。

### 7.2 -check-file / -backup-file 冒烟（复用 7.1 环境）

复用 7.1 的 8 对象数据（5 干净普通 + 1 损坏普通 + 2 个 6MB 两段对象，多段 offsets = `[0, 5242880]`）。要确定性对象数就用全新 data 目录重跑 7.1 步骤 1-4。

**-check-file**（HEAD 权威判型 + 失败分流 + 漂移回落）：

```bash
cat > /tmp/chunked-e2e/check-input.txt <<'EOF'
testbucket|corrupted/corrupted.bin
testbucket|data/2026/01/file_01.bin
testbucket|mp/corrupt.bin|2|0|5242880
testbucket|mp/clean.bin|2|0|5242880
testbucket|mp/clean.bin
testbucket|no-such-key
testbucket|bad|1|100
EOF
# cfg 关键项：is_check+is_success_log+is_multipart_success_log，multipart_check_mode: 0
# （0 验证文件模式无视该配置——mp 输出照常开启，offsets 来自输入行）
/tmp/chunked_check_tool -c cfg.yaml -bkt testbucket -check-file /tmp/chunked-e2e/check-input.txt
```

期望 summary：`read: 7` + `ok_obj: 1 corrupt_obj: 1 ok_mp: 1 corrupt_mp: 1 check_failed: 1 mp_check_failed: 0 parse_failed: 1`。文件（全落 `_unknown/`）：

| 文件 | 内容 |
|---|---|
| `corrupted_objects.txt` | `testbucket\|corrupted/corrupted.bin` |
| `corrupted_mp.txt` | `testbucket\|mp/corrupt.bin\|2\|0\|5242880`（带 offsets，可直喂 -backup-file） |
| `ok_mp.txt` / `ok_objects.txt` | `mp/clean.bin` / `file_01.bin` |
| `mp.txt` | `testbucket\|mp/clean.bin`（漂移行：普通行 + HEAD 多段 → 无 offsets 不认干净） |
| `check_failed.txt` | `testbucket\|no-such-key`（普通行 HEAD 404 按行型分流） |
| `parse_failed.txt` | `testbucket\|bad\|1\|100` |

**-backup-file**（不探测直接中转）：

```bash
cat > /tmp/chunked-e2e/backup-input.txt <<'EOF'
testbucket|corrupted/corrupted.bin
testbucket|mp/corrupt.bin|2|0|5242880
testbucket|mp/clean.bin|2|0|5242880
testbucket|mp/clean.bin
EOF
# cfg 关键项：backup_bucket: backupbucket + backup_output_dir（先 mc mb local/backupbucket）
/tmp/chunked_check_tool -c cfg.yaml -bkt testbucket -backup-file /tmp/chunked-e2e/backup-input.txt
```

期望 summary：`read: 4` + `backup_ok: 3 backup_failed: 0 backup_mismatch: 1`，**`get_calls: 0`**（零探测的证据——HEAD/下载不经过 RangeGetAt 计数路径）。文件与远端：

- `backup_ok.txt` 3 行——**含干净多段**（旧行为是 backup_skipped_clean 跳过）；`backup_skipped_clean.txt` 不存在
- `mismatch.txt` = `testbucket|mp/clean.bin`（第 4 行普通行 vs HEAD 多段），可喂回 -check-file 重查
- 目标桶三对象 `mc stat` ETag 与源一致（多段 `-2` 形式 = 分段边界保真）；`mc cat local/testbucket/<k> | cmp -s - <(mc cat local/backupbucket/<k>)` 字节级相同
- `.backup_lists/backup-input_<时间戳>.txt` 归档存在

### 7.3 两段式信号冒烟（SIGINT 优雅排空 + -resume-list 闭环）

本地 minio 很快（9000 对象 <1.6s 跑完），SIGINT 时机取预估运行时长的 ~70-80%：

```bash
# seed 2000 前缀 × 3 对象（拉长运行；配合 7.1 的 300×10 共 9008 对象）
mkdir -p /tmp/chunked-e2e/bulk2 && cd /tmp/chunked-e2e/bulk2
for d in $(seq -w 0 1999); do mkdir -p $d; for f in 1 2 3; do head -c 4096 /dev/urandom > $d/obj_$f.bin; done; done
mc cp --recursive . local/testbucket/bulk2/

# 后台全量扫描，1.2s 后 SIGINT（check_concurrency 调低可拉长运行便于掐点）
/tmp/chunked_check_tool -c cfg-sig.yaml -bkt testbucket > /tmp/chunked-e2e/sig.log 2>&1 &
PID=$!; sleep 1.2; kill -INT $PID; wait $PID; echo "exit=$?"
```

期望：

- run.log 含 `signal received: stopping listing, draining in-flight work`；summary 在 drain 完成后照常 flush（total_sec 覆盖 drain 时长）
- 退出码非 0，run.log 末尾 `run: interrupted: graceful drain complete, output is a resumable checkpoint`
- **终态完备**：`list_obj == ok_obj + corrupt_obj`（+ mode 下落 mp.txt 的多段量），`check_failed.txt`/`mp_check_failed.txt` 为空——已列举对象在 drain 中全部检查完
- `list_failed.txt` 非空：未启动前缀为**裸 prefix**，`list_failed.log` 记 `err="interrupted before listing started: context canceled"`；worker 恰在分页中时会记 `prefix|token`（时序相关，本次未命中——该路径由 `TestListerGracefulCancelRecordsNextPageToken` 单测钉死）
- **闭环验证**：`/tmp/chunked_check_tool -c cfg -bkt testbucket -resume-list <中断轮目录>/list_failed.txt` 跑完 `list_failed: 0`，且两轮 `list_all` 之和 == `mc ls --recursive local/testbucket | wc -l`（无丢失无重复的完备性证明）
- 第二次信号（硬停）时序难在真机稳定触发，由单测覆盖（`TestListerHardAbortMidPageRecordsCurrentPageToken` + `TestRunInterruptedDrainsQueue`）

## 8. 已知遗留项（改动时留意，非阻塞）

来源：SDD ledger 的 deferred minors（见 `.superpowers/sdd/2026-09-03-chunked-check-tool/progress.md`）。

- **Queue.Pop 阻塞路径的 lost-signal 窗口**：fast-path unlock 与 re-lock+Wait 之间存在理论上的丢失唤醒窗口。当前 pipeline 关闭队列时用 `Broadcast`，不会触发。若未来改为单消费者关闭场景需重新评估。
- **NodePool.Assign 空端点 panic**：`n=0` 时除零。`Config` 上游校验 `endpoints` 非空，故不会触发。无 `idx` 越界保护（调用方只用 `Assign` 返回的索引）。
- **NewOutput 部分初始化失败的 goroutine/file 泄漏**：brief 继承的设计，仅 `os.OpenFile` 失败时触发（罕见启动期磁盘错误）。修复需偏离 brief。
- **bufio flush 错误静默丢弃**：writer goroutine 不检查 `Flush` 错误，无 Write/Close race guard（标准使用契约）。
- **Core.ListObjectsV2 无 ctx 参数**：minio-go 限制， cancellation 在更高层（放弃 goroutine on `ctx.Done`），靠 socket 超时兜底。
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
