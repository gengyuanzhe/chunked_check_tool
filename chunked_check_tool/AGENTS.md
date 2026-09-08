# AGENTS.md — chunked_check_tool

本文件面向在本仓库工作的 AI 代理（和人类工程师），说明项目意图、模块边界、关键不变量与已知遗留项。改动前请先读完本文件对应章节。

## 1. 项目意图

自研 S3 存储系统曾因未正确解析 `X-Amz-Content-Sha256` header，把 `aws-chunked` PUT 请求的 `length;chunk-signature=…` / `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式化内容当作原始 body 写入存储。

本工具**并发**列举并校验对象：
- 普通对象（ETag 为 32 位小写 MD5 hex）→ Range GET 前 128 字节，匹配 chunk-signature 正则 → 命中即视为损坏。
- 多段对象（任何非 `^[0-9a-f]{32}$` 的 ETag）→ 直接入 `mp.txt`，**不做 Range GET**。
- 列举失败、校验失败分别落不同文件。
- 支持几十亿对象规模，内存有界，背压自调节。

**不在本工具范围**：修复损坏对象（另行处理）。

## 2. 技术栈

- Go 1.27（`go.mod` module `chunked_check_tool`）
- `github.com/minio/minio-go/v7`（MinIO SDK，非 AWS SDK；用 `Core.ListObjectsV2` 拿 Contents + CommonPrefixes，用 `Client.GetObject` + `GetObjectOptions.SetRange(0, 127)` 做 Range GET）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（用户明确要求忽略证书）

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析、`signal.NotifyContext`（SIGINT/SIGTERM）、`run` 编排、Mode 1 根列举分页、worker 启停、stats 写盘 |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，带默认值） |
| `nodepool.go` | `NodePool`：轮询 `Assign`、`MarkFailed`、`URL`、`Endpoint`（故障隔离，全局共享 failed 集） |
| `s3client.go` | `S3API` 接口、`S3Client`（`minioListAPI` 接口包装 minio.Core + minio.Client，按 `cfg.ListAPIVersion` 分派 V1/V2）、`FakeS3`/`scriptedS3`（测试用）、节点故障重试一次 |
| `lister.go` | `Lister`（无 `s3` 字段；`Run`/`processPrefix` 接 `s3` 参数）、无界队列 BFS、`inflight` atomic 计数 |
| `walker.go` | `runRecursiveWalk`：Mode 3 信号量递归列举，`sync.WaitGroup` 终止，不用 queue/inflight |
| `checker.go` | `Checker`、`isNormalETag`（严格 32 位小写 hex）、`chunkSigRe` |
| `output.go` | 8 channel + writer goroutine（5 个按 OwnerID 分目录 fan-out，3 个根目录全局），`bufio.Writer` 64KB，append 模式，per-owner 文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`） |
| `queue.go` | 无界队列（slice + mutex + cond），ctx-aware 阻塞 Pop |
| `stats.go` | atomic.Int64 计数器 + `StatsSnapshot` + `PrintSummary`（按 `RunMode` 分模式输出指标集；写入 stdout，被 `mwOut` tee 进 run.log） |
| `progress.go` | `ProgressPrinter`（按 `RunMode` 输出进度行字段集）+ `localCounter`（每 worker 本地 int，无 per-obj atomic） |

## 4. 关键不变量（改动前必须守住）

1. **多段判定严格**：仅 `^[0-9a-f]{32}$`（32 位小写 MD5 hex）算普通对象。大写、长度不对、`<hex>-N`、空值一律按多段处理。**绝不把多段误判为普通对象**。`isNormalETag` 用逐字节循环实现（非正则），不要改成宽松匹配。

2. **多段对象跳过 Range GET**：直接写 `<ownerID>/mp.txt`。每行按 `result_line_format` 渲染（默认 `<bucket>|<key>`），不带 ETag。不要给多段对象发 Range GET（浪费请求 + 可能误判）。

3. **ETag 来源**：list 响应（统一），**不从 Range GET response header 取**。OwnerID 同样来自 list 响应（minio-go v7.3.0 默认 `fetchOwner=true`，无额外请求开销）。

4. **结果文件按 OwnerID 分目录，处理文件全局**：`corrupted_objects`/`mp`/`corrupted_mp`/`ok_mp`/`ok_objects` 这五类结果文件按 `<ownerID>/<filename>` 路由（OwnerID 为空 → `_unknown/`）；`list_failed`/`check_failed`/`mp_check_failed`/`stats` 留根目录全局。理由：结果文件数量大且天然按 owner 分桶有意义；处理文件全局方便运维统一排查；stats 全局一份避免 owner 分桶后还要汇总。ownerDirName 折叠空/`.`/`..`/含路径分隔符的 OwnerID 到 `_unknown`，防止路径穿越。

5. **checker goroutine 绝不退出**：任何错误写 `check_failed`（普通对象）/ `mp_check_failed`（多段分段）后继续。若 checker 退出，`objCh` 无人消费，list worker 永久阻塞。

6. **`objCh` 永远会被关闭**：list 阶段保证（`listWg.Wait` → `close(objCh)`；或 Mode 1 无 seed 时显式 `q.Close()`）。

7. **Add-before-Push 顺序**（BFS 终止性）：Mode 2 在 push 新 subprefix 前 `inflight++`，否则 push 后 worker 消费完 inflight 已归零，新 subprefix 无人处理 → 永久挂起。改动 `lister.go` 的 `processPrefix` 时务必保留此顺序。

8. **Mode 1 不 seed 根 prefix**：根的直接对象由 `main` 用 delimiter 分页拿（Contents → objCh）；只把 CommonPrefixes（子目录）seed 进队列。若 seed 根，worker 会用 `delim=false` 平铺列出根，与 main 的 delimiter 分页重复，根下子目录对象被计两次。

9. **`-nextmarker` 仅 Mode 1 生效**：作为根列举的 start-after 参数。Mode 2/3 忽略（文档化限制）。

10. **节点故障重试一次**：`S3Client.ListPage`/`RangeGet`/`RangeGetAt` 在 `isNodeFaultErr`（连接拒绝、超时、5xx，**不含 4xx**）时 `pool.MarkFailed` → `pool.Assign` 找下一个存活节点 → 重建 client → 重试一次。再失败按业务错误处理（写 `list_failed`/`check_failed`/`mp_check_failed`）。普通 S3 业务错误（404/403）不触发重绑。

11. **性能优先但可读**：HTTP keep-alive（minio-go 自带连接池，不要自建）、`bufio.Writer` 64KB、合理 channel 容量、避免 per-obj 分配。**但**任何"复杂难读"的优化（手写内存池、unsafe、lock-free 结构）需先向用户请求确认，不要直接写。见 `memory/performance-vs-readability.md`。

12. **Mode 3 用 WaitGroup 终止**：`walker.go` 的 `runRecursiveWalk` 不用 queue、不用 inflight 计数；终止性靠 `sync.WaitGroup`。每个 `go walk(subprefix)` **前**必须 `wg.Add(1)`，否则 Wait 可能在 spawn 前归零、过早关闭 objCh。root 首个 `wg.Add(1)` 同理在 `go walk(prefix)` 前。objCh 由 main 中的 walk goroutine 在 `wg.Wait()` 返回后显式关闭。子树列举失败只写 `list_failed` 跳过该子树，不中止其他分支。

13. **V1/V2 分页协议对 caller 透明**：`S3Client.listPageOnce` 按 `cfg.ListAPIVersion` 分派 `Core.ListObjects`（V1，marker 游标）或 `Core.ListObjectsV2`（V2，continuation token）。两条路径都归一化进 `listResult{contents, commonPrefixes, next}`，`next` 作为下一次 `ListPage` 的 `continuationToken` 参数回传。V1 无 delimiter 且 `IsTruncated=true` 但 `NextMarker` 为空时，回退到最后一个 Contents key 作 marker；有 delimiter 时 S3 返回 `NextMarker`。caller（lister/walker/main 根分页）只需把 `next` 喂回 `continuationToken`，不感知 V1/V2 差异。`S3Client.core` 是 `minioListAPI` 接口（非 `*minio.Core`）以支持测试注入。

14. **多段分段检查的失败分流**：分段 RangeGet 报错走 `mp_check_failed` 路径（`WriteMpCheckFailed` + `IncrMpCheckFailed`），**不走** `check_failed`。任一段命中 chunk-signature 即视为整段对象损坏，写 `<ownerID>/corrupted_mp.txt` 并 `IncrCorruptedMp`（同时**不** `IncrOkMp`）。干净的多段对象 `IncrOkMp`，仅 `is_multipart_success_log=true` 时写 `<ownerID>/ok_mp.txt`（与普通对象的 `is_success_log` 独立，互不影响）。

15. **统计字段命名**（display name / Go 字段）：`list_obj` (`ListedObjects`) / `list_mp` (`ListedMp`) / `list_all` (`ListedAll=list_obj+list_mp`) / `ok_obj` (`OkObjects`) / `corrupt_obj` (`CorruptedObjects`) / `ok_mp` (`OkMp`) / `corrupt_mp` (`CorruptedMp`) / `list_failed` (`ListFailed`) / `check_failed` (`CheckFailed`) / `mp_check_failed` (`MpCheckFailed`) / `read` (`ReadLines`，-list-file/-backup-file 的输入行消耗，每读一行 +1 含坏行) / `total` (`TotalLines`，启动时 `countFileLines` 统计的输入文件总行数)。**关键语义**：`ok_mp` 只在 `is_multipart_segment_check=true` 且通过分段检查时 +1；switch off 时多段对象只计 `list_mp`，**不**计 `ok_mp`——未校验不能谎称干净。`is_check=false`（list-only）时 `ok_obj`/`corrupt_obj`/`ok_mp`/`corrupt_mp`/`check_failed`/`mp_check_failed` 全部为 0，summary 不输出这些字段。

16. **进度行与 summary 按 `RunMode` 输出指标集**：`ModeListCheck`/`ModeListOnly` 用 list/check 全量字段；`ModeListFile`（-list-file）打 `read=X/Y` + `ok_mp/corrupt_mp/mp_check_failed`；`ModeBackup`（-backup-file）打 `read=X/Y` + `backup_ok/backup_failed/backup_mismatch/backup_skipped_clean`。后两种模式**不**输出 `list_all`/`list_calls`/`list_obj` 等 list 指标——无 S3 LIST，全是 0 噪声。`q=` 队列快照同理按模式裁剪（list-file 无 BFS 队列，backup 用 `BackupChannelSnapshot`）。`Y`（总行数）统计失败时输出裸 `read: X`，不打印 `/0`。

## 5. CLI 与配置

### CLI flags
```
-c <path>          # 配置文件，必填
-bkt <bucket>      # 桶名，必填
-prefix <prefix>   # 列举前缀，可选，默认空
-nextmarker <key>  # start-after key，可选；仅 Mode 1 根分页使用
-list-file <path>  # 列表文件，可选；跳过 S3 LIST 按行校验（与 -backup-file 互斥，需 is_check=true）
-backup-file <path># 备份列表文件，可选；HEAD+校验+中转（与 -list-file 互斥，需配置 backup_bucket）
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
| `output_dir_timestamp` | `true` | true=目录名追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`）隔离每次运行；false=固定使用 output_dir 原样路径（断点续跑需显式 false）。后缀在 `LoadConfig` 内追加（尾部 `/` 先裁剪），下游全部用改写后的 `cfg.OutputDir` |
| `is_check` | `true` | true=列举+校验；false=仅列举（不校验普通对象，不写对象文件，不创建 owner 目录，仅写 list_failed.*） |
| `is_success_log` | `false` | 是否记录正常普通对象到 `<ownerID>/ok_objects.txt` |
| `is_multipart_segment_check` | `false` | 是否按固定 part size（`multipart_segment_size`）对多段对象做分段损坏检查；`true` 时必须配 `multipart_segment_size > 0`，否则启动报错中止 |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `is_multipart_success_log` | `false` | 是否记录干净的多段对象到 `<ownerID>/ok_mp.txt` |
| `progress_interval` | `5000` | 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；`0` 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；`0` 走默认 |
| `result_line_format` | `<bucket>\|<key>` | 结果文件每行格式，支持 `<bucket>`/`<key>`/`<owner>` 占位符；只影响 per-owner 结果文件，处理文件始终只存 key/prefix |

## 6. 输出文件（append 模式）

### 结果文件（按 OwnerID 分目录，`<output_dir>/<ownerID>/<filename>`；OwnerID 为空 → `_unknown/`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `corrupted_objects.txt` | 损坏普通对象 key | Range GET 命中 chunk-signature（`is_check=true`） |
| `mp.txt` | 多段对象 key（仅 key） | `is_multipart_segment_check=false` 时所有多段对象 |
| `corrupted_mp.txt` | 损坏多段对象 key | `is_multipart_segment_check=true` 时分段检查命中 |
| `ok_mp.txt` | 干净多段对象 key | `is_multipart_segment_check=true` 且 `is_multipart_success_log=true` |
| `ok_objects.txt` | 正常普通对象 key | `is_success_log=true` |

### 处理文件（全局，根目录 `<output_dir>/<filename>`）

| 文件 | 内容 | 何时写 |
|---|---|---|
| `list_failed.txt` | 列举失败 prefix | list worker 调用失败（list-only 模式也写） |
| `list_failed.log` | 列举失败结构化错误（slog text，req_id/prefix/http_code/s3_code/err） | 同上 |
| `check_failed.txt` | 普通对象校验失败 key | checker 普通对象 RangeGet 失败 |
| `check_failed.log` | 校验失败结构化错误（slog text，req_id/key/http_code/s3_code/err） | 同上 |
| `mp_check_failed.txt` | 多段分段检查失败 key | `is_multipart_segment_check=true` 时分段 RangeGet 失败 |
| `mp_check_failed.log` | 多段分段检查失败结构化错误（slog text） | 同上 |

`is_check=false` 时不校验普通对象，不写任何对象文件，不创建 owner 目录，仅写 `list_failed.*`。`is_check=true && is_multipart_segment_check=false` 时 `corrupted_mp.txt` / `ok_mp.txt` / `mp_check_failed.*` 不创建。

### 结果文件行格式

per-owner 结果文件每行按 `result_line_format` 配置渲染（默认 `<bucket>|<key>`），启动时在配置快照里打印实际生效值。解析在 `NewOutput` 完成（`parseLineFormat`），未知占位符 / 未闭合 `<` 报错并中止启动。处理文件（`list_failed`/`check_failed`/`mp_check_failed` 的 .txt 与 .log）**不**套用此格式，始终只写 key/prefix（.log 已含 `bucket` 字段）。

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

# 5. 写 cfg（is_check=true, is_multipart_segment_check=true, segment_size=5242880）
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
is_multipart_segment_check: true
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

**断点续跑**：`output_dir` 是 append 模式，重跑会累加。想干净跑就换 `output_dir`（`sed 's#out#out2#'`）。

**清场**（minio 后台进程 + /tmp 数据）：
```bash
pkill -f 'minio server.*127.0.0.1:9100'
# /tmp/chunked-e2e 视情况删；权限系统可能拒绝 rm -rf，必要时用 rm 逐文件
```

**mp-seed helper**（`/tmp/chunked-e2e/mp-seed/main.go`，非仓库代码）：用 `github.com/minio/minio-go/v7` 上传两个 5MiB+ 对象触发 multipart，partSize 必须是 `5*1024*1024`（minio 最小 part size）。`mp/corrupt.bin` 的首段前 128 字节是 `1000;chunk-signature=...` 头，其余是 filler——分段检查在段 0 offset 0 命中。

## 8. 已知遗留项（改动时留意，非阻塞）

来源：SDD ledger 的 deferred minors（见 `.superpowers/sdd/2026-09-03-chunked-check-tool/progress.md`）。

- **Queue.Pop 阻塞路径的 lost-signal 窗口**：fast-path unlock 与 re-lock+Wait 之间存在理论上的丢失唤醒窗口。当前 pipeline 关闭队列时用 `Broadcast`，不会触发。若未来改为单消费者关闭场景需重新评估。
- **NodePool.Assign 空端点 panic**：`n=0` 时除零。`Config` 上游校验 `endpoints` 非空，故不会触发。无 `idx` 越界保护（调用方只用 `Assign` 返回的索引）。
- **NewOutput 部分初始化失败的 goroutine/file 泄漏**：brief 继承的设计，仅 `os.OpenFile` 失败时触发（罕见启动期磁盘错误）。修复需偏离 brief。
- **bufio flush 错误静默丢弃**：writer goroutine 不检查 `Flush` 错误，无 Write/Close race guard（标准使用契约）。
- **Core.ListObjectsV2 无 ctx 参数**：minio-go 限制， cancellation 在更高层（放弃 goroutine on `ctx.Done`），靠 socket 超时兜底。
- **Checker.Handle 用 `context.Background()`**：非父 ctx（brief 逐字；改签名会级联到 Task 8）。
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
