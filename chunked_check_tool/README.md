# chunked_check_tool

并发列举并校验 S3 对象，识别因 `aws-chunked` 编码未被正确解析而产生的损坏对象。

## 作用

处理未正确解析 `X-Amz-Content-Sha256` header，把 `aws-chunked` PUT 请求的 `length;chunk-signature=…` / `x-amz-checksum-**` / `x-amz-trailer-signature` 等格式化内容当作原始 body 写入存储。本工具遍历整个桶，把损坏的普通对象、损坏的多段对象、列举失败、校验失败分别写入不同文件。

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

## 运行命令

```bash
./chunked_check_tool -c config.yaml -bkt mybucket
./chunked_check_tool -c config.yaml -bkt mybucket -prefix data/2026/
./chunked_check_tool -c config.yaml -bkt mybucket -prefix data/2026/ -nextmarker data/2026/file_005
./chunked_check_tool -c config.yaml -bkt mybucket -check-file retry.txt   # 重查失败对象（check_failed/mp_check_failed/mismatch 的统一重试入口）
./chunked_check_tool -c config.yaml -bkt mybucket -backup-file list.txt  # 损坏对象备份，不校验直接中转（见 ## backup-file 模式）
./chunked_check_tool -c config.yaml -bkt mybucket -resume-list <list_failed.txt>  # 从上次列举失败的断点续跑（仅 Mode 2）
```


## 命令参数

| flag | 必填 | 说明                                                               |
|---|---|--------------------------------------------------------------------|
| `-c` | 是 | 配置文件路径                                                       |
| `-bkt` | 是 | 桶名                                                               |
| `-prefix` | 否 | 列举前缀，默认空（整个桶）                                         |
| `-nextmarker` | 否 | start-after key，跳过该 key 之前的对象；**仅 list_type 为1时生效** |
| `-check-file` | 否 | 重查文件路径；mixed 格式（`bkt\x1fkey` 普通行 / `bkt\x1fkey\x1fpartcnt\x1foff...` 多段行），即 check_failed.txt + mp_check_failed.txt + mismatch.txt 的统一重试载体；需 `is_check=true`（见 `## check-file 模式`） |
| `-backup-file` | 否 | 备份列表文件路径（mixed 格式）；HEAD 判型 + 中转 + ETag 终验，**不探测损坏**；需配置 `backup_bucket`（见 `## backup-file 模式`） |
| `-resume-list` | 否 | 断点续跑文件路径（通常是上一轮的 `list_failed.txt`）；与其他文件 flag 互斥，仅 `list_type=2` 生效。每行格式 `prefix\x1ftoken`（第一页失败时为单字段 `prefix`），工具按 `(prefix, token)` 重新 seed 进 BFS 队列，从失败页的 continuation token 续页，避免重复枚举已成功的前几页 |

> **行分隔符**：工具内所有 `bkt|key|partcnt|off...` 形式的行格式都改用 ASCII Unit Separator `\x1f`（`<US>`）作字段分隔——S3 key 可合法包含 `|`，旧 pipe 分隔会把含 `|` 的 key 静默错切。Server 字段（key/prefix/token）写入文件时按 `url.QueryEscape` 转义，读取侧 `QueryUnescape`，含 `\x1f`、`%`、`+`、`/` 的 key 都能逐字节往返。owner 非空时为 `[a-z0-9]+`，不编不解。

## 配置

`config.yaml` 示例：

```yaml
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http                        # http 或 https（后者忽略证书校验）
ak: <access-key>
sk: <secret-key>
list_type: 2                        # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter
list_api_version: 1                 # 1=ListObjects V1 (marker 分页, 默认), 2=ListObjectsV2 (continuation token)
list_concurrency: 8                 # 列举并发度
check_concurrency: 16               # 检查并发度
output_dir: ./out                   # 输出目录
output_dir_timestamp: true         # true=output_dir 追加 _YYYYMMDD_HHMMSS 后缀（./out → ./out_20260908_175201）隔离每次运行; false=固定用 output_dir
is_check: true                      # true=列举+校验, false=仅列举
is_success_log: false               # 是否记录正常普通对象到 <ownerID>/ok_objects.txt
max_keys: 1000                       # 每次 LIST 请求的最大键数（默认 1000，范围 1..1000）
encoding_type_url: true             # true=LIST 请求带 EncodingType=url，wire 上 server 字段 URL 编码（保护 XML 反序列化免受 \x1f 等非法控制字符撕裂）
debug_log: false                     # true=每次 LIST 请求打 req/resp/err 诊断行到 run.log（Debug 级）
latency_window_sec: 60               # 进度行 latency_avg_ms/latency_max_ms 的滑动窗口秒数（默认 60，负值报错；窗口越短越能反映当前批次健康）
multipart_check_mode: 0             # 多段检查模式：0=关闭 1=offset 检查
is_multipart_success_log: false     # 是否记录干净的多段对象到 <ownerID>/ok_mp.txt
node_isolate_threshold: 3           # 节点隔离阈值（累积节点故障数达到才隔离；1=旧即时隔离）
node_recover_probe_interval: 60     # 隔离节点恢复探测间隔秒数（连续 2 次健康恢复；0=禁用）
dial_timeout: 10                    # 单次 TCP 建连超时秒数（黑洞 IP 默认 10s 兜底；0=Go 默认无超时）
response_header_timeout: 0           # 等服务端响应头超时秒数（0=Go 默认无超时，不兜底）
journal_checkpoint_interval_sec: 600  # journal 快照间隔秒数（list_type=2 扫描进度每隔这么久写一次 journal.txt；默认 600，负值报错）
progress_interval: 5000             # 进度记录间隔
obj_ch_capacity: 0                  # lister→checker channel 容量；0=max(check_concurrency*4, 2000)
output_ch_capacity: 0               # output writer channel 容量；0=1024
backup_bucket: backup-target       # 备份目标桶（-backup-file 模式必填）
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | S3 节点 ip:port 列表，至少 1 个 |
| `scheme` | `http` | `https` 时跳过 TLS 证书校验 |
| `ak` / `sk` | 必填 | 访问凭证（SigV4 静态凭证） |
| `list_type` | `2` | 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter |
| `list_api_version` | `1` | 1=ListObjects V1（marker 分页），2=ListObjectsV2（continuation token） |
| `list_concurrency` | `8` | 列举 worker 数 |
| `check_concurrency` | `16` | 校验 worker 数 |
| `output_dir` | `.` | 输出目录（自动创建） |
| `output_dir_timestamp` | `true` | `true` 时在目录名后追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`），每次运行互不混写；`false` 时固定使用 `output_dir` 原样路径（断点续跑请显式设 `false`，否则每次重启进入新目录）。同时作用于 `backup_output_dir` |
| `backup_output_dir` | （无） | `-backup-file` 模式专用输出目录，与 `output_dir` 分离避免 backup 结果与 list/check 结果混写。仅 backup 模式必填，其他模式忽略；同样受 `output_dir_timestamp` 控制并共享同一时间戳成对生成 |
| `is_check` | `true` | `false` 时只列举不校验，不写对象文件，仅写 `list_failed.*` |
| `is_success_log` | `false` | 主扫描：`true` 时把正常普通对象 key 写入 `<ownerID>/ok_objects.txt`。`-check-file` 模式恒记录，不受此项控制 |
| `max_keys` | `1000` | 每次 LIST 请求的最大键数（不配时默认 1000；配了须在 1..1000 内，否则启动报错）。服务端单页响应上限决定分页粒度 |
| `encoding_type_url` | `false` | `true` 时 LIST 请求带 `EncodingType=url`，wire 上 server 字段（key/prefix/marker）URL 编码。保护 XML 反序列化免受 `\x1f` 等 XML 1.0 非法控制字符撕裂；wire 出口由工具统一 `QueryUnescape` 解码。V1 NextMarker 出口解码回原始 key 形态回传（probe-verified minio 语义）；V2 NextContinuationToken 是不透明 base64，**不**解码（字面 `+` 必须原样往返） |
| `debug_log` | `false` | `true` 时每次 LIST 请求打三行 Debug 级诊断（req/resp/err）到 run.log，记录 marker/token/req_id/next/truncated/nContents/nCP 等，便于追 marker 游标漂移与短响应 quirk |
| `latency_window_sec` | `60` | 进度行 `latency_avg_ms`/`latency_max_ms` 的滑动窗口秒数：每窗口重置，反映**当前批次**的 LIST 延迟健康（全局平均会被早期 spike 稀释、掩盖当前批次问题）。负值启动报错 |
| `multipart_check_mode` | `0` | 多段对象损坏检查模式：`0`=关闭（全部写 `mp.txt` 不检查）；`1`=offset 检查（LIST 带 `internal-list-mp-offset: true` header，服务端返回 `<md5>-<partcnt>-<off0>|<off1>|...` 格式 ETag（注：ETag 内部分隔符仍是 `|`，由服务端生成），按真实 part 边界逐段检查；解析不出 offsets 的对象回落 `list_parse_failed.txt`，配套 `list_parse_failed.log` 记录完整 ETag） |
| `whole_object_probe_threshold` | `1024` | 小对象全读阈值（字节）。`0 < Size <= 该值` 时走快路径：单次 RangeGet 读全对象，1 请求覆盖段首+段尾两种损坏；超过该值走多探测：普通对象先读 head@0，head 以 chunk 长度行 `<hexlen>\r\n` 开头才再读 tail@Size-128（否则 1 请求即判干净）；多段每 part offset 一次 `[off_i, off_i+128]` 探测（N 请求，无 tail probe）。普通对象两种路径是同一条规则：开头是签名头，或者以 chunk 长度行开头且尾部有 trailer 标记，才算损坏。`0` 禁用快路径恒走多探测；负值启动报错 |
| `is_multipart_success_log` | `false` | 主扫描：`true` 时把干净的多段对象 key 写入 `<ownerID>/ok_mp.txt`。`-check-file` 模式恒记录，不受此项控制 |
| `node_isolate_threshold` | `3` | 节点隔离阈值：进程级累积节点故障数（连接错误/超时/5xx，4xx 不计）达到才隔离节点，跨 worker 共享、无时间衰减；`1` 恢复旧的首次故障即隔离；故障后的重试一律换节点（仅剩单节点时同节点重试），真死节点不丢工作项 |
| `node_recover_probe_interval` | `60` | 隔离节点恢复探测间隔（秒）：后台每轮 HEAD bucket，连续 2 次健康应答 → 恢复进轮询池并清零故障计数；`0` 禁用恢复（隔离进程内永久） |
| `dial_timeout` | `10` | 单次 TCP 建连超时（秒）：建连阶段（SYN→SYN-ACK→ACK）上限。黑洞 IP（不路由、不回 RST）场景 OS 默认 SYN 重试要 ~75s（macOS）/ ~127s（Linux），默认 10s 兜底；`0` = Go net/http 默认（无超时，走 OS 默认）。负值启动报错。只控制建连阶段，不影响已建立连接上的请求/响应传输 |
| `response_header_timeout` | `0` | 等服务端响应头超时（秒）：请求发出后等 HTTP 响应头（status line + headers）的上限，不含响应体传输。服务端 accept 了 TCP 但进程挂死不响应时，这是唯一能检测的兜底。默认 `0` 不兜底（Go net/http 默认 = 无超时，永远等），用户遇到挂死服务端时显式配。负值启动报错 |
| `journal_checkpoint_interval_sec` | `600` | journal 快照间隔（秒）：`list_type=2` 扫描每隔该值把扫描进度写一次 `journal.txt`，被 kill -9 后最多重做一个间隔的进度。调小更安全，写盘更频繁；负值启动报错 |
| `progress_interval` | `5000` | stdout 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；0 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；0 走默认 |
| `backup_bucket` | 空 | 备份目标桶名；`-backup-file` 模式必填，其他模式忽略 |

## check-file 模式

`-check-file <path>` 是 check 失败对象的统一重试入口（也是任意外部清单的检查入口）。输入是 **mixed 格式**（与 `-backup-file` 完全一致，按字段数自描述，字段分隔符 `\x1f`）：

    bkt\x1fkey                          # 普通对象（check_failed.txt 的行格式）
    bkt\x1fkey\x1fpartcnt\x1foffset0\x1foffset1\x1f...  # 多段对象（mp_check_failed.txt / corrupted_mp.txt 的行格式）

典型输入 = 上一轮（或多轮）运行产出的 `check_failed.txt` + `mp_check_failed.txt` + `mismatch.txt` 的并集。

行为 = **重新检查对象当前状态**：

- 每行先 HEAD 填充 ETag/Size；**HEAD ETag 是权威判型**（输入行描述的是上一轮检查时的对象，期间可能被覆盖）：
  - HEAD 普通 → 按 head+tail 探测（行内 offsets 忽略）→ `corrupted_objects.txt` / `ok_objects.txt` / `check_failed.txt`
  - HEAD 多段 + 行有 offsets → 每 part offset 一次探测 → `corrupted_mp.txt`（带 offsets）/ `ok_mp.txt`（带 offsets）/ `mp_check_failed.txt`
  - HEAD 多段 + 行无 offsets（普通行漂移成多段）→ 写 `mp.txt` 不认干净（无边界无法逐段验证）
- HEAD 失败按**行声明的类型**分流：普通行 → `check_failed.txt`（`bkt\x1fkey`）；多段行 → `mp_check_failed.txt`（保留 offsets）——两个文件都可再喂回本模式循环重试
- 无 S3 LIST，无需配 `multipart_check_mode`（offsets 来自输入行）；结果文件落在 `_unknown/` 子目录（行内无 owner）
- **成功和失败都记录**：`ok_objects.txt` / `ok_mp.txt` 恒写，不受 `is_success_log` / `is_multipart_success_log` 控制（两个开关只管主扫描），与失败文件、备份模式一致
- **输入文件读不出来时，进程以退出码 1 结束**：文件打不开，或读取中途出错（例如某一行超过 1 MiB 的长度上限）时，已读入的行照常处理、汇总照常打印，然后报错退出，错误信息带文件路径和出错的行号。坏行不属于这种情况：坏行写 `parse_failed.txt` 后继续
- 进度行 `[progress] read=X ok_obj=… corrupt_obj=… ok_mp=… corrupt_mp=… check_failed=… mp_check_failed=… get_calls=… get_avg_ms=… (checked=N) q=obj:…`；汇总 `read: X` + `list_failed`（仅坏行）+ `get_calls` + `ok_obj/corrupt_obj/ok_mp/corrupt_mp/check_failed/mp_check_failed`

**收敛重试**：本模式自己也会产出更小的 `check_failed.txt` / `mp_check_failed.txt`（瞬时故障恢复后重试即可清零），循环喂回直到为空。对象在检查后被改写导致的 `mismatch.txt`（backup 模式产出）同样喂本模式重查。判断一轮是否做完，除了看失败文件，还要看退出码是 0：退出码 1 表示那一轮的输入没读完，失败文件为空也不算收敛。

**中断恢复**：本模式没有快照，中断后重跑同一输入文件即可（检查幂等）。输入很大时用差集只重跑没有定论的对象，见下方「kill -9 后的续跑」。

## backup-file 模式

`-backup-file <path>` 把输入列表里的对象**原样中转**到 `backup_bucket`（客户端下载再上传，并做 ETag 终验）。输入文件每行格式（两种可混排，与 `-check-file` 相同，字段分隔符 `\x1f`）：

    bkt\x1fkey                          # 普通对象（即上一轮 <ownerID>/corrupted_objects.txt 的行）
    bkt\x1fkey\x1fpartcnt\x1foffset0\x1foffset1\x1f...  # 多段对象（corrupted_mp.txt 的行，自带真实 part 边界）

**本模式不做损坏探测**：输入列表是检查阶段的产物（`corrupted_objects.txt` + `corrupted_mp.txt` 跨轮并集），对象是否损坏已在检查阶段判定，备份的职责只是保字节。普通行与多段行一视同仁直接中转。

流程：

1. 启动时先把输入列表文件**流式**上传到 `backup_bucket` 的 `.backup_lists/<原名>_<YYYYMMDD_HHMMSS>.txt`（磁盘直读、不进内存——损坏清单可能有数 GB；失败则中止，不处理任何对象）
2. 逐对象 HEAD，以 ETag 判型（32 位小写 hex = 普通，其余 = 多段）
3. 输入类型校验（mismatch）：输入行声明的对象类型（2 字段=普通，带 partcnt=多段）与 HEAD 判型不一致（对象在检查后被改写）→ 原始行写入 `mismatch.txt`，结构化诊断写入 `mismatch.log`，跳过该对象。**mismatch 行可喂 `-check-file` 重查当前状态**。这是对输入列表的校验，与第 6 步 ETag 终验失败（`backup_failed.txt`，stage=etag）是两回事
4. 中转 = 客户端下载再上传（非服务端 copy）：
   - 普通对象：整对象流式 GET → 单次 PUT，流式转发不缓冲
   - 多段对象：按行内 offsets 分段，第 i 段 = `[offset_i, offset_{i+1})`，末段到对象末尾；逐段流式 GET → UploadPart，最后 CompleteMultipartUpload
   - 目标 key 与源 key 相同，直接放 `backup_bucket` 根
5. ETag 终验：中转完成后比对目标 ETag 与 HEAD 源 ETag，不一致 → `backup_failed.txt`（坏副本保留在目标桶留证据）。多段对象按原始段边界重新分段上传，字节保真时目标 ETag 与源完全相同（`MD5(各段MD5)-N`），任一段损坏/截断都会被检出

约束与说明：

- 四个文件 flag（`-check-file`/`-backup-file`/`-resume-list`/`-resume-journal`）互斥；配置必须含非空 `backup_bucket` 与 `backup_output_dir`
- 三个备份结果文件（`backup_ok.txt`/`backup_failed.txt`/`mismatch.txt`）均写入**原始输入行**（`bkt\x1fkey` 或 `bkt\x1fkey\x1fpartcnt\x1foffset…`），与输入文件同构：`backup_failed.txt` 可直接作为 `-backup-file` 输入重试失败对象（行内自带 offsets）；`.log` 文件保持结构化（key + 错误详情）
- HEAD/中转失败 → 原始输入行写入 `backup_failed.txt`，`backup_failed.log` 记录失败阶段（head/upload/etag）与错误；多段中转失败会 AbortMultipartUpload 清理未完成分片
- 坏行（格式错误/bkt 不匹配）与文件模式一致：写入 `parse_failed.txt` 并跳过
- **输入文件读不出来时，进程以退出码 1 结束**：读取中途出错（例如某一行超过 1 MiB）时，已读入的行照常备份、汇总照常打印，然后报错退出，错误信息带行号。文件不存在时更早失败，因为归档上传那一步会先打开它
- 复用 `check_concurrency` 作为备份 worker 数；节点故障轮询仅覆盖请求发起阶段——流式中转一旦开始，中途故障不重试（流不可重放），整对象记为失败
- S3 多段约束：非末段必须 ≥5MB。输入行语义是原始 part 边界（原上传本来合规）；喂入非末段小于 5MB 的行会被 S3 拒绝（EntityTooSmall）→ `backup_failed`
- `mp.txt`（mode=0 未验证多段、`-check-file` 时普通行 HEAD 判多段的漂移对象）**无 offsets，无法自动中转**（ETag 终验对不上），需人工补 part 边界后走 `-check-file`/`-backup-file`。mode=1 ETag 解析失败的对象落到根目录 `list_parse_failed.txt`（同样无 offsets，无法自动中转）
- 观测指标（无 S3 LIST，不显示 `list_*`）：进度行 `[progress] read=X list_failed=… backup_ok=… backup_failed=… backup_mismatch=… get_calls=… get_avg_ms=… (backed=N) q=obj:… lf:… bok:… bfail:… mm:…`；汇总 `read: X` + `list_failed` + `get_calls` + `backup_ok: N backup_failed: N backup_mismatch: N`
- **中断恢复**：进程不拦截信号，Ctrl-C / kill 都是立即退出；重跑同一输入文件即可（探测/中转幂等，重复中转 = 覆盖写）。输入很大时用差集只重跑没备份成功的对象，见下方「kill -9 后的续跑」

### kill -9 后的续跑（`-check-file` / `-backup-file`）

这两个模式没有快照，也不记录输入读到哪一行。进程被 `kill -9` 或 OOM 杀掉后，用"原输入减去已有定论的对象"得到重跑输入。规则对两个模式统一：

- **按 key 过滤**：只比每行的第 2 个字段（同一个桶内，key 唯一确定对象）。
- **只有有定论的结果算完成**，其余的行留在重跑输入里：

| 模式 | 算完成 | 不算完成，留在重跑输入里 |
|---|---|---|
| `-backup-file` | `backup_ok.txt` | `backup_failed.txt`、`mismatch.txt`、`parse_failed.txt` |
| `-check-file` | `_unknown/` 下的 `ok_objects.txt`、`ok_mp.txt`、`corrupted_objects.txt`、`corrupted_mp.txt` | `mp.txt`、`check_failed.txt`、`mp_check_failed.txt`、`parse_failed.txt` |

```bash
# -backup-file
D=<被杀那一轮的输出目录>              # backup_output_dir（默认带 <时间戳> 后缀）
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
# ./chunked_check_tool -c config.yaml -bkt <bucket> -backup-file retry.txt
```

```bash
# -check-file
D=<被杀那一轮的输出目录>              # output_dir（默认带 <时间戳> 后缀）
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
# ./chunked_check_tool -c config.yaml -bkt <bucket> -check-file retry.txt
```

几点说明：

- **为什么按 key 而不按整行**：`-check-file` 的结果行是按字段重新拼的。对象在两轮之间被改写成普通对象后，结果行没有分段边界，按整行比永远对不上。
- **`mismatch.txt` 和 `mp.txt` 为什么不算完成**：它们表示"这一行处理不了"，不是定论。同一个 key 可能在输入里有两行（一行写普通对象、一行写多段对象）；算完成会跳过另一行，对象就得不到处理。
- `LC_ALL=C` 必须设。被杀那一轮的结果文件末行可能只写了一半，`drop_torn` 会去掉它，这个对象因此留在重跑输入里。
- 速度：全程只用外部排序和顺序关联，内存不随输入增长。macOS 上实测 500 万行（180 MB）约 4 秒，是整行比的 1.7 倍。
- 详细原因和验证记录见 `AGENTS.md` §7.4。

## 输出

全部以 append 模式打开。目录结构（`output_dir_timestamp=true`（默认）时目录名为 `<output_dir>_<YYYYMMDD_HHMMSS>`，如 `./out` → `./out_20260908_175201`；`false` 时即 `output_dir` 本身）：

```
<output_dir>/
├── run.log                     # 进程运行日志
├── list_failed.txt             # 列举失败 `prefix\x1ftoken`；token 是失败页的 continuationToken（第一页失败/未启动时为单字段 prefix）。未配 `-prefix` 时根 prefix 失败写空行（合法条目，可喂 `-resume-list` 续跑）
├── list_failed.log             # 列举失败结构化错误
├── parse_failed.txt            # -check-file / -backup-file / -resume-list 输入解析失败的原始行（坏行、bkt 不匹配、whitespace-only 行、无效 %XX 转义）；不可自动续跑，需人工修输入文件
├── check_failed.txt            # 普通对象 RangeGet 失败 `bkt\x1fkey`（可直喂 -check-file / -backup-file 普通对象输入）
├── check_failed.log            # 普通对象 RangeGet 失败结构化错误
├── mp_check_failed.txt         # 多段分段 RangeGet 失败 `bkt\x1fkey\x1fpartcnt\x1foff0\x1f...`（可直喂 -check-file / -backup-file 多段输入）
├── mp_check_failed.log         # 多段分段 RangeGet 失败结构化错误
├── list_parse_failed.txt       # mode=offset 多段 ETag 解析失败 `bkt\x1fkey`（无 offsets，无法自动中转；喂回 -check-file 只会落到 mp.txt，要检查需人工补 part 边界）
├── list_parse_failed.log       # mode=offset 多段 ETag 解析失败结构化诊断（key/owner/size/原始 ETag，不截断）
├── backup_ok.txt               # 备份成功的原始输入行（-backup-file 模式）
├── backup_failed.txt           # 备份失败的原始输入行（-backup-file 模式；可自喂重试）
├── backup_failed.log           # 备份失败结构化错误（stage=head/upload/etag）
├── mismatch.txt                # 输入类型校验失败：输入行声明的对象类型与 HEAD 判型不一致的原始行（-backup-file 模式；可直喂 -check-file 重查）
├── mismatch.log                # 输入类型校验失败的结构化诊断：line_is_multipart/head_etag/head_size/reason（-backup-file 模式）
└── <ownerID>/                  # OwnerID 为空（含全部文件输入模式）时落到 _unknown/
    ├── corrupted_objects.txt   # 损坏普通对象 key（Range GET 命中 chunk-signature/trailer）
    ├── ok_objects.txt          # 正常普通对象 key（主扫描 is_success_log=true 时；-check-file 恒写）
    ├── mp.txt                  # 多段对象 key（multipart_check_mode=0 时全部多段；-check-file 时普通行 HEAD 判多段的漂移对象）
    ├── corrupted_mp.txt        # 损坏多段对象 key（分段检查命中）
    └── ok_mp.txt               # 干净多段对象（主扫描 is_multipart_success_log=true 时；-check-file 恒写）；行格式同 corrupted_mp.txt：multipart_check_mode=1 与 -check-file 带 partcnt+offsets
```

> **行分隔与转义**：所有 `bkt|key|partcnt|off...` 形式的行格式实际用 ASCII Unit Separator `\x1f`（`<US>`）作字段分隔——S3 key 可合法包含 `|`，旧 pipe 分隔会静默错切。Server 字段（key/prefix/token）写入文件时 `url.QueryEscape` 转义，读取侧 `QueryUnescape` 还原；含 `\x1f`、`%`、`+`、`/` 的 key 都能逐字节往返。bucket 字段来自 CLI，不转义；partcnt 与 offsets 是纯数字，不转义；owner 非空时为 `[a-z0-9]+`，不转义。

### 失败分类与补跑链路

工具把"失败"分成几类，分别落不同文件，**每个失败文件有且仅有一个补跑入口**：

| 文件 | 触发场景 | 是否可自动补跑 | 补跑方式 |
|---|---|---|---|
| `list_failed.txt` | S3 LIST 调用失败（节点故障/5xx/超时） | 是 | `-resume-list <list_failed.txt>`（Mode 2 BFS，从失败页 token 续页） |
| `parse_failed.txt` | 输入文件解析失败（坏行/bkt 不匹配/whitespace-only 行/无效 %XX 转义） | 否 | 人工修输入文件后重跑 |
| `check_failed.txt` | 普通对象 RangeGet 失败（状态未知） | 是 | `-check-file check_failed.txt` 重查；确认损坏后再 `-backup-file` |
| `mp_check_failed.txt` | 多段对象分段 RangeGet 失败（状态未知） | 是 | `-check-file mp_check_failed.txt` 重查 |
| `mismatch.txt` | 对象在检查后被改写（类型漂移），检查结果过期 | 是 | `-check-file mismatch.txt` 重查当前状态 |
| `backup_failed.txt` | 备份中转失败 | 是 | `-backup-file backup_failed.txt` 自喂重试 |
| `list_parse_failed.txt` | mode=offset 多段 ETag 解析失败（服务端未返回 `internal-list-mp-offset` 或 ETag 格式不合法） | 否 | 人工补 part 边界后走 `-check-file`/`-backup-file`（直喂 `-check-file` 只会落到 `mp.txt`：HEAD 不带分段边界，不会真正检查） |
| `mp.txt` | 未验证多段（mode=0、`-check-file` 时普通行 HEAD 判多段的漂移对象） | 否 | 人工补 part 边界后走 `-check-file`/`-backup-file` |

`list_failed.txt` 行格式 `prefix\x1ftoken`：token 是失败页的 continuationToken（V1 是上一页最后一个 key，V2 是服务端返回的不透明 token）。第一页就失败时 token 为空，整行就是单字段 `prefix`。补跑时 `-resume-list` 读这些行，把 `(prefix, token)` 作为初始状态 seed 进 BFS 队列，`processPrefix` 从该 token 续页，避免重复枚举已成功的前几页。**仅 Mode 2 支持**——Mode 1 的根/子前缀游标语义不可区分，其列举失败重试 = 原命令重跑（幂等，跨轮并集吸收重复）。

### 停止与断点续传

工具不拦截任何信号。Ctrl-C、kill、`kill -9` 效果一样：进程立即退出，不执行收尾代码——不打印汇总，结果文件可能缺最后 3 秒的行，末行可能是半行。

被杀之后按运行模式续跑：

- `list_type=2` 扫描：`-resume-journal <被杀那一轮的目录>/journal.txt`，最多重做一个快照间隔的进度（默认 10 分钟，见 `journal_checkpoint_interval_sec`）
- `-check-file` / `-backup-file`：重跑原输入文件，或用结果文件求差集只跑没有定论的对象（见上文「kill -9 后的续跑」）
- `list_type=1`：只能用相同参数重跑全量扫描（探测幂等、输出 append-only 并集语义容忍重复）

**跨轮合并**：`output_dir_timestamp` 默认 true，每轮（含每轮补跑）独立目录；逻辑结果 = 跨目录并集，收敛判据 = 最新一轮失败文件为空。最终备份输入：

```bash
cat out_*/corrupted_objects.txt out_*/corrupted_mp.txt | sort -u > backup_input.txt
```

完整运维流程（四阶段）：

```bash
# Stage 1 全量扫描
./chunked_check_tool -c config.yaml -bkt B [-prefix P]                 → out_<t1>/
# Stage 2 补齐列举（循环至 list_failed 为空）
./chunked_check_tool -c config.yaml -bkt B -resume-list out_<tN>/list_failed.txt
# Stage 3 补齐检查（循环至失败文件为空）
cat out_*/{check_failed,mp_check_failed,mismatch}.txt | sort -u > retry.txt
./chunked_check_tool -c config.yaml -bkt B -check-file retry.txt
# Stage 4 备份
cat out_*/corrupted_objects.txt out_*/corrupted_mp.txt | sort -u > backup_in.txt
./chunked_check_tool -c config.yaml -bkt B -backup-file backup_in.txt   → backup_out_<t>/
```

**stats 注意**：补跑（`-resume-list` 或 append 模式重跑）会让 `list_obj`/`list_mp`/`ok_obj`/`corrupt_mp` 等 atomic 计数器累加重复对象——summary 数字会翻倍，结果文件会有重复行。这是已知行为：append 模式不去重，使用者需自行知晓。`corrupted_mp.txt` 单文件内字节级重复行可用 `sort -u` 去重（同一对象同 offsets 字节级相同）。