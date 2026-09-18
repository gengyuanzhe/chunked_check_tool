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
./chunked_check_tool -c config.yaml -bkt mybucket -list-file list.txt    # 旧模式：跳过 S3 列举，按行校验（多段-only 格式）
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
| `-check-file` | 否 | 重查文件路径；mixed 格式（`bkt\|key` 普通行 / `bkt\|key\|partcnt\|off...` 多段行），即 check_failed.txt + mp_check_failed.txt + mismatch.txt 的统一重试载体；需 `is_check=true`（见 `## check-file 模式`） |
| `-list-file` | 否 | 旧模式：多段-only 格式列表文件，跳过 S3 列举按行校验；与 `-check-file` 等四个文件 flag 互斥（见 `## list-file 模式`） |
| `-backup-file` | 否 | 备份列表文件路径（mixed 格式）；HEAD 判型 + 中转 + ETag 终验，**不探测损坏**；需配置 `backup_bucket`（见 `## backup-file 模式`） |
| `-resume-list` | 否 | 断点续跑文件路径（通常是上一轮的 `list_failed.txt`）；与其他文件 flag 互斥，仅 `list_type=2` 生效。每行格式 `prefix\|token`（第一页失败时为单字段 `prefix`），工具按 `(prefix, token)` 重新 seed 进 BFS 队列，从失败页的 continuation token 续页，避免重复枚举已成功的前几页 |

## 配置

`config.yaml` 示例：

```yaml
endpoints:
  - 10.0.0.1:9000
  - 10.0.0.2:9000
scheme: http                        # http 或 https（后者忽略证书校验）
ak: <access-key>
sk: <secret-key>
list_type: 2                        # 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter, 3=递归+信号量
list_api_version: 1                 # 1=ListObjects V1 (marker 分页, 默认), 2=ListObjectsV2 (continuation token)
list_concurrency: 8                 # 列举并发度
check_concurrency: 16               # 检查并发度
output_dir: ./out                   # 输出目录
output_dir_timestamp: true         # true=output_dir 追加 _YYYYMMDD_HHMMSS 后缀（./out → ./out_20260908_175201）隔离每次运行; false=固定用 output_dir
is_check: true                      # true=列举+校验, false=仅列举
is_success_log: false               # 是否记录正常普通对象到 <ownerID>/ok_objects.txt
multipart_check_mode: 0             # 多段检查模式：0=关闭 1=offset 检查 2=固定分段
multipart_segment_size: 0           # 模式 2 的段长度(字节)，需与上传 part size 一致
is_multipart_success_log: false     # 是否记录干净的多段对象到 <ownerID>/ok_mp.txt
node_isolate_threshold: 3           # 节点隔离阈值（累积节点故障数达到才隔离；1=旧即时隔离）
node_recover_probe_interval: 60     # 隔离节点恢复探测间隔秒数（连续 2 次健康恢复；0=禁用）
progress_interval: 5000             # 进度记录间隔
obj_ch_capacity: 0                  # lister→checker channel 容量；0=max(check_concurrency*4, 2000)
output_ch_capacity: 0               # output writer channel 容量；0=1024
result_line_format: <bucket>|<key>  # 结果文件每行格式，支持 <bucket>/<key>/<owner> 占位符
backup_bucket: backup-target       # 备份目标桶（-backup-file 模式必填）
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `endpoints` | 必填 | S3 节点 ip:port 列表，至少 1 个 |
| `scheme` | `http` | `https` 时跳过 TLS 证书校验 |
| `ak` / `sk` | 必填 | 访问凭证（SigV4 静态凭证） |
| `list_type` | `2` | 1=子目录+平铺 nextmarker, 2=递归 BFS delimiter, 3=递归+信号量 |
| `list_api_version` | `1` | 1=ListObjects V1（marker 分页），2=ListObjectsV2（continuation token） |
| `list_concurrency` | `8` | 列举 worker 数（Mode 3 为信号量容量） |
| `check_concurrency` | `16` | 校验 worker 数 |
| `output_dir` | `.` | 输出目录（自动创建） |
| `output_dir_timestamp` | `true` | `true` 时在目录名后追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`），每次运行互不混写；`false` 时固定使用 `output_dir` 原样路径（断点续跑请显式设 `false`，否则每次重启进入新目录）。同时作用于 `backup_output_dir` |
| `backup_output_dir` | （无） | `-backup-file` 模式专用输出目录，与 `output_dir` 分离避免 backup 结果与 list/check 结果混写。仅 backup 模式必填，其他模式忽略；同样受 `output_dir_timestamp` 控制并共享同一时间戳成对生成 |
| `is_check` | `true` | `false` 时只列举不校验，不写对象文件，仅写 `list_failed.*` |
| `is_success_log` | `false` | `true` 时把正常普通对象 key 写入 `<ownerID>/ok_objects.txt` |
| `multipart_check_mode` | `0` | 多段对象损坏检查模式：`0`=关闭（全部写 `mp.txt` 不检查）；`1`=offset 检查（LIST 带 `internal-list-mp-offset: true` header，服务端返回 `<md5>-<partcnt>-<off0>\|<off1>\|...` 格式 ETag，按真实 part 边界逐段检查；解析不出 offsets 的对象回落 `mp.txt`）；`2`=固定分段检查（旧模式，未来废弃；必须配 `multipart_segment_size > 0`） |
| `multipart_segment_size` | `0` | 模式 2 的段长度（字节），需与上传 part size 一致；仅 `multipart_check_mode: 2` 时必填 |
| `whole_object_probe_threshold` | `1024` | 小对象全读阈值（字节）。`0 < Size <= 该值` 时走快路径：单次 RangeGet 读全对象，body 同时匹配 chunk-signature 与 trailer 正则（`x-amz-checksum-(sha256\|crc32\|crc32c\|sha1\|crc64):`），1 请求覆盖段首+段尾两种损坏；超过该值走 head@0+tail@Size-128（普通对象 2 请求）/head@0+(N-1) 边界+tail@Size-128（多段 N+1 请求）多探测。`0` 禁用快路径恒走多探测；负值启动报错 |
| `is_multipart_success_log` | `false` | `true` 时把干净的多段对象 key 写入 `<ownerID>/ok_mp.txt` |
| `node_isolate_threshold` | `3` | 节点隔离阈值：进程级累积节点故障数（连接错误/超时/5xx，4xx 不计）达到才隔离节点，跨 worker 共享、无时间衰减；`1` 恢复旧的首次故障即隔离；故障后的重试一律换节点（仅剩单节点时同节点重试），真死节点不丢工作项 |
| `node_recover_probe_interval` | `60` | 隔离节点恢复探测间隔（秒）：后台每轮 HEAD bucket，连续 2 次健康应答 → 恢复进轮询池并清零故障计数；`0` 禁用恢复（隔离进程内永久） |
| `progress_interval` | `5000` | stdout 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；0 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；0 走默认 |
| `result_line_format` | `<bucket>\|<key>` | 结果文件每行格式，支持 `<bucket>`/`<key>`/`<owner>` 占位符；只影响 per-owner 结果文件，处理文件始终只存 key/prefix |
| `backup_bucket` | 空 | 备份目标桶名；`-backup-file` 模式必填，其他模式忽略 |

## check-file 模式

`-check-file <path>` 是 check 失败对象的统一重试入口（也是任意外部清单的检查入口）。输入是 **mixed 格式**（与 `-backup-file` 完全一致，按字段数自描述）：

    bkt|key                          # 普通对象（check_failed.txt 的行格式）
    bkt|key|partcnt|offset0|offset1|...  # 多段对象（mp_check_failed.txt / corrupted_mp.txt 的行格式）

典型输入 = 上一轮（或多轮）运行产出的 `check_failed.txt` + `mp_check_failed.txt` + `mismatch.txt` 的并集。旧 `-list-file` 的多段-only 文件是合法子集，可直接喂入。

行为 = **重新检查对象当前状态**：

- 每行先 HEAD 填充 ETag/Size；**HEAD ETag 是权威判型**（输入行描述的是上一轮检查时的对象，期间可能被覆盖）：
  - HEAD 普通 → 按 head+tail 探测（行内 offsets 忽略）→ `corrupted_objects.txt` / `ok_objects.txt` / `check_failed.txt`
  - HEAD 多段 + 行有 offsets → head+边界+tail 探测 → `corrupted_mp.txt`（带 offsets）/ `ok_mp.txt` / `mp_check_failed.txt`
  - HEAD 多段 + 行无 offsets（普通行漂移成多段）→ 写 `mp.txt` 不认干净（无边界无法逐段验证）
- HEAD 失败按**行声明的类型**分流：普通行 → `check_failed.txt`（`bkt|key`）；多段行 → `mp_check_failed.txt`（保留 offsets）——两个文件都可再喂回本模式循环重试
- 无 S3 LIST，无需配 `multipart_check_mode`（offsets 来自输入行）；结果文件落在 `_unknown/` 子目录（行内无 owner）
- 进度行 `[progress] read=X ok_obj=… corrupt_obj=… ok_mp=… corrupt_mp=… check_failed=… mp_check_failed=… get_calls=… get_avg_ms=… (checked=N) q=obj:…`；汇总 `read: X` + `list_failed`（仅坏行）+ `get_calls` + `ok_obj/corrupt_obj/ok_mp/corrupt_mp/check_failed/mp_check_failed`

**收敛重试**：本模式自己也会产出更小的 `check_failed.txt` / `mp_check_failed.txt`（瞬时故障恢复后重试即可清零），循环喂回直到为空。对象在检查后被改写导致的 `mismatch.txt`（backup 模式产出）同样喂本模式重查。

## list-file 模式（旧）

`-list-file <path>` 跳过 S3 列举，直接读文件按行校验。每行格式：

    bkt|key|partcnt|offset0|offset1|...

这是历史模式的输入格式：`multipart_check_mode=0/2` 时代多段对象无法在 list+check 内拿到真实 part 边界，需人工补 offset 后用本模式复检。`multipart_check_mode=1`（offset 检查）已让 list+check 自含校验，本模式保留仅为兼容既有流程，**新用法请用 `-check-file`**（mixed 格式，本模式文件是其合法子集）。

- `bkt` 必须等于 `-bkt`；`partcnt` 个 offset，`offset0=0`，严格递增
- 坏行（格式错误/bkt 不匹配）写入 `parse_failed.txt` 并跳过（计数 `parse_failed`）
- 必须 `is_check=true`；无 S3 LIST，进度/汇总用 `read` 替代 `list_*`：`ok_mp/corrupt_mp/mp_check_failed`
- 校验结果与 check-file 模式相同的 HEAD 权威判型规则（行恒声明多段，但 HEAD 判普通时按 head+tail 探测并路由到普通对象结果文件）；结果落在 `_unknown/` 子目录

## backup-file 模式

`-backup-file <path>` 把输入列表里的对象**原样中转**到 `backup_bucket`（客户端下载再上传，并做 ETag 终验）。输入文件每行格式（两种可混排，与 `-check-file` 相同）：

    bkt|key                          # 普通对象（即上一轮 <ownerID>/corrupted_objects.txt 的行）
    bkt|key|partcnt|offset0|offset1|...  # 多段对象（corrupted_mp.txt 的行，自带真实 part 边界）

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

- 四个文件 flag（`-check-file`/`-list-file`/`-backup-file`/`-resume-list`）互斥；配置必须含非空 `backup_bucket` 与 `backup_output_dir`
- 三个备份结果文件（`backup_ok.txt`/`backup_failed.txt`/`mismatch.txt`）均写入**原始输入行**（`bkt|key` 或 `bkt|key|partcnt|offset…`），与输入文件同构：`backup_failed.txt` 可直接作为 `-backup-file` 输入重试失败对象（行内自带 offsets）；`.log` 文件保持结构化（key + 错误详情）
- HEAD/中转失败 → 原始输入行写入 `backup_failed.txt`，`backup_failed.log` 记录失败阶段（head/upload/etag）与错误；多段中转失败会 AbortMultipartUpload 清理未完成分片
- 坏行（格式错误/bkt 不匹配）与文件模式一致：写入 `parse_failed.txt` 并跳过
- 复用 `check_concurrency` 作为备份 worker 数；节点故障轮询仅覆盖请求发起阶段——流式中转一旦开始，中途故障不重试（流不可重放），整对象记为失败
- S3 多段约束：非末段必须 ≥5MB。输入行语义是原始 part 边界（原上传本来合规）；若喂入"固定分段"格式（段 <5MB）会被 S3 拒绝（EntityTooSmall）→ `backup_failed`
- `mp.txt`（mode=0/2 未验证多段、mode=1 ETag 解析失败回落）**无 offsets，无法自动中转**（ETag 终验对不上），需人工补 part 边界后走 `-check-file`/`-backup-file`
- 观测指标（无 S3 LIST，不显示 `list_*`）：进度行 `[progress] read=X list_failed=… backup_ok=… backup_failed=… backup_mismatch=… get_calls=… get_avg_ms=… (backed=N) q=obj:… lf:… bok:… bfail:… mm:…`；汇总 `read: X` + `list_failed` + `get_calls` + `backup_ok: N backup_failed: N backup_mismatch: N`
- **中断恢复**：信号到达时停止读输入、排空已入队对象；重跑同一输入文件即可（探测/中转幂等，重复中转 = 覆盖写）。大输入可用 comm 差集跳过已成功对象（三个输出文件的行都是原始输入行）：
  `comm -23 <(sort input.txt) <(cat out/{backup_ok,backup_failed,mismatch}.txt | sort -u) > retry.txt`

## 输出

全部以 append 模式打开。目录结构（`output_dir_timestamp=true`（默认）时目录名为 `<output_dir>_<YYYYMMDD_HHMMSS>`，如 `./out` → `./out_20260908_175201`；`false` 时即 `output_dir` 本身）：

```
<output_dir>/
├── run.log                     # 进程运行日志
├── list_failed.txt             # 列举失败 `prefix|token`；token 是失败页的 continuationToken（第一页失败/未启动时为单字段 prefix），可喂给 -resume-list 续跑
├── list_failed.log             # 列举失败结构化错误
├── parse_failed.txt            # -check-file / -list-file / -backup-file / -resume-list 输入解析失败的原始行（坏行、bkt 不匹配、空行等）；不可自动续跑，需人工修输入文件
├── invalid_keys.txt            # 违反 `|` 字段分隔契约的 key/prefix（S3 key 含 `|`）；不可被工具处理，需人工修数据或改工具
├── check_failed.txt            # 普通对象 RangeGet 失败 `bkt|key`（可直喂 -check-file / -backup-file 普通对象输入）
├── check_failed.log            # 普通对象 RangeGet 失败结构化错误
├── mp_check_failed.txt         # 多段分段 RangeGet 失败 `bkt|key|partcnt|off0|...`（可直喂 -check-file / -backup-file 多段输入）
├── mp_check_failed.log         # 多段分段 RangeGet 失败结构化错误
├── backup_ok.txt               # 备份成功的原始输入行（-backup-file 模式）
├── backup_failed.txt           # 备份失败的原始输入行（-backup-file 模式；可自喂重试）
├── backup_failed.log           # 备份失败结构化错误（stage=head/upload/etag）
├── mismatch.txt                # 输入类型校验失败：输入行声明的对象类型与 HEAD 判型不一致的原始行（-backup-file 模式；可直喂 -check-file 重查）
├── mismatch.log                # 输入类型校验失败的结构化诊断：line_is_multipart/head_etag/head_size/reason（-backup-file 模式）
└── <ownerID>/                  # OwnerID 为空（含全部文件输入模式）时落到 _unknown/
    ├── corrupted_objects.txt   # 损坏普通对象 key（Range GET 命中 chunk-signature/trailer）
    ├── ok_objects.txt          # 正常普通对象 key（is_success_log=true 时）
    ├── mp.txt                  # 多段对象 key（multipart_check_mode=0 时全部多段；=1 时为 ETag 解析失败的回落；-check-file 时普通行 HEAD 判多段的漂移对象）
    ├── corrupted_mp.txt        # 损坏多段对象 key（分段检查命中）
    └── ok_mp.txt               # 干净多段对象 key（is_multipart_success_log=true 时）
```

### 失败分类与补跑链路

工具把"失败"分成几类，分别落不同文件，**每个失败文件有且仅有一个补跑入口**：

| 文件 | 触发场景 | 是否可自动补跑 | 补跑方式 |
|---|---|---|---|
| `list_failed.txt` | S3 LIST 调用失败（节点故障/5xx/超时/中断未启动） | 是 | `-resume-list <list_failed.txt>`（Mode 2 BFS，从失败页 token 续页） |
| `parse_failed.txt` | 输入文件解析失败（坏行/bkt 不匹配/空行） | 否 | 人工修输入文件后重跑 |
| `invalid_keys.txt` | S3 key/prefix 含 `\|`（违反字段分隔契约） | 否 | 人工修数据或改工具 |
| `check_failed.txt` | 普通对象 RangeGet 失败（状态未知） | 是 | `-check-file check_failed.txt` 重查；确认损坏后再 `-backup-file` |
| `mp_check_failed.txt` | 多段对象分段 RangeGet 失败（状态未知） | 是 | `-check-file mp_check_failed.txt` 重查 |
| `mismatch.txt` | 对象在检查后被改写（类型漂移），检查结果过期 | 是 | `-check-file mismatch.txt` 重查当前状态 |
| `backup_failed.txt` | 备份中转失败 | 是 | `-backup-file backup_failed.txt` 自喂重试 |
| `mp.txt` | 未验证多段（mode=0/2、mode=1 解析失败回落） | 否 | 人工补 part 边界后走 `-check-file`/`-backup-file` |

`list_failed.txt` 行格式 `prefix|token`：token 是失败页的 continuationToken（V1 是上一页最后一个 key，V2 是服务端返回的不透明 token）。第一页就失败或该 prefix 从未启动时 token 为空，整行就是单字段 `prefix`。补跑时 `-resume-list` 读这些行，把 `(prefix, token)` 作为初始状态 seed 进 BFS 队列，`processPrefix` 从该 token 续页，避免重复枚举已成功的前几页。**仅 Mode 2 支持**——Mode 1 的根/子前缀游标语义不可区分、Mode 3 递归无游标，这两类的列举失败重试 = 原命令重跑（幂等，跨轮并集吸收重复）。

### 信号与断点续传（两段式）

**第一次 SIGINT/SIGTERM**：停止列举/读输入，**排空**已入队的检查/中转任务；进行中的前缀把剩余页的游标写入 `list_failed.txt`，队列里未启动的前缀也写入（裸 prefix；`-resume-list` 种子带原 token 精确往返）。效果：**run 目录成为完备检查点**——每个 prefix 要么列完且每个对象都有终态，要么带可续游标；进程以非零退出（`interrupted: graceful drain complete`）。

**第二次信号**：立即硬停——进行中的探测/中转中止（多段上传 Abort 清理），队列里未处理的对象快速记入 `check_failed.txt` / `mp_check_failed.txt` / `backup_failed.txt`（即各自的补跑输入），不静默丢任何对象。

**kill -9 / 崩溃**是唯一有损路径：未处理部分无记录。补救 = 用相同 `-prefix` 重跑全量扫描（探测幂等、输出 append-only 并集语义容忍重复）。

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