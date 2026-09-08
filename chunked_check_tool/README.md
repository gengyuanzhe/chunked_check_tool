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
./chunked_check_tool -c config.yaml -bkt mybucket -list-file list.txt   # 跳过 S3 列举，按行校验
./chunked_check_tool -c config.yaml -bkt mybucket -backup-file list.txt # 损坏对象备份（见 ## backup-file 模式）
```


## 命令参数

| flag | 必填 | 说明                                                               |
|---|---|--------------------------------------------------------------------|
| `-c` | 是 | 配置文件路径                                                       |
| `-bkt` | 是 | 桶名                                                               |
| `-prefix` | 否 | 列举前缀，默认空（整个桶）                                         |
| `-nextmarker` | 否 | start-after key，跳过该 key 之前的对象；**仅 list_type 为1时生效** |
| `-list-file` | 否 | 列表文件路径；设置后跳过 S3 列举，直接读文件按行校验（见 `## list-file 模式`） |
| `-backup-file` | 否 | 备份列表文件路径；与 `-list-file` 互斥，需配置 `backup_bucket`（见 `## backup-file 模式`） |

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
is_multipart_segment_check: false   # 是否按固定 part size 对多段对象做分段损坏检查
multipart_segment_size: 0           # 多段分段检查的段长度(字节)，需与上传 part size 一致
is_multipart_success_log: false     # 是否记录干净的多段对象到 <ownerID>/ok_mp.txt
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
| `output_dir_timestamp` | `true` | `true` 时在目录名后追加 `<YYYYMMDD_HHMMSS>` 后缀（`./out` → `./out_20260908_175201`），每次运行互不混写；`false` 时固定使用 `output_dir` 原样路径（断点续跑请显式设 `false`，否则每次重启进入新目录） |
| `is_check` | `true` | `false` 时只列举不校验，不写对象文件，仅写 `list_failed.*` |
| `is_success_log` | `false` | `true` 时把正常普通对象 key 写入 `<ownerID>/ok_objects.txt` |
| `is_multipart_segment_check` | `false` | `true` 时按固定 part size（`multipart_segment_size`）对多段对象做分段损坏检查；`true` 时必须配 `multipart_segment_size > 0`，否则启动报错 |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `is_multipart_success_log` | `false` | `true` 时把干净的多段对象 key 写入 `<ownerID>/ok_mp.txt` |
| `progress_interval` | `5000` | stdout 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；0 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；0 走默认 |
| `result_line_format` | `<bucket>\|<key>` | 结果文件每行格式，支持 `<bucket>`/`<key>`/`<owner>` 占位符；只影响 per-owner 结果文件，处理文件始终只存 key/prefix |
| `backup_bucket` | 空 | 备份目标桶名；`-backup-file` 模式必填，其他模式忽略 |

## list-file 模式

`-list-file <path>` 跳过 S3 列举，直接读文件按行校验。每行格式：

    bkt|key|partcnt|offset0|offset1|...

- `bkt` 必须等于 `-bkt`
- `partcnt` 个 offset，`offset0=0`，严格递增
- 坏行（格式错误/bkt 不匹配）写入 `list_failed.txt` 并跳过
- 必须 `is_check=true`（文件即列表，无需列举）
- 此模式下 `list_all` 显示 0（不经过 S3 LIST），`list_failed` 仅统计坏行
- 校验结果正常输出：损坏 → `<ownerID>/corrupted_mp.txt`，GET 失败 → `mp_check_failed.txt/.log`（无需配 `is_multipart_segment_check`）；全部干净 → `ok_mp.txt`（需配 `is_multipart_success_log: true`）。行内无 owner 信息，结果落在 `_unknown/` 子目录

固定分段校验（`is_multipart_segment_check=true` + `multipart_segment_size`）是本模式的特殊情况：offsets 由 `[0, seg, 2*seg, ...]` 计算而来，本模式则显式给出。

## backup-file 模式

`-backup-file <path>` 对损坏对象做备份（下载中转再上传到 `backup_bucket`，并做 ETag 终验）。输入文件每行格式（两种可混排，按字段数自描述）：

    bkt|key                          # 普通对象（即上一轮 <ownerID>/corrupted_objects.txt 的 err 列表）
    bkt|key|partcnt|offset0|offset1|...  # 多段对象（与 -list-file 同格式）

流程：

1. 启动时先把输入列表文件整体上传到 `backup_bucket` 的 `.backup_lists/<原名>_<YYYYMMDD_HHMMSS>.txt`（失败则中止，不处理任何对象）
2. 逐对象 HEAD，以 ETag 判型（32 位小写 hex = 普通，其余 = 多段）
3. HEAD 类型与行类型不一致 → 原始行写入 `mismatch.txt`，结构化诊断（行声明类型、HEAD ETag/size、原因）写入 `mismatch.log`，跳过
4. 普通行：直接下载中转（输入列表即上一轮校验的损坏结果，不重新探测）
5. 多段行：按行内 offset 逐段 Range GET 探测 chunk-signature，命中损坏才中转；全部干净 → 写入 `backup_skipped_clean.txt`，不中转
6. 中转 = 客户端下载再上传（非服务端 copy）：
   - 普通对象：整对象流式 GET → 单次 PUT，流式转发不缓冲
   - 多段对象：按行内 offsets 分段，第 i 段 = `[offset_i, offset_{i+1})`，末段到对象末尾；逐段流式 GET → UploadPart，最后 CompleteMultipartUpload
   - 目标 key 与源 key 相同，直接放 `backup_bucket` 根
7. ETag 终验：中转完成后比对目标 ETag 与 HEAD 源 ETag，不一致 → `backup_failed.txt`（坏副本保留在目标桶留证据）。多段对象按原始段边界重新分段上传，字节保真时目标 ETag 与源完全相同（`MD5(各段MD5)-N`），任一段损坏/截断都会被检出

约束与说明：

- `-list-file` 与 `-backup-file` 互斥；配置必须含非空 `backup_bucket`
- HEAD/探测/中转失败 → key 写入 `backup_failed.txt`，`backup_failed.log` 记录失败阶段（head/verify/upload/etag）与错误；多段中转失败会 AbortMultipartUpload 清理未完成分片
- 坏行（格式错误/bkt 不匹配）与 `-list-file` 一致：写入 `list_failed.txt` 并跳过
- 复用 `check_concurrency` 作为备份 worker 数；节点故障轮询仅覆盖请求发起阶段——流式中转一旦开始，中途故障不重试（流不可重放），整对象记为失败
- S3 多段约束：非末段必须 ≥5MB。输入行语义是原始 part 边界（原上传本来合规）；若喂入"固定分段"格式（段 <5MB）会被 S3 拒绝（EntityTooSmall）→ `backup_failed`
- 汇总行：`backup_ok: N backup_failed: N backup_mismatch: N backup_skipped_clean: N`（另含 `get_calls`，多段校验产生）

## 输出

全部以 append 模式打开。目录结构（`output_dir_timestamp=true`（默认）时目录名为 `<output_dir>_<YYYYMMDD_HHMMSS>`，如 `./out` → `./out_20260908_175201`；`false` 时即 `output_dir` 本身）：

```
<output_dir>/
├── run.log                     # 进程运行日志
├── list_failed.txt             # 列举失败 prefix（list worker 调用失败时写）
├── list_failed.log             # 列举失败结构化错误
├── check_failed.txt            # 普通对象 RangeGet 失败 key
├── check_failed.log            # 普通对象 RangeGet 失败结构化错误
├── mp_check_failed.txt         # 多段分段 RangeGet 失败 key（is_multipart_segment_check=true 时）
├── mp_check_failed.log         # 多段分段 RangeGet 失败结构化错误
├── backup_ok.txt               # 备份成功 key（-backup-file 模式）
├── backup_failed.txt           # 备份失败 key（-backup-file 模式）
├── backup_failed.log           # 备份失败结构化错误（stage=head/verify/upload/etag）
├── mismatch.txt                # HEAD 类型与输入行类型不一致的原始行（-backup-file 模式）
├── mismatch.log                # mismatch 结构化诊断：行声明类型、HEAD ETag/size、原因（-backup-file 模式）
├── backup_skipped_clean.txt    # 多段校验全部干净未备份的 key（-backup-file 模式）
└── <ownerID>/                  # OwnerID 为空时落到 _unknown/
    ├── corrupted_objects.txt   # 损坏普通对象 key（Range GET 命中 chunk-signature）
    ├── ok_objects.txt          # 正常普通对象 key（is_success_log=true 时）
    ├── mp.txt                  # 多段对象 key（is_multipart_segment_check=false 时）
    ├── corrupted_mp.txt        # 损坏多段对象 key（分段检查命中 chunk-signature）
    └── ok_mp.txt               # 干净多段对象 key（is_multipart_segment_check=true && is_multipart_success_log=true 时）
```