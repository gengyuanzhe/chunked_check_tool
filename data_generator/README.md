# data_generator

S3 数据生成工具：按指定目录树结构（扇形链 depth/width/files_per_dir）批量上传对象，对象大小与 multipart part size 随机可配，每上传一个对象本地计算 MD5 写入 `<output_dir>/md5.txt`（格式 `bucket|object|md5hex`）。配套 `chunked_check_tool` 用作损坏检测的数据源。

## 编译

```bash
# 默认（当前平台）
go build -o data_generator .

# Linux 交叉编译
GOOS=linux GOARCH=amd64 go build -o data_generator-linux-amd64 .
```

Go 1.27 路径（若不在 PATH）：`/Users/gengyuanzhe/sdk/go1.27.1/bin/go`

## 命令

```bash
./data_generator -c config.yaml
# 可选 -bkt 覆盖 config 里的 bucket
./data_generator -c config.yaml -bkt otherbucket
```

SIGINT/SIGTERM 触发优雅退出。

## 目录树结构

扇形链：每层 `l*` 桥目录下同层放 `files_per_dir` 个文件 + (width-1) 个叶子目录 `d*`（最深层 width 个）+ 桥嵌套下一层（非最深层）。

```
<prefix>/<lprefix>1/<fprefix>_*..N                                  ← l1 同层文件
<prefix>/<lprefix>1/<dprefix>1/<fprefix>_*..N
<prefix>/<lprefix>1/<dprefix>2/<fprefix>_*..N
<prefix>/<lprefix>1/<dprefix>3/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<fprefix>_*..N                       ← l2 同层文件
<prefix>/<lprefix>1/<lprefix>2/<dprefix>1/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<dprefix>2/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<dprefix>3/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<lprefix>3/<fprefix>_*..N           ← 到达 depth=3
<prefix>/<lprefix>1/<lprefix>2/<lprefix>3/<dprefix>1/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<lprefix>3/<dprefix>2/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<lprefix>3/<dprefix>3/<fprefix>_*..N
<prefix>/<lprefix>1/<lprefix>2/<lprefix>3/<dprefix>4/<fprefix>_*..N
```

总文件数 = `(depth + (width-1)*(depth-1) + width) * files_per_dir`——其中 `depth * files_per_dir` 是每层 `l*` 桥同层文件，`((width-1)*(depth-1) + width) * files_per_dir` 是叶子目录文件。

`lprefix`/`dprefix`/`fprefix` 默认为 `l`/`d`/`file_`，可自定义（例：`lprefix=layer, dprefix=dir, fprefix=obj_` → `layer1/dir1/obj_1`）。

## 配置

`config.yaml` 字段（全部必填无默认，除下列可选项）：

| 字段 | 必填 | 说明 |
|---|---|---|
| `endpoints` | ✓ | ip:port 列表，至少 1 个 |
| `scheme` | | `http`（默认）或 `https`（后者跳过证书校验） |
| `ak` / `sk` | ✓ | SigV4 静态凭证 |
| `bucket` | ✓ | 必须预先存在 |
| `prefix` | | 对象 key 前缀，空串即无 |
| `depth` | ✓ | 目录树深度 ≥ 1 |
| `width` | ✓ | 每层目录数 ≥ 2 |
| `files_per_dir` | ✓ | 每叶子目录文件数 ≥ 1 |
| `object_size_min` / `object_size_max` | ✓ | 字节；max≥min≥1（流式上传，无上限） |
| `part_size_min` / `part_size_max` | ✓ | multipart part size；min≥5MiB；max≥min |
| `output_dir` | | `.`（默认） |
| `concurrency` | | 8（默认） |
| `progress_interval` | | 100（默认） |
| `md5_file` | | `md5.txt`（默认） |
| `use_trailer` | | `false`（默认）；true 时开启 aws-chunked + `x-amz-checksum-sha256` trailer 上传（chunked_check_tool 检测的损坏路径） |
| `multipart_endpoint_pattern` | | `[]`（默认）；非空时手动编排 multipart，控制每个操作（init / 各 part / complete）发到哪个 endpoint index。长度 = `N+2`，N = `ceil(size/partSize)`。前置条件：S3 集群跨节点共享 multipart upload 状态。仅当 `size > partSize` 时走此路径 |
| `lprefix` / `dprefix` / `fprefix` | | 默认 `l` / `d` / `file_`；扇形链三个 segment 的前缀字符串（层级目录 / 叶子目录 / 文件名） |

校验类参数（endpoints/ak/sk/bucket/depth/width/files_per_dir/sizes）无默认值，必须显式配置——避免静默误判。

## 多段上传与节点选择

- **默认（自动）**：`multipart_endpoint_pattern` 为空时走 minio-go 自动 multipart。对象 size > partSize → multipart；否则单 PUT。partSize 在 `[part_size_min, part_size_max]` 内随机（min==max 即固定）。minio-go 自动拆段，每对象单节点 round-robin（`endpointIdx = objIdx % len(endpoints)`）。

- **手动编排**：`multipart_endpoint_pattern` 非空时**强制**走手动 multipart（不再比较 `size > partSize`）。pattern 控制每个操作的 endpoint index：
  ```
  pattern[0]         = NewMultipartUpload (init)
  pattern[1..N]     = 各 PutObjectPart（N = ceil(size/partSize)）
  pattern[N+1]      = CompleteMultipartUpload
  ```
  例：2 节点 + size=15MiB + partSize=5MiB → N=3，pattern `[0, 0, 1, 0, 1]` 表示 init→ep0, p1→ep0, p2→ep1, p3→ep0, complete→ep1。任一步失败 → `AbortMultipartUpload` (best-effort, pattern[0]) → 返回 err。

- **前置条件（手动模式）**：S3 集群必须跨节点共享 multipart upload 状态——某节点 init 拿到的 uploadID 在另一节点 PutObjectPart 必须可用。本工具不负责验证此特性，由用户保证集群支持。

- **手动模式 size 约束**：`multipart_endpoint_pattern` 非空时，`object_size_min` 必须 > `part_size_max`——保证每个对象 size 恒 > partSize，手动多段一定可走（否则 S3 拒绝 part < 5MiB）。`LoadConfig` 启动期校验，违反即报错中止。

- `part_size_min >= 5MiB` 是 S3 最小 part size 硬约束；minio-go 在 size > partSize 时按 partSize 拆段，最后一段可小于 5MiB。

- `use_trailer=true` 在手动模式下：init 与 complete 的 PutObjectOptions 带 `ChecksumSHA256`，每个 part 走 aws-chunked + `x-amz-checksum-sha256` trailer 编码。

## 输出文件

| 文件 | 内容 | 模式 |
|---|---|---|
| `<output_dir>/md5.txt` | `bucket\|object\|md5hex` 每行（仅成功对象） | truncate（重跑覆盖） |
| `<output_dir>/run.log` | 配置快照（ak/sk 屏蔽）+ 进度行 + summary + 失败日志 | append |

stdout/stderr tee 进 run.log。

## 统计字段

`=== summary ===` 段输出：`uploaded` / `failed` / `bytes` / `single_objs`（单 PUT 对象数）/ `multipart_objs`（multipart 对象数）/ `elapsed` / `rate`。

## 测试

```bash
go test -race ./...
```
