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
```


## 命令参数

| flag | 必填 | 说明                                                               |
|---|---|--------------------------------------------------------------------|
| `-c` | 是 | 配置文件路径                                                       |
| `-bkt` | 是 | 桶名                                                               |
| `-prefix` | 否 | 列举前缀，默认空（整个桶）                                         |
| `-nextmarker` | 否 | start-after key，跳过该 key 之前的对象；**仅 list_type 为1时生效** |

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
is_check: true                      # true=列举+校验, false=仅列举
is_success_log: false               # 是否记录正常普通对象到 <ownerID>/ok_objects.txt
is_multipart_segment_check: false   # 是否按固定 part size 对多段对象做分段损坏检查
multipart_segment_size: 0           # 多段分段检查的段长度(字节)，需与上传 part size 一致
is_multipart_success_log: false     # 是否记录干净的多段对象到 <ownerID>/ok_mp.txt
progress_interval: 5000             # 进度记录间隔
obj_ch_capacity: 0                  # lister→checker channel 容量；0=max(check_concurrency*4, 2000)
output_ch_capacity: 0               # output writer channel 容量；0=1024
result_line_format: <bucket>|<key>  # 结果文件每行格式，支持 <bucket>/<key>/<owner> 占位符
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
| `is_check` | `true` | `false` 时只列举不校验，不写对象文件，仅写 `list_failed.*` |
| `is_success_log` | `false` | `true` 时把正常普通对象 key 写入 `<ownerID>/ok_objects.txt` |
| `is_multipart_segment_check` | `false` | `true` 时按固定 part size（`multipart_segment_size`）对多段对象做分段损坏检查；`true` 时必须配 `multipart_segment_size > 0`，否则启动报错 |
| `multipart_segment_size` | `0` | 多段分段检查的段长度（字节），需与上传 part size 一致；`0` 表示不分段 |
| `is_multipart_success_log` | `false` | `true` 时把干净的多段对象 key 写入 `<ownerID>/ok_mp.txt` |
| `progress_interval` | `5000` | stdout 进度打印阈值（约） |
| `obj_ch_capacity` | `max(check_concurrency*4, 2000)` | lister→checker channel 容量；0 走默认 |
| `output_ch_capacity` | `1024` | output writer channel 容量（每个结果/处理文件一个 channel）；0 走默认 |
| `result_line_format` | `<bucket>\|<key>` | 结果文件每行格式，支持 `<bucket>`/`<key>`/`<owner>` 占位符；只影响 per-owner 结果文件，处理文件始终只存 key/prefix |

## 输出

全部以 append 模式打开。目录结构：

```
<output_dir>/
├── run.log                     # 进程运行日志
├── list_failed.txt             # 列举失败 prefix（list worker 调用失败时写）
├── list_failed.log             # 列举失败结构化错误
├── check_failed.txt            # 普通对象 RangeGet 失败 key
├── check_failed.log            # 普通对象 RangeGet 失败结构化错误
├── mp_check_failed.txt         # 多段分段 RangeGet 失败 key（is_multipart_segment_check=true 时）
├── mp_check_failed.log         # 多段分段 RangeGet 失败结构化错误
└── <ownerID>/                  # OwnerID 为空时落到 _unknown/
    ├── corrupted_objects.txt   # 损坏普通对象 key（Range GET 命中 chunk-signature）
    ├── ok_objects.txt          # 正常普通对象 key（is_success_log=true 时）
    ├── mp.txt                  # 多段对象 key（is_multipart_segment_check=false 时）
    ├── corrupted_mp.txt        # 损坏多段对象 key（分段检查命中 chunk-signature）
    └── ok_mp.txt               # 干净多段对象 key（is_multipart_segment_check=true && is_multipart_success_log=true 时）
```