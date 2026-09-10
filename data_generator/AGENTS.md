# AGENTS.md — data_generator

本文件面向在本仓库工作的 AI 代理（和人类工程师），说明项目意图、模块边界、关键不变量与已知遗留项。改动前请先读完本文件对应章节。

## 1. 项目意图

`chunked_check_tool` 用于检测自研 S3 存储把 `aws-chunked` PUT 请求的 `length;chunk-signature=…` 等格式化内容当作原始 body 写入存储的损坏。本工具 `data_generator` 是其上游数据源：按配置的目录树批量上传对象，本地计算每个对象的 MD5 写入 `md5.txt`，供后续校验/对比。

**不在本工具范围**：损坏检测（由 `chunked_check_tool` 负责）、桶管理（必须预先创建桶）、断点续跑（重跑 truncate `md5.txt`）。

## 2. 技术栈

- Go 1.27（`go.mod` module `data_generator`）
- `github.com/minio/minio-go/v7`（PutObject 自动 multipart；BucketLookupAuto）
- `gopkg.in/yaml.v3`（配置）
- TLS：`scheme=https` 时 `InsecureSkipVerify=true`（与 `chunked_check_tool` 一致）

## 3. 模块布局

| 文件 | 职责 |
|---|---|
| `main.go` | flag 解析、`signal.NotifyContext`、`runWorkers` 编排、`processOne` per-object 流程、配置快照打印、summary |
| `config.go` | `Config` 结构体 + `LoadConfig`（YAML，强制显式配置，无默认值的字段空即报错） |
| `nodepool.go` | `NodePool`：round-robin `Assign(i)` 返回 endpoint index（无故障转移） |
| `s3client.go` | `minioPutAPI` 接口（minio.Client 子集）、`minioCoreAPI` 接口（minio.Core 子集）、`Uploader` 接口（高层）、`S3Uploader`（懒缓存 client/core per endpoint）、`NewMinioClient`/`NewMinioCore` 工厂 |
| `treegen.go` | `WalkTree`：扇形链遍历，每层 `l*` 桥同层 emit files_per_dir 个文件 + 叶子目录循环 + 桥嵌套；产 key channel；`ObjectKey{Key, Idx}` |
| `md5writer.go` | 单 goroutine + `bufio.Writer` 64KB 写 `md5.txt`，行格式 `bucket\|key\|md5hex\n`，ctx-cancel 后 drain 残留记录再 flush |
| `stats.go` | atomic.Int64 计数器（uploaded/failed/bytes/single_objs/multipart_objs）+ `Snapshot` + `PrintSummary` |
| `progress.go` | `Progress.Mark` 每 progress_interval 个对象打印进度行（CAS-free，靠 Add 的唯一返回值天然去重） |

## 4. 关键不变量（改动前必须守住）

1. **MD5 本地计算**：`io.TeeReader(prngReader, md5.New())` 在上传**期间**对流过的字节算 plain MD5，**不**用 S3 返回的 ETag（multipart ETag 是 part-MD5 的聚合，不是整对象 MD5）。`processOne` 中 `tee := io.TeeReader(body, hasher)` 必须作为 body 传给 uploader；uploader 读取多少字节，hasher 就累加多少；上传成功后 `hasher.Sum(nil)` 得到整对象的 plain MD5（单段多段一致）。**prngReader 必须恰好产 `size` 字节**——若 uploader 多读会 EOF、少读则 MD5 与 S3 对象字节不一致。

2. **失败对象不写 md5**：`UploadObject` 报错走 `stats.IncFailed()` + `progress.Mark()`，**不**写 `md5.txt`。否则 checker 会去找不存在的对象。

3. **round-robin 按对象序号**：`pool.Assign(key.Idx)` 用 treegen 分配的 0-based 全局序号做 round-robin，**不**用 worker 本地计数器——这样无论 worker 调度顺序如何，对象到节点的分布都是确定的均匀。

4. **扇形链结构**：每层 `l*` 桥目录下同层放 `files_per_dir` 个文件 + (width-1) 个叶子目录 `d*`（max depth 层 width 个）+ 桥嵌套下一层（非 max depth）。总文件数 = `(depth + (width-1)*(depth-1) + width) * files_per_dir`——其中 `depth * files_per_dir` 来自每层桥同层文件，`((width-1)*(depth-1) + width) * files_per_dir` 来自叶子目录。三个 segment 前缀（`lprefix`/`dprefix`/`fprefix`）默认 `l`/`d`/`file_`，由 `LoadConfig` 填默认值——`WalkTree` 直接用 `cfg.LPrefix`/`DPrefix`/`FPrefix`，**不**对空字符串兜底。改 `walkLayer` 时务必保留：a) 桥同层 emit 在叶子循环之前；b) 非 max depth 时叶子数 = width-1；c) max depth 时叶子数 = width；d) 桥名 `<lprefix><layer+1>` 嵌套在 `layerPath` 下。

5. **multipart 判定**：`size > partSize` → multipart=true。minio-go 在 size > partSize 时自动拆段（最后一段可小于 5MiB）；size <= partSize 时单 PUT。**不**从 `UploadInfo.ETag` 反推 multipart 状态（脆弱，依赖 ETag 格式）。

6. **part_size_min >= 5MiB**：S3 最小 part size 硬约束。`LoadConfig` 启动期校验失败即中止，避免运行到 multipart 调用时才报错。

7. **流式上传 + 无 object_size_max 上限**：`processOne` 用 `prngReader`（从 `*rand.Rand` 顺序产 `size` 字节）+ `io.TeeReader` 绑定 `md5.New()`，body 以 `io.Reader` 形式传给 uploader。内存峰值 = 一个 worker 的 part 缓冲（minio-go 内部 64KB 级），与对象大小无关，因此 **object_size_max 无上限**。`Uploader` 接口签名 `(body io.Reader, size, partSize int64)`——multipart 用 `io.LimitReader(body, partLen)` 顺序读每个 part。

8. **per-worker PRNG 独立种子**：每个 worker `rand.NewSource(baseSeed ^ int64(workerIdx))`，避免多 worker 共享全局 rand 的锁竞争。content 用 `math/rand`（非 `crypto/rand`）——快，对损坏检测场景足够（chunked_check_tool 看的是 body 头部的 chunk-signature 头，不关心 content 的随机性强度）。`prngReader` 顺序调用 `r.Read(out[:n])`——prng 流的精确字节取决于下游读取模式（minio-go 的 buffer 大小），但同一二进制内可复现。

9. **md5writer 的 ctx-cancel drain**：`Close()` 取消内部 ctx，run goroutine 进入 drain 循环读取 channel 残留记录再 flush + close file。`defer` 顺序：先 `bw.Flush()` → `file.Close()` → `close(done)`——**不能**先 `close(done)`，否则 `Close()` 在 `<-w.done` 解阻塞时 bufio 还没落盘，读文件得到空/部分内容。

10. **progress 不走 CAS 去重**：`p.counter.Add(1)` 返回唯一值，只有调用者恰好得到 `v % interval == 0` 的那个才打印，无需 CompareAndSwap。改 `Mark` 时不要改成"读 snapshot 后判断"——并发下会跳过间隔或多打。

11. **`processOne` 不退出 worker**：任何错误（prng 读失败、upload 失败、md5 写失败）都返回 err 给 `runWorkers`，worker 写日志后继续消费下一个 key。worker 退出会减少并行度但不中止其他 worker；ctx cancel 时 worker 从 `keyCh` 收到 close 后退出。

12. **use_trailer 走 minio-go TrailingHeaders + Checksum**：`UseTrailer=true` 时 `NewMinioClient` 设 `Options.TrailingHeaders=true`，`S3Uploader.UploadObject` 设 `opts.Checksum=minio.ChecksumSHA256`。minio-go 自动转 aws-chunked + `x-amz-checksum-sha256` trailer（**仅 multipart 上传**——单 PUT 只在请求头加 checksum，不发 chunked 编码）。要求 v4 签名（本工具始终用 `credentials.NewStaticV4`，满足）。**改 `NewMinioClient`/`NewS3Uploader`/`UploadObject` 时务必保留这条联动**——三者必须同时打开/关闭，否则 minio-go 会报 `Checksum requires Client with TrailingHeaders enabled`。本地 `md5.txt` 不受影响（仍写 content 的 MD5，与 S3 侧的 sha256 checksum 是两个独立量）。**手动 multipart 模式下 `UploadObjectMultipart` 同样遵循此联动**：`NewMinioCore` 也带 `TrailingHeaders`，init 与 complete 的 PutObjectOptions 带 `ChecksumSHA256`。

13. **手动 multipart pattern 路由**：`MultipartEndpointPattern` 非空且 `size > partSize` 时走 `UploadObjectMultipart`，否则走 `UploadObject`。pattern 长度必须 = `N+2`（`N = ceil(size/partSize)`）——运行时校验不匹配立即返回 err（不调 init）。任一步失败 → `AbortMultipartUpload` (best-effort, pattern[0])。**前置条件：S3 集群跨节点共享 multipart upload 状态**（uploadID 全集群可见）——本工具不验证，用户保证。改 `UploadObjectMultipart` 时保留：a) size<=partSize 拒绝；b) pattern 长度校验先于任何 S3 调用；c) 失败路径必走 abort。

## 5. CLI 与配置

### CLI flags
```
-c <path>      # 配置文件，必填
-bkt <bucket>  # 可选，覆盖 config 里的 bucket
```

### config.yaml 字段

见 `README.md` 的"配置"表。强制显式配置（endpoints/ak/sk/bucket/depth/width/files_per_dir/object_size_min/max/part_size_min/max 全部必填），可选项有默认（scheme/output_dir/concurrency/progress_interval/md5_file）。

## 6. 输出文件

| 文件 | 内容 | 何时写 | 模式 |
|---|---|---|---|
| `<output_dir>/md5.txt` | `bucket\|object\|md5hex` 每行 | 每个**成功**上传的对象 | truncate（重跑覆盖） |
| `<output_dir>/run.log` | 配置快照 + 进度行 + summary + 失败日志 | 启动→结束全程 | append |

stdout/stderr 经 `MultiWriter` tee 进 run.log。配置快照中 `ak` 用 `mask()` 显示首尾各 2 字符 + `***`，`sk` 全屏蔽。

## 7. 编译与测试

```bash
go build -o data_generator .
go test -race ./...
```

Go 1.27 二进制路径：`/Users/gengyuanzhe/sdk/go1.27.1/bin/go`。

## 8. 已知遗留项（改动时留意，非阻塞）

- **无断点续跑**：`md5.txt` truncate，重跑从头开始。需要 append + 索引去重再加。
- **multipart 不实现"每段随机节点"（自动模式）**：minio-go 自动 multipart 用单一 client。手动模式（`multipart_endpoint_pattern` 非空）实现了 per-operation 显式路由，但要求集群跨节点共享 multipart upload 状态。
- **content 用 math/rand**：可复现，对损坏检测场景足够（chunked_check_tool 看的是 body 头部的 chunk-signature 头）；若需不可预测内容，换 crypto/rand（性能下降）。
- **流式 prng 的字节取决于下游读取模式**：`prngReader` 直接调 `r.Read(out[:n])`，prng 流的精确字节取决于 minio-go 的 buffer 读取大小（同二进制内可复现，跨 minio-go 版本不一定）。若需跨版本可复现，可改成固定大小内部 buffer 的 prngReader。
- **NodePool 无故障转移**：节点宕时 upload 失败即失败，不重绑。生成场景下重试策略由用户决定（重跑或人工处理）。
- **md5writer drain 的 `default` 退出**：ctx-cancel 后 drain 用 `select { case rec := <-ch; default: return }`。理论上若 producer 在 cancel 后还在发，drain 可能在 producer 还没发完时退出——但 `runWorkers` 在 ctx cancel 后 worker 从 `keyCh` 收到 close 才退出，`md5w.Write` 不会被调用。实际无 race。
- **`processOne` 中 md5 写失败仍 IncUploaded**：stats 已 IncUploaded 在 md5 写之前；若 md5 写失败，对象已上传但 md5 没记录——`stats.IncFailed()` 在返回前补上，但 uploaded 计数仍 +1。理想是 md5 写失败时回滚 uploaded，但 S3 没有"删除已上传对象"的语义，回滚 stats 也不解决问题。当前行为：uploaded +1 + failed +1（双计），summary 时用户自行解读。
- **use_trailer 仅对 multipart 生效**：minio-go 在 size > partSize 走 multipart 时才发 aws-chunked + trailer；单 PUT（size <= partSize）只在请求头加 `x-amz-checksum-sha256`，不发 chunked 编码。若用户想覆盖单 PUT 路径，需把 `object_size_min` 设到 `part_size_min` 之上强制 multipart。

## 9. 工作流约定

- 实现性改动遵循 TDD：先写失败测试，再实现，再跑测试，再 commit。
- 每个 commit 聚焦一个职责（feat/fix/refactor 前缀）。
- `data_generator` 二进制已 `.gitignore`（父仓库），不要提交。
- `.superpowers/` 目录是 SDD 工作区，已 gitignore，不要提交。
- 改动涉及 `Uploader` 接口或 `WalkTree` 签名时，先记录决策再改。
