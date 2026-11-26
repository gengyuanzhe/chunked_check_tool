# AGENTS.md — 私有存储约定

本仓库下所有工具（`chunked_check_tool`、`delete_marker_list_tool`、`delete_marker_cleanup_tool`、`list_only`、`list_sub`、`verify_tool`、`data_generator`）都对接同一台**私有 S3 存储**。这里只记录这台存储上**与标准 S3 不一致、或尚未验证**的行为约定，按需补充。其他行为一律按 S3 规范建模。

换服务端、或得知服务端行为有变时，先对照本表再动代码。

## 1. 与标准 S3 不一致的行为约定

| 事实 | 代码义务 |
|---|---|
| 桶名可以短于 3 个字符（例：`ab`） | 用 aws-sdk-go-v2 + `UsePathStyle: true`，**不要引入 minio-go**（它在客户端就按 3–63 规则拒绝短桶名） |
| 签名校验覆盖 `x-amz-*` 和 `x-obs-*` 两类前缀（标准 SigV4 只强制 `x-amz-*`） | 自定义 header 一律不以 `x-` 开头；内部修复专用 header 用 `internal-` 前缀（SigV2/V4 下允许不签名） |
| LIST 带 `internal-list-mp-offset: true` 时改写多段 ETag：`<md5>-N` → `<md5>-<partcnt>-<off0>\|<off1>\|...`，`off_i` 是第 i 段在落盘字节里的物理起始偏移 | 解析时校验 md5 是 32 位 hex、offset 个数 = partcnt、`off_0 = 0`、严格递增、每个 `off_i ≤ Size`；该 header 只挂 LIST 调用，HEAD/GET/PUT 不带 |
| `Owner.ID` 非空时为 `[a-z0-9]+` | 不做 URL 编解码，原样读写；owner 为空落 `_unknown/` |
| **trailer 类多段上传报错不落盘**：trailer 类编码（`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`、`STREAMING-UNSIGNED-PAYLOAD-TRAILER`）作为**多段 part** 上传时报错，对象不落盘。**但单次 PUT 走 trailer 编码仍能成功落盘** | 检测工具的 trailer 损坏检测（`trailerRe`）只需覆盖**单次 PUT** 场景（普通对象的 whole-object / tail 探测）；多段边界探测不会遇到真实 trailer marker（多段 part 不可能是 trailer 上传）。trailerRe **不能删**——它守护单次 PUT 的 unsigned-trailer 损坏检测。具体机制未深究，是行为约定；服务端若修此行为需重测 |

## 2. 实测过、行为标准

- **HEAD + Range**：2026-11-23 实测，range 能满足时 `Content-Length` 等于 range 长度，与 S3 规范一致。此前 2026-09-08 backup 整批失败时记的「HEAD+Range 回 `Content-Length: 0`」是当时为解释症状做的**推断**，没有实测证据，不要再当事实引用。

## 3. 没验证过、依赖前必须先实测

- **`encoding_type_url` 开启后 marker 怎么回传**（V1 翻页）。客户端回传 marker 应该是「解码后的原始 key」（minio 做法）还是「编码值原样回传」（AWS 文档写法），两种服务端实现互斥，选错会漏扫对象。当前各工具按 minio 做法实现。在私有存储上启用之前，先用小 `max_keys` 跑多页实测。
- **以 `/` 结尾的 key 在 `delimiter=/` 重列时还在不在**。姊妹工具 delete_marker_list_tool 在 `ListObjectVersions` 上实测到偏差：`delimiter=/` 列举时以 `/` 结尾的 key 被归入 `CommonPrefixes`，再用该前缀 + `delimiter=/` 重列时，这个 key 本身**不**返回（标准 S3 会作为一条记录返回）。`ListObjects` 上没测过——启用前同样要实测。

## 4. 工作方法

- **本地 minio ≠ 私有存储**。本地冒烟和集成测试跑的都是本地 minio，能证明「客户端逻辑自洽 + minio 语义」，证明不了私有存储的行为。涉及分页、编码、token 的改动，上生产前先在私有存储上实测。
- **服务端在持续修 bug，记下的「怪癖」会过期**。引用某条 quirk 之前先重测；记录时把**症状**（看到了什么）和**机制诊断**（为什么）分开写，没实测过的机制标明是推断。

## 5. 已知服务端 BUG（设计时考虑项）

服务端存在以下**已确认的 BUG**，不是存储的"行为约定"——它是 bug，未来会被修。但在它存在期间，开发工具选方案时要把它考虑进来：两个方案都能完成任务时，优先选与该 bug 兼容的；如果选中的方案与该 bug 不兼容，**必须明确告诉用户**，由用户决定是否接受。

- **aws-chunked 上传落盘为原始字节**：PUT 请求处理未正确解析 `X-Amz-Content-Sha256` header，把 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` / `-TRAILER` / `STREAMING-UNSIGNED-PAYLOAD-TRAILER` 三类 streaming 上传的分帧格式 + trailer + 签名全部当作原始 body 字节流落盘。**后果**：备份/中转上传如果走 aws-chunked，落盘的就不是用户字节，备份本身会制造损坏。**驱动的设计选择**：中转上传一律带 `withUnsignedPayload`（裸 body + `x-amz-content-sha256: UNSIGNED-PAYLOAD`，不出现 aws-chunked 分帧），字节保真由 ETag 终验把关。详见 `chunked_check_tool/AGENTS.md §1.1`。
