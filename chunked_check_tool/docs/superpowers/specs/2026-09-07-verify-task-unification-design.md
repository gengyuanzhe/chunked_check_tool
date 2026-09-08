# VerifyTask 统一校验内核与 list-file 输入源 — 设计

日期: 2026-09-07
状态: 待评审

## 1. 背景与目标

现有工具已支持：
- 对象列举（Mode 1/2/3，均产出 `ObjectInfo` 到 `objCh`）
- 普通对象 chunked 损坏校验（RangeGet 头 128B，正则 `chunkSigRe`）
- 多段对象按**固定段大小** chunked 损坏校验（offsets = `0, segSize, 2*segSize, ...`）

未覆盖的场景：每个多段对象的**每段大小不固定**，offsets 需要外部给定。

目标：
1. 引入 `VerifyTask` + 统一 `verify()` 内核，把"normal 单段"和"固定分段"折叠成同一条路径，并自然支持"显式 offset 列表"。
2. 新增 `-list-file` 输入：按行解析 `bkt|key|partcnt|offset0|offset1|...`，对每个 offset 做 128B RangeGet 探测。
3. 预留 `InputSource` 接口，未来"从 etag 提取 offset"作为第三个输入源接入，不动校验内核。

非目标：
- 不实现 etag 源（仅预留接口位）。
- 不改 S3 API 层（`S3API` / `RangeGetAt` / NodePool 故障转移都不动）。
- 不改输出文件格式与 per-owner 分片逻辑。

## 2. 核心抽象

### 2.1 VerifyTask

```go
type VerifyTask struct {
    Key         string
    OwnerID     string
    ETag        string  // list-file 源可能为空
    Size        int64   // list-file 源可能为 0
    IsMultipart bool    // 决定输出路由：true→mp 文件/计数器，false→normal
    Offsets     []int64 // 每个 offset 处 RangeGet 128B；nil=不探测，仅记录到 multipart_all
}
```

`objCh` 类型从 `chan ObjectInfo` 改为 `chan VerifyTask`。

### 2.2 统一 verify 内核

```go
func (c *Checker) verify(task VerifyTask) {
    // multipart + 未开启 segcheck → 不探测，写 mp_all，不计 ok_mp
    if task.IsMultipart && len(task.Offsets) == 0 {
        c.out.WriteMultipartAll(task.OwnerID, task.Key)
        return
    }
    // normal：单次探测 offset 0（Offsets==nil 的隐式语义），不进循环、不分配切片
    // multipart：遍历 Offsets 逐个探测
    settled := false
    if !task.IsMultipart {
        settled = c.probeAndRoute(task, 0)
    } else {
        for _, off := range task.Offsets {
            if c.probeAndRoute(task, off) { settled = true; break }
        }
    }
    if settled { return }
    // 全部干净 → 按 IsMultipart 路由 ok_mp / ok_object
}

// probeAndRoute 做一次 128B RangeGet，按 IsMultipart 路由失败/损坏。
// 返回 true 表示 task 已定性，调用方停止后续探测。normal 和 multipart 共享此路由。
func (c *Checker) probeAndRoute(task VerifyTask, off int64) bool {
    body, err := c.worker.RangeGetAt(context.Background(), task.Key, off, 128)
    if err != nil {
        // 按 IsMultipart 路由 mp_check_failed / check_failed
        return true
    }
    if chunkSigRe.Match(body) {
        // 按 IsMultipart 路由 corrupted_mp / corrupted
        return true
    }
    return false
}
```

等价性：
- 原 normal 单段 = `VerifyTask{IsMultipart:false, Offsets:nil}`（verify 单次探测 offset 0）
- 原固定分段 = `VerifyTask{IsMultipart:true, Offsets:[0,seg,2seg,...]}`
- 新列表 offset = `VerifyTask{IsMultipart:true, Offsets:[来自文件]}`

`Checker.Handle` 改签名为 `Handle(task VerifyTask)`，仅做参数透传、size==0 normal 短路、与 localCounter 自增。

### 2.3 normal 路径无切片

normal 对象 `Offsets == nil`，`verify()` 对 `!IsMultipart` 分支直接调 `probeAndRoute(task, 0)`——不构造切片、不进循环、零分配。比"共享 `normalOffsets` 包级变量"更直白，且不需要任何共享状态。固定分段路径每对象仍分配 `ceil(Size/seg)` 长度切片——可接受（段数远小于对象数；后续若需优化可池化，不在本 spec 范围）。

## 3. 输入源抽象

### 3.1 InputSource 接口

```go
type InputSource interface {
    Run(ctx context.Context, objCh chan<- VerifyTask) error
}
```

实现：
| 实现 | 用途 | offsets 来源 |
|---|---|---|
| `s3ListSource` | 包装现有 Lister/walker（Mode 1/2/3） | `resolveOffsets(obj, cfg)` 计算 |
| `listFileSource` | 读 `-list-file`，按行解析 | 行内显式 |
| _etagOffsetSource_（预留，不实现） | 未来从 etag 提取 offsets | etag 解析 |

`s3ListSource` 不重写 Lister——它委托给现有 `Lister.Run` / `runRecursiveWalk`，只在产出的 `ObjectInfo` 处即时调 `resolveOffsets` 转 `VerifyTask` 再推 `objCh`。具体做法：把 objCh 类型改成 `chan VerifyTask`，Lister/walker 内部"收到 o → resolveOffsets(o) → 推 task"。`s3ListSource.Run` 仅作为接口适配层，封装 main.go 里的 mode 分派。

### 3.2 listFileSource 行解析

行格式：`bkt|key|partcnt|offset0|offset1|...`

字段语义（用户确认）：
- `bkt`：bucket 名，必须等于 `-bkt`
- `key`：对象 key
- `partcnt`：段数，≥1 整数
- `offset_i`：第 i 段在对象体中的**绝对字节偏移**；`offset0` 必须为 0；共 `partcnt` 个 offset（含 offset0）

校验规则：
1. 以 `|` 分割。字段数必须 == `3 + partcnt`（bkt, key, partcnt, 然后恰好 partcnt 个 offset）。partcnt 未知时先解析前 3 字段拿到 partcnt，再核对剩余字段数。
2. `partcnt` ≥ 1 且为十进制整数
3. offset 字段数 == partcnt
4. 所有 offset ≥ 0
5. offset 严格递增
6. `offset0 == 0`
7. `bkt == -bkt`

任一不满足 → 坏行：写 `list_failed.txt`（含原始行 + 行号）+ `IncrListFailed`，跳过继续。

成功行 → `VerifyTask{Key, IsMultipart:true, Offsets:offsets, ETag:"", Size:0}` 推入 objCh。

### 3.3 list-only 模式（is_check=false）

`-list-file` 与 `is_check=false` 互斥。文件即列表，无 S3 LIST 可枚举；仅校验模式下 `-list-file` 才有意义。启动时若 `-list-file != ""` 且 `is_check=false` → 报错中止。

### 3.4 与 list_type 的关系

`-list-file != ""` 时跳过 S3 枚举路径（Mode 1/2/3 的分支都不走），直接跑 `listFileSource.Run`。`-list_type` 配置值此时被忽略，不报错（兼容 yaml 模板里写死的 list_type=2）。

## 4. 改动文件清单

| 文件 | 改动 |
|---|---|
| `verify_task.go`（新） | `VerifyTask` 类型；`resolveOffsets(obj, cfg)` |
| `input_source.go`（新） | `InputSource` 接口；`listFileSource`（行解析 + 校验 + Run）；`s3ListSource`（委托适配层） |
| `checker.go` | `Handle` 签名 `ObjectInfo`→`VerifyTask`，调 `verify()`；删除 `checkMultipartSegments`（折叠进 `verify`）；`chunkSigRe`/`extract*` 不动 |
| `lister.go` | objCh 元素类型 `ObjectInfo`→`VerifyTask`；收到 o 后调 `resolveOffsets` 转 task；list-only 模式计数逻辑保留 |
| `walker.go` | 同 lister，Mode 3 路径同步改造 |
| `main.go` | 新增 `-list-file` flag；分支：有 file→`listFileSource.Run`，无→现有 s3 list 路径；`objCh` 类型改 `chan VerifyTask`；printConfig 增加 list-file 行 |
| `s3client.go` | `S3API`/`FakeS3` 不动（`RangeGetAt` 已满足） |
| `config.go` `config.yaml` | 无新字段（`-list-file` 是 CLI flag，不进 yaml） |
| `checker_test.go` | 输入从 `ObjectInfo` 改为 `VerifyTask`；保留 normal/seg/corrupt/failed 各路径覆盖 |
| `list_file_test.go`（新） | 行解析 / 坏行 / partcnt 校验 / bkt 不匹配 / offset 非法 |
| `lister_test.go` `walker_test.go` | objCh 类型适配 |
| `main_test.go` | 适配 objCh 类型；新增 `-list-file` 分支的 smoke 测试 |

## 5. 性能与兼容

- 性能：normal 路径 `Offsets=nil`，`verify()` 对 `!IsMultipart` 直接单次 `probeAndRoute(task, 0)`——零分配、无切片。固定分段路径每对象仍分配 ceil(Size/seg) 长度切片——可接受（段数远小于对象数；后续若需优化可池化，不在本 spec 范围）。
- 兼容：`is_multipart_segment_check` / `multipart_segment_size` 配置语义不变，仅作用于 S3 列举源。`-list-file` 源忽略它们（offsets 来自文件）。
- 断点续跑：list-file 源同样支持 append 模式输出。重跑需重新读整个文件（不记游标），与今天 S3 列举重跑语义一致。

## 6. 失败模式与输出路由

| 情况 | 输出文件 | 计数器 |
|---|---|---|
| normal 对象全 offsets 干净 | `ok_objects.txt`（若 `is_success_log`） | `IncrOkObjects` |
| normal 对象某 offset GET 失败 | `check_failed.txt` + `check_failed.log` | `IncrCheckFailed` |
| normal 对象某 offset 命中 chunkSig | `corrupted_objects.txt` | `IncrCorruptedObjects` |
| multipart 对象全 offsets 干净 | `ok_mp.txt`（若 `is_multipart_success_log`） | `IncrOkMp` |
| multipart 对象某 offset GET 失败 | `mp_check_failed.txt` + `mp_check_failed.log` | `IncrMpCheckFailed` |
| multipart 对象某 offset 命中 chunkSig | `corrupted_mp.txt` | `IncrCorruptedMp` |
| multipart 对象 `Offsets==nil`（S3 源未开启 segcheck） | `mp_all.txt` | （不计 ok_mp） |
| list-file 坏行 | `list_failed.txt` + `list_failed.log` | `IncrListFailed` |

`listed_obj` / `listed_mp` 计数：list-file 源不计入这两项（无 ETag 可分类）。在 `list_file_source` 启动时不调 `IncrListedObject`/`IncrListedMp`。`list_all` 在 summary 里 = `listed_obj + listed_mp`，因此 list-file 模式下这两个为 0，summary 会显示 `list_all: 0`。这是预期行为（list-file 模式下"列举"语义不适用）。

## 7. 测试策略

- `verify_task_test.go`：`resolveOffsets` 三分支（normal / 固定分段 / multipart 不探测）。
- `list_file_test.go`：表驱动覆盖正行 + 各类坏行（字段不足 / partcnt 不符 / 非数字 / 递减 / offset0≠0 / bkt≠-bkt）。
- `checker_test.go`：`VerifyTask` 输入下的 6 条输出路由（normal ok/corrupt/failed + mp ok/corrupt/failed），用 `FakeS3.RangeGetHandler` 模拟命中/干净/失败。
- `main_test.go`：`-list-file` 端到端 smoke（fake S3 + 临时文件）。
- 既有 lister/walker 测试适配新 objCh 类型。

## 8. 未来扩展（etag 源）

预留 `InputSource` 接口的第三个实现 `etagOffsetSource`：
- 输入：对象列表（key + etag）
- 解析：从 etag 提取 part sizes（具体协议待定——S3 原生 multipart etag 不编码 part sizes，可能依赖自定义元数据或外部映射）
- 输出：`VerifyTask{IsMultipart:true, Offsets: 累加计算}`

本 spec 仅声明接口位，不实现。实现时新增 `etag_offset_source.go`，无需改 `verify` 内核或 `VerifyTask`。

## 9. 开放问题

无（所有澄清问题已回答）。
