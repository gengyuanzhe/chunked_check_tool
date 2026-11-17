# delete_marker_list_tool `-retry` 续传失败 prefix 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `delete_marker_list_tool` 加 `-retry <file>` flag，从文件读失败 prefix 列表只扫这些 prefix，互斥于 `-prefix`，实现"失败 prefix 续传"工作流。

**Architecture:** `-retry` 文件每行一个 prefix（格式对齐当前 `list_failed.txt` 输出——纯 prefix 字符串，空行=root prefix）。`-bkt` 仍必填，`-prefix` 和 `-retry` 互斥。`-retry` 模式下 BFS 用文件里的 prefix 列表做初始入队，而不是单根 prefix——其余 BFS 逻辑、worker 池、inflight 终止不变。prefix 粒度重扫（不是 next-marker 页级续传）——失败 prefix 从头扫，已成功写入 `delete_markers.txt` 的 marker 不重复（因为续传扫的是失败 prefix，原本就没写进去）。

**Tech Stack:** Go 1.27、aws-sdk-go-v2 v1.114.10、`gopkg.in/yaml.v3`、stdlib `bufio`/`os`/`flag`。

**Spec:** `docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md`（本计划是该 spec 的续传能力补充，spec 原文 §9 标注"无 retry/journal/signal handler"为非目标——本计划显式推翻该非目标，需在 spec 后续更新中同步。）

## Global Constraints

- 行分隔符 `\x1f`，`list_failed.txt` 格式：一行一个 prefix（root prefix = 空行）
- `version_id == "null"` 是合法 VersionId，原样透传
- 无界 queue（slice + mutex + cond），ctx-aware Pop，`SignalDone` 终止
- Add-before-Push 不变量：入队新前缀先 `inflight.Add(1)` 再 `q.Push`
- run.log 第一个 open，`mwOut`/`mwErr = MultiWriter`，sk 掩码 `***`
- `list_failed.txt` 惰性 open（文件存在即有失败）
- Go 在 `~/sdk/go1.27.1/bin`，测试前 `export PATH="$HOME/sdk/go1.27.1/bin:$PATH"`

## Review Focus

1. **空 `list_failed.txt` 文件**（文件存在但全空行 = 只有 root prefix 失败）—— 应入队 root prefix 扫，不应被当成"没有 prefix"跳过。`-retry` 模式下 root prefix 必须能扫。
2. **`-retry` 文件不存在** —— 应在预检阶段报错退出，不应启动 BFS 后才在第一次读行时报错（否则已经 open 了 run.log / 写了 config 快照，半启动状态）。
3. **`-retry` 和 `-prefix` 同时给** —— 应报错退出，不应静默选一个（互斥语义，防误用）。
4. **`-retry` 文件里有重复 prefix** —— 应去重入队，不应让多个 worker 重复扫同一个 prefix（重复扫无害但浪费 N 倍请求）。
5. **`-retry` 文件第一行格式非法**（含 `\x1f` 等）—— 应按 `list_failed.txt` 原始格式读（整行就是一个 prefix，prefix 本身可能含任意字符），不应做 `\x1f` 字段拆分。

---

## File Structure

| 文件 | 职责 |
|---|---|
| `main.go` | 加 `-retry` flag、与 `-prefix` 互斥校验、`-retry` 文件存在性预检、把"初始 prefix 列表"传给 `run` |
| `lister.go` | `Run`/`runMode2` 接受初始 prefix 列表（slice）而非单 prefix；mode 1 不受影响（单流不支持 retry，retry 只走 mode 2） |
| `lister_test.go` | 加 retry 模式测试：多 prefix 初始入队、去重、空文件 root prefix |
| `main_test.go` | 加 retry 模式 e2e：`-retry` + `-prefix` 互斥、`-retry` 文件不存在 |
| `AGENTS.md` | 不变量更新：`-retry` 语义 + 互斥约束 |

`mode 1` 不支持 `-retry`——`-retry` 模式只走 mode 2 BFS。理由：mode 1 是单流递归，prefix 列表多入口和单流语义冲突；用户若要重试 mode 1 失败，直接 `-bkt` + `-prefix` 重扫该 prefix 即可。`-retry` 模式遇到 `list_type=1` 报错。

---

## Task 1: `Run` 签名改为接受初始 prefix 列表

**Files:**
- Modify: `delete_marker_list_tool/lister.go:42-82`（`Run`/`runMode1`/`runMode2`）
- Test: `delete_marker_list_tool/lister_test.go`

**Interfaces:**
- Consumes: 当前 `Run(ctx, bucket, prefix string) error`
- Produces: `Run(ctx, bucket string, prefixes []string) error` —— `prefixes` 是初始入队前缀列表，至少 1 个元素；mode 1 只用 `prefixes[0]`，mode 2 全部入队。

- [ ] **Step 1: 写失败测试**

在 `lister_test.go` 加 `TestLister_Mode2_RetryMultiplePrefixes`：fake S3 按 query prefix 返回不同 XML——`prefix=a/` 返回 marker `a/m.txt`，`prefix=b/` 返回 marker `b/m.txt`。调 `l.Run(ctx, "testbucket", []string{"a/", "b/"})`，断言 `delete_markers.txt` 含两条 marker、`scanned_versions==2`、`DeleteMarkers==2`。

- [ ] **Step 2: 跑测试确认失败**

Run: `export PATH="$HOME/sdk/go1.27.1/bin:$PATH" && cd delete_marker_list_tool && go test -run TestLister_Mode2_RetryMultiplePrefixes -v`
Expected: FAIL（`Run` 签名不匹配，编译错误）

- [ ] **Step 3: 改 `Run`/`runMode2` 签名与初始入队**

`lister.go`：`Run` 签名改 `func (l *Lister) Run(ctx context.Context, bucket string, prefixes []string) error`。`runMode1` 调 `l.runMode1(ctx, bucket, prefixes[0])`。`runMode2` 把 `inflight.Add(1); q.Push(prefix)` 改成循环 `for _, p := range prefixes { inflight.Add(1); q.Push(p) }`。Add-before-Push 不变量保持。

- [ ] **Step 4: 修所有 `Run` 调用点**

`main.go:86` 当前 `l.Run(ctx, bucket, prefix)` 改成 `l.Run(ctx, bucket, []string{prefix})`（`-retry` 还没接，本 step 只保持现状不破坏）。`lister_test.go` 里所有 `l.Run(ctx, "testbucket", "")` 改成 `l.Run(ctx, "testbucket", []string{""})`。

- [ ] **Step 5: 跑测试确认通过**

Run: `go test -count=1 ./...`
Expected: PASS（所有现有测试 + 新测试）

- [ ] **Step 6: Commit**

```bash
git add delete_marker_list_tool/lister.go delete_marker_list_tool/lister_test.go delete_marker_list_tool/main.go
git commit -m "refactor(lister): Run accepts initial prefix slice instead of single prefix"
```

---

## Task 2: `readRetryPrefixes` 读 retry 文件 + 去重

**Files:**
- Create: `delete_marker_list_tool/retry.go`
- Test: `delete_marker_list_tool/retry_test.go`

**Interfaces:**
- Produces: `func readRetryPrefixes(path string) ([]string, error)` —— 读文件每行一个 prefix，跳过空行不跳（空行=root prefix，保留），去重保序（首次出现顺序），文件不存在返回 error。返回的 slice 至少含 1 个元素（空文件 = 1 个空 prefix = root）。

- [ ] **Step 1: 写失败测试**

`retry_test.go` 加四个测试：
- `TestReadRetryPrefixes_HappyPath`：文件内容 `a/\nb/\n`，期望返回 `["a/", "b/"]`
- `TestReadRetryPrefixes_EmptyLineIsRootPrefix`：文件内容 `\na/\n`，期望返回 `["", "a/"]`（空行保留为 root prefix）
- `TestReadRetryPrefixes_DedupPreservesFirstOrder`：文件内容 `a/\nb/\na/\n`，期望返回 `["a/", "b/"]`（去重保序）
- `TestReadRetryPrefixes_FileNotFound`：不存在路径，期望 error

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run TestReadRetryPrefixes -v`
Expected: FAIL（`retry.go` 不存在）

- [ ] **Step 3: 实现 `readRetryPrefixes`**

`retry.go`：`bufio.Scanner` 逐行读，每行（含空行）去重保序后入 slice。去重用 `map[string]struct{}` 记已见。文件 `os.Stat` 不存在直接返回 `fmt.Errorf("read retry file %s: %w", path, err)`。注意 `Scanner.Text()` 自动去掉行尾 `\n`，空行返回 `""`（root prefix），不跳过。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -run TestReadRetryPrefixes -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/retry.go delete_marker_list_tool/retry_test.go
git commit -m "feat(lister): add readRetryPrefixes to load+dedup retry prefix file"
```

---

## Task 3: `-retry` flag + 互斥校验 + 预检

**Files:**
- Modify: `delete_marker_list_tool/main.go:15-55`（flag 解析、`run` 签名、预检）
- Modify: `delete_marker_list_tool/main_test.go`（e2e 测试）
- Modify: `delete_marker_list_tool/lister.go`（mode 1 拒绝 retry，见 Review Focus §5）

**Interfaces:**
- Consumes: Task 1 的 `Run(ctx, bucket string, prefixes []string) error`，Task 2 的 `readRetryPrefixes(path string) ([]string, error)`
- Produces: `run(cfgPath, bucket, prefixes []string) error`（main 调用），`-retry <path>` flag

- [ ] **Step 1: 写失败测试**

`main_test.go` 加：
- `TestRun_RetryMode_ScansRetryPrefixes`：fake S3 + retry 文件含 `a/`+`b/`，调 `run` with `prefixes=["a/","b/"]`，断言 `delete_markers.txt` 含两条
- `TestMain_RetryAndPrefixMutuallyExclusive`：直接测 `main()` 入口不方便——改成测一个新 helper `validateFlags(cfgPath, bucket, prefix, retryPath string) error`，同时给 `-prefix` 和 `-retry` 时返回 error
- `TestMain_RetryFileMustExist`：`validateFlags` 给不存在的 `-retry` 路径时返回 error

`lister_test.go` 加 `TestLister_RetryRejectedByMode1`：调 `l.Run` 走 mode 1 + `len(prefixes) > 1` 时报错（mode 1 不支持多 prefix）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestRun_RetryMode|TestMain_Retry|TestLister_RetryRejectedByMode1' -v`
Expected: FAIL

- [ ] **Step 3: 实现 flag 互斥 + `run` 改签名**

`main.go`：加 `retryPath := flag.String("retry", "", "retry prefix file (one prefix per line; mutually exclusive with -prefix)")`。`main()` 调 `validateFlags(*cfgPath, *bucket, *prefix, *retryPath)` —— 互斥规则：`*prefix != "" && *retryPath != ""` 报错；`*retryPath != ""` 时读 `readRetryPrefixes(*retryPath)` 得到 `prefixes`，`*prefix != ""` 时 `prefixes = []string{*prefix}`，两者都空 `prefixes = []string{""}`。`run` 签名改 `func run(cfgPath, bucket string, prefixes []string) error`，把 `prefix` 参数换成 `prefixes`。`run` 内部 mode 1 校验：`if cfg.ListType == 1 && len(prefixes) > 1 { return fmt.Errorf("mode 1 does not support -retry (multiple prefixes); use -prefix instead") }`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -count=1 ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/main.go delete_marker_list_tool/main_test.go delete_marker_list_tool/lister.go delete_marker_list_tool/lister_test.go
git commit -m "feat(lister): add -retry flag for failed prefix resume (mode 2 only)"
```

---

## Task 4: MinIO 集成测试 + 文档

**Files:**
- Modify: `delete_marker_list_tool/integration_test.go`（新建 `TestIntegration_Mode2_RetryFromFailedList`）
- Modify: `delete_marker_list_tool/AGENTS.md`（不变量 §4 加 retry 约束）
- Modify: `delete_marker_list_tool/README.md`（flag 说明 + retry 工作流）

**Interfaces:**
- Consumes: Task 3 的 `-retry` flag 与 `run` 签名

- [ ] **Step 1: 写失败测试**

`integration_test.go` 加 `TestIntegration_Mode2_RetryFromFailedList`（`//go:build integration` tag）：MinIO 桶 `a/` 和 `b/` 下各放一个 delete marker，写 retry 文件含 `a/\nb/`，调 `run(cfgPath, bucket, []string{"a/", "b/"})`，断言 `delete_markers.txt` 含两条、`scanned_versions==2`。

- [ ] **Step 2: 跑集成测试确认通过**

Run: `MINIO_ENDPOINT=127.0.0.1:19001 MINIO_AK=minioadmin MINIO_SK=minioadmin123 go test -count=1 -tags integration -run TestIntegration_Mode2_RetryFromFailedList -v`
Expected: PASS（需先启 MinIO；如未启则跳过）

- [ ] **Step 3: 更新 AGENTS.md**

`§4 关键不变量` 加一条：
> 13. **`-retry <file>` 与 `-prefix` 互斥**：`-retry` 读文件每行一个 prefix，初始 BFS 入队用该列表（去重保序）。`-retry` 只支持 mode 2（mode 1 单流不支持多 prefix 入口）。`-retry` 文件不存在 = 硬退出。`-retry` 模式下 `list_failed.txt` 仍会记新失败——可多次 `-retry list_failed.txt` 直到空文件。

`§4.12 已知偏差` 末尾加一句：`-retry` 是 prefix 粒度重扫，不是 next-marker 页级续传——失败 prefix 已成功的前 N 页会重扫，但因 `delete_markers.txt` 是 `O_APPEND`，已写入的 marker 会重复追加。续传场景下前 N 页未写入（prefix 失败前根本没扫到那些 marker），所以实际不重复——但用户若手动构造 retry 文件（含已扫完的 prefix）会重复，需自行去重。

- [ ] **Step 4: 更新 README.md**

flag 表加 `-retry` 行；加"续传工作流"小节：
```
list → 看 list_failed.txt → -retry list_failed.txt → 再看 list_failed.txt → 循环直到空文件
```

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/integration_test.go delete_marker_list_tool/AGENTS.md delete_marker_list_tool/README.md
git commit -m "docs(lister): document -retry flag + add MinIO integration test"
```

---

## Self-Review

**1. Spec coverage:** spec §9 原"无 retry"非目标被本计划显式推翻——Task 3 Step 3 实现了 retry，Task 4 Step 3 在 AGENTS.md 标注。Review Focus 5 条都有对应测试任务。✓

**2. Step scan:** 每个 step 是一个动作 + 一个可检查结果，无 TBD/兜底语。Task 3 Step 3 的 `validateFlags` helper 是新增决策点——main.go 直接调即可，无需独立文件。✓

**3. Type consistency:** `Run(ctx, bucket string, prefixes []string) error` 在 Task 1 定义、Task 3 调用一致。`readRetryPrefixes(path string) ([]string, error)` 在 Task 2 定义、Task 3 调用一致。`run(cfgPath, bucket string, prefixes []string) error` 在 Task 3 定义、Task 4 集成测试调用一致。✓

**4. Review Focus:** 5 条均有测试覆盖——§1（空文件 root prefix）在 Task 2 Step 1 的 `EmptyLineIsRootPrefix` 测试；§2（文件不存在）在 Task 2 + Task 3 Step 1 的 `FileNotFound` + `RetryFileMustExist`；§3（互斥）在 Task 3 Step 1 的 `MutuallyExclusive`；§4（去重）在 Task 2 Step 1 的 `DedupPreservesFirstOrder`；§5（含 `\x1f` 的 prefix）在 Task 2 Step 3 的实现注释 + Task 2 Step 1 不做字段拆分（`Scanner.Text()` 整行就是 prefix）。✓

**5. Proportion:** 计划长度 vs 实现代码量（~150 行）合理，4 个 task 各有独立测试周期。✓
