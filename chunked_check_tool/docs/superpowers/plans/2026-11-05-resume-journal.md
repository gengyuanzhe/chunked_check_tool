# Resume Journal（Mode 2 kill -9 断点续跑）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** kill -9 之后，Mode 2 扫描从 journal.txt 记录的进度断点续跑，损失 ≤ 一个 checkpoint 间隔的工作量。

**Architecture:** 一个 JournalTracker 在运行时旁路记账（页完成位置、子前缀发现、终态计数），一个 checkpoint goroutine 每 10 分钟执行"快照 → flush 输出 → 追加 journal 批次"的固定顺序；恢复时重放 journal 重建每个前缀的状态（done / 有游标 / 未启动），按现有 -resume-list 的 seed 机制喂回 BFS。记账不阻塞流水线。

**Tech Stack:** Go 1.27，标准库（sync、time、bufio、os）；复用现有 Lister/Checker/Output/Queue/FakeS3 测试基建。

**Spec:** `chunked_check_tool/docs/superpowers/specs/2026-11-05-resume-journal-design.md`（v2，平实语言版）。计划从 spec 立论，执行者需先读 spec。

## Global Constraints

- go 工具链在 `~/sdk/go1.27.1/bin`：每次 shell 命令先 `export PATH=~/sdk/go1.27.1/bin:$PATH`。
- 所有命令在 `chunked_check_tool/`（包目录）下执行：`go test -run <TestName> -count=1 .`
- gofmt：`config.go` / `resume_list.go` / `s3client.go` 有历史格式偏差，**不要顺手 reformat**；新建/修改文件本身必须 gofmt 干净。
- AGENTS §9 工作流：TDD（先失败测试）、每个 commit 一个职责（feat/fix/refactor 前缀）、二进制不提交。
- 热路径约束（spec §2）：每个对象新增操作 = checker 侧一次加锁计数 + 一个 defer；不得有 per-object 堆分配。
- 正确性北极星（spec §6）：任何出错/竞态的后果只能是"恢复时多做工作"，不允许"有对象漏掉没人检查"。
- 注释与 AGENTS 文档用中文、平实语言；代码注释只在"为什么"不明显时写（spec 的顺序不变量属于此类）。
- `run()` 签名要加一个参数（`resumeJournal`）：改 `main.go` 调用点与 `main_test.go` 的调用点；commit message 里按 §9 记录接口变更。
- Lister/Checker 挂 journal 用 setter（`SetJournal`），不动现有构造函数签名——避免波及几十处既有测试构造点。先例：`ProgressPrinter.SetQueueSnapshotProvider`。

## Review Focus

以下 5 类最容易咬人的输入/竞态，spec 隐含但分散；每个都已钉进所属任务的测试：

1. **硬停打断在半页 + 之后 checkpoint 触发**——页完成位置必须冻结（不是把 400/1000 记成完成）→ Task 5 `TestListerJournalHardAbortFreezesPosition`。
2. **journal 末尾撕裂行**（kill 在批次写一半）→ 重放丢弃 → Task 1 `TestReplayJournalDropsTornTail`。
3. **前缀名含 `|`** → 解析不能左切分 → Task 1 `TestParseJournalLinePrefixWithPipe`。
4. **kill 发生在第一次周期性 checkpoint 之前** → checkpoint 循环启动时先立即执行一次（把 root/种子写进 journal）→ Task 4 `TestCheckpointImmediateFirstWrite`。
5. **恢复轮自己被 kill 在零页进度时** → 种子游标（baseToken）必须已落 journal → Task 2 `TestTrackerSeedTokenPreservedBeforeFirstPage` + Task 4 立即首检。

---

### Task 1: journal 行格式与重放器

**Files:**
- Create: `journal.go`（本任务只放 event/parse/format/Replay 部分，后续任务继续往里加）
- Test: `journal_test.go`

**Interfaces:**
- Produces（后续任务依赖）:
  - `type journalEventKind int`；常量 `journalEventDiscovered / journalEventCursor / journalEventDone`
  - `type journalEvent struct { Kind journalEventKind; Prefix string; Token string }`
  - `func parseJournalLine(line string) (journalEvent, error)`
  - `func formatJournalEvent(prefix string, token string, kind journalEventKind) string`（cursor 用 token；discovered/done 忽略 token）
  - `type journalState struct { Discovered, Done bool; Token string }`
  - `func ReplayJournal(path string) (map[string]journalState, error)`

- [ ] **Step 1: 写失败测试**

```go
func TestParseJournalLineDiscoveredRestOfLine(t *testing.T) // "discovered|data|2026" → Prefix "data|2026"
func TestParseJournalLineCursorSplitsAtLastPipe(t *testing.T) // "cursor|a|b|TOKEN" → Prefix "a|b", Token "TOKEN"
func TestParseJournalLineEmptyPrefix(t *testing.T) // "discovered|" → Prefix ""；"cursor||T" → Prefix "", Token "T"
func TestParseJournalLineMalformed(t *testing.T) // "unknown|x"、"cursor|onlytoken"（rest 无 "|"）→ error
func TestReplayJournalLastEventWins(t *testing.T) // discovered|p; cursor|p|T1; cursor|p|T2; done|q → p:{Token:T2}, q:{Done}
func TestReplayJournalDropsTornTail(t *testing.T) // 末行无换行 → 丢弃该行
func TestReplayJournalMissingFile(t *testing.T) // 不存在的路径 → error
```

torn-tail 检测：先 `f.Stat()`，`Seek(Size-1)` 读 1 字节，非 `'\n'` 则扫描时丢弃最后一个 token（bufio.Scanner 会把无换行的尾巴当正常行返回，必须显式处理）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestParseJournalLine|TestReplayJournal' -count=1 .`
Expected: 编译失败（journal.go 未定义符号）。

- [ ] **Step 3: 实现 journal.go 的 event/parse/format/Replay**

解析规则（spec §3）：discovered/done 行 prefix = 去掉首字段后的整行余量；cursor 行在余量中取**最后一个** `|` 切分（token 永不含 `|`）。未知首字段或 cursor 余量无 `|` → error。ReplayJournal 逐行 parse，同 prefix 后写覆盖先写；done 终态优先于 token。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -run 'TestParseJournalLine|TestReplayJournal' -count=1 .`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add journal.go journal_test.go
git commit -m "feat: journal line format + replay (last-event-wins, torn-tail drop, pipe-tolerant prefixes)"
```

### Task 2: JournalTracker 记账核心

**Files:**
- Modify: `journal.go`
- Test: `journal_test.go`

**Interfaces:**
- Consumes: Task 1 的类型。
- Produces（Task 4/5/6 依赖，签名固定）:

```go
type JournalTracker struct{ /* mu sync.Mutex; per-prefix state */ }
func NewJournalTracker() *JournalTracker
func (t *JournalTracker) RecordSeed(prefix, token string)   // 种子入记（token="" 从头列）；= AddDiscovered + baseToken
func (t *JournalTracker) AddDiscovered(prefix string)
func (t *JournalTracker) RegisterPage(prefix string, objCount int, nextToken string) int32
func (t *JournalTracker) Terminal(prefix string, seq int32)
func (t *JournalTracker) MarkPaginationDone(prefix string)

type journalCursorEntry struct{ Prefix, Token string }
type journalSnapshot struct {
    Discovered []string            // 自上次 Commit 的新发现（delta）
    Cursors    []journalCursorEntry // 所有在途前缀的当前续列点
    Dones      []string            // 自上次 Commit 新完成的前缀（delta）
}
func (t *JournalTracker) Snapshot() journalSnapshot
func (t *JournalTracker) Commit(snap journalSnapshot)   // 落盘后调用：清 delta、删 done 前缀
func (t *JournalTracker) trackedPrefixes() int          // 仅供测试观测内存回收
```

内部状态（签名和测试定不下来的部分，照此实现）：

```go
type journalPrefixState struct {
    baseToken      string           // 种子游标（"" = 从头列）
    paginationDone bool
    maxSeq         int32            // 已登记的最大页号
    outstanding    map[int32]int    // 页号 -> 未完成终态数（界 = objCh 容量 + in-flight）
    nextTokens     map[int32]string // 页号 -> 该页返回的 next token（界同上）
}
// 完成位置 = 最小的 outstanding>0 页号 - 1（全 0 则 maxSeq）；连续完成的最末页
// 续列 token = 完成位置>=0 ? nextTokens[完成位置] : baseToken
// done = paginationDone && 完成位置 == maxSeq
```

- [ ] **Step 1: 写失败测试**

```go
func TestTrackerPageCompletesWhenAllTerminals(t *testing.T)
// RegisterPage("p", 3, "T1") → 3×Terminal → Snapshot.Cursors 含 {p, T1}
func TestTrackerConsecutivePagesRequired(t *testing.T)
// 页1(2 obj) 全完成 + 页2(2 obj) 只完成 1 → cursor 停在页1 的 token
func TestTrackerFrozenOnPartialPage(t *testing.T)
// RegisterPage(p, 1000, T2) 只 Terminal 400 次 → cursor 停留上页；1000 次后才前进 —— Review Focus #1 的纯逻辑版
func TestTrackerDoneRequiresPaginationDoneAndAllPages(t *testing.T)
// 分页未结束或页未全完成 → Dones 为空；两者满足 → Dones=[p]
func TestTrackerSeedTokenPreservedBeforeFirstPage(t *testing.T)
// RecordSeed(p, T) 后零页完成 → Snapshot.Cursors 含 {p, T} —— Review Focus #5
func TestTrackerDiscoveredDeltaAndCommit(t *testing.T)
// AddDiscovered → Snapshot.Discovered=[x]；Commit(snap) → 下一轮 Snapshot.Discovered 不含 x
func TestTrackerDropAfterCommit(t *testing.T)
// done + Commit 后 trackedPrefixes() 下降；在途前缀保留
func TestTrackerEmptyPrefixDoneInOnePage(t *testing.T)
// RegisterPage(p, 0, "") + MarkPaginationDone → done（空前缀一页即完成）
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestTracker' -count=1 .` Expected: 编译失败。

- [ ] **Step 3: 实现 tracker**

RegisterPage 分配 `maxSeq+1` 并登记 `outstanding[seq]=objCount`；Terminal 递减（对已删除/done 前缀的迟到 Terminal 静默忽略——只会推迟位置，安全方向）；Commit 删 done 前缀的 map 条目。Commit 移除 snapshot.Discovered 中列出的 pending 条目（pendingDiscovered 用 set+list 双结构，Commit 只删快照里的）。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -run 'TestTracker' -count=1 .` Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add journal.go journal_test.go
git commit -m "feat: JournalTracker page-level accounting (frozen-on-partial, drop-after-persist)"
```

### Task 3: Output.FlushAndWait（排干 channel 再 flush）

**Files:**
- Modify: `output.go`（`openAndStartRoot`、`openAndStartOwner` 的 select 循环加 ctrl case；`Output` 加 flushCtrls 字段与 `FlushAndWait`）
- Test: `output_test.go`

**Interfaces:**
- Produces: `func (o *Output) FlushAndWait()`——返回时保证"调用开始前已推入任何数据 channel 的行"都写进了文件。
- 机制：每个 writer goroutine 注册一个 `flushCtrl{req chan chan struct{}}`；收到 req 后**先非阻塞排干数据 channel**（`select … default` 循环），再 flush bufio，再 `close(reply)`。排干保证顺序：调用前完成的 send 必在缓冲里，drain 到空必经过它。spec §5 的"哨兵"方案与此等价（都靠 channel FIFO），此实现免魔法值、不改 channel 类型——spec 措辞按此实现为准，保证的顺序不变。

- [ ] **Step 1: 写失败测试**

```go
func TestOutputFlushAndWaitRoot(t *testing.T)
// WriteCheckFailed 一行 → FlushAndWait() → 不调 Close 直接读 check_failed.txt，行在
func TestOutputFlushAndWaitOwner(t *testing.T)
// WriteCorrupted 两个 owner → FlushAndWait() → 两个 owner 文件均可读（不调 Close）
func TestOutputFlushAndWaitLiveProducer(t *testing.T)
// 生产者 goroutine 持续推 N 行（含 FlushAndWait 期间仍在推的），FlushAndWait 返回后
// "启动信号之前推完的行"全部在盘；带 -race 跑
```

注意 drain 循环里 `case line, ok := <-ch` 的 `ok=false`（channel 已关）分支：直接跳出排干去 flush。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestOutputFlushAndWait' -count=1 .` Expected: 编译失败（FlushAndWait 未定义）。

- [ ] **Step 3: 实现 flushCtrl + FlushAndWait**

`openAndStartRoot`/`openAndStartOwner` 各创建一个 ctrl 注册进 `o.flushCtrls`（append 需在 goroutine 启动前，无锁）；`FlushAndWait` 向每个 ctrl 发 reply chan 并等全部 close。禁止在 Close 之后调用（运行期 checkpoint 循环在 out.Close 前停止，Task 4 保证）。

- [ ] **Step 4: 跑测试确认通过（含 race）**

Run: `go test -race -run 'TestOutputFlushAndWait' -count=1 .` Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add output.go output_test.go
git commit -m "feat: Output.FlushAndWait drains writer channels before flushing bufio"
```

### Task 4: checkpoint 循环（快照 → flush → 写盘 → Commit）

**Files:**
- Modify: `journal.go`
- Test: `journal_test.go`

**Interfaces:**
- Consumes: `JournalTracker`（Task 2）、`Output.FlushAndWait`（Task 3）、`formatJournalEvent`（Task 1）。
- Produces:

```go
var journalCheckpointInterval = 10 * time.Minute // 包级 var，测试改小

// 启动时立即执行一次 checkpoint（把种子写进 journal——Review Focus #4/#5），
// 之后每 interval 一次；ctx.Done 时再执行最后一次（终态落盘）然后返回。
// out.Close() 之前必须先 cancel 这个 ctx。
func runJournalCheckpoints(ctx context.Context, tr *JournalTracker, out *Output, journalPath string)
```

批次写入顺序（spec §5）：**Discovered 行 → Cursors 行 → Dones 行**，单个 append（一批一个 write 循环，保持文件句柄打开）。

- [ ] **Step 1: 写失败测试**

```go
func TestCheckpointImmediateFirstWrite(t *testing.T)
// 间隔改 10s；启动循环（RecordSeed 先行）→ 短暂等待 → journal.txt 已含种子 discovered 行，不等 tick
func TestCheckpointOrderAndDurability(t *testing.T)
// 间隔 10ms；驱动 tracker（页完成）+ out.WriteCheckFailed（结果行）→ 等一次 checkpoint →
// 断言两件事：journal.txt 的 cursor 行出现；对应结果行**不调 Close**已在 check_failed.txt —— 顺序不变量的功能级验证
func TestCheckpointBatchOrderDiscoveredFirst(t *testing.T)
// 人为制造本批同时有 Discovered 与 Cursors → 读 journal.txt，discovered 行在 cursor 行之前
func TestCheckpointFinalOnStop(t *testing.T)
// cancel ctx → 循环退出前执行最后一次 checkpoint → journal 反映 cancel 时刻状态
func TestCheckpointCommitClearsDelta(t *testing.T)
// 两个 checkpoint 后，第二次批次不再重复第一次的 discovered 行
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestCheckpoint' -count=1 .` Expected: 编译失败。

- [ ] **Step 3: 实现 runJournalCheckpoints**

启动即 checkpoint 一次 → `for { select { case <-ctx.Done(): 最终 checkpoint; return; case <-ticker.C: checkpoint() } }`，其中 `checkpoint() = tr.Snapshot() → out.FlushAndWait() → appendJournalBatch → tr.Commit(snap)`；写盘失败只 log（下轮重试，重放幂等）。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -run 'TestCheckpoint' -count=1 .` Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add journal.go journal_test.go
git commit -m "feat: journal checkpoint loop (snapshot->flush->append->commit, immediate first write)"
```

### Task 5: lister / checker 接线（记账不阻塞流水线）

**Files:**
- Modify: `lister.go`（`Lister` 加 journal 字段 + `SetJournal`；`processPrefix` 簿记；Seed/SeedWithToken 记种子）
- Modify: `checker.go`（`Checker` 加 journal 字段 + `SetJournal`；`Handle` 用 defer 报终态）
- Modify: `verify_task.go`（`VerifyTask` 加 `JournalPrefix string`、`PageSeq int32`）
- Test: `journal_test.go`（复用 `FakeS3`，模式参考 `drain_test.go` / 现有 `TestListerHardAbortMidPageRecordsCurrentPageToken`）

**Interfaces:**
- Consumes: tracker API（Task 2）。
- Produces: `func (l *Lister) SetJournal(j *JournalTracker)`、`func (c *Checker) SetJournal(j *JournalTracker)`；`VerifyTask.JournalPrefix/PageSeq`。

接线规则（spec §4，顺序是正确性的核心）：

1. `processPrefix` 中 `ListPage` 成功后、**对象循环之前**：`seq := journal.RegisterPage(prefix, len(objs), next)`，然后对本页返回的每个 sub-prefix `AddDiscovered`（必须先于任何对象可能完成）；
2. 对象循环里：key 含 `|` 的短路对象 → `WriteInvalidKey` + `Terminal(prefix, seq)`（它们不进 objCh）；正常对象 → task 带 `{JournalPrefix: prefix, PageSeq: seq}`；
3. list-only 分支（is_check=false）：本地分类后 `Terminal(prefix, seq)`；
4. `next == ""` → `MarkPaginationDone(prefix)`；
5. `Seed`/`SeedWithToken` 内：journal 非空时 `RecordSeed(prefix, token)`；
6. `Checker.Handle` 开头：`if c.journal != nil { defer c.journal.Terminal(task.JournalPrefix, task.PageSeq) }`——defer 在函数体（结果行已推 channel）之后执行，这正是"完成 = 结果行已推进 channel"的实现形态；
7. `ListPage` 出错路径**不登记该页**（位置冻结在失败页之前，恢复自然重试——spec §8）。

- [ ] **Step 1: 写失败测试**

```go
func TestListerJournalRegistersPages(t *testing.T)
// FakeS3 两页各 2 对象 → 全部处理 → tracker cursor = 页1 token（页2 next=="" → done）
func TestListerJournalHardAbortFreezesPosition(t *testing.T)
// 硬停打断半页（仿 TestListerHardAbortMidPageRecordsCurrentPageToken 的场景搭法）→
// 该页 outstanding>0 → cursor 冻结在前一页 token —— Review Focus #1 的管线级验证
func TestListerJournalShortCircuitKeysTerminal(t *testing.T)
// 页内含 | 的 key → 页仍能完成（短路对象即时 Terminal）
func TestListerJournalDiscoversBeforeEmission(t *testing.T)
// 语义上由实现顺序保证；测试用"快照在页完成时必含该页发现的子前缀"间接钉住
func TestCheckerJournalTerminalAfterHandle(t *testing.T)
// Checker.Handle 单任务 → tracker 该页 outstanding 归零
func TestListerJournalListOnlyTerminals(t *testing.T)
// is_check=false：lister 分类后页完成（无 checker 参与）
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestListerJournal|TestCheckerJournal' -count=1 .` Expected: 编译失败。

- [ ] **Step 3: 实现接线（上述 7 条规则）**

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `go test -race -run 'TestListerJournal|TestCheckerJournal' -count=1 .` → PASS；
Run: `go test -race -count=1 .` → 全部 PASS（现有测试零回归，`VerifyTask` 加字段是纯增量）。

- [ ] **Step 5: Commit**

```bash
git add lister.go checker.go verify_task.go journal_test.go
git commit -m "feat: wire journal accounting into lister/checker (terminal-after-push, frozen on hard abort)"
```

### Task 6: -resume-journal 入口与 run() 装配

**Files:**
- Modify: `main.go`（flag、校验、`run()` 签名 +1 参数、种子/装配/checkpoint 生命周期）
- Test: `journal_test.go`（种子函数与装配逻辑单元级）+ `main_test.go`（`run()` 调用点更新）

**Interfaces:**
- Consumes: `ReplayJournal`（Task 1）、tracker/checkpoint（Task 2/4/5）。
- Produces:

```go
// states → BFS 种子；返回是否有东西被 seed（决定 q.Close 与否）
func seedFromJournal(states map[string]journalState, lister *Lister) bool
// done 跳过；Token != "" → SeedWithToken；否则裸 Seed

// 读 + 校验 journal：文件缺失/零条目 → error（提示"死在首次 checkpoint 前，请用原命令重跑"）
func loadJournalForResume(path string) (map[string]journalState, error)
```

main()/run() 装配规则：

1. flag `-resume-journal <path>`；与其他四个文件 flag 互斥（扩展现有 fileFlags 计数）；`list_type != 2` 报错；`-prefix` 非空报错（journal 即扫描范围）——校验放 main() 的现有 os.Exit(2) 段；
2. `run(listCtx, hardCtx, cfg, bucket, prefix, startAfter, listFile, checkFile, backupFile, resumeList, resumeJournal, stdout)`——新参数插在 resumeList 后；`main_test.go` 既有调用点补空串；
3. journal 启用条件：`cfg.ListType == 2 && 文件输入 == 无`（覆盖全新扫描 / -resume-list / -resume-journal 三类运行——spec §7）；
4. 种子段：`resumeJournal != ""` → `loadJournalForResume` → `seedFromJournal`（失败走 seedErr 路径，模式同现有 readResumeList 段）；
5. checkpoint 生命周期：tracker 建好后 `go runJournalCheckpoints(jCtx, tr, out, journalPath)`，jCtx 在**全部工作完成后、`out.Close()` 之前** cancel（check 模式在 `checkWg.Wait()` 之后；list-only 在 objCh 排空之后）——cancel 触发最终 checkpoint。

- [ ] **Step 1: 写失败测试**

```go
func TestSeedFromJournal(t *testing.T)
// states: {p1:done} {p2:Token:T} {p3:Discovered} → Queue.Drain() 含 p2\x00T、p3，不含 p1
func TestLoadJournalForResumeEmpty(t *testing.T) // 空 journal → error
func TestLoadJournalForResumeMissing(t *testing.T) // 缺文件 → error
func TestJournalResumeRoundTrip(t *testing.T) // ★ capstone：模拟 kill
// FakeS3 + lister/checker 跑到一半 cancel（不调 Close，不优雅收尾）→ 手动强制一次 checkpoint →
// ReplayJournal(该 journal.txt) → 断言状态表 = 未完成部分的准确快照（done 的已 done，
// 在途的有游标，未启动的仅 discovered）→ seedFromJournal 的种子集合 == 未覆盖余量
func TestJournalEnabledOnlyMode2S3List(t *testing.T)
// list_type=1 / -check-file 输入 → journal 不创建（journal.txt 不存在）
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestSeedFromJournal|TestLoadJournal|TestJournalResume|TestJournalEnabled' -count=1 .` Expected: 编译失败。

- [ ] **Step 3: 实现装配（上述 5 条规则）**

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `go test -race -run 'TestSeedFromJournal|TestLoadJournal|TestJournalResume|TestJournalEnabled' -count=1 .` → PASS；
Run: `go test -race -count=1 .` → 全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add main.go journal_test.go main_test.go
git commit -m "feat: -resume-journal entry point (run() +1 param: journal-based kill-9 resume for Mode 2)"
```

### Task 7: AGENTS.md 文档与 §7.5 冒烟 runbook

**Files:**
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: 全部前序任务的行为。
- Produces: 文档一致性（无代码）。

- [ ] **Step 1: 更新四节**

1. §4.20：kill -9 bullet 改为"journal 兜底（Mode 2），损失 ≤ checkpoint 间隔；恢复入口 -resume-journal"；补充顺序不变量一段（快照→flush→写盘、discovered 先行、页计数整页登记不随发射数变化）；
2. §5 flags 表加 `-resume-journal` 行（互斥、仅 list_type=2、-prefix 必须为空）；
3. §6 输出文件表加 `journal.txt` 行（三行格式、append-only、重放取最后一条、含 `|` 前缀的解析规则）；
4. 新增 §7.5 runbook「kill -9 + -resume-journal 冒烟」：复用 §7.1 环境，后台跑全量扫描、`kill -9 $PID`、断言 journal.txt 存在且可重放、`-resume-journal` 续跑后两轮 `list_all` 之和 == 对象总数（对齐 §7.3 的闭环断言风格）。真机 minio 环境不在本机，写成可执行步骤不实际运行。

- [ ] **Step 2: 验证**

Run: `go build ./...` → OK（文档任务，构建无回归）；人工核对 §5/§6 表格与代码 flag/文件名一致。

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs: journal checkpoint invariant in AGENTS §4.20, -resume-journal flag, §7.5 kill-9 smoke runbook"
```

---

## Self-Review 记录

- **Spec 覆盖**：spec §3→Task 1；§4→Task 2/5；§5→Task 4；§6 表→各任务的错误路径测试；§7→Task 6；§10 全部条目分入对应任务；§8 的 AGENTS 更新→Task 7。spec §5"哨兵"措辞与 Task 3 的 ctrl+排干实现等价（同一 FIFO 保证），已在 Task 3 注明。
- **与 spec 的一处偏差（已定）**：spec §7"journal 为空 → 退化为普通全量重跑"改为**报错并提示用原命令重跑**——因为 Task 4 的"启动立即首检"使空 journal 只剩"用错文件/手工清空"两种成因，静默跑一次空扫描比报错更危险。需在交付时向用户说明。
- **类型一致性**：journalEvent/journalState/journalSnapshot/journalCursorEntry/JournalTracker 方法名在各任务间已交叉核对；`runJournalCheckpoints` 签名在 Task 4/6 一致；`run()` 第 12 参数名 `resumeJournal` 与 flag 一致。
- **Review Focus**：5 条全部有归属测试（见各 Task Step 1）。
