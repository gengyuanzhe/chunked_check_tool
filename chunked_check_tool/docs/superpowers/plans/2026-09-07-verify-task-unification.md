# VerifyTask 统一校验内核 + list-file 输入源 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 normal/固定分段/显式 offset 列表三种多段校验折叠为单一 `verify()` 内核，并新增 `-list-file` 输入源按行解析 `bkt|key|partcnt|offset0|...`。

**Architecture:** 引入 `VerifyTask` 结构作为校验内核的统一输入；`objCh` 从 `chan ObjectInfo` 改为 `chan VerifyTask`；新增 `InputSource` 接口与 `listFileSource` 实现，预留 etag 源扩展位。S3 列举路径（Mode 1/2/3）保持 inline 在 `main.go`，仅改为产出 `VerifyTask`。

**Tech Stack:** Go 1.x, minio-go v7, 现有测试用 `FakeS3`/`scriptedS3`/`countingS3` fakes。

**Spec:** `docs/superpowers/specs/2026-09-07-verify-task-unification-design.md`

## Global Constraints

- 不改 `S3API` 接口、`S3Client`、`NodePool`、`Output` 公共方法签名。
- 不改 `config.go`/`config.yaml` 字段（`-list-file` 是 CLI flag，不进 yaml）。
- `is_multipart_segment_check`/`multipart_segment_size` 配置语义不变，仅作用于 S3 列举源。
- 性能：normal 路径 `Offsets` 共享包级 `[1]int64{0}`，零分配。
- 测试：每个 task 含 TDD 步骤（写失败测试 → 验证失败 → 实现 → 验证通过 → 提交）。
- 提交粒度：每 task 至少一个提交，提交信息用 `feat:`/`refactor:`/`test:` 前缀。

---

### Task 1: VerifyTask 类型 + resolveOffsets + 共享 normalOffsets

**Files:**
- Create: `verify_task.go`
- Create: `verify_task_test.go`

**Interfaces:**
- Produces: `VerifyTask` struct（字段：`Key, OwnerID, ETag string; Size int64; IsMultipart bool; Offsets []int64`）；包级 `normalOffsets = [1]int64{0}`；`resolveOffsets(obj ObjectInfo, cfg *Config) VerifyTask`
- Consumes: `ObjectInfo`（来自 `s3client.go`，已存在）、`Config`（已存在）、`isNormalETag`（来自 `checker.go`，已存在）

- [ ] **Step 1: Write the failing test**

```go
// verify_task_test.go
package main

import (
	"reflect"
	"testing"
)

func TestResolveOffsets(t *testing.T) {
	const seg = int64(5 * 1024 * 1024)
	cases := []struct {
		name string
		obj  ObjectInfo
		cfg  *Config
		want VerifyTask
	}{
		{
			name: "normal etag single offset zero",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef", Size: 100, OwnerID: "o"},
			cfg:  &Config{},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef", Size: 100, IsMultipart: false, Offsets: []int64{0}},
		},
		{
			name: "multipart segcheck on builds ceil size/seg offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 10 * 1024 * 1024, IsMultipart: true, Offsets: []int64{0, seg}},
		},
		{
			name: "multipart segcheck on size not multiple of seg",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-3", Size: 12*1024*1024 + 1, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-3", Size: 12*1024*1024 + 1, IsMultipart: true, Offsets: []int64{0, seg, 2 * seg, 3 * seg}},
		},
		{
			name: "multipart segcheck off nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: false, MultipartSegmentSize: 0},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 100, IsMultipart: true, Offsets: nil},
		},
		{
			name: "multipart segcheck on but size zero nil offsets",
			obj:  ObjectInfo{Key: "k", ETag: "0123456789abcdef0123456789abcdef-2", Size: 0, OwnerID: "o"},
			cfg:  &Config{IsMultipartSegmentCheck: true, MultipartSegmentSize: seg},
			want: VerifyTask{Key: "k", OwnerID: "o", ETag: "0123456789abcdef0123456789abcdef-2", Size: 0, IsMultipart: true, Offsets: nil},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveOffsets(c.obj, c.cfg)
			// For the normal case, check that Offsets shares the package-level
			// normalOffsets array (zero-allocation invariant).
			if c.name == "normal etag single offset zero" {
				if &got.Offsets[0] != &normalOffsets[0] {
					t.Errorf("normal Offsets does not share normalOffsets (got cap=%d len=%d)", cap(got.Offsets), len(got.Offsets))
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("resolveOffsets mismatch\ngot:  %+v\nwant: %+v", got, c.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestResolveOffsets -v .`
Expected: FAIL with "undefined: resolveOffsets" / "undefined: VerifyTask" / "undefined: normalOffsets".

- [ ] **Step 3: Write minimal implementation**

```go
// verify_task.go
package main

// VerifyTask is the unit of verification work that flows through objCh. The
// checker's verify() method probes 128 bytes at each offset in Offsets and
// routes the result based on IsMultipart. Offsets==nil means "do not probe"
// (multipart with segment check disabled → write to mp_all without claiming
// ok_mp). ETag/Size may be empty/zero for list-file-sourced tasks.
type VerifyTask struct {
	Key         string
	OwnerID     string
	ETag        string
	Size        int64
	IsMultipart bool
	Offsets     []int64
}

// normalOffsets is the shared slice used by resolveOffsets for normal ETag
// objects. verify() only reads Offsets, never writes — sharing is safe and
// avoids a per-object allocation on the hot normal path.
var normalOffsets = [1]int64{0}

// resolveOffsets builds a VerifyTask from an S3-listed object. Three cases:
//   - normal ETag → IsMultipart=false, Offsets shares normalOffsets (=[0])
//   - multipart ETag + segcheck on + Size>0 → IsMultipart=true, Offsets =
//     [0, seg, 2*seg, ...] ceil(Size/seg) entries
//   - multipart ETag + segcheck off (or Size==0) → IsMultipart=true, Offsets=nil
func resolveOffsets(obj ObjectInfo, cfg *Config) VerifyTask {
	if isNormalETag(obj.ETag) {
		return VerifyTask{
			Key:         obj.Key,
			OwnerID:     obj.OwnerID,
			ETag:        obj.ETag,
			Size:        obj.Size,
			IsMultipart: false,
			Offsets:     normalOffsets[:],
		}
	}
	task := VerifyTask{
		Key:         obj.Key,
		OwnerID:     obj.OwnerID,
		ETag:        obj.ETag,
		Size:        obj.Size,
		IsMultipart: true,
	}
	if cfg.IsMultipartSegmentCheck && cfg.MultipartSegmentSize > 0 && obj.Size > 0 {
		seg := cfg.MultipartSegmentSize
		numSegs := (obj.Size + seg - 1) / seg
		offs := make([]int64, numSegs)
		for i := int64(0); i < numSegs; i++ {
			offs[i] = i * seg
		}
		task.Offsets = offs
	}
	return task
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -run TestResolveOffsets -v .`
Expected: PASS — all 5 subtests green.

- [ ] **Step 5: Commit**

```bash
git add verify_task.go verify_task_test.go
git commit -m "feat: add VerifyTask type and resolveOffsets for unified verify kernel"
```

---

### Task 2: Refactor Checker — Handle(VerifyTask) + unified verify()

**Files:**
- Modify: `checker.go` (rewrite `Handle`, remove `checkMultipartSegments`, add `verify`)
- Modify: `checker_test.go` (update all `Handle(ObjectInfo{...})` → `Handle(VerifyTask{...})`)

**Interfaces:**
- Consumes: `VerifyTask`, `normalOffsets` from Task 1; existing `Output`/`Stats`/`S3API`/`chunkSigRe`/`extract*` helpers
- Produces: `Checker.Handle(task VerifyTask)` (new signature), `Checker.verify(task VerifyTask)` (new private method). Listed-counter bumps (`IncrListedObject`/`IncrListedMp`) move OUT of Handle to Lister/walker (Task 3).

**Behavior change:** listed-counter bumps move from `Handle` to the S3 lister (check mode). List-file source does NOT bump them → summary shows `list_all: 0` in list-file mode (per spec §6). The `size==0` normal-object shortcut stays in Handle (RangeGet on empty body returns 416 → would misclassify as check_failed).

- [ ] **Step 1: Update checker_test.go to use VerifyTask inputs**

Rewrite the test bodies that call `c.Handle(ObjectInfo{...})`. New pattern: construct `VerifyTask` directly (bypass `resolveOffsets`) to assert verify's routing. Drop assertions on `ListedMp`/`ListedObjects` from Handle tests (those counters now belong to the lister).

Replace each `c.Handle(ObjectInfo{...})` call site in `checker_test.go`:

```go
// TestCheckerHandleNormal
c.Handle(VerifyTask{Key: "k", OwnerID: "", ETag: "0123456789abcdef0123456789abcdef", Size: 1, IsMultipart: false, Offsets: normalOffsets[:]})

// TestCheckerHandleCorrupted
c.Handle(VerifyTask{Key: "k", IsMultipart: false, Offsets: normalOffsets[:]})

// TestCheckerHandleMultipartSkipsRangeGet — drop the list_mp assertion
// (that counter now lives in the lister). Keep the RangeGet-not-called assertion.
c.Handle(VerifyTask{Key: "k", IsMultipart: true, Offsets: nil})

// TestCheckerHandleRangeGetError
c.Handle(VerifyTask{Key: "k", IsMultipart: false, Offsets: normalOffsets[:]})

// TestCheckerHandleEmptyObjectSkipsRangeGet — size=0 normal shortcut
c.Handle(VerifyTask{Key: "k", Size: 0, IsMultipart: false, Offsets: normalOffsets[:]})

// TestCheckerHandleCheckFailedLogsStructured
c.Handle(VerifyTask{Key: "path/obj", IsMultipart: false, Offsets: normalOffsets[:]})

// TestCheckerMultipartSegmentCheckCorrupted
c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})

// TestCheckerMultipartSegmentCheckClean
c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})

// TestCheckerMultipartSegmentCheckRangeError
c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})

// TestCheckerMultipartSegmentCheckDisabled — drop the list_mp assertion.
c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: nil})

// TestCheckerMultipartSegmentCheckSecondSegmentMatches
c.Handle(VerifyTask{Key: "k", OwnerID: "owner-A", IsMultipart: true, Offsets: []int64{0, 5 * 1024 * 1024}})
```

Drop `TestCheckerHandleMultipartSkipsRangeGet`'s `s.Snapshot().ListedMp` assertion (lines 98-100 in current file). Drop `TestCheckerMultipartSegmentCheckDisabled`'s `ListedMp` assertion (lines 319-321). Both move to lister tests in Task 3.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run 'TestChecker' -v .`
Expected: FAIL — `Handle` still takes `ObjectInfo`, compiler rejects `VerifyTask` arg.

- [ ] **Step 3: Rewrite checker.go**

Replace the entire `Handle` method and delete `checkMultipartSegments`. Add `verify`. The new `checker.go` body (top portion unchanged from line 1 to line 72 — `chunkSigRe`, `extractHTTPStatusCode`, `extractS3Code`, `extractRequestID`, `isNormalETag` all stay):

```go
// Checker classifies a single VerifyTask: routes to corrupted/ok/failed
// outputs based on a 128-byte RangeGet at each offset in task.Offsets.
type Checker struct {
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

func NewChecker(worker S3API, out *Output, stats *Stats, cfg *Config) *Checker {
	return &Checker{worker: worker, out: out, stats: stats, cfg: cfg}
}

// Handle routes task to verify. The size==0 normal-object shortcut stays
// here (RangeGet on an empty body returns 416 → would misclassify as
// check_failed). Listed-counter bumps (list_obj/list_mp) are NOT done here —
// the S3 lister bumps them in check mode before pushing the task. List-file
// source does not bump them, so summary shows list_all: 0 in list-file mode.
func (c *Checker) Handle(task VerifyTask) {
	if !task.IsMultipart && task.Size == 0 {
		c.stats.IncrOkObjects()
		if c.cfg.IsSuccessLog {
			c.out.WriteSuccess(task.OwnerID, task.Key)
		}
		return
	}
	c.verify(task)
}

// verify probes 128 bytes at each offset in task.Offsets. If any probe
// matches chunkSigRe, the task is corrupted. If any probe's RangeGet errors,
// the task is check_failed (or mp_check_failed when IsMultipart). If
// task.Offsets is nil/empty, the task is recorded as multipart_all without
// an ok_mp claim (the "segment check disabled" path). Otherwise all probes
// clean → ok_object / ok_mp.
func (c *Checker) verify(task VerifyTask) {
	if len(task.Offsets) == 0 {
		c.out.WriteMultipartAll(task.OwnerID, task.Key)
		return
	}
	for _, off := range task.Offsets {
		body, err := c.worker.RangeGetAt(context.Background(), task.Key, off, 128)
		if err != nil {
			if task.IsMultipart {
				c.out.WriteMpCheckFailed(task.Key)
				c.out.WriteMpCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				c.stats.IncrMpCheckFailed()
			} else {
				c.out.WriteCheckFailed(task.Key)
				c.out.WriteCheckFailedLog(task.Key, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
				c.stats.IncrCheckFailed()
			}
			return
		}
		if chunkSigRe.Match(body) {
			if task.IsMultipart {
				c.out.WriteCorruptedMultipart(task.OwnerID, task.Key)
				c.stats.IncrCorruptedMp()
			} else {
				c.out.WriteCorrupted(task.OwnerID, task.Key)
				c.stats.IncrCorruptedObjects()
			}
			return
		}
	}
	if task.IsMultipart {
		if c.cfg.IsMultipartSuccessLog {
			c.out.WriteMultipartOk(task.OwnerID, task.Key)
		}
		c.stats.IncrOkMp()
	} else {
		if c.cfg.IsSuccessLog {
			c.out.WriteSuccess(task.OwnerID, task.Key)
		}
		c.stats.IncrOkObjects()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run 'TestChecker' -v .`
Expected: PASS — all checker tests green with new signatures.

- [ ] **Step 5: Run full test suite to catch downstream breakage**

Run: `go test ./...`
Expected: FAIL in `lister_test.go` / `walker_test.go` / `main_test.go` — they push `ObjectInfo` to objCh but objCh is still `chan ObjectInfo`. Those breakages are fixed in Task 3. Do NOT fix them here.

- [ ] **Step 6: Commit**

```bash
git add checker.go checker_test.go
git commit -m "refactor: fold normal/multipart check paths into unified verify(VerifyTask)"
```

---

### Task 3: Lister/walker produce VerifyTask + bump listed in check mode

**Files:**
- Modify: `lister.go` (objCh type, resolveOffsets call, listed-counter bumps in check mode)
- Modify: `walker.go` (same)
- Modify: `lister_test.go` (objCh type change, assertions adapt)
- Modify: `walker_test.go` (same)

**Interfaces:**
- Consumes: `VerifyTask`, `resolveOffsets` from Task 1; `Checker.Handle(VerifyTask)` from Task 2
- Produces: `Lister.Run` and `runRecursiveWalk` now take `chan<- VerifyTask`; bump `IncrListedObject`/`IncrListedMp` in check mode before pushing (previously done by `Checker.Handle`)

**Behavior preservation:** S3 list path's listed counters (`list_obj`/`list_mp`) still bump exactly once per object — location moves from `Checker.Handle` to `Lister.processPrefix`/`runRecursiveWalk` (check mode branch). List-only mode bumps are unchanged (already in lister/walker).

- [ ] **Step 1: Update lister_test.go objCh type**

Change `objCh := make(chan ObjectInfo, ...)` to `objCh := make(chan VerifyTask, ...)` and the range loop to read `VerifyTask`:

```go
// In TestListerMode2BFSPrefixes and TestListerMode2CommonPrefixesOnlyPage:
objCh := make(chan VerifyTask, 8)  // was: chan ObjectInfo
...
got := []string{}
for t := range objCh {
    got = append(got, t.Key)  // was: o.Key
}
```

Also add assertions that listed counters bump in check mode (since they now live in the lister). In `TestListerMode2BFSPrefixes`, after the range loop, add:

```go
// Check mode → lister bumps listed_obj before pushing. 2 normal objects.
if got := stats.Snapshot().ListedObjects; got != 2 {
    t.Errorf("list_obj=%d want 2 (lister bumps in check mode)", got)
}
```

(In `TestListerMode2CommonPrefixesOnlyPage`, both objects have normal ETags → expect `ListedObjects == 3`.)

- [ ] **Step 2: Update walker_test.go objCh type**

Same pattern: `make(chan VerifyTask, ...)` and `for t := range objCh { ... t.Key }`. The walker's listed-counter bump in check mode is verified in `TestWalkerHappyPath`:

```go
// After the existing ListFailed==0 assertion:
if got := stats.Snapshot().ListedObjects; got != int64(len(want)) {
    t.Errorf("list_obj=%d want %d (walker bumps in check mode)", got, len(want))
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test -run 'TestLister|TestWalker' -v .`
Expected: FAIL — compiler rejects `chan ObjectInfo` vs `chan VerifyTask` mismatch in lister.go/walker.go signatures.

- [ ] **Step 4: Update lister.go**

Change `Run` signature and `processPrefix` to push `VerifyTask`. Add listed-counter bumps in check mode (move from old `Checker.Handle`):

```go
// In Run signature: objCh chan<- ObjectInfo → objCh chan<- VerifyTask
func (l *Lister) Run(ctx context.Context, wg *sync.WaitGroup, objCh chan<- VerifyTask, workerIdx int, s3 S3API, onObject func()) {

// In processPrefix signature: same change
func (l *Lister) processPrefix(ctx context.Context, prefix string, objCh chan<- VerifyTask, s3 S3API, onObject func()) {
```

In `processPrefix`, replace the loop body:

```go
for _, o := range objs {
    task := resolveOffsets(o, l.cfg)
    if l.cfg.IsCheck {
        // Check mode: lister bumps listed counters (moved from Checker.Handle).
        // list-file source does not go through this path, so its summary
        // shows list_all: 0 — see listFileSource.
        if task.IsMultipart {
            l.stats.IncrListedMp()
        } else {
            l.stats.IncrListedObject()
        }
        select {
        case objCh <- task:
        case <-ctx.Done():
            return
        }
    } else {
        // list-only mode: same classification, no check performed.
        if task.IsMultipart {
            l.stats.IncrListedMp()
        } else {
            l.stats.IncrListedObject()
        }
    }
    if onObject != nil {
        onObject()
    }
}
```

- [ ] **Step 5: Update walker.go**

Same changes. `runRecursiveWalk` signature: `objCh chan<- ObjectInfo` → `objCh chan<- VerifyTask`. In the per-object loop:

```go
for _, o := range objs {
    task := resolveOffsets(o, cfg)
    if cfg.IsCheck {
        if task.IsMultipart {
            stats.IncrListedMp()
        } else {
            stats.IncrListedObject()
        }
        select {
        case objCh <- task:
        case <-ctx.Done():
            return
        }
    } else {
        if task.IsMultipart {
            stats.IncrListedMp()
        } else {
            stats.IncrListedObject()
        }
    }
    if onObject != nil {
        onObject()
    }
}
```

- [ ] **Step 6: Run lister/walker tests to verify they pass**

Run: `go test -run 'TestLister|TestWalker' -v .`
Expected: PASS — objCh type matches, listed counters bump in check mode.

- [ ] **Step 7: Run full suite (main.go still broken — fixed in Task 6)**

Run: `go test ./...`
Expected: FAIL in `main_test.go` or compile error in `main.go` (objCh type). Fixed in Task 6.

- [ ] **Step 8: Commit**

```bash
git add lister.go walker.go lister_test.go walker_test.go
git commit -m "refactor: lister/walker produce VerifyTask; move listed-counter bumps from checker to lister"
```

---

### Task 4: list-file line parser (pure function)

**Files:**
- Create: `list_file_source.go` (only the parser function + the `InputSource` interface declaration for now)
- Create: `list_file_source_test.go`

**Interfaces:**
- Produces: `InputSource` interface; `parseListFileLine(line string, expectedBucket string, lineNum int) (VerifyTask, error)` — private function; `MalformedLineError` error type carrying line/lineNum/reason for caller to log.
- Consumes: `VerifyTask` from Task 1

**Line format:** `bkt|key|partcnt|offset0|offset1|...` — `partcnt` offsets total, `offset0` must be 0, offsets strictly increasing, all ≥ 0, `bkt == expectedBucket`.

- [ ] **Step 1: Write the failing test (table-driven)**

```go
// list_file_source_test.go
package main

import (
	"errors"
	"strings"
	"testing"
)

func TestParseListFileLine(t *testing.T) {
	const bkt = "mybucket"
	cases := []struct {
		name     string
		line     string
		wantKey  string
		wantOffs []int64
		wantErr  string // empty means no error; substring match on err.Error()
	}{
		{
			name:     "single part",
			line:     "mybucket|k1|1|0",
			wantKey:  "k1",
			wantOffs: []int64{0},
		},
		{
			name:     "three parts increasing",
			line:     "mybucket|path/obj|3|0|5242880|10485760",
			wantKey:  "path/obj",
			wantOffs: []int64{0, 5242880, 10485760},
		},
		{
			name:    "bucket mismatch",
			line:    "other|k|1|0",
			wantErr: "bucket mismatch",
		},
		{
			name:    "too few fields",
			line:    "mybucket|k",
			wantErr: "too few fields",
		},
		{
			name:    "partcnt not integer",
			line:    "mybucket|k|x|0",
			wantErr: "partcnt not an integer",
		},
		{
			name:    "partcnt zero",
			line:    "mybucket|k|0",
			wantErr: "partcnt must be >= 1",
		},
		{
			name:    "offset count mismatch",
			line:    "mybucket|k|3|0|5242880",
			wantErr: "offset count != partcnt",
		},
		{
			name:    "offset0 not zero",
			line:    "mybucket|k|1|100",
			wantErr: "offset0 must be 0",
		},
		{
			name:    "offset not integer",
			line:    "mybucket|k|2|0|abc",
			wantErr: "offset not an integer",
		},
		{
			name:    "offsets not strictly increasing",
			line:    "mybucket|k|3|0|5242880|5242880",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "offsets decreasing",
			line:    "mybucket|k|3|0|5242880|100",
			wantErr: "offsets must be strictly increasing",
		},
		{
			name:    "negative offset",
			line:    "mybucket|k|2|0|-5",
			wantErr: "offset negative",
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: "empty line",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseListFileLine(c.line, bkt, 1)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			var mle *MalformedLineError
			if !errors.As(err, &mle) {
				t.Fatalf("expected *MalformedLineError, got %T: %v", err, err)
			}
			if !strings.Contains(mle.Reason, c.wantErr) {
				t.Errorf("err reason = %q, want substring %q", mle.Reason, c.wantErr)
			}
		})
	}
}

func TestParseListFileLineSuccessFields(t *testing.T) {
	task, err := parseListFileLine("mybucket|path/obj|3|0|5242880|10485760", "mybucket", 42)
	if err != nil {
		t.Fatal(err)
	}
	if task.Key != "path/obj" {
		t.Errorf("Key=%q want path/obj", task.Key)
	}
	if !task.IsMultipart {
		t.Errorf("IsMultipart=false, want true (list-file is always multipart)")
	}
	if len(task.Offsets) != 3 {
		t.Errorf("len(Offsets)=%d want 3", len(task.Offsets))
	}
	if task.Offsets[2] != 10485760 {
		t.Errorf("Offsets[2]=%d want 10485760", task.Offsets[2])
	}
	// ETag/Size empty for list-file tasks (caller doesn't know them).
	if task.ETag != "" || task.Size != 0 {
		t.Errorf("ETag=%q Size=%d, want empty/0", task.ETag, task.Size)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestParseListFileLine -v .`
Expected: FAIL — `undefined: parseListFileLine`, `undefined: MalformedLineError`, `undefined: InputSource`.

- [ ] **Step 3: Write minimal implementation**

```go
// list_file_source.go
package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// InputSource produces VerifyTasks and pushes them to objCh until EOF or
// context cancellation. Reserved for future extension (e.g. etag-based
// offset extraction). The S3 list path (Mode 1/2/3) is currently inline in
// main.go; listFileSource is the first InputSource implementation.
type InputSource interface {
	Run(ctx context.Context, objCh chan<- VerifyTask) error
}

// MalformedLineError carries the original line and its 1-based line number
// so the caller can write it to list_failed for resumable debugging.
type MalformedLineError struct {
	Line   string
	LineNum int
	Reason string
}

func (e *MalformedLineError) Error() string {
	return fmt.Sprintf("line %d: %s: %q", e.LineNum, e.Reason, e.Line)
}

// parseListFileLine parses one line of the list file.
//
// Format: bkt|key|partcnt|offset0|offset1|...
//
// Semantic rules (per spec §3.2):
//   - bkt must equal expectedBucket
//   - partcnt >= 1 and integer
//   - exactly partcnt offsets follow
//   - all offsets >= 0, strictly increasing
//   - offset0 must be 0
//
// On success returns a VerifyTask with IsMultipart=true (list-file tasks are
// always multipart), ETag="" and Size=0 (the file does not carry them).
func parseListFileLine(line string, expectedBucket string, lineNum int) (VerifyTask, error) {
	if strings.TrimSpace(line) == "" {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "empty line"}
	}
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "too few fields (want bkt|key|partcnt|offsets...)"}
	}
	bkt, key, partcntStr := parts[0], parts[1], parts[2]
	if bkt != expectedBucket {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("bucket mismatch: got %q want %q", bkt, expectedBucket)}
	}
	partcnt, err := strconv.Atoi(partcntStr)
	if err != nil {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("partcnt not an integer: %q", partcntStr)}
	}
	if partcnt < 1 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("partcnt must be >= 1, got %d", partcnt)}
	}
	if len(parts) != 3+partcnt {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset count != partcnt: got %d offsets, partcnt=%d", len(parts)-3, partcnt)}
	}
	offs := make([]int64, 0, partcnt)
	for i := 0; i < partcnt; i++ {
		s := parts[3+i]
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset not an integer: %q", s)}
		}
		if v < 0 {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset negative: %d", v)}
		}
		offs = append(offs, v)
	}
	if offs[0] != 0 {
		return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: fmt.Sprintf("offset0 must be 0, got %d", offs[0])}
	}
	for i := 1; i < partcnt; i++ {
		if offs[i] <= offs[i-1] {
			return VerifyTask{}, &MalformedLineError{Line: line, LineNum: lineNum, Reason: "offsets must be strictly increasing"}
		}
	}
	return VerifyTask{
		Key:         key,
		IsMultipart: true,
		Offsets:     offs,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run 'TestParseListFileLine' -v .`
Expected: PASS — all 13 malformed/success subtests green.

- [ ] **Step 5: Commit**

```bash
git add list_file_source.go list_file_source_test.go
git commit -m "feat: add list-file line parser and InputSource interface"
```

---

### Task 5: listFileSource — Run method, end-to-end

**Files:**
- Modify: `list_file_source.go` (add `listFileSource` struct + `Run`)
- Modify: `list_file_source_test.go` (add end-to-end test)

**Interfaces:**
- Produces: `listFileSource` struct implementing `InputSource`; `newListFileSource(path, bucket string, out *Output, stats *Stats) *listFileSource`
- Consumes: `parseListFileLine`, `MalformedLineError` from Task 4; `Output.WriteListFailed`/`WriteListFailedLog`, `Stats.IncrListFailed`

- [ ] **Step 1: Write the failing test (end-to-end)**

```go
// Append to list_file_source_test.go
func TestListFileSourceRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, err := NewOutput(cfg, "mybucket")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	stats := NewStats()

	// File: 2 valid lines + 1 malformed (offset0 != 0) + 1 valid.
	content := "mybucket|k1|1|0\n" +
		"mybucket|k2|3|0|5242880|10485760\n" +
		"mybucket|bad|1|100\n" +
		"mybucket|k3|2|0|9999\n"
	filePath := filepath.Join(dir, "list.txt")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src := newListFileSource(filePath, "mybucket", out, stats)
	objCh := make(chan VerifyTask, 16)
	go func() {
		if err := src.Run(context.Background(), objCh); err != nil {
			t.Logf("Run returned err: %v", err)
		}
		close(objCh)
	}()

	got := []VerifyTask{}
	for task := range objCh {
		got = append(got, task)
	}
	if len(got) != 3 {
		t.Fatalf("got %d tasks, want 3 (malformed line skipped): %+v", len(got), got)
	}
	if got[0].Key != "k1" || got[1].Key != "k2" || got[2].Key != "k3" {
		t.Errorf("keys in wrong order: %+v", got)
	}
	// list-file source must NOT bump listed counters (per spec §6).
	if got := stats.Snapshot().ListedMp; got != 0 {
		t.Errorf("ListedMp=%d want 0 (list-file source does not bump listed)", got)
	}
	if got := stats.Snapshot().ListedObjects; got != 0 {
		t.Errorf("ListedObjects=%d want 0", got)
	}
	// Malformed line → list_failed + IncrListFailed.
	if got := stats.Snapshot().ListFailed; got != 1 {
		t.Errorf("ListFailed=%d want 1 (one malformed line)", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestListFileSourceRunEndToEnd -v .`
Expected: FAIL — `undefined: newListFileSource`.

- [ ] **Step 3: Implement listFileSource**

Append to `list_file_source.go`:

```go
import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// listFileSource reads a list file line-by-line and pushes VerifyTasks to
// objCh. Malformed lines are written to list_failed (with line number) and
// bump IncrListFailed; processing continues. The source does NOT bump
// listed_obj/listed_mp counters (per spec §6 — list-file mode bypasses S3
// LIST, so "listed" semantics don't apply; summary shows list_all: 0).
type listFileSource struct {
	path   string
	bucket string
	out    *Output
	stats  *Stats
}

func newListFileSource(path, bucket string, out *Output, stats *Stats) *listFileSource {
	return &listFileSource{path: path, bucket: bucket, out: out, stats: stats}
}

func (s *listFileSource) Run(ctx context.Context, objCh chan<- VerifyTask) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open list file %q: %w", s.path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// Allow long lines (default 64KB limit is too small for huge offset lists).
	const maxLineLen = 1 << 20 // 1 MiB
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineLen)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Text()
		task, err := parseListFileLine(line, s.bucket, lineNum)
		if err != nil {
			var mle *MalformedLineError
			if errorsAs(err, &mle) {
				s.out.WriteListFailed(mle.Line)
				s.out.WriteListFailedLog(mle.Line, 0, "", "", err)
				s.stats.IncrListFailed()
				continue
			}
			// Non-malformed error (shouldn't happen for parseListFileLine).
			s.out.WriteListFailed(line)
			s.out.WriteListFailedLog(line, 0, "", "", err)
			s.stats.IncrListFailed()
			continue
		}
		select {
		case objCh <- task:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return scanner.Err()
}
```

Note: `errorsAs` helper — if not present, use stdlib `errors.As` directly. Add `"errors"` to imports and replace `errorsAs(err, &mle)` with `errors.As(err, &mle)`. (The codebase already uses `errors.As` in `s3client.go` line 364 — no need for a wrapper.)

Final imports block for `list_file_source.go`:

```go
import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run 'TestListFile|TestParseListFile' -v .`
Expected: PASS — end-to-end and parser tests green.

- [ ] **Step 5: Commit**

```bash
git add list_file_source.go list_file_source_test.go
git commit -m "feat: listFileSource.Run reads list file, skips malformed lines, pushes VerifyTasks"
```

---

### Task 6: main.go wire-up — -list-file flag + objCh type

**Files:**
- Modify: `main.go` (add `-list-file` flag, objCh type, dispatch, printConfig)
- Modify: `main_test.go` (objCh type in any helper; env-gated smoke test for -list-file)

**Interfaces:**
- Consumes: `listFileSource`/`newListFileSource` from Task 5; `VerifyTask` from Task 1; `Checker.Handle(VerifyTask)` from Task 2
- Produces: `main.run` with new signature accepting `listFile string`; CLI flag `-list-file`

- [ ] **Step 1: Update main.go flag parsing and run() signature**

In `main()`:

```go
listFile := flag.String("list-file", "", "list file path: lines of bkt|key|partcnt|offset0|offset1|... (bypasses S3 listing; requires is_check=true)")
// ... after existing validation:
if *listFile != "" && !cfg.IsCheck {
    fmt.Fprintln(os.Stderr, "usage: -list-file requires -c config with is_check=true")
    os.Exit(2)
}
```

Pass `*listFile` through to `run()`:

```go
if err := run(ctx, cfg, *bucket, *prefix, *startAfter, *listFile, mwOut); err != nil {
    log.Fatalf("run: %v", err)
}
```

- [ ] **Step 2: Update run() signature and objCh type**

```go
func run(ctx context.Context, cfg *Config, bucket, prefix, startAfter, listFile string, stdout io.Writer) error {
    ...
    objCh := make(chan VerifyTask, objChCap)  // was: chan ObjectInfo
    ...
}
```

- [ ] **Step 3: Add list-file dispatch in run()**

After the check-worker spawn block (the `if cfg.IsCheck { ... }` loop that spawns check workers), insert the list-file branch BEFORE the existing `var listWg sync.WaitGroup` / Mode 1/2/3 dispatch:

```go
// List-file source: bypass S3 listing entirely. Read the file line-by-line,
// push VerifyTasks to objCh. No list workers, no queue, no Lister. is_check
// must be true (validated in main).
if listFile != "" {
    listStart := time.Now()
    src := newListFileSource(listFile, bucket, out, stats)
    go func() {
        if err := src.Run(ctx, objCh); err != nil {
            log.Printf("list-file source: %v", err)
        }
        close(objCh)
        stats.SetListDuration(time.Since(listStart))
    }()
    checkWg.Wait()
    if err := out.Close(); err != nil {
        log.Printf("output close: %v", err)
    }
    stats.SetTotalDuration(time.Since(start))
    stats.PrintSummary(stdout, cfg.IsCheck)
    return nil
}
```

The existing Mode 1/2/3 dispatch below stays unchanged (it runs only when `listFile == ""`).

- [ ] **Step 4: Update printConfig to include list-file**

Add a line after `nextmarker`:

```go
fmt.Fprintf(w, "    list_file: %s\n", orEmpty(listFilePath))
```

And pass `listFile` to `printConfig` as a new parameter (update the call site in `main()` and the function signature). For the `listFile == ""` case, `orEmpty` already prints "(none)".

- [ ] **Step 5: Update main_test.go objCh type (if any)**

`TestRunIntegration_FakeS3` and `TestRunEndToEnd_smoke` don't construct objCh directly — they call `run()`. Update `TestRunEndToEnd_smoke` call to pass empty listFile:

```go
if err := run(ctx, cfg, os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX"), "", "", &buf); err != nil {
```

(The 6th arg `""` is the new `listFile` parameter.)

- [ ] **Step 6: Run full test suite to verify everything passes**

Run: `go test ./...`
Expected: PASS — all tests green.

- [ ] **Step 7: Build the binary**

Run: `go build -o chunked_check_tool .`
Expected: builds without errors.

- [ ] **Step 8: Manual smoke — list-file path against a fake file (no S3)**

Create a tiny list file with one malformed line and verify `list_failed.txt` is written:

```bash
echo "wrongbucket|k|1|0" > /tmp/list.txt
./chunked_check_tool -c config.yaml -bkt mybucket -list-file /tmp/list.txt
# Expected: list_failed.txt contains "wrongbucket|k|1|0", summary shows list_failed: 1
```

(If no live S3 endpoint is available, this still verifies the parse-failure path since the malformed line is rejected before any GET. Valid lines would fail at GET time against an unreachable endpoint — that's expected without a real S3.)

- [ ] **Step 9: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: add -list-file flag bypassing S3 listing for explicit per-part offsets"
```

---

### Task 7: Update README and config template

**Files:**
- Modify: `README.md`
- Modify: `config.yaml` (comment-only — no new field, but mention -list-file in usage header)

- [ ] **Step 1: Add -list-file section to README**

In the "命令" or "参数" section of README, add:

```markdown
## list-file 模式

`-list-file <path>` 跳过 S3 列举，直接读文件按行校验。每行格式：

    bkt|key|partcnt|offset0|offset1|...

- `bkt` 必须等于 `-bkt`
- `partcnt` 个 offset，`offset0=0`，严格递增
- 坏行（格式错误/bkt 不匹配）写入 `list_failed.txt` 并跳过
- 必须 `is_check=true`（文件即列表，无需列举）
- 此模式下 `list_all` 显示 0（不经过 S3 LIST），`list_failed` 仅统计坏行

固定分段校验（`is_multipart_segment_check=true` + `multipart_segment_size`）是本模式的特殊情况：offsets 由 `[0, seg, 2*seg, ...]` 计算而来，本模式则显式给出。
```

- [ ] **Step 2: Update config.yaml usage header comment**

Top of `config.yaml`:

```yaml
# 用法:
#   ./chunked_check_tool -c config.yaml -bkt <bucket> [-prefix <p>] [-nextmarker <key>]
#   ./chunked_check_tool -c config.yaml -bkt <bucket> -list-file <path>   # 跳过 S3 列举
```

- [ ] **Step 3: Commit**

```bash
git add README.md config.yaml
git commit -m "docs: document -list-file mode and per-part offset format"
```

---

## Self-Review

### 1. Spec coverage

- §2.1 VerifyTask struct — Task 1 ✓
- §2.2 unified verify() — Task 2 ✓
- §2.3 shared normalOffsets — Task 1 (test asserts `&got.Offsets[0] == &normalOffsets[0]`) ✓
- §3.1 InputSource interface — Task 4 (declaration) ✓
- §3.1 s3ListSource — **deviation**: S3 list path stays inline in main.go rather than extracting s3ListSource struct. The interface is still reserved; listFileSource is the sole impl today; future etag source adds a second. Rationale: YAGNI — extraction is a pure refactor with no behavior change, defer until a second non-list-file source actually lands.
- §3.2 list-file line format + validation — Task 4 ✓
- §3.3 is_check=false mutual exclusion — Task 6 Step 1 ✓
- §3.4 list_type ignored when -list-file set — Task 6 Step 3 (list-file branch returns before Mode 1/2/3 dispatch) ✓
- §4 改动文件清单 — covered by Task 1-7 ✓
- §6 output routing table — Task 2 verify() implements all 8 routes ✓
- §6 list-file source does not bump listed — Task 3 (move bumps to lister) + Task 5 (listFileSource doesn't bump) + Task 5 test asserts ListedMp==0 ✓
- §8 etag extension point — Task 4 declares InputSource interface, comment notes reservation ✓

### 2. Placeholder scan

No "TBD", "TODO", "implement later", or hand-wavy "add error handling" in any task. Each code step has full runnable code.

### 3. Type consistency

- `VerifyTask` fields: `Key, OwnerID, ETag string; Size int64; IsMultipart bool; Offsets []int64` — used identically in Tasks 1, 2, 3, 4, 5, 6 ✓
- `resolveOffsets(obj ObjectInfo, cfg *Config) VerifyTask` — Task 1 defines, Tasks 3 calls ✓
- `Checker.Handle(task VerifyTask)` — Task 2 defines, Task 6 (via check worker loop) calls ✓
- `Checker.verify(task VerifyTask)` — Task 2 defines, called by Handle ✓
- `InputSource` interface — Task 4 declares, Task 5 implements via `listFileSource.Run(ctx, objCh chan<- VerifyTask) error` ✓
- `parseListFileLine(line, expectedBucket string, lineNum int) (VerifyTask, error)` — Task 4 defines, Task 5 calls ✓
- `MalformedLineError{Line, LineNum, Reason}` — Task 4 defines, Task 5 reads ✓
- `newListFileSource(path, bucket string, out *Output, stats *Stats) *listFileSource` — Task 5 defines, Task 6 calls ✓
- `run(ctx, cfg, bucket, prefix, startAfter, listFile string, stdout io.Writer) error` — Task 6 defines, main_test.go call updated ✓

All signatures consistent across tasks.
