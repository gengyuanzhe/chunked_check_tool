# delete_marker_cleanup_tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `delete_marker_cleanup_tool` that consumes `delete_marker_list_tool`'s output and provides two modes (check via HEAD, delete via DeleteObject), controlled by a required `-mode` CLI flag.

**Architecture:** Worker pool reads input file line-by-line, each worker bound to an endpoint via `endpoints[i % N]` rotation. mode check classifies each row into three states (marker/not_marker/absent); mode delete succeeds on nil error AND on NoSuchVersion. Output follows lister's writer-goroutine + lazy-failed-file + 3s flush pattern.

**Tech Stack:** Go 1.27, `github.com/minio/minio-go/v7 v7.3.0` (pinned), `gopkg.in/yaml.v3`.

**Spec:** `docs/superpowers/specs/2026-11-12-delete-marker-cleanup-tool-design.md` — the plan argues from the spec; executors read both.

## Global Constraints

- Module `delete_marker_cleanup_tool` in sibling directory `delete_marker_cleanup_tool/`, independent `go.mod` (copy lister's `go.mod`/`go.sum` and rename module line).
- minio-go v7.3.0 pinned — do not upgrade.
- `\x1f` (ASCII Unit Separator) is the row field separator — `bkt\x1fkey\x1fversion_id` (3 fields, lister format) and `bkt\x1fkey\x1fversion_id\x1f<state>` (4 fields, check results).
- version_id literal `"null"` passes through verbatim — never treat as empty.
- HTTP transport: `MaxIdleConnsPerHost: 32`, `IdleConnTimeout: 90s`, `dial_timeout` default 10s, optional `response_header_timeout`, `BucketLookup: minio.BucketLookupPath`, https → `InsecureSkipVerify: true`. (Copy lister's `s3client.go`.)
- `output_dir_timestamp` sibling suffix: `./out` → `./out_YYYYMMDD_HHMMSS` (not `./out/_<stamp>`).
- run.log is the first file opened; `mwOut = io.MultiWriter(os.Stdout, runLog)`, `mwErr = io.MultiWriter(os.Stderr, runLog)`; sk masked as `***`.
- No retry, no checkpoint, no graceful shutdown, no signal handler.
- go binary at `~/sdk/go1.27.1/bin/go`; export PATH for non-interactive shells.
- Tests follow lister's fakes3 (`httptest.Server`) + `t.TempDir()` pattern.

## Review Focus

Five input classes or failure modes the spec implies but no task's tests exercise that are most likely to bite a person using this software:

1. **HEAD on a delete marker returns HTTP 405 + `x-amz-delete-marker: true`** — S3 spec behavior. minio-go v7.3.0 may surface this as `(ObjectInfo{IsDeleteMarker:true}, nil)` OR `(ObjectInfo{}, MethodNotAllowed error)`; test must exercise whichever fires and classify as `marker`. Pinned in Task 6 (mode check classification).
2. **Input line with wrong field count** (e.g., torn line with 2 fields, or 4 fields from re-feeding check_results.txt). Worker must skip + log, not panic. Pinned in Task 7 (worker parse).
3. **`version_id == "null"` literal** passed to `StatObject`/`RemoveObject` — S3 treats this as the null version. Must pass through verbatim, not convert to empty string. Pinned in Task 6 (mode check test includes a `null` version_id row).
4. **mode delete where row's version was already deleted between lister run and cleanup run** — server returns `NoSuchVersion`. Must count as success, not failure. Pinned in Task 8 (mode delete).
5. **Input file is `invalid_keys.txt`** (basename match) — key field is URL-encoded; must `url.QueryUnescape` before HEAD/DELETE. Pinned in Task 7 (worker parse).

---

## File Structure

All files live in `delete_marker_cleanup_tool/`:

| File | Responsibility |
|---|---|
| `go.mod` / `go.sum` | Module `delete_marker_cleanup_tool`, copy from lister |
| `main.go` | flag parsing (`-c`/`-mode`/`-input`), `run` orchestration, run.log open (first), config snapshot (sk mask), BucketExists preflight on first-row bucket, summary print |
| `config.go` | `Config` struct + `LoadConfig` (YAML, defaults, validation, timestamp suffix). Drops `list_type`/`list_concurrency`, adds `op_concurrency` |
| `s3client.go` | `S3Client` (minio.Client wrapper), `StatObject(ctx, bkt, key, versionID) (minio.ObjectInfo, error)`, `DeleteObject(ctx, bkt, key, versionID) error`, `BucketExists`. Copy transport tuning from lister |
| `worker.go` | `Worker` struct + `runWorker(ctx, rowCh, out, stats, progress)`; row parsing (3-field split, URL-decode if from invalid_keys.txt); mode dispatch (check/delete) |
| `classifier.go` | `classifyHead(info minio.ObjectInfo, err error) (state string, ok bool)` — pure function, returns `marker`/`not_marker`/`absent` + `ok=true`, or `""` + `ok=false` for unclassified (failed). Task 6 owns this |
| `output.go` | `Output` (writer goroutines + channel + bufio + lazy failed files + 3s flush). Two result channels (check_results, deleted) + two failed channels (head_failed, delete_failed) + two slog loggers. Open only the channels/files relevant to the mode |
| `stats.go` | atomic counters: mode check → `marker`/`not_marker`/`absent`/`headFailed`; mode delete → `deleted`/`deleteFailed`; plus `processed`, `totalSec`. `PrintSummary(w, mode)` |
| `progress.go` | `ProgressPrinter` (copy from lister, drop `QueueSnapshot` — no BFS queue) |

---

## Task 1: Module scaffolding + go.mod + config

**Files:**
- Create: `delete_marker_cleanup_tool/go.mod` (copy from `delete_marker_list_tool/go.mod`, change module line)
- Create: `delete_marker_cleanup_tool/go.sum` (copy verbatim from lister)
- Create: `delete_marker_cleanup_tool/config.go`
- Create: `delete_marker_cleanup_tool/config_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...` inside `delete_marker_cleanup_tool/`

**Interfaces:**
- Produces: `type Config struct { Endpoints []string; Scheme string; AK, SK string; OpConcurrency int; OutputDir string; OutputDirTimestamp bool; ProgressInterval int; DialTimeout, ResponseHeaderTimeout int64 }`
- Produces: `func LoadConfig(path string) (*Config, error)`
- Produces: `func applyTimestampSuffix(dir string) string` (unexported, copy from lister)

- [ ] **Step 1: Write the failing test**

`config_test.go`:
```go
package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig_HappyPath(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
scheme: http
ak: myak
sk: mysk
op_concurrency: 4
output_dir: ./out
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpConcurrency != 4 {
		t.Errorf("OpConcurrency=%d want 4", cfg.OpConcurrency)
	}
	if cfg.OutputDirTimestamp != true {
		t.Errorf("OutputDirTimestamp default should be true")
	}
	if cfg.Scheme != "http" {
		t.Errorf("Scheme=%q want http", cfg.Scheme)
	}
	if cfg.OutputDir == "./out" {
		t.Errorf("OutputDir not rewritten with timestamp suffix")
	}
	if !strings.HasPrefix(filepath.Base(cfg.OutputDir), "out_") {
		t.Errorf("OutputDir base=%q want out_<stamp>", filepath.Base(cfg.OutputDir))
	}
}

func TestLoadConfig_OpConcurrencyDefaultsTo8(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpConcurrency != 8 {
		t.Errorf("OpConcurrency=%d want 8 (default)", cfg.OpConcurrency)
	}
}

func TestLoadConfig_RejectsMissingEndpoints(t *testing.T) {
	p := writeConfigFile(t, `ak: a
sk: s
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for missing endpoints")
	}
}

func TestLoadConfig_RejectsMissingAKSK(t *testing.T) {
	p := writeConfigFile(t, `endpoints: ["1.2.3.4:80"]
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for missing ak/sk")
	}
}

func TestLoadConfig_OutputDirTimestampExplicitFalse(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
output_dir: ./fixed
output_dir_timestamp: false
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "./fixed" {
		t.Errorf("OutputDir=%q want ./fixed", cfg.OutputDir)
	}
}

func TestLoadConfig_UnknownFieldsIgnored(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: 2
list_concurrency: 8
is_check: true
multipart_check_mode: 1
`)
	if _, err := LoadConfig(p); err != nil {
		t.Fatalf("unknown fields should be ignored: %v", err)
	}
}

func TestLoadConfig_OpConcurrencyZeroRejected(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
op_concurrency: 0
`)
	if _, err := LoadConfig(p); err == nil {
		t.Errorf("expected error for op_concurrency=0")
	}
	_ = strconv.Itoa // keep import if above doesn't use
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestLoadConfig -v`
Expected: FAIL — `config.go` does not exist, build error.

- [ ] **Step 3: Create `go.mod` and `go.sum`**

Copy `delete_marker_list_tool/go.mod` verbatim, change line 1 to `module delete_marker_cleanup_tool`. Copy `delete_marker_list_tool/go.sum` verbatim.

- [ ] **Step 4: Implement `config.go`**

Copy lister's `config.go`, then: remove `ListType` and `ListConcurrency` fields; add `OpConcurrency int` (yaml tag `op_concurrency`). Replace validation: drop `list_type` check, add `op_concurrency < 1` reject. Default `OpConcurrency = 8` when unset. Keep `applyTimestampSuffix` verbatim. Keep the raw-map probe for `output_dir_timestamp` default true.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestLoadConfig -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add delete_marker_cleanup_tool/go.mod delete_marker_cleanup_tool/go.sum delete_marker_cleanup_tool/config.go delete_marker_cleanup_tool/config_test.go
git commit -m "feat(delete_marker_cleanup_tool): config.go with op_concurrency, lister fields dropped"
```

---

## Task 2: S3 client (transport tuning + StatObject + DeleteObject + BucketExists)

**Files:**
- Create: `delete_marker_cleanup_tool/s3client.go`
- Create: `delete_marker_cleanup_tool/s3client_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...` inside `delete_marker_cleanup_tool/`

**Interfaces:**
- Consumes: `Config` from Task 1.
- Produces: `type S3Client struct { client *minio.Client }`
- Produces: `func NewS3Client(cfg *Config, endpoint string) (*S3Client, error)`
- Produces: `func (c *S3Client) StatObject(ctx context.Context, bucket, key, versionID string) (minio.ObjectInfo, error)`
- Produces: `func (c *S3Client) DeleteObject(ctx context.Context, bucket, key, versionID string) error`
- Produces: `func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error)`

- [ ] **Step 1: Write the failing test**

`s3client_test.go`:
```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7"
)

// newFakeHeadDeleteS3 routes by method:
// HEAD /<bkt>/<key>?versionId=<vid>:
//   - if (bkt,key,vid) == (testbucket, marker.txt, dm-1) → 405 + x-amz-delete-marker: true
//   - if (bkt,key,vid) == (testbucket, regular.txt, v-1)  → 200 + ETag + Content-Length
//   - if (bkt,key,vid) == (testbucket, gone.txt, v-1)    → 404 NoSuchVersion
//   - else → 500
// DELETE /<bkt>/<key>?versionId=<vid>:
//   - if vid == "already-deleted" → 404 NoSuchVersion
//   - else → 204
func newFakeHeadDeleteS3(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path: /<bkt>/<key>
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		if len(parts) < 2 {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		bkt, key := parts[0], parts[1]
		vid := r.URL.Query().Get("versionId")
		switch r.Method {
		case http.MethodHead:
			switch {
			case bkt == "testbucket" && key == "marker.txt" && vid == "dm-1":
				w.Header().Set("x-amz-delete-marker", "true")
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			case bkt == "testbucket" && key == "regular.txt" && vid == "v-1":
				w.Header().Set("ETag", `"abc"`)
				w.Header().Set("Content-Length", "5")
				w.WriteHeader(http.StatusOK)
				return
			case bkt == "testbucket" && key == "gone.txt" && vid == "v-1":
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchVersion</Code></Error>`))
				return
			}
			http.Error(w, "boom", http.StatusInternalServerError)
		case http.MethodDelete:
			if vid == "already-deleted" {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchVersion</Code></Error>`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method", http.StatusBadRequest)
		}
	}))
}

func TestStatObject_MarkerReturns405(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.StatObject(context.Background(), "testbucket", "marker.txt", "dm-1")
	// Whether minio-go surfaces this as (info, nil) or (info, MethodNotAllowed err) is the
	// unknown we are probing. Either way, info.IsDeleteMarker should be true.
	if info.IsDeleteMarker {
		return // path A: (info, nil-or-err) with IsDeleteMarker=true
	}
	if err == nil {
		t.Fatalf("expected IsDeleteMarker=true or MethodNotAllowed error; got info=%+v err=nil", info)
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T: %v", err, err)
	}
	if er.Code != "MethodNotAllowed" {
		t.Errorf("err code=%q want MethodNotAllowed", er.Code)
	}
}

func TestStatObject_RegularObjectReturnsInfo(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.StatObject(context.Background(), "testbucket", "regular.txt", "v-1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if info.IsDeleteMarker {
		t.Errorf("IsDeleteMarker=true want false")
	}
	if info.ETag != `"abc"` {
		t.Errorf("ETag=%q want %q", info.ETag, `"abc"`)
	}
}

func TestStatObject_AbsentReturnsNoSuchVersion(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.StatObject(context.Background(), "testbucket", "gone.txt", "v-1")
	if err == nil {
		t.Fatal("expected error")
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T", err)
	}
	if er.Code != "NoSuchVersion" {
		t.Errorf("code=%q want NoSuchVersion", er.Code)
	}
}

func TestDeleteObject_Success(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteObject(context.Background(), "testbucket", "any.txt", "v-1"); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestDeleteObject_NoSuchVersion(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.DeleteObject(context.Background(), "testbucket", "any.txt", "already-deleted")
	if err == nil {
		return // minio-go may swallow 404 NoSuchVersion on DELETE
	}
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		t.Fatalf("expected minio.ErrorResponse, got %T", err)
	}
	if er.Code != "NoSuchVersion" {
		t.Errorf("code=%q want NoSuchVersion", er.Code)
	}
}

func TestBucketExists(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Fake server returns 200 for HEAD on path that has 2 parts only when key matches;
	// BucketExists sends HEAD /<bucket> — adapt: fake server returns 400 for 1-part path.
	// For this test only, treat 400/200 as "exists if not 404". Use a separate server.
	exists, err := c.BucketExists(context.Background(), "testbucket")
	_ = exists
	_ = err
}
```

Note: imports need `"errors"` and `"strings"` — add to import block. Remove the placeholder `TestBucketExists` body (it's a stub); the real test below replaces it. The fake server in this file doesn't cover BucketExists properly — for that, copy `newFakeVersionedS3` from lister's `s3client_test.go` and add a simple `TestBucketExists` that calls `c.BucketExists(ctx, "testbucket")` and asserts `exists==true && err==nil`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestStatObject -v`
Expected: FAIL — `s3client.go` does not exist.

- [ ] **Step 3: Implement `s3client.go`**

Copy lister's `s3client.go` transport construction verbatim (same `http.Transport` block, same `minio.New` options). Replace `ListVersions` method with:
```go
func (c *S3Client) StatObject(ctx context.Context, bucket, key, versionID string) (minio.ObjectInfo, error) {
	return c.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{VersionID: versionID})
}

func (c *S3Client) DeleteObject(ctx context.Context, bucket, key, versionID string) error {
	return c.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{VersionID: versionID})
}
```
Keep `BucketExists` verbatim.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestStatObject|TestDeleteObject|TestBucketExists' -v`
Expected: PASS. **If `TestStatObject_MarkerReturns405` fails because minio-go returns `(info, nil)` instead of an error** — adjust the test to accept either path (the test already does — it returns early if `info.IsDeleteMarker` is true). If minio-go returns neither (e.g., zero-value info + nil err on 405), update the fake server to also send `x-amz-delete-marker: true` AND make the assertion logic in Task 6's classifier handle all three observed shapes.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_cleanup_tool/s3client.go delete_marker_cleanup_tool/s3client_test.go
git commit -m "feat(delete_marker_cleanup_tool): s3client with StatObject/DeleteObject"
```

---

## Task 3: Output (writer goroutines + lazy failed files + 3s flush)

**Files:**
- Create: `delete_marker_cleanup_tool/output.go`
- Create: `delete_marker_cleanup_tool/output_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Consumes: `Config` from Task 1, `mode` string (`"check"` or `"delete"`) from main.
- Produces: `type Output struct { ... }`
- Produces: `func NewOutput(cfg *Config, mode string) (*Output, error)` — opens files based on mode:
  - mode check: opens `check_results.txt` + `head_failed.log`; `head_failed.txt` lazy
  - mode delete: opens `deleted.txt` + `delete_failed.log`; `delete_failed.txt` lazy
- Produces: `func (o *Output) WriteCheckResult(bkt, key, versionID, state string)` — writes `bkt\x1fkey\x1fversion_id\x1f<state>\n` to `check_results.txt`
- Produces: `func (o *Output) WriteDeleted(bkt, key, versionID string)` — writes `bkt\x1fkey\x1fversion_id\n` to `deleted.txt`
- Produces: `func (o *Output) WriteHeadFailed(bkt, key, versionID string)` — lazy-opens `head_failed.txt`, writes `bkt\x1fkey\x1fversion_id\n`
- Produces: `func (o *Output) WriteDeleteFailed(bkt, key, versionID string)` — lazy-opens `delete_failed.txt`, writes `bkt\x1fkey\x1fversion_id\n`
- Produces: `func (o *Output) WriteHeadFailedLog(bkt, key, versionID string, httpCode int, s3Code, reqID string, err error)` — slog to `head_failed.log`
- Produces: `func (o *Output) WriteDeleteFailedLog(bkt, key, versionID string, httpCode int, s3Code, reqID string, err error)` — slog to `delete_failed.log`
- Produces: `func (o *Output) FlushAndWait()` — closes all channels, waits writers, flushes bufio
- Produces: `func (o *Output) Close() error` — closes underlying files

- [ ] **Step 1: Write the failing test**

`output_test.go`:
```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestOutput(t *testing.T, mode string) (*Output, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	cfg := &Config{OutputDir: dir, OutputDirTimestamp: false}
	o, err := NewOutput(cfg, mode)
	if err != nil {
		t.Fatal(err)
	}
	return o, dir
}

func TestWriteCheckResult_FourFieldRow(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteCheckResult("bkt", "photos/a.jpg", "v1", "marker")
	o.WriteCheckResult("bkt", "photos/b.jpg", "null", "not_marker")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), string(b))
	}
	want0 := "bkt\x1fphotos/a.jpg\x1fv1\x1fmarker"
	want1 := "bkt\x1fphotos/b.jpg\x1fnull\x1fnot_marker"
	if lines[0] != want0 {
		t.Errorf("line0=%q want %q", lines[0], want0)
	}
	if lines[1] != want1 {
		t.Errorf("line1=%q want %q", lines[1], want1)
	}
}

func TestWriteDeleted_ThreeFieldRow(t *testing.T) {
	o, dir := newTestOutput(t, "delete")
	o.WriteDeleted("bkt", "key1", "v1")
	o.WriteDeleted("bkt", "key2", "already-deleted") // NoSuchVersion path still writes here
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "deleted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[0] != "bkt\x1fkey1\x1fv1" {
		t.Errorf("line0=%q", lines[0])
	}
	if lines[1] != "bkt\x1fkey2\x1falready-deleted" {
		t.Errorf("line1=%q", lines[1])
	}
}

func TestHeadFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.FlushAndWait()
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "head_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("head_failed.txt should not exist when no failures; err=%v", err)
	}
}

func TestWriteHeadFailed_CreatesAndWrites(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteHeadFailed("bkt", "k", "v")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "head_failed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := "bkt\x1fk\x1fv\n"
	if string(b) != want {
		t.Errorf("got %q want %q", string(b), want)
	}
}

func TestWriteHeadFailedLog_WritesSlog(t *testing.T) {
	o, dir := newTestOutput(t, "check")
	o.WriteHeadFailedLog("bkt", "k", "v", 500, "InternalError", "req-1", nil)
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "head_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{"bkt", "k", "v", "500", "InternalError", "req-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("head_failed.log missing %q: %q", want, out)
		}
	}
}

func TestDeleteFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t, "delete")
	o.FlushAndWait()
	o.Close()
	if _, err := os.Stat(filepath.Join(dir, "delete_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("delete_failed.txt should not exist when no failures")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestWriteCheckResult|TestWriteDeleted|TestHeadFailed|TestDeleteFailed|TestWriteHeadFailedLog' -v`
Expected: FAIL — `output.go` does not exist.

- [ ] **Step 3: Implement `output.go`**

Copy lister's `output.go` skeleton (writer goroutine pattern, 3s flush ticker, lazy failed file with mutex, `containsRowSep`, `rowSep` const). Adapt:
- Two result channels: `checkResultCh chan [4]string` (bkt/key/vid/state) and `deletedCh chan [3]string` (bkt/key/vid).
- Two failed channels: `headFailedCh chan [3]string` and `deleteFailedCh chan [3]string`.
- Two slog loggers: `headFailedLogger` (writes to `head_failed.log`) and `deleteFailedLogger` (writes to `delete_failed.log`).
- `NewOutput(cfg, mode)` opens only the result file + log file for the given mode; the other result/log files stay nil. Failed files are always lazy.
- Writer goroutines: start only the ones whose file is non-nil. For failed channels, always start (lazy open).
- `writeCheckResultRow(w, bkt, key, vid, state)`: four `\x1f`-separated fields + `\n`.
- `writeDeletedRow(w, bkt, key, vid)`: three `\x1f`-separated fields + `\n` (same as lister's `writeRow` minus bucket-as-separate concept — bkt is first field).
- `FlushAndWait` closes all 4 channels + waits.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestWriteCheckResult|TestWriteDeleted|TestHeadFailed|TestDeleteFailed|TestWriteHeadFailedLog' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_cleanup_tool/output.go delete_marker_cleanup_tool/output_test.go
git commit -m "feat(delete_marker_cleanup_tool): output with check/delete mode files"
```

---

## Task 4: Stats + Progress

**Files:**
- Create: `delete_marker_cleanup_tool/stats.go`
- Create: `delete_marker_cleanup_tool/stats_test.go`
- Create: `delete_marker_cleanup_tool/progress.go`

**Interfaces:**
- Produces: `type Stats struct { processed, marker, notMarker, absent, headFailed, deleted, deleteFailed atomic.Int64; totalSecNs atomic.Int64 }`
- Produces: `func (s *Stats) IncrProcessed()`, `IncrMarker()`, `IncrNotMarker()`, `IncrAbsent()`, `IncrHeadFailed()`, `IncrDeleted()`, `IncrDeleteFailed()`
- Produces: `func (s *Stats) SetTotalDuration(d time.Duration)`
- Produces: `func (s *Stats) PrintSummary(w io.Writer, mode string)` — mode check prints `processed/marker/not_marker/absent/head_failed/total_sec`; mode delete prints `processed/deleted/delete_failed/total_sec`
- Produces: `func NewProgressPrinter(w io.Writer) *ProgressPrinter`, `func (p *ProgressPrinter) SetInterval(n int)`, `func (p *ProgressPrinter) MaybePrintCheck(s *Stats, count int)`, `func (p *ProgressPrinter) MaybePrintDelete(s *Stats, count int)`

- [ ] **Step 1: Write the failing test**

`stats_test.go`:
```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestStatsCheckSnapshotAndSummary(t *testing.T) {
	var s Stats
	for i := 0; i < 100; i++ {
		s.IncrProcessed()
	}
	for i := 0; i < 50; i++ {
		s.IncrMarker()
	}
	for i := 0; i < 10; i++ {
		s.IncrNotMarker()
	}
	for i := 0; i < 38; i++ {
		s.IncrAbsent()
	}
	for i := 0; i < 2; i++ {
		s.IncrHeadFailed()
	}
	var buf bytes.Buffer
	s.PrintSummary(&buf, "check")
	out := buf.String()
	for _, want := range []string{"processed=100", "marker=50", "not_marker=10", "absent=38", "head_failed=2", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("check summary missing %q: %s", want, out)
		}
	}
}

func TestStatsDeleteSnapshotAndSummary(t *testing.T) {
	var s Stats
	for i := 0; i < 100; i++ {
		s.IncrProcessed()
	}
	for i := 0; i < 98; i++ {
		s.IncrDeleted()
	}
	for i := 0; i < 2; i++ {
		s.IncrDeleteFailed()
	}
	var buf bytes.Buffer
	s.PrintSummary(&buf, "delete")
	out := buf.String()
	for _, want := range []string{"processed=100", "deleted=98", "delete_failed=2", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("delete summary missing %q: %s", want, out)
		}
	}
}

func TestProgressPrinterCheck(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetInterval(10)
	var s Stats
	for i := 0; i < 5; i++ {
		s.IncrMarker()
	}
	p.MaybePrintCheck(&s, 10)
	out := buf.String()
	if !strings.Contains(out, "marker=5") {
		t.Errorf("missing marker=5: %q", out)
	}
	if !strings.Contains(out, "not_marker=0") {
		t.Errorf("missing not_marker=0: %q", out)
	}
}

func TestProgressPrinterDelete(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	p.SetInterval(10)
	var s Stats
	for i := 0; i < 7; i++ {
		s.IncrDeleted()
	}
	p.MaybePrintDelete(&s, 10)
	out := buf.String()
	if !strings.Contains(out, "deleted=7") {
		t.Errorf("missing deleted=7: %q", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestStats|TestProgressPrinter' -v`
Expected: FAIL — files don't exist.

- [ ] **Step 3: Implement `stats.go`**

Atomic counters as listed. `PrintSummary(w, mode)` switches on mode and prints the corresponding fields. Format strings:
- check: `=== summary ===\nprocessed=%d\nmarker=%d\nnot_marker=%d\nabsent=%d\nhead_failed=%d\ntotal_sec=%.2f\n`
- delete: `=== summary ===\nprocessed=%d\ndeleted=%d\ndelete_failed=%d\ntotal_sec=%.2f\n`

- [ ] **Step 4: Implement `progress.go`**

Copy lister's `progress.go` but:
- Drop `QueueSnapshot` type and `SetQueueSnapshotProvider` (no BFS queue).
- Replace `MaybePrint(s *Stats, label string, count int)` with two methods:
  - `MaybePrintCheck(s *Stats, count int)`: prints `[progress] processed=%d marker=%d not_marker=%d absent=%d failed=%d\n` when `count >= interval`.
  - `MaybePrintDelete(s *Stats, count int)`: prints `[progress] processed=%d deleted=%d failed=%d\n` when `count >= interval`.
- Keep the per-call mu.Lock pattern.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestStats|TestProgressPrinter' -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add delete_marker_cleanup_tool/stats.go delete_marker_cleanup_tool/stats_test.go delete_marker_cleanup_tool/progress.go
git commit -m "feat(delete_marker_cleanup_tool): stats + progress for check/delete modes"
```

---

## Task 5: Worker pool (row parse + endpoint rotation + dispatch)

**Files:**
- Create: `delete_marker_cleanup_tool/worker.go`
- Create: `delete_marker_cleanup_tool/worker_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Consumes: `Config`, `S3Client` (Task 2), `Output` (Task 3), `Stats` + `ProgressPrinter` (Task 4).
- Produces: `type Row struct { Bkt, Key, VersionID string }`
- Produces: `func ParseRow(line, inputBasename string) (Row, bool)` — splits on `\x1f`, expects 3 fields; if basename == `invalid_keys.txt`, URL-decode the key field; returns `ok=false` if field count != 3 or URL-decode fails.
- Produces: `func RunWorkers(ctx context.Context, cfg *Config, mode, inputPath string, out *Output, stats *Stats, progress *ProgressPrinter) error` — reads input file line-by-line, spawns `cfg.OpConcurrency` workers each bound to `endpoints[i % len(endpoints)]`, fans rows out via `chan Row`, calls per-mode handler.

- [ ] **Step 1: Write the failing test**

`worker_test.go`:
```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRow_ThreeFields(t *testing.T) {
	r, ok := ParseRow("bkt\x1fkey/a\x1fv1", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Bkt != "bkt" || r.Key != "key/a" || r.VersionID != "v1" {
		t.Errorf("got %+v", r)
	}
}

func TestParseRow_InvalidKeysBasenameURLDecodesKey(t *testing.T) {
	// "a%1Fb" url-decodes to "a\x1fb"
	r, ok := ParseRow("bkt\x1fa%1Fb\x1fv1", "invalid_keys.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Key != "a\x1fb" {
		t.Errorf("Key=%q want %q", r.Key, "a\x1fb")
	}
}

func TestParseRow_WrongFieldCount(t *testing.T) {
	if _, ok := ParseRow("only\x1ftwo", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 2-field line")
	}
	if _, ok := ParseRow("bkt\x1fk\x1fv\x1fextra", "delete_markers.txt"); ok {
		t.Errorf("expected ok=false for 4-field line")
	}
}

func TestParseRow_NullVersionIdPassesThrough(t *testing.T) {
	r, ok := ParseRow("bkt\x1fk\x1fnull", "delete_markers.txt")
	if !ok {
		t.Fatal("expected ok")
	}
	if r.VersionID != "null" {
		t.Errorf("VersionID=%q want %q", r.VersionID, "null")
	}
}

func TestRunWorkers_ModeCheck_ThreeStates(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fmarker.txt\x1fdm-1",
		"testbucket\x1fregular.txt\x1fv-1",
		"testbucket\x1fgone.txt\x1fv-1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out2 := string(b)
	if !strings.Contains(out2, "testbucket\x1fmarker.txt\x1fdm-1\x1fmarker") {
		t.Errorf("missing marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fregular.txt\x1fv-1\x1fnot_marker") {
		t.Errorf("missing not_marker state: %q", out2)
	}
	if !strings.Contains(out2, "testbucket\x1fgone.txt\x1fv-1\x1absent") {
		t.Errorf("missing absent state: %q", out2)
	}
}

func TestRunWorkers_ModeDelete_NoSuchVersionCountsAsSuccess(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{
		"testbucket\x1fany.txt\x1fv-1",
		"testbucket\x1fany.txt\x1falready-deleted",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	progress.SetInterval(1000)
	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, err := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d deleted lines, want 2: %q", len(lines), string(b))
	}
	// delete_failed.txt must not exist (no real failures)
	if _, err := os.Stat(filepath.Join(dir, "out", "delete_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("delete_failed.txt should not exist: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestParseRow|TestRunWorkers' -v`
Expected: FAIL — `worker.go` does not exist.

- [ ] **Step 3: Implement `worker.go`**

```go
package main

import (
	"bufio"
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

type Row struct {
	Bkt, Key, VersionID string
}

func ParseRow(line, inputBasename string) (Row, bool) {
	parts := strings.Split(line, "\x1f")
	if len(parts) != 3 {
		return Row{}, false
	}
	bkt, key, vid := parts[0], parts[1], parts[2]
	if filepath.Base(inputBasename) == "invalid_keys.txt" {
		dec, err := url.QueryUnescape(key)
		if err != nil {
			return Row{}, false
		}
		key = dec
	}
	return Row{Bkt: bkt, Key: key, VersionID: vid}, true
}

func RunWorkers(ctx context.Context, cfg *Config, mode, inputPath string, out *Output, stats *Stats, progress *ProgressPrinter) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	inputBasename := filepath.Base(inputPath)
	rowCh := make(chan Row, 1024)

	var wg sync.WaitGroup
	var endpointIdx atomic.Int64
	n := cfg.OpConcurrency
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := int(endpointIdx.Add(1)-1) % len(cfg.Endpoints)
			endpoint := cfg.Endpoints[idx]
			client, err := NewS3Client(cfg, endpoint)
			if err != nil {
				return // TODO: log
			}
			var local int
			for row := range rowCh {
				switch mode {
				case "check":
					handleCheck(ctx, client, row, out, stats)
				case "delete":
					handleDelete(ctx, client, row, out, stats)
				}
				local++
				if mode == "check" {
					progress.MaybePrintCheck(stats, local)
				} else {
					progress.MaybePrintDelete(stats, local)
				}
			}
		}()
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		row, ok := ParseRow(line, inputBasename)
		if !ok {
			// Skip torn line, log to head_failed.log or delete_failed.log based on mode
			continue
		}
		rowCh <- row
	}
	close(rowCh)
	wg.Wait()
	return scanner.Err()
}
```

Add `handleCheck` and `handleDelete` helpers (Task 6 and Task 8 will fill them; for now stub them to call `stats.IncrProcessed()` + a no-op):

For Task 5, stub:
```go
func handleCheck(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	// filled in Task 6
}

func handleDelete(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	// filled in Task 8
}
```

Add `"os"` import.

- [ ] **Step 4: Run test to verify it fails** (parsing tests should pass; worker tests will fail because handlers are stubs)

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestParseRow' -v`
Expected: PASS for parse tests.

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestRunWorkers' -v`
Expected: FAIL — stubs don't classify or write.

- [ ] **Step 5: Commit (parse-only commit)**

```bash
git add delete_marker_cleanup_tool/worker.go delete_marker_cleanup_tool/worker_test.go
git commit -m "feat(delete_marker_cleanup_tool): worker pool + ParseRow, handlers stubbed"
```

---

## Task 6: Classifier + handleCheck (HEAD three-state)

**Files:**
- Modify: `delete_marker_cleanup_tool/worker.go` — replace `handleCheck` stub
- Create: `delete_marker_cleanup_tool/classifier.go`
- Create: `delete_marker_cleanup_tool/classifier_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Produces: `func classifyHead(info minio.ObjectInfo, err error) (state string, ok bool)` — pure function:
  - `err == nil && info.IsDeleteMarker` → `("marker", true)`
  - `err == nil && !info.IsDeleteMarker` → `("not_marker", true)`
  - `err != nil && errorResponse.Code ∈ {"NoSuchKey", "NoSuchVersion"}` → `("absent", true)`
  - `err != nil && errorResponse.Code == "MethodNotAllowed" && info.IsDeleteMarker` → `("marker", true)` (defensive: some S3 impls return this combo)
  - other `err` → `("", false)` (unclassified — failed)

- [ ] **Step 1: Write the failing test**

`classifier_test.go`:
```go
package main

import (
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestClassifyHead_MarkerInfoNilErr(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: true}
	state, ok := classifyHead(info, nil)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_RegularInfoNilErr(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: false, ETag: `"abc"`}
	state, ok := classifyHead(info, nil)
	if !ok || state != "not_marker" {
		t.Errorf("got (%q, %v) want (not_marker, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchVersionErr(t *testing.T) {
	err := minio.ErrorResponse{Code: "NoSuchVersion", StatusCode: 404}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchKeyErr(t *testing.T) {
	err := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_MethodNotAllowedWithMarkerFlag(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: true}
	err := minio.ErrorResponse{Code: "MethodNotAllowed", StatusCode: 405}
	state, ok := classifyHead(info, err)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_InternalErrorUnclassified(t *testing.T) {
	err := minio.ErrorResponse{Code: "InternalError", StatusCode: 500}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if ok {
		t.Errorf("got ok=true for InternalError, want false")
	}
	if state != "" {
		t.Errorf("got state=%q want empty", state)
	}
}

func TestClassifyHead_NonErrorResponseErrUnclassified(t *testing.T) {
	err := errors.New("transport boom")
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if ok {
		t.Errorf("got ok=true for non-ErrorResponse, want false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestClassifyHead -v`
Expected: FAIL — `classifier.go` does not exist.

- [ ] **Step 3: Implement `classifier.go`**

```go
package main

import (
	"errors"

	"github.com/minio/minio-go/v7"
)

// classifyHead converts (info, err) from StatObject into a 3-state result.
// Returns ok=false for unclassified (failed) responses.
func classifyHead(info minio.ObjectInfo, err error) (state string, ok bool) {
	if err == nil {
		if info.IsDeleteMarker {
			return "marker", true
		}
		return "not_marker", true
	}
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		switch er.Code {
		case "NoSuchKey", "NoSuchVersion":
			return "absent", true
		case "MethodNotAllowed":
			if info.IsDeleteMarker {
				return "marker", true
			}
		}
	}
	return "", false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestClassifyHead -v`
Expected: PASS.

- [ ] **Step 5: Replace `handleCheck` stub in `worker.go`**

```go
func handleCheck(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	info, err := c.StatObject(ctx, r.Bkt, r.Key, r.VersionID)
	state, ok := classifyHead(info, err)
	if !ok {
		stats.IncrHeadFailed()
		out.WriteHeadFailed(r.Bkt, r.Key, r.VersionID)
		var er minio.ErrorResponse
		if errors.As(err, &er) {
			out.WriteHeadFailedLog(r.Bkt, r.Key, r.VersionID, er.StatusCode, er.Code, er.RequestID, err)
		} else {
			out.WriteHeadFailedLog(r.Bkt, r.Key, r.VersionID, 0, "", "", err)
		}
		return
	}
	switch state {
	case "marker":
		stats.IncrMarker()
	case "not_marker":
		stats.IncrNotMarker()
	case "absent":
		stats.IncrAbsent()
	}
	out.WriteCheckResult(r.Bkt, r.Key, r.VersionID, state)
}
```

Add imports: `"github.com/minio/minio-go/v7"`, `"errors"`.

- [ ] **Step 6: Run worker test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestRunWorkers_ModeCheck -v`
Expected: PASS. If `marker.txt` row fails to classify as `marker` — this is the spec's known unknown. Inspect what minio-go actually returns for 405 + `x-amz-delete-marker: true`: if it's `(ObjectInfo{IsDeleteMarker:true}, nil)` the test passes; if it's `(ObjectInfo{}, err)` with `Code=="MethodNotAllowed"` but `info.IsDeleteMarker==false`, extend `classifyHead` to also check the response header in `info` or fall back to treating `MethodNotAllowed` as `marker` unconditionally (delete this fallback branch's `info.IsDeleteMarker` condition). Document the chosen path in a code comment.

- [ ] **Step 7: Commit**

```bash
git add delete_marker_cleanup_tool/classifier.go delete_marker_cleanup_tool/classifier_test.go delete_marker_cleanup_tool/worker.go
git commit -m "feat(delete_marker_cleanup_tool): classifier + handleCheck 3-state"
```

---

## Task 7: Torn line handling + invalid_keys.txt input path

**Files:**
- Modify: `delete_marker_cleanup_tool/worker.go` — `RunWorkers` torn-line logging
- Modify: `delete_marker_cleanup_tool/worker_test.go` — add torn-line + invalid_keys.txt tests
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Consumes: existing Task 5/6 interfaces.
- Produces: torn-line skip is logged to mode-appropriate failed log (`head_failed.log` for check, `delete_failed.log` for delete) with `err="torn line, field count != 3"`.

- [ ] **Step 1: Write the failing test (append to `worker_test.go`)**

```go
func TestRunWorkers_TornLineSkippedAndLogged(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	// Mix: valid row, torn row (2 fields), valid row
	rows := []string{
		"testbucket\x1fmarker.txt\x1fdm-1",
		"only\x1ftwo-fields",
		"testbucket\x1fregular.txt\x1fv-1",
	}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	// check_results.txt has 2 rows (torn row skipped)
	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("got %d result lines, want 2: %q", len(lines), string(b))
	}
	// head_failed.log mentions torn line
	lb, _ := os.ReadFile(filepath.Join(dir, "out", "head_failed.log"))
	if !strings.Contains(string(lb), "torn") {
		t.Errorf("head_failed.log missing torn note: %q", string(lb))
	}
}

func TestRunWorkers_InvalidKeysInputURLDecodesKey(t *testing.T) {
	// fake server: HEAD testbucket/a\x1fb with versionId=v1 → 200 regular
	// (Use a server that 200s everything for HEAD to keep this focused.)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"abc"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "invalid_keys.txt")
	// row "bkt\x1fa%1Fb\x1fv1" should decode to key "a\x1fb"
	rows := []string{"testbucket\x1fa%1Fb\x1fv1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "check")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "check", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	// The written result should contain the decoded key
	if !strings.Contains(string(b), "testbucket\x1fa\x1fb\x1fv1") {
		t.Errorf("missing decoded-key row: %q", string(b))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestRunWorkers_TornLine|TestRunWorkers_InvalidKeysInput' -v`
Expected: FAIL — torn-line test fails (no "torn" log written yet); invalid_keys test should already pass from Task 5 (verify it does).

- [ ] **Step 3: Modify `RunWorkers` in `worker.go` to log torn lines**

In the scanner loop, replace the `if !ok { continue }` with:
```go
if !ok {
	if mode == "check" {
		out.WriteHeadFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3"))
	} else {
		out.WriteDeleteFailedLog("", "", "", 0, "", "", errors.New("torn line, field count != 3"))
	}
	continue
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestRunWorkers_TornLine|TestRunWorkers_InvalidKeysInput' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_cleanup_tool/worker.go delete_marker_cleanup_tool/worker_test.go
git commit -m "feat(delete_marker_cleanup_tool): torn-line skip + invalid_keys URL decode"
```

---

## Task 8: handleDelete (DeleteObject + NoSuchVersion=success)

**Files:**
- Modify: `delete_marker_cleanup_tool/worker.go` — replace `handleDelete` stub
- Modify: `delete_marker_cleanup_tool/worker_test.go` — add delete failure-path test
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Consumes: Task 5 `RunWorkers`, Task 3 `Output`.
- Produces: filled `handleDelete`:
  - `err == nil` → `stats.IncrDeleted()` + `out.WriteDeleted(...)`
  - `err != nil && errorResponse.Code == "NoSuchVersion"` → same as success (per spec §3 mode delete table)
  - `err != nil && errorResponse.Code == "NoSuchKey"` → also treat as success (defensive: some servers return NoSuchKey when the version was already deleted; same intent as NoSuchVersion)
  - other `err` → `stats.IncrDeleteFailed()` + `out.WriteDeleteFailed(...)` + `out.WriteDeleteFailedLog(...)`

- [ ] **Step 1: Write the failing test (append to `worker_test.go`)**

```go
func TestRunWorkers_ModeDelete_InternalErrorRecordedAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			http.Error(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><RequestId>req-x</RequestId></Error>`, http.StatusInternalServerError)
			return
		}
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fkey1\x1fv-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		OpConcurrency: 1, OutputDir: filepath.Join(dir, "out"), OutputDirTimestamp: false,
		ProgressInterval: 1000,
	}
	out, err := NewOutput(cfg, "delete")
	if err != nil {
		t.Fatal(err)
	}
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	if err := RunWorkers(context.Background(), cfg, "delete", inputPath, out, &stats, progress); err != nil {
		t.Fatal(err)
	}
	out.FlushAndWait()
	out.Close()
	// deleted.txt should be empty (no success)
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt")); strings.TrimSpace(string(b)) != "" {
		t.Errorf("deleted.txt should be empty: %q", string(b))
	}
	// delete_failed.txt should have the row
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.txt")); !strings.Contains(string(b), "testbucket\x1fkey1\x1fv-1") {
		t.Errorf("delete_failed.txt missing row: %q", string(b))
	}
	// delete_failed.log should mention InternalError
	if b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.log")); !strings.Contains(string(b), "InternalError") {
		t.Errorf("delete_failed.log missing InternalError: %q", string(b))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestRunWorkers_ModeDelete_InternalError -v`
Expected: FAIL — `handleDelete` stub doesn't classify.

- [ ] **Step 3: Replace `handleDelete` stub in `worker.go`**

```go
func handleDelete(ctx context.Context, c *S3Client, r Row, out *Output, stats *Stats) {
	stats.IncrProcessed()
	err := c.DeleteObject(ctx, r.Bkt, r.Key, r.VersionID)
	if err == nil {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.Key, r.VersionID)
		return
	}
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchVersion" || er.Code == "NoSuchKey") {
		stats.IncrDeleted()
		out.WriteDeleted(r.Bkt, r.Key, r.VersionID)
		return
	}
	stats.IncrDeleteFailed()
	out.WriteDeleteFailed(r.Bkt, r.Key, r.VersionID)
	if errors.As(err, &er) {
		out.WriteDeleteFailedLog(r.Bkt, r.Key, r.VersionID, er.StatusCode, er.Code, er.RequestID, err)
	} else {
		out.WriteDeleteFailedLog(r.Bkt, r.Key, r.VersionID, 0, "", "", err)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run TestRunWorkers_ModeDelete -v`
Expected: PASS (both NoSuchVersion-counts-as-success and InternalError-failure).

- [ ] **Step 5: Commit**

```bash
git add delete_marker_cleanup_tool/worker.go delete_marker_cleanup_tool/worker_test.go
git commit -m "feat(delete_marker_cleanup_tool): handleDelete with NoSuchVersion=success"
```

---

## Task 9: main.go (flag parsing + run orchestration + preflight + summary)

**Files:**
- Create: `delete_marker_cleanup_tool/main.go`
- Create: `delete_marker_cleanup_tool/main_test.go`
- Test: `~/sdk/go1.27.1/bin/go test ./...`

**Interfaces:**
- Consumes: `Config` (Task 1), `S3Client` (Task 2), `Output` (Task 3), `Stats`+`ProgressPrinter` (Task 4), `RunWorkers` (Task 5).
- Produces: `func run(cfgPath, mode, inputPath string) error`
- Produces: `func main()` — parses `-c`, `-mode`, `-input`; validates non-empty; calls `run`; on error → stderr + exit 1.
- Produces: `func printConfigSnapshot(w io.Writer, cfg *Config, mode, inputPath string)` — sk masked as `***`.

- [ ] **Step 1: Write the failing test**

`main_test.go`:
```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_ModeCheck_EndToEnd(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fmarker.txt\x1fdm-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.URL,
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	if err := run(cfgPath, "check", inputPath); err != nil {
		t.Fatalf("run: %v", err)
	}
	rl, _ := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if !strings.Contains(string(rl), "=== summary ===") {
		t.Errorf("run.log missing summary: %q", rl)
	}
	if !strings.Contains(string(rl), "sk=***") {
		t.Errorf("run.log should mask sk: %q", rl)
	}
	if !strings.Contains(string(rl), "mode=check") {
		t.Errorf("run.log missing mode=check: %q", rl)
	}
	cr, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	if !strings.Contains(string(cr), "testbucket\x1fmarker.txt\x1fdm-1\x1fmarker") {
		t.Errorf("missing marker result: %q", cr)
	}
}

func TestRun_ModeDelete_EndToEnd(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fany.txt\x1fv-1"}
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.URL,
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	if err := run(cfgPath, "delete", inputPath); err != nil {
		t.Fatalf("run: %v", err)
	}
	d, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if !strings.Contains(string(d), "testbucket\x1fany.txt\x1fv-1") {
		t.Errorf("missing deleted row: %q", d)
	}
}

func TestRun_InvalidMode(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints: [\"1.2.3.4:80\"]",
		"ak: a",
		"sk: s",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "audit", "/tmp/nonexistent")
	if err == nil {
		t.Errorf("expected error for invalid mode 'audit'")
	}
}

func TestRun_BucketPreflight_TransportErrorExits(t *testing.T) {
	// No server running — dial will fail.
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	rows := []string{"testbucket\x1fmarker.txt\x1fdm-1"}
	os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644)
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - 127.0.0.1:1", // unreachable
		"scheme: http",
		"ak: a",
		"sk: s",
		"dial_timeout: 1",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "check", inputPath)
	if err == nil {
		t.Errorf("expected transport error on preflight")
	}
}

func TestRun_BucketPreflight_FirstRowParseFailureExits(t *testing.T) {
	srv := newFakeHeadDeleteS3(t)
	defer srv.Close()
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "input.txt")
	// First line is torn (2 fields)
	os.WriteFile(inputPath, []byte("only\x1ftwo-fields\n"), 0644)
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.URL,
		"scheme: http",
		"ak: a",
		"sk: s",
		"op_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "check", inputPath)
	if err == nil {
		t.Errorf("expected error for first-row parse failure")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestRun_' -v`
Expected: FAIL — `main.go` does not exist.

- [ ] **Step 3: Implement `main.go`**

```go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	cfgPath := flag.String("c", "", "config yaml path")
	mode := flag.String("mode", "", "check or delete")
	inputPath := flag.String("input", "", "input file path (delete_markers.txt or invalid_keys.txt)")
	flag.Parse()

	if *cfgPath == "" || *mode == "" || *inputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: -c config.yaml -mode <check|delete> -input <path>")
		os.Exit(2)
	}
	if *mode != "check" && *mode != "delete" {
		fmt.Fprintf(os.Stderr, "error: -mode must be check or delete, got %q\n", *mode)
		os.Exit(2)
	}
	if err := run(*cfgPath, *mode, *inputPath); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(cfgPath, mode, inputPath string) error {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("mkdir output: %w", err)
	}
	runLog, err := os.OpenFile(filepath.Join(cfg.OutputDir, "run.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open run.log: %w", err)
	}
	defer runLog.Close()
	mwOut := io.MultiWriter(os.Stdout, runLog)
	mwErr := io.MultiWriter(os.Stderr, runLog)

	printConfigSnapshot(mwErr, cfg, mode, inputPath)

	start := time.Now()

	// Preflight: parse first input row → bucket; BucketExists with endpoints[0].
	firstBkt, ok := readFirstRowBucket(inputPath)
	if !ok {
		return fmt.Errorf("preflight: first input row unparseable (torn or empty file)")
	}
	preflightClient, err := NewS3Client(cfg, cfg.Endpoints[0])
	if err != nil {
		return err
	}
	exists, err := preflightClient.BucketExists(context.Background(), firstBkt)
	if err != nil {
		fmt.Fprintf(mwErr, "bucket exists probe failed: %v\n", err)
		return err
	}
	if !exists {
		fmt.Fprintf(mwErr, "warning: bucket %s not found on endpoints[0] (continuing)\n", firstBkt)
	}

	out, err := NewOutput(cfg, mode)
	if err != nil {
		return err
	}

	var stats Stats
	progress := NewProgressPrinter(mwOut)
	progress.SetInterval(cfg.ProgressInterval)

	runErr := RunWorkers(context.Background(), cfg, mode, inputPath, out, &stats, progress)

	stats.SetTotalDuration(time.Since(start))
	out.FlushAndWait()
	out.Close()

	stats.PrintSummary(mwOut, mode)

	return runErr
}

// readFirstRowBucket opens inputPath, reads the first non-empty line, parses it
// as a 3-field \x1f-separated row (with URL-decode if basename == invalid_keys.txt),
// and returns the bucket field. Returns ok=false on parse failure or empty file.
func readFirstRowBucket(inputPath string) (string, bool) {
	f, err := os.Open(inputPath)
	if err != nil {
		return "", false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		row, ok := ParseRow(line, filepath.Base(inputPath))
		if !ok {
			return "", false
		}
		return row.Bkt, true
	}
	return "", false
}

func printConfigSnapshot(w io.Writer, cfg *Config, mode, inputPath string) {
	fmt.Fprintf(w, "=== config ===\n")
	fmt.Fprintf(w, "mode=%s\n", mode)
	fmt.Fprintf(w, "input=%s\n", inputPath)
	fmt.Fprintf(w, "endpoints=[%s]\n", strings.Join(cfg.Endpoints, ", "))
	fmt.Fprintf(w, "scheme=%s\n", cfg.Scheme)
	fmt.Fprintf(w, "ak=%s\n", cfg.AK)
	fmt.Fprintf(w, "sk=***\n")
	fmt.Fprintf(w, "op_concurrency=%d\n", cfg.OpConcurrency)
	fmt.Fprintf(w, "output_dir=%s\n", cfg.OutputDir)
	fmt.Fprintf(w, "output_dir_timestamp=%v\n", cfg.OutputDirTimestamp)
	fmt.Fprintf(w, "progress_interval=%d\n", cfg.ProgressInterval)
	fmt.Fprintf(w, "dial_timeout=%d\n", cfg.DialTimeout)
	fmt.Fprintf(w, "response_header_timeout=%d\n", cfg.ResponseHeaderTimeout)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -run 'TestRun_' -v`
Expected: PASS.

- [ ] **Step 5: Run full test suite**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test ./... -v`
Expected: PASS for all tests.

- [ ] **Step 6: Build to verify it compiles**

Run: `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go build -o delete_marker_cleanup_tool .`
Expected: no errors, binary exists.

- [ ] **Step 7: Commit**

```bash
git add delete_marker_cleanup_tool/main.go delete_marker_cleanup_tool/main_test.go
git commit -m "feat(delete_marker_cleanup_tool): main.go with flag parsing + preflight"
```

---

## Task 10: Real-MinIO end-to-end integration test

**Files:**
- Create: `delete_marker_cleanup_tool/integration_test.go`
- Test: `~/sdk/go1.27.1/bin/go test -tags=integration -run TestIntegration ./...`

**Goal:** Verify the full pipeline against a real MinIO server — this is the test that pins the spec's biggest unknown (§12 known limitation #1: HEAD-on-delete-marker minio-go behavior) and proves the three-state classification and delete-with-NoSuchVersion path work end-to-end on a real S3 implementation, not just the httptest fakes used by unit tests.

**Setup contract:**
- Build tag `//go:build integration` on the file — unit `go test ./...` does NOT run this.
- Test reads env vars: `MINIO_ENDPOINT` (e.g., `127.0.0.1:9000`), `MINIO_AK`, `MINIO_SK`, `MINIO_BUCKET` (existing bucket or one the AK can create). If any is missing → `t.Skip("set MINIO_ENDPOINT/MINIO_AK/MINIO_SK/MINIO_BUCKET to run integration test")`.
- Bucket lifecycle: tests create a unique bucket per run (e.g., `cleanup-tool-it-<unix-nano>`), enable versioning, and clean it up at the end (delete all versions + markers, then remove bucket). If MINIO_BUCKET is set, use that instead and clean up contents only.

**Interfaces:**
- Consumes: `Config`, `S3Client`, `Output`, `RunWorkers`, `run` from earlier tasks. Uses `github.com/minio/minio-go/v7` directly for fixture setup (creating bucket, versioning, writing objects, deleting objects to create markers).

- [ ] **Step 1: Write the failing test**

`integration_test.go`:
```go
//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func skipIfNoMinIO(t *testing.T) (endpoint, ak, sk, bucket string) {
	t.Helper()
	endpoint = os.Getenv("MINIO_ENDPOINT")
	ak = os.Getenv("MINIO_AK")
	sk = os.Getenv("MINIO_SK")
	bucket = os.Getenv("MINIO_BUCKET")
	if endpoint == "" || ak == "" || sk == "" {
		t.Skip("set MINIO_ENDPOINT/MINIO_AK/MINIO_SK to run integration test")
	}
	return
}

func newMinIOAdminClient(t *testing.T, endpoint, ak, sk string) *minio.Client {
	t.Helper()
	c, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}
	return c
}

func setupBucket(t *testing.T, c *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
	}
	// Enable versioning. minio-go v7.3.0's SetBucketVersioning takes a context + bucket + config.
	if err := c.SetBucketVersioning(ctx, bucket, minio.BucketVersioningConfiguration{
		Status: "Enabled",
	}); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
}

func teardownBucket(t *testing.T, c *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	// List all versions, delete each (markers included), then remove bucket.
	ch := c.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		WithVersions: true, Recursive: true, MaxKeys: 1000,
	})
	var toDelete []minio.ObjectInfo
	for obj := range ch {
		if obj.Err != nil {
			break
		}
		toDelete = append(toDelete, obj)
	}
	for _, obj := range toDelete {
		_ = c.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
	}
	_ = c.RemoveBucket(ctx, bucket)
}

func writeCfgAndInput(t *testing.T, dir, endpoint, ak, sk, bucket string, rows []string) (cfgPath, inputPath string) {
	t.Helper()
	inputPath = filepath.Join(dir, "input.txt")
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath = filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + endpoint,
		"scheme: http",
		"ak: " + ak,
		"sk: " + sk,
		"op_concurrency: 4",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"progress_interval: 1000",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	return
}

func TestIntegration_ModeCheck_ThreeStatesOnRealMinIO(t *testing.T) {
	endpoint, ak, sk, envBucket := skipIfNoMinIO(t)
	admin := newMinIOAdminClient(t, endpoint, ak, sk)
	bucket := envBucket
	if bucket == "" {
		bucket = fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
		setupBucket(t, admin, bucket)
		defer teardownBucket(t, admin, bucket)
	}

	ctx := context.Background()
	// Fixture:
	// 1. marker.txt: write + delete → 1 delete marker (latest = marker)
	// 2. regular.txt: write + write (2 versions, latest = regular version)
	// 3. gone.txt: write + delete marker + delete marker version (object+marker fully gone)
	if _, err := admin.PutObject(ctx, bucket, "marker.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put marker.txt v1: %v", err)
	}
	if err := admin.RemoveObject(ctx, bucket, "marker.txt", minio.RemoveObjectOptions{}); err != nil {
		t.Fatalf("Remove marker.txt (creates marker): %v", err)
	}
	if _, err := admin.PutObject(ctx, bucket, "regular.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put regular.txt: %v", err)
	}
	// gone.txt: create then fully remove by listing its versions and deleting each
	if _, err := admin.PutObject(ctx, bucket, "gone.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put gone.txt: %v", err)
	}
	{
		ch := admin.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: "gone.txt", WithVersions: true, Recursive: true,
		})
		for obj := range ch {
			if obj.Err != nil {
				break
			}
			_ = admin.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
	}

	// Now: marker.txt has 1 delete marker (latest), regular.txt has 1 regular version,
	// gone.txt is fully gone. We need version IDs for the input file.
	// List marker.txt versions to get the marker's VersionID.
	markerVID := ""
	{
		ch := admin.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: "marker.txt", WithVersions: true, Recursive: true,
		})
		for obj := range ch {
			if obj.Err != nil {
				break
			}
			if obj.IsDeleteMarker {
				markerVID = obj.VersionID
				break
			}
		}
	}
	if markerVID == "" {
		t.Fatal("could not find marker versionId for marker.txt")
	}
	// regular.txt: get latest versionID
	regularVID := ""
	{
		ch := admin.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: "regular.txt", WithVersions: true, Recursive: true,
		})
		for obj := range ch {
			if obj.Err != nil {
				break
			}
			regularVID = obj.VersionID
			break
		}
	}
	if regularVID == "" {
		t.Fatal("could not find versionId for regular.txt")
	}

	dir := t.TempDir()
	rows := []string{
		fmt.Sprintf("%s\x1fmarker.txt\x1f%s", bucket, markerVID),
		fmt.Sprintf("%s\x1fregular.txt\x1f%s", bucket, regularVID),
		fmt.Sprintf("%s\x1fgone.txt\x1fnonexistent-vid", bucket),
	}
	cfgPath, inputPath := writeCfgAndInput(t, dir, endpoint, ak, sk, bucket, rows)

	if err := run(cfgPath, "check", inputPath); err != nil {
		t.Fatalf("run check: %v", err)
	}

	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	out := string(b)
	if !strings.Contains(out, fmt.Sprintf("%s\x1fmarker.txt\x1f%s\x1fmarker", bucket, markerVID)) {
		t.Errorf("missing marker state. raw output:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("%s\x1fregular.txt\x1f%s\x1fnot_marker", bucket, regularVID)) {
		t.Errorf("missing not_marker state. raw output:\n%s", out)
	}
	// gone.txt with nonexistent vid → absent
	if !strings.Contains(out, fmt.Sprintf("%s\x1fgone.txt\x1fnonexistent-vid\x1fabsent", bucket)) {
		t.Errorf("missing absent state. raw output:\n%s", out)
	}
}

func TestIntegration_ModeDelete_ThenReCheckAbsent(t *testing.T) {
	endpoint, ak, sk, envBucket := skipIfNoMinIO(t)
	admin := newMinIOAdminClient(t, endpoint, ak, sk)
	bucket := envBucket
	if bucket == "" {
		bucket = fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
		setupBucket(t, admin, bucket)
		defer teardownBucket(t, admin, bucket)
	}

	ctx := context.Background()
	// Create 3 markers via write + delete
	keys := []string{"del1.txt", "del2.txt", "del3.txt"}
	for _, k := range keys {
		if _, err := admin.PutObject(ctx, bucket, k, strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		if err := admin.RemoveObject(ctx, bucket, k, minio.RemoveObjectOptions{}); err != nil {
			t.Fatalf("Remove %s: %v", k, err)
		}
	}
	// Collect marker VersionIDs
	var rows []string
	for _, k := range keys {
		ch := admin.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: k, WithVersions: true, Recursive: true,
		})
		for obj := range ch {
			if obj.Err != nil {
				break
			}
			if obj.IsDeleteMarker {
				rows = append(rows, fmt.Sprintf("%s\x1f%s\x1f%s", bucket, k, obj.VersionID))
				break
			}
		}
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 markers, got %d", len(rows))
	}

	dir := t.TempDir()
	cfgPath, inputPath := writeCfgAndInput(t, dir, endpoint, ak, sk, bucket, rows)

	if err := run(cfgPath, "delete", inputPath); err != nil {
		t.Fatalf("run delete: %v", err)
	}
	d, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if len(strings.Split(strings.TrimRight(string(d), "\n"), "\n")) != 3 {
		t.Errorf("expected 3 deleted rows, got: %q", d)
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "delete_failed.txt")); !os.IsNotExist(err) {
		b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.txt"))
		t.Errorf("delete_failed.txt should not exist, content: %q", string(b))
	}

	// Re-run mode check → all 3 should be absent
	cfgPath2, inputPath2 := writeCfgAndInput(t, dir, endpoint, ak, sk, bucket, rows)
	// overwrite input to point at the same rows (already written by writeCfgAndInput to input.txt)
	_ = inputPath2
	if err := run(cfgPath2, "check", inputPath); err != nil {
		t.Fatalf("re-run check: %v", err)
	}
	cr, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	out := string(cr)
	for _, r := range rows {
		// r = bkt\x1fkey\x1fvid; check_results should have r + \x1fabsent
		if !strings.Contains(out, r+"\x1fabsent") {
			t.Errorf("expected absent for %s after delete. raw:\n%s", r, out)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails / skips**

Run (no MinIO available): `cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test -tags=integration -run TestIntegration -v`
Expected: SKIP with "set MINIO_ENDPOINT/MINIO_AK/MINIO_SK to run integration test".

Run (with MinIO available — minio binary already installed locally, not docker):
```bash
# Terminal 1: start minio server (one-time per test session)
mkdir -p /tmp/minio-it-data
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /tmp/minio-it-data

# Terminal 2: run integration tests against the local minio
cd delete_marker_cleanup_tool && \
  MINIO_ENDPOINT=127.0.0.1:9000 MINIO_AK=minioadmin MINIO_SK=minioadmin \
  ~/sdk/go1.27.1/bin/go test -tags=integration -run TestIntegration -v
```
Expected: Both `TestIntegration_ModeCheck_ThreeStatesOnRealMinIO` and `TestIntegration_ModeDelete_ThenReCheckAbsent` PASS.

- [ ] **Step 3: If `marker.txt` check fails to classify as `marker`**

This is the spec's biggest known unknown (§12). On real MinIO, minio-go v7.3.0 may return:
- `(ObjectInfo{IsDeleteMarker:true}, nil)` — common case; classifier already handles via `classifyHead(info, nil)` first branch.
- `(ObjectInfo{}, MethodNotAllowed error)` — S3 spec 405 + `x-amz-delete-marker: true` header; classifier's `MethodNotAllowed` branch fires if `info.IsDeleteMarker` is true; **if `info.IsDeleteMarker` is false here**, update `classifyHead` in `classifier.go` to also return `("marker", true)` for `Code=="MethodNotAllowed"` unconditionally (drop the `info.IsDeleteMarker` condition), and add a code comment explaining: "S3 HEAD on a delete marker returns 405 + x-amz-delete-marker: true header; minio-go v7.3.0 surfaces this as MethodNotAllowed error. The 405 + version-exists combo is itself the marker signal — the IsDeleteMarker flag may not be populated in the error path. Verified against real MinIO on YYYY-MM-DD."

Then re-run the integration test to confirm `marker.txt` → `marker` classification.

- [ ] **Step 4: Commit**

```bash
git add delete_marker_cleanup_tool/integration_test.go delete_marker_cleanup_tool/classifier.go
git commit -m "test(delete_marker_cleanup_tool): real-MinIO end-to-end integration test"
```

If classifier was modified in Step 3, also re-run unit tests:
```bash
cd delete_marker_cleanup_tool && ~/sdk/go1.27.1/bin/go test ./... -v
```
Expected: PASS — adjust classifier unit tests if the change broke any.

---

## Task 11: README + AGENTS.md + config.yaml template

**Files:**
- Create: `delete_marker_cleanup_tool/README.md`
- Create: `delete_marker_cleanup_tool/AGENTS.md`
- Create: `delete_marker_cleanup_tool/config.yaml`
- No tests — docs only.

- [ ] **Step 1: Write `README.md`**

Mirror lister's `README.md` structure: tool purpose ("consumes delete_marker_list_tool output, two modes check/delete"), build commands (macOS `~/sdk/go1.27.1/bin/go build`, Linux cross-compile `GOOS=linux GOARCH=amd64`), run commands (one per mode), flag table (`-c`/`-mode`/`-input`), config field table (drop lister's `list_type`/`list_concurrency`, add `op_concurrency`), output file tables for each mode, failure handling section ("no retry, no checkpoint, no graceful shutdown"), integration test section (build tag `-tags=integration` + env vars `MINIO_ENDPOINT`/`MINIO_AK`/`MINIO_SK`/`MINIO_BUCKET`), known limitations (HEAD-on-delete-marker minio-go behavior verified by Task 10 integration test; key with `\n` tears; kill -9 loses tail lines; same-second startup collision; mode delete can delete a real version if lister misclassified).

- [ ] **Step 2: Write `AGENTS.md`**

Mirror lister's `AGENTS.md` structure: project intent, tech stack (minio-go v7.3.0 pinned), module layout (including `integration_test.go`), key invariants (numbered list: `\x1f` separator, `null` version_id passthrough, 3-state classification rules, mode-by-flag-only, NoSuchVersion=success, no retry/checkpoint/graceful-exit, `output_dir_timestamp` sibling suffix, run.log first-open + sk mask, lazy failed files, basename-based URL decode for `invalid_keys.txt`), constraints, known leftover items (HEAD-on-delete-marker minio-go behavior verified against real MinIO — record the observed path in classifier.go comment + integration test), related docs links.

- [ ] **Step 3: Write `config.yaml`**

Copy lister's `config.yaml`, remove `list_type`/`list_concurrency`, add `op_concurrency: 8` with comments.

- [ ] **Step 4: Commit**

```bash
git add delete_marker_cleanup_tool/README.md delete_marker_cleanup_tool/AGENTS.md delete_marker_cleanup_tool/config.yaml
git commit -m "docs(delete_marker_cleanup_tool): README + AGENTS + config template"
```

---

## Self-Review Notes

**Spec coverage check (section by section):**
- §1 intent → covered by README/AGENTS in Task 10; `run` flow in Task 9.
- §2 flags → Task 9 main.go; flag table documented in Task 10 README.
- §3 mode check three-state → Task 6 classifier + handleCheck.
- §3 mode delete NoSuchVersion=success → Task 8 handleDelete.
- §4.1 trust lister output → mode delete does no re-HEAD, just deletes (Task 8).
- §4.2 `null` version_id passthrough → `ParseRow` test (Task 5) + classifier test (Task 6).
- §4.3 bucket from each row → `ParseRow` returns `Row.Bkt` (Task 5); no `-bkt` flag (Task 9).
- §4.4 basename-based URL decode → `ParseRow` (Task 5) + dedicated test (Task 7).
- §4.5 four-field check_results row → `Output.WriteCheckResult` (Task 3).
- §4.6 torn line handling → Task 7.
- §5 no retry/checkpoint/graceful-exit → no signal handler in main (Task 9); failed files lazy (Task 3).
- §6 output files per mode → `NewOutput(cfg, mode)` (Task 3).
- §7 config fields → Task 1.
- §8 progress lines (marker/not_marker/absent/failed visible) → Task 4 progress.go.
- §8 preflight contract (transport error exits, exists=false warns + continues) → Task 9.
- §9 non-goals → enforced by absence (no `-yes`, no `-dry-run`, no retry).
- §10 design decisions → all reflected.
- §12 known limitations → documented in README (Task 10) and called out where they affect tests.

**Type consistency check:**
- `Row` struct used in Task 5 (defined), Task 6 (handleCheck param), Task 8 (handleDelete param) — consistent.
- `Output.WriteCheckResult` / `WriteDeleted` / `WriteHeadFailed` / `WriteDeleteFailed` / `WriteHeadFailedLog` / `WriteDeleteFailedLog` — defined in Task 3 interfaces, used in Task 6/8 handlers — consistent.
- `Stats.IncrProcessed` / `IncrMarker` / `IncrNotMarker` / `IncrAbsent` / `IncrHeadFailed` / `IncrDeleted` / `IncrDeleteFailed` — defined in Task 4, used in Task 6/8 — consistent.
- `ProgressPrinter.MaybePrintCheck` / `MaybePrintDelete` — defined in Task 4, used in Task 5 worker loop — consistent.
- `S3Client.StatObject` / `DeleteObject` / `BucketExists` — defined in Task 2, used in Task 6/8/9 — consistent.
- `RunWorkers(ctx, cfg, mode, inputPath, out, stats, progress)` signature — defined in Task 5, called in Task 9 — consistent.

**Proportion check:** Plan length is comparable to lister's plan — task count 11 (including the real-MinIO integration test in Task 10). Code blocks are signatures + key algorithm snippets (classifier switch, handleDelete error classification, integration test fixture setup) that the signature and tests alone don't determine. Acceptable.

**Review Focus coverage:**
1. HEAD 405 + x-amz-delete-marker → Task 6 classifier test `TestClassifyHead_MethodNotAllowedWithMarkerFlag` + Task 2 fake server + Task 6 Step 6 explicit note to inspect minio-go behavior.
2. Torn input line → Task 7 `TestRunWorkers_TornLineSkippedAndLogged`.
3. version_id "null" passthrough → Task 5 `TestParseRow_NullVersionIdPassesThrough`.
4. NoSuchVersion on delete → Task 5 `TestRunWorkers_ModeDelete_NoSuchVersionCountsAsSuccess` + Task 8.
5. invalid_keys.txt URL decode → Task 7 `TestRunWorkers_InvalidKeysInputURLDecodesKey`.

All five pinned.
