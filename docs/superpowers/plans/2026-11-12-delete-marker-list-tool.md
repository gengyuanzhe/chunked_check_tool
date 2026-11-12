# delete_marker_list_tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal S3 sibling tool that lists every delete marker in a versioned bucket, writing a machine-readable result file for a downstream deletion tool to consume.

**Architecture:** Single Go module in a new sibling directory. Two listing modes selected by `list_type`: mode 1 = single goroutine consuming minio-go's `Client.ListObjects` channel with `WithVersions: true, Recursive: true`; mode 2 = BFS with `list_concurrency` workers, each consuming a delimited versioned listing per prefix and enqueuing common prefixes. Output goes through per-file writer goroutines with timed flush, mirroring chunked_check_tool's conventions. No retries, no journal, no graceful shutdown.

**Tech Stack:** Go 1.27, `github.com/minio/minio-go/v7 v7.3.0` (pinned to match chunked_check_tool), `gopkg.in/yaml.v3 v3.0.1`.

**Spec:** `docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md` (this plan argues from the spec; executors read both).

## Global Constraints

- Module path `delete_marker_list_tool`, new directory `/Users/gengyuanzhe/code/S3/golang/chunked_check_tool/delete_marker_list_tool/`.
- `go.mod`: `module delete_marker_list_tool`, `go 1.27`, requires `github.com/minio/minio-go/v7 v7.3.0` + `gopkg.in/yaml.v3 v3.0.1` (mirror list_only/go.mod exactly).
- Single `package main`, one file per responsibility (config.go / s3client.go / lister.go / output.go / stats.go / progress.go / main.go + tests).
- Result row separator is `\x1f` (ASCII Unit Separator). Row shape: `bkt\x1fkey\x1fversion_id`. Never `|`.
- `version_id` may be the literal string `"null"` (S3 null version); pass through verbatim, never treat as empty.
- No `NodePool`, no node isolation/recovery, no journal, no resume flags, no signal handler. Failure → write `list_failed.txt`/`list_failed.log`, continue.
- No `is_check`/multipart/backup config fields — only the subset in spec §7.
- `list_type` accepts only `1` or `2`; any other value fails at startup with a clear message.
- `output_dir_timestamp` sibling-suffix behavior must match chunked_check_tool/config.go:93-106 verbatim (trim trailing `/`, append `_<YYYYMMDD_HHMMSS>`).
- `run.log` is opened first, before any other file; `mwOut = io.MultiWriter(os.Stdout, runLog)`, `mwErr = io.MultiWriter(os.Stderr, runLog)`; `sk` masked as `***` in config snapshot.
- Tests use `net/http/httptest` for fake S3 (mirror chunked_check_tool's fakes3 pattern). No real S3 calls in tests.

## Review Focus

Five input classes / failure modes the spec implies but no task's tests exercise that are most likely to bite a person using this software:

1. **A common-prefix entry in the versioned channel** — minio-go emits it as `ObjectInfo{Key: "photos/"}` with empty VersionID/ETag and `IsDeleteMarker=false`. A lister that doesn't distinguish prefixes from real versions will either enqueue `"photos/"` as a marker-less prefix (re-scan, possibly infinite loop) or write it as a marker. Pinned by Task 5.
2. **A delete marker that is also the latest version of its key** — must appear in results with its real VersionID, not skipped. The channel emits these with `IsLatest=true, IsDeleteMarker=true`. Pinned by Task 5 (covered by the general "all markers land in results" test, which uses a key whose latest version is the marker).
3. **A failed listing mid-prefix** — the prefix lands in `list_failed.txt` (raw prefix, one line) and `list_failed.log` (slog with HTTP code + error), listing continues for other prefixes, already-flushed markers for the failed prefix are kept. Pinned by Task 7.
4. **A key containing `\x1f`** — the marker must NOT silently land in `delete_markers.txt` with corrupted fields; it goes to `invalid_keys.txt` with the key URL-encoded (`url.QueryEscape`). Pinned by Task 6.
5. **`list_type: 3` (or any non-1/2 value) in config** — startup must abort with a clear message, not silently fall through to some default mode. Pinned by Task 2.

---

## File Structure

| File | Responsibility |
|---|---|
| `go.mod` | Module definition, pinned deps. |
| `config.go` | `Config` struct, `LoadConfig(path)`, timestamp-suffix rewrite, validation. |
| `s3client.go` | `S3Client` (minio client + transport tuning), `ListVersions(ctx, bkt, prefix, recursive) <-chan minio.ObjectInfo`, `BucketExists`. |
| `lister.go` | `Lister` driving both modes; BFS queue + inflight WaitGroup for mode 2; common-prefix classification. |
| `output.go` | `Output`: per-file writer goroutines (delete_markers, invalid_keys, list_failed), slog listLogger, `FlushAndWait`, 3-second timed flush. |
| `stats.go` | `Stats` (atomics) + `Snapshot` + `PrintSummary`. |
| `progress.go` | `ProgressPrinter` (per-worker local counter, threshold-gated). |
| `main.go` | Flag parsing, run.log setup, config snapshot print, orchestration, summary. |
| `config_test.go` | Config validation, timestamp suffix, list_type rejection. |
| `s3client_test.go` | httptest fake S3; ListVersions happy path + error path. |
| `lister_test.go` | Mode 1 + mode 2 end-to-end against fake S3; common-prefix classification; failure recording. |
| `output_test.go` | Row formatting, `\x1f` handling, flush, list_failed contract. |
| `main_test.go` | Run-mode end-to-end (mirror chunked_check_tool/main_test.go pattern). |

---

## Task 1: Module scaffold + go.mod + config

**Files:**
- Create: `delete_marker_list_tool/go.mod`
- Create: `delete_marker_list_tool/config.go`
- Create: `delete_marker_list_tool/config_test.go`

**Interfaces:**
- Produces: `type Config struct { Endpoints []string; Scheme string; AK, SK string; ListType int; ListConcurrency int; OutputDir string; OutputDirTimestamp bool; ProgressInterval int; DialTimeout int64; ResponseHeaderTimeout int64 }`, `func LoadConfig(path string) (*Config, error)`.

- [ ] **Step 1: Write `go.mod`**

Mirror list_only/go.mod exactly:

```
module delete_marker_list_tool

go 1.27

require (
	github.com/minio/minio-go/v7 v7.3.0
	gopkg.in/yaml.v3 v3.0.1
)
```

Run `~/sdk/go1.27.1/bin/go mod tidy` to populate indirects (env: `export PATH=~/sdk/go1.27.1/bin:$PATH`).

- [ ] **Step 2: Write the failing test**

`config_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
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
list_type: 2
list_concurrency: 4
output_dir: ./out
`)
	cfg, err := LoadConfig(p)
	if err != nil { t.Fatal(err) }
	if cfg.ListType != 2 { t.Errorf("ListType=%d want 2", cfg.ListType) }
	if cfg.ListConcurrency != 4 { t.Errorf("ListConcurrency=%d want 4", cfg.ListConcurrency) }
	if cfg.OutputDirTimestamp != true { t.Errorf("OutputDirTimestamp default should be true") }
	if cfg.Scheme != "http" { t.Errorf("Scheme=%q want http", cfg.Scheme) }
	// timestamp suffix applied to OutputDir
	if cfg.OutputDir == "./out" { t.Errorf("OutputDir not rewritten with timestamp suffix") }
}

func TestLoadConfig_RejectsInvalidListType(t *testing.T) {
	for _, v := range []int{0, 3, -1} {
		p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
list_type: `+itoa(v)+`
`)
		_, err := LoadConfig(p)
		if err == nil { t.Errorf("list_type=%d: expected error, got nil", v) }
	}
}

func TestLoadConfig_OutputDirTimestampSiblingSuffix(t *testing.T) {
	p := writeConfigFile(t, `
endpoints: ["1.2.3.4:80"]
ak: a
sk: s
output_dir: ./out/
output_dir_timestamp: true
`)
	cfg, err := LoadConfig(p)
	if err != nil { t.Fatal(err) }
	base := filepath.Base(cfg.OutputDir)
	// ./out/ → ./out_<stamp> (not ./out/_<stamp>)
	if filepath.Base(filepath.Dir(cfg.OutputDir)) != "." {
		t.Errorf("OutputDir=%q should have parent '.'", cfg.OutputDir)
	}
	if base == "out" || !strings.HasPrefix(base, "out_") {
		t.Errorf("OutputDir base=%q want out_<stamp>", base)
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
	if err != nil { t.Fatal(err) }
	if cfg.OutputDir != "./fixed" { t.Errorf("OutputDir=%q want ./fixed", cfg.OutputDir) }
}
```

(`itoa` is a tiny test helper using `strconv.Itoa`; define inline or use `strconv.Itoa` directly. Use `strconv.Itoa`.)

- [ ] **Step 3: Run test to verify it fails**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestLoadConfig -v`
Expected: FAIL (no config.go yet / LoadConfig undefined).

- [ ] **Step 4: Implement `config.go`**

Signature: `type Config struct { ... }` and `func LoadConfig(path string) (*Config, error)`. Validation: endpoints ≥1, ak/sk non-empty, list_type ∈ {1,2}. Defaults (when zero-valued): scheme=`http`, list_type=`2`, list_concurrency=`8`, output_dir=`.`, output_dir_timestamp=`true`, progress_interval=`5000`, dial_timeout=`10`. Timestamp suffix logic copies chunked_check_tool/config.go:93-106 logic exactly: if `OutputDirTimestamp`, trim trailing `/` (or `\`), append `_<YYYYMMDD_HHMMSS>` via `time.Now().Format("20060102_150405")`. Use `yaml.v3` strict decode (yaml.Unmarshal into struct; unknown fields ignored — DO NOT use `KnownFields(true)` because users may reuse chunked_check_tool's config.yaml which has many unknown fields).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestLoadConfig -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add delete_marker_list_tool/go.mod delete_marker_list_tool/go.sum delete_marker_list_tool/config.go delete_marker_list_tool/config_test.go
git commit -m "feat(delete_marker_list_tool): config + module scaffold"
```

---

## Task 2: S3 client — `ListVersions` channel wrapper + `BucketExists`

**Files:**
- Create: `delete_marker_list_tool/s3client.go`
- Create: `delete_marker_list_tool/s3client_test.go`

**Interfaces:**
- Consumes: `Config` from Task 1.
- Produces: `type S3Client struct { ... }`, `func NewS3Client(cfg *Config, endpoint string) (*S3Client, error)`, `func (c *S3Client) ListVersions(ctx context.Context, bucket, prefix string, recursive bool) <-chan minio.ObjectInfo`, `func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error)`.

- [ ] **Step 1: Write the failing test**

`s3client_test.go` — fake S3 server returns a `ListBucketResult` (versions) XML. Use `net/http/httptest`. Mirror chunked_check_tool/s3client_test.go:183 pattern but the response is `?versions` XML (`<ListVersionsResult>` with `<Version>` and `<DeleteMarker>` children). Assert: (a) one DeleteMarker entry arrives with `IsDeleteMarker=true, VersionID="v1", Key="a.txt"`; (b) one regular Version arrives with `IsDeleteMarker=false`; (c) when the request sets `versions` query, server sees it.

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7"
)

func newFakeVersionedS3(t *testing.T, versionsXML string) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("versions") == "" {
			t.Errorf("request missing versions= query: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(versionsXML))
	}))
	return srv, srv.URL
}

const versionsXMLOneMarker = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker>
<IsTruncated>false</IsTruncated>
<Version><Key>a.txt</Key><VersionId>regular-v1</VersionId><IsLatest>false</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>"abc"</ETag><Size>5</Size></Version>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-v2</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-02T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`

func TestListVersions_OneMarkerOneVersion(t *testing.T) {
	srv, url := newFakeVersionedS3(t, versionsXMLOneMarker)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{url}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, url)
	if err != nil { t.Fatal(err) }
	ch := c.ListVersions(context.Background(), "testbucket", "", true)
	var got []minio.ObjectInfo
	for obj := range ch {
		if obj.Err != nil { t.Fatalf("unexpected err: %v", obj.Err) }
		got = append(got, obj)
	}
	if len(got) != 2 { t.Fatalf("got %d entries, want 2", len(got)) }
	var markerCount, regularCount int
	for _, o := range got {
		if o.IsDeleteMarker { markerCount++ } else { regularCount++ }
	}
	if markerCount != 1 || regularCount != 1 {
		t.Errorf("markerCount=%d regularCount=%d want 1/1", markerCount, regularCount)
	}
}
```

Also a second test `TestListVersions_ServerError` returning HTTP 500 — assert the channel emits one entry with `obj.Err != nil` and closes.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestListVersions -v`
Expected: FAIL (S3Client undefined).

- [ ] **Step 3: Implement `s3client.go`**

`NewS3Client(cfg, endpoint)` builds a `*minio.Client` with: endpoint, ak/sk, `Secure: cfg.Scheme == "https"`, `BucketLookup: minio.BucketLookupPath` (chunked_check_tool uses Auto but path-style is safer for ip:port endpoints; verify against the fake server which uses path-style). HTTP transport matches chunked_check_tool: `MaxIdleConnsPerHost: 32`, `IdleConnTimeout: 90s`, optional `DialContext` with `dial_timeout`, optional `ResponseHeaderTimeout`, `TLSClientConfig{InsecureSkipVerify: true}` when https. No custom transport wrappers (no mpOffsetTransport — that's multipart-only).

`ListVersions(ctx, bucket, prefix, recursive)` returns `c.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive, WithVersions: true})`. The channel already emits `minio.ObjectInfo` including `Err` entries; pass through verbatim.

`BucketExists` delegates to `c.client.BucketExists(ctx, bucket)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestListVersions -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/s3client.go delete_marker_list_tool/s3client_test.go
git commit -m "feat(delete_marker_list_tool): S3Client with versioned ListObjects wrapper"
```

---

## Task 3: Stats + Progress (minimal)

**Files:**
- Create: `delete_marker_list_tool/stats.go`
- Create: `delete_marker_list_tool/progress.go`
- Create: `delete_marker_list_tool/stats_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type Stats struct { ... }` with atomic counters `scannedVersions`, `deleteMarkers`, `listFailed`, `listCalls` + `listLatencySumNs` + `totalDurationNs`; `func (s *Stats) Snapshot() StatsSnapshot`; `func (s *Stats) PrintSummary(w io.Writer)`; `type ProgressPrinter struct { ... }`, `func NewProgressPrinter(w io.Writer) *ProgressPrinter`, `func (p *ProgressPrinter) MaybePrint(s *Stats, label string, count int)`, `func (p *ProgressPrinter) SetQueueSnapshotProvider(fn func() QueueSnapshot)`.

- [ ] **Step 1: Write the failing test**

`stats_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestStatsSnapshot(t *testing.T) {
	var s Stats
	s.scannedVersions.Add(100)
	s.deleteMarkers.Add(7)
	s.listFailed.Add(2)
	snap := s.Snapshot()
	if snap.ScannedVersions != 100 { t.Errorf("ScannedVersions=%d want 100", snap.ScannedVersions) }
	if snap.DeleteMarkers != 7 { t.Errorf("DeleteMarkers=%d want 7", snap.DeleteMarkers) }
	if snap.ListFailed != 2 { t.Errorf("ListFailed=%d want 2", snap.ListFailed) }
}

func TestPrintSummaryContainsAllFields(t *testing.T) {
	var s Stats
	s.deleteMarkers.Add(42)
	s.scannedVersions.Add(1000)
	s.listFailed.Add(3)
	var buf bytes.Buffer
	s.PrintSummary(&buf)
	out := buf.String()
	for _, want := range []string{"delete_markers", "scanned_versions", "list_failed", "total_sec"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q: %s", want, out)
		}
	}
}

func TestProgressPrinterMaybePrintThreshold(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgressPrinter(&buf)
	var s Stats
	// First call at count=1 with interval default 5000 should NOT print (below threshold).
	// MaybePrint's contract: caller passes count since last print; print only when count >= 5000.
	p.MaybePrint(&s, "scanned", 1)
	if buf.Len() != 0 { t.Errorf("expected no print below threshold, got %q", buf.String()) }
	p.MaybePrint(&s, "scanned", 5000)
	if buf.Len() == 0 { t.Errorf("expected progress line at threshold") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run "TestStats|TestPrintSummary|TestProgressPrinter" -v`
Expected: FAIL (Stats / ProgressPrinter undefined).

- [ ] **Step 3: Implement `stats.go` and `progress.go`**

`stats.go`: 4 atomic.Int64 counters + `listLatencySumNs` + `totalDurationNs`. `Snapshot()` returns a value-typed `StatsSnapshot`. `PrintSummary(w)` writes a `=== summary ===` block with `scanned_versions`, `delete_markers`, `list_failed`, `list_calls`, `list_avg_latency_ms`, `total_sec`. Match chunked_check_tool/stats.go:169-209 output shape.

`progress.go`: `ProgressPrinter` with `mu sync.Mutex`, `w io.Writer`, `queueSnapshot func() QueueSnapshot`. `MaybePrint(s, label, count)` prints one `[progress] scanned=<n> markers=<n> [q=<n> inflight=<n>]` line iff `count >= 5000` (interval comes from a field set by main; default 5000). `QueueSnapshot` struct has `Prefix int` (BFS queue len) + `Inflight int` only (no ObjCh — no checker in this tool).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run "TestStats|TestPrintSummary|TestProgressPrinter" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/stats.go delete_marker_list_tool/progress.go delete_marker_list_tool/stats_test.go
git commit -m "feat(delete_marker_list_tool): stats + progress printer"
```

---

## Task 4: Output — per-file writers, `\x1f` rows, list_failed, timed flush

**Files:**
- Create: `delete_marker_list_tool/output.go`
- Create: `delete_marker_list_tool/output_test.go`

**Interfaces:**
- Consumes: `Config` from Task 1.
- Produces: `type Output struct { ... }`, `func NewOutput(cfg *Config, bucket string) (*Output, error)`, `func (o *Output) WriteDeleteMarker(key, versionID string)`, `func (o *Output) WriteInvalidKey(key, versionID string)`, `func (o *Output) WriteListFailed(prefix string)`, `func (o *Output) WriteListFailedLog(prefix string, statusCode int, s3Code, reqID string, err error)`, `func (o *Output) FlushAndWait()`, `func (o *Output) Close() error`.

- [ ] **Step 1: Write the failing test**

`output_test.go`:

```go
package main

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestOutput(t *testing.T) (*Output, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	cfg := &Config{OutputDir: dir, OutputDirTimestamp: false}
	o, err := NewOutput(cfg, "testbucket")
	if err != nil { t.Fatal(err) }
	return o, dir
}

func TestWriteDeleteMarker_RowShape(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteDeleteMarker("photos/a.jpg", "v1")
	o.WriteDeleteMarker("photos/b.jpg", "null")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "delete_markers.txt"))
	if err != nil { t.Fatal(err) }
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 2 { t.Fatalf("got %d lines, want 2: %q", len(lines), string(b)) }
	want0 := "testbucket\x1fphotos/a.jpg\x1fv1"
	want1 := "testbucket\x1fphotos/b.jpg\x1fnull"
	if lines[0] != want0 { t.Errorf("line0=%q want %q", lines[0], want0) }
	if lines[1] != want1 { t.Errorf("line1=%q want %q", lines[1], want1) }
}

func TestWriteInvalidKey_UrlEncodesKey(t *testing.T) {
	o, dir := newTestOutput(t)
	// key contains \x1f — must be URL-encoded and routed to invalid_keys.txt
	o.WriteInvalidKey("a\x1fb", "v1")
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
	if err != nil { t.Fatal(err) }
	line := strings.TrimRight(string(b), "\n")
	// "a\x1fb" → url.QueryEscape → "a%1Fb"
	want := "testbucket\x1fa%1Fb\x1fv1"
	if line != want { t.Errorf("got %q want %q", line, want) }
	// delete_markers.txt must NOT exist or be empty
	if _, err := os.Stat(filepath.Join(dir, "delete_markers.txt")); err == nil {
		t.Errorf("delete_markers.txt should not contain the \\x1f key")
	}
}

func TestWriteListFailed_Contract(t *testing.T) {
	o, dir := newTestOutput(t)
	o.WriteListFailed("photos/")    // non-empty prefix
	o.WriteListFailed("")            // empty prefix → empty line
	o.FlushAndWait()
	o.Close()
	b, err := os.ReadFile(filepath.Join(dir, "list_failed.txt"))
	if err != nil { t.Fatal(err) }
	// Two lines: "photos/" and ""
	want := "photos/\n\n"
	if string(b) != want { t.Errorf("got %q want %q", string(b), want) }
}

func TestListFailed_LazilyCreated(t *testing.T) {
	o, dir := newTestOutput(t)
	o.FlushAndWait()
	o.Close()
	// No WriteListFailed calls → file should not exist
	if _, err := os.Stat(filepath.Join(dir, "list_failed.txt")); !os.IsNotExist(err) {
		t.Errorf("list_failed.txt should not exist when no failures; err=%v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestWrite -v`
Expected: FAIL (Output / NewOutput undefined).

- [ ] **Step 3: Implement `output.go`**

`Output` fields: `dir string`, `bucket string`, three channels (`deleteMarkerCh chan [2]string`, `invalidKeysCh chan [2]string`, `listFailedCh chan string`), three `*bufio.Writer`s, three `*os.File`s, three sync.WaitGroup (one per writer goroutine), a `*slog.Logger listLogger` + its `*os.File`, a `flushWG sync.WaitGroup` over all writer goroutines. `NewOutput(cfg, bucket)`: `os.MkdirAll(cfg.OutputDir, 0755)`, open `list_failed.log` with `O_CREATE|O_WRONLY|O_APPEND` (do NOT open `list_failed.txt` here — lazily opened on first WriteListFailed call; protect with a mutex). Start three writer goroutines: each reads from its channel, writes `\n`-terminated lines, flushes on a 3-second ticker (channel-scoped) or when channel is closed. `WriteDeleteMarker(key, versionID)` sends `[2]string{bucket, key, versionID}` — but check `strings.Contains(key, "\x1f")` first; if true, route to invalidKeysCh with `url.QueryEscape(key)`; else send to deleteMarkerCh. `WriteInvalidKey` is the explicit-escape path (caller already knows). `WriteListFailed(prefix)` lazily opens `list_failed.txt` if not open. `WriteListFailedLog` calls `listLogger.Error(...)` with structured attrs (bucket, prefix, http_code, s3_code, req_id, err). `FlushAndWait()` closes all channels (no more sends from callers), waits for writer goroutines which then drain and `Flush()` their buffered writers. `Close()` closes the underlying files.

Row writing helper: `writeRow(w *bufio.Writer, bkt, key, versionID string)` does `w.WriteString(bkt); w.WriteByte('\x1f'); w.WriteString(key); w.WriteByte('\x1f'); w.WriteString(versionID); w.WriteByte('\n')`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestWrite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delete_marker_list_tool/output.go delete_marker_list_tool/output_test.go
git commit -m "feat(delete_marker_list_tool): output with \\x1f rows + list_failed contract"
```

---

## Task 5: Lister — mode 1 + mode 2 + common-prefix classification

**Files:**
- Create: `delete_marker_list_tool/lister.go`
- Create: `delete_marker_list_tool/lister_test.go`

**Interfaces:**
- Consumes: `S3Client` from Task 2, `Output` from Task 4, `Stats` from Task 3, `Config` from Task 1.
- Produces: `type Lister struct { ... }`, `func NewLister(cfg *Config, client *S3Client, out *Output, stats *Stats, progress *ProgressPrinter) *Lister`, `func (l *Lister) Run(ctx context.Context, bucket, prefix string) error`.

- [ ] **Step 1: Write the failing test — mode 1 happy path**

`lister_test.go` — fake S3 returns one marker + one version, plus a common prefix `sub/` containing another marker. Mode 1 lists recursively (no delimiter) so common prefixes appear as bare `ObjectInfo{Key:"sub/"}` entries; classify them and skip. Assert `delete_markers.txt` has exactly the 2 markers (`a.txt`/dm + `sub/c.txt`/dm), NOT the common prefix itself.

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

const mode1XML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<Version><Key>a.txt</Key><VersionId>reg-a</VersionId><IsLatest>false</IsLatest><ETag>"e"</ETag><Size>1</Size></Version>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`

func TestLister_Mode1_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(mode1XML))
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 1, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.URL)
	if err != nil { t.Fatal(err) }
	out, err := NewOutput(cfg, "b")
	if err != nil { t.Fatal(err) }
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "b", ""); err != nil { t.Fatal(err) }
	out.FlushAndWait()
	out.Close()
	b, err := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	if err != nil { t.Fatal(err) }
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 { t.Fatalf("got %d markers, want 2: %q", len(lines), b) }
	want := map[string]bool{"b\x1fa.txt\x1fdm-a": false, "b\x1fsub/c.txt\x1fdm-c": false}
	for _, l := range lines {
		if _, ok := want[l]; !ok { t.Errorf("unexpected line %q", l) } else { want[l] = true }
	}
	for k, seen := range want { if !seen { t.Errorf("missing line %q", k) } }
}
```

(Helper `readFile(t, p) (string, error)` wraps `os.ReadFile`. Define inline.)

- [ ] **Step 2: Write the failing test — mode 2 BFS with common prefix**

```go
func TestLister_Mode2_BFSWithCommonPrefix(t *testing.T) {
	// Two responses keyed by the query prefix:
	//   prefix="" → returns DeleteMarker(a.txt) + CommonPrefixes(sub/)
	//   prefix="sub/" → returns DeleteMarker(sub/c.txt)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<CommonPrefixes><Prefix>sub/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
		} else {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix>sub/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
		}
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 2,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, err := NewS3Client(cfg, srv.URL)
	if err != nil { t.Fatal(err) }
	out, err := NewOutput(cfg, "b")
	if err != nil { t.Fatal(err) }
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "b", ""); err != nil { t.Fatal(err) }
	out.FlushAndWait()
	out.Close()
	b, _ := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	lines := strings.Split(strings.TrimRight(b, "\n"), "\n")
	if len(lines) != 2 { t.Fatalf("got %d markers, want 2: %q", len(lines), b) }
	want := map[string]bool{"b\x1fa.txt\x1fdm-a": false, "b\x1fsub/c.txt\x1fdm-c": false}
	for _, l := range lines {
		if _, ok := want[l]; !ok { t.Errorf("unexpected line %q", l) } else { want[l] = true }
	}
	for k, seen := range want { if !seen { t.Errorf("missing line %q", k) } }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestLister -v`
Expected: FAIL (Lister / NewLister / Run undefined).

- [ ] **Step 4: Implement `lister.go`**

Two paths in `Run(ctx, bucket, prefix)`:

`if cfg.ListType == 1`: single goroutine consumes `client.ListVersions(ctx, bucket, prefix, true)` (recursive=true). For each `minio.ObjectInfo`:
- If `obj.Err != nil`: call `out.WriteListFailed(prefix)` + `out.WriteListFailedLog(prefix, httpCode, s3Code, reqID, obj.Err)`, `stats.listFailed.Add(1)`, return error (mode 1 has only one stream; abort).
- Else if `isCommonPrefix(obj)`: skip.
- Else if `obj.IsDeleteMarker`: `out.WriteDeleteMarker(obj.Key, obj.VersionID)`, `stats.deleteMarkers.Add(1)`.
- Else: skip (regular version, not a marker).
- Always: `stats.scannedVersions.Add(1)`, `progress.MaybePrint(&stats, "scanned", localCount)`.

`else` (mode 2): BFS.
- `queue := NewQueue()` (simple unbounded queue with `sync.Cond` + ctx-aware Pop; copy the shape from chunked_check_tool/queue.go but inline it into lister.go as a small struct to avoid an extra file).
- `var inflight sync.WaitGroup`.
- `seed := prefix`; `inflight.Add(1)` BEFORE `queue.Push(seed)`.
- `list_concurrency` worker goroutines, each bound to `cfg.Endpoints[workerIdx % len(cfg.Endpoints)]` (atomic counter per assignment, not NodePool).
- Worker loop: `p := queue.Pop(ctx)`; if `ctx.Err != nil` return; `ch := client.ListVersions(ctx, bucket, p, false)` (recursive=false, delimited). For each obj:
  - `obj.Err != nil`: `out.WriteListFailed(p)` + `out.WriteListFailedLog(p, ...)`, `stats.listFailed.Add(1)`, `inflight.Done()`, return.
  - `isCommonPrefix(obj)`: `inflight.Add(1)` BEFORE `queue.Push(obj.Key)` (termination invariant — chunked_check_tool/AGENTS.md §4.7).
  - `obj.IsDeleteMarker`: `out.WriteDeleteMarker(obj.Key, obj.VersionID)`, `stats.deleteMarkers.Add(1)`.
  - else: skip.
  - `stats.scannedVersions.Add(1)`.
  - End of channel: `inflight.Done()`, loop to next Pop.
- After workers launched, main goroutine waits: `inflight.Wait()` then cancels a "done" signal.
- Use a context-with-cancel: cancel when inflight reaches zero to unblock any remaining `queue.Pop` blocked workers.

`isCommonPrefix(o minio.ObjectInfo) bool`: `return o.VersionID == "" && !o.IsDeleteMarker && o.ETag == "" && strings.HasSuffix(o.Key, "/")` (spec §4.2).

Extract HTTP code / S3 code / reqID from `obj.Err` via `minio.ToErrorResponse` (or `errors.As`). If `obj.Err` is not a `minio.ErrorResponse`, write 0/""/err.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestLister -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add delete_marker_list_tool/lister.go delete_marker_list_tool/lister_test.go
git commit -m "feat(delete_marker_list_tool): lister with mode 1 + mode 2 BFS"
```

---

## Task 6: Lister — `\x1f`-in-key routing + failure recording (Review Focus #3, #4)

**Files:**
- Modify: `delete_marker_list_tool/lister.go` (add tests only; no implementation change expected — Task 4 already routes `\x1f` keys via `WriteDeleteMarker`)
- Create: `delete_marker_list_tool/lister_edge_test.go`

**Interfaces:**
- Consumes: same as Task 5.

- [ ] **Step 1: Write the failing test — `\x1f` in key routed to invalid_keys.txt**

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestLister_KeyWithUnitSeparator_RoutedToInvalid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a\x1Fb</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
	}))
	defer srv.Close()
	// NOTE: the XML above has a literal "\x1F" inside <Key>; the XML parser will read it as
	// the actual control character. (If the parser escapes it differently, switch to CDATA.)
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 1, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, _ := NewS3Client(cfg, srv.URL)
	out, _ := NewOutput(cfg, "b")
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "b", ""); err != nil { t.Fatal(err) }
	out.FlushAndWait()
	out.Close()
	// delete_markers.txt must be empty or missing
	if b, err := readFile(t, filepath.Join(dir, "delete_markers.txt")); err == nil && strings.TrimSpace(b) != "" {
		t.Errorf("delete_markers.txt should be empty, got %q", b)
	}
	// invalid_keys.txt must contain url-encoded key
	b, err := readFile(t, filepath.Join(dir, "invalid_keys.txt"))
	if err != nil { t.Fatal(err) }
	want := "b\x1fa%1Fb\x1fv1"
	if !strings.Contains(b, want) { t.Errorf("invalid_keys.txt missing %q: got %q", want, b) }
}
```

- [ ] **Step 2: Write the failing test — failure recorded, other prefixes continue**

```go
func TestLister_Mode2_FailureRecordedOtherPrefixesContinue(t *testing.T) {
	// prefix="" returns DeleteMarker(a.txt) + CommonPrefixes(sub/, bad/)
	// prefix="sub/" returns DeleteMarker(sub/c.txt)
	// prefix="bad/" returns HTTP 500
	calls := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		calls[prefix]++
		w.Header().Set("Content-Type", "application/xml")
		switch prefix {
		case "":
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest></DeleteMarker>
<CommonPrefixes><Prefix>sub/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>bad/</Prefix></CommonPrefixes>
</ListVersionsResult>`))
		case "sub/":
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>b</Name><Prefix>sub/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>sub/c.txt</Key><VersionId>dm-c</VersionId><IsLatest>true</IsLatest></DeleteMarker>
</ListVersionsResult>`))
		case "bad/":
			http.Error(w, "InternalError", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	cfg := &Config{
		Endpoints: []string{srv.URL}, Scheme: "http", AK: "a", SK: "s",
		ListType: 2, ListConcurrency: 1,
		OutputDir: filepath.Join(t.TempDir(), "out"), OutputDirTimestamp: false,
	}
	dir := cfg.OutputDir
	client, _ := NewS3Client(cfg, srv.URL)
	out, _ := NewOutput(cfg, "b")
	var stats Stats
	progress := NewProgressPrinter(&strings.Builder{})
	l := NewLister(cfg, client, out, &stats, progress)
	if err := l.Run(context.Background(), "b", ""); err != nil { t.Fatal(err) }
	out.FlushAndWait()
	out.Close()
	// delete_markers.txt should have a.txt + sub/c.txt (bad/ marker missing — failed)
	b, _ := readFile(t, filepath.Join(dir, "delete_markers.txt"))
	if !strings.Contains(b, "b\x1fa.txt\x1fdm-a") { t.Errorf("missing a.txt marker: %q", b) }
	if !strings.Contains(b, "b\x1fsub/c.txt\x1fdm-c") { t.Errorf("missing sub/c.txt marker: %q", b) }
	// list_failed.txt should contain "bad/"
	fb, _ := readFile(t, filepath.Join(dir, "list_failed.txt"))
	if !strings.Contains(fb, "bad/") { t.Errorf("list_failed.txt missing bad/: %q", fb) }
	// list_failed.log should exist and mention 500
	lb, _ := readFile(t, filepath.Join(dir, "list_failed.log"))
	if !strings.Contains(lb, "500") { t.Errorf("list_failed.log missing 500: %q", lb) }
}
```

- [ ] **Step 3: Run tests to verify they fail or pass honestly**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run "TestLister_KeyWithUnitSeparator|TestLister_Mode2_FailureRecorded" -v`
Expected: PASS if Task 5 implemented routing correctly. If FAIL, fix the gap in lister.go — likely the `\x1f`-routing is in `Output.WriteDeleteMarker` (Task 4) and lister just calls it, so this should pass without code changes. If it does not pass, debug.

- [ ] **Step 4: Commit**

```bash
git add delete_marker_list_tool/lister_edge_test.go delete_marker_list_tool/lister.go
git commit -m "test(delete_marker_list_tool): \\x1f-in-key routing + failure recording"
```

---

## Task 7: main.go — wiring, run.log, config snapshot, summary

**Files:**
- Create: `delete_marker_list_tool/main.go`
- Create: `delete_marker_list_tool/main_test.go`

**Interfaces:**
- Consumes: all of Tasks 1-5.

- [ ] **Step 1: Write the failing test — end-to-end run via a fake S3**

`main_test.go` — set up a fake server, write a config file, call `run(cfgPath, bucket, prefix)` directly (extract main's body into a `run(...)` function so tests can call it without `os.Exit`). Assert: `run.log` exists and contains `=== summary ===`, `delete_markers.txt` has expected lines, exit behavior on bad config (return non-nil error).

```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_EndToEnd_HappyPath(t *testing.T) {
	// reuse the mode 2 fake server shape from lister_test.go inline
	srv := newMode2FakeServer(t)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := "endpoints:\n  - " + srv.URL + "\nscheme: http\nak: a\nsk: s\nlist_type: 2\nlist_concurrency: 1\noutput_dir: " + filepath.Join(dir, "out") + "\noutput_dir_timestamp: false\n"
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil { t.Fatal(err) }
	if err := run(cfgPath, "b", ""); err != nil { t.Fatalf("run: %v", err) }
	// run.log exists with summary
	rl, err := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if err != nil { t.Fatal(err) }
	if !strings.Contains(string(rl), "=== summary ===") { t.Errorf("run.log missing summary: %q", rl) }
	if !strings.Contains(string(rl), "sk=***") { t.Errorf("run.log should mask sk: %q", rl) }
}

func TestRun_RejectsInvalidListType(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := "endpoints:\n  - 1.2.3.4:80\nak: a\nsk: s\nlist_type: 5\noutput_dir: " + filepath.Join(dir, "out") + "\n"
	os.WriteFile(cfgPath, []byte(cfgBody), 0644)
	err := run(cfgPath, "b", "")
	if err == nil { t.Errorf("expected error for list_type=5") }
}
```

(`newMode2FakeServer(t)` returns a `*httptest.Server` returning the same two-response logic as `TestLister_Mode2_BFSWithCommonPrefix`. Factor the handler into a helper that both tests use, or inline it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestRun -v`
Expected: FAIL (run undefined).

- [ ] **Step 3: Implement `main.go`**

Flags: `-c`, `-bkt`, `-prefix` (same as chunked_check_tool/main.go:28-37 minus the file-input flags). `main()` parses flags, calls `run(*cfgPath, *bucket, *prefix)`; on error, `fmt.Fprintln(os.Stderr, err); os.Exit(1)`.

`run(cfgPath, bucket, prefix string) error`:
1. `cfg, err := LoadConfig(cfgPath)` — propagates validation errors (including `list_type` rejection).
2. `os.MkdirAll(cfg.OutputDir, 0755)`.
3. Open `run.log` at `filepath.Join(cfg.OutputDir, "run.log")` with `O_CREATE|O_WRONLY|O_APPEND, 0644` (open BEFORE anything else, mirror chunked_check_tool/main.go:102-110).
4. `mwOut = io.MultiWriter(os.Stdout, runLog)`, `mwErr = io.MultiWriter(os.Stderr, runLog)`.
5. Print config snapshot to `mwErr` (`endpoints`, `scheme`, `ak` clear, `sk=***`, `list_type`, `list_concurrency`, `output_dir`, `output_dir_timestamp`).
6. `client, err := NewS3Client(cfg, cfg.Endpoints[0])`; on error, write to mwErr, return.
7. `exists, err := client.BucketExists(ctx, bucket)` — if `!exists || err != nil`, log + return error.
8. `out, err := NewOutput(cfg, bucket)`.
9. `var stats Stats`; `progress := NewProgressPrinter(mwOut)`; `progress.SetQueueSnapshotProvider(...)` (mode 2 only — return `QueueSnapshot{Prefix: queue.Len(), Inflight: ...}`; nil for mode 1).
10. `l := NewLister(cfg, client, out, &stats, progress)`.
11. `ctx, cancel := context.WithCancel(context.Background())` (no signal handler — spec §9).
12. `err = l.Run(ctx, bucket, prefix)`; log err if any.
13. `cancel()` (no-op if lister already returned).
14. `out.FlushAndWait()`; `out.Close()`.
15. `stats.SetTotalDuration(time.Since(start))`; `stats.PrintSummary(mwOut)`.
16. If `l.Run` returned an error, return it; else nil.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -run TestRun -v`
Expected: PASS.

- [ ] **Step 5: Build the binary**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go build -o delete_marker_list_tool .`
Expected: succeeds; binary `delete_marker_list_tool/delete_marker_list_tool` exists.

- [ ] **Step 6: Commit**

```bash
git add delete_marker_list_tool/main.go delete_marker_list_tool/main_test.go
git commit -m "feat(delete_marker_list_tool): main wiring + run.log + summary"
```

---

## Task 9: README.md + AGENTS.md

**Files:**
- Create: `delete_marker_list_tool/README.md`
- Create: `delete_marker_list_tool/AGENTS.md`

**Interfaces:** none new.

- [ ] **Step 1: Write `README.md`**

Sections (in order), matching chunked_check_tool/README.md voice (plain language, term-first-define, concrete examples):

1. **Title + one-line summary**: `# delete_marker_list_tool` + 枚举多版本桶内所有 delete marker（删除标记），输出机器可读列表供下游删除工具消费。
2. **作用**: 段落解释——多版本桶每次"无版本号删除"留一个 marker；本工具扫全桶多版本、把所有 marker 落盘成 `delete_markers.txt`，下游工具（另行实现）读该文件逐条 DeleteObject(VersionId) 完成清理。**本工具只读不删**。
3. **编译**: 三段 `go build` 命令——当前平台（macOS）、Linux amd64 交叉编译、Linux arm64 交叉编译。说明产物可直接 scp 运行无需目标机 Go 运行时。引用 `~/sdk/go1.27.1/bin/go` 路径作为本机 build 命令前缀（与 chunked_check_tool 环境一致）。
4. **运行命令**: 四行示例——`-c config.yaml -bkt mybucket`、`-prefix data/2026/`、`-prefix` + mode 1（说明 `list_type: 1`）、配置补齐场景（`output_dir_timestamp: false` 同目录追加）。无其他 flag（明确只有 `-c`/`-bkt`/`-prefix`）。
5. **命令参数**: 三行表格（`-c` 必填 / `-bkt` 必填 / `-prefix` 可选）。
6. **配置**: `config.yaml` 示例块 + 字段表格。字段表只列本工具认识的子集（endpoints/scheme/ak/sk/list_type/list_concurrency/output_dir/output_dir_timestamp/progress_interval/dial_timeout/response_header_timeout）。每个字段一行说明，注释里给出默认值。**明确声明**：本工具可直接复用 chunked_check_tool 的 config.yaml（未识别字段被忽略），未知字段不报错。
7. **输出文件**: 表格——`delete_markers.txt`（结果文件，行格式 `bkt\x1fkey\x1fversion_id`，`\x1f` 分隔）、`invalid_keys.txt`（key 含 `\x1f` 的 marker，key 经 URL 编码）、`run.log`（stdout/stderr tee + 配置快照 + 进度 + summary）、`list_failed.txt`（失败前缀，惰性创建）、`list_failed.log`（slog 详情）。每个文件一行说明 + 行格式。说明 `\x1f` 是 ASCII Unit Separator 不可见，用 `awk -F'\x1f'` 查看。
8. **两种 mode 详解**: 小节分别解释 mode 1（单流递归）和 mode 2（BFS delimiter 并发）的语义、何时选哪个。mode 1 适合小桶/调试；mode 2 默认值，适合大桶并发扫描。
9. **失败处理**: 段落——无重试无断点，失败靠整轮重跑补齐；`list_failed.txt` 为空（或不存在）即全桶扫完；用 `output_dir_timestamp: false` 同目录追加跑多轮自动并集。
10. **已知限制**: bullet 列表——key 含 `\n` 撕裂行、kill -9 丢尾部整行无提示、mode 1 无并发、同秒时间戳冲突、version_id 可能是字面量 `"null"`（下游透传不要当空值）。

- [ ] **Step 2: Write `AGENTS.md`**

Sections (in order), matching chunked_check_tool/AGENTS.md structure (numbered top-level sections, plain language, invariants called out):

1. **项目意图**: 段落——本工具是删除清理的前置侦察；下游工具另行实现做删除；本工具只读不删。引用 spec 文档路径 `docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md`。
2. **技术栈**: bullet——Go 1.27、`github.com/minio/minio-go/v7 v7.3.0`（pinned to match chunked_check_tool，**不可升级**，channel API 的 `WithVersions` 行为按 v7.3.0 校准）、`gopkg.in/yaml.v3`、TLS `scheme=https` 时 `InsecureSkipVerify=true`。
3. **模块布局**: 表格列出每个文件（main.go / config.go / s3client.go / lister.go / output.go / stats.go / progress.go）的职责，一行一个文件。
4. **关键不变量（改动前必须守住）**:
   - 不变量 1: 行分隔符 `\x1f`，行格式 `bkt\x1fkey\x1fversion_id`。**绝不**改回 `|`（chunked_check_tool 用 `|`，本工具因 key 含 `|` 漏删风险刻意换）。
   - 不变量 2: `version_id` 字面量 `"null"` 是合法值（旧版本对象），下游透传不要当空。本工具原样写入结果文件。
   - 不变量 3: 共同前缀判别规则 `VersionID == "" && !IsDeleteMarker && ETag == "" && strings.HasSuffix(Key, "/")`。**四条件合取**，不可放宽任一——误判前缀为版本会漏扫子目录，误判版本为前缀会死循环。
   - 不变量 4: mode 2 BFS 终止不变量——`inflight.Add(1)` BEFORE `queue.Push`（chunked_check_tool/AGENTS.md §4.7 同款规则，违反会提前终止漏扫子目录）。
   - 不变量 5: `list_failed.txt` 惰性创建——文件不存在 = 无失败，文件存在 = 至少一次失败，空行 = 空前缀失败。**不可**在启动时创建空文件（会破坏"文件存在即有失败"契约）。
   - 不变量 6: 无重试无断点无优雅退出——失败靠整轮重跑，**不可**加 retry/journal/signal handler（与 chunked_check_tool 不同；spec §9 非目标）。
   - 不变量 7: 无 NodePool 无节点恢复——mode 2 worker 用原子轮询绑 endpoint，失败即记 list_failed 继续，**不可**引入节点隔离/重绑（spec §9 非目标）。
   - 不变量 8: `output_dir_timestamp` 同级后缀规则——trim trailing `/`，append `_<YYYYMMDD_HHMMSS>`，**不是**子目录嵌套（chunked_check_tool/config.go:93-106 同款）。
   - 不变量 9: `run.log` 第一个 open——先于任何输出文件，`mwOut`/`mwErr` tee stdout/stderr，`sk` 掩码为 `***`。
   - 不变量 10: `\x1f`-in-key 路由——key 含 `\x1f` 的 marker 走 `invalid_keys.txt`，key 经 `url.QueryEscape` 编码；**不可**直接写入 `delete_markers.txt`（会撕裂行字段）。
   - 不变量 11: minio-go v7.3.0 channel API `Client.ListObjects(WithVersions: true)`——**不要**改用 `Core.ListObjects`（Core 不支持 WithVersions）；升级 minio-go 需重验 channel API 是否吐出 CommonPrefixes（spec §4.1）。
5. **约束**: bullet——单 package main、独立 go.mod、与 list_only 平级；HTTP transport 调优对齐 chunked_check_tool（MaxIdleConnsPerHost: 32、IdleConnTimeout: 90s 等）；无 -resume/-nextmarker/-check-file/-list-file/-backup-file flag。
6. **遗留任务（已知未决/未来工作）**:
   - key 含 `\n` 的 marker 仍会撕裂行（chunked_check_tool 同样不防护）——极罕见 S3 key，未来如需可加 `\n` 检测 + URL 编码路由。
   - 同秒内连续启动两次（timestamp=true）会撞目录并发 append 撕裂尾部行——靠下游解析时丢弃格式非法行兜底；如需严格保证可加目录已存在即拒绝（需同时支持 timestamp=false 的补齐追加语义）。
   - 无优雅退出——Ctrl-C / kill -9 丢尾部整行；未来如需可加 chunked_check_tool 的两段式 SIGINT handler + journal（spec §9 明确非目标）。
   - 下游删除工具未实现——需读 `delete_markers.txt` + `invalid_keys.txt`（后者 key 需 URL 解码），逐条 `DeleteObject(bucket, key, VersionId)`。
7. **相关文档**: bullet 列出——spec 路径、本计划路径、chunked_check_tool AGENTS.md 路径（作为约定参考）。

- [ ] **Step 3: Verify both files exist and are non-trivial**

Run: `ls -la delete_marker_list_tool/README.md delete_marker_list_tool/AGENTS.md && wc -l delete_marker_list_tool/README.md delete_marker_list_tool/AGENTS.md`
Expected: both exist; README ≥ 80 lines; AGENTS ≥ 60 lines.

- [ ] **Step 4: Commit**

```bash
git add delete_marker_list_tool/README.md delete_marker_list_tool/AGENTS.md
git commit -m "docs(delete_marker_list_tool): README + AGENTS"
```

---

## Task 10: Full test suite + `go vet` + integration smoke

**Files:**
- Modify: any test that has a flaky/buggy assertion from earlier tasks.

**Interfaces:** none new.

- [ ] **Step 1: Run the full test suite**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go test ./... -v -count=1`
Expected: all tests PASS.

- [ ] **Step 2: Run `go vet`**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go vet ./...`
Expected: no issues.

- [ ] **Step 3: Build + verify binary**

Run: `cd delete_marker_list_tool && ~/sdk/go1.27.1/bin/go build -o /tmp/dmlt . && /tmp/dmlt -c /nonexistent -bkt x 2>&1 | head -5`
Expected: error message about missing config file (not a stack trace).

- [ ] **Step 4: Self-review against spec**

Read `docs/superpowers/specs/2026-11-12-delete-marker-list-tool-design.md` end-to-end. Confirm each section maps to implemented code:
- §3 modes → lister.go
- §4.2 classification → lister.go `isCommonPrefix`
- §4.3 dedup (append-union) — no code needed (idempotent), but confirm no dedup logic was accidentally added
- §4.4 list_failed contract → output.go (lazy open, empty line = empty prefix)
- §4.5 same-second timestamp — no code needed (acceptance documented)
- §6 files → output.go
- §7 config subset → config.go
- §9 non-goals — verify none snuck in (no NodePool, no journal, no signal handler, no retries, no resume flags)

If a gap is found, add the missing test + code now.

- [ ] **Step 5: Commit any fixes**

```bash
git add -A delete_marker_list_tool
git commit -m "test(delete_marker_list_tool): full suite + vet + smoke"
```

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-12-delete-marker-list-tool.md`. Please review the plan. Which execution approach would you prefer?

- **Subagent-driven** — A fresh subagent implements each task and a fresh reviewer checks it before the next one starts, then a whole-branch review at the end. Most thorough; costs a fresh context per task and per review.
- **Native** — I implement every task myself in this session, the way this harness runs work, then one fresh reviewer on the most capable model checks the whole branch. Cheapest and fastest; no independent review until the end. Runs well with a mid-tier session model, since the plan carries the design.

For this plan I recommend **Native**, because the 10 tasks share tight interfaces (Config → S3Client → Output → Lister → main → docs) where each task consumes the previous task's exact signatures — a single implementer holding all the interfaces in context avoids signature drift between tasks, and a wrong guess would be caught by the same-task tests before the next task starts. Does the plan capture what you want, and which approach should we use?

---

## Self-Review Notes

- **Spec coverage** (§1-§13): §3 modes → Task 5; §4.2 classification → Task 5; §4.3 dedup → no code (idempotent), verified Task 10; §4.4 list_failed → Task 4; §4.5 timestamp → no code (documented Task 9); §6 files → Task 4; §7 config → Task 1; §9 non-goals → verified Task 10; §13 row separator + invalid_keys routing → Task 4 (routing) + Task 6 (test).
- **Review Focus**: all 5 input classes have tests pinned (RF1 Task 5, RF2 Task 5 happy path, RF3 Task 6, RF4 Task 6, RF5 Task 1).
- **Type consistency**: `Config`, `S3Client`, `Output`, `Stats`, `ProgressPrinter`, `Lister` signatures consistent across Tasks 1-7.
- **README/AGENTS coverage**: Task 9 writes both, sections enumerated, plain-language style aligned with chunked_check_tool's existing docs.
