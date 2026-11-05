package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestParseJournalLineDiscoveredRestOfLine — discovered lines carry the
// prefix as the whole rest of the line (verbatim, pipes included).
func TestParseJournalLineDiscoveredRestOfLine(t *testing.T) {
	ev, err := parseJournalLine("discovered|data|2026")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Kind != journalEventDiscovered {
		t.Errorf("kind = %v, want journalEventDiscovered", ev.Kind)
	}
	if ev.Prefix != "data|2026" {
		t.Errorf("prefix = %q, want %q (pipe must stay in the prefix)", ev.Prefix, "data|2026")
	}
	if ev.Token != "" {
		t.Errorf("token = %q, want empty", ev.Token)
	}
}

// TestParseJournalLineCursorSplitsAtLastPipe — tokens never contain "|" (S3
// contract: V2 base64, V1 keys), so the split point is the LAST pipe.
func TestParseJournalLineCursorSplitsAtLastPipe(t *testing.T) {
	ev, err := parseJournalLine("cursor|a|b|TOKEN")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Kind != journalEventCursor {
		t.Errorf("kind = %v, want journalEventCursor", ev.Kind)
	}
	if ev.Prefix != "a|b" {
		t.Errorf("prefix = %q, want %q", ev.Prefix, "a|b")
	}
	if ev.Token != "TOKEN" {
		t.Errorf("token = %q, want %q", ev.Token, "TOKEN")
	}
}

// TestParseJournalLineEmptyPrefix — the root prefix renders as an empty
// field; cursor with empty prefix + token stays parseable.
func TestParseJournalLineEmptyPrefix(t *testing.T) {
	ev, err := parseJournalLine("discovered|")
	if err != nil {
		t.Fatalf("parse discovered|: %v", err)
	}
	if ev.Prefix != "" {
		t.Errorf("prefix = %q, want empty (root)", ev.Prefix)
	}
	ev, err = parseJournalLine("cursor||T")
	if err != nil {
		t.Fatalf("parse cursor||T: %v", err)
	}
	if ev.Prefix != "" || ev.Token != "T" {
		t.Errorf("cursor||T = (%q, %q), want (%q, %q)", ev.Prefix, ev.Token, "", "T")
	}
	ev, err = parseJournalLine("done|")
	if err != nil {
		t.Fatalf("parse done|: %v", err)
	}
	if ev.Prefix != "" || ev.Kind != journalEventDone {
		t.Errorf("done| = (%q, %v), want (\"\", done)", ev.Prefix, ev.Kind)
	}
}

// TestParseJournalLineMalformed — unknown verbs, cursor without a token
// separator, and lines without any separator are all rejected.
func TestParseJournalLineMalformed(t *testing.T) {
	for _, line := range []string{
		"unknown|x",
		"cursor|onlytoken",
		"discovered", // no verb separator
		"",           // empty line
	} {
		if _, err := parseJournalLine(line); err == nil {
			t.Errorf("parseJournalLine(%q) = nil error, want error", line)
		}
	}
}

// TestFormatJournalEvent — rendering is the inverse of parsing for all
// three event kinds, including pipe-containing prefixes.
func TestFormatJournalEvent(t *testing.T) {
	cases := []struct {
		kind   journalEventKind
		prefix string
		token  string
		want   string
	}{
		{journalEventDiscovered, "data|2026", "", "discovered|data|2026"},
		{journalEventCursor, "a|b", "TOKEN", "cursor|a|b|TOKEN"},
		{journalEventCursor, "", "T", "cursor||T"},
		{journalEventDone, "p", "", "done|p"},
	}
	for _, c := range cases {
		got := formatJournalEvent(c.prefix, c.token, c.kind)
		if got != c.want {
			t.Errorf("formatJournalEvent(%v, %q, %q) = %q, want %q", c.kind, c.prefix, c.token, got, c.want)
		}
		back, err := parseJournalLine(got)
		if err != nil || back.Kind != c.kind || back.Prefix != c.prefix || back.Token != c.token {
			t.Errorf("round-trip %q = (%v, %q, %q, err=%v), want (%v, %q, %q)", got, back.Kind, back.Prefix, back.Token, err, c.kind, c.prefix, c.token)
		}
	}
}

// TestReplayJournalLastEventWins — replay keeps the last event per prefix;
// every event marks the prefix as known (Discovered).
func TestReplayJournalLastEventWins(t *testing.T) {
	path := writeJournal(t, "discovered|p\ncursor|p|T1\ncursor|p|T2\ndone|q\n")
	states, err := ReplayJournal(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	p := states["p"]
	if !p.Discovered || p.Done || p.Token != "T2" {
		t.Errorf("p = %+v, want {Discovered, Token: T2, !Done}", p)
	}
	q := states["q"]
	if !q.Discovered || !q.Done || q.Token != "" {
		t.Errorf("q = %+v, want {Discovered, Done}", q)
	}
}

// TestReplayJournalDropsTornTail — a trailing line without a newline is a
// batch the dying process half-wrote; replay drops it (the journal then
// remembers less, recovery redoes more — the safe direction).
func TestReplayJournalDropsTornTail(t *testing.T) {
	path := writeJournal(t, "discovered|p\ncursor|p|T")
	states, err := ReplayJournal(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	p := states["p"]
	if !p.Discovered || p.Token != "" {
		t.Errorf("p = %+v, want {Discovered, no token} (torn cursor line dropped)", p)
	}
}

// TestReplayJournalMalformedLine — a structurally broken line in the middle
// of the journal is a real corruption, not a torn tail: replay fails loudly.
func TestReplayJournalMalformedLine(t *testing.T) {
	path := writeJournal(t, "discovered|p\nnonsense\n")
	if _, err := ReplayJournal(path); err == nil {
		t.Errorf("replay with malformed middle line = nil error, want error")
	}
}

// TestReplayJournalMissingFile — a journal path that does not exist is an
// operator error (replay must not silently produce an empty scan).
func TestReplayJournalMissingFile(t *testing.T) {
	if _, err := ReplayJournal(filepath.Join(t.TempDir(), "no-such-journal.txt")); err == nil {
		t.Errorf("missing journal = nil error, want error")
	}
}

// TestReplayJournalEmptyFile — an existing but empty journal replays to an
// empty state table (the "empty journal" guard lives in the resume loader,
// not here).
func TestReplayJournalEmptyFile(t *testing.T) {
	path := writeJournal(t, "")
	states, err := ReplayJournal(path)
	if err != nil {
		t.Fatalf("replay empty journal: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("states = %d entries, want 0", len(states))
	}
}

// writeJournal writes content verbatim to a journal.txt in a temp dir.
func writeJournal(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.txt")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// findCursor returns the snapshot's cursor entry for prefix, or nil.
func findCursor(snap journalSnapshot, prefix string) *journalCursorEntry {
	for i := range snap.Cursors {
		if snap.Cursors[i].Prefix == prefix {
			return &snap.Cursors[i]
		}
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// TestTrackerPageCompletesWhenAllTerminals — a page's completion requires
// every object's terminal event; the cursor then points past it.
func TestTrackerPageCompletesWhenAllTerminals(t *testing.T) {
	tr := NewJournalTracker()
	tr.RegisterPage("p", 3, "T1")
	snap := tr.Snapshot()
	if c := findCursor(snap, "p"); c != nil {
		t.Errorf("cursor before any terminal = %+v, want none", c)
	}
	tr.Terminal("p", 0)
	tr.Terminal("p", 0)
	snap = tr.Snapshot()
	if c := findCursor(snap, "p"); c != nil {
		t.Errorf("cursor at 2/3 terminals = %+v, want none", c)
	}
	tr.Terminal("p", 0)
	snap = tr.Snapshot()
	if c := findCursor(snap, "p"); c == nil || c.Token != "T1" {
		t.Errorf("cursor at 3/3 terminals = %+v, want {p, T1}", c)
	}
}

// TestTrackerConsecutivePagesRequired — the completion position only moves
// across fully completed pages: page 2 half-done pins the cursor at page 1.
func TestTrackerConsecutivePagesRequired(t *testing.T) {
	tr := NewJournalTracker()
	tr.RegisterPage("p", 2, "T1") // page 0
	tr.RegisterPage("p", 2, "T2") // page 1
	tr.Terminal("p", 0)
	tr.Terminal("p", 0)
	tr.Terminal("p", 1) // page 1: 1/2 done
	snap := tr.Snapshot()
	if c := findCursor(snap, "p"); c == nil || c.Token != "T1" {
		t.Errorf("cursor = %+v, want {p, T1} (page 1 incomplete pins position at page 0)", c)
	}
}

// TestTrackerFrozenOnPartialPage — the core kill -9 invariant: a page whose
// objects were only partially emitted (hard abort mid-page) never reaches
// zero outstanding, so the cursor freezes BEFORE it. Registering the full
// page count up front is what makes this hold.
func TestTrackerFrozenOnPartialPage(t *testing.T) {
	tr := NewJournalTracker()
	tr.RegisterPage("p", 1, "T1") // page 0 completes fully
	tr.Terminal("p", 0)
	seq := tr.RegisterPage("p", 1000, "T2") // page 1: hard abort after 400 pushes
	for i := 0; i < 400; i++ {
		tr.Terminal("p", seq)
	}
	snap := tr.Snapshot()
	if c := findCursor(snap, "p"); c == nil || c.Token != "T1" {
		t.Errorf("cursor at 400/1000 = %+v, want {p, T1} (frozen before the partial page)", c)
	}
	for i := 0; i < 600; i++ {
		tr.Terminal("p", seq)
	}
	snap = tr.Snapshot()
	if c := findCursor(snap, "p"); c == nil || c.Token != "T2" {
		t.Errorf("cursor at 1000/1000 = %+v, want {p, T2}", c)
	}
}

// TestTrackerDoneRequiresPaginationDoneAndAllPages — done = listing ended
// AND every page complete.
func TestTrackerDoneRequiresPaginationDoneAndAllPages(t *testing.T) {
	tr := NewJournalTracker()
	tr.RegisterPage("p", 1, "")
	tr.Terminal("p", 0)
	if snap := tr.Snapshot(); containsString(snap.Dones, "p") {
		t.Errorf("done before pagination end = true, want false")
	}
	tr.MarkPaginationDone("p")
	if snap := tr.Snapshot(); !containsString(snap.Dones, "p") {
		t.Errorf("done after pagination end + all pages = false, want true")
	}
}

// TestTrackerSeedTokenPreservedBeforeFirstPage — a seeded resume token
// survives in snapshots even before the first page completes, so a run
// killed early does not fall back to listing the prefix from the top.
func TestTrackerSeedTokenPreservedBeforeFirstPage(t *testing.T) {
	tr := NewJournalTracker()
	tr.RecordSeed("p", "T0")
	snap := tr.Snapshot()
	if c := findCursor(snap, "p"); c == nil || c.Token != "T0" {
		t.Errorf("cursor before first page = %+v, want {p, T0} (base token)", c)
	}
}

// TestTrackerDiscoveredDeltaAndCommit — discovered entries are delta: they
// appear once, and Commit (the write landed) removes them from the pending
// set so later snapshots do not repeat them.
func TestTrackerDiscoveredDeltaAndCommit(t *testing.T) {
	tr := NewJournalTracker()
	tr.AddDiscovered("x")
	snap := tr.Snapshot()
	if !containsString(snap.Discovered, "x") {
		t.Errorf("snapshot after AddDiscovered misses x: %+v", snap.Discovered)
	}
	tr.Commit(snap)
	snap = tr.Snapshot()
	if containsString(snap.Discovered, "x") {
		t.Errorf("snapshot after Commit repeats x: %+v", snap.Discovered)
	}
}

// TestTrackerDropAfterCommit — committed done prefixes leave the in-memory
// map (journal file is the authoritative state); in-flight prefixes stay.
func TestTrackerDropAfterCommit(t *testing.T) {
	tr := NewJournalTracker()
	tr.AddDiscovered("inflight")
	tr.RegisterPage("p", 1, "")
	tr.Terminal("p", 0)
	tr.MarkPaginationDone("p")
	before := tr.trackedPrefixes()
	snap := tr.Snapshot()
	tr.Commit(snap)
	after := tr.trackedPrefixes()
	if after != before-1 {
		t.Errorf("trackedPrefixes after Commit = %d, want %d (done prefix dropped)", after, before-1)
	}
}

// TestTrackerEmptyPrefixDoneInOnePage — an empty prefix registers a
// zero-object page, completes instantly, and is done once pagination ends.
func TestTrackerEmptyPrefixDoneInOnePage(t *testing.T) {
	tr := NewJournalTracker()
	tr.RegisterPage("p", 0, "")
	tr.MarkPaginationDone("p")
	snap := tr.Snapshot()
	if !containsString(snap.Dones, "p") {
		t.Errorf("empty prefix done = false, want true (snapshot: %+v)", snap)
	}
}

// TestTrackerLateTerminalIgnored — terminals for unknown/dropped prefixes
// are ignored (safe direction: at most delays the position).
func TestTrackerLateTerminalIgnored(t *testing.T) {
	tr := NewJournalTracker()
	tr.Terminal("ghost", 0) // must not panic
	tr.RegisterPage("p", 1, "")
	tr.Terminal("p", 0)
	tr.Terminal("p", 0) // duplicate terminal must not push outstanding negative
	tr.Terminal("p", 5) // unknown page must not panic
	tr.MarkPaginationDone("p")
	if snap := tr.Snapshot(); !containsString(snap.Dones, "p") {
		t.Errorf("done = false after duplicate/unknown terminals, want true")
	}
}

// startCheckpointLoop launches runJournalCheckpoints on a fresh Output in
// dir, with the interval already shrunk by the caller. Returns the journal
// path, the Output (closed by t.Cleanup), and the stop function (cancels the
// loop and waits for its final checkpoint).
func startCheckpointLoop(t *testing.T, dir string, tr *JournalTracker) (journalPath string, o *Output, stop func()) {
	t.Helper()
	o, err := NewOutput(&Config{OutputDir: dir, IsCheck: true}, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	journalPath = filepath.Join(dir, "journal.txt")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runJournalCheckpoints(ctx, tr, o, journalPath, nil)
		close(done)
	}()
	return journalPath, o, func() {
		cancel()
		<-done
	}
}

// TestCheckpointImmediateFirstWrite — the loop's first checkpoint runs at
// startup, not at the first tick: a kill during the FIRST interval must
// still leave the seeded root in the journal (otherwise early kills are
// unresumable and the resume loader cannot even know the scan scope).
func TestCheckpointImmediateFirstWrite(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Second
	defer func() { journalCheckpointInterval = orig }()

	tr := NewJournalTracker()
	tr.RecordSeed("", "")
	journalPath, _, stop := startCheckpointLoop(t, t.TempDir(), tr)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(journalPath)
		if err == nil && strings.Contains(string(data), "discovered|\n") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("root seed line not journaled before the first tick: content=%q err=%v", data, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestCheckpointOrderAndDurability — the functional form of the ordering
// invariant: once the journal claims a page complete (cursor line), the
// result lines for that page's objects are already IN THE FILE, without
// Close. snapshot -> FlushAndWait -> write is what makes this hold.
func TestCheckpointOrderAndDurability(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Millisecond
	defer func() { journalCheckpointInterval = orig }()

	dir := t.TempDir()
	tr := NewJournalTracker()
	tr.RecordSeed("p", "")
	tr.RegisterPage("p", 1, "T1")
	journalPath, o, stop := startCheckpointLoop(t, dir, tr)
	defer stop()

	// Push the result line BEFORE the terminal report — the checker-side
	// invariant (terminal-after-push) exercised end to end.
	o.WriteCheckFailed("obj/k1")
	tr.Terminal("p", 0)

	deadline := time.Now().Add(2 * time.Second)
	for {
		jdata, _ := os.ReadFile(journalPath)
		fdata, ferr := os.ReadFile(filepath.Join(dir, "check_failed.txt"))
		if strings.Contains(string(jdata), "cursor|p|T1") &&
			ferr == nil && strings.Contains(string(fdata), "test-bkt|obj/k1") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("invariant broken: journal=%q, check_failed=%q (err=%v)", jdata, fdata, ferr)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestCheckpointBatchOrderDiscoveredFirst — within one batch, discovered
// lines are written before cursor lines: a kill mid-batch may leave "child
// discovered, parent not done" (recoverable) but never the reverse.
func TestCheckpointBatchOrderDiscoveredFirst(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Second
	defer func() { journalCheckpointInterval = orig }()

	tr := NewJournalTracker()
	tr.AddDiscovered("sub")
	tr.RecordSeed("p", "")
	tr.RegisterPage("p", 1, "T1")
	tr.Terminal("p", 0)
	journalPath, _, stop := startCheckpointLoop(t, t.TempDir(), tr)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(journalPath)
		if err == nil && strings.Contains(string(data), "cursor|p|T1") {
			di := strings.Index(string(data), "discovered|sub")
			ci := strings.Index(string(data), "cursor|p|T1")
			if di < 0 || di > ci {
				t.Fatalf("batch order wrong: discovered@%d cursor@%d in %q", di, ci, data)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor line never appeared: %q err=%v", data, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestCheckpointFinalOnStop — cancelling the loop's ctx triggers one final
// checkpoint, so the journal reflects state as of shutdown, not as of the
// last tick.
func TestCheckpointFinalOnStop(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Second
	defer func() { journalCheckpointInterval = orig }()

	tr := NewJournalTracker()
	tr.RecordSeed("p", "")
	journalPath, _, stop := startCheckpointLoop(t, t.TempDir(), tr)

	deadline := time.Now().Add(2 * time.Second)
	for { // wait for the startup checkpoint
		data, err := os.ReadFile(journalPath)
		if err == nil && strings.Contains(string(data), "discovered|p") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("startup checkpoint missing: %q err=%v", data, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	tr.AddDiscovered("late-sub") // arrives after the startup checkpoint
	stop()                       // cancel -> final checkpoint must include it
	data, err := os.ReadFile(journalPath)
	if err != nil || !strings.Contains(string(data), "discovered|late-sub") {
		t.Fatalf("final checkpoint missed post-tick state: %q err=%v", data, err)
	}
}

// TestCheckpointCommitClearsDelta — a discovery line lands exactly once:
// Commit removed it from the pending set, later checkpoints do not repeat it.
func TestCheckpointCommitClearsDelta(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Millisecond
	defer func() { journalCheckpointInterval = orig }()

	tr := NewJournalTracker()
	tr.AddDiscovered("x")
	journalPath, _, stop := startCheckpointLoop(t, t.TempDir(), tr)
	defer stop()

	time.Sleep(50 * time.Millisecond) // several ticks pass
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "discovered|x\n"); n != 1 {
		t.Errorf("discovered|x appears %d times, want exactly 1 (Commit clears the delta): %q", n, data)
	}
}

// TestListerJournalRegistersPages — the lister registers every page with
// its FULL object count, tasks carry (prefix, pageSeq), and a fully
// processed prefix reaches done.
func TestListerJournalRegistersPages(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	tr := NewJournalTracker()
	l.SetJournal(tr)

	const normal = "0123456789abcdef0123456789abcdef"
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/a", ETag: normal}, {Key: "root/b", ETag: normal}}, nextToken: "tok2"},
			{objs: []ObjectInfo{{Key: "root/c", ETag: normal}, {Key: "root/d", ETag: normal}}, nextToken: ""},
		},
	}}
	objCh := make(chan VerifyTask, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(context.Background(), context.Background(), "root/", "", objCh, fake, nil)
	}()
	var tasks []VerifyTask
	for len(tasks) < 4 {
		select {
		case task := <-objCh:
			tasks = append(tasks, task)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d tasks arrived, want 4", len(tasks))
		}
	}
	<-done

	for i, task := range tasks {
		if task.JournalPrefix != "root/" {
			t.Errorf("task %d JournalPrefix = %q, want root/", i, task.JournalPrefix)
		}
		wantSeq := int32(0)
		if i >= 2 {
			wantSeq = 1
		}
		if task.PageSeq != wantSeq {
			t.Errorf("task %d PageSeq = %d, want %d", i, task.PageSeq, wantSeq)
		}
	}
	for _, task := range tasks {
		tr.Terminal(task.JournalPrefix, task.PageSeq)
	}
	if snap := tr.Snapshot(); !containsString(snap.Dones, "root/") {
		t.Errorf("done = false after all terminals + pagination end; snapshot: %+v", snap)
	}
}

// TestListerJournalHardAbortFreezesPosition — the pipeline-level form of
// the core invariant: a second-signal abort mid-page leaves the page's
// outstanding count short, so the journal cursor freezes on the LAST
// completed page and recovery re-lists the aborted page.
func TestListerJournalHardAbortFreezesPosition(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	tr := NewJournalTracker()
	l.SetJournal(tr)

	const normal = "0123456789abcdef0123456789abcdef"
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/a", ETag: normal}}, nextToken: "tok2"},
			{objs: []ObjectInfo{{Key: "root/b", ETag: normal}, {Key: "root/c", ETag: normal}}, nextToken: ""},
		},
	}}
	objCh := make(chan VerifyTask, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(context.Background(), hardCtx, "root/", "", objCh, fake, nil)
	}()
	// Consume page 0's only object as a checker would and report its
	// terminal: page 0 completes, cursor moves to its token.
	select {
	case task := <-objCh:
		tr.Terminal(task.JournalPrefix, task.PageSeq)
	case <-time.After(2 * time.Second):
		t.Fatal("page 0 task never arrived")
	}
	// Page 1: root/b fills the capacity-1 channel, root/c's send blocks.
	waitFor(t, func() bool { return len(objCh) == 1 }, 2*time.Second)
	hardCancel() // second signal: abort the mid-page send
	<-done

	snap := tr.Snapshot()
	if c := findCursor(snap, "root/"); c == nil || c.Token != "tok2" {
		t.Errorf("cursor after mid-page hard abort = %+v, want {root/, tok2} (frozen at last completed page)", c)
	}
	if containsString(snap.Dones, "root/") {
		t.Errorf("done = true after hard abort, want false")
	}
}

// TestListerJournalShortCircuitKeysTerminal — objects whose keys contain
// "|" never become tasks; the lister reports their terminal itself so the
// page can still complete.
func TestListerJournalShortCircuitKeysTerminal(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	tr := NewJournalTracker()
	l.SetJournal(tr)

	const normal = "0123456789abcdef0123456789abcdef"
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/ok", ETag: normal}, {Key: "root/ba|d", ETag: normal}}, nextToken: ""},
		},
	}}
	objCh := make(chan VerifyTask, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(context.Background(), context.Background(), "root/", "", objCh, fake, nil)
	}()
	select {
	case task := <-objCh:
		tr.Terminal(task.JournalPrefix, task.PageSeq)
	case <-time.After(2 * time.Second):
		t.Fatal("task never arrived")
	}
	<-done
	out.FlushAndWait() // the invalid_keys line is still queued; put it on disk

	if snap := tr.Snapshot(); !containsString(snap.Dones, "root/") {
		t.Errorf("done = false with short-circuited key in page; snapshot: %+v", snap)
	}
	data, err := os.ReadFile(filepath.Join(dir, "invalid_keys.txt"))
	if err != nil || !strings.Contains(string(data), "root/ba|d") {
		t.Errorf("invalid_keys.txt = %q err=%v, want the pipe key recorded", data, err)
	}
}

// TestListerJournalDiscoversBeforeEmission — when the parent's cursor
// crosses a page, the sub-prefixes that page discovered are visible in the
// SAME snapshot: the checkpoint batch can never claim the parent's progress
// past the page while the discoveries are missing.
func TestListerJournalDiscoversBeforeEmission(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	tr := NewJournalTracker()
	l.SetJournal(tr)

	const normal = "0123456789abcdef0123456789abcdef"
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/a", ETag: normal}}, prefixes: []string{"root/sub/"}, nextToken: "tok2"},
			{objs: []ObjectInfo{{Key: "root/c", ETag: normal}}, nextToken: ""},
		},
	}}
	objCh := make(chan VerifyTask, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.processPrefix(context.Background(), context.Background(), "root/", "", objCh, fake, nil)
	}()
	var tasks []VerifyTask
	for len(tasks) < 2 {
		select {
		case task := <-objCh:
			tasks = append(tasks, task)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d tasks arrived, want 2", len(tasks))
		}
	}
	<-done
	tr.Terminal(tasks[0].JournalPrefix, tasks[0].PageSeq) // page 0 completes

	snap := tr.Snapshot()
	if c := findCursor(snap, "root/"); c == nil || c.Token != "tok2" {
		t.Errorf("cursor after page 0 complete = %+v, want {root/, tok2}", c)
	}
	if !containsString(snap.Discovered, "root/sub/") {
		t.Errorf("page 0's discovery missing from the same snapshot as its cursor: %+v", snap.Discovered)
	}
}

// TestCheckerJournalTerminalAfterHandle — Checker.Handle reports the
// terminal AFTER the body pushed the result line (deferred), so the page
// completes through the real checker path.
func TestCheckerJournalTerminalAfterHandle(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, IsCheck: true}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	c := NewChecker(context.Background(), &scriptedS3{}, out, NewStats(), cfg)
	tr := NewJournalTracker()
	c.SetJournal(tr)
	tr.RegisterPage("root/", 1, "T1")

	const normal = "0123456789abcdef0123456789abcdef"
	c.Handle(VerifyTask{Key: "root/a", ETag: normal, Size: 16, JournalPrefix: "root/", PageSeq: 0})

	snap := tr.Snapshot()
	if c := findCursor(snap, "root/"); c == nil || c.Token != "T1" {
		t.Errorf("cursor after Handle = %+v, want {root/, T1} (terminal deferred to after the result push)", c)
	}
}

// TestListerJournalListOnlyTerminals — list-only mode has no checkers; the
// lister's local classification is the terminal event, so pages complete.
func TestListerJournalListOnlyTerminals(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: false}
	out, _ := NewOutput(cfg, "test-bkt", FileInputNone)
	defer out.Close()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	tr := NewJournalTracker()
	l.SetJournal(tr)

	const normal = "0123456789abcdef0123456789abcdef"
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/a", ETag: normal}, {Key: "root/b", ETag: normal}}, nextToken: ""},
		},
	}}
	objCh := make(chan VerifyTask, 8)
	l.processPrefix(context.Background(), context.Background(), "root/", "", objCh, fake, nil)

	if snap := tr.Snapshot(); !containsString(snap.Dones, "root/") {
		t.Errorf("done = false in list-only mode; snapshot: %+v", snap)
	}
}

// TestSeedFromJournal — replayed states seed the BFS: done prefixes are
// skipped, cursor entries continue from their token, discovered-only
// prefixes list from the top.
func TestSeedFromJournal(t *testing.T) {
	l := NewLister(NewQueue(), nil, NewStats(), &Config{ListType: 2, IsCheck: true})
	states := map[string]journalState{
		"p1": {Discovered: true, Done: true},
		"p2": {Discovered: true, Token: "T"},
		"p3": {Discovered: true},
	}
	if !seedFromJournal(states, l) {
		t.Fatal("seedFromJournal = false, want true (something to seed)")
	}
	seeds := l.queue.Drain()
	if len(seeds) != 2 {
		t.Fatalf("queue = %v, want 2 seeds", seeds)
	}
	got := map[string]bool{}
	for _, s := range seeds {
		got[s] = true
	}
	if !got["p2\x00T"] || !got["p3"] {
		t.Errorf("seeds = %v, want {p2\\x00T, p3} (done p1 skipped)", seeds)
	}
}

// TestLoadJournalForResumeEmpty — an empty journal cannot be resumed: the
// run died before its first checkpoint (or the path is wrong). Scanning
// nothing silently would look like a completed scan.
func TestLoadJournalForResumeEmpty(t *testing.T) {
	if _, err := loadJournalForResume(writeJournal(t, "")); err == nil {
		t.Errorf("empty journal = nil error, want error")
	}
}

// TestLoadJournalForResumeMissing — a missing journal file is an operator
// error, surfaced before any scan work.
func TestLoadJournalForResumeMissing(t *testing.T) {
	if _, err := loadJournalForResume(filepath.Join(t.TempDir(), "nope.txt")); err == nil {
		t.Errorf("missing journal = nil error, want error")
	}
}

// TestJournalResumeRoundTrip — the kill -9 recovery loop end to end: a
// pipeline killed with pages emitted-but-unprobed leaves a journal whose
// replayed frontier seeds EXACTLY the un-done remainder.
func TestJournalResumeRoundTrip(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 5 * time.Millisecond
	defer func() { journalCheckpointInterval = orig }()

	dir := t.TempDir()
	cfg := &Config{OutputDir: dir, ListType: 2, IsCheck: true}
	out, err := NewOutput(cfg, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	tr := NewJournalTracker()
	l := NewLister(NewQueue(), out, NewStats(), cfg)
	l.SetJournal(tr)
	c := NewChecker(context.Background(), &scriptedS3{}, out, NewStats(), cfg)
	c.SetJournal(tr)

	// Checkpoint loop mirrors run()'s wiring; registered cleanups replicate
	// run()'s shutdown order (stop the loop, then close the output).
	journalPath := filepath.Join(dir, "journal.txt")
	jCtx, jCancel := context.WithCancel(context.Background())
	jDone := make(chan struct{})
	t.Cleanup(func() { out.Close() })
	go func() {
		runJournalCheckpoints(jCtx, tr, out, journalPath, nil)
		close(jDone)
	}()
	t.Cleanup(func() { jCancel(); <-jDone })

	const normal = "0123456789abcdef0123456789abcdef"
	fake := &scriptedS3{pages: map[string][]pageResult{
		"root/": {
			{objs: []ObjectInfo{{Key: "root/a", ETag: normal}, {Key: "root/b", ETag: normal}}, prefixes: []string{"root/sub/"}, nextToken: "tok2"},
			{objs: []ObjectInfo{{Key: "root/c", ETag: normal}, {Key: "root/d", ETag: normal}}, nextToken: "tok3"},
			{objs: []ObjectInfo{{Key: "root/e", ETag: normal}, {Key: "root/f", ETag: normal}}, nextToken: ""},
		},
	}}

	l.Seed("root/")
	objCh := make(chan VerifyTask, 8)
	prefixDone := make(chan struct{})
	go func() {
		defer close(prefixDone)
		l.processPrefix(context.Background(), context.Background(), "root/", "", objCh, fake, nil)
	}()
	// Checkers probe the first page's objects (terminals flow, page 0
	// completes). The rest are emitted but never probed — the kill window.
	for i := 0; i < 2; i++ {
		select {
		case task := <-objCh:
			c.Handle(task)
		case <-time.After(2 * time.Second):
			t.Fatal("page 0 task never arrived")
		}
	}
	<-prefixDone
	time.Sleep(50 * time.Millisecond) // let ticks commit the quiesced state

	// The "killed process's" journal, replayed cold.
	states, err := ReplayJournal(journalPath)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	root := states["root/"]
	if !root.Discovered || root.Done || root.Token != "tok2" {
		t.Errorf("root/ = %+v, want {Discovered, Token: tok2, !Done} (cursor frozen at the last fully-probed page)", root)
	}
	sub := states["root/sub/"]
	if !sub.Discovered || sub.Done || sub.Token != "" {
		t.Errorf("root/sub/ = %+v, want {Discovered only}", sub)
	}

	// Seed the next run from the replayed frontier.
	l2 := NewLister(NewQueue(), nil, NewStats(), &Config{ListType: 2, IsCheck: true})
	seedFromJournal(states, l2)
	seeds := map[string]bool{}
	for _, s := range l2.queue.Drain() {
		seeds[s] = true
	}
	if !seeds["root/\x00tok2"] || !seeds["root/sub/"] || len(seeds) != 2 {
		t.Errorf("resume seeds = %v, want {root/\\x00tok2, root/sub/} — exactly the un-done remainder", seeds)
	}
}

// TestJournalEnabledOnlyMode2S3List — the journal only exists for Mode 2
// S3-listing runs: Mode 1 and file-input modes must not create journal.txt.
func TestJournalEnabledOnlyMode2S3List(t *testing.T) {
	runWithCfg := func(t *testing.T, cfgYAML string, args string) string {
		t.Helper()
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "cfg.yaml")
		if err := os.WriteFile(cfgPath, []byte(cfgYAML+"output_dir: "+dir+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		// args carries the mode-specific flag payload; unused here for Mode 1.
		_ = args
		listCtx, cancel := context.WithCancel(context.Background())
		cancel() // pre-cancelled: no network, no listing, run returns errInterrupted
		_ = run(listCtx, context.Background(), cfg, "mybucket", "", "", "", "", "", "", "", &buf)
		return cfg.OutputDir
	}

	base := "endpoints:\n  - 127.0.0.1:1\nscheme: http\nak: test\nsk: test\n" +
		"list_concurrency: 2\ncheck_concurrency: 2\nis_check: true\nprogress_interval: 1000\n"

	outDir := runWithCfg(t, base+"list_type: 1\n", "")
	if _, err := os.Stat(filepath.Join(outDir, "journal.txt")); !os.IsNotExist(err) {
		t.Errorf("Mode 1 created journal.txt (err=%v), want none", err)
	}

	// -check-file: file input, no journal even though list_type=2.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - 127.0.0.1:1\nscheme: http\nak: test\nsk: test\n" +
		"list_type: 2\nlist_concurrency: 2\ncheck_concurrency: 2\noutput_dir: " + dir + "\n" +
		"is_check: true\nprogress_interval: 1000\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	checkPath := filepath.Join(dir, "check.txt")
	if err := os.WriteFile(checkPath, []byte("wrongbucket|k\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), context.Background(), cfg, "mybucket", "", "", checkPath, "", "", "", "", &buf); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.OutputDir, "journal.txt")); !os.IsNotExist(err) {
		t.Errorf("check-file mode created journal.txt (err=%v), want none", err)
	}
}

// shortWriteFile wraps a real file whose Write short-writes once past a
// byte limit, recording Truncate calls — the C1 regression harness.
type shortWriteFile struct {
	*os.File
	limit     int64
	truncates []int64
}

func (s *shortWriteFile) Write(b []byte) (int, error) {
	if int64(len(b)) > s.limit {
		n := s.limit
		s.limit = 1 << 30
		return int(n), io.ErrShortWrite
	}
	return s.File.Write(b)
}

func (s *shortWriteFile) Truncate(size int64) error {
	s.truncates = append(s.truncates, size)
	return s.File.Truncate(size)
}

// TestAppendJournalBatchRollsBackShortWrite — a partial batch write must
// roll the file back to the batch boundary: a surviving mid-line fragment
// would merge with the NEXT batch's first line on replay, silently eating
// a discovered line (a subtree vanishes from the resume frontier = the
// forbidden coverage hole). Truncate-to-offset + re-send next batch.
func TestAppendJournalBatchRollsBackShortWrite(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("done|early\n"); err != nil {
		t.Fatal(err)
	}
	swf := &shortWriteFile{File: f, limit: 7} // "discovered|p" is 12 bytes: writes 7, fails
	snap := journalSnapshot{
		Discovered: []string{"p"},
		Cursors:    []journalCursorEntry{{Prefix: "p", Token: "T"}},
	}
	if err := appendJournalBatch(swf, snap); err == nil {
		t.Fatal("short write returned nil error")
	}
	if len(swf.truncates) != 1 {
		t.Fatalf("truncate calls = %v, want exactly one rollback to the batch boundary", swf.truncates)
	}
	if want := int64(len("done|early\n")); swf.truncates[0] != want {
		t.Errorf("truncate offset = %d, want %d (pre-batch boundary)", swf.truncates[0], want)
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "done|early\n" {
		t.Errorf("file after rollback = %q, want %q (fragment gone)", data, "done|early\n")
	}
	// The next batch (full write) must land cleanly: no merge with ghosts.
	swf.limit = 1 << 30
	if err := appendJournalBatch(swf, snap); err != nil {
		t.Fatalf("retry append: %v", err)
	}
	data, _ = os.ReadFile(f.Name())
	want := "done|early\ndiscovered|p\ncursor|p|T\n"
	if string(data) != want {
		t.Errorf("file after retry = %q, want %q", data, want)
	}
}

// TestSnapshotSkipsCursorWhenTokenHasPipeOrNewline — the cursor line
// grammar ("cursor|<prefix>|<token>", split at the LAST pipe) assumes the
// token has no "|". A V1 NextMarker is a real key and may contain "|":
// emitting "cursor|p1|tok|pipe" would replay as prefix "p1|tok" token
// "pipe" — a phantom prefix plus a lost cursor (a coverage risk when the
// phantom prefix really exists). A token with "\n" corrupts the line
// format outright. Skip the cursor line for both: the prefix stays
// discovered, so a resume re-lists it from the top (redo work, never a
// hole).
func TestSnapshotSkipsCursorWhenTokenHasPipeOrNewline(t *testing.T) {
	tr := NewJournalTracker()
	tr.RecordSeed("p1", "")
	seq1 := tr.RegisterPage("p1", 1, "tok|pipe")
	tr.Terminal("p1", seq1)
	tr.RecordSeed("p2", "")
	seq2 := tr.RegisterPage("p2", 1, "tok\nnl")
	tr.Terminal("p2", seq2)

	snap := tr.Snapshot()
	for _, c := range snap.Cursors {
		if strings.ContainsAny(c.Token, "|\n") {
			t.Errorf("cursor emitted for token %q — grammar cannot round-trip it", c.Token)
		}
	}
	foundP1, foundP2 := false, false
	for _, p := range snap.Discovered {
		if p == "p1" {
			foundP1 = true
		}
		if p == "p2" {
			foundP2 = true
		}
	}
	if !foundP1 || !foundP2 {
		t.Errorf("discovered = %v, want p1 and p2 present (top-re-list fallback)", snap.Discovered)
	}
}

// TestCheckpointPostSeedTrigger — production wiring starts the checkpoint
// loop BEFORE seeding (main launches the goroutine, then runs the seeding
// branches), so the startup checkpoint sees an empty tracker and the seed
// would otherwise land only at the first tick (10 minutes later): an early
// kill left an empty journal and an unresumable run. The synchronous
// post-seed trigger must persist the seed immediately.
func TestCheckpointPostSeedTrigger(t *testing.T) {
	orig := journalCheckpointInterval
	journalCheckpointInterval = 10 * time.Second
	defer func() { journalCheckpointInterval = orig }()

	dir := t.TempDir()
	o, err := NewOutput(&Config{OutputDir: dir, IsCheck: true}, "test-bkt", FileInputNone)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	journalPath := filepath.Join(dir, "journal.txt")

	tr := NewJournalTracker()
	// Production ordering: the loop is ALREADY running when seeds land.
	r := StartJournalCheckpoints(tr, o, journalPath)
	defer r.Stop()

	tr.RecordSeed("", "")
	tr.RecordSeed("media", "tok1")

	r.CheckpointNow()

	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"discovered|\n", "discovered|media\n", "cursor|media|tok1\n"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("journal after post-seed trigger missing %q (got %q) — early kill is unresumable", want, data)
		}
	}
}
