package main

import (
	"os"
	"path/filepath"
	"testing"
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
	tr.RegisterPage("p", 1, "T1")     // page 0 completes fully
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
