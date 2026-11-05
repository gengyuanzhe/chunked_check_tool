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
