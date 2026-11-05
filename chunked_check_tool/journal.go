package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// journal.go — kill-9 resumable scan progress for Mode 2.
//
// The journal is an append-only record of the scan's frontier: which
// sub-prefixes were discovered, how far each in-flight prefix's pages are
// confirmed complete, and which prefixes are fully done. A checkpoint
// goroutine appends batches periodically (see runJournalCheckpoints); on a
// kill -9, replaying the journal rebuilds the frontier and the scan resumes
// from it. Every error direction is "recovery redoes work" — never "objects
// silently skipped" (spec §6).

type journalEventKind int

const (
	journalEventDiscovered journalEventKind = iota
	journalEventCursor
	journalEventDone
)

// journalEvent is one parsed journal line. Prefix is verbatim and may
// contain "|" — discovered/done lines carry it as the whole rest of the
// line, cursor lines split at the LAST "|" because tokens never contain
// "|" (V2 continuation tokens are base64, V1 tokens are keys).
type journalEvent struct {
	Kind   journalEventKind
	Prefix string
	Token  string
}

// journalState is the replayed state of one prefix (last event wins).
type journalState struct {
	Discovered bool
	Done       bool
	Token      string
}

func parseJournalLine(line string) (journalEvent, error) {
	verbEnd := strings.IndexByte(line, '|')
	if verbEnd < 0 {
		return journalEvent{}, fmt.Errorf("journal line has no verb separator: %q", line)
	}
	verb, rest := line[:verbEnd], line[verbEnd+1:]
	switch verb {
	case "discovered":
		return journalEvent{Kind: journalEventDiscovered, Prefix: rest}, nil
	case "done":
		return journalEvent{Kind: journalEventDone, Prefix: rest}, nil
	case "cursor":
		tokenStart := strings.LastIndexByte(rest, '|')
		if tokenStart < 0 {
			return journalEvent{}, fmt.Errorf("cursor line has no token separator: %q", line)
		}
		return journalEvent{Kind: journalEventCursor, Prefix: rest[:tokenStart], Token: rest[tokenStart+1:]}, nil
	default:
		return journalEvent{}, fmt.Errorf("unknown journal verb %q", verb)
	}
}

func formatJournalEvent(prefix, token string, kind journalEventKind) string {
	switch kind {
	case journalEventDiscovered:
		return "discovered|" + prefix
	case journalEventDone:
		return "done|" + prefix
	default:
		return "cursor|" + prefix + "|" + token
	}
}

// ReplayJournal reads a journal file and returns the per-prefix state
// table (last event per prefix wins). The final line is dropped when the
// file does not end with a newline — that is a batch the dying process
// half-wrote, and dropping it makes recovery redo work rather than trust a
// fragment. A structurally malformed line anywhere else fails loudly: the
// journal is machine-written, so mid-file corruption means real damage.
func ReplayJournal(path string) (map[string]journalState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open journal %q: %w", path, err)
	}
	defer f.Close()
	torn := false
	if fi, statErr := f.Stat(); statErr == nil && fi.Size() > 0 {
		b := make([]byte, 1)
		if _, readErr := f.ReadAt(b, fi.Size()-1); readErr == nil && b[0] != '\n' {
			torn = true
		}
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	states := make(map[string]journalState)
	lineNum := 0
	// One-line lookahead so the trailing fragment can be skipped without
	// buffering the whole (possibly hundreds of MB) journal.
	var pending string
	hasPending := false
	for scanner.Scan() {
		lineNum++
		if hasPending {
			if err := applyJournalEvent(states, pending); err != nil {
				return nil, fmt.Errorf("journal line %d: %w", lineNum-1, err)
			}
		}
		pending = scanner.Text()
		hasPending = true
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan journal %q: %w", path, err)
	}
	if hasPending && !torn {
		if err := applyJournalEvent(states, pending); err != nil {
			return nil, fmt.Errorf("journal line %d: %w", lineNum, err)
		}
	}
	return states, nil
}

func applyJournalEvent(states map[string]journalState, line string) error {
	ev, err := parseJournalLine(line)
	if err != nil {
		return err
	}
	st := states[ev.Prefix]
	st.Discovered = true
	switch ev.Kind {
	case journalEventCursor:
		st.Token = ev.Token
		st.Done = false
	case journalEventDone:
		st.Done = true
	}
	states[ev.Prefix] = st
	return nil
}
