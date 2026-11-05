package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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

// JournalTracker accumulates scan progress at page granularity. It is a
// pure accounting side-channel: nothing in the scan pipeline blocks on it.
//
// The completion position of a prefix is the highest page seq whose objects
// have ALL reported terminal (checker Handle returned, result line pushed
// into an output channel) — consecutive from page 0. The cursor the journal
// persists is the continuation token AT that position (the next token the
// page at that position returned), or the seed token when no page has
// completed yet. Because the position never crosses a page with unreported
// objects, a hard abort mid-page (objects pushed but never probed) freezes
// the cursor BEFORE that page: recovery re-lists it. That is the load-bearing
// safety property — see TestTrackerFrozenOnPartialPage.
//
// Memory is bounded by drop-after-commit: prefixes whose done state has
// been persisted (Commit) leave the map, and per-page bookkeeping below the
// completion position is discarded as the position advances. The journal
// file is the authoritative full state; the map only ever holds one
// checkpoint interval of delta plus in-flight work.
type JournalTracker struct {
	mu sync.Mutex
	// states holds prefixes with listing activity (seeded or pages
	// registered) that have not been persisted as done yet.
	states map[string]*journalPrefixState
	// pendingDiscovered holds discovery events not yet persisted by a
	// checkpoint Commit (the Discovered delta).
	pendingDiscovered map[string]struct{}
}

type journalPrefixState struct {
	baseToken      string
	paginationDone bool
	maxSeq         int32            // highest registered page seq
	position       int32            // highest page seq fully terminal, consecutive from 0; -1 = none
	outstanding    map[int32]int    // pageSeq -> terminals still expected
	nextTokens     map[int32]string // pageSeq -> the page's returned next token
}

func NewJournalTracker() *JournalTracker {
	return &JournalTracker{
		states:            make(map[string]*journalPrefixState),
		pendingDiscovered: make(map[string]struct{}),
	}
}

func (t *JournalTracker) stateFor(prefix string) *journalPrefixState {
	st, ok := t.states[prefix]
	if !ok {
		st = &journalPrefixState{
			maxSeq:      -1,
			position:    -1,
			outstanding: make(map[int32]int),
			nextTokens:  make(map[int32]string),
		}
		t.states[prefix] = st
	}
	return st
}

// RecordSeed records a prefix entering the scan (root seed or a resume
// seed). token is the continuation token listing starts from ("" = from the
// top). The seed line lands in the next checkpoint's Discovered batch; the
// token survives in cursor snapshots even before the first page completes,
// so a run killed early does not fall back to re-listing from the top.
func (t *JournalTracker) RecordSeed(prefix, token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pendingDiscovered[prefix] = struct{}{}
	t.stateFor(prefix).baseToken = token
}

// AddDiscovered records a sub-prefix discovered by a parent page's LIST.
// Callers MUST call this before pushing that page's objects to objCh: the
// parent's position may only cross the page once the discovery is visible
// here (otherwise a checkpoint could persist the parent's progress past the
// page while the discovered prefix was never recorded — the subtree would
// vanish from the replayed frontier).
func (t *JournalTracker) AddDiscovered(prefix string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pendingDiscovered[prefix] = struct{}{}
}

// RegisterPage records a successfully listed page: objCount objects will
// report terminal states, nextToken is the page's returned continuation.
// The count is the FULL page length, registered once — it never shrinks
// with partial emission, which is what freezes the position on hard abort.
// Returns the page seq tasks must carry for their Terminal reports.
func (t *JournalTracker) RegisterPage(prefix string, objCount int, nextToken string) int32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.stateFor(prefix)
	st.maxSeq++
	seq := st.maxSeq
	st.outstanding[seq] = objCount
	st.nextTokens[seq] = nextToken
	st.advance()
	return seq
}

// Terminal reports one object finished processing. Late or duplicate
// reports are ignored (safe direction: at most they delay the position).
func (t *JournalTracker) Terminal(prefix string, seq int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.states[prefix]
	if !ok {
		return
	}
	if n, ok := st.outstanding[seq]; ok && n > 0 {
		st.outstanding[seq] = n - 1
	}
	st.advance()
}

// MarkPaginationDone records that the prefix's listing returned next=="".
func (t *JournalTracker) MarkPaginationDone(prefix string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.states[prefix]; ok {
		st.paginationDone = true
	}
}

// advance moves the completion position forward while consecutive pages
// are fully terminal, discarding the bookkeeping it passes (bounded memory).
func (st *journalPrefixState) advance() {
	for st.position < st.maxSeq && st.outstanding[st.position+1] == 0 {
		st.position++
		delete(st.outstanding, st.position)
		delete(st.nextTokens, st.position-1)
	}
}

// continuation returns the token listing should resume from: the last
// fully-terminal page's next token, or the seed token when no page has
// completed. "" means "from the top".
func (st *journalPrefixState) continuation() string {
	if st.position >= 0 {
		return st.nextTokens[st.position]
	}
	return st.baseToken
}

// isDone reports pagination ended with every page terminal.
func (st *journalPrefixState) isDone() bool {
	return st.paginationDone && st.maxSeq >= 0 && st.position == st.maxSeq
}

type journalCursorEntry struct {
	Prefix string
	Token  string
}

// journalSnapshot is one checkpoint's delta: new discoveries, the current
// cursor of every in-flight prefix (re-sent each checkpoint; replay keeps
// the last), and newly-done prefixes.
type journalSnapshot struct {
	Discovered []string
	Cursors    []journalCursorEntry
	Dones      []string
}

// Snapshot takes the current delta. Call before flushing outputs, persist
// after (see runJournalCheckpoints); the snapshot-then-flush order is what
// guarantees every terminal counted here has its result line flushed.
func (t *JournalTracker) Snapshot() journalSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	var snap journalSnapshot
	for p := range t.pendingDiscovered {
		snap.Discovered = append(snap.Discovered, p)
	}
	for prefix, st := range t.states {
		if st.isDone() {
			snap.Dones = append(snap.Dones, prefix)
			continue
		}
		if tok := st.continuation(); tok != "" && !strings.ContainsAny(tok, "|\n") {
			// A token with "|" breaks the cursor grammar's last-pipe split
			// (V1 NextMarker is a key and can contain it); "\n" breaks the
			// line format. Skipping costs a full prefix re-list on resume —
			// redo work, never a coverage hole.
			snap.Cursors = append(snap.Cursors, journalCursorEntry{Prefix: prefix, Token: tok})
		}
	}
	return snap
}

// Commit drops the state the snapshot persisted: the discovered delta
// entries and prefixes recorded as done. Call only after the journal batch
// was successfully written; a lost Commit at most re-sends duplicate lines
// (replay is last-event-wins, so duplicates are harmless).
func (t *JournalTracker) Commit(snap journalSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range snap.Discovered {
		delete(t.pendingDiscovered, p)
	}
	for _, p := range snap.Dones {
		delete(t.states, p)
	}
}

// trackedPrefixes reports the live in-memory prefix count (test hook for
// the drop-after-commit memory bound).
func (t *JournalTracker) trackedPrefixes() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.states)
}

// journalCheckpointInterval is how often scan progress is persisted to the
// journal; a kill -9 loses at most one interval of work. Package var (not
// config) so tests can shrink it — same pattern as outputFlushInterval.
// The JOURNAL_INTERVAL_SEC env var (see journalIntervalFromEnv) exists for
// the kill -9 smoke runbook: local-minio scans finish in seconds, long
// before the first 10-minute tick, so the runbook shrinks the interval to
// exercise mid-run checkpoints.
var journalCheckpointInterval = 10 * time.Minute

// journalIntervalFromEnv resolves JOURNAL_INTERVAL_SEC (seconds). Unset or
// empty keeps def. Anything but a positive integer is a loud error: a bad
// value must never be silently ignored, since it silently changes the
// kill -9 loss bound.
func journalIntervalFromEnv(getenv func(string) string, def time.Duration) (time.Duration, error) {
	v := getenv("JOURNAL_INTERVAL_SEC")
	if v == "" {
		return def, nil
	}
	sec, err := strconv.Atoi(v)
	if err != nil || sec < 1 {
		return 0, fmt.Errorf("JOURNAL_INTERVAL_SEC=%q: want an integer >= 1 (seconds)", v)
	}
	return time.Duration(sec) * time.Second, nil
}

// runJournalCheckpoints persists scan progress to journalPath until ctx is
// cancelled, then once more (final state) and returns. One checkpoint runs
// IMMEDIATELY at start, before any tick — but production wiring starts this
// loop BEFORE seeding, so that checkpoint sees an empty tracker. The seed
// lands via the trigger channel instead: main calls JournalRunner's
// CheckpointNow right after seeding, making the seed durable long before
// the first tick (a kill seconds after seeding stays resumable).
//
// Each checkpoint is the ordering-invariant sequence from spec §5:
//
//	snapshot tracker -> Output.FlushAndWait -> append batch -> Commit
//
// Snapshot BEFORE flushing: Terminal is counted after the result line was
// pushed into an output channel, so everything counted in the snapshot was
// pushed before the flush started and is guaranteed to be written by
// FlushAndWait's drain. Write failure skips Commit — the next checkpoint
// re-sends the delta (replay is last-event-wins, duplicates are harmless).
//
// Lifecycle: cancel ctx after listing and checking have fully finished and
// BEFORE out.Close() — FlushAndWait must never meet closed channels.
func runJournalCheckpoints(ctx context.Context, tr *JournalTracker, out *Output, journalPath string, trigger <-chan chan struct{}) {
	f, err := os.OpenFile(journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		// The scan itself stays correct without a journal; only kill -9
		// resumability degrades. Log and continue rather than kill the run.
		log.Printf("journal: open %s: %v (journal disabled, kill -9 will not be resumable)", journalPath, err)
		return
	}
	defer f.Close()
	ticker := time.NewTicker(journalCheckpointInterval)
	defer ticker.Stop()
	checkpoint := func() {
		snap := tr.Snapshot()
		out.FlushAndWait()
		if err := appendJournalBatch(f, snap); err != nil {
			log.Printf("journal: append batch to %s: %v", journalPath, err)
			return
		}
		tr.Commit(snap)
	}
	checkpoint()
	for {
		select {
		case <-ctx.Done():
			checkpoint()
			return
		case reply := <-trigger:
			checkpoint()
			close(reply)
		case <-ticker.C:
			checkpoint()
		}
	}
}

// JournalRunner owns the checkpoint loop's lifecycle: the periodic ticker
// plus on-demand synchronous checkpoints (the post-seed one above).
type JournalRunner struct {
	cancel  context.CancelFunc
	trigger chan chan struct{}
	done    chan struct{}
}

// StartJournalCheckpoints launches the loop. Stop cancels it and waits for
// the final checkpoint; CheckpointNow forces one checkpoint synchronously.
func StartJournalCheckpoints(tr *JournalTracker, out *Output, journalPath string) *JournalRunner {
	ctx, cancel := context.WithCancel(context.Background())
	r := &JournalRunner{
		cancel:  cancel,
		trigger: make(chan chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		runJournalCheckpoints(ctx, tr, out, journalPath, r.trigger)
		close(r.done)
	}()
	return r
}

// CheckpointNow runs one checkpoint synchronously. No-op after the loop has
// stopped (done guards the send against blocking on a dead select).
func (r *JournalRunner) CheckpointNow() {
	reply := make(chan struct{})
	select {
	case r.trigger <- reply:
	case <-r.done:
		return
	}
	select {
	case <-reply:
	case <-r.done:
	}
}

// Stop cancels the loop and waits for its final checkpoint to finish. Call
// after listing/checking are fully done and BEFORE out.Close().
func (r *JournalRunner) Stop() {
	r.cancel()
	<-r.done
}

// journalAppendFile is the file surface appendJournalBatch needs; an
// interface lets tests inject short-write/rollback behavior.
type journalAppendFile interface {
	io.Writer
	io.Seeker
	Truncate(size int64) error
}

// appendJournalBatch writes one checkpoint batch: all discovered lines
// first, then cursors, then dones. The within-batch order matters for the
// torn mid-batch kill: "child discovered, parent not done" is recoverable
// (re-list the parent), while the reverse would drop the subtree from the
// replayed frontier entirely.
//
// A short write (ENOSPC, EIO) would leave a mid-line fragment that the
// NEXT batch appends onto; replay would merge the two half-lines and
// silently eat a discovered line — a subtree vanishing from the frontier.
// So on a short write the file is truncated back to the pre-batch offset
// and the error is returned: the next checkpoint re-sends the whole batch.
func appendJournalBatch(f journalAppendFile, snap journalSnapshot) error {
	var b []byte
	for _, p := range snap.Discovered {
		b = append(b, formatJournalEvent(p, "", journalEventDiscovered)...)
		b = append(b, '\n')
	}
	for _, c := range snap.Cursors {
		b = append(b, formatJournalEvent(c.Prefix, c.Token, journalEventCursor)...)
		b = append(b, '\n')
	}
	for _, p := range snap.Dones {
		b = append(b, formatJournalEvent(p, "", journalEventDone)...)
		b = append(b, '\n')
	}
	if len(b) == 0 {
		return nil
	}
	off, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	n, err := f.Write(b)
	if n != len(b) || err != nil {
		if terr := f.Truncate(off); terr != nil {
			return fmt.Errorf("write: %v (rollback truncate: %v)", err, terr)
		}
		return err
	}
	return nil
}

// loadJournalForResume reads and sanity-checks a journal for resuming. An
// empty journal means the run died before its first checkpoint (with the
// loop's immediate startup checkpoint, that takes a kill within the very
// first moments) or the path is wrong: silently scanning nothing would look
// like a completed scan, so fail with guidance to re-run the original
// command instead.
func loadJournalForResume(path string) (map[string]journalState, error) {
	states, err := ReplayJournal(path)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		return nil, fmt.Errorf("journal %q has no entries: the run died before its first checkpoint or the path is wrong — re-run the original scan command", path)
	}
	return states, nil
}

// seedFromJournal seeds the BFS from a replayed journal state table: done
// prefixes are skipped, cursor entries continue from their token, anything
// else lists from the top. Returns whether anything was seeded (drives the
// empty-queue close, same as the other seed paths).
func seedFromJournal(states map[string]journalState, lister *Lister) bool {
	seeded := false
	for prefix, st := range states {
		switch {
		case st.Done:
			// fully covered by the killed run — skip
		case st.Token != "":
			lister.SeedWithToken(prefix, st.Token)
			seeded = true
		default:
			lister.Seed(prefix)
			seeded = true
		}
	}
	return seeded
}
