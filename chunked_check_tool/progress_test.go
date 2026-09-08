package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestProgressLocalCounter(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListCheck)
	lc := &localCounter{interval: 3, printer: pp}
	for i := 0; i < 7; i++ {
		lc.incr(s, "checked")
	}
	out := buf.String()
	// Should print at n=3 and n=6, then reset — 2 lines total.
	if strings.Count(out, "[progress]") != 2 {
		t.Errorf("expected 2 progress lines, got:\n%s", out)
	}
}

func TestProgressNoPrintBelowInterval(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListCheck)
	lc := &localCounter{interval: 1000, printer: pp}
	for i := 0; i < 100; i++ {
		lc.incr(s, "listed")
	}
	if buf.Len() != 0 {
		t.Errorf("should not print, got %q", buf.String())
	}
}

func TestProgressPrintsQueueLengths(t *testing.T) {
	s := NewStats()
	// Seed some calls so list_calls/get_calls show non-zero values.
	s.AddListCall(1_000_000) // 1ms
	s.AddGetCall(2_000_000)  // 2ms
	s.IncrCorruptedMp()
	s.IncrMpCheckFailed()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListCheck)
	pp.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{
			Prefix: 7, ObjCh: 42,
			CorruptedObjects: 1, OkMp: 2,
			CorruptedMp: 6,
			ListFailed:  3, CheckFailed: 4, OkObjects: 5,
			MpCheckFailed: 8,
		}
	})
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	for _, want := range []string{
		`list_all=0`,
		`list_obj=0`,
		`list_mp=0`,
		`ok_obj=0`,
		`corrupt_obj=0`,
		`ok_mp=0`,
		`corrupt_mp=1`,
		`mp_check_failed=1`,
		`list_calls=1`,
		`get_calls=1`,
		`q=pfx:7`,
		`obj:42`,
		`cor_obj:1`,
		`ok_mp:2`,
		`cor_mp:6`,
		`mcf:8`,
		`lf:3`,
		`cf:4`,
		`ok_o:5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("progress line missing %q\nfull line:\n%s", want, out)
		}
	}
}

func TestProgressNoQueueLengthsWhenProvidersNil(t *testing.T) {
	s := NewStats()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListCheck)
	// no SetQueueSnapshotProvider call
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	if strings.Contains(out, "q=") {
		t.Errorf("progress line should not contain queue fields when provider is nil\nfull line:\n%s", out)
	}
}

func TestProgressListFileMode(t *testing.T) {
	s := NewStats()
	s.SetTotalLines(50000)
	for i := 0; i < 12000; i++ {
		s.IncrReadLine()
	}
	s.IncrOkMp()
	s.IncrCorruptedMp()
	s.IncrCorruptedMp()
	s.IncrMpCheckFailed()
	s.IncrListFailed()
	s.AddGetCall(2 * time.Millisecond)
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListFile)
	pp.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{
			ObjCh: 42, OkMp: 2, CorruptedMp: 6, MpCheckFailed: 8, ListFailed: 3,
			// bucket-mode-only fields: must NOT be printed in this mode.
			Prefix: 7, CorruptedObjects: 1, CheckFailed: 4, OkObjects: 5,
		}
	})
	pp.MaybePrint(s, "checked", 100)
	out := buf.String()
	for _, want := range []string{
		`read=12000/50000`,
		`ok_mp=1`,
		`corrupt_mp=2`,
		`list_failed=1`,
		`mp_check_failed=1`,
		`get_calls=1`,
		`(checked=100)`,
		`q=obj:42`,
		`cor_mp:6`,
		`ok_mp:2`,
		`mcf:8`,
		`lf:3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("list-file progress line missing %q\nfull line:\n%s", want, out)
		}
	}
	// S3-listing fields are noise in list-file mode (no LIST happens). The
	// leading space on " check_failed="/" cf:" avoids matching the legit
	// mp_check_failed/mcf fields.
	for _, notWant := range []string{"list_all=", "list_obj=", "ok_obj=", "corrupt_obj=", "list_calls=", " check_failed=", "pfx:", "cor_obj:", " cf:", "ok_o:"} {
		if strings.Contains(out, notWant) {
			t.Errorf("list-file progress line should not contain %q\nfull line:\n%s", notWant, out)
		}
	}
}

func TestProgressListFileModeNoTotal(t *testing.T) {
	// Total unknown (count failed / empty file): bare read count, no /0.
	s := NewStats()
	s.IncrReadLine()
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeListFile)
	pp.MaybePrint(s, "checked", 1)
	if !strings.Contains(buf.String(), "read=1 ") {
		t.Errorf("want read=1 without denominator, got:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "read=1/") {
		t.Errorf("should not print /0 total, got:\n%s", buf.String())
	}
}

func TestProgressBackupMode(t *testing.T) {
	s := NewStats()
	s.SetTotalLines(1000)
	for i := 0; i < 300; i++ {
		s.IncrReadLine()
	}
	s.IncrBackupOk()
	s.IncrBackupOk()
	s.IncrBackupOk()
	s.IncrBackupFailed()
	s.IncrBackupMismatch()
	s.IncrBackupSkippedClean()
	s.IncrBackupSkippedClean()
	s.IncrListFailed()
	s.AddGetCall(5 * time.Millisecond)
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf, ModeBackup)
	pp.SetQueueSnapshotProvider(func() QueueSnapshot {
		return QueueSnapshot{
			ObjCh: 11, ListFailed: 1, BackupOk: 2, BackupFailed: 3, Mismatch: 4, BackupSkippedClean: 5,
			// bucket-mode-only fields: must NOT be printed in this mode.
			Prefix: 7, CorruptedObjects: 1, OkMp: 2, CorruptedMp: 6, CheckFailed: 8, OkObjects: 9,
		}
	})
	pp.MaybePrint(s, "backed", 50)
	out := buf.String()
	for _, want := range []string{
		`read=300/1000`,
		`list_failed=1`,
		`backup_ok=3`,
		`backup_failed=1`,
		`backup_mismatch=1`,
		`backup_skipped_clean=2`,
		`get_calls=1`,
		`(backed=50)`,
		`q=obj:11`,
		`lf:1`,
		`bok:2`,
		`bfail:3`,
		`mm:4`,
		`bsc:5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("backup progress line missing %q\nfull line:\n%s", want, out)
		}
	}
	// Check-mode outcome counters can never move in backup mode.
	for _, notWant := range []string{"list_all=", "ok_obj=", "corrupt_obj=", "ok_mp=", "corrupt_mp=", "mp_check_failed=", "list_calls=", "pfx:", "cor_obj:", "ok_o:", "cor_mp:", "mcf:", "cf:"} {
		if strings.Contains(out, notWant) {
			t.Errorf("backup progress line should not contain %q\nfull line:\n%s", notWant, out)
		}
	}
}
