package main

import (
	"strings"
	"testing"
	"time"
)

func TestStatsIncrAndSnapshot(t *testing.T) {
	s := NewStats()
	s.IncrListedObject()
	s.IncrListedObject()
	s.IncrListedMp()
	s.IncrListedMp()
	s.IncrListedMp()
	s.IncrOkObjects()
	s.IncrOkMp()
	s.IncrCorruptedObjects()
	s.IncrCorruptedMp()
	s.IncrListFailed()
	s.IncrCheckFailed()
	s.IncrMpCheckFailed()
	s.AddListCall(2 * time.Millisecond)
	s.AddListCall(4 * time.Millisecond)
	s.AddGetCall(10 * time.Millisecond)
	s.AddGetCall(20 * time.Millisecond)
	s.SetListDuration(10 * time.Second)
	s.SetTotalDuration(15 * time.Second)

	snap := s.Snapshot()
	if snap.ListedObjects != 2 {
		t.Errorf("list_obj=%d want 2", snap.ListedObjects)
	}
	if snap.ListedMp != 3 {
		t.Errorf("list_mp=%d want 3", snap.ListedMp)
	}
	if snap.ListedAll != 5 {
		t.Errorf("list_all=%d want 5 (list_obj+list_mp)", snap.ListedAll)
	}
	if snap.OkObjects != 1 || snap.CorruptedObjects != 1 || snap.ListFailed != 1 || snap.CheckFailed != 1 {
		t.Errorf("counts wrong: %+v", snap)
	}
	if snap.CorruptedMp != 1 {
		t.Errorf("corrupt_mp=%d want 1", snap.CorruptedMp)
	}
	if snap.OkMp != 1 {
		t.Errorf("ok_mp=%d want 1", snap.OkMp)
	}
	if snap.MpCheckFailed != 1 {
		t.Errorf("mp_check_failed=%d want 1", snap.MpCheckFailed)
	}
	if snap.ListCalls != 2 {
		t.Errorf("listcalls=%d want 2", snap.ListCalls)
	}
	if snap.ListAvgLatencyMs != 3.0 {
		t.Errorf("avg latency=%v want 3.0", snap.ListAvgLatencyMs)
	}
	if snap.GetCalls != 2 {
		t.Errorf("getcalls=%d want 2", snap.GetCalls)
	}
	if snap.GetAvgLatencyMs != 15.0 {
		t.Errorf("get avg latency=%v want 15.0", snap.GetAvgLatencyMs)
	}
	if snap.GetTotalSec != 0.03 { // 30ms total
		t.Errorf("get_total_sec=%v want 0.03", snap.GetTotalSec)
	}
	if snap.ListTotalSec != 10.0 {
		t.Errorf("list duration=%v want 10.0", snap.ListTotalSec)
	}
}

func TestStatsPrintSummaryCheckMode(t *testing.T) {
	s := NewStats()
	s.IncrListedObject()
	s.IncrListedObject()
	s.IncrListedMp()
	s.IncrOkObjects()
	s.IncrCorruptedObjects()
	s.IncrOkMp()
	s.IncrCorruptedMp()
	s.IncrListFailed()
	s.IncrCheckFailed()
	s.IncrMpCheckFailed()
	s.SetTotalDuration(15 * time.Second)
	var buf strings.Builder
	s.PrintSummary(&buf, ModeListCheck)
	out := buf.String()
	for _, want := range []string{
		"=== summary ===",
		"list_all: 3",
		"list_obj: 2",
		"list_mp: 1",
		"ok_obj: 1",
		"corrupt_obj: 1",
		"ok_mp: 1",
		"corrupt_mp: 1",
		"list_failed: 1",
		"check_failed: 1",
		"mp_check_failed: 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q\nfull:\n%s", want, out)
		}
	}
}

func TestStatsPrintSummaryListOnlyMode(t *testing.T) {
	s := NewStats()
	s.IncrListedObject()
	s.IncrListedMp()
	s.IncrListFailed()
	s.SetTotalDuration(2 * time.Second)
	var buf strings.Builder
	s.PrintSummary(&buf, ModeListOnly)
	out := buf.String()
	for _, want := range []string{
		"=== summary ===",
		"list_all: 2",
		"list_obj: 1",
		"list_mp: 1",
		"list_failed: 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("list-only summary missing %q\nfull:\n%s", want, out)
		}
	}
	for _, notWant := range []string{
		"ok_obj:",
		"corrupt_obj:",
		"ok_mp:",
		"corrupt_mp:",
		"check_failed:",
		"mp_check_failed:",
		"get_calls:",
	} {
		if strings.Contains(out, notWant) {
			t.Errorf("list-only summary should not contain %q\nfull:\n%s", notWant, out)
		}
	}
}

func TestStatsPrintSummaryListFileMode(t *testing.T) {
	s := NewStats()
	s.SetTotalLines(4)
	for i := 0; i < 4; i++ {
		s.IncrReadLine()
	}
	s.IncrOkMp()
	s.IncrOkMp()
	s.IncrCorruptedMp()
	s.IncrMpCheckFailed()
	s.IncrListFailed()
	s.AddGetCall(10 * time.Millisecond)
	s.SetTotalDuration(20 * time.Second)
	var buf strings.Builder
	s.PrintSummary(&buf, ModeListFile)
	out := buf.String()
	for _, want := range []string{
		"=== summary ===",
		"read: 4/4 total_sec: 20.00",
		"list_failed: 1",
		"get_calls: 1",
		"ok_mp: 2 corrupt_mp: 1 mp_check_failed: 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("list-file summary missing %q\nfull:\n%s", want, out)
		}
	}
	// No S3 LIST happens in list-file mode — list_* lines are all-zero noise.
	// The leading space on " check_failed:" avoids matching mp_check_failed.
	for _, notWant := range []string{"list_all:", "list_obj:", "list_calls:", "ok_obj:", "corrupt_obj:", " check_failed:", "backup_ok:", "read: 4 "} {
		if strings.Contains(out, notWant) {
			t.Errorf("list-file summary should not contain %q\nfull:\n%s", notWant, out)
		}
	}
}

func TestStatsPrintSummaryListFileModeNoTotal(t *testing.T) {
	// Total unknown: bare read count, no /0.
	s := NewStats()
	s.IncrReadLine()
	var buf strings.Builder
	s.PrintSummary(&buf, ModeListFile)
	if !strings.Contains(buf.String(), "read: 1 total_sec") {
		t.Errorf("want read: 1 without denominator, got:\n%s", buf.String())
	}
}

func TestStatsBackupCounters(t *testing.T) {
	s := NewStats()
	s.IncrBackupOk()
	s.IncrBackupOk()
	s.IncrBackupFailed()
	s.IncrBackupMismatch()
	s.IncrBackupSkippedClean()
	snap := s.Snapshot()
	if snap.BackupOk != 2 || snap.BackupFailed != 1 || snap.BackupMismatch != 1 || snap.BackupSkippedClean != 1 {
		t.Errorf("backup counters = %+v", snap)
	}
}

func TestStatsPrintSummaryBackupMode(t *testing.T) {
	s := NewStats()
	s.SetTotalLines(5)
	for i := 0; i < 5; i++ {
		s.IncrReadLine()
	}
	s.IncrBackupOk()
	s.IncrBackupFailed()
	s.IncrBackupMismatch()
	s.IncrBackupSkippedClean()
	s.AddGetCall(5 * time.Millisecond)
	s.SetTotalDuration(3 * time.Second)
	var buf strings.Builder
	s.PrintSummary(&buf, ModeBackup)
	out := buf.String()
	if !strings.Contains(out, "read: 5/5 total_sec: 3.00") {
		t.Errorf("backup summary missing read line\nfull:\n%s", out)
	}
	// One new line, all four counters on it.
	if !strings.Contains(out, "backup_ok: 1 backup_failed: 1 backup_mismatch: 1 backup_skipped_clean: 1") {
		t.Errorf("backup summary line missing\nfull:\n%s", out)
	}
	// get_calls stays (multipart verify uses RangeGetAt).
	if !strings.Contains(out, "get_calls: 1") {
		t.Errorf("backup summary missing get_calls\nfull:\n%s", out)
	}
	// Check-mode counters are not bumped in backup mode — don't print them.
	for _, notWant := range []string{"ok_obj:", "corrupt_obj:", "ok_mp:", "corrupt_mp:", "mp_check_failed:"} {
		if strings.Contains(out, notWant) {
			t.Errorf("backup summary should not contain %q\nfull:\n%s", notWant, out)
		}
	}
	// check_failed appears only in the list_failed form on line 1? No —
	// check_failed: must be absent too.
	if strings.Contains(out, "check_failed:") {
		t.Errorf("backup summary should not contain check_failed\nfull:\n%s", out)
	}
	// No S3 LIST happens in backup mode — list_* lines are all-zero noise.
	for _, notWant := range []string{"list_all:", "list_obj:", "list_calls:"} {
		if strings.Contains(out, notWant) {
			t.Errorf("backup summary should not contain %q\nfull:\n%s", notWant, out)
		}
	}
}
