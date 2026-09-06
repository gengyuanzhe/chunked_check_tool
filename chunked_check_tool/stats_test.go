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
	s.PrintSummary(&buf, true)
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
	s.PrintSummary(&buf, false)
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
