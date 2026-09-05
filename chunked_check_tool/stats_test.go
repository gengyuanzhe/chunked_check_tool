package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatsIncrAndSnapshot(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrListed()
	s.IncrOkObjects()
	s.IncrOkMp()
	s.IncrCorruptedObjects()
	s.IncrCorruptedMp()
	s.IncrListFailed()
	s.IncrCheckFailed()
	s.IncrMultipartCheckFailed()
	s.AddListCall(2 * time.Millisecond)
	s.AddListCall(4 * time.Millisecond)
	s.AddGetCall(10 * time.Millisecond)
	s.AddGetCall(20 * time.Millisecond)
	s.SetListDuration(10 * time.Second)
	s.SetTotalDuration(15 * time.Second)

	snap := s.Snapshot()
	if snap.ListedTotal != 2 {
		t.Errorf("listed=%d want 2", snap.ListedTotal)
	}
	if snap.OkObjects != 1 || snap.CorruptedObjects != 1 || snap.ListFailed != 1 || snap.CheckFailed != 1 {
		t.Errorf("counts wrong: %+v", snap)
	}
	if snap.CorruptedMp != 1 {
		t.Errorf("corrupted_mp=%d want 1", snap.CorruptedMp)
	}
	if snap.OkMp != 1 {
		t.Errorf("ok_mp=%d want 1", snap.OkMp)
	}
	if snap.MultipartCheckFailed != 1 {
		t.Errorf("multipart_check_failed=%d want 1", snap.MultipartCheckFailed)
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

func TestStatsWriteFile(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrOkMp()
	s.AddListCall(1 * time.Millisecond)
	s.SetListDuration(2 * time.Second)
	s.SetTotalDuration(5 * time.Second)

	dir := t.TempDir()
	path := filepath.Join(dir, "stats.txt")
	if err := s.WriteToFile(path, true); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	content := string(data)
	for _, key := range []string{"total_objects:", "total_sec:", "list_calls:", "list_avg_latency_ms:", "list_total_sec:", "get_calls:", "get_avg_latency_ms:", "get_total_sec:", "ok_objects:", "corrupted_objects:", "ok_mp:", "corrupted_mp:"} {
		if !strings.Contains(content, key) {
			t.Errorf("missing %q in:\n%s", key, content)
		}
	}
}

func TestStatsWriteFileListOnly(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrOkMp()
	s.SetListDuration(1 * time.Second)
	s.SetTotalDuration(2 * time.Second)
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.txt")
	if err := s.WriteToFile(path, false); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	content := string(data)
	if strings.Contains(content, "corrupted_objects:") {
		t.Errorf("list-only mode should not contain corrupted_objects: but got:\n%s", content)
	}
	if strings.Contains(content, "ok_mp:") {
		t.Errorf("list-only mode should not contain ok_mp: but got:\n%s", content)
	}
}
