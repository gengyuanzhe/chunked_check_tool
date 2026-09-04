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
	s.IncrMultipart()
	s.IncrCorrupted()
	s.IncrListFailed()
	s.IncrCheckFailed()
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
	if snap.Multipart != 1 || snap.Corrupted != 1 || snap.ListFailed != 1 || snap.CheckFailed != 1 {
		t.Errorf("counts wrong: %+v", snap)
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
	if snap.ListTotalDurationSec != 10.0 {
		t.Errorf("list duration=%v want 10.0", snap.ListTotalDurationSec)
	}
}

func TestStatsWriteFile(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrMultipart()
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
	for _, key := range []string{"total_objects:", "list_calls:", "list_avg_latency_ms:", "list_total_duration_sec:", "get_calls:", "get_avg_latency_ms:", "total_duration_sec:", "multipart:", "corrupted:"} {
		if !strings.Contains(content, key) {
			t.Errorf("missing %q in:\n%s", key, content)
		}
	}
}

func TestStatsWriteFileListOnly(t *testing.T) {
	s := NewStats()
	s.IncrListed()
	s.IncrMultipart()
	s.SetListDuration(1 * time.Second)
	s.SetTotalDuration(2 * time.Second)
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.txt")
	if err := s.WriteToFile(path, false); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	content := string(data)
	if strings.Contains(content, "corrupted:") {
		t.Errorf("list-only mode should not contain corrupted: but got:\n%s", content)
	}
}
