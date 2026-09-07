package main

import (
	"strings"
	"testing"
)

func TestStats_IncrementAll(t *testing.T) {
	s := NewStats()
	s.IncUploaded(1024)
	s.IncFailed()
	s.IncSingle()
	s.IncMultipart()

	snap := s.Snapshot()
	if snap.Uploaded != 1 {
		t.Errorf("Uploaded = %d, want 1", snap.Uploaded)
	}
	if snap.Bytes != 1024 {
		t.Errorf("Bytes = %d, want 1024", snap.Bytes)
	}
	if snap.Failed != 1 {
		t.Errorf("Failed = %d, want 1", snap.Failed)
	}
	if snap.SingleObjs != 1 {
		t.Errorf("SingleObjs = %d, want 1", snap.SingleObjs)
	}
	if snap.MultipartObjs != 1 {
		t.Errorf("MultipartObjs = %d, want 1", snap.MultipartObjs)
	}
}

func TestStats_IncUploadedAddsBytes(t *testing.T) {
	s := NewStats()
	s.IncUploaded(100)
	s.IncUploaded(200)
	s.IncUploaded(300)
	if snap := s.Snapshot(); snap.Uploaded != 3 || snap.Bytes != 600 {
		t.Errorf("snapshot = %+v, want Uploaded=3 Bytes=600", snap)
	}
}

func TestStats_PrintSummary(t *testing.T) {
	s := NewStats()
	s.IncUploaded(1024)
	s.IncUploaded(2048)
	s.IncFailed()
	s.IncSingle()
	s.IncMultipart()

	var sb strings.Builder
	s.PrintSummary(&sb, 12.5)
	out := sb.String()
	for _, want := range []string{"uploaded", "failed", "bytes", "single_objs", "multipart_objs", "elapsed"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q; out=%q", want, out)
		}
	}
}
