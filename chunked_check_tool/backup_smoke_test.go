// backup_smoke_test.go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// TestBackupRelayRealS3 drives the full -backup-file relay against a real
// S3 endpoint: seeds four object flavors (regular corrupt/clean, multipart
// corrupt/clean), runs the backup, and verifies the ETag-gated routing
// plus byte-identical relays in the destination bucket.
//
// Skipped unless S3_ENDPOINT is set:
//
//	S3_ENDPOINT=127.0.0.1:9100 S3_AK=testak S3_SK=testsk123 \
//	  go test . -run TestBackupRelayRealS3 -v
func TestBackupRelayRealS3(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set S3_ENDPOINT to run the real-S3 backup smoke test")
	}
	ak, sk := envOr("S3_AK", "testak"), envOr("S3_SK", "testsk123")
	srcBkt := envOr("S3_BUCKET", "verify-src")
	dstBkt := envOr("S3_BACKUP_BUCKET", "verify-dst")

	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(ak, sk, ""),
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	ctx := context.Background()
	for _, b := range []string{srcBkt, dstBkt} {
		if err := client.MakeBucket(ctx, b, minio.MakeBucketOptions{}); err != nil {
			var er minio.ErrorResponse
			if !errors.As(err, &er) || er.Code != "BucketAlreadyOwnedByYou" {
				t.Fatalf("make bucket %s: %v", b, err)
			}
		}
	}

	// Test data. The corrupt bodies start with a chunk-signature header so
	// the verification probes match chunkSigRe; the rest is a deterministic
	// filler pattern.
	const partSize = 5 << 20 // 5 MiB
	sig := "100000;chunk-signature=" + strings.Repeat("ab", 32) + "\r\n"
	fill := func(n int, seed byte) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i)%251 + seed
		}
		return b
	}
	regCorrupt := append([]byte(sig), fill(1<<20, 1)...)
	regClean := fill(1<<20, 2)
	mpPart1Corrupt := append([]byte(sig), fill(partSize-len(sig), 3)...)
	mpPart1Clean := fill(partSize, 4)
	mpPart2 := fill(1 << 20, 5)

	putRegular := func(key string, content []byte) {
		t.Helper()
		_, err := client.PutObject(ctx, srcBkt, key, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	putMultipart := func(key string, parts ...[]byte) {
		t.Helper()
		core := minio.Core{Client: client}
		uploadID, err := core.NewMultipartUpload(ctx, srcBkt, key, minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("initiate %s: %v", key, err)
		}
		var completed []minio.CompletePart
		for i, p := range parts {
			op, err := core.PutObjectPart(ctx, srcBkt, key, uploadID, i+1, bytes.NewReader(p), int64(len(p)), minio.PutObjectPartOptions{})
			if err != nil {
				t.Fatalf("part %d of %s: %v", i+1, key, err)
			}
			completed = append(completed, minio.CompletePart{PartNumber: op.PartNumber, ETag: op.ETag})
		}
		if _, err := core.CompleteMultipartUpload(ctx, srcBkt, key, uploadID, completed, minio.PutObjectOptions{}); err != nil {
			t.Fatalf("complete %s: %v", key, err)
		}
	}

	putRegular("reg-corrupt", regCorrupt)
	putRegular("reg-clean", regClean)
	putMultipart("mp-corrupt", mpPart1Corrupt, mpPart2)
	putMultipart("mp-clean", mpPart1Clean, mpPart2)

	// Source ETags for the post-run comparison.
	srcETags := map[string]string{}
	for _, k := range []string{"reg-corrupt", "reg-clean", "mp-corrupt"} {
		info, err := client.StatObject(ctx, srcBkt, k, minio.StatObjectOptions{})
		if err != nil {
			t.Fatalf("stat src %s: %v", k, err)
		}
		srcETags[k] = info.ETag
	}

	// Backup list: 3 relays, 1 clean skip, 1 type mismatch, 1 malformed.
	dir := t.TempDir()
	backupPath := filepath.Join(dir, "list.txt")
	list := fmt.Sprintf("%s|reg-corrupt\n"+
		"%s|reg-clean\n"+
		"%s|mp-corrupt|2|0|%d\n"+
		"%s|mp-clean|2|0|%d\n"+
		"%s|mp-clean\n"+ // regular line for a multipart object → mismatch
		"%s|bad|1|100\n", // malformed → list_failed
		srcBkt, srcBkt, srcBkt, partSize, srcBkt, partSize, srcBkt, srcBkt)
	if err := os.WriteFile(backupPath, []byte(list), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		Endpoints:        []string{endpoint},
		Scheme:           "http",
		AK:               ak,
		SK:               sk,
		OutputDir:        dir,
		BackupBucket:     dstBkt,
		CheckConcurrency: 4,
		ListConcurrency:  2,
		ListAPIVersion:   2,
		ProgressInterval: 100,
	}
	var buf bytes.Buffer
	if err := run(ctx, cfg, srcBkt, "", "", "", backupPath, &buf); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Local result files (order-insensitive: check workers finish
	// concurrently). dumpResults prints everything when anything is off.
	dumpResults := func() {
		for _, name := range []string{"backup_ok.txt", "backup_failed.txt", "backup_failed.log", "backup_skipped_clean.txt", "mismatch.txt", "list_failed.txt"} {
			if data, err := os.ReadFile(filepath.Join(dir, name)); err == nil && len(data) > 0 {
				t.Logf("--- %s ---\n%s", name, data)
			}
		}
		t.Logf("--- stdout ---\n%s", buf.String())
	}
	assertLines := func(name string, want ...string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		got := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
		if len(got) == 1 && got[0] == "" {
			got = nil
		}
		sort.Strings(got)
		sort.Strings(want)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			dumpResults()
			t.Errorf("%s = %v, want %v (order-insensitive run)", name, got, want)
		}
	}
	assertLines("backup_ok.txt", "reg-corrupt", "reg-clean", "mp-corrupt")
	assertLines("backup_skipped_clean.txt", "mp-clean")
	assertLines("mismatch.txt", srcBkt+"|mp-clean")
	assertLines("list_failed.txt", srcBkt+"|bad|1|100")
	if data, err := os.ReadFile(filepath.Join(dir, "backup_failed.txt")); err == nil && len(data) > 0 {
		dumpResults()
		t.Errorf("backup_failed.txt not empty: %s", data)
	}
	out := buf.String()
	if !strings.Contains(out, "backup_ok: 3 backup_failed: 0 backup_mismatch: 1 backup_skipped_clean: 1") {
		dumpResults()
		t.Errorf("summary line wrong")
	}

	// Destination objects: ETag equal to source and bytes identical.
	for _, k := range []string{"reg-corrupt", "reg-clean", "mp-corrupt"} {
		info, err := client.StatObject(ctx, dstBkt, k, minio.StatObjectOptions{})
		if err != nil {
			t.Fatalf("stat dst %s: %v", k, err)
		}
		if info.ETag != srcETags[k] {
			t.Errorf("dst etag %s = %q, src %q", k, info.ETag, srcETags[k])
		}
		got, err := client.GetObject(ctx, dstBkt, k, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("get dst %s: %v", k, err)
		}
		dst, err := io.ReadAll(got)
		got.Close()
		if err != nil {
			t.Fatalf("read dst %s: %v", k, err)
		}
		src, err := client.GetObject(ctx, srcBkt, k, minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("get src %s: %v", k, err)
		}
		want, err := io.ReadAll(src)
		src.Close()
		if err != nil {
			t.Fatalf("read src %s: %v", k, err)
		}
		if !bytes.Equal(dst, want) {
			t.Errorf("relayed %s differs from source (%d vs %d bytes)", k, len(dst), len(want))
		}
	}

	// mp-clean must NOT be in the destination.
	if _, err := client.StatObject(ctx, dstBkt, "mp-clean", minio.StatObjectOptions{}); err == nil {
		t.Errorf("mp-clean must not be relayed (clean multipart)")
	}

	// The list archive landed under .backup_lists/.
	archives := 0
	for obj := range client.ListObjects(ctx, dstBkt, minio.ListObjectsOptions{Prefix: ".backup_lists/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list archives: %v", obj.Err)
		}
		archives++
	}
	if archives < 1 {
		t.Errorf("no .backup_lists/ archive found in destination bucket")
	}
}
