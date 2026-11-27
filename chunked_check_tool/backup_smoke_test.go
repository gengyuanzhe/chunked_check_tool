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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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

	client, err := NewAWSClient(endpoint, ak, sk, false, 0, 0)
	if err != nil {
		t.Fatalf("aws client: %v", err)
	}
	ctx := context.Background()
	for _, b := range []string{srcBkt, dstBkt} {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)})
		if err != nil {
			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "BucketAlreadyOwnedByYou" {
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
	mpPart2 := fill(1<<20, 5)

	putRegular := func(key string, content []byte) {
		t.Helper()
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(srcBkt),
			Key:           aws.String(key),
			Body:          bytes.NewReader(content),
			ContentLength: aws.Int64(int64(len(content))),
		})
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	putMultipart := func(key string, parts ...[]byte) {
		t.Helper()
		uploadID, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(srcBkt),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("initiate %s: %v", key, err)
		}
		var completed []types.CompletedPart
		for i, p := range parts {
			op, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:        aws.String(srcBkt),
				Key:           aws.String(key),
				UploadId:      uploadID.UploadId,
				PartNumber:    aws.Int32(int32(i + 1)),
				Body:          bytes.NewReader(p),
				ContentLength: aws.Int64(int64(len(p))),
			})
			if err != nil {
				t.Fatalf("part %d of %s: %v", i+1, key, err)
			}
			// Trim to mirror the tool's own CompleteMultipart path (it
			// re-wraps UploadPart's trimmed etag).
			completed = append(completed, types.CompletedPart{
				PartNumber: aws.Int32(int32(i + 1)),
				ETag:       aws.String(trimETagQuotes(aws.ToString(op.ETag))),
			})
		}
		if _, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(srcBkt),
			Key:             aws.String(key),
			UploadId:        uploadID.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		}); err != nil {
			t.Fatalf("complete %s: %v", key, err)
		}
	}

	putRegular("reg-corrupt", regCorrupt)
	putRegular("reg-clean", regClean)
	putMultipart("mp-corrupt", mpPart1Corrupt, mpPart2)
	putMultipart("mp-clean", mpPart1Clean, mpPart2)

	// Source ETags for the post-run comparison.
	headETag := func(bucket, key string) string {
		t.Helper()
		out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("stat %s/%s: %v", bucket, key, err)
		}
		return trimETagQuotes(aws.ToString(out.ETag))
	}
	srcETags := map[string]string{}
	for _, k := range []string{"reg-corrupt", "reg-clean", "mp-corrupt", "mp-clean"} {
		srcETags[k] = headETag(srcBkt, k)
	}

	// Backup list: 4 relays, 1 type mismatch, 1 malformed. Backup does not
	// probe — reg-clean and mp-clean are relayed like the corrupt ones.
	dir := t.TempDir()
	backupPath := filepath.Join(dir, "list.txt")
	list := fmt.Sprintf("%s|reg-corrupt\n"+
		"%s|reg-clean\n"+
		"%s|mp-corrupt|2|0|%d\n"+
		"%s|mp-clean|2|0|%d\n"+
		"%s|mp-clean\n"+ // regular line for a multipart object → mismatch
		"%s|bad|1|100\n", // malformed → parse_failed
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
		BackupOutputDir:  dir,
		BackupBucket:     dstBkt,
		CheckConcurrency: 4,
		ListConcurrency:  2,
		ListAPIVersion:   2,
		ProgressInterval: 100,
	}
	var buf bytes.Buffer
	if err := run(ctx, cfg, srcBkt, "", "", "", backupPath, "", "", &buf); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Local result files (order-insensitive: check workers finish
	// concurrently). dumpResults prints everything when anything is off.
	dumpResults := func() {
		for _, name := range []string{"backup_ok.txt", "backup_failed.txt", "backup_failed.log", "mismatch.txt", "parse_failed.txt"} {
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
	assertLines("backup_ok.txt", srcBkt+"|reg-corrupt", srcBkt+"|reg-clean",
		fmt.Sprintf("%s|mp-corrupt|2|0|%d", srcBkt, partSize),
		fmt.Sprintf("%s|mp-clean|2|0|%d", srcBkt, partSize))
	assertLines("mismatch.txt", srcBkt+"|mp-clean")
	// Malformed input lines route to parse_failed (invariant 4a), not
	// list_failed (S3 LIST failures).
	assertLines("parse_failed.txt", srcBkt+"|bad|1|100")
	if data, err := os.ReadFile(filepath.Join(dir, "backup_failed.txt")); err == nil && len(data) > 0 {
		dumpResults()
		t.Errorf("backup_failed.txt not empty: %s", data)
	}
	out := buf.String()
	if !strings.Contains(out, "backup_ok: 4 backup_failed: 0 backup_mismatch: 1") {
		dumpResults()
		t.Errorf("summary line wrong")
	}

	// Destination objects: ETag equal to source and bytes identical.
	getObject := func(bucket, key string) []byte {
		t.Helper()
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("get %s/%s: %v", bucket, key, err)
		}
		defer out.Body.Close()
		b, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatalf("read %s/%s: %v", bucket, key, err)
		}
		return b
	}
	for _, k := range []string{"reg-corrupt", "reg-clean", "mp-corrupt", "mp-clean"} {
		if got := headETag(dstBkt, k); got != srcETags[k] {
			t.Errorf("dst etag %s = %q, src %q", k, got, srcETags[k])
		}
		if dst, want := getObject(dstBkt, k), getObject(srcBkt, k); !bytes.Equal(dst, want) {
			t.Errorf("relayed %s differs from source (%d vs %d bytes)", k, len(dst), len(want))
		}
	}

	// The list archive landed under .backup_lists/.
	archives := 0
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(dstBkt),
		Prefix: aws.String(".backup_lists/"),
	}
	for {
		out, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Fatalf("list archives: %v", err)
		}
		archives += len(out.Contents)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		input.ContinuationToken = out.NextContinuationToken
	}
	if archives < 1 {
		t.Errorf("no .backup_lists/ archive found in destination bucket")
	}
}
