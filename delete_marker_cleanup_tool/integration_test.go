//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func skipIfNoMinIO(t *testing.T) (endpoint, ak, sk string) {
	t.Helper()
	endpoint = os.Getenv("MINIO_ENDPOINT")
	ak = os.Getenv("MINIO_AK")
	sk = os.Getenv("MINIO_SK")
	if endpoint == "" || ak == "" || sk == "" {
		t.Skip("set MINIO_ENDPOINT/MINIO_AK/MINIO_SK to run integration test")
	}
	return
}

func newMinIOAdminClient(t *testing.T, endpoint, ak, sk string) *minio.Client {
	t.Helper()
	c, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}
	return c
}

func setupBucket(t *testing.T, c *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("MakeBucket: %v", err)
		}
	}
	if err := c.SetBucketVersioning(ctx, bucket, minio.BucketVersioningConfiguration{
		Status: "Enabled",
	}); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
}

func teardownBucket(t *testing.T, c *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	ch := c.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		WithVersions: true, Recursive: true, MaxKeys: 1000,
	})
	var toDelete []minio.ObjectInfo
	for obj := range ch {
		if obj.Err != nil {
			break
		}
		toDelete = append(toDelete, obj)
	}
	for _, obj := range toDelete {
		_ = c.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
	}
	_ = c.RemoveBucket(ctx, bucket)
}

func writeCfgAndInput(t *testing.T, dir, endpoint, ak, sk string, rows []string) (cfgPath, inputPath string) {
	t.Helper()
	inputPath = filepath.Join(dir, "input.txt")
	if err := os.WriteFile(inputPath, []byte(strings.Join(rows, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfgPath = filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + endpoint,
		"scheme: http",
		"ak: " + ak,
		"sk: " + sk,
		"op_concurrency: 4",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"progress_interval: 1000",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	return
}

func findMarkerVID(t *testing.T, c *minio.Client, bucket, key string) string {
	t.Helper()
	ctx := context.Background()
	ch := c.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix: key, WithVersions: true, Recursive: true,
	})
	for obj := range ch {
		if obj.Err != nil {
			break
		}
		if obj.IsDeleteMarker {
			return obj.VersionID
		}
	}
	return ""
}

func findLatestVID(t *testing.T, c *minio.Client, bucket, key string) string {
	t.Helper()
	ctx := context.Background()
	ch := c.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix: key, WithVersions: true, Recursive: true,
	})
	for obj := range ch {
		if obj.Err != nil {
			break
		}
		return obj.VersionID
	}
	return ""
}

func TestIntegration_ModeCheck_ThreeStatesOnRealMinIO(t *testing.T) {
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newMinIOAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	ctx := context.Background()
	if _, err := admin.PutObject(ctx, bucket, "marker.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put marker.txt v1: %v", err)
	}
	if err := admin.RemoveObject(ctx, bucket, "marker.txt", minio.RemoveObjectOptions{}); err != nil {
		t.Fatalf("Remove marker.txt (creates marker): %v", err)
	}
	if _, err := admin.PutObject(ctx, bucket, "regular.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put regular.txt: %v", err)
	}
	if _, err := admin.PutObject(ctx, bucket, "gone.txt", strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("Put gone.txt: %v", err)
	}
	// gone.txt: list all versions and delete each (object fully gone)
	{
		ch := admin.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: "gone.txt", WithVersions: true, Recursive: true,
		})
		for obj := range ch {
			if obj.Err != nil {
				break
			}
			_ = admin.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
	}

	markerVID := findMarkerVID(t, admin, bucket, "marker.txt")
	if markerVID == "" {
		t.Fatal("could not find marker versionId for marker.txt")
	}
	regularVID := findLatestVID(t, admin, bucket, "regular.txt")
	if regularVID == "" {
		t.Fatal("could not find versionId for regular.txt")
	}

	dir := t.TempDir()
	rows := []string{
		fmt.Sprintf("%s\x1fmarker.txt\x1f%s", bucket, markerVID),
		fmt.Sprintf("%s\x1fregular.txt\x1f%s", bucket, regularVID),
		// gone.txt has no versions; HEAD with a valid-format-but-nonexistent UUID
		// returns 404 NoSuchKey or NoSuchVersion (MinIO returns NoSuchKey when the
		// object has no versions at all). Use a valid UUID format — MinIO rejects
		// malformed version IDs with 400 InvalidArgument before checking existence.
		fmt.Sprintf("%s\x1fgone.txt\x1f00000000-0000-0000-0000-000000000000", bucket),
	}
	cfgPath, inputPath := writeCfgAndInput(t, dir, endpoint, ak, sk, rows)

	if err := run(cfgPath, "check", inputPath); err != nil {
		t.Fatalf("run check: %v", err)
	}

	b, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	out := string(b)
	if !strings.Contains(out, fmt.Sprintf("%s\x1fmarker.txt\x1f%s\x1fmarker", bucket, markerVID)) {
		t.Errorf("missing marker state. raw output:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("%s\x1fregular.txt\x1f%s\x1fnot_marker", bucket, regularVID)) {
		t.Errorf("missing not_marker state. raw output:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("%s\x1fgone.txt\x1f00000000-0000-0000-0000-000000000000\x1fabsent", bucket)) {
		t.Errorf("missing absent state. raw output:\n%s", out)
	}
}

func TestIntegration_ModeDelete_ThenReCheckAbsent(t *testing.T) {
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newMinIOAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	ctx := context.Background()
	keys := []string{"del1.txt", "del2.txt", "del3.txt"}
	for _, k := range keys {
		if _, err := admin.PutObject(ctx, bucket, k, strings.NewReader("v1"), 2, minio.PutObjectOptions{}); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		if err := admin.RemoveObject(ctx, bucket, k, minio.RemoveObjectOptions{}); err != nil {
			t.Fatalf("Remove %s: %v", k, err)
		}
	}
	var rows []string
	for _, k := range keys {
		vid := findMarkerVID(t, admin, bucket, k)
		if vid == "" {
			t.Fatalf("no marker for %s", k)
		}
		rows = append(rows, fmt.Sprintf("%s\x1f%s\x1f%s", bucket, k, vid))
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 markers, got %d", len(rows))
	}

	dir := t.TempDir()
	cfgPath, inputPath := writeCfgAndInput(t, dir, endpoint, ak, sk, rows)

	if err := run(cfgPath, "delete", inputPath); err != nil {
		t.Fatalf("run delete: %v", err)
	}
	d, _ := os.ReadFile(filepath.Join(dir, "out", "deleted.txt"))
	if len(strings.Split(strings.TrimRight(string(d), "\n"), "\n")) != 3 {
		t.Errorf("expected 3 deleted rows, got: %q", d)
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "delete_failed.txt")); !os.IsNotExist(err) {
		b, _ := os.ReadFile(filepath.Join(dir, "out", "delete_failed.txt"))
		t.Errorf("delete_failed.txt should not exist, content: %q", string(b))
	}

	if err := run(cfgPath, "check", inputPath); err != nil {
		t.Fatalf("re-run check: %v", err)
	}
	cr, _ := os.ReadFile(filepath.Join(dir, "out", "check_results.txt"))
	out := string(cr)
	for _, r := range rows {
		if !strings.Contains(out, r+"\x1fabsent") {
			t.Errorf("expected absent for %s after delete. raw:\n%s", r, out)
		}
	}
}
