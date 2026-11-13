//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func newAdminClient(t *testing.T, endpoint, ak, sk string) *s3.Client {
	t.Helper()
	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})
}

func setupBucket(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		// bucket already exists is fine
	}
	_, err = c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	})
	if err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
}

func teardownBucket(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	paginator := s3.NewListObjectVersionsPaginator(c, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), MaxKeys: aws.Int32(1000),
	})
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			break
		}
		for i := range out.Versions {
			v := out.Versions[i]
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: v.Key, VersionId: v.VersionId,
			})
		}
		for i := range out.DeleteMarkers {
			v := out.DeleteMarkers[i]
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: v.Key, VersionId: v.VersionId,
			})
		}
	}
	_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
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

func findMarkerVID(t *testing.T, c *s3.Client, bucket, key string) string {
	t.Helper()
	ctx := context.Background()
	out, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	if err != nil {
		return ""
	}
	for i := range out.DeleteMarkers {
		if aws.ToString(out.DeleteMarkers[i].Key) == key {
			return aws.ToString(out.DeleteMarkers[i].VersionId)
		}
	}
	return ""
}

func findLatestVID(t *testing.T, c *s3.Client, bucket, key string) string {
	t.Helper()
	ctx := context.Background()
	out, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	if err != nil {
		return ""
	}
	for i := range out.Versions {
		if aws.ToString(out.Versions[i].Key) == key {
			return aws.ToString(out.Versions[i].VersionId)
		}
	}
	return ""
}

func putObject(t *testing.T, c *s3.Client, bucket, key, content string) {
	t.Helper()
	_, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader([]byte(content)),
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

func removeObject(t *testing.T, c *s3.Client, bucket, key string) {
	t.Helper()
	_, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject %s: %v", key, err)
	}
}

func TestIntegration_ModeCheck_ThreeStatesOnRealMinIO(t *testing.T) {
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	putObject(t, admin, bucket, "marker.txt", "v1")
	removeObject(t, admin, bucket, "marker.txt") // creates delete marker
	putObject(t, admin, bucket, "regular.txt", "v1")
	putObject(t, admin, bucket, "gone.txt", "v1")
	// gone.txt: delete all versions so object is fully gone
	{
		out, _ := admin.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("gone.txt"),
		})
		for i := range out.Versions {
			_, _ = admin.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: out.Versions[i].Key, VersionId: out.Versions[i].VersionId,
			})
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
	admin := newAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("cleanup-tool-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	keys := []string{"del1.txt", "del2.txt", "del3.txt"}
	for _, k := range keys {
		putObject(t, admin, bucket, k, "v1")
		removeObject(t, admin, bucket, k) // creates delete marker
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
