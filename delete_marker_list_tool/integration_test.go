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
	_, _ = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	_, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
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

// TestIntegration_Mode2_RetryFromFailedList 验证 retry 模式：retry 文件含多个 prefix，
// run 接受 []string 入队，每个 prefix 下的 delete marker 都写入 delete_markers.txt。
// 真实 MinIO + versioning 桶，验证 retry 文件路径 → prefixes → 扫描链路。
func TestIntegration_Mode2_RetryFromFailedList(t *testing.T) {
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("list-tool-retry-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	// 在 a/ 和 b/ 下各制造一个 delete marker（put 后 delete）。
	putObject(t, admin, bucket, "a/m.txt", "v1")
	removeObject(t, admin, bucket, "a/m.txt")
	putObject(t, admin, bucket, "b/m.txt", "v1")
	removeObject(t, admin, bucket, "b/m.txt")

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + endpoint,
		"scheme: http",
		"ak: " + ak,
		"sk: " + sk,
		"list_type: 2",
		"list_concurrency: 2",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"progress_interval: 1000",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}

	// retry 文件含 a/ 和 b/——模拟 list_failed.txt 续传场景。
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte("a/\nb/\n"), 0644); err != nil {
		t.Fatal(err)
	}

	prefixes, err := readRetryPrefixes(retryPath)
	if err != nil {
		t.Fatalf("readRetryPrefixes: %v", err)
	}
	if err := run(cfgPath, bucket, prefixes); err != nil {
		t.Fatalf("run: %v", err)
	}

	dm, err := os.ReadFile(filepath.Join(dir, "out", "delete_markers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dm), bucket+"\x1fa/m.txt") {
		t.Errorf("missing a/m.txt marker: %q", dm)
	}
	if !strings.Contains(string(dm), bucket+"\x1fb/m.txt") {
		t.Errorf("missing b/m.txt marker: %q", dm)
	}

	rl, err := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if err != nil {
		t.Fatal(err)
	}
	// 每个 prefix 下有 2 个版本（1 put + 1 delete marker），共 4 个 scanned_versions
	// + 2 个 delete_markers。scanned_versions 包含所有版本（含 put），不是只算 marker。
	if !strings.Contains(string(rl), "scanned_versions=4") {
		t.Errorf("run.log missing scanned_versions=4: %q", rl)
	}
	if !strings.Contains(string(rl), "delete_markers=2") {
		t.Errorf("run.log missing delete_markers=2: %q", rl)
	}
}
