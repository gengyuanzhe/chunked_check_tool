//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	entries, err := readRetryEntries(retryPath)
	if err != nil {
		t.Fatalf("readRetryEntries: %v", err)
	}
	if err := run(cfgPath, bucket, entries); err != nil {
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

// TestIntegration_Mode2_PageLevelMarkerResume 验证页级 marker 续传：
// 用 ListObjectVersions 直接拿到第 1 页后的 NextKeyMarker/NextVersionIdMarker，
// 写入 retry 文件（prefix\x1fkeyMarker\x1fversionIdMarker），调 run 应从该 marker
// 起续扫——只扫第 2 页之后的内容，不重扫第 1 页。
//
// 数据布局：bucket 下 a/ 前缀放 3 个 delete marker 对象（每个含 put+delete = 2 版本），
// 共 6 个版本——超过 S3 默认每页 1000 keys 不会分页，所以本测试用 admin client
// 直接取 ListObjectVersions 第 1 页的 NextKeyMarker/NextVersionIdMarker 模拟
// "扫完第 1 页失败"的场景。
func TestIntegration_Mode2_PageLevelMarkerResume(t *testing.T) {
	endpoint, ak, sk := skipIfNoMinIO(t)
	admin := newAdminClient(t, endpoint, ak, sk)
	bucket := fmt.Sprintf("list-tool-marker-it-%d", time.Now().UnixNano())
	setupBucket(t, admin, bucket)
	defer teardownBucket(t, admin, bucket)

	// 制造 3 个 delete marker（每个 put+delete = 2 版本，共 6 个版本）。
	for _, k := range []string{"a/m1.txt", "a/m2.txt", "a/m3.txt"} {
		putObject(t, admin, bucket, k, "v1")
		removeObject(t, admin, bucket, k)
	}

	// 用 admin client 取第 1 页（MaxKeys=2 强制分页）的 NextKeyMarker/NextVersionIdMarker。
	// 模拟"扫完第 1 页（2 个版本）失败"——retry 时从该 marker 起，只扫剩下 4 个版本。
	firstPage, err := admin.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String("a/"), MaxKeys: aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("first page ListObjectVersions: %v", err)
	}
	if !aws.ToBool(firstPage.IsTruncated) {
		t.Fatalf("expected truncated first page, got IsTruncated=false")
	}
	km := aws.ToString(firstPage.NextKeyMarker)
	vidm := aws.ToString(firstPage.NextVersionIdMarker)
	if km == "" || vidm == "" {
		t.Fatalf("expected non-empty markers, got km=%q vidm=%q", km, vidm)
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + endpoint,
		"scheme: http",
		"ak: " + ak,
		"sk: " + sk,
		"list_type: 2",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"progress_interval: 1000",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}

	// retry 文件用 marker 续传格式：a/\x1f<km>\x1f<vidm>\n
	retryContent := "a/\x1f" + km + "\x1f" + vidm + "\n"
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte(retryContent), 0644); err != nil {
		t.Fatal(err)
	}
	entries, err := readRetryEntries(retryPath)
	if err != nil {
		t.Fatalf("readRetryEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Prefix != "a/" || entries[0].KeyMarker != km || entries[0].VersionIDMarker != vidm {
		t.Fatalf("parsed entry wrong: %+v", entries)
	}
	if err := run(cfgPath, bucket, entries); err != nil {
		t.Fatalf("run: %v", err)
	}

	rl, err := os.ReadFile(filepath.Join(dir, "out", "run.log"))
	if err != nil {
		t.Fatal(err)
	}
	// 第 1 页扫了 2 个版本（m1.txt 的 put+delete），retry 从 marker 起只扫剩下 4 个版本
	// （m2.txt 和 m3.txt 各 put+delete）。
	if !strings.Contains(string(rl), "scanned_versions=4") {
		t.Errorf("run.log missing scanned_versions=4 (marker resume): %q", rl)
	}
	if !strings.Contains(string(rl), "delete_markers=2") {
		t.Errorf("run.log missing delete_markers=2: %q", rl)
	}
}

// TestIntegration_Mode2_RetryMarkerPassedToRequest 验证 retry 文件里的 marker
// 被传到 ListObjectVersions 请求——用 fake server 检查 query 里的 key-marker /
// version-id-marker 参数。这是页级续传的核心契约：marker 必须在请求里出现。
// 用 fake server 而非 MinIO——MinIO 不容易模拟"分页中途失败"，fake server 可控。
func TestIntegration_Mode2_RetryMarkerPassedToRequest(t *testing.T) {
	var sawMarker bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		if !q.Has("versions") {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		km := q.Get("key-marker")
		vidm := q.Get("version-id-marker")
		// 验证 retry 文件里的 marker 被传到请求
		if km == "k1" && vidm == "v1" {
			sawMarker = true
		}
		xml := `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix>a/</Prefix><IsTruncated>false</IsTruncated>
<DeleteMarker><Key>a/m.txt</Key><VersionId>dm-a</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T15:04:05.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`
		w.Write([]byte(xml))
	}))
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgBody := strings.Join([]string{
		"endpoints:",
		"  - " + srv.Listener.Addr().String(),
		"scheme: http",
		"ak: a",
		"sk: s",
		"list_type: 2",
		"list_concurrency: 1",
		"output_dir: " + filepath.Join(dir, "out"),
		"output_dir_timestamp: false",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0644); err != nil {
		t.Fatal(err)
	}
	// retry 文件带 marker——验证 marker 被传到请求
	retryContent := "a/\x1fk1\x1fv1\n"
	retryPath := filepath.Join(dir, "retry.txt")
	if err := os.WriteFile(retryPath, []byte(retryContent), 0644); err != nil {
		t.Fatal(err)
	}
	entries, err := readRetryEntries(retryPath)
	if err != nil {
		t.Fatalf("readRetryEntries: %v", err)
	}
	if err := run(cfgPath, "testbucket", entries); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !sawMarker {
		t.Errorf("expected request with key-marker=k1 version-id-marker=v1, but server never saw it")
	}
}
