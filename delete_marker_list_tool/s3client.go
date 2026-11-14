package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client 包装单个 s3.Client。本工具用 ListObjectVersions（版本化列举）
// + HeadBucket（探测桶存在）。AWS SDK 把 <Version>/<DeleteMarker>/
// <CommonPrefixes> 解析到独立 slice 字段，靠 XML 标签区分，不靠启发式。
type S3Client struct {
	client  *s3.Client
	latency *LatencyTracker // 可选——nil 不记录。main 启动时注入 &stats.Latency。
}

// SetLatencyTracker 注入窗口式时延统计器。main 在 NewS3Client 后调一次。
// nil 表示不记录（test 默认 nil，不影响功能）。
func (c *S3Client) SetLatencyTracker(l *LatencyTracker) { c.latency = l }

// NewS3Client 构造绑定到单个 endpoint 的 s3.Client。
// transport 调优对齐 chunked_check_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP（默认 10s）、
// optional ResponseHeaderTimeout。https 时 InsecureSkipVerify。
// endpoint 必须是 "host:port"（无 scheme）——scheme 由 cfg.Scheme（http/https）决定，
// 单一来源，不允许 endpoint 自带 scheme 覆盖。URL 由 NewS3Client 内部拼。
// 构造期校验 endpoint 格式——aws sdk 的 LoadDefaultConfig 不校验 BaseEndpoint，
// 非法格式会延迟到第一次请求才报错，掩盖启动失败。
func NewS3Client(cfg *Config, endpoint string) (*S3Client, error) {
	secure := cfg.Scheme == "https"
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if cfg.DialTimeout > 0 {
		tr.DialContext = (&net.Dialer{Timeout: time.Duration(cfg.DialTimeout) * time.Second}).DialContext
	}
	if cfg.ResponseHeaderTimeout > 0 {
		tr.ResponseHeaderTimeout = time.Duration(cfg.ResponseHeaderTimeout) * time.Second
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// endpoint 必须是 host:port，不允许带 scheme（scheme 由 cfg.Scheme 决定）。
	if strings.Contains(endpoint, "://") {
		return nil, fmt.Errorf("invalid endpoint %q: must be host:port without scheme (scheme comes from cfg.scheme)", endpoint)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("invalid endpoint %q: want host:port", endpoint)
	}
	raw := cfg.Scheme + "://" + endpoint

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK, "")),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config load %s: %w", endpoint, err)
	}

	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(raw)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: tr}
	})
	return &S3Client{client: client}, nil
}

// VersionEntry 把 AWS SDK 的 <Version> 和 <DeleteMarker> 摊平成一条目——
// 调用方按 IsDeleteMarker 区分。CommonPrefix 单独走 VersionPage.CommonPrefixes。
type VersionEntry struct {
	Key            string
	VersionID      string
	IsDeleteMarker bool
	LastModified   time.Time
}

// VersionPage 是单页列举结果。keyMarker/versionIDMarker 是调用
// ListVersionsPage 时传入的本次请求用的 marker；err 非 nil 时这两个值就是
// 续传点（直接写 list_failed）。NextKeyMarker/NextVersionIDMarker 是下一页
// 用的 marker——IsTruncated=false 时这两个值无意义。
type VersionPage struct {
	Versions            []VersionEntry
	CommonPrefixes      []string
	NextKeyMarker       string
	NextVersionIDMarker string
	IsTruncated         bool
}

// ListVersionsPage 列举一页。recursive=true 不带 delimiter（所有 key 所有版本）；
// recursive=false 带 / delimiter（直接子 key 的所有版本 + common prefix）。
// keyMarker/versionIDMarker 非空 = 从该 marker 起续扫（retry 场景）。
// 三类条目（Version/DeleteMarker/CommonPrefix）按 XML 标签区分，不靠字段零值
// 启发式——key 以 / 结尾的真实版本不会被误判成 common prefix。
func (c *S3Client) ListVersionsPage(ctx context.Context, bucket, prefix string, recursive bool, keyMarker, versionIDMarker string) (*VersionPage, error) {
	delimiter := "/"
	if recursive {
		delimiter = ""
	}
	var reqKeyMarker, reqVersionIDMarker *string
	if keyMarker != "" {
		reqKeyMarker = aws.String(keyMarker)
	}
	if versionIDMarker != "" {
		reqVersionIDMarker = aws.String(versionIDMarker)
	}
	// [diag] 记录每次请求的入参 marker（=进 URL 的 key-marker/version-id-marker）。
	// 追 version-id-marker=0 来源：看 reqVIDM 是否非预期地是 "0"。
	log.Printf("[list-page] req bucket=%s prefix=%q recursive=%v km=%q vidm=%q", bucket, prefix, recursive, keyMarker, versionIDMarker)
	start := time.Now()
	out, err := c.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucket),
		Prefix:          aws.String(prefix),
		Delimiter:       aws.String(delimiter),
		KeyMarker:       reqKeyMarker,
		VersionIdMarker: reqVersionIDMarker,
	})
	if c.latency != nil {
		c.latency.Record(time.Since(start))
	}
	if err != nil {
		// [diag] 请求失败也打返回值（err 里无 marker）——入参已在上面记录。
		log.Printf("[list-page] err bucket=%s prefix=%q km=%q vidm=%q err=%v", bucket, prefix, keyMarker, versionIDMarker, err)
		return nil, err
	}
	// [diag] 记录服务端返回的 next-marker——这是下一页请求用的 marker，
	// 也是失败时（下一页）写进 list_failed 的续传点。
	log.Printf("[list-page] resp bucket=%s prefix=%q nextKM=%q nextVIDM=%q truncated=%v nVer=%d nDM=%d nCP=%d",
		bucket, prefix,
		aws.ToString(out.NextKeyMarker), aws.ToString(out.NextVersionIdMarker),
		aws.ToBool(out.IsTruncated), len(out.Versions), len(out.DeleteMarkers), len(out.CommonPrefixes))
	page := &VersionPage{
		NextKeyMarker:       aws.ToString(out.NextKeyMarker),
		NextVersionIDMarker: aws.ToString(out.NextVersionIdMarker),
		IsTruncated:         aws.ToBool(out.IsTruncated),
	}
	page.Versions = make([]VersionEntry, 0, len(out.Versions)+len(out.DeleteMarkers))
	for i := range out.Versions {
		v := out.Versions[i]
		page.Versions = append(page.Versions, VersionEntry{
			Key:            aws.ToString(v.Key),
			VersionID:      aws.ToString(v.VersionId),
			IsDeleteMarker: false,
			LastModified:   aws.ToTime(v.LastModified),
		})
	}
	for i := range out.DeleteMarkers {
		v := out.DeleteMarkers[i]
		page.Versions = append(page.Versions, VersionEntry{
			Key:            aws.ToString(v.Key),
			VersionID:      aws.ToString(v.VersionId),
			IsDeleteMarker: true,
			LastModified:   aws.ToTime(v.LastModified),
		})
	}
	page.CommonPrefixes = make([]string, 0, len(out.CommonPrefixes))
	for i := range out.CommonPrefixes {
		page.CommonPrefixes = append(page.CommonPrefixes, aws.ToString(out.CommonPrefixes[i].Prefix))
	}
	return page, nil
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return true, nil
}
