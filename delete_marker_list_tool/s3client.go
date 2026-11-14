package main

import (
	"context"
	"crypto/tls"
	"fmt"
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

// VersionEntry 是 ListVersions 返回流里的统一条目。
// AWS SDK 把 <Version> 和 <DeleteMarker> 解析到不同类型，这里摊平成
// 一个结构——调用方按 IsDeleteMarker 区分。CommonPrefix 不走这个类型，
// ListVersions 单独返回。
type VersionEntry struct {
	Key            string
	VersionID      string
	IsDeleteMarker bool
	LastModified   time.Time
	Err            error // 非空表示列举失败，调用方应终止该前缀
}

// PageBoundary 是每成功一页后发的页边界快照——NextKeyMarker /
// NextVersionIDMarker 是续扫下一页的起始 marker。调用方收到后可作 checkpoint；
// 因 vc 是无缓冲 channel，调用方收到 PageBoundary 时本页所有 entry 已被消费，
// 即"上一页完成"的严格同步点。
type PageBoundary struct {
	NextKeyMarker       string
	NextVersionIDMarker string
}

// ListVersions 返回版本化列举流。
// recursive=true 不带 delimiter（流里吐所有 key 的所有版本）；
// recursive=false 带 / delimiter（流里吐直接子 key 的所有版本 +
// common prefix 作为独立返回值）。
// keyMarker/versionIDMarker 非空 = 从该 marker 起续扫（retry 场景）。
// 三类条目（Version/DeleteMarker/CommonPrefix）按 XML 标签区分，
// 不靠字段零值启发式——key 以 / 结尾的真实版本不会被误判成 common prefix。
// 三路 channel：vc（Version+DeleteMarker）、pc（CommonPrefix）、
// bc（PageBoundary，每成功一页后发）。
func (c *S3Client) ListVersions(ctx context.Context, bucket, prefix string, recursive bool, keyMarker, versionIDMarker string) (<-chan VersionEntry, <-chan string, <-chan PageBoundary) {
	vc := make(chan VersionEntry)
	pc := make(chan string)
	bc := make(chan PageBoundary)
	go func() {
		defer close(vc)
		defer close(pc)
		defer close(bc)
		var km, vidm *string
		if keyMarker != "" {
			km = aws.String(keyMarker)
		}
		if versionIDMarker != "" {
			vidm = aws.String(versionIDMarker)
		}
		delimiter := "/"
		if recursive {
			delimiter = ""
		}
		for {
			start := time.Now()
			out, err := c.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucket),
				Prefix:          aws.String(prefix),
				Delimiter:       aws.String(delimiter),
				KeyMarker:       km,
				VersionIdMarker: vidm,
			})
			if c.latency != nil {
				c.latency.Record(time.Since(start))
			}
			if err != nil {
				vc <- VersionEntry{Err: err}
				return
			}
			for i := range out.Versions {
				v := out.Versions[i]
				vc <- VersionEntry{
					Key:            aws.ToString(v.Key),
					VersionID:       aws.ToString(v.VersionId),
					IsDeleteMarker: false,
					LastModified:   aws.ToTime(v.LastModified),
				}
			}
			for i := range out.DeleteMarkers {
				v := out.DeleteMarkers[i]
				vc <- VersionEntry{
					Key:            aws.ToString(v.Key),
					VersionID:      aws.ToString(v.VersionId),
					IsDeleteMarker: true,
					LastModified:   aws.ToTime(v.LastModified),
				}
			}
			for i := range out.CommonPrefixes {
				pc <- aws.ToString(out.CommonPrefixes[i].Prefix)
			}
			if !aws.ToBool(out.IsTruncated) {
				return
			}
			km = out.NextKeyMarker
			vidm = out.NextVersionIdMarker
			bc <- PageBoundary{
				NextKeyMarker:       aws.ToString(km),
				NextVersionIDMarker: aws.ToString(vidm),
			}
		}
	}()
	return vc, pc, bc
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return true, nil
}
