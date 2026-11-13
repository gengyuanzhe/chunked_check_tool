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
	client *s3.Client
}

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

// ListVersions 返回版本化列举流。
// recursive=true 不带 delimiter（流里吐所有 key 的所有版本）；
// recursive=false 带 / delimiter（流里吐直接子 key 的所有版本 +
// common prefix 作为独立返回值）。
// 三类条目（Version/DeleteMarker/CommonPrefix）按 XML 标签区分，
// 不靠字段零值启发式——key 以 / 结尾的真实版本不会被误判成 common prefix。
func (c *S3Client) ListVersions(ctx context.Context, bucket, prefix string, recursive bool) (<-chan VersionEntry, <-chan string) {
	vc := make(chan VersionEntry)
	pc := make(chan string)
	go func() {
		defer close(vc)
		defer close(pc)
		var keyMarker, versionIDMarker *string
		delimiter := "/"
		if recursive {
			delimiter = ""
		}
		for {
			out, err := c.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucket),
				Prefix:          aws.String(prefix),
				Delimiter:       aws.String(delimiter),
				KeyMarker:       keyMarker,
				VersionIdMarker: versionIDMarker,
			})
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
			keyMarker = out.NextKeyMarker
			versionIDMarker = out.NextVersionIdMarker
		}
	}()
	return vc, pc
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return true, nil
}
