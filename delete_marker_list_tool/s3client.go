package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Client 包装单个 minio.Client。本工具只用 channel API
// Client.ListObjects(WithVersions: true)——Core 不支持版本化列举
// (minio-go v7.3.0)，必须用 Client。
type S3Client struct {
	client *minio.Client
}

// NewS3Client 构造绑定到单个 endpoint 的 minio.Client。
// transport 调优对齐 chunked_check_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP（默认 10s）、
// optional ResponseHeaderTimeout。https 时 InsecureSkipVerify。
// 无 mpOffsetTransport——那是 multipart-only，本工具不用。
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
	c, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AK, cfg.SK, ""),
		Secure:       secure,
		Transport:    tr,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("minio.New %s: %w", endpoint, err)
	}
	return &S3Client{client: c}, nil
}

// ListVersions 返回 minio channel API 的版本化列举流。
// recursive=true 不带 delimiter（流里吐所有 key 的所有版本）；
// recursive=false 带 / delimiter（流里吐直接子 key 的所有版本 +
// common prefix 作为 ObjectInfo{Key: ".../", 其余字段空}）。
// channel API 在 obj.Err != nil 时吐一个错误条目然后关闭流。
func (c *S3Client) ListVersions(ctx context.Context, bucket, prefix string, recursive bool) <-chan minio.ObjectInfo {
	return c.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    recursive,
		WithVersions: true,
	})
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return c.client.BucketExists(ctx, bucket)
}
