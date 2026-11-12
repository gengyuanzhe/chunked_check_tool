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

// S3Client 包装单个 minio.Client。本工具只用 StatObject / DeleteObject / BucketExists。
type S3Client struct {
	client *minio.Client
}

// NewS3Client 构造绑定到单个 endpoint 的 minio.Client。
// transport 调优对齐 delete_marker_list_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP（默认 10s）、
// optional ResponseHeaderTimeout。https 时 InsecureSkipVerify。
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

// StatObject HEAD 一个对象版本。versionID 原样透传（含字面量 "null"）。
// S3 对 delete marker 版本的 HEAD 返回 405 + x-amz-delete-marker: true 头；
// minio-go v7.3.0 可能返回 (ObjectInfo{IsDeleteMarker:true}, nil) 或
// (ObjectInfo{}, MethodNotAllowed error) —— 由 classifier.go 统一分类。
func (c *S3Client) StatObject(ctx context.Context, bucket, key, versionID string) (minio.ObjectInfo, error) {
	return c.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{VersionID: versionID})
}

// DeleteObject 删除一个对象版本（对 delete marker 即消除该 marker）。
// versionID 原样透传（含字面量 "null"）。NoSuchVersion 由调用方判定为成功。
func (c *S3Client) DeleteObject(ctx context.Context, bucket, key, versionID string) error {
	return c.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{VersionID: versionID})
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return c.client.BucketExists(ctx, bucket)
}
