package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
)

// S3Client 包装单个 s3.Client。本工具只用 HeadObject / DeleteObject / HeadBucket。
type S3Client struct {
	client   *s3.Client
	latency  *LatencyTracker // 可选——nil 不记录。worker 启动时注入 &stats.Latency。
	logger   *slog.Logger    // 可选——nil 不打诊断。main 在 cfg.DebugLog=true 时注入 Debug-level logger。
	endpoint string          // 绑定的 endpoint（host:port），日志区分多 client。
}

// SetLatencyTracker 注入窗口式时延统计器。worker 在 NewS3Client 后调一次。
// nil 表示不记录（test 默认 nil，不影响功能）。
func (c *S3Client) SetLatencyTracker(l *LatencyTracker) { c.latency = l }

// SetLogger 注入诊断 logger。main 在 cfg.DebugLog=true 时调一次，
// 传入 Debug-level slog logger（输出到 run.log + 终端）。nil 表示不打诊断。
func (c *S3Client) SetLogger(l *slog.Logger) { c.logger = l }

// NewS3Client 构造绑定到单个 endpoint 的 s3.Client。
// transport 调优对齐 delete_marker_list_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP、optional ResponseHeaderTimeout。
// https 时 InsecureSkipVerify。
// endpoint 必须是 "host:port"（无 scheme）——scheme 由 cfg.Scheme（http/https）决定，
// 单一来源，不允许 endpoint 自带 scheme 覆盖。URL 由 NewS3Client 内部拼。
// 构造期校验 endpoint 格式——aws sdk 的 LoadDefaultConfig 不校验 BaseEndpoint，
// 非法格式会延迟到第一次请求才报错，掩盖 worker 启动失败。
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
	return &S3Client{client: client, endpoint: endpoint}, nil
}

// HeadResult 是 StatObject 的摊平返回——aws SDK 把成功响应和错误响应都封装，
// 这里摊平成调用方好用的形态。Err != nil 表示请求失败（含 404/405）。
type HeadResult struct {
	Key            string
	VersionID      string
	IsDeleteMarker bool // 从 response header x-amz-delete-marker 或错误路径推断
	StatusCode     int
	RequestID      string
	Err            error
}

// StatObject HEAD 一个对象版本。versionID 原样透传（含字面量 "null"）。
// aws SDK 对 405 返回 (nil, *smithy.OperationError)——错误路径上 classifier
// 从 ResponseError.Response.StatusCode + header 判定 marker。
// workerIdx 是调用方 worker 的编号（worker goroutine 的 idx），仅用于诊断日志
// 区分并发 worker——S3Client 自身不持有它（1:1 时与 client idx 相同，N>clients
// 时多个 worker 共享一个 client）。
func (c *S3Client) StatObject(ctx context.Context, bucket, key, versionID string, workerIdx int) HeadResult {
	if c.logger != nil {
		c.logger.Debug("head req",
			"worker", workerIdx, "endpoint", c.endpoint, "bucket", bucket,
			"key", key, "versionID", versionID)
	}
	start := time.Now()
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	elapsed := time.Since(start)
	if c.latency != nil {
		c.latency.Record(elapsed)
	}
	if c.logger != nil {
		status := 200
		reqID := ""
		if err != nil {
			var re *awshttp.ResponseError
			if errors.As(err, &re) {
				status = re.Response.StatusCode
				reqID = re.Response.Header.Get("x-amz-request-id")
			}
		}
		c.logger.Debug("head resp",
			"worker", workerIdx, "endpoint", c.endpoint, "bucket", bucket,
			"key", key, "versionID", versionID,
			"status", status, "req_id", reqID, "elapsed_ms", elapsed.Milliseconds(), "err", err)
	}
	if err == nil {
		return HeadResult{
			Key:            key,
			VersionID:       versionID,
			IsDeleteMarker: false,
			StatusCode:     200,
		}
	}
	return headResultFromErr(bucket, key, versionID, err)
}

// DeleteObject 删除一个对象版本（对 delete marker 即消除该 marker）。
// versionID 原样透传（含字面量 "null"）。NoSuchVersion 由调用方判定为成功。
// workerIdx 仅用于诊断日志，详见 StatObject 注释。
func (c *S3Client) DeleteObject(ctx context.Context, bucket, key, versionID string, workerIdx int) (int, string, error) {
	if c.logger != nil {
		c.logger.Debug("delete req",
			"worker", workerIdx, "endpoint", c.endpoint, "bucket", bucket,
			"key", key, "versionID", versionID)
	}
	start := time.Now()
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	elapsed := time.Since(start)
	if c.latency != nil {
		c.latency.Record(elapsed)
	}
	if c.logger != nil {
		status := 204
		reqID := ""
		if err != nil {
			var re *awshttp.ResponseError
			if errors.As(err, &re) {
				status = re.Response.StatusCode
				reqID = re.Response.Header.Get("x-amz-request-id")
			}
		}
		c.logger.Debug("delete resp",
			"worker", workerIdx, "endpoint", c.endpoint, "bucket", bucket,
			"key", key, "versionID", versionID,
			"status", status, "req_id", reqID, "elapsed_ms", elapsed.Milliseconds(), "err", err)
	}
	if err == nil {
		return 204, "", nil
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.Response.StatusCode, re.Response.Header.Get("x-amz-request-id"), err
	}
	return 0, "", err
}

// headResultFromErr 从 aws sdk 错误提取 StatusCode/RequestID/ErrorCode，
// 构造 HeadResult。405 + x-amz-delete-marker: true → IsDeleteMarker=true。
func headResultFromErr(bucket, key, versionID string, err error) HeadResult {
	r := HeadResult{
		Key:       key,
		VersionID: versionID,
		Err:       err,
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		r.StatusCode = re.Response.StatusCode
		r.RequestID = re.Response.Header.Get("x-amz-request-id")
		if re.Response.StatusCode == 405 && re.Response.Header.Get("x-amz-delete-marker") == "true" {
			r.IsDeleteMarker = true
			r.Err = nil // 405 + marker 头是预期路径，不再算错误
		}
	}
	return r
}

// errorCode 从 smithy 错误提取 ErrorCode（如 "NoSuchVersion"）。
func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return true, nil
}
