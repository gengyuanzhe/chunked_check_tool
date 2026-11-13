package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
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
	client *s3.Client
}

// NewS3Client 构造绑定到单个 endpoint 的 s3.Client。
// transport 调优对齐 delete_marker_list_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP、optional ResponseHeaderTimeout。
// https 时 InsecureSkipVerify。
// 构造期校验 endpoint 是合法 http/https URL——aws sdk 的 LoadDefaultConfig 不校验
// BaseEndpoint 格式，非法 scheme 会延迟到第一次请求才报错，掩盖 worker 启动失败。
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

	// 构造期校验 endpoint——aws sdk 接受 BaseEndpoint 时不校验格式。
	u, err := neturl.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint %q: want http(s)://host", endpoint)
	}

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK, "")),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config load %s: %w", endpoint, err)
	}

	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: tr}
	})
	return &S3Client{client: client}, nil
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
func (c *S3Client) StatObject(ctx context.Context, bucket, key, versionID string) HeadResult {
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err == nil {
		return HeadResult{
			Key:            key,
			VersionID:      versionID,
			IsDeleteMarker: false,
			StatusCode:     200,
		}
	}
	return headResultFromErr(bucket, key, versionID, err)
}

// DeleteObject 删除一个对象版本（对 delete marker 即消除该 marker）。
// versionID 原样透传（含字面量 "null"）。NoSuchVersion 由调用方判定为成功。
func (c *S3Client) DeleteObject(ctx context.Context, bucket, key, versionID string) (int, string, error) {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
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
