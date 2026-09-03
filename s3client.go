package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectInfo is the subset of S3 object metadata we care about.
type ObjectInfo struct {
	Key  string
	ETag string
}

// S3API is the S3 surface area the checker depends on.
// ListPage performs a single LIST request (V2) and returns object keys,
// common prefixes (when a delimiter is requested), and the next start-after
// cursor for pagination.
// RangeGet fetches the first 128 bytes of an object (for chunked-upload
// signature verification).
type S3API interface {
	ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error)
	RangeGet(ctx context.Context, key string) ([]byte, error)
}

// NewMinioClient constructs a minio.Client bound to a single endpoint.
// When secure is true the transport skips TLS verification (typical for
// internal nodes with self-signed certs); set secure=false for plain HTTP.
func NewMinioClient(endpoint, ak, sk string, secure bool) (*minio.Client, error) {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       secure,
		Transport:    tr,
		BucketLookup: minio.BucketLookupAuto,
	})
}

// S3Client wraps minio.Core for ListPage (exposes CommonPrefixes, which the
// higher-level minio.Client.ListObjects channel API does not) and minio.Client
// for RangeGet.
type S3Client struct {
	core   *minio.Core
	client *minio.Client
	bucket string
}

// NewS3Client builds an S3Client from an existing minio.Client and a bucket.
func NewS3Client(client *minio.Client, bucket string) *S3Client {
	return &S3Client{
		core:   &minio.Core{Client: client},
		client: client,
		bucket: bucket,
	}
}

// ListPage lists one page of objects under prefix.
//
// NOTE: minio.Core.ListObjectsV2 does not accept a context.Context; it uses
// context.Background() internally. The caller's ctx is therefore not honored
// for cancellation of the underlying HTTP request. This is a known limitation
// of the minio-go v7 Core API. RangeGet does accept ctx.
func (c *S3Client) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	delimiter := ""
	if delim {
		delimiter = "/"
	}
	result, err := c.core.ListObjectsV2(c.bucket, prefix, startAfter, "", delimiter, maxKeys)
	if err != nil {
		return nil, nil, "", err
	}
	objs := make([]ObjectInfo, 0, len(result.Contents))
	for _, o := range result.Contents {
		objs = append(objs, ObjectInfo{Key: o.Key, ETag: o.ETag})
	}
	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, cp := range result.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	// Use the last returned key as the next start-after cursor. When the
	// response is truncated and no NextContinuationToken is exposed by the
	// Core API for V2, paging by key is the documented S3 fallback.
	next := ""
	if len(objs) > 0 {
		next = objs[len(objs)-1].Key
	}
	return objs, prefixes, next, nil
}

// RangeGet returns the first 128 bytes of an object, used to inspect the
// chunked-upload streaming signature header.
func (c *S3Client) RangeGet(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var opts minio.GetObjectOptions
	if err := opts.SetRange(0, 127); err != nil {
		return nil, err
	}
	obj, err := c.client.GetObject(ctx, c.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	body, err := io.ReadAll(io.LimitReader(obj, 128))
	if err != nil {
		return nil, err
	}
	return body, nil
}

// FakeS3 is an in-memory S3API for testing.
type FakeS3 struct {
	Objects         []ObjectInfo
	CommonPrefixes  []string
	NextStartAfter  string
	Body            []byte
	Err             error
}

func (f *FakeS3) ListPage(ctx context.Context, prefix, startAfter string, delim bool, maxKeys int) ([]ObjectInfo, []string, string, error) {
	if f.Err != nil {
		return nil, nil, "", f.Err
	}
	return f.Objects, f.CommonPrefixes, f.NextStartAfter, nil
}

func (f *FakeS3) RangeGet(ctx context.Context, key string) ([]byte, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Body, nil
}

var _ S3API = (*FakeS3)(nil)
var _ S3API = (*S3Client)(nil)

var ErrNodeDown = errors.New("node down")
