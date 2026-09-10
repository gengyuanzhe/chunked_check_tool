package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// minioPutAPI is the subset of *minio.Client the uploader uses. Extracted
// as an interface so tests can inject a fake to verify partSize透传 and
// multipart classification without a live endpoint.
type minioPutAPI interface {
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	BucketExists(ctx context.Context, bucketName string) (bool, error)
}

// minioCoreAPI is the subset of *minio.Core for manual multipart orchestration.
// Tests inject a fake to verify per-operation endpoint routing.
type minioCoreAPI interface {
	NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error)
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error
}

// Uploader is the high-level surface workers depend on. S3Uploader
// satisfies it; tests inject a recording fake.
type Uploader interface {
	UploadObject(ctx context.Context, endpointIdx int, bucket, key string, body io.Reader, size, partSize int64) (bool, error)
	UploadObjectMultipart(ctx context.Context, bucket, key string, body io.Reader, size, partSize int64, pattern []int) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
}

// S3Uploader lazily caches one minio client per endpoint index. Workers
// call UploadObject with an endpointIdx (round-robin assigned by caller);
// the uploader routes to the bound client and classifies multipart by
// comparing object size to partSize (minio-go splits iff size > partSize).
//
// When useTrailer is true, clients are built with TrailingHeaders=true and
// each PutObject sets Checksum=ChecksumSHA256 — triggers aws-chunked +
// x-amz-checksum-sha256 trailer (the body-corruption path chunked_check_tool
// detects). Requires v4 signatures (always used here).
type S3Uploader struct {
	pool          *NodePool
	ak, sk        string
	secure        bool
	useTrailer    bool
	clientFactory func(endpoint string) (minioPutAPI, error)
	clients       map[int]minioPutAPI
	coreFactory   func(endpoint string) (minioCoreAPI, error)
	cores         map[int]minioCoreAPI
	mu            sync.Mutex
}

func NewS3Uploader(pool *NodePool, ak, sk string, secure, useTrailer bool) *S3Uploader {
	u := &S3Uploader{
		pool:       pool,
		ak:         ak,
		sk:         sk,
		secure:     secure,
		useTrailer: useTrailer,
		clients:    make(map[int]minioPutAPI),
		cores:      make(map[int]minioCoreAPI),
	}
	u.clientFactory = func(endpoint string) (minioPutAPI, error) {
		return NewMinioClient(endpoint, ak, sk, secure, useTrailer)
	}
	u.coreFactory = func(endpoint string) (minioCoreAPI, error) {
		return NewMinioCore(endpoint, ak, sk, secure, useTrailer)
	}
	return u
}

// newS3UploaderWithFactory builds an uploader with an injectable client
// factory (for tests). The pool is a fake single-endpoint pool.
func newS3UploaderWithFactory(factory func(string) (minioPutAPI, error), useTrailer bool) *S3Uploader {
	pool := &NodePool{endpoints: []string{"fake:80"}, scheme: "http"}
	return &S3Uploader{
		pool:          pool,
		useTrailer:    useTrailer,
		clientFactory: factory,
		clients:       make(map[int]minioPutAPI),
		cores:         make(map[int]minioCoreAPI),
	}
}

func (u *S3Uploader) getClient(idx int) (minioPutAPI, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if c, ok := u.clients[idx]; ok {
		return c, nil
	}
	endpoint := u.pool.Endpoint(idx)
	c, err := u.clientFactory(endpoint)
	if err != nil {
		return nil, err
	}
	u.clients[idx] = c
	return c, nil
}

func (u *S3Uploader) getCore(idx int) (minioCoreAPI, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if c, ok := u.cores[idx]; ok {
		return c, nil
	}
	if u.coreFactory == nil {
		return nil, fmt.Errorf("core factory not configured")
	}
	endpoint := u.pool.Endpoint(idx)
	c, err := u.coreFactory(endpoint)
	if err != nil {
		return nil, err
	}
	u.cores[idx] = c
	return c, nil
}

// UploadObject uploads content to (bucket, key) via the client bound to
// endpointIdx. partSize is the multipart part-size hint passed to minio-go.
// Returns multipart=true iff size > partSize.
// When useTrailer is set, opts.Checksum=ChecksumSHA256 is also set, which
// (combined with the TrailingHeaders=true client) makes minio-go emit the
// aws-chunked + x-amz-checksum-sha256 trailer for multipart uploads.
func (u *S3Uploader) UploadObject(ctx context.Context, endpointIdx int, bucket, key string, body io.Reader, size, partSize int64) (bool, error) {
	c, err := u.getClient(endpointIdx)
	if err != nil {
		return false, err
	}
	opts := minio.PutObjectOptions{PartSize: uint64(partSize)}
	if u.useTrailer {
		opts.Checksum = minio.ChecksumSHA256
	}
	if _, err := c.PutObject(ctx, bucket, key, body, size, opts); err != nil {
		return false, err
	}
	return size > partSize, nil
}

func (u *S3Uploader) BucketExists(ctx context.Context, bucket string) (bool, error) {
	c, err := u.getClient(0)
	if err != nil {
		return false, err
	}
	return c.BucketExists(ctx, bucket)
}

// UploadObjectMultipart manually orchestrates a multipart upload across
// per-operation endpoints defined by pattern. pattern layout:
//
//	pattern[0]              → NewMultipartUpload endpoint (init)
//	pattern[1..N]          → PutObjectPart endpoints (one per part)
//	pattern[N+1]           → CompleteMultipartUpload endpoint
//
// where N = ceil(size/partSize). Caller MUST ensure size > partSize
// (otherwise single PUT applies and pattern is irrelevant). Requires the S3
// cluster to share multipart upload state across endpoints (UploadID issued
// by init on one node must be valid on every other node).
//
// body is read sequentially part-by-part; exactly `size` bytes must be
// available. Each part reads partLen bytes via io.LimitReader.
//
// On any per-step failure, AbortMultipartUpload is attempted on pattern[0]'s
// endpoint (best-effort; abort errors are ignored) before returning the error.
//
// use_trailer linkage mirrors UploadObject: when useTrailer=true, the init and
// complete PutObjectOptions carry Checksum=ChecksumSHA256, which (combined
// with TrailingHeaders=true clients) makes minio-go emit aws-chunked +
// x-amz-checksum-sha256 trailer for each part.
// UploadObjectMultipart on S3Uploader is the real implementation; see below.
func (u *S3Uploader) UploadObjectMultipart(ctx context.Context, bucket, key string, body io.Reader, size, partSize int64, pattern []int) error {
	if size <= partSize {
		return fmt.Errorf("UploadObjectMultipart called with size=%d <= partSize=%d (caller must use single PUT)", size, partSize)
	}
	nParts := (size + partSize - 1) / partSize
	if want := int(nParts) + 2; len(pattern) != want {
		return fmt.Errorf("pattern length %d does not match expected %d (init + %d parts + complete) for size=%d partSize=%d", len(pattern), want, nParts, size, partSize)
	}

	opts := minio.PutObjectOptions{PartSize: uint64(partSize)}
	if u.useTrailer {
		opts.Checksum = minio.ChecksumSHA256
	}

	initCore, err := u.getCore(pattern[0])
	if err != nil {
		return fmt.Errorf("init core: %w", err)
	}
	uploadID, err := initCore.NewMultipartUpload(ctx, bucket, key, opts)
	if err != nil {
		return fmt.Errorf("init multipart: %w", err)
	}

	parts := make([]minio.CompletePart, 0, nParts)
	remaining := size
	for i := 0; i < int(nParts); i++ {
		partLen := partSize
		if partLen > remaining {
			partLen = remaining
		}
		remaining -= partLen
		partCore, err := u.getCore(pattern[1+i])
		if err != nil {
			u.abortMultipart(ctx, pattern[0], bucket, key, uploadID)
			return fmt.Errorf("part %d core: %w", i+1, err)
		}
		op, err := partCore.PutObjectPart(ctx, bucket, key, uploadID, i+1, io.LimitReader(body, partLen), partLen, minio.PutObjectPartOptions{})
		if err != nil {
			u.abortMultipart(ctx, pattern[0], bucket, key, uploadID)
			return fmt.Errorf("part %d: %w", i+1, err)
		}
		parts = append(parts, minio.CompletePart{PartNumber: op.PartNumber, ETag: op.ETag})
	}

	completeCore, err := u.getCore(pattern[int(nParts)+1])
	if err != nil {
		u.abortMultipart(ctx, pattern[0], bucket, key, uploadID)
		return fmt.Errorf("complete core: %w", err)
	}
	if _, err := completeCore.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts, opts); err != nil {
		u.abortMultipart(ctx, pattern[0], bucket, key, uploadID)
		return fmt.Errorf("complete multipart: %w", err)
	}
	return nil
}

func (u *S3Uploader) abortMultipart(ctx context.Context, endpointIdx int, bucket, key, uploadID string) {
	c, err := u.getCore(endpointIdx)
	if err != nil {
		return
	}
	_ = c.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

// NewMinioCore builds a *minio.Core bound to a single endpoint. Core embeds
// *Client, so it serves both the multipart primitive API and (transitively)
// the high-level PutObject API. TrailingHeaders mirrors NewMinioClient so the
// use_trailer three-way linkage stays consistent across both call paths.
func NewMinioCore(endpoint, ak, sk string, secure, trailingHeaders bool) (*minio.Core, error) {
	client, err := NewMinioClient(endpoint, ak, sk, secure, trailingHeaders)
	if err != nil {
		return nil, err
	}
	return &minio.Core{Client: client}, nil
}

// NewMinioClient constructs a minio.Client bound to a single endpoint.
// secure=true skips TLS verification (typical for internal nodes with
// self-signed certs), matching chunked_check_tool's transport setup.
// trailingHeaders=true enables aws-chunked trailer support (required for
// opts.Checksum on PutObject; only effective with v4 signatures).
func NewMinioClient(endpoint, ak, sk string, secure, trailingHeaders bool) (*minio.Client, error) {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	if secure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return minio.New(endpoint, &minio.Options{
		Creds:           credentials.NewStaticV4(ak, sk, ""),
		Secure:          secure,
		Transport:       tr,
		BucketLookup:    minio.BucketLookupAuto,
		TrailingHeaders: trailingHeaders,
	})
}

var _ minioPutAPI = (*minio.Client)(nil)
var _ minioCoreAPI = (*minio.Core)(nil)
var _ Uploader = (*S3Uploader)(nil)
