package main

import (
	"bytes"
	"context"
	"crypto/tls"
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

// Uploader is the high-level surface workers depend on. S3Uploader
// satisfies it; tests inject a recording fake.
type Uploader interface {
	UploadObject(ctx context.Context, endpointIdx int, bucket, key string, content []byte, partSize int64) (bool, error)
	BucketExists(ctx context.Context, bucket string) (bool, error)
}

// S3Uploader lazily caches one minio client per endpoint index. Workers
// call UploadObject with an endpointIdx (round-robin assigned by caller);
// the uploader routes to the bound client and classifies multipart by
// comparing object size to partSize (minio-go splits iff size > partSize).
type S3Uploader struct {
	pool           *NodePool
	ak, sk         string
	secure         bool
	clientFactory  func(endpoint string) (minioPutAPI, error)
	clients        map[int]minioPutAPI
	mu             sync.Mutex
}

func NewS3Uploader(pool *NodePool, ak, sk string, secure bool) *S3Uploader {
	u := &S3Uploader{
		pool:           pool,
		ak:             ak,
		sk:             sk,
		secure:         secure,
		clients:        make(map[int]minioPutAPI),
	}
	u.clientFactory = func(endpoint string) (minioPutAPI, error) {
		return NewMinioClient(endpoint, ak, sk, secure)
	}
	return u
}

// newS3UploaderWithFactory builds an uploader with an injectable client
// factory (for tests). The pool is a fake single-endpoint pool.
func newS3UploaderWithFactory(factory func(string) (minioPutAPI, error)) *S3Uploader {
	pool := &NodePool{endpoints: []string{"fake:80"}, scheme: "http"}
	return &S3Uploader{
		pool:          pool,
		clientFactory: factory,
		clients:       make(map[int]minioPutAPI),
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

// UploadObject uploads content to (bucket, key) via the client bound to
// endpointIdx. partSize is the multipart part-size hint passed to minio-go.
// Returns multipart=true iff size > partSize.
func (u *S3Uploader) UploadObject(ctx context.Context, endpointIdx int, bucket, key string, content []byte, partSize int64) (bool, error) {
	c, err := u.getClient(endpointIdx)
	if err != nil {
		return false, err
	}
	size := int64(len(content))
	opts := minio.PutObjectOptions{PartSize: uint64(partSize)}
	if _, err := c.PutObject(ctx, bucket, key, bytes.NewReader(content), size, opts); err != nil {
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

// NewMinioClient constructs a minio.Client bound to a single endpoint.
// secure=true skips TLS verification (typical for internal nodes with
// self-signed certs), matching chunked_check_tool's transport setup.
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

var _ minioPutAPI = (*minio.Client)(nil)
var _ Uploader = (*S3Uploader)(nil)
