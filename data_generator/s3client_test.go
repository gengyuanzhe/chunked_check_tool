package main

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
)

type fakeMinioClient struct {
	putCalls      []fakePutCall
	bucketExists_ bool
	bucketExistsErr error
	putErr         error
}

type fakePutCall struct {
	bucket   string
	object   string
	size     int64
	partSize uint64
}

func (f *fakeMinioClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	f.putCalls = append(f.putCalls, fakePutCall{bucket: bucket, object: object, size: size, partSize: opts.PartSize})
	if f.putErr != nil {
		return minio.UploadInfo{}, f.putErr
	}
	return minio.UploadInfo{Bucket: bucket, Key: object, Size: size}, nil
}

func (f *fakeMinioClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return f.bucketExists_, f.bucketExistsErr
}

func TestS3Uploader_UploadSmallObjectSinglePUT(t *testing.T) {
	fake := &fakeMinioClient{}
	u := newS3UploaderWithFactory(fakeFactory(fake))

	content := make([]byte, 1000)
	multipart, err := u.UploadObject(context.Background(), 0, "bkt", "k1", content, 5*1024*1024)
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if multipart {
		t.Errorf("multipart = true for small object, want false")
	}
	if len(fake.putCalls) != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", len(fake.putCalls))
	}
	call := fake.putCalls[0]
	if call.bucket != "bkt" || call.object != "k1" {
		t.Errorf("call bucket/object = %q/%q, want bkt/k1", call.bucket, call.object)
	}
	if call.size != 1000 {
		t.Errorf("call size = %d, want 1000", call.size)
	}
	if call.partSize != 5*1024*1024 {
		t.Errorf("call partSize = %d, want %d", call.partSize, 5*1024*1024)
	}
}

func TestS3Uploader_UploadLargeObjectMultipart(t *testing.T) {
	fake := &fakeMinioClient{}
	u := newS3UploaderWithFactory(fakeFactory(fake))

	content := make([]byte, 10*1024*1024) // 10MiB > 5MiB partSize
	multipart, err := u.UploadObject(context.Background(), 0, "bkt", "k1", content, 5*1024*1024)
	if err != nil {
		t.Fatalf("UploadObject: %v", err)
	}
	if !multipart {
		t.Errorf("multipart = false for large object, want true")
	}
}

func TestS3Uploader_PropagatesError(t *testing.T) {
	putErr := errors.New("simulated put failure")
	fake := &fakeMinioClient{putErr: putErr}
	u := newS3UploaderWithFactory(fakeFactory(fake))

	content := []byte{1, 2, 3}
	_, err := u.UploadObject(context.Background(), 0, "bkt", "k1", content, 5*1024*1024)
	if !errors.Is(err, putErr) {
		t.Errorf("err = %v, want %v", err, putErr)
	}
}

func TestS3Uploader_LazyClientCaching(t *testing.T) {
	fake := &fakeMinioClient{}
	creates := 0
	factory := func(endpoint string) (minioPutAPI, error) {
		creates++
		return fake, nil
	}
	u := newS3UploaderWithFactory(factory)

	content := []byte{1, 2, 3}
	for i := 0; i < 5; i++ {
		_, err := u.UploadObject(context.Background(), 0, "bkt", "k1", content, 5*1024*1024)
		if err != nil {
			t.Fatalf("UploadObject %d: %v", i, err)
		}
	}
	if creates != 1 {
		t.Errorf("client created %d times, want 1 (lazy cache per endpoint)", creates)
	}
}

func TestS3Uploader_DifferentEndpointsDifferentClients(t *testing.T) {
	creates := 0
	clients := map[string]*fakeMinioClient{}
	factory := func(endpoint string) (minioPutAPI, error) {
		creates++
		c := &fakeMinioClient{}
		clients[endpoint] = c
		return c, nil
	}
	pool := &NodePool{endpoints: []string{"1.1.1.1:80", "2.2.2.2:80"}, scheme: "http"}
	u := &S3Uploader{pool: pool, clientFactory: factory, clients: make(map[int]minioPutAPI)}

	content := []byte{1, 2, 3}
	_, _ = u.UploadObject(context.Background(), 0, "b", "k1", content, 5*1024*1024)
	_, _ = u.UploadObject(context.Background(), 1, "b", "k2", content, 5*1024*1024)

	if creates != 2 {
		t.Errorf("client created %d times, want 2 (one per endpoint)", creates)
	}
	if len(clients["1.1.1.1:80"].putCalls) != 1 {
		t.Errorf("endpoint 0 should have 1 call, got %d", len(clients["1.1.1.1:80"].putCalls))
	}
	if len(clients["2.2.2.2:80"].putCalls) != 1 {
		t.Errorf("endpoint 1 should have 1 call, got %d", len(clients["2.2.2.2:80"].putCalls))
	}
}

func TestS3Uploader_BucketExists(t *testing.T) {
	fake := &fakeMinioClient{bucketExists_: true}
	u := newS3UploaderWithFactory(fakeFactory(fake))

	exists, err := u.BucketExists(context.Background(), "bkt")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Errorf("exists = false, want true")
	}
}

func fakeFactory(fake *fakeMinioClient) func(string) (minioPutAPI, error) {
	return func(endpoint string) (minioPutAPI, error) {
		return fake, nil
	}
}
