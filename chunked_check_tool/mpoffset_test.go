package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// --- parseMultipartOffsetETag ---

func TestParseMultipartOffsetETag(t *testing.T) {
	const md5 = "0123456789abcdef0123456789abcdef"
	cases := []struct {
		name string
		etag string
		want []int64
		ok   bool
	}{
		{"three parts", md5 + "-3-0|5242880|10485760", []int64{0, 5242880, 10485760}, true},
		{"single part", md5 + "-1-0", []int64{0}, true},
		{"plain multipart etag", md5 + "-3", nil, false},
		{"normal etag", md5, nil, false},
		{"uppercase md5", strings.ToUpper(md5) + "-2-0|5", nil, false},
		{"md5 not 32 chars", "0123456789abcdef-3-0|5|10", nil, false},
		{"md5 not hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-2-0|5", nil, false},
		{"partcnt zero", md5 + "-0-0", nil, false},
		{"partcnt negative", md5 + "--1-0", nil, false},
		{"partcnt not a number", md5 + "-x-0|5", nil, false},
		{"offset count mismatch", md5 + "-3-0|5", nil, false},
		{"offset0 not zero", md5 + "-2-1|5", nil, false},
		{"offsets equal", md5 + "-3-0|10|10", nil, false},
		{"offsets decreasing", md5 + "-3-0|10|5", nil, false},
		{"offset not a number", md5 + "-2-0|x", nil, false},
		{"offset negative", md5 + "-2-0|-5", nil, false},
		{"empty offsets section", md5 + "-3-", nil, false},
		{"empty etag", "", nil, false},
		{"two sections only", "abc-def", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseMultipartOffsetETag(c.etag)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if !c.ok {
				return
			}
			if len(got) != len(c.want) {
				t.Fatalf("offsets = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("offsets = %v, want %v", got, c.want)
				}
			}
		})
	}
}

// --- isListRequest ---

func TestIsListRequest(t *testing.T) {
	cases := []struct {
		name  string
		method string
		path   string
		query  string
		want  bool
	}{
		// V2 lists minio-go emits via Core.ListObjectsV2.
		{"v2 full", "GET", "/bkt", "delimiter=%2F&encoding-type=url&fetch-owner=true&list-type=2&max-keys=1000&prefix=dir%2F", true},
		{"v2 continuation", "GET", "/bkt", "continuation-token=abc%3D%3D&list-type=2", true},
		{"v2 start-after", "GET", "/bkt", "list-type=2&start-after=key1", true},
		// V1 lists minio-go emits via Core.ListObjects.
		{"v1 full", "GET", "/bkt", "delimiter=%2F&encoding-type=url&marker=dir%2Fk&max-keys=1000&prefix=dir%2F", true},
		{"v1 minimal", "GET", "/bkt", "delimiter=&encoding-type=url&prefix=", true},
		{"v1 marker only page", "GET", "/bkt", "encoding-type=url&marker=abc", true},
		// Non-list requests the tool issues.
		{"bucket location probe", "GET", "/bkt", "location=", false},
		{"object get", "GET", "/bkt/key1", "", false},
		{"object get versionId", "GET", "/bkt/key1", "versionId=v1", false},
		{"object get partNumber", "GET", "/bkt/key1", "partNumber=1&uploadId=u1", false},
		{"head object", "HEAD", "/bkt/key1", "", false},
		{"put object", "PUT", "/bkt/key1", "", false},
		{"multipart init", "POST", "/bkt/key1", "uploads", false},
		{"complete multipart", "POST", "/bkt/key1", "uploadId=u1", false},
		{"abort multipart", "DELETE", "/bkt/key1", "uploadId=u1", false},
		// Unknown bucket-level GETs default to no injection.
		{"no query bucket root", "GET", "/bkt", "", false},
		{"bucket policy", "GET", "/bkt", "policy", false},
		// Virtual-host style: bucket in host, path is / (list) or /key.
		{"vhost v2 list", "GET", "/", "list-type=2", true},
		{"vhost object get", "GET", "/key1", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequest(c.method, "http://10.0.0.1:9000"+c.path+"?"+c.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := isListRequest(req); got != c.want {
				t.Errorf("isListRequest(%s %s?%s) = %v, want %v", c.method, c.path, c.query, got, c.want)
			}
		})
	}
}

// TestMpOffsetTransportNotMutatingOriginal verifies RoundTrip clones before
// setting the header: minio-go reuses one request object across internal
// retries, so the wrapper must not mutate it.
func TestMpOffsetTransportNotMutatingOriginal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	tr := &mpOffsetTransport{base: http.DefaultTransport}
	req, _ := http.NewRequest("GET", srv.URL+"/bkt?list-type=2&prefix=", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, ok := req.Header[http.CanonicalHeaderKey(mpOffsetListHeader)]; ok {
		t.Error("original request header map was mutated by RoundTrip")
	}
}

// TestMpOffsetTransportMinioList is the pin on minio-go v7.3.0's actual
// request shapes: a Core.ListObjectsV2 and Core.ListObjects call through a
// wrapped client MUST carry the header, while the ?location= region probe and
// a StatObject MUST NOT. If minio-go is ever upgraded and changes its LIST
// query shape so isListRequest stops matching, this test fails.
func TestMpOffsetTransportMinioList(t *testing.T) {
	const listXMLV2 = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>bkt</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`
	var mu sync.Mutex
	var listWithHdr, listWithoutHdr, probeWithHdr int
	statWithHdr := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasHdr := r.Header.Get(mpOffsetListHeader) == "true"
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.RawQuery == "location=":
			if hasHdr {
				probeWithHdr++
			}
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
		case r.Method == http.MethodHead:
			if hasHdr {
				statWithHdr++
			}
			w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(200)
		default:
			if hasHdr {
				listWithHdr++
			} else {
				listWithoutHdr++
			}
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(listXMLV2))
		}
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	client, err := minio.New(host, &minio.Options{
		Creds:     credentials.NewStaticV4("ak", "sk", ""),
		Transport: &mpOffsetTransport{base: &http.Transport{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	core := &minio.Core{Client: client}

	if _, err := core.ListObjectsV2("bkt", "", "", "", "/", 1000); err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if _, err := core.ListObjects("bkt", "", "", "/", 1000); err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if _, err := client.StatObject(context.Background(), "bkt", "k1", minio.StatObjectOptions{}); err != nil {
		t.Fatalf("StatObject: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if listWithHdr != 2 {
		t.Errorf("list requests carrying header = %d, want 2 (V1+V2)", listWithHdr)
	}
	if listWithoutHdr != 0 {
		t.Errorf("list requests without header = %d, want 0", listWithoutHdr)
	}
	if probeWithHdr != 0 {
		t.Errorf("location probe carried header %d times, want 0", probeWithHdr)
	}
	if statWithHdr != 0 {
		t.Errorf("StatObject carried header, want none")
	}
}
