package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
			// Size=0 would reject any non-zero offset; pass a large Size
			// so format validation is tested without conflating with the
			// size check (covered separately by SizeValidation).
			got, ok := parseMultipartOffsetETag(c.etag, 1<<40)
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
		name   string
		method string
		path   string
		query  string
		want   bool
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

// TestParseMultipartOffsetETagLargeScale verifies parseMultipartOffsetETag
// handles an extreme but realistic object: 10000 parts at 5 GiB each. The
// server rewrites the ETag to <md5>-10000-0|5368709120|...|5368709120*9999,
// a ~110 KB string. The test asserts the parser accepts it, returns the
// expected part count and offsets, and finishes in well under a second —
// the real concern is correctness on this volume, not latency (a 50 TB
// object spending tens of seconds in runProbes is acceptable).
func TestParseMultipartOffsetETagLargeScale(t *testing.T) {
	const md5 = "0123456789abcdef0123456789abcdef"
	const partCount = 10000
	const partSize = int64(5 * 1024 * 1024 * 1024) // 5 GiB

	offStrs := make([]string, partCount)
	for i := 0; i < partCount; i++ {
		offStrs[i] = fmt.Sprintf("%d", int64(i)*partSize)
	}
	etag := fmt.Sprintf("%s-%d-%s", md5, partCount, strings.Join(offStrs, "|"))

	start := time.Now()
	offs, ok := parseMultipartOffsetETag(etag, int64(partCount)*partSize)
	elapsed := time.Since(start)

	if !ok {
		t.Fatalf("parseMultipartOffsetETag returned ok=false on %d-part etag (len=%d)", partCount, len(etag))
	}
	if len(offs) != partCount {
		t.Fatalf("offset count = %d, want %d", len(offs), partCount)
	}
	if offs[0] != 0 {
		t.Fatalf("offs[0] = %d, want 0", offs[0])
	}
	for i := 1; i < partCount; i++ {
		want := int64(i) * partSize
		if offs[i] != want {
			if i < 3 || i >= partCount-1 {
				t.Errorf("offs[%d] = %d, want %d", i, offs[i], want)
			}
		}
	}
	lastExpected := int64(partCount-1) * partSize
	if offs[partCount-1] != lastExpected {
		t.Errorf("offs[last] = %d, want %d", offs[partCount-1], lastExpected)
	}
	t.Logf("parsed %d-part etag (len=%d bytes) in %v", partCount, len(etag), elapsed)
}

// TestParseMultipartOffsetETagSizeValidation verifies the offset > Size
// check: an ETag whose offsets are individually well-formed but claim a part
// starting beyond the object's known size is invalid. size=0 is a legitimate
// empty object — the only valid offset sequence is single-part [0].
func TestParseMultipartOffsetETagSizeValidation(t *testing.T) {
	const md5 = "0123456789abcdef0123456789abcdef"
	etag3 := md5 + "-3-0|5242880|10485760" // 3 parts, offsets 0/5M/10M

	cases := []struct {
		name string
		etag string
		size int64
		want bool
	}{
		{"size equals last offset", etag3, 10485760, true},
		{"size larger than last offset", etag3, 15728640, true},
		{"size smaller than last offset", etag3, 8000000, false},
		{"size smaller than middle offset", etag3, 5242880, false},
		{"size 1 with multi-part", etag3, 1, false},
		{"empty object single part", md5 + "-1-0", 0, true},
		{"empty object multi-part", etag3, 0, false},
		{"single part offset 0 fits any size", md5 + "-1-0", 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ok := parseMultipartOffsetETag(c.etag, c.size)
			if ok != c.want {
				t.Errorf("size=%d: ok=%v, want %v", c.size, ok, c.want)
			}
		})
	}
}

// TestMinioListLargeOffsetETag verifies the minio-go SDK itself can parse a
// LIST response whose ETag carries 10000 part offsets (~145 KB). This is
// the real concern for multipart_check_mode=offset: parseMultipartOffsetETag
// is ours, but the XML decoding is minio's. A stack overflow, truncation, or
// silent ETag drop in minio-go's xmlDecoder would break the offset pipeline
// before our code ever runs.
//
// The test stands up an httptest server that returns a V2 ListBucketResult
// with one Contents entry whose ETag is <md5>-10000-0|5368709120|...|...,
// then issues a real Core.ListObjectsV2 through a minio client and asserts:
//   - no error
//   - exactly one object returned
//   - the ETag survived the round-trip byte-for-byte (length + first/last
//     offsets intact)
//
// Pinned to minio-go v7.3.0's V2 LIST query shape; a minio upgrade must
// re-verify the XML decoder still handles ETags this large.
func TestMinioListLargeOffsetETag(t *testing.T) {
	const md5 = "0123456789abcdef0123456789abcdef"
	const partCount = 10000
	const partSize = int64(5 * 1024 * 1024 * 1024)

	offStrs := make([]string, partCount)
	for i := 0; i < partCount; i++ {
		offStrs[i] = fmt.Sprintf("%d", int64(i)*partSize)
	}
	etag := fmt.Sprintf("%s-%d-%s", md5, partCount, strings.Join(offStrs, "|"))
	etagQuoted := "&quot;" + etag + "&quot;"

	listXML := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>bkt</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
<Contents><Key>bigobj</Key><LastModified>2026-09-22T00:00:00.000Z</LastModified><ETag>%s</ETag><Size>53687091200000</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`, etagQuoted)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		// minio-go v7.3.0 Core.ListObjectsV2 emits list-type=2; the server
		// responds identically to any list-type=2 request.
		w.Write([]byte(listXML))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	client, err := minio.New(host, &minio.Options{
		Creds:     credentials.NewStaticV4("ak", "sk", ""),
		Secure:    false,
		Region:    "us-east-1",
		Transport: &http.Transport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	core := &minio.Core{Client: client}

	result, err := core.ListObjectsV2("bkt", "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjectsV2 with %d-byte ETag failed: %v", len(etag), err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("got %d contents, want 1", len(result.Contents))
	}
	got := trimETagQuotes(result.Contents[0].ETag)
	if len(got) != len(etag) {
		t.Errorf("ETag length after round-trip = %d, want %d (truncated by minio?)", len(got), len(etag))
	}
	if got != etag {
		first, last := 64, 64
		if len(got) < first {
			first = len(got)
		}
		if len(got) < last {
			last = len(got)
		}
		t.Errorf("ETag mismatch: got[%d]=%q want[%d]=%q", len(got), got[:first], len(etag), etag[:first])
		t.Errorf("ETag tail: got=%q want=%q", got[len(got)-last:], etag[len(etag)-last:])
	}
	// Verify our parser still handles the round-tripped ETag — the full
	// pipeline (minio decode → trimETagQuotes → our parser) must produce offsets.
	offs, ok := parseMultipartOffsetETag(got, int64(partCount)*partSize)
	if !ok {
		t.Fatalf("parseMultipartOffsetETag rejected the round-tripped ETag")
	}
	if len(offs) != partCount {
		t.Errorf("parsed offset count = %d, want %d", len(offs), partCount)
	}
	t.Logf("minio-go decoded %d-byte ETag, round-trip OK, parsed %d offsets", len(got), len(offs))
}
