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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// --- withMpOffsetListHeader ---

// listXMLV2Empty is a one-page, empty V2 LIST response shared by the
// mp-offset header tests.
const listXMLV2Empty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>bkt</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

// TestMpOffsetHeaderListOnly is the per-call replacement for the old
// transport-shape pinning: listPageOnce passes withMpOffsetListHeader on
// LIST calls only, so a LIST through the real aws client must carry
// internal-list-mp-offset: true (in multipart_check_mode=offset + is_check)
// while a HeadObject through the same S3Client must not — "LIST only" is
// now structural (the option exists only at the list call sites), and this
// test pins the end-to-end wire behavior.
func TestMpOffsetHeaderListOnly(t *testing.T) {
	var mu sync.Mutex
	var listWithHdr, listWithoutHdr, headWithHdr int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasHdr := r.Header.Get(mpOffsetListHeader) == "true"
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodHead {
			if hasHdr {
				headWithHdr++
			}
			w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(200)
			return
		}
		if hasHdr {
			listWithHdr++
		} else {
			listWithoutHdr++
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(listXMLV2Empty))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	client, err := NewAWSClient(host, "ak", "sk", false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// mode=offset + is_check → LISTs carry the header.
	c := NewS3Client(client, "bkt", NewStats(), nil, -1,
		&Config{ListAPIVersion: 2, IsCheck: true, MultipartCheckMode: MultipartCheckModeOffset})
	if _, _, _, err := c.ListPage(context.Background(), "", "", "", false, 1000); err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if _, _, err := c.HeadObject(context.Background(), "k1"); err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	// The same bucket through a client without mode=offset: no header.
	c2 := NewS3Client(client, "bkt", NewStats(), nil, -1, &Config{ListAPIVersion: 2})
	if _, _, _, err := c2.ListPage(context.Background(), "", "", "", false, 1000); err != nil {
		t.Fatalf("ListPage (no mp-offset): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if listWithHdr != 1 {
		t.Errorf("LIST requests carrying header = %d, want 1", listWithHdr)
	}
	if listWithoutHdr != 1 {
		t.Errorf("LIST requests without header = %d, want 1", listWithoutHdr)
	}
	if headWithHdr != 0 {
		t.Errorf("HeadObject carried header %d times, want 0", headWithHdr)
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

// TestAWSListLargeOffsetETag — the aws SDK must round-trip a ~110 KB
// offset-carrying ETag (XML-escaped quotes, one Contents entry) without
// truncation, and our parser must still handle the result. This pins the
// end-to-end ETag path: server XML → SDK decode → trimETagQuotes →
// parseMultipartOffsetETag.
func TestAWSListLargeOffsetETag(t *testing.T) {
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
		// The aws SDK emits list-type=2 for ListObjectsV2; the server
		// responds identically to any list-type=2 request.
		w.Write([]byte(listXML))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	client, err := NewAWSClient(host, "ak", "sk", false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	result, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("bkt"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 with %d-byte ETag failed: %v", len(etag), err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("got %d contents, want 1", len(result.Contents))
	}
	got := trimETagQuotes(aws.ToString(result.Contents[0].ETag))
	if len(got) != len(etag) {
		t.Errorf("ETag length after round-trip = %d, want %d (truncated by the SDK?)", len(got), len(etag))
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
	// pipeline (SDK decode → trimETagQuotes → our parser) must produce offsets.
	offs, ok := parseMultipartOffsetETag(got, int64(partCount)*partSize)
	if !ok {
		t.Fatalf("parseMultipartOffsetETag rejected the round-tripped ETag")
	}
	if len(offs) != partCount {
		t.Errorf("parsed offset count = %d, want %d", len(offs), partCount)
	}
	t.Logf("aws SDK decoded %d-byte ETag, round-trip OK, parsed %d offsets", len(got), len(offs))
}
