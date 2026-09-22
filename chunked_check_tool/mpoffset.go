// mpoffset.go — multipart offset-etag check plumbing.
//
// The S3 server (self-developed, repair tooling only) accepts one extra
// request header on LIST calls: internal-list-mp-offset: true. When present,
// multipart objects come back with their ETag rewritten to carry the real
// part boundaries:
//
//	<32hex-md5>-<partcnt>-<off0>|<off1>|...
//
// The header never participates in SigV2/SigV4 signature calculation (only
// x-amz-* headers must be signed), so it is injected at the transport layer
// after signing — no custom request building needed.
package main

import (
	"net/http"
	"strconv"
	"strings"
)

// mpOffsetListHeader is the internal repair-scenario header. Not part of any
// public API contract; never expose in external docs.
const mpOffsetListHeader = "internal-list-mp-offset"

// mpOffsetTransport wraps the base transport and adds mpOffsetListHeader to
// LIST requests only. Everything else passes through untouched.
type mpOffsetTransport struct {
	base http.RoundTripper
}

func (t *mpOffsetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if isListRequest(req) {
		// Clone before mutating: minio-go reuses one request object across
		// its internal retry loop, so the caller's header map must stay
		// untouched.
		r := req.Clone(req.Context())
		r.Header.Set(mpOffsetListHeader, "true")
		return t.base.RoundTrip(r)
	}
	return t.base.RoundTrip(req)
}

// isListRequest reports whether req is a bucket LIST the tool would issue
// through minio-go v7.3.0's Core API. The shapes are pinned by
// TestMpOffsetTransportMinioList; a minio-go upgrade must re-verify them.
//
//   - Method must be GET.
//   - Path is the bucket root (at most one non-empty segment), so object
//     GETs (path-style /<bucket>/<key>) are excluded. Virtual-host style
//     (path "/" or "/<key>") is covered by the query checks below.
//   - ListObjectsV2: query carries list-type=2.
//   - ListObjects V1: query is non-empty and every key is one of
//     prefix/delimiter/encoding-type/marker/max-keys — exactly the set
//     minio-go emits. This excludes bucket subresource probes (?location,
//     ?policy) and object-level params (versionId, partNumber, uploadId).
//
// A bare GET /<bucket> with no query at all (V1 list with all defaults) is
// deliberately NOT matched: minio-go always sends prefix/delimiter/
// encoding-type, so such a request never originates from this tool, and
// conservative non-injection is the safer default.
func isListRequest(req *http.Request) bool {
	if req.Method != http.MethodGet {
		return false
	}
	if strings.Count(strings.Trim(req.URL.Path, "/"), "/") > 1 {
		return false
	}
	q := req.URL.Query()
	if q.Get("list-type") == "2" {
		return true
	}
	if len(q) == 0 {
		return false
	}
	for k := range q {
		switch k {
		case "prefix", "delimiter", "encoding-type", "marker", "max-keys":
		default:
			return false
		}
	}
	return true
}

// parseMultipartOffsetETag parses the offset-carrying multipart ETag
// <32hex-md5>-<partcnt>-<off0>|<off1>|... into its part offsets. Validation
// mirrors parseListFileLine's rules so both offset sources are
// interchangeable downstream: md5 is a 32-char lowercase hex (isNormalETag
// strictness), partcnt >= 1 and equals the offset count, offsets are
// non-negative, strictly increasing, and the first is 0.
//
// Each offset is also checked against the object's known size: any offset >
// size means the ETag claims a part starting beyond the object's end, which
// is invalid. size=0 is a legitimate object (empty multipart); the only
// valid offset sequence for size=0 is single-part [0] (a multipart with
// >1 parts would require a non-zero second offset, which exceeds size=0).
//
// Anything else — including a plain "<md5>-<N>" ETag from a server that does
// not implement the header — returns (nil, false); the caller falls back to
// routing the object into list_parse_failed (mode=offset) or mp.txt
// unverified.
func parseMultipartOffsetETag(etag string, size int64) ([]int64, bool) {
	parts := strings.SplitN(etag, "-", 3)
	if len(parts) != 3 || !isNormalETag(parts[0]) {
		return nil, false
	}
	partcnt, err := strconv.Atoi(parts[1])
	if err != nil || partcnt < 1 {
		return nil, false
	}
	offStrs := strings.Split(parts[2], "|")
	if len(offStrs) != partcnt {
		return nil, false
	}
	offs := make([]int64, partcnt)
	for i, s := range offStrs {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil || v < 0 {
			return nil, false
		}
		offs[i] = v
	}
	if offs[0] != 0 {
		return nil, false
	}
	for i := 1; i < partcnt; i++ {
		if offs[i] <= offs[i-1] {
			return nil, false
		}
	}
	for _, off := range offs {
		if off > size {
			return nil, false
		}
	}
	return offs, true
}
