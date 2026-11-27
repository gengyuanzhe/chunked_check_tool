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
// host, x-amz-*, and content-type headers are signed), so adding it at
// middleware Build time — before the signer runs — has the same semantics
// as the old transport-after-sign injection: the server that checks
// signatures never sees it included in them.
package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// mpOffsetListHeader is the internal repair-scenario header. Not part of any
// public API contract; never expose in external docs.
const mpOffsetListHeader = "internal-list-mp-offset"

// withMpOffsetListHeader is a per-call s3.Options mutator that adds
// internal-list-mp-offset: true to exactly the one request it is passed to.
// s3client passes it on ListObjects / ListObjectsV2 calls only (see
// listPageOnce), so "LIST requests only, never HEAD/GET/PUT" is structural —
// no request-shape sniffing and no transport wrapping, which is what the
// old minio-go transport needed (its Core API had no per-call header hook,
// and the shape pinning broke on every SDK upgrade).
func withMpOffsetListHeader(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc(
			"mpOffsetListHeader",
			func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set(mpOffsetListHeader, "true")
				}
				return next.HandleBuild(ctx, in)
			},
		), middleware.After)
	})
}

// parseMultipartOffsetETag parses the offset-carrying multipart ETag
// <32hex-md5>-<partcnt>-<off0>|<off1>|... into its part offsets. Validation
// mirrors parseMultipartLine's rules so both offset sources are
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
