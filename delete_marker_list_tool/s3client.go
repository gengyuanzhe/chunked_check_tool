package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// S3Client 包装单个 s3.Client。本工具用 ListObjectVersions（版本化列举）
// + HeadBucket（探测桶存在）。AWS SDK 把 <Version>/<DeleteMarker>/
// <CommonPrefixes> 解析到独立 slice 字段，靠 XML 标签区分，不靠启发式。
type S3Client struct {
	client                 *s3.Client
	latency                *LatencyTracker // 可选——nil 不记录。main 启动时注入 &stats.Latency。
	logger                 *slog.Logger    // 可选——nil 不打诊断。main 在 cfg.DebugLog=true 时注入 Debug-level logger。
	maxKeys                int32           // 每页 MaxKeys；0=用 SDK 默认 1000（不显式设）。NewS3Client 从 cfg.MaxKeys 读。
	encodingTypeURL        bool            // true=请求带 EncodingType=url（wire 上 server 字段 URL 编码，避免 \x1f 等控制字符撕裂 XML）。SDK 不自动解码——s3client 出口（decodeWire，边界 A）把全部 server 字段（Key/CommonPrefix/NextKeyMarker）解码为原始形态；文件统一 QueryEscape 交给 Output 层（边界 B），与此配置无关。
	repairNextVIDMarkerOnShortResponse bool          // true=短响应触发时按 a/b/c 规则修复 NextVersionIdMarker（见 repairNextVIDMarkerShortResponse）。
}

// SetLatencyTracker 注入窗口式时延统计器。main 在 NewS3Client 后调一次。
// nil 表示不记录（test 默认 nil，不影响功能）。
func (c *S3Client) SetLatencyTracker(l *LatencyTracker) { c.latency = l }

// SetLogger 注入诊断 logger。main 在 cfg.DebugLog=true 时调一次，
// 传入 Debug-level slog logger（输出到 run.log + 终端）。nil 表示不打诊断。
func (c *S3Client) SetLogger(l *slog.Logger) { c.logger = l }

// NewS3Client 构造绑定到单个 endpoint 的 s3.Client。
// transport 调优对齐 chunked_check_tool：MaxIdleConnsPerHost=32、
// IdleConnTimeout=90s、dial_timeout 兜底黑洞 IP（默认 10s）、
// optional ResponseHeaderTimeout。https 时 InsecureSkipVerify。
// endpoint 必须是 "host:port"（无 scheme）——scheme 由 cfg.Scheme（http/https）决定，
// 单一来源，不允许 endpoint 自带 scheme 覆盖。URL 由 NewS3Client 内部拼。
// 构造期校验 endpoint 格式——aws sdk 的 LoadDefaultConfig 不校验 BaseEndpoint，
// 非法格式会延迟到第一次请求才报错，掩盖启动失败。
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

	// endpoint 必须是 host:port，不允许带 scheme（scheme 由 cfg.Scheme 决定）。
	if strings.Contains(endpoint, "://") {
		return nil, fmt.Errorf("invalid endpoint %q: must be host:port without scheme (scheme comes from cfg.scheme)", endpoint)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("invalid endpoint %q: want host:port", endpoint)
	}
	raw := cfg.Scheme + "://" + endpoint

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK, "")),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config load %s: %w", endpoint, err)
	}

	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(raw)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: tr}
	})
	return &S3Client{client: client, maxKeys: cfg.MaxKeys, encodingTypeURL: cfg.EncodingTypeURL, repairNextVIDMarkerOnShortResponse: cfg.RepairNextVIDMarkerOnShortResponse}, nil
}

// VersionEntry 把 AWS SDK 的 <Version> 和 <DeleteMarker> 摊平成一条目——
// 调用方按 IsDeleteMarker 区分。CommonPrefix 单独走 VersionPage.CommonPrefixes。
type VersionEntry struct {
	Key            string
	VersionID      string
	IsDeleteMarker bool
	LastModified   time.Time
}

// VersionPage 是单页列举结果。keyMarker/versionIDMarker 是调用
// ListVersionsPage 时传入的本次请求用的 marker；err 非 nil 时这两个值就是
// 续传点（直接写 list_failed）。NextKeyMarker/NextVersionIDMarker 是下一页
// 用的 marker——encoding_type_url=true 时 NextKeyMarker 已在出口解码为原始
// key（直接回传下一页请求），VersionIdMarker 不编码故原样。IsTruncated=false
// 时 next-marker 无意义。
// NextVIDMarkerRewritten=true 表示 s3client 在短响应触发时修复了 NextVersionIDMarker
// （cfg.RepairNextVIDMarkerOnShortResponse=true 时启用）。修复分三支（见 repairNextVIDMarkerShortResponse）：
// a. 非整数（含 "null"、真实 version-id）→ 不动；b. 整数 0 → "null"；
// c. 非 0 整数 → 用本页尾条目推导覆盖。RewrittenRawVIDMarker 存修复前的原值，便于日志定位。
type VersionPage struct {
	Versions                []VersionEntry
	CommonPrefixes          []string
	NextKeyMarker           string
	NextVersionIDMarker     string
	IsTruncated             bool
	NextVIDMarkerRewritten  bool
	RewrittenRawVIDMarker   string
}

// decodeWire 是"边界 A"：encoding_type_url=true 时服务端返回值在 wire 上是 URL
// 编码态（%XX），出口统一解码为原始形态——此后工具内部（BFS 前缀、翻页 marker、
// 逐条目的 key）恒为原始形态，文件编码统一交给 Output 层（边界 B）。
// encoding_type_url=false 时服务端不编码，原样返回（对原始值做解码会破坏含
// 字面 %XX/+ 序列的真实 key——解码仅发生在服务端确实编过码时，恰一次）。
// 解码失败（非法 %XX——服务端 bug）：保留原值 + Warn，best-effort——翻页 marker
// 变形时页位置可能错位，Warn 是唯一信号（不会记 list_failed）。
func (c *S3Client) decodeWire(bucket, prefix, raw string) string {
	if !c.encodingTypeURL || raw == "" {
		return raw
	}
	dec, err := url.QueryUnescape(raw)
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("wire url decode failed",
				"bucket", bucket, "prefix", prefix, "raw", raw, "err", err)
		}
		return raw
	}
	return dec
}

// ListVersionsPage 列举一页。recursive=true 不带 delimiter（所有 key 所有版本）；
// recursive=false 带 / delimiter（直接子 key 的所有版本 + common prefix）。
// keyMarker/versionIDMarker 非空 = 从该 marker 起续扫（retry 场景）。
// 三类条目（Version/DeleteMarker/CommonPrefix）按 XML 标签区分，不靠字段零值
// 启发式——key 以 / 结尾的真实版本不会被误判成 common prefix。
func (c *S3Client) ListVersionsPage(ctx context.Context, bucket, prefix string, recursive bool, keyMarker, versionIDMarker string) (*VersionPage, error) {
	delimiter := "/"
	if recursive {
		delimiter = ""
	}
	var reqKeyMarker, reqVersionIDMarker *string
	if keyMarker != "" {
		reqKeyMarker = aws.String(keyMarker)
	}
	if versionIDMarker != "" {
		reqVersionIDMarker = aws.String(versionIDMarker)
	}
	// [diag] 每次请求的入参 marker（=进 URL 的 key-marker/version-id-marker）。
	// cfg.DebugLog=true 时打——追 version-id-marker=0 来源看 req vidm 是否非预期地是 "0"。
	if c.logger != nil {
		c.logger.Debug("list-page req",
			"bucket", bucket, "prefix", prefix, "recursive", recursive,
			"km", keyMarker, "vidm", versionIDMarker, "maxKeys", c.maxKeys)
	}
	start := time.Now()
	input := &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucket),
		Prefix:          aws.String(prefix),
		Delimiter:       aws.String(delimiter),
		KeyMarker:       reqKeyMarker,
		VersionIdMarker: reqVersionIDMarker,
	}
	// maxKeys 是必填字段（config 校验保证 1..1000），故总是显式设 MaxKeys。
	input.MaxKeys = aws.Int32(c.maxKeys)
	// encoding_type_url=true 时请求带 EncodingType=url——wire 上 server 字段 URL 编码，
	// 避免 \x1f 等 XML 1.0 非法控制字符撕裂 XML 反序列化。SDK 不自动解码，出口由
	// decodeWire 统一解码（见其注释）。该配置只影响 wire 行为；记录文件的编码与
	// 它无关（Output 层无条件 QueryEscape，读取侧无条件 QueryUnescape）。
	if c.encodingTypeURL {
		input.EncodingType = types.EncodingTypeUrl
	}
	out, err := c.client.ListObjectVersions(ctx, input)
	if c.latency != nil {
		c.latency.Record(time.Since(start))
	}
	if err != nil {
		if c.logger != nil {
			// req_id 从 *awshttp.ResponseError 提（失败响应一定有 HTTP response）。
			reqID := ""
			var re *awshttp.ResponseError
			if errors.As(err, &re) {
				reqID = re.Response.Header.Get("x-amz-request-id")
			}
			c.logger.Debug("list-page err",
				"bucket", bucket, "prefix", prefix,
				"km", keyMarker, "vidm", versionIDMarker, "req_id", reqID, "err", err)
		}
		return nil, err
	}
	// [diag] 服务端返回的 next-marker——下一页请求用的 marker，也是失败时
	// （下一页）写进 list_failed 的续传点。nVer/nDM/nCP 是三个 slice 的计数。
	if c.logger != nil {
		c.logger.Debug("list-page resp",
			"bucket", bucket, "prefix", prefix,
			"nextKM", aws.ToString(out.NextKeyMarker), "nextVIDM", aws.ToString(out.NextVersionIdMarker),
			"truncated", aws.ToBool(out.IsTruncated),
			"nVer", len(out.Versions), "nDM", len(out.DeleteMarkers), "nCP", len(out.CommonPrefixes))
	}
	page := &VersionPage{
		NextKeyMarker:       aws.ToString(out.NextKeyMarker),
		NextVersionIDMarker: aws.ToString(out.NextVersionIdMarker),
		IsTruncated:         aws.ToBool(out.IsTruncated),
	}
	// 边界 A：NextKeyMarker 解码为原始 key——下一页请求的 key-marker 必须回传原始
	// 形态（服务端对 key-marker 只做一次标准 query 解码后按原始 key 定位翻页位置，
	// 编码态回传会让位置错位；minio 实测漏扫）。NextVersionIdMarker 不在
	// encoding-type=url 的编码字段列表（AWS 只编 KeyMarker/NextKeyMarker/Prefix/
	// Key/Delimiter；minio 也不编 version-id），保持原样。
	page.NextKeyMarker = c.decodeWire(bucket, prefix, page.NextKeyMarker)
	page.Versions = make([]VersionEntry, 0, len(out.Versions)+len(out.DeleteMarkers))
	for i := range out.Versions {
		v := out.Versions[i]
		page.Versions = append(page.Versions, VersionEntry{
			Key:            c.decodeWire(bucket, prefix, aws.ToString(v.Key)),
			VersionID:      aws.ToString(v.VersionId),
			IsDeleteMarker: false,
			LastModified:   aws.ToTime(v.LastModified),
		})
	}
	for i := range out.DeleteMarkers {
		v := out.DeleteMarkers[i]
		page.Versions = append(page.Versions, VersionEntry{
			Key:            c.decodeWire(bucket, prefix, aws.ToString(v.Key)),
			VersionID:      aws.ToString(v.VersionId),
			IsDeleteMarker: true,
			LastModified:   aws.ToTime(v.LastModified),
		})
	}
	page.CommonPrefixes = make([]string, 0, len(out.CommonPrefixes))
	for i := range out.CommonPrefixes {
		// CommonPrefix 与 Key 同规则解码：它是 BFS 内部状态（下一轮 prefix），SDK 发
		// 请求时会再 URL 编码一次，不解码会二次编码（photos%2F → photos%252F），服务端
		// 解一层得到 photos%2F 匹配不到真实目录，BFS 漏扫。
		cp := c.decodeWire(bucket, prefix, aws.ToString(out.CommonPrefixes[i].Prefix))
		page.CommonPrefixes = append(page.CommonPrefixes, cp)
	}
	// 短响应触发的 next-marker 修复（cfg.RepairNextVIDMarkerOnShortResponse=true 时启用）。
	// 触发条件：IsTruncated=true 且本页条目数（Version+DeleteMarker+CommonPrefix）< maxKeys
	// ——矛盾状态，常预示服务端翻页 quirk。修复 NextVersionIDMarker 的 a/b/c 三支见
	// repairNextVIDMarkerShortResponse 的注释。改写发生在 s3client 出口——lister 和
	// list_failed.txt 看到的都是改写后的值，下一页请求也用改写后的值。改写命中时
	// 设 NextVIDMarkerRewritten=true + RewrittenRawVIDMarker=<原值>，lister 据此打 Warn
	// 日志到 list_failed.log（与 short-response 一致，便于线上排查）。
	if c.repairNextVIDMarkerOnShortResponse && page.IsTruncated {
		count := int32(len(page.Versions) + len(page.CommonPrefixes))
		if count < c.maxKeys {
			repairNextVIDMarkerShortResponse(page, out.Versions, out.DeleteMarkers, c.encodingTypeURL)
		}
	}
	return page, nil
}

// repairNextVIDMarkerShortResponse 在短响应触发时按 a/b/c 规则修复
// page.NextVersionIDMarker。调用前提：page.IsTruncated=true 且本页条目数 < maxKeys
// （由调用方判断）。命中修复时设 page.NextVIDMarkerRewritten=true 并把原值存进
// page.RewrittenRawVIDMarker，便于 lister 打 Warn 日志定位。versions/deleteMarkers
// 是 SDK 解析出的原始 slice（未被合并）。
//
// 修复分支（按 NextVersionIDMarker 的内容分派）：
//
//	a. 非整数（"" / "null" / 真实 version-id 等非数字串）→ 不动，认为服务端给得对。
//	b. 解析为整数 0 → 改写为 "null"。覆盖 CommonPrefix 作 NextKeyMarker 的情况——
//	   目录在存储底层无版本概念，version-id 恒为 0/null，"0" 是服务端 quirk。
//	c. 解析为非 0 整数 → 取 versions/deleteMarkers 各自尾元素中按全局序靠后者，
//	   与 NextKeyMarker 解码后比较；一致→该条目的 VersionID 覆盖 NextVersionIDMarker，
//	   不一致→不动（NextKeyMarker 指向本页之外，server marker 可能本就对）。
//
// 尾元素比较的依据：S3 协议保证 wire 上 Version/DeleteMarker 按全局序交错出现，SDK
// 按出现顺序灌进各自 slice，故每个 slice 内部都保持全局序——全局尾 = 两个 slice
// 尾元素中按全局序较大者。全局序规则：先按 key 字典序升序，同 key 内按 LastModified
// 倒序（新版本在前）。所以两尾 key 不同时取 key 大者；key 相同时取 LastModified 更小
// （更旧版本）者。VersionId 不参与排序（S3 不保证 version-id 有任何序）。
//
// key 解码：encoding_type_url=true 时 SDK 不自动解码，本函数收到的 versions/
// deleteMarkers（SDK 原始 slice，非 page.Versions）的 Key 是 wire 态（%XX 编码），
// 比较前 url.QueryUnescape。page.NextKeyMarker 已在 ListVersionsPage 出口解码为
// 原始 key，直接用（二次解码会破坏含字面 % 序列的真实 key）。
// 服务端排序基于存储里的真实 key（解码态）——编码只是输出的表示层，排序在编码之前
// 发生，故候选必须统一到解码态比，结果才反映服务端序。
func repairNextVIDMarkerShortResponse(page *VersionPage, versions []types.ObjectVersion, deleteMarkers []types.DeleteMarkerEntry, encodingTypeURL bool) {
	vid := page.NextVersionIDMarker
	if vid == "" {
		return // 分支 a：空串视为正常
	}
	n, err := strconv.ParseInt(vid, 10, 64)
	if err != nil {
		return // 分支 a：非整数（含 "null" 与真实 version-id 串）
	}
	if n == 0 {
		// 分支 b：0 → "null"。
		page.NextVersionIDMarker = "null"
		page.NextVIDMarkerRewritten = true
		page.RewrittenRawVIDMarker = vid
		return
	}
	// 分支 c：非 0 整数。用本页尾条目推导真实 version-id。
	decode := func(s string) string {
		if !encodingTypeURL {
			return s
		}
		d, err := url.QueryUnescape(s)
		if err != nil {
			return s // 解码失败保留原值——后续比较大概率不一致，走"不动"分支
		}
		return d
	}
	// 协议保证每个 slice 内部保持全局序，故全局尾 = 两个 slice 尾元素中按全局序较大者。
	// 全局序：key 字典序升序，同 key 内 LastModified 倒序（新版本在前）——所以
	// key 不同取 key 大者；key 相同取 LastModified 更小（更旧）者。
	// 候选只需考虑非空 slice 的尾；都空则无 Version/DeleteMarker 可推导，不动。
	var bestKey, bestVID string
	var bestLM time.Time
	found := false
	consider := func(key, vid string, lm time.Time) {
		if !found {
			bestKey, bestVID, bestLM = key, vid, lm
			found = true
			return
		}
		if key > bestKey || (key == bestKey && lm.Before(bestLM)) {
			bestKey, bestVID, bestLM = key, vid, lm
		}
	}
	if n := len(versions); n > 0 {
		v := versions[n-1]
		consider(decode(aws.ToString(v.Key)), aws.ToString(v.VersionId), aws.ToTime(v.LastModified))
	}
	if n := len(deleteMarkers); n > 0 {
		dm := deleteMarkers[n-1]
		consider(decode(aws.ToString(dm.Key)), aws.ToString(dm.VersionId), aws.ToTime(dm.LastModified))
	}
	if !found {
		return // 本页无 Version/DeleteMarker（仅 CommonPrefix）——交给分支 b 处理或不动
	}
	nextKM := page.NextKeyMarker // 出口已解码（见 ListVersionsPage），不能再解
	if nextKM == bestKey {
		// 对上了——server 给的 NextKeyMarker 是本页尾条目的 key，但 version-id 错了，
		// 用本页尾条目的真实 VersionID 覆盖。
		page.NextVersionIDMarker = bestVID
		page.NextVIDMarkerRewritten = true
		page.RewrittenRawVIDMarker = vid
	}
	// 不一致——NextKeyMarker 指向本页之外，server marker 可能本就对，不动。
}

// BucketExists 探测桶是否存在/可达。
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return false, err
	}
	return true, nil
}
