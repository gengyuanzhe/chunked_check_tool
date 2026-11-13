package main

// classifyHead 把 StatObject 返回的 HeadResult 转成三态结果。
// ok=false 表示未分类（失败），调用方应写入 head_failed。
//
// aws sdk 对 HEAD delete marker 版本的返回：
//   - (HeadResult{StatusCode:200, IsDeleteMarker:false}, nil) → 普通版本
//   - (HeadResult{IsDeleteMarker:true, Err:nil}, nil) → 405 + x-amz-delete-marker: true
//     路径（headResultFromErr 已把 405+marker 头转成 IsDeleteMarker=true，Err=nil）
//   - (HeadResult{StatusCode:404, Err:...}, ...) → NoSuchKey/NoSuchVersion
//   - 其他 → 未分类失败
func classifyHead(r HeadResult) (state string, ok bool) {
	if r.Err == nil {
		if r.IsDeleteMarker {
			return "marker", true
		}
		return "not_marker", true
	}
	code := errorCode(r.Err)
	switch code {
	case "NoSuchKey", "NoSuchVersion":
		return "absent", true
	}
	// aws sdk 对 HEAD 的 404 返回 code="NotFound"（HEAD 无 body，SDK 用状态码推断），
	// 不等于 "NoSuchKey"/"NoSuchVersion"。按 StatusCode==404 兜底判定为 absent。
	if r.StatusCode == 404 {
		return "absent", true
	}
	return "", false
}
