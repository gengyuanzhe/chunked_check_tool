package main

import (
	"errors"

	"github.com/minio/minio-go/v7"
)

// classifyHead 把 StatObject 返回的 (info, err) 转成三态结果。
// ok=false 表示未分类（失败），调用方应写入 head_failed。
//
// S3 对 delete marker 版本的 HEAD 返回 405 + x-amz-delete-marker: true 头；
// minio-go v7.3.0 在该路径上的转译形态未假定——两条候选路径都覆盖：
//   - (ObjectInfo{IsDeleteMarker:true}, nil) → marker
//   - (ObjectInfo{}, error with Code=="MethodNotAllowed") → 也按 marker 处理
// 实测以 Task 10 真实 MinIO 集成测试为准；若实际表现为后者但不带 IsDeleteMarker
// 标志，下方的 info.IsDeleteMarker 检查会被 bypass——见 MethodNotAllowed 分支。
func classifyHead(info minio.ObjectInfo, err error) (state string, ok bool) {
	if err == nil {
		if info.IsDeleteMarker {
			return "marker", true
		}
		return "not_marker", true
	}
	var er minio.ErrorResponse
	if errors.As(err, &er) {
		switch er.Code {
		case "NoSuchKey", "NoSuchVersion":
			return "absent", true
		case "MethodNotAllowed":
			// S3 HEAD-on-delete-marker 路径。info.IsDeleteMarker 在 error 路径
			// 上可能未被填充——405 + 该版本存在的语义本身就是 marker 信号。
			return "marker", true
		}
	}
	return "", false
}
