package main

import (
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestClassifyHead_MarkerInfoNilErr(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: true}
	state, ok := classifyHead(info, nil)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_RegularInfoNilErr(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: false, ETag: "abc"}
	state, ok := classifyHead(info, nil)
	if !ok || state != "not_marker" {
		t.Errorf("got (%q, %v) want (not_marker, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchVersionErr(t *testing.T) {
	err := minio.ErrorResponse{Code: "NoSuchVersion", StatusCode: 404}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchKeyErr(t *testing.T) {
	err := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_MethodNotAllowed(t *testing.T) {
	info := minio.ObjectInfo{IsDeleteMarker: true}
	err := minio.ErrorResponse{Code: "MethodNotAllowed", StatusCode: 405}
	state, ok := classifyHead(info, err)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_MethodNotAllowedWithoutMarkerFlag(t *testing.T) {
	// 防御性：minio-go 在 error 路径可能不填 IsDeleteMarker —— 405 + 版本存在
	// 的语义本身就是 marker 信号。详见 classifier.go 注释。
	info := minio.ObjectInfo{IsDeleteMarker: false}
	err := minio.ErrorResponse{Code: "MethodNotAllowed", StatusCode: 405}
	state, ok := classifyHead(info, err)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_InternalErrorUnclassified(t *testing.T) {
	err := minio.ErrorResponse{Code: "InternalError", StatusCode: 500}
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if ok {
		t.Errorf("got ok=true for InternalError, want false")
	}
	if state != "" {
		t.Errorf("got state=%q want empty", state)
	}
}

func TestClassifyHead_NonErrorResponseErrUnclassified(t *testing.T) {
	err := errors.New("transport boom")
	state, ok := classifyHead(minio.ObjectInfo{}, err)
	if ok {
		t.Errorf("got ok=true for non-ErrorResponse, want false")
	}
	_ = state
}
