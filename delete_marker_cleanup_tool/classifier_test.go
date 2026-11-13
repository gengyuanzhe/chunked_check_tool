package main

import (
	"errors"
	"testing"

	"github.com/aws/smithy-go"
)

// fakeAPIError 实现 smithy.APIError 接口，便于测试错误路径。
type fakeAPIError struct{ code string }

func (e *fakeAPIError) Error() string       { return e.code }
func (e *fakeAPIError) ErrorCode() string  { return e.code }
func (e *fakeAPIError) ErrorMessage() string { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return 0 }

func TestClassifyHead_MarkerResult(t *testing.T) {
	r := HeadResult{IsDeleteMarker: true, StatusCode: 405}
	state, ok := classifyHead(r)
	if !ok || state != "marker" {
		t.Errorf("got (%q, %v) want (marker, true)", state, ok)
	}
}

func TestClassifyHead_RegularResult(t *testing.T) {
	r := HeadResult{IsDeleteMarker: false, StatusCode: 200}
	state, ok := classifyHead(r)
	if !ok || state != "not_marker" {
		t.Errorf("got (%q, %v) want (not_marker, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchVersionErr(t *testing.T) {
	r := HeadResult{Err: &fakeAPIError{code: "NoSuchVersion"}, StatusCode: 404}
	state, ok := classifyHead(r)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_NoSuchKeyErr(t *testing.T) {
	r := HeadResult{Err: &fakeAPIError{code: "NoSuchKey"}, StatusCode: 404}
	state, ok := classifyHead(r)
	if !ok || state != "absent" {
		t.Errorf("got (%q, %v) want (absent, true)", state, ok)
	}
}

func TestClassifyHead_InternalErrorUnclassified(t *testing.T) {
	r := HeadResult{Err: &fakeAPIError{code: "InternalError"}, StatusCode: 500}
	_, ok := classifyHead(r)
	if ok {
		t.Errorf("got ok=true for InternalError, want false")
	}
}

func TestClassifyHead_NonAPIErrorUnclassified(t *testing.T) {
	r := HeadResult{Err: errors.New("transport boom")}
	_, ok := classifyHead(r)
	if ok {
		t.Errorf("got ok=true for non-APIError, want false")
	}
}
