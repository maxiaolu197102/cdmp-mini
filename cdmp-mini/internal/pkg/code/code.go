// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package code

import (
	"fmt"
	"net/http"

	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// ErrCode implements `github.com/marmotedu/errors`.Coder interface.
type ErrCode struct {
	// C refers to the code of the ErrCode.
	C int

	// HTTP status that should be used for the associated error code.
	HTTP int

	// External (user) facing error text.
	Ext string

	// Ref specify the reference document.
	Ref string
}

var _ errors.Coder = &ErrCode{}

// Code returns the integer code of ErrCode.
func (coder ErrCode) Code() int {
	return coder.C
}

// String implements stringer. String returns the external error message,
// if any.
func (coder ErrCode) String() string {
	return coder.Ext
}

// Reference returns the reference document.
func (coder ErrCode) Reference() string {
	return coder.Ref
}

// HTTPStatus returns the associated HTTP status code, if any. Otherwise,
// returns 200.
func (coder ErrCode) HTTPStatus() int {
	if coder.HTTP == 0 {
		return http.StatusInternalServerError
	}

	return coder.HTTP
}

// Coder defines an interface for an error code detail information.
type Coder interface {
	// HTTP status that should be used for the associated error code.
	HTTPStatus() int

	// External (user) facing error text.
	String() string

	// Reference returns the reference document.
	Reference() string

	// Code returns the integer code of the coder
	Code() int
}

// register 按 HTTP 通用规则注册错误码，支持所有合法 HTTP 状态码（100~599）
func register(code int, httpStatus int, message string, refs ...string) {
	// 1. 校验 HTTP 状态码是否符合通用规则（100~599 是标准 HTTP 状态码区间）
	if httpStatus < 100 || httpStatus > 599 {
		panic(fmt.Sprintf("HTTP 状态码 %d 不符合通用规则（必须在 100~599 之间）", httpStatus))
	}

	// // 2. 可选：对常见状态码做语义提示（避免误用，如 404 用于非资源不存在场景）
	// switch httpStatus {
	// case 404:
	// 	if !strings.Contains(message, "不存在") && !strings.Contains(message, "未找到") {
	// 		fmt.Printf("[WARN] HTTP 404 建议用于「资源不存在」场景，当前描述：%s\n", message)
	// 	}
	// case 409:
	// 	if !strings.Contains(message, "冲突") && !strings.Contains(message, "已存在") {
	// 		fmt.Printf("[WARN] HTTP 409 建议用于「资源冲突」场景，当前描述：%s\n", message)
	// 	}
	// }

	// 3. 处理参考文档（保持原有逻辑）
	var reference string
	if len(refs) > 0 {
		reference = refs[0]
	}

	// 4. 注册错误码（保持原有逻辑）
	coder := &ErrCode{
		C:    code,
		HTTP: httpStatus,
		Ext:  message,
		Ref:  reference,
	}
	errors.MustRegister(coder)
}
