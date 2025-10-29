// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package code

//go:generate codegen -type=int
//go:generate codegen -type=int -doc -output ../../../docs/guide/zh-CN/api/error_code_generated.md

// 通用基本错误（1000xx）：服务10 + 模块00 + 序号
const (
	// ErrSuccess - 200: 成功
	ErrSuccess int = iota + 100001 // 100001

	// ErrUnknown - 500: 内部服务器错误
	ErrUnknown // 100002

	// ErrBind - 400: 100003请求体绑定结构体失败
	ErrBind // 100003

	// ErrValidation - 400: 参数格式错误
	ErrValidation // 100004

	// ErrPageNotFound - 404: 页面不存在
	ErrPageNotFound // 100005

	//ErrMethodNotAllowed - 405 方法不存在
	ErrMethodNotAllowed //100006
	//ErrUnsupportedMediaType 415 不支持的Content-Type，仅支持application/json
	ErrUnsupportedMediaType //100007

	ErrContextCanceled //100008 408

)

// 通用数据库错误（1001xx）：服务10 + 模块01 + 序号
const (
	// ErrDatabase - 500: 数据库操作错误
	ErrDatabase int = iota + 100101 // 100101

	// ErrDatabaseTimeout - 500: 数据库超时
	ErrDatabaseTimeout  // 100102
	ErrDatabaseDeadlock //100103 数据库死锁
)

// 通用授权认证错误（1002xx）：服务10 + 模块02 + 序号
const (
	// ErrEncrypt - 401: 用户密码加密失败
	ErrEncrypt int = iota + 100201 // 100201

	// ErrSignatureInvalid - 401: 签名无效
	ErrSignatureInvalid // 100202

	// ErrExpired - 401: 令牌已过期
	ErrExpired // 100203

	// ErrInvalidAuthHeader - 401: 无效的授权头
	ErrInvalidAuthHeader // 100204

	// ErrMissingHeader - 401: Authorization头为空
	ErrMissingHeader // 100205

	// ErrPasswordIncorrect - 401: 密码不正确
	ErrPasswordIncorrect // 100206

	// ErrPermissionDenied - 403: 权限不足
	ErrPermissionDenied // 100207

	// ErrTokenInvalid - 401: 令牌无效
	ErrTokenInvalid // 100208（补充：归为授权错误更合理）

	//100209 ErrBase64DecodeFail - 400: Basic认证 payload Base64解码失败
	ErrBase64DecodeFail

	// 100210 ErrInvalidBasicPayload - 400: Basic认证 payload格式无效（缺少冒号分隔）
	ErrInvalidBasicPayload
	//100211 403 //令牌被撤销
	ErrRespCodeRTRevoked
	//100212
	ErrTokenMismatch
	//100213 429 登录请求过于频繁
	ErrAccountLocked
	//100214 503 系统繁忙
	ErrServerBusy
)

// 通用加解码错误（1003xx）：服务10 + 模块03 + 序号
const (
	// ErrEncodingFailed - 500: 数据编码失败
	ErrEncodingFailed int = iota + 100301 // 100301

	// ErrDecodingFailed - 500: 数据解码失败
	ErrDecodingFailed // 100302

	// ErrInvalidJSON - 500: 无效的JSON格式
	ErrInvalidJSON // 100303

	// ErrEncodingJSON - 500: JSON编码失败
	ErrEncodingJSON // 100304

	// ErrDecodingJSON - 500: JSON解码失败
	ErrDecodingJSON // 100305

	// ErrInvalidYaml - 500: 无效的Yaml格式
	ErrInvalidYaml // 100306

	// ErrEncodingYaml - 500: Yaml编码失败
	ErrEncodingYaml // 100307

	// ErrDecodingYaml - 500: Yaml解码失败
	ErrDecodingYaml // 100308
)

// kafka redis（1004xx）：服务10 + 模块04 + 序号
const (
	// ErrKafkaFailed - 500: 数据编码失败
	ErrKafkaFailed int = iota + 100401 // 100401
	ErrRedisFailed int = iota + 100401 // 100402
)
