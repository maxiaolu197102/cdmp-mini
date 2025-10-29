// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package code

//go:generate codegen -type=int

// iam-apiserver用户模块错误（1100xx）：服务11 + 模块00 + 序号
const (
	// ErrUserNotFound - 404: 用户不存在
	ErrUserNotFound int = iota + 110001 // 110001

	// ErrUserAlreadyExist - 400: 用户已存在
	ErrUserAlreadyExist // 110002

	// ErrUnauthorized - 401: 未授权访问用户资源
	ErrUnauthorized // 110003

	// ErrInvalidParameter - 400: 用户参数无效（如用户名为空）
	ErrInvalidParameter // 110004

	// ErrInternal - 500: 用户模块内部错误
	ErrInternal // 110005

	// ErrResourceConflict - 409: 用户资源冲突
	ErrResourceConflict // 110006

	//ErrInternalServer - 500: 用户模块服务器内部错误
	ErrInternalServer // 110007
	//ErrNotAdministrator - 403
	ErrNotAdministrator // 110008 //不是管理员
	//ErrNotAdministrator - 403
	ErrUserDisabled //110009 //用户已经失效
	// ErrRateLimitExceeded - 429: Too many requests.
	ErrRateLimitExceeded // 110010
)

// iam-apiserver密钥模块错误（1101xx）：服务11 + 模块01 + 序号
const (
	// ErrReachMaxCount - 400: 密钥数量达到上限
	ErrReachMaxCount  int = iota + 110101 // 110101	// ErrSecretNotFound - 404: 密钥不存在
	ErrSecretNotFound                     // 110102
)

// iam-apiserver策略模块错误（1102xx）：服务11 + 模块02 + 序号
const (
	// ErrPolicyNotFound - 404: 策略不存在
	ErrPolicyNotFound int = iota + 110201 // 110201
)
