/*
包摘要
该包（package core）提供了 HTTP 响应处理的核心功能，定义了统一的错误响应格式（ErrResponse）和响应写入函数（WriteResponse）。其主要作用是规范服务端与客户端之间的响应格式，特别是错误信息的标准化输出，确保前后端交互的一致性。
核心流程
WriteResponse 函数是核心，其处理流程如下：
判断是否有错误：若传入 err 不为空，则进入错误处理流程；否则直接返回成功响应。
错误解析：通过 errors.ParseCoderByErr 将错误解析为自定义的错误编码结构（errors.Coder），该结构包含业务错误码、用户可见消息和 HTTP 状态码。
错误响应构建：根据解析结果，生成 ErrResponse 结构体（包含错误码、消息和参考信息），并以对应的 HTTP 状态码返回给客户端。
成功响应处理：若没有错误，直接以 200 OK 状态码返回业务数据（data）。

*/

package core

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/nexuscore/errors" // 自定义错误处理包
	// 自定义日志包
)

// ErrResponse 定义了错误发生时的返回格式
// 若不存在参考文档，Reference 字段会被省略
// swagger:model  // 用于 Swagger 文档生成，标记该结构体为 API 模型
type ErrResponse struct {
	// Code 表示业务错误码（非 HTTP 状态码）
	Code int `json:"code"`

	// Message 包含错误的详细描述，适合对外暴露（用户可理解的信息）
	Message string `json:"message"`

	// Reference 指向解决该错误的参考文档（可选，不存在时会省略）
	Reference string `json:"reference,omitempty"`
	// Data 可选的附加数据字段，提供更多错误上下文（如调试信息）
	Data interface{} `json:"data,omitempty"`
}

// SuccessResponse 定义成功响应的标准格式
type SuccessResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func WriteResponse(c *gin.Context, err error, data interface{}) {
	if err != nil {
		// 错误处理保持不变
		coder := errors.ParseCoderByErr(err)
		c.JSON(coder.HTTPStatus(), ErrResponse{
			Code:      errors.GetCode(err),
			Message:   err.Error(),
			Reference: coder.Reference(),
			Data:      data,
		})
		return
	}

	// ✅ 根据请求动态生成成功消息
	statusCode := determineStatusCode(c.Request)
	statusCode = adjustStatusByPayload(statusCode, data)
	successMessage := getSuccessMessage(c.Request, data)

	// 构建统一成功响应
	successResp := SuccessResponse{
		Code:    100001, // 成功业务码
		Message: successMessage,
		Data:    data,
	}
	c.JSON(statusCode, successResp)
}

// ✅ 明确的状态码判断逻辑
func determineStatusCode(req *http.Request) int {
	path := req.URL.Path
	method := req.Method

	switch {
	case path == "/login" && method == http.MethodPost:
		return http.StatusOK

	case method == http.MethodPost:
		if strings.HasPrefix(path, "/v1/users") || strings.HasPrefix(path, "/api/users") {
			return http.StatusCreated
		}
		return http.StatusOK

	case method == http.MethodPut:
		if strings.HasPrefix(path, "/v1/users") || strings.HasPrefix(path, "/api/users") {
			return http.StatusOK
		}
		if isResourceUpdate(path) {
			return http.StatusOK
		}
		return http.StatusOK

	case method == http.MethodPatch:
		if strings.HasPrefix(path, "/v1/users") || strings.HasPrefix(path, "/api/users") {
			return http.StatusOK
		}
		return http.StatusOK

	case method == http.MethodDelete:
		return http.StatusOK

	case method == http.MethodGet:
		return http.StatusOK

	default:
		return http.StatusOK
	}
}

// ✅ 定义资源更新端点匹配逻辑
func isResourceUpdate(path string) bool {
	return strings.HasPrefix(path, "/v1/users/")
}

const (
	MsgUserCreated  = "用户创建成功"
	MsgUserDeleted  = "用户删除成功"
	MsgUserUpdated  = "用户更新成功"
	MsgLoginSuccess = "登录成功"
	// ... 其他成功消息
)

// 获取动态成功消息
func getSuccessMessage(req *http.Request, data interface{}) string {
	path := req.URL.Path
	method := req.Method

	// 基于路径和方法的动态消息
	switch {
	case path == "/login" && method == http.MethodPost:
		return MsgLoginSuccess

	case strings.HasPrefix(path, "/v1/users") && method == http.MethodPost:
		return MsgUserCreated

	case strings.HasPrefix(path, "/v1/users") && method == http.MethodDelete:
		// 如果是强制删除
		if strings.Contains(path, "/force") {
			return "用户强制删除成功"
		}
		return MsgUserDeleted

	case strings.HasPrefix(path, "/v1/users") && method == http.MethodPut:
		return MsgUserUpdated

	case strings.HasPrefix(path, "/v1/users") && method == http.MethodPatch:
		return "用户信息更新成功"

	case strings.HasPrefix(path, "/api/users") && method == http.MethodPut:
		return "用户信息更新成功"

	case strings.HasPrefix(path, "/api/users") && method == http.MethodPatch:
		return "用户信息更新成功"

	default:
		// 基于数据内容进一步判断
		if data != nil {
			return getMessageFromData(data)
		}
		return "操作成功"
	}
}

// 从数据中提取更精确的消息
func getMessageFromData(data interface{}) string {
	switch v := data.(type) {
	case map[string]interface{}:
		if operationType, ok := v["operation_type"].(string); ok {
			switch operationType {
			case "force_delete":
				return "用户强制删除成功"
			case "soft_delete":
				return "用户已禁用"
			case "restore":
				return "用户恢复成功"
			}
		}
	case gin.H:
		if operationType, ok := v["operation_type"].(string); ok {
			switch operationType {
			case "force_delete":
				return "用户强制删除成功"
			case "soft_delete":
				return "用户已禁用"
			case "restore":
				return "用户恢复成功"
			}
		}
	}
	return "操作成功"
}

func adjustStatusByPayload(base int, data interface{}) int {
	if base != http.StatusOK {
		return base
	}

	var op string
	switch v := data.(type) {
	case map[string]interface{}:
		if operation, ok := v["operation_type"].(string); ok {
			op = operation
		}
	case gin.H:
		if operation, ok := v["operation_type"].(string); ok {
			op = operation
		}
	}

	if op == "create" {
		return http.StatusCreated
	}

	return base
}
