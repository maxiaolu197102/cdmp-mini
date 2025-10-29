package common

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

// 定义上下文键类型（避免字符串键冲突）
type contextKey string

const (
	KeyRequestID contextKey = "request_id"
	KeyUsername  contextKey = "username"
)

const (
	UsernameKey = "username"
)

// Context 中间件 - 增强版
func Context() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 设置Gin上下文的值（保持向后兼容）
		requestID := c.GetString(XRequestIDKey)
		username := c.GetString(UsernameKey)

		c.Set(log.KeyRequestID, requestID)
		c.Set(log.KeyUsername, username)

		// 2. 增强：在标准context中设置值（用于Service层）
		ctx := c.Request.Context()
		if requestID != "" {
			ctx = context.WithValue(ctx, KeyRequestID, requestID)
		}
		if username != "" {
			ctx = context.WithValue(ctx, KeyUsername, username)
		}

		// 3. 更新请求的上下文
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// 辅助函数：从标准context中获取值
func GetRequestID(ctx context.Context) string {
	if val, ok := ctx.Value(KeyRequestID).(string); ok {
		return val
	}
	return "unknown"
}

func GetUsername(ctx context.Context) string {
	if val, ok := ctx.Value(KeyUsername).(string); ok {
		return val
	}
	return "unknown"
}
