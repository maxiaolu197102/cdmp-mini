/*
这个包是IAM项目的请求ID和日志中间件模块，提供了请求追踪和日志格式化功能。以下是包摘要：

包功能
请求追踪: 为每个请求生成唯一ID并传递

日志格式化: 提供自定义的日志格式器

工具函数: 提供从上下文和头部获取请求ID的函数

核心常量
XRequestIDKey
go
const XRequestIDKey = "X-Request-ID" // 请求ID的键名
主要组件
1. RequestID() 中间件
请求ID注入中间件：

检查传入的X-Request-ID头部，如果存在则使用

不存在时生成UUIDv4作为请求ID

设置到请求头部、响应头部和Gin上下文中

2. 日志配置函数
GetLoggerConfig()
创建Gin日志配置：

支持自定义日志格式器和输出目标

支持跳过特定路径的日志记录

默认使用带请求ID的日志格式器

GetDefaultLogFormatterWithRequestID()
返回默认的日志格式器，包含：

状态码颜色标记

请求方法颜色标记

客户端IP、延迟时间、路径等信息

特别注意: 当前实现实际上未包含请求ID输出（需要改进）

3. 工具函数
GetRequestIDFromContext()
从Gin上下文获取请求ID

GetRequestIDFromHeaders()
从请求头部获取请求ID

设计特点
请求追踪: 通过X-Request-ID实现全链路追踪

向后兼容: 支持使用客户端提供的请求ID

颜色支持: 日志输出支持颜色高亮

灵活配置: 可定制的日志格式和输出目标

使用场景
微服务架构中的请求追踪

问题排查和日志分析

性能监控和延迟测量

待改进点
当前日志格式器实际上未输出请求ID，需要调整格式字符串

可以考虑集成更完整的结构化日志输出

这个包为IAM项目提供了基础的请求追踪和日志记录能力，是分布式系统中不可或缺的可观测性组件。

开启新对话

*/

package common

import (
	"fmt"
	"io"
	"time"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
)

const (
	// XRequestIDKey defines X-Request-ID key string.
	XRequestIDKey = "X-Request-ID"
)

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader(XRequestIDKey)
		if rid == "" {
			rid = uuid.Must(uuid.NewV4()).String()
			c.Request.Header.Set(XRequestIDKey, rid)
			c.Set(XRequestIDKey, rid)
		}
		c.Writer.Header().Set(XRequestIDKey, rid)
		c.Next()
	}
}

// GetLoggerConfig return gin.LoggerConfig which will write the logs to specified io.Writer with given gin.LogFormatter.
// By default gin.DefaultWriter = os.Stdout
// reference: https://github.com/gin-gonic/gin#custom-log-format
func GetLoggerConfig(formatter gin.LogFormatter, output io.Writer, skipPaths []string) gin.LoggerConfig {
	if formatter == nil {
		formatter = GetDefaultLogFormatterWithRequestID()
	}

	return gin.LoggerConfig{
		Formatter: formatter,
		Output:    output,
		SkipPaths: skipPaths,
	}
}

// GetDefaultLogFormatterWithRequestID returns gin.LogFormatter with 'RequestID'.
func GetDefaultLogFormatterWithRequestID() gin.LogFormatter {
	return func(param gin.LogFormatterParams) string {
		var statusColor, methodColor, resetColor string
		if param.IsOutputColor() {
			statusColor = param.StatusCodeColor()
			methodColor = param.MethodColor()
			resetColor = param.ResetColor()
		}

		if param.Latency > time.Minute {
			// Truncate in a golang < 1.8 safe way
			param.Latency -= param.Latency % time.Second
		}

		return fmt.Sprintf("%s%3d%s - [%s] \"%v %s%s%s %s\" %s",
			// param.TimeStamp.Format("2006/01/02 - 15:04:05"),
			statusColor, param.StatusCode, resetColor,
			param.ClientIP,
			param.Latency,
			methodColor, param.Method, resetColor,
			param.Path,
			param.ErrorMessage,
		)
	}
}

// GetRequestIDFromContext returns 'RequestID' from the given context if present.
func GetRequestIDFromContext(c *gin.Context) string {
	if v, ok := c.Get(XRequestIDKey); ok {
		if requestID, ok := v.(string); ok {
			return requestID
		}
	}

	return ""
}

// GetRequestIDFromHeaders returns 'RequestID' from the headers if present.
func GetRequestIDFromHeaders(c *gin.Context) string {
	return c.Request.Header.Get(XRequestIDKey)
}
