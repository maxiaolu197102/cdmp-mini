package common

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	HeaderCORSOrigin              = "Access-Control-Allow-Origin"
	HeaderFrameOptions            = "X-Frame-Options"
	HeaderContentTypeOptions      = "X-Content-Type-Options"
	HeaderXSSProtection           = "X-XSS-Protection"
	HeaderStrictTransportSecurity = "Strict-Transport-Security"
	HeaderContentSecurityPolicy   = "Content-Security-Policy"
)

func Secure(c *gin.Context) {
	c.Header(HeaderCORSOrigin, "*")
	c.Header(HeaderFrameOptions, "DENY")
	c.Header(HeaderContentTypeOptions, "nosniff")
	c.Header(HeaderXSSProtection, "1; mode=block")
	c.Header(HeaderContentSecurityPolicy, "script-src 'self'")
	if c.Request.TLS != nil {
		c.Header("Strict-Transport-Security", "max-age=31536000")
	}
}

func NoCache(c *gin.Context) {
	c.Header("Cache-Control", "no-store, no-cache, must-revalidate")
	c.Header("Pragma", "no-cache")
	c.Header("Expires", "0")
	c.Next()
}

// EmptyMiddleware 空中间件，什么也不做
func EmptyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
	}
}

// 基础中间件（所有环境共用）
var baseMiddlewares = map[string]gin.HandlerFunc{
	"recovery":  gin.Recovery(),
	"requestid": RequestID(),
	"context":   Context(),
	"secure":    Secure,
	"nocache":   NoCache,
}

// 开发环境特定中间件
var devMiddlewares = map[string]gin.HandlerFunc{
	"logger": devLogger(),
	"cors":   DevCors(),
	//"dump":   gindump.Dump(), // 开发环境使用真实的dump
}

// 测试环境特定中间件
var testMiddlewares = map[string]gin.HandlerFunc{
	"logger": testLogger(),
	"cors":   TestCors(),
	"dump":   EmptyMiddleware(), // 测试环境使用空中间件
}

// 生产环境特定中间件
var prodMiddlewares = map[string]gin.HandlerFunc{
	"logger": productionLogger(),
	"cors":   ProductionCors(),
	"dump":   EmptyMiddleware(), // 生产环境使用空中间件
}

// devLogger 开发环境日志（彩色输出）
func devLogger() gin.HandlerFunc {
	return LoggerWithConfig(gin.LoggerConfig{
		Formatter: defaultLogFormatter,
		Output:    gin.DefaultWriter,
		SkipPaths: []string{"/healthz"},
	})
}

// testLogger 测试环境日志（简洁格式）
func testLogger() gin.HandlerFunc {
	return LoggerWithConfig(gin.LoggerConfig{
		Formatter: func(param gin.LogFormatterParams) string {
			return fmt.Sprintf("TEST %s %s %s - %d (%s)\n",
				param.TimeStamp.Format("15:04:05"),
				param.Method,
				param.Path,
				param.StatusCode,
				param.Latency)
		},
		Output:    os.Stdout,
		SkipPaths: []string{"/healthz", "/metrics"},
	})
}

// productionLogger 生产环境日志（JSON格式）
func productionLogger() gin.HandlerFunc {
	file, err := os.OpenFile("/var/log/iam/app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		file = os.Stdout
	}

	return LoggerWithConfig(gin.LoggerConfig{
		Formatter: func(param gin.LogFormatterParams) string {
			logEntry := map[string]interface{}{
				"timestamp": param.TimeStamp.Format(time.RFC3339),
				"ip":        param.ClientIP,
				"method":    param.Method,
				"path":      param.Path,
				"status":    param.StatusCode,
				"latency":   param.Latency.Milliseconds(),
				"error":     param.ErrorMessage,
			}
			jsonData, _ := json.Marshal(logEntry)
			return string(jsonData) + "\n"
		},
		Output:    file,
		SkipPaths: []string{"/healthz", "/metrics", "/readyz"},
	})
}

// GetMiddlewares 获取当前环境的中间件
func GetMiddlewares(opt *options.Options) map[string]gin.HandlerFunc {
	middlewares := make(map[string]gin.HandlerFunc)
	for k, v := range baseMiddlewares {
		middlewares[k] = v
	}

	var envSpecific map[string]gin.HandlerFunc

	switch opt.ServerRunOptions.Env {
	case "test":
		envSpecific = testMiddlewares
	case "release":
		envSpecific = prodMiddlewares
	default:
		envSpecific = devMiddlewares
	}

	for k, v := range envSpecific {
		middlewares[k] = v
	}

	return middlewares
}

// GetMiddlewareStack 获取排序后的中间件切片
func GetMiddlewareStack(opt *options.Options) []gin.HandlerFunc {
	allMiddlewares := GetMiddlewares(opt)

	// 统一的执行顺序
	executionOrder := []string{
		"recovery", "secure", "cors", "metrics", "requestid", "context", "logger", "nocache", "dump",
	}

	var stack []gin.HandlerFunc
	for _, key := range executionOrder {
		if middleware, exists := allMiddlewares[key]; exists {

			if opt.ServerRunOptions.Env == "development" {
				log.Infof("安装中间件: %s (%T)", key, middleware)
			} else {
				log.Infof("安装中间件: %s", key)
			}
			stack = append(stack, middleware)
		}
	}
	log.Infof("环境: %s, 共安装 %d 个中间件", opt.ServerRunOptions.Env, len(stack))
	return stack
}
