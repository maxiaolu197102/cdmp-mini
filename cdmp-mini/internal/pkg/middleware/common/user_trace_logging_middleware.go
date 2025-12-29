package common

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	apiv1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/version"
)

// UserTraceConfig controls user trace logging behaviour.
type UserTraceConfig struct {
	Enabled         bool          //：是否启用用户追踪日志功能。
	ServiceName     string        //当前服务的名称
	Env             string        //当前服务运行的环境。
	PathPrefixes    []string      //需要追踪的请求路径前缀列表
	AwaitTimeout    time.Duration //等待追踪日志完成写入的超时时间。
	LogSampleRate   float64       //日志采样率（取值范围 0.0~1.0）
	ForceLogOnError bool          //发生错误时是否强制记录日志，忽略采样率。
	DisableLogging  bool          //是否禁用具体的日志输出（仅关闭日志打印，不影响追踪链路的其他功能）       //
}

// UserTraceLoggingMiddleware instruments user-related APIs with tracing and metrics.
func UserTraceLoggingMiddleware(cfg UserTraceConfig) gin.HandlerFunc {
	if len(cfg.PathPrefixes) == 0 {
		cfg.PathPrefixes = []string{"/v1/users"}
	}
	if cfg.AwaitTimeout <= 0 {
		cfg.AwaitTimeout = 30 * time.Second
	}

	hostname, _ := os.Hostname()
	buildInfo := version.Get()

	return func(c *gin.Context) {
		if c == nil {
			return
		}

		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}

		if !cfg.Enabled || !matchesPrefix(path, cfg.PathPrefixes) {
			c.Next()
			return
		}
		// 请求开始 - 增加并发计数
		metrics.HTTPMiddlewareStart()
		// 请求结束 - 减少并发计数
		defer metrics.HTTPMiddlewareEnd()

		requestID := ensureRequestID(c)
		operation := c.Request.Method + " " + path

		traceCtx, _ := trace.Start(
			c.Request.Context(),
			trace.Options{
				TraceID:         requestID,
				Service:         nonEmpty(cfg.ServiceName, "iam-apiserver"),
				Component:       "user-api",
				Operation:       operation,
				Phase:           trace.PhaseHTTP,
				Path:            path,
				Method:          c.Request.Method,
				ClientIP:        c.ClientIP(),
				RequestID:       requestID,
				AwaitTimeout:    cfg.AwaitTimeout,
				DisableLogging:  cfg.DisableLogging,
				LogSampleRate:   cfg.LogSampleRate,
				ForceLogOnError: cfg.ForceLogOnError,
			},
		)

		spanCtx, httpSpan := trace.StartSpan(traceCtx, "user-api", "http_request")
		if spanCtx != nil {
			traceCtx = spanCtx
		}

		trace.AddRequestTag(traceCtx, "hostname", hostname)
		trace.AddRequestTag(traceCtx, "env", cfg.Env)
		trace.AddRequestTag(traceCtx, "build_version", buildInfo.GitVersion)
		trace.AddRequestTag(traceCtx, "user_agent", c.Request.UserAgent())
		trace.AddRequestTag(traceCtx, "host", c.Request.Host)

		c.Request = c.Request.WithContext(traceCtx)

		start := time.Now()

		c.Next()

		duration := time.Since(start)
		statusCode := c.Writer.Status()
		trace.UpdateHTTPStatus(traceCtx, statusCode)
		if httpSpan != nil {
			status := "success"
			if statusCode >= http.StatusBadRequest {
				status = "error"
			}
			trace.EndSpan(httpSpan, status, strconv.Itoa(statusCode), map[string]interface{}{
				"method":       c.Request.Method,
				"path":         path,
				"status":       statusCode,
				"duration_ms":  duration.Milliseconds(),
				"content_type": c.ContentType(),
			})
		}

		operator := extractTraceUserID(c)
		if operator != "" {
			trace.SetOperator(traceCtx, operator)
		}

		statusStr := strconv.Itoa(statusCode)
		errorType := classifyStatus(statusCode)

		requestSize := c.Request.ContentLength
		if requestSize < 0 {
			requestSize = 0
		}
		responseSize := int64(c.Writer.Size())
		if responseSize < 0 {
			responseSize = 0
		}

		metrics.RecordHTTPRequest(
			path,
			c.Request.Method,
			statusStr,
			duration.Seconds(),
			requestSize,
			responseSize,
			c.ClientIP(),
			c.Request.UserAgent(),
			c.Request.Host,
			"",
			errorType,
			operator,
			"",
		)

		trace.Complete(traceCtx)
	}
}

func classifyStatus(status int) string {
	if status >= http.StatusInternalServerError {
		return "server_error"
	}
	if status >= http.StatusBadRequest {
		return "client_error"
	}
	return "success"
}

func ensureRequestID(c *gin.Context) string {
	requestID := GetRequestIDFromContext(c)
	if requestID == "" {
		requestID = GetRequestIDFromHeaders(c)
	}
	if requestID == "" {
		requestID = uuid.Must(uuid.NewV4()).String()
	}
	c.Set(XRequestIDKey, requestID)
	c.Request.Header.Set(XRequestIDKey, requestID)
	return requestID
}

func matchesPrefix(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func extractTraceUserID(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if name := c.GetString("username"); name != "" {
		return name
	}
	if current, exists := c.Get("current_user"); exists {
		switch v := current.(type) {
		case *apiv1.User:
			if v != nil {
				return v.Name
			}
		case interface{ GetName() string }:
			return v.GetName()
		case fmt.Stringer:
			return v.String()
		case string:
			return v
		default:
			return fmt.Sprint(v)
		}
	}
	if val := c.Param("name"); val != "" {
		return val
	}
	return ""
}

func nonEmpty(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}
