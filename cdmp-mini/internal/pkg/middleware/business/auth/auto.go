package auth

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	middleware "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

const authHeaderCount = 2

type AutoStrategy struct {
	basicStrategy middleware.AuthStrategy
	jwtStrategy   middleware.AuthStrategy
}

func NewAutoStrategy(basic, jwt middleware.AuthStrategy) AutoStrategy {
	return AutoStrategy{
		basicStrategy: basic,
		jwtStrategy:   jwt,
	}
}

// AuthFunc 生成 Gin 中间件函数（核心认证逻辑）
func (a AutoStrategy) AuthFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		// login 白名单直接放行
		if c.FullPath() == "/login" {
			c.Next()
			return
		}

		ctx := c.Request.Context()
		parseCtx, parseSpan := trace.StartSpan(ctx, "auth-middleware", "parse_token")
		parseStart := time.Now()
		var parseErr error
		finished := false
		finishParse := func() {
			if parseSpan == nil || finished {
				return
			}
			finished = true
			dur := time.Since(parseStart)
			status := "success"
			codeStr := strconv.Itoa(code.ErrSuccess)
			if parseErr != nil {
				status = "error"
				if c := errors.GetCode(parseErr); c != 0 {
					codeStr = strconv.Itoa(c)
				} else {
					codeStr = strconv.Itoa(code.ErrUnknown)
				}
			}

			shouldRecord := shouldRecordAuthSpan(parseErr, dur, time.Millisecond, 0.1)
			endAt := parseStart.Add(dur)
			if shouldRecord {
				// 精确设置结束时间，避免 defer 延迟导致跨度覆盖后续逻辑
				trace.EndSpanAt(parseSpan, endAt, status, codeStr, map[string]interface{}{
					"path":        c.FullPath(),
					"method":      c.Request.Method,
					"duration_ms": dur.Milliseconds(),
				})
			} else {
				// 即便不采样，也要封口 span，避免后续 defer 二次覆盖
				parseSpan.EndTime = endAt
				parseSpan.DurationMs = float64(dur) / float64(time.Millisecond)
				parseSpan.Status = status
				parseSpan.BusinessCode = codeStr
			}
			parseSpan = nil
		}
		defer finishParse()

		operator := middleware.AuthOperator{}
		c.Set("AuthOperator", &operator)

		authHeader := c.Request.Header.Get("Authorization")
		if authHeader == "" {
			parseErr = errors.WithCode(
				code.ErrMissingHeader,
				"缺少Authorization头，支持格式：\n1. Basic认证：Authorization: Basic {base64(username:password)}\n2. JWT认证：Authorization: Bearer {jwt-token}",
			)
			core.WriteResponse(c, parseErr, nil)
			c.Abort()
			return
		}

		authParts := strings.SplitN(authHeader, " ", authHeaderCount)
		if len(authParts) != authHeaderCount || strings.TrimSpace(authParts[0]) == "" || strings.TrimSpace(authParts[1]) == "" {
			parseErr = errors.WithCode(
				code.ErrInvalidAuthHeader,
				"授权头格式无效，正确格式：\n- Basic认证：Authorization: Basic {base64编码的用户名:密码}\n- JWT认证：Authorization: Bearer {完整JWT令牌}",
			)
			core.WriteResponse(c, parseErr, nil)
			c.Abort()
			return
		}

		authScheme := strings.TrimSpace(authParts[0])
		switch authScheme {
		case "Basic":
			operator.SetAuthStrategy(a.basicStrategy)
		case "Bearer":
			operator.SetAuthStrategy(a.jwtStrategy)
		default:
			parseErr = errors.WithCode(
				code.ErrTokenInvalid,
				"未识别的认证方式，仅支持两种：\n1. Basic认证（用户名密码）\n2. Bearer认证（JWT令牌）",
			)
			core.WriteResponse(c, parseErr, nil)
			c.Abort()
			return
		}

		authFunc := operator.AuthFunc()
		if authFunc == nil {
			parseErr = errors.WithCode(
				code.ErrInternalServer,
				"认证策略初始化异常，无法执行认证",
			)
			core.WriteResponse(c, parseErr, nil)
			c.Abort()
			return
		}
		parseErr = nil
		finishParse()

		authFunc(c)
		if c.IsAborted() {
			return
		}

		username := operator.GetUsername()
		if username == "" {
			err := errors.WithCode(
				code.ErrUnauthorized,
				"认证流程异常：未获取到用户名",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		c.Set(common.UsernameKey, username)
		newCtx := context.WithValue(parseCtx, common.KeyUsername, username)
		c.Request = c.Request.WithContext(newCtx)

		c.Next()
	}
}

func shouldRecordAuthSpan(err error, dur time.Duration, slowThreshold time.Duration, sampleRate float64) bool {
	if err != nil {
		return true
	}
	if dur >= slowThreshold {
		return true
	}
	return rand.Float64() < sampleRate
}
