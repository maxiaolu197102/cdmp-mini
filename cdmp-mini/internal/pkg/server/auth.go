package server

import (
	"context"
	"encoding/base64"
	stdErrors "errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	gojwt "github.com/golang-jwt/jwt/v4"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	authkeys "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/auth/keys"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"

	middleware "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business/auth"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	_ "github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator/jwtvalidator"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"

	"github.com/spf13/viper"
	"golang.org/x/crypto/bcrypt"
)

type loginInfo struct {
	Username string `form:"username" json:"username" ` // 仅校验非空
	Password string `form:"password" json:"password" ` // 仅校验非空
}

const (
	ctxLoginFailCountKey  = "auth.login_fail_count"
	ctxLoginFailTTLKey    = "auth.login_fail_ttl"
	ctxLoginFailMaxKey    = "auth.login_fail_max"
	ctxLoginFailStatusKey = "auth.login_fail_status"
)

const (
	loginFailStatusIncremented = "incremented"
	loginFailStatusLocked      = "locked"
	loginFailStatusReset       = "reset"
)

func (g *GenericAPIServer) auditLoginAttempt(c *gin.Context, username, outcome string, err error) {
	if g == nil || c == nil {
		return
	}
	event := audit.BuildEventFromRequest(c.Request)
	if username != "" {
		event.Actor = username
		event.ResourceID = username
	}
	event.Action = "auth.login"
	event.ResourceType = "auth"
	event.Outcome = outcome
	if err != nil {
		event.ErrorMessage = err.Error()
	}
	if event.Metadata == nil {
		event.Metadata = map[string]any{}
	}
	if route := c.FullPath(); route != "" {
		event.Metadata["route"] = route
	}
	if maxVal, ok := c.Get(ctxLoginFailMaxKey); ok {
		event.Metadata["login_fail_limit"] = maxVal
	}
	if countVal, ok := c.Get(ctxLoginFailCountKey); ok {
		event.Metadata["login_fail_count"] = countVal
	}
	if ttlVal, ok := c.Get(ctxLoginFailTTLKey); ok {
		switch v := ttlVal.(type) {
		case time.Duration:
			if v > 0 {
				event.Metadata["login_fail_ttl_seconds"] = int(math.Ceil(v.Seconds()))
			}
		case int:
			if v > 0 {
				event.Metadata["login_fail_ttl_seconds"] = v
			}
		case int64:
			if v > 0 {
				event.Metadata["login_fail_ttl_seconds"] = int(v)
			}
		case float64:
			if v > 0 {
				event.Metadata["login_fail_ttl_seconds"] = int(math.Ceil(v))
			}
		}
	}
	if statusVal, ok := c.Get(ctxLoginFailStatusKey); ok {
		event.Metadata["login_fail_status"] = statusVal
	}
	g.submitAuditEvent(c.Request.Context(), event)
}

func (g *GenericAPIServer) auditLogoutEvent(c *gin.Context, username, outcome, reason string) {
	if g == nil || c == nil {
		return
	}
	event := audit.BuildEventFromRequest(c.Request)
	if username != "" {
		event.Actor = username
		event.ResourceID = username
	}
	event.Action = "auth.logout"
	event.ResourceType = "auth"
	event.Outcome = outcome
	if reason != "" {
		event.ErrorMessage = reason
	}
	if event.Metadata == nil {
		event.Metadata = map[string]any{}
	}
	if route := c.FullPath(); route != "" {
		event.Metadata["route"] = route
	}
	g.submitAuditEvent(c.Request.Context(), event)
}

func (g *GenericAPIServer) auditRefreshEvent(c *gin.Context, username, outcome string, err error, metadata map[string]any) {
	if g == nil || c == nil {
		return
	}
	event := audit.BuildEventFromRequest(c.Request)
	if username != "" {
		event.Actor = username
		event.ResourceID = username
	}
	event.Action = "auth.refresh"
	event.ResourceType = "auth"
	event.Outcome = outcome
	if err != nil {
		event.ErrorMessage = err.Error()
	}
	if event.Metadata == nil {
		event.Metadata = map[string]any{}
	}
	for k, v := range metadata {
		event.Metadata[k] = v
	}
	submitAuditFromGinContext(c, event)
}

func (g *GenericAPIServer) refreshError(c *gin.Context, username string, err error, metadata map[string]any) {
	if c == nil || err == nil {
		return
	}
	recordErrorToContext(c, err)
	g.auditRefreshEvent(c, username, "fail", err, metadata)
	core.WriteResponse(c, err, nil)
}

func (g *GenericAPIServer) loginFailLimit() int {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return DefaultMaxLoginFails
	}
	limit := g.options.ServerRunOptions.MaxLoginFailures
	if limit <= 0 {
		return DefaultMaxLoginFails
	}
	return limit
}

func (g *GenericAPIServer) loginFailWindow() time.Duration {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return DefaultLoginFailReset
	}
	window := g.options.ServerRunOptions.LoginFailReset
	if window <= 0 {
		return DefaultLoginFailReset
	}
	return window
}

func (g *GenericAPIServer) loginFailTTL(ctx context.Context, username string) time.Duration {
	if g == nil || g.redis == nil {
		return 0
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return 0
	}
	ttlSec, err := g.redis.GetKeyTTL(ctx, authkeys.LoginFailKey(username))
	if err != nil || ttlSec <= 0 {
		return 0
	}
	return time.Duration(ttlSec) * time.Second
}

func formatRetryAfter(d time.Duration) string {
	if d <= 0 {
		return "稍后"
	}
	if d >= time.Minute {
		minutes := int(math.Ceil(d.Minutes()))
		if minutes <= 0 {
			minutes = 1
		}
		return strconv.Itoa(minutes) + "分钟"
	}
	seconds := int(math.Ceil(d.Seconds()))
	if seconds <= 0 {
		seconds = 1
	}
	return strconv.Itoa(seconds) + "秒"
}

func buildUnlockNextStep(ttl time.Duration) string {
	minutes := int(math.Ceil(ttl.Minutes()))
	if minutes <= 0 {
		minutes = 1
	}
	var builder strings.Builder
	builder.WriteString("请")
	builder.WriteString(strconv.Itoa(minutes))
	builder.WriteString("分钟后重试，或联系管理员解锁")
	return builder.String()
}

func (g *GenericAPIServer) annotateLoginAttemptMetrics(c *gin.Context, count int, ttl time.Duration, status string) {
	if c == nil {
		return
	}
	if limit := g.loginFailLimit(); limit > 0 {
		c.Set(ctxLoginFailMaxKey, limit)
	}
	c.Set(ctxLoginFailCountKey, count)
	if ttl > 0 {
		c.Set(ctxLoginFailTTLKey, ttl)
	} else {
		c.Set(ctxLoginFailTTLKey, time.Duration(0))
	}
	if status != "" {
		c.Set(ctxLoginFailStatusKey, status)
	}
}

func (g *GenericAPIServer) recordLoginFailure(c *gin.Context, username string) (int, time.Duration, error) {
	if g == nil || c == nil || g.redis == nil {
		return 0, 0, fmt.Errorf("redis 未初始化")
	}
	userID := strings.TrimSpace(username)
	if userID == "" {
		return 0, 0, fmt.Errorf("用户名为空")
	}
	if !g.checkRedisAlive() {
		return 0, 0, fmt.Errorf("redis 不可用")
	}
	window := g.loginFailWindow()
	expireSeconds := int64(math.Ceil(window.Seconds()))
	if expireSeconds <= 0 {
		expireSeconds = 1
	}
	key := authkeys.LoginFailKey(userID)
	count := g.redis.IncrememntWithExpire(c.Request.Context(), key, expireSeconds)
	ttlSec, err := g.redis.GetKeyTTL(c.Request.Context(), key)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Warnf("获取登录失败计数TTL失败: username=%s, err=%v", username, err)
	}
	var ttl time.Duration
	if ttlSec > 0 {
		ttl = time.Duration(ttlSec) * time.Second
	}
	return int(count), ttl, nil
}

func (g *GenericAPIServer) shouldCountLoginFailure(err error) bool {
	if err == nil {
		return false
	}
	if !errors.IsWithCode(err) {
		return false
	}
	switch errors.GetCode(err) {
	case code.ErrInvalidParameter, code.ErrPasswordIncorrect, code.ErrUserNotFound, code.ErrUserDisabled:
		return true
	default:
		return false
	}
}

func (g *GenericAPIServer) maybeRecordLoginFailure(c *gin.Context, username string, err error) {
	if !g.shouldCountLoginFailure(err) {
		return
	}
	count, ttl, recErr := g.recordLoginFailure(c, username)
	if recErr != nil {
		log.Warnf("记录登录失败计数失败: username=%s, err=%v", username, recErr)
	}
	if ttl <= 0 {
		ttl = g.loginFailTTL(c.Request.Context(), username)
	}
	status := loginFailStatusIncremented
	if limit := g.loginFailLimit(); limit > 0 && count >= limit {
		status = loginFailStatusLocked
	}
	g.annotateLoginAttemptMetrics(c, count, ttl, status)
}

func submitAuditFromGinContext(c *gin.Context, event audit.Event) {
	if c == nil {
		return
	}
	if event.Metadata == nil {
		event.Metadata = map[string]any{}
	}
	if route := c.FullPath(); route != "" {
		event.Metadata["route"] = route
	}
	if mgr := audit.FromGinContext(c); mgr != nil {
		mgr.Submit(c.Request.Context(), event)
	}
}

// 认证策略工厂
func (g *GenericAPIServer) newBasicAuth() middleware.AuthStrategy {
	return auth.NewBasicStrategy(func(username string, password string) bool {
		start := time.Now()

		ctx := context.Background()
		var (
			user *v1.User
			err  error
		)

		if g.userService != nil {
			user, err = g.userService.Get(ctx, username, metav1.GetOptions{}, g.options)
		} else {
			user, err = interfaces.Client().Users().Get(ctx, username, metav1.GetOptions{}, g.options)
		}
		if err != nil || user == nil {
			elapsed := time.Since(start)
			targetDelay := 150 * time.Millisecond
			if elapsed < targetDelay {
				time.Sleep(targetDelay - elapsed)
			}
			return false
		}

		if user.Status == 0 {
			log.Warnf("disabled user attempted basic auth: username=%s", username)
			return false
		}

		matched, cmpErr := g.verifyPassword(user, password)
		if cmpErr != nil || !matched {
			return false
		}

		user.LoginedAt = time.Now()
		g.asyncUpdateLoginTime(user)
		return true
	})
}

func (g *GenericAPIServer) newJWTAuth() (middleware.AuthStrategy, error) {
	//基础配置：初始化核心参数
	realm := viper.GetString("jwt.realm")
	signingAlgorithm := "HS256"
	key := []byte(viper.GetString("jwt.key"))
	timeout := viper.GetDuration("jwt.timeout")
	maxRefresh := viper.GetDuration("jwt.max-refresh")
	identityKey := common.UsernameKey
	// 令牌解析配置：定义令牌的获取位置和格式
	tokenLoopup := "header: Authorization, query: token, cookie: jwt"
	tokenHeadName := "Bearer"

	ginjwt, err := jwt.New(&jwt.GinJWTMiddleware{
		Realm:            realm,
		SigningAlgorithm: signingAlgorithm,
		Key:              key,
		Timeout:          timeout,
		MaxRefresh:       maxRefresh,
		IdentityKey:      identityKey,
		TokenLookup:      tokenLoopup,
		TokenHeadName:    tokenHeadName,
		SendCookie:       true,
		TimeFunc:         time.Now,
		//login
		Authenticator: func(c *gin.Context) (interface{}, error) {
			return g.authenticate(c)
		},
		//生成访问令牌
		PayloadFunc: func(data interface{}) jwt.MapClaims {
			return g.generateAccessTokenClaims(data)
		},
		IdentityHandler: func(ctx *gin.Context) interface{} {
			return g.identityHandler(ctx)
		},
		Authorizator:          g.authorizator(),
		HTTPStatusMessageFunc: errors.HTTPStatusMessageFunc,
		LoginResponse: func(c *gin.Context, statusCode int, token string, expire time.Time) {
			g.loginResponse(c, token, expire)
		},
		Unauthorized: handleUnauthorized,
	})
	if err != nil {
		return nil, fmt.Errorf("建立 JWT middleware 失败: %w", err)
	}
	return auth.NewJWTStrategy(*ginjwt), nil
}

func (g *GenericAPIServer) identityHandler(c *gin.Context) interface{} {
	{
		originalUserVal, originalExists := c.Get("username")
		if originalExists && originalUserVal != nil {
			if originalUser, ok := originalUserVal.(*v1.User); ok {
				// 验证原始用户的核心字段非空（避免空结构体）
				if originalUser.Name != "" && originalUser.InstanceID != "" {
					// 检查黑名单（原有逻辑保留）
					claims := jwt.ExtractClaims(c)
					userID := getUserIDString(claims["user_id"])
					jti, ok := claims["jti"].(string)
					if ok && jti != "" {
						isBlacklisted, err := isTokenInBlacklist(g, c, userID, jti)
						if err != nil {
							log.Errorf("IdentityHandler: 检查黑名单失败，error=%v", err)
							return nil
						}
						if isBlacklisted {
							log.Warnf("IdentityHandler: 令牌在黑名单，jti=%s", jti)
							return nil
						}
					}

					// 复用原始 *v1.User，不覆盖为空

					c.Set("username", originalUser) // 显式确认存储类型
					return originalUser
				}
			}
		}

		claims := jwt.ExtractClaims(c)
		userID := getUserIDString(claims["user_id"])
		//优先从 jwt.IdentityKey 提取（与 payload 对应）
		username, ok := claims[jwt.IdentityKey].(string)

		//若失败，从 sub 字段提取（payload 中同步存储了该字段）
		if !ok || username == "" {
			username, ok = claims["sub"].(string)
			if !ok || username == "" {
				return nil
			}
		}
		// 检查令牌是否在黑名单中（带Redis容错）
		jti, ok := claims["jti"].(string)
		if !ok {
			log.Warn("从claims获取jti失败")
		}

		if ok && jti != "" {
			isBlacklisted, err := isTokenInBlacklist(g, c, userID, jti)
			if err != nil {
				return errors.New("安全服务不可用,拒绝服务")
			}
			if isBlacklisted {
				log.Warnf("令牌以已经被注销,jti=%s", jti)
				return nil
			}
		}

		if sessionID, _ := claims["session_id"].(string); sessionID != "" {
			revoked, err := g.isSessionRevoked(c, userID, sessionID)
			if err != nil {
				log.Warnf("校验session撤销状态失败: user_id=%s session_id=%s err=%v", userID, sessionID, err)
			} else if revoked {
				revErr := errors.WithCode(code.ErrRespCodeRTRevoked, "令牌已经撤销")
				recordErrorToContext(c, revErr)
				return nil
			}
		}

		revokedByPassword, err := g.isTokenRevokedByPasswordChange(c, userID, claims)
		if err != nil {
			log.Warnf("校验密码版本标记失败: user_id=%s err=%v", userID, err)
		} else if revokedByPassword {
			revErr := errors.WithCode(code.ErrRespCodeRTRevoked, "令牌已经撤销")
			recordErrorToContext(c, revErr)
			return nil
		}

		//后续：设置到 AuthOperator 和上下文（保持之前的逻辑）
		operatorVal, exists := c.Get("AuthOperator")
		if exists {
			if operator, ok := operatorVal.(*middleware.AuthOperator); ok {
				operator.SetUsername(username)
			}
		}

		c.Set(common.UsernameKey, username)
		ctx := context.WithValue(c.Request.Context(), common.KeyUsername, username)
		c.Request = c.Request.WithContext(ctx)

		return username
	}

}

// authoricator 认证逻辑：返回用户信息或具体错误
func (g *GenericAPIServer) authenticate(c *gin.Context) (interface{}, error) {
	threshold := 0
	if g != nil && g.options != nil && g.options.ServerRunOptions != nil {
		threshold = g.options.ServerRunOptions.LoginFastFailThreshold
	}
	if threshold > 0 {
		current := g.loginInFlight.Add(1)
		if current > int64(threshold) {
			g.loginInFlight.Add(-1)
			message := "系统繁忙，请稍后再试"
			if g.options != nil && g.options.ServerRunOptions != nil && g.options.ServerRunOptions.LoginFastFailMessage != "" {
				message = g.options.ServerRunOptions.LoginFastFailMessage
			}
			err := errors.WithCode(code.ErrServerBusy, "%s", message)
			g.auditLoginAttempt(c, "", "busy", err)
			recordErrorToContext(c, err)
			return nil, err
		}
		defer g.loginInFlight.Add(-1)
	}

	var login loginInfo
	var err error

	if authHeader := c.Request.Header.Get("Authorization"); authHeader != "" {
		login, err = parseWithHeader(c) // 之前已修复：返回 Basic 认证相关错误码（如 ErrInvalidAuthHeader）
	} else {
		login, err = parseWithBody(c) // 同理：返回 Body 解析相关错误码（如 ErrInvalidParameter）
	}
	if err != nil {
		log.Errorf("parse authentication info failed: %v", err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}

	limit := g.loginFailLimit()
	if limit > 0 {
		c.Set(ctxLoginFailMaxKey, limit)
	}

	failCount, err := g.getLoginFailCount(c, login.Username)
	if err != nil {
		log.Warnf("查询登录失败计数失败: username=%s, err=%v", login.Username, err)
	}

	if limit > 0 && failCount >= limit {
		retryAfter := g.loginFailTTL(c.Request.Context(), login.Username)
		if retryAfter <= 0 {
			retryAfter = g.loginFailWindow()
		}
		g.annotateLoginAttemptMetrics(c, failCount, retryAfter, loginFailStatusLocked)
		err := errors.WithCode(code.ErrAccountLocked, "登录失败次数太多,%s后重试", formatRetryAfter(retryAfter))
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}
	//检查用户名是否合法
	if errs := validation.IsQualifiedName(login.Username); len(errs) > 0 {
		errsMsg := strings.Join(errs, ":")
		log.Warnw("用户名不合法:", errsMsg)
		err := errors.WithCode(code.ErrInvalidParameter, "%s", errsMsg)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err

	}
	//检查密码是否有效
	if err := validation.IsValidPassword(login.Password); err != nil {
		errMsg := "密码不合法：" + err.Error()
		err := errors.WithCode(code.ErrInvalidParameter, "%s", errMsg)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}

	requestCtx := c.Request.Context()
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	var user *v1.User
	if g.userService != nil {
		user, err = g.userService.Get(requestCtx, login.Username, metav1.GetOptions{}, g.options)
	} else {
		user, err = interfaces.Client().Users().Get(requestCtx, login.Username, metav1.GetOptions{}, g.options)
	}
	if err != nil {
		log.Errorf("获取用户信息失败: username=%s, error=%v", login.Username, err)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}
	if user == nil || !strings.EqualFold(user.Name, login.Username) {
		err := errors.WithCode(code.ErrUserNotFound, "用户不存在")
		log.Warnf("用户不存在: username=%s", login.Username)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}
	if user.Status == 0 {
		err := errors.WithCode(code.ErrUserDisabled, "用户已经失效")
		log.Warnf("disabled user login attempt blocked: username=%s", login.Username)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}

	matched, cmpErr := g.verifyPassword(user, login.Password)
	if cmpErr != nil {
		log.Errorf("password verification error: username=%s, err=%v", login.Username, cmpErr)
		err := errors.WithCode(code.ErrInternalServer, "密码校验出现异常，请稍后再试")
		g.auditLoginAttempt(c, login.Username, "error", err)
		recordErrorToContext(c, err)
		return nil, err
	}
	if !matched {
		log.Errorf("password compare failed: username=%s", login.Username)
		err := errors.WithCode(code.ErrPasswordIncorrect, "密码校验失败：用户名【%s】的密码不正确", login.Username)
		g.maybeRecordLoginFailure(c, login.Username, err)
		g.auditLoginAttempt(c, login.Username, "fail", err)
		recordErrorToContext(c, err)
		return nil, err
	}
	if err := g.restLoginFailCount(c.Request.Context(), login.Username); err != nil {
		log.Errorf("重置登录次数失败:username=%s,error=%v", login.Username, err)
	} else {
		g.annotateLoginAttemptMetrics(c, 0, 0, loginFailStatusReset)
	}
	//设置user
	c.Set("current_user", user)
	user.LoginedAt = time.Now()
	g.asyncUpdateLoginTime(user)

	g.auditLoginAttempt(c, login.Username, "success", nil)

	return user, nil
}

func (g *GenericAPIServer) logoutRespons(c *gin.Context) {
	// 获取请求头中的令牌（带Bearer前缀）
	token := c.GetHeader("Authorization")
	claims, err := jwtvalidator.ValidateToken(token, g.options.JwtOptions.Key)

	g.clearAuthCookies(c)

	username := ""

	if err != nil {
		// 降级处理：只清理客户端Cookie
		g.auditLogoutEvent(c, username, "fail", err.Error())

		if !errors.IsWithCode(err) {
			// 非预期错误类型，返回默认未授权
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    code.ErrUnauthorized,
				"message": "令牌校验失败",
			})
			return
		}
		bid := errors.GetCode(err)
		// 2.2 无令牌或令牌过期，友好返回已登出
		if bid == code.ErrMissingHeader {
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    code.ErrMissingHeader,
				"message": "请先登录",
			})
			return
		}
		if bid == code.ErrExpired {
			g.auditLogoutEvent(c, username, "timeout", "令牌已经过期")
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    code.ErrExpired,
				"message": "令牌已经过期,请重新登录",
			})
			return
		}
		message := errors.GetMessage(err)
		// 2.3 其他错误（如签名无效）返回具体业务码
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    bid,
			"message": message,
		})
		return
	}
	if claims != nil {
		username = claims.Username
	}

	//异步或后台执行可能失败的操作（黑名单、会话清理）
	go g.executeBackgroundCleanup(claims)

	// 4. 登出成功响应

	// 🔧 优化4：成功场景也通过core.WriteResponse，确保格式统一（code=成功码，message=成功消息）
	core.WriteResponse(c, nil, "登出成功")
	g.auditLogoutEvent(c, username, "success", "")
}

func (g *GenericAPIServer) debugTokenOverride(c *gin.Context) (time.Duration, bool) {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return 0, false
	}
	if !g.isDebugMode() {
		return 0, false
	}
	raw := strings.TrimSpace(c.GetHeader("X-Debug-Token-Timeout"))
	if raw == "" {
		return 0, false
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil || ttl <= 0 {
		log.Warnf("X-Debug-Token-Timeout 无效: %s", raw)
		return 0, false
	}
	if ttl < time.Second {
		ttl = time.Second
	}
	// 防止过长，限制在 24h 内
	if ttl > 24*time.Hour {
		ttl = 24 * time.Hour
	}
	return ttl, true
}

func (g *GenericAPIServer) asyncUpdateLoginTime(user *v1.User) {
	if g == nil || user == nil {
		return
	}
	update := *user
	if g.loginUpdates == nil {
		go g.directLoginUpdate(update)
		return
	}
	select {
	case g.loginUpdates <- &update:
		return
	default:
		log.Warnf("login update queue saturated, fallback to direct update: username=%s", update.Name)
		go g.directLoginUpdate(update)
	}
}

func (g *GenericAPIServer) directLoginUpdate(user v1.User) {
	if g == nil || user.Name == "" {
		return
	}
	timeout := 2 * time.Second
	if g.options != nil && g.options.ServerRunOptions != nil && g.options.ServerRunOptions.LoginUpdateTimeout > 0 {
		timeout = g.options.ServerRunOptions.LoginUpdateTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := interfaces.Client().Users().Update(ctx, &user, metav1.UpdateOptions{}, g.options); err != nil {
		log.Warnf("direct update loginedAt failed: username=%s, err=%v", user.Name, err)
	}
}

func (g *GenericAPIServer) verifyPassword(user *v1.User, plain string) (bool, error) {
	if user == nil {
		return false, fmt.Errorf("invalid user for password verification")
	}
	if g.credentialCache != nil {
		if cached, ok := g.credentialCache.lookup(user.Name, user.Password, plain); ok {
			return cached, nil
		}
	}
	if err := user.Compare(plain); err != nil {
		if stdErrors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			if g.credentialCache != nil {
				g.credentialCache.store(user.Name, user.Password, plain, false)
			}
			return false, nil
		}
		return false, err
	}
	if g.credentialCache != nil {
		g.credentialCache.store(user.Name, user.Password, plain, true)
	}
	return true, nil
}

func (g *GenericAPIServer) reissueAccessTokenWithTTL(atToken string, ttl time.Duration) (string, time.Time, error) {
	if g == nil || g.options == nil {
		return "", time.Time{}, fmt.Errorf("server options 未初始化")
	}
	parser := &gojwt.Parser{}
	claims := gojwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(atToken, claims); err != nil {
		return "", time.Time{}, fmt.Errorf("解析原始访问令牌失败: %w", err)
	}
	now := time.Now()
	claims["iat"] = now.Unix()
	claims["exp"] = now.Add(ttl).Unix()
	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(g.options.JwtOptions.Key))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("重新签名访问令牌失败: %w", err)
	}
	return signed, now.Add(ttl), nil
}

func (g *GenericAPIServer) executeBackgroundCleanup(claims *jwtvalidator.CustomClaims) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userID := claims.UserID
	userSessionsKey := authkeys.UserSessionsKey(userID)
	jti := claims.ID
	expTimestamp := claims.ExpiresAt.Time
	refreshPrefix := authkeys.WithGenericPrefix(authkeys.RefreshTokenPrefix(userID))
	blacklistPrefix := authkeys.WithGenericPrefix(authkeys.BlacklistPrefix(g.options.JwtOptions.Blacklist_key_prefix, userID))

	// Lua 脚本实现原子操作
	luaScript := `
	-- KEYS[1]: 用户会话集合的完整键名
	-- ARGV[1]: Refresh Token键前缀（含哈希标签，以冒号结尾）
	-- ARGV[2]: 黑名单键前缀（含哈希标签，以冒号结尾）
	-- ARGV[3]: jti
	-- ARGV[4]: 过期时间戳
	-- ARGV[5]: 当前时间戳

	local userSessionsKey = KEYS[1]
	local refreshTokenPrefix = ARGV[1]
	local blacklistPrefix = ARGV[2]
	local jti = ARGV[3]
	local expireTimestamp = tonumber(ARGV[4])
	local currentTime = tonumber(ARGV[5])

	local tokens = redis.call('SMEMBERS', userSessionsKey)
	redis.call('DEL', userSessionsKey)

	for _, token in ipairs(tokens) do
		redis.call('DEL', refreshTokenPrefix .. token)
	end

	local blacklistKey = blacklistPrefix .. jti
	local ttl = expireTimestamp - currentTime + 3600

	if ttl > 0 then
		redis.call('SETEX', blacklistKey, ttl, '1')
	else
		redis.call('SETEX', blacklistKey, 3600, '1')
	end

	return #tokens
	`

	result, err := g.redis.GetClient().Eval(ctx, luaScript,
		[]string{userSessionsKey},
		[]interface{}{
			refreshPrefix,
			blacklistPrefix,
			jti,
			expTimestamp.Unix(),
			time.Now().Unix(),
		},
	).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			log.Warnf("用户会话不存在，无需清理: user_id=%s", userID)
			return
		}
		log.Errorf("登出清理Lua脚本执行失败: user_id=%s, error=%v", userID, err)
		return
	}

	tokenCount := 0
	if result != nil {
		tokenCount = int(result.(int64))
	}

	log.Debugf("用户登出清理完成: user_id=%s, 清理了%d个refresh token, jti=%s",
		userID, tokenCount, jti)
	log.Debugf("登出-用户会话Key: %s", userSessionsKey)
}

// clearAuthCookies 清理客户端Cookie
func (g *GenericAPIServer) clearAuthCookies(c *gin.Context) {
	domain := g.options.ServerRunOptions.CookieDomain
	secure := g.options.ServerRunOptions.CookieSecure

	// 清理访问令牌和刷新令牌Cookie
	c.SetCookie("access_token", "", -1, "/", domain, secure, true)
	c.SetCookie("refresh_token", "", -1, "/", domain, secure, true)

	log.Debugf("客户端Cookie已清理")
}

//go:noinline  // 告诉编译器不要内联此函数
func parseWithHeader(c *gin.Context) (loginInfo, error) {
	// 1. 获取Authorization头
	authHeader := c.Request.Header.Get("Authorization")
	if authHeader == "" {
		// 场景1：授权头为空 → 用通用“缺少授权头”错误码
		return loginInfo{}, errors.WithCode(
			code.ErrMissingHeader,
			"Basic认证：缺少Authorization头，正确格式：Authorization: Basic {base64(username:password)}",
		)
	}

	// 2. 分割前缀和内容（必须为"Basic " + 内容）
	authParts := strings.SplitN(authHeader, " ", 2)
	if len(authParts) != 2 || strings.TrimSpace(authParts[0]) != "Basic" {
		// 场景2：非Basic前缀或分割后长度不对 → 用“授权头格式无效”错误码
		return loginInfo{}, errors.WithCode(
			code.ErrInvalidAuthHeader,
			"Basic认证：授权头格式无效，正确格式：Authorization: Basic {base64(username:password)}（前缀必须为Basic）",
		)
	}
	authPayload := strings.TrimSpace(authParts[1])
	if authPayload == "" {
		// 场景3：Basic前缀后无内容 → 单独判断，提示更精准
		return loginInfo{}, errors.WithCode(
			code.ErrInvalidAuthHeader,
			"Basic认证：Authorization头中Basic前缀后无内容，请提供base64编码的username:password",
		)
	}

	// 3. Base64解码（优先处理解码错误，不掩盖细节）
	payload, decodeErr := base64.StdEncoding.DecodeString(authPayload)
	if decodeErr != nil {
		// 场景4：Base64解码失败 → 用“Base64解码失败”错误码
		return loginInfo{}, errors.WithCode(
			code.ErrBase64DecodeFail,
			"Basic认证：Base64解码失败（%v），请确保内容是username:password的Base64编码",
			decodeErr,
		)
	}

	// 4. 分割用户名和密码（必须含冒号）
	userPassPair := strings.SplitN(string(payload), ":", 2)
	if len(userPassPair) != 2 || strings.TrimSpace(userPassPair[0]) == "" || strings.TrimSpace(userPassPair[1]) == "" {
		// 场景5：解码后无冒号/用户名/密码为空 → 用“payload格式无效”错误码
		return loginInfo{}, errors.WithCode(
			code.ErrInvalidBasicPayload,
			"Basic认证：解码后的内容格式无效，需用冒号分隔非空用户名和密码（如 username:password）",
		)
	}

	// 解析成功，返回用户名密码（去除首尾空格）
	return loginInfo{
		Username: strings.TrimSpace(userPassPair[0]),
		Password: strings.TrimSpace(userPassPair[1]),
	}, nil
}

func parseWithBody(c *gin.Context) (loginInfo, error) {
	var login loginInfo
	// 关键：使用 ShouldBindJSON 解析JSON格式的请求体（与测试用例的Content-Type: application/json匹配）
	if err := c.ShouldBindJSON(&login); err != nil {
		// 解析失败时，返回参数错误码（如100004或100006，而非100210）
		return loginInfo{}, errors.WithCode(
			code.ErrInvalidParameter, // 参数错误码（对应400或422，非401）
			"Body参数解析失败：请检查JSON格式是否正确，包含username和password字段",
		)
	}

	// 检查用户名/密码是否为空（基础校验）
	if login.Username == "" || login.Password == "" {
		return loginInfo{}, errors.WithCode(
			code.ErrInvalidParameter,
			"参数错误：username和password不能为空",
		)
	}

	return login, nil
}

// 生成at的claims
func (g *GenericAPIServer) generateAccessTokenClaims(data interface{}) jwt.MapClaims {

	//统一生成sessionid,存储与at和rt中
	sessionID := idutil.GenerateSecureSessionID("")

	expirationTime := time.Now().Add(g.options.JwtOptions.Timeout)

	var userID string

	claims := jwt.MapClaims{
		"iss": APIServerIssuer,
		"aud": APIServerAudience,
		"iat": time.Now().Unix(),
		"jti": idutil.GetUUID36("jwt_"),
		"exp": expirationTime.Unix(),
	}
	if u, ok := data.(*v1.User); ok {
		userID = strconv.FormatUint(u.ID, 10)
		claims["username"] = u.Name
		claims["sub"] = u.Name
		//先写死，后面再调整
		claims["role"] = "admin"
		claims["user_id"] = userID
		claims["session_id"] = sessionID
		claims["type"] = "access"
		claims["user_status"] = u.Status
		claims["isadmin"] = strconv.Itoa(u.IsAdmin)
	}

	return claims
}

func (g *GenericAPIServer) authorizator() func(data interface{}, c *gin.Context) bool {
	return func(data interface{}, c *gin.Context) bool {

		start := time.Now()
		path := c.Request.URL.Path

		var username string
		switch v := data.(type) {
		case nil:
			log.L(c).Warn("Authorizator: identity 信息缺失，可能是令牌已失效或被吊销")
			return false
		case string:
			username = v
		case *v1.User:
			username = v.Name
		case error:
			log.L(c).Warnf("Authorizator: identity 返回错误: %v", v)
			return false
		default:
			log.L(c).Warnf("Authorizator: identity 类型非法: %T", v)
			return false
		}

		claims := jwt.ExtractClaims(c)
		status, _ := claims["user_status"].(float64)
		role, _ := claims["role"].(string)
		if claimsUsername, _ := claims["username"].(string); claimsUsername != "" && claimsUsername != username {
			log.L(c).Warnf("Authorizator: 身份与 claims 不一致，claims=%s identity=%s", claimsUsername, username)
		}

		if username == "" {
			elapsed := time.Since(start)
			targetDelay := 150 * time.Millisecond
			if elapsed < targetDelay {
				time.Sleep(targetDelay - elapsed)
			}
			log.L(c).Warn("Authorizator: 无法解析身份信息，拒绝访问")
			return false
		}

		if status != 1 {
			log.L(c).Warnf("用户%s没有激活", username)
			return false
		}

		if strings.HasPrefix(path, "/admin/") && role != "admin" {
			log.L(c).Warnf("用户%s无权访问%s(需要管理员校色)", username, path)
			return false
		}

		c.Set(common.UsernameKey, username)

		return true
	}
}

func (g *GenericAPIServer) generateRefreshTokenAndGetUserID(c *gin.Context, atToken string) (string, uint64, error) {
	userVal, exists := c.Get("current_user")
	if !exists {
		log.Errorf("loginResponse: 上下文未找到用户数据（键：%s）", jwt.IdentityKey)
		return "", 0, errors.WithCode(code.ErrUserNotFound, "未找到用户信息")
	}
	// 类型断言为 *v1.User（与 Authenticator 返回的类型一致）
	user, ok := userVal.(*v1.User)
	if !ok {
		log.Errorf("loginResponse: 用户数据类型错误，预期 *v1.User，实际 %T", userVal)
		return "", 0, errors.WithCode(code.ErrBind, " 错误绑定")
	}

	// 1. 手动生成刷新令牌
	refreshToken, err := g.generateRefreshToken(user, atToken)
	if err != nil {
		log.Errorf("生成刷新令牌失败: %v", err)
		errors.WithCode(code.ErrTokenInvalid, "生成刷新令牌失败")
		return "", 0, err
	}
	return refreshToken, user.ID, nil
}

func (g *GenericAPIServer) loginResponse(c *gin.Context, atToken string, expire time.Time) {

	currentUser, _ := c.Get("current_user")
	user, _ := currentUser.(*v1.User)

	var username string
	if user != nil {
		username = user.Name
	}
	if ttl, ok := g.debugTokenOverride(c); ok {
		newToken, newExpire, err := g.reissueAccessTokenWithTTL(atToken, ttl)
		if err != nil {
			log.Errorf("调试令牌过期覆盖失败: %v", err)
			core.WriteResponse(c, errors.WithCode(code.ErrInternal, "调试令牌生成失败"), nil)
			return
		}
		atToken = newToken
		expire = newExpire
	}

	// 获取刷新令牌
	refreshToken, userID, err := g.generateRefreshTokenAndGetUserID(c, atToken)
	if err != nil {
		core.WriteResponse(c, err, nil)
		return
	}
	// 尝试存储
	rollback, err := g.StoreAuthSessionWithRollback(strconv.FormatUint(userID, 10), refreshToken)
	if err != nil {
		// 存储失败：直接返回错误，不需要回滚（因为根本没存成功）
		log.Errorf("会话存储失败: %v", err)
		c.JSON(500, gin.H{"error": "系统错误"})
		return
	}

	// 存储成功，设置回滚函数到上下文（用于后续可能失败的操作）
	c.Set("session_rollback", rollback)
	c.Set("stored_refresh_token", refreshToken)

	if err := g.setAuthCookies(c, atToken, refreshToken, expire); err != nil {
		log.Warnf("设置认证Cookie失败: %v", err)
	}

	// 构建成功数据
	successData := gin.H{
		"login_user":     username,
		"operator":       username,
		"operation_time": time.Now().Format(time.RFC3339),
		"operation_type": "login",
		"access_token":   atToken,
		"refresh_token":  refreshToken,
		"expire":         expire.Format(time.RFC3339),
		"token_type":     "Bearer",
	}
	core.WriteResponse(c, nil, successData)

}

func (g *GenericAPIServer) ValidateATForRefreshMiddleware(c *gin.Context) {

	if c.Request.Method != http.MethodPost {
		err := errors.WithCode(code.ErrMethodNotAllowed, "必须使用post方法传输")
		g.refreshError(c, "", err, nil)
		return
	}

	metadata := map[string]any{}

	type TokenRequest struct {
		RefreshToken string `json:"refresh_token"`
	}
	var r TokenRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		log.Warnf("解析刷新令牌错误%v", err)
		respErr := errors.WithCode(code.ErrBind, "解析刷新令牌错误,必须")
		g.refreshError(c, "", respErr, metadata)
		return
	}
	if r.RefreshToken == "" {
		log.Warn("refresh token为空")
		respErr := errors.WithCode(code.ErrInvalidParameter, "refresh token为空")
		g.refreshError(c, "", respErr, metadata)
		return
	}
	metadata["has_refresh_token"] = true

	claimsValue, exists := c.Get("jwt_claims")
	if !exists {
		respErr := errors.WithCode(code.ErrInternal, "认证信息缺失")
		g.refreshError(c, "", respErr, metadata)
		return
	}
	claims, ok := claimsValue.(gojwt.MapClaims)
	if !ok {
		respErr := errors.WithCode(code.ErrInvalidParameter, "Token声明无效")
		g.refreshError(c, "", respErr, metadata)
		return
	}
	username, ok := getUsernameFromClaims(claims)
	if !ok {
		respErr := errors.WithCode(code.ErrUserNotFound, "请在username,sub,user任意字段中填入用户名")
		g.refreshError(c, "", respErr, metadata)
		return
	}
	metadata["username"] = username

	authHeader := c.GetHeader("Authorization")
	if authHeader != "" {
		metadata["has_access_token"] = true
	}
	accessToken := strings.TrimPrefix(authHeader, "Bearer ")

	// 并发刷新场景下可能只携带 RT，此时跳过 AT 会话一致性校验
	if strings.TrimSpace(accessToken) != "" {
		if err := g.validateSameSession(accessToken, r.RefreshToken); err != nil {
			g.refreshError(c, username, err, metadata)
			return
		}
	} else {
		metadata["has_access_token"] = false
	}

	rtClaims, err := parseTokenWithoutValidation(r.RefreshToken)
	if err != nil {
		respErr := errors.WithCode(code.ErrTokenInvalid, "刷新令牌无效或已过期")
		g.refreshError(c, username, respErr, metadata)
		return
	}
	rtUserID := getUserIDString(rtClaims["user_id"])
	if rtUserID == "" {
		respErr := errors.WithCode(code.ErrTokenInvalid, "刷新令牌缺少用户标识")
		g.refreshError(c, username, respErr, metadata)
		return
	}
	if sessionID, _ := rtClaims["session_id"].(string); sessionID != "" {
		c.Set("refresh_session_id", sessionID)
	}

	refreshTokenKey := authkeys.RefreshTokenKey(rtUserID, r.RefreshToken)
	storedUserID, err := g.redis.GetKey(c, refreshTokenKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			respErr := errors.WithCode(code.ErrExpired, "刷新令牌已经过期,请重新登录")
			g.refreshError(c, username, respErr, metadata)
			return
		}
		respErr := errors.WithCode(code.ErrTokenInvalid, "刷新令牌无效或已过期")
		g.refreshError(c, username, respErr, metadata)
		return
	}
	if storedUserID == "" {
		respErr := errors.WithCode(code.ErrExpired, "刷新令牌已经过期,请重新登录")
		g.refreshError(c, username, respErr, metadata)
		return
	}

	userSessionsKey := authkeys.UserSessionsKey(rtUserID)
	isMember, err := g.redis.IsMemberOfSet(c, userSessionsKey, r.RefreshToken)
	if err != nil {
		log.Errorf("验证Refresh Token会话失败: %v", err)
		respErr := errors.WithCode(code.ErrInternal, "验证Refresh Token会话失败")
		g.refreshError(c, username, respErr, metadata)
		return
	}
	if !isMember {
		respErr := errors.WithCode(code.ErrExpired, "刷新令牌已失效，请重新登录")
		g.refreshError(c, username, respErr, metadata)
		return
	}

	claimsUserID := getUserIDString(claims["user_id"])
	if claimsUserID == "" || claimsUserID != rtUserID {
		respErr := errors.WithCode(code.ErrPermissionDenied, "用户身份不匹配")
		g.refreshError(c, username, respErr, metadata)
		return
	}

	if atSessionID, ok := claims["session_id"].(string); ok && atSessionID != "" {
		if rtSessionID, _ := c.Get("refresh_session_id"); rtSessionID != nil {
			if rtSessionStr, ok := rtSessionID.(string); ok && rtSessionStr != "" && atSessionID != rtSessionStr {
				respErr := errors.WithCode(code.ErrTokenMismatch, "access token和refresh token不匹配")
				g.refreshError(c, username, respErr, metadata)
				return
			}
		}
	}

	user, err := interfaces.Client().Users().Get(c, username, metav1.GetOptions{}, g.options)
	if err != nil {
		log.Errorf("查询用户信息失败: %v", err)
		respErr := errors.WithCode(code.ErrUserNotFound, "用户不存在")
		g.refreshError(c, username, respErr, metadata)
		return
	}
	c.Set("current_user", user)

	newAccessToken, exp, err := g.generateAccessTokenAndGetID(c)
	if err != nil {
		respErr := errors.WithCode(code.ErrInternal, "访问令牌生成错误%v", err)
		g.refreshError(c, username, respErr, metadata)
		return
	}

	expireIn := int64(time.Until(exp).Seconds())
	if expireIn < 0 {
		expireIn = 0
	}

	response := map[string]string{
		"access_token": newAccessToken,
		"expire_in":    strconv.Itoa(int(expireIn)),
		"token_type":   "Bearer",
	}
	core.WriteResponse(c, nil, response)
	g.auditRefreshEvent(c, username, "success", nil, map[string]any{
		"expire_in_seconds": expireIn,
	})

}

func (g *GenericAPIServer) generateAccessTokenAndGetID(c *gin.Context) (string, time.Time, error) {
	user := c.MustGet("current_user").(*v1.User)

	expireTime := time.Now().Add(g.options.JwtOptions.Timeout)
	baseClaims := g.generateAccessTokenClaims(user)
	claims := gojwt.MapClaims{}
	for k, v := range baseClaims {
		claims[k] = v
	}
	claims["exp"] = expireTime.Unix()
	claims["iat"] = time.Now().Unix()
	claims["jti"] = idutil.GetUUID36("access_")
	claims["type"] = "access"

	if sessionID, ok := c.Get("refresh_session_id"); ok {
		if s, ok := sessionID.(string); ok && s != "" {
			claims["session_id"] = s
		}
	}

	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims)
	accessToken, err := token.SignedString([]byte(g.options.JwtOptions.Key))
	if err != nil {
		return "", time.Time{}, err
	}
	return accessToken, expireTime, nil
}

// newAutoAuth 创建Auto认证策略（处理所有错误场景，避免panic）
func (g *GenericAPIServer) newAutoAuth() (middleware.AuthStrategy, error) {
	// 1. 初始化JWT认证策略：处理初始化失败错误
	jwtStrategy, err := g.newJWTAuth()
	if err != nil {
		// 场景1：JWT策略初始化失败（如密钥加载失败、配置错误）
		return nil, errors.WithCode(
			code.ErrInternalServer,
			"Auto认证策略初始化失败：JWT认证策略创建失败，原因：%v",
			err, // 携带原始错误原因，便于调试
		)
	}

	// 2. JWT策略类型转换：处理转换失败（避免强制断言panic）
	jwtAuth, ok := jwtStrategy.(auth.JWTStrategy)
	if !ok {
		// 场景2：类型转换失败（明确预期类型和实际类型）
		return nil, errors.WithCode(
			code.ErrInternalServer,
			"Auto认证策略初始化失败：JWT策略类型转换错误，预期类型为 auth.JWTStrategy，实际类型为 %T",
			jwtStrategy, // 打印实际类型，快速定位依赖问题
		)
	}

	// 3. 初始化Basic认证策略：补充错误处理（原代码直接断言，会panic）
	basicStrategy := g.newBasicAuth()
	// 3.1 Basic策略类型转换：处理转换失败
	basicAuth, ok := basicStrategy.(auth.BasicStrategy)
	if !ok {
		// 场景3：Basic策略类型转换失败
		return nil, errors.WithCode(
			code.ErrInternalServer,
			"Auto认证策略初始化失败：Basic策略类型转换错误，预期类型为 auth.BasicStrategy，实际类型为 %T",
			basicStrategy,
		)
	}

	// 4. 所有依赖初始化成功，创建AutoStrategy
	autoAuth := auth.NewAutoStrategy(basicAuth, jwtAuth)
	return autoAuth, nil
}

func recordErrorToContext(c *gin.Context, err error) {
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Err:  err,
			Type: gin.ErrorTypePrivate, // 标记为私有错误，避免框架暴露敏感信息
		})
	}
}

// handleUnauthorized 统一处理未授权场景（封装Unauthorized回调核心逻辑）
// 参数：
//   - c: gin上下文（用于获取请求信息、返回响应）
//   - httpCode: HTTPStatusMessageFunc映射后的HTTP状态码
//   - message: HTTPStatusMessageFunc映射后的基础错误消息
func handleUnauthorized(c *gin.Context, httpCode int, message string) {

	// 1. 从上下文提取业务码（优先使用HTTPStatusMessageFunc映射后的withCode错误）
	bizCode := extractBizCode(c, message)
	if bizCode == code.ErrExpired {
		event := audit.BuildEventFromRequest(c.Request)
		event.Action = "auth.logout"
		event.ResourceType = "auth"
		event.Outcome = "timeout"
		event.ErrorMessage = message
		if actor := c.GetHeader("X-User"); actor != "" {
			event.Actor = actor
			event.ResourceID = actor
		}
		submitAuditFromGinContext(c, event)
	}

	// 2. 日志分级：基于业务码重要性输出差异化日志（含request-id便于追踪）
	LogWithLevelByBizCode(c, bizCode, message)

	// 3. 补充上下文信息：不同业务码返回专属指引（帮助客户端快速定位问题）
	extraInfo := buildExtraInfo(c, bizCode)

	// 4. 生成标准withCode错误（避免格式化安全问题）
	err := errors.WithCode(bizCode, "%s", message)

	// 5. 统一返回响应（依赖core.WriteResponse确保格式一致）
	core.WriteResponse(c, err, extraInfo)

	// 6. 终止流程：防止后续中间件覆盖当前响应
	c.Abort()
}

// 带request-id的分级日志（按业务码重要性划分级别）
func LogWithLevelByBizCode(c *gin.Context, bizCode int, message string) {
	requestID := getRequestID(c)
	switch bizCode {
	// 安全风险：Warn级别（需重点关注，可能是恶意请求）
	case code.ErrSignatureInvalid, code.ErrTokenInvalid, code.ErrPasswordIncorrect:
		log.Warnf("[安全风险] 未授权（bizCode: %d），request-id: %s，消息: %s",
			bizCode, requestID, message)
	// 客户端错误：Debug级别（便于客户端调试，非恶意）
	case code.ErrInvalidAuthHeader, code.ErrBase64DecodeFail, code.ErrInvalidBasicPayload:
		log.Warnf("[客户端错误] 未授权（bizCode: %d），request-id: %s，消息: %s",
			bizCode, requestID, message)
	// 常规场景：Info级别（正常用户操作，如令牌过期、缺少头）
	case code.ErrExpired, code.ErrMissingHeader, code.ErrPermissionDenied:
		log.Warnf("[常规场景] 未授权（bizCode: %d），request-id: %s，消息: %s",
			bizCode, requestID, message)
	// 未分类：Warn级别（需后续补充匹配规则）
	default:
		log.Warnf("[未分类] 未授权（bizCode: %d），request-id: %s，消息: %s",
			bizCode, requestID, message)
	}
}

// buildExtraInfo 基于业务码构建额外上下文信息（帮助客户端快速修复问题）
// 关键：给函数增加 c *gin.Context 参数，用于获取请求头/上下文信息
func buildExtraInfo(c *gin.Context, bizCode int) gin.H {
	switch bizCode {
	case code.ErrExpired: // 100203：令牌已过期
		return gin.H{
			"suggestion": "令牌已过期，请重新调用/login接口获取新令牌",
			"next_step":  "POST /login（携带用户名密码）",
		}
	case code.ErrInvalidAuthHeader: // 100204：授权头格式无效
		return gin.H{
			"example": "正确格式：Authorization: Bearer <your-jwt-token>（Bearer后需带1个空格）",
			"note":    "仅支持Bearer认证方案，不支持Basic/其他方案",
		}
	case code.ErrAccountLocked: // 建议新增：账户被锁定
		ttl := c.GetDuration(ctxLoginFailTTLKey)
		retryAfter := int(math.Ceil(ttl.Seconds()))
		if retryAfter < 0 {
			retryAfter = 0
		}
		return gin.H{
			"suggestion":  "账户因连续登录失败已被暂时锁定",
			"next_step":   buildUnlockNextStep(ttl),
			"retry_after": retryAfter, // 秒数，可用于前端倒计时
		}
	case code.ErrTokenInvalid: // 100208：令牌无效
		return gin.H{
			"possible_reason": []string{
				"令牌格式错误（需包含2个.分隔，如xx.xx.xx）",
				"令牌被篡改（签名验证失败）",
				"令牌未经过正确编码（需Base64Url编码）",
			},
		}
	case code.ErrBase64DecodeFail: // 100209：Basic认证Base64解码失败
		return gin.H{
			"example":    "正确格式：Authorization: Basic dXNlcjE6cGFzc3dvcmQ=（dXNlcjE6cGFzc3dvcmQ=是base64(\"user1:password\")）",
			"check_tool": "可通过echo -n 'user:pass' | base64 验证编码是否正确",
		}
	case code.ErrInvalidBasicPayload: // 100210：Basic认证payload格式无效
		return gin.H{
			"requirement": "Base64解码后必须包含冒号（:），格式为\"用户名:密码\"",
			"example":     "解码后应为\"admin:123456\"，而非\"admin123456\"",
		}
	case code.ErrPermissionDenied: // 100207：权限不足
		// 现在 c 是函数参数，可正常调用 GetHeader 获取 X-User 头
		currentUser := c.GetHeader("X-User")
		// 优化：若 X-User 头为空，返回“未知用户”避免空值
		if currentUser == "" {
			currentUser = "未知用户（未携带X-User头）"
		}
		return gin.H{
			"suggestion":   "联系管理员授予操作权限（需包含xxx角色）",
			"current_user": currentUser, // 正常返回当前用户信息
		}
	case code.ErrRespCodeRTRevoked:
		currentUser := c.GetHeader("X-User")
		// 优化：若 X-User 头为空，返回“未知用户”避免空值
		if currentUser == "" {
			currentUser = "未知用户（未携带X-User头）"
		}
		return gin.H{
			"current_user": currentUser, // 正常返回当前用户信息
			"message":      "令牌已经过期",
		}
	default: // 其他场景：返回空（避免冗余）
		return gin.H{}
	}
}

// getRequestID 从上下文获取request-id（便于链路追踪）
func getRequestID(c *gin.Context) string {
	if requestID, exists := c.Get("requestID"); exists {
		if idStr, ok := requestID.(string); ok {
			return idStr
		}
	}
	// 降级：从请求头获取
	return c.GetHeader("X-Request-ID")
}

func isTokenInBlacklist(g *GenericAPIServer, c *gin.Context, userID, jti string) (bool, error) {
	key := authkeys.BlacklistKey(g.options.JwtOptions.Blacklist_key_prefix, userID, jti)
	exists, err := g.redis.Exists(c, key)
	if err != nil {
		return false, errors.WithCode(code.ErrDatabase, "redis:查询令牌黑名单失败: %v", err)
	}
	return exists, nil
}

func (g *GenericAPIServer) getLoginFailCount(c *gin.Context, username string) (int, error) {
	if g == nil || g.redis == nil {
		return 0, nil
	}
	key := authkeys.LoginFailKey(username)
	val, err := g.redis.GetKey(c.Request.Context(), key)
	if err != nil {
		if errors.Is(err, redis.Nil) {

			return 0, nil
		} else {
			log.Errorf("redis服务错误:err=%v", err)
			return 0, err
		}
	}
	count, err := strconv.Atoi(val)
	if err != nil {
		log.Warnf("解析登录失败次数失败: username=%s, val=%s, error=%v", username, val, err)
		return 0, nil // 解析失败默认返回0次
	}
	return count, nil
}

func (g *GenericAPIServer) checkRedisAlive() bool {
	if err := g.redis.Up(); err != nil { // 用Ping检查存活
		log.Errorf("Redis连接失败: %v", err)
		return false
	}
	return true
}

func (g *GenericAPIServer) restLoginFailCount(ctx context.Context, username string) error {
	if !g.checkRedisAlive() {
		log.Warnf("Redis不可用,无法重置登录失败次数:username:%s", username)
		return nil
	}
	key := authkeys.LoginFailKey(username)
	if _, err := g.redis.DeleteKey(ctx, key); err != nil {
		return errors.New("重置登录次数失败")
	}
	return nil
}

func (g *GenericAPIServer) setAuthCookies(c *gin.Context, accessToken, refreshToken string, accessTokenExpire time.Time) error {
	domain := g.options.ServerRunOptions.CookieDomain // 从配置获取域名
	secure := g.options.ServerRunOptions.CookieSecure // 是否仅HTTPS

	// 计算Cookie过期时间（秒数）
	accessTokenMaxAge := int(time.Until(accessTokenExpire).Seconds())
	refreshTokenMaxAge := int(g.options.JwtOptions.MaxRefresh.Seconds())

	// 设置Access Token Cookie
	c.SetCookie("access_token", accessToken, accessTokenMaxAge, "/", domain, secure, true)

	// 设置Refresh Token Cookie
	c.SetCookie("refresh_token", refreshToken, refreshTokenMaxAge, "/", domain, secure, true)

	return nil
}

// generateRefreshToken 生成刷新令牌
func (g *GenericAPIServer) generateRefreshToken(user *v1.User, atToken string) (string, error) {

	atSessionID, err := extractSessionID(atToken)
	if err != nil {
		return "", errors.New("获取atSessionid错误")
	}
	// 创建刷新令牌的 claims
	exp := time.Now().Add(g.options.JwtOptions.MaxRefresh).Unix()
	jti := idutil.GetUUID36("refresh_")
	iat := time.Now().Unix()
	refreshClaims := gojwt.MapClaims{
		"iss":        APIServerIssuer,
		"aud":        APIServerAudience,
		"iat":        iat,
		"exp":        exp,
		"jti":        jti,
		"sub":        user.Name,
		"user_id":    user.ID,
		"session_id": atSessionID,
		"type":       "refresh", // 标记为刷新令牌
	}

	// 生成刷新令牌
	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, refreshClaims)

	refreshToken, err := token.SignedString([]byte(g.options.JwtOptions.Key))
	if err != nil {
		return "", fmt.Errorf("签名刷新令牌失败: %w", err)
	}

	return refreshToken, nil
}

func extractBizCode(c *gin.Context, message string) int {
	// 优先：从c.Errors提取带Code()方法的错误
	if len(c.Errors) > 0 {
		rawErr := c.Errors.Last().Err

		// 适配自定义withCode错误（必须实现Code() int方法）
		if customErr, ok := rawErr.(interface{ Code() int }); ok {
			bizCode := customErr.Code()

			return bizCode
		}

		// 新增：检查JWT标准错误类型
		if bizCode := classifyJWTError(rawErr); bizCode != 0 {
			return bizCode
		}
	}

	// 降级：基于消息文本匹配业务码
	msgLower := strings.ToLower(message)
	return classifyByMessage(msgLower, c)
}

// 辅助函数：分类JWT标准错误
func classifyJWTError(err error) int {
	var jwtErr *gojwt.ValidationError
	if errors.As(err, &jwtErr) {
		switch {
		case jwtErr.Is(gojwt.ErrTokenExpired):
			return code.ErrExpired // 100203
		case jwtErr.Is(gojwt.ErrTokenMalformed):
			return code.ErrTokenInvalid // 100208
		case jwtErr.Is(gojwt.ErrTokenNotValidYet):
			return code.ErrTokenInvalid // 100208
		case jwtErr.Is(gojwt.ErrTokenInvalidClaims):
			return code.ErrTokenInvalid // 100208
		case jwtErr.Is(gojwt.ErrTokenUnverifiable):
			return code.ErrSignatureInvalid // 100202
		}
	}
	return 0
}

// 辅助函数：基于消息内容分类
func classifyByMessage(message string, c *gin.Context) int {
	switch {
	case strings.Contains(message, "expired") || strings.Contains(message, "expiry"):
		return code.ErrExpired // 100203
	case strings.Contains(message, "signature") && (strings.Contains(message, "invalid") || strings.Contains(message, "fail")):
		return code.ErrSignatureInvalid // 100202
	case containsAny(message, "authorization", "header") && containsAny(message, "missing", "not present", "empty"):
		return code.ErrMissingHeader // 100205
	case containsAny(message, "authorization", "header") && containsAny(message, "invalid", "format"):
		return code.ErrInvalidAuthHeader // 100204
	case containsAny(message, "base64") && containsAny(message, "decode", "fail"):
		return code.ErrBase64DecodeFail // 100209
	case containsAny(message, "basic") && containsAny(message, "payload", "format"):
		return code.ErrInvalidBasicPayload // 100210
	case containsAny(message, "password") && containsAny(message, "incorrect", "wrong"):
		return code.ErrPasswordIncorrect // 100206
	case containsAny(message, "permission", "access") && containsAny(message, "denied", "forbidden"):
		return code.ErrPermissionDenied // 100207
	case isJsonParseError(message):
		return code.ErrTokenInvalid // 100208
	case isEncodingError(message):
		return code.ErrTokenInvalid // 100208
	case containsAny(message, "invalid", "malformed") && containsAny(message, "token", "jwt"):
		return code.ErrTokenInvalid // 100208
	default:
		log.Warnf("[handleUnauthorized] 未匹配到业务码，使用默认未授权码（request-id: %s），原始消息: %s",
			getRequestID(c), message)
		return code.ErrTokenInvalid // 100208（更具体的默认值）
	}
}

// 辅助函数：检查是否包含任意一个子字符串
func containsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// 辅助函数：判断是否为JSON解析错误
func isJsonParseError(msg string) bool {
	patterns := []string{"json", "character", "looking for", "beginning", "value", "unmarshal", "parse"}
	return containsMultiple(msg, patterns, 2)
}

// 辅助函数：判断是否为编码错误
func isEncodingError(msg string) bool {
	patterns := []string{"base64", "decode", "encoding", "invalid character", "utf-8", "hex"}
	return containsMultiple(msg, patterns, 2)
}

// 辅助函数：检查是否包含多个模式
func containsMultiple(s string, patterns []string, minMatch int) bool {
	count := 0
	for _, pattern := range patterns {
		if strings.Contains(s, pattern) {
			count++
			if count >= minMatch {
				return true
			}
		}
	}
	return false
}

func (g *GenericAPIServer) ValidateATMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"error": "Authorization头缺失"})
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		// 1. 首先验证AT基本格式
		parser := &gojwt.Parser{}
		token, _, err := parser.ParseUnverified(tokenString, gojwt.MapClaims{})
		if err != nil {
			core.WriteResponse(c, errors.WithCode(code.ErrTokenInvalid, "invalid:访问令牌无效:%V", err), nil)
			return
		}

		// 2. 验证必要声明存在（但不验证过期时间）
		claims, ok := token.Claims.(gojwt.MapClaims)
		if !ok {
			c.AbortWithStatusJSON(401, gin.H{"error": "Token声明无效"})
			return
		}

		// 3. 验证用户标识存在（支持多种字段）
		hasUserIdentity := false
		userIdentityFields := []string{"sub", "username", "user_id", "user"}
		for _, field := range userIdentityFields {
			if claims[field] != nil {
				hasUserIdentity = true
				break
			}
		}

		if !hasUserIdentity {
			c.AbortWithStatusJSON(401, gin.H{"error": "Token缺少用户标识"})
			return
		}

		// 4. 验证token类型（如果是区分类型的）
		if tokenType, exists := claims["type"]; exists {
			if typeStr, ok := tokenType.(string); ok && typeStr != "access" {
				c.AbortWithStatusJSON(401, gin.H{"error": "非Access Token"})
				return
			}
		}

		c.Set("jwt_claims", claims)
		c.Next()
	}
}

func getUsernameFromClaims(claims map[string]interface{}) (string, bool) {
	var username string
	var ok bool

	// 1. 首先尝试从username字段获取
	if username, ok = claims["username"].(string); ok && username != "" {
		return username, true
	}

	// 2. 如果username为空或不存在，尝试从sub字段获取
	if sub, ok := claims["sub"].(string); ok && sub != "" {
		return sub, true
	}

	// 3. 如果sub也为空，尝试从user字段获取
	if user, ok := claims["user"].(string); ok && user != "" {
		return user, true
	}

	// 4. 如果所有字段都为空或不存在，返回空和false
	return "", false
}

func getUserIDString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.FormatInt(v, 10)
	case jsoniter.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func (g *GenericAPIServer) validateSameSession(accessToken, refreshToken string) error {
	// 解析AT
	atClaims, err := parseTokenWithoutValidation(accessToken)
	if err != nil {
		return errors.WithCode(code.ErrTokenInvalid, "AT解析失败")
	}

	// 解析RT
	rtClaims, err := parseTokenWithoutValidation(refreshToken)
	if err != nil {
		return errors.WithCode(code.ErrTokenInvalid, "RT解析失败")
	}

	// 获取session_id
	atSessionID, ok1 := atClaims["session_id"].(string)
	rtSessionID, ok2 := rtClaims["session_id"].(string)

	if !ok1 || atSessionID == "" {
		return errors.WithCode(code.ErrTokenInvalid, "AT缺少session_id")
	}
	if !ok2 || rtSessionID == "" {
		return errors.WithCode(code.ErrTokenInvalid, "RT缺少session_id")
	}

	// 直接比较session_id
	if atSessionID != rtSessionID {
		log.Warnf("Token不匹配: AT session_id=%s, RT session_id=%s", atSessionID, rtSessionID)
		return errors.WithCode(code.ErrTokenMismatch, "access token和refresh token不匹配")
	}
	return nil
}

// 解析token但不验证签名（因为我们只关心claims）
func parseTokenWithoutValidation(tokenString string) (gojwt.MapClaims, error) {
	parser := gojwt.Parser{}
	token, _, err := parser.ParseUnverified(tokenString, gojwt.MapClaims{})
	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(gojwt.MapClaims); ok {
		return claims, nil
	}

	return nil, errors.New("无法解析token claims")
}

// StoreAuthSessionWithRollback 存储完整的认证会话信息，并返回回滚函数
func (g *GenericAPIServer) StoreAuthSessionWithRollback(userID, refreshToken string) (func(), error) {

	// 执行事务性存储所有相关数据
	err := g.storeCompleteAuthSession(userID, refreshToken)
	if err != nil {
		return nil, fmt.Errorf("存储认证会话失败: %v", err)
	}

	// 返回完整的回滚函数
	rollback := func() {
		g.rollbackAuthSession(userID, refreshToken)
	}

	return rollback, nil
}

// 事务处理
func (g *GenericAPIServer) storeCompleteAuthSession(userID, refreshToken string) error {
	ctx := context.Background()
	client := g.redis.GetClient()

	//atExpire := g.options.JwtOptions.Timeout
	rtExpire := g.options.JwtOptions.MaxRefresh
	refreshTokenKey := authkeys.WithGenericPrefix(authkeys.RefreshTokenKey(userID, refreshToken))
	userSessionsKey := authkeys.WithGenericPrefix(authkeys.UserSessionsKey(userID))

	_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, refreshTokenKey, userID, rtExpire)
		pipe.SAdd(ctx, userSessionsKey, refreshToken)
		pipe.Expire(ctx, userSessionsKey, rtExpire)

		return nil
	})

	return err
}

// rollbackCompleteAuthSession 回滚完整的认证会话数据
func (g *GenericAPIServer) rollbackAuthSession(userID, refreshToken string) {
	ctx := context.Background()
	client := g.redis.GetClient()
	refreshTokenKey := authkeys.WithGenericPrefix(authkeys.RefreshTokenKey(userID, refreshToken))
	userSessionsKey := authkeys.WithGenericPrefix(authkeys.UserSessionsKey(userID))

	log.Warnf("执行登录会话回滚: user_id=%s", userID)

	_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, refreshTokenKey)
		pipe.SRem(ctx, userSessionsKey, refreshToken)

		return nil
	})

	if err != nil {
		log.Errorf("回滚操作失败: user_id=%s, error=%v", userID, err)
	}
}

// extractSessionID 使用jwt库解析token提取session_id
func extractSessionID(tokenString string) (string, error) {
	if tokenString == "" {
		return "", errors.New("空token")
	}

	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return "", errors.New("无效的JWT格式")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("base64解码失败: %v", err)
	}

	var claims map[string]interface{}
	if err := jsonCodec.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("JSON解析失败: %v", err)
	}

	sessionID, ok := claims["session_id"].(string)
	if !ok || sessionID == "" {
		return "", errors.New("缺少session_id")
	}

	return sessionID, nil
}

func (g *GenericAPIServer) isSessionRevoked(c *gin.Context, userID, sessionID string) (bool, error) {
	if g == nil || g.redis == nil {
		return false, nil
	}
	if userID == "" || sessionID == "" {
		return false, nil
	}

	key := authkeys.RevokedSessionsKey(userID)
	return g.redis.IsMemberOfSet(c, key, sessionID)
}

func (g *GenericAPIServer) isTokenRevokedByPasswordChange(c *gin.Context, userID string, claims jwt.MapClaims) (bool, error) {
	if g == nil || g.redis == nil {
		return false, nil
	}
	if userID == "" {
		return false, nil
	}

	key := authkeys.PasswordVersionKey(userID)
	markerStr, err := g.redis.GetKey(c, key)
	if err != nil {
		if stdErrors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	if markerStr == "" {
		return false, nil
	}

	marker, err := strconv.ParseInt(markerStr, 10, 64)
	if err != nil {
		log.Warnf("解析密码版本标记失败: user_id=%s value=%s err=%v", userID, markerStr, err)
		return false, nil
	}

	iatValue, ok := claims["iat"]
	if !ok {
		return false, nil
	}

	iat, ok := normalizeUnixClaim(iatValue)
	if !ok {
		return false, nil
	}

	return iat < marker, nil
}

func normalizeUnixClaim(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
}
