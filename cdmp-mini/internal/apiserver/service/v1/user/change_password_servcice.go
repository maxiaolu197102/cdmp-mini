package user

import (
	"context"
	"encoding/base64"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	authkeys "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/auth/keys"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/util"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator/jwtvalidator"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/redis/go-redis/v9"
)

const changePasswordRetryLimit = 3

var slowStoreCallWarningThreshold = 120 * time.Millisecond

func (u *UserService) ChangePassword(ctx context.Context, user *v1.User, claims *jwtvalidator.CustomClaims, opt *options.Options) (err error) {
	serviceCtx, span := trace.StartSpan(ctx, "user-service", "change_password")
	if serviceCtx != nil {
		ctx = serviceCtx
	}
	trace.AddRequestTag(ctx, "target_user", user.Name)

	spanStatus := "success"
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": user.Name,
	}
	outcomeStatus := "success"
	outcomeCode := businessCode
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK
	defer func() {
		if err != nil {
			spanStatus = "error"
			outcomeStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
				outcomeCode = businessCode
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
				outcomeCode = businessCode
			}
			if msg := errors.GetMessage(err); msg != "" {
				outcomeMessage = msg
			}
			if status := errors.GetHTTPStatus(err); status != 0 {
				outcomeHTTP = status
			} else {
				outcomeHTTP = http.StatusInternalServerError
			}
		}
		if span != nil {
			trace.EndSpan(span, spanStatus, businessCode, spanDetails)
		}
		trace.RecordOutcome(ctx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
	}()

	// 判断用户是否存在 - forceRefresh=true 强制回源验证
	ruser, checkErr := u.checkUserExist(ctx, user.Name, true)
	if checkErr != nil {
		log.Warnf("查询用户%s checkUserExist方法返回错误, 可能是系统繁忙, 将忽略是否存在的检查: %v", user.Name, checkErr)
	}
	if ruser != nil && (ruser.Name == RATE_LIMIT_PREVENTION || ruser.Name == BLACKLIST_SENTINEL) {
		log.Warnf("用户%s不存在,无法修改密码", user.Name)
		return errors.WithCode(code.ErrUserNotFound, "用户不存在")
	}
	currentPasswordHash := ""

	if ruser != nil {
		if ruser.ID != 0 {
			user.ID = ruser.ID
			user.ObjectMeta.ID = ruser.ID
		}
		if ruser.InstanceID != "" {
			user.InstanceID = ruser.InstanceID
			user.ObjectMeta.InstanceID = ruser.InstanceID
		}
		if user.ObjectMeta.CreatedAt.IsZero() && !ruser.CreatedAt.IsZero() {
			user.ObjectMeta.CreatedAt = ruser.CreatedAt
		}
		if !ruser.UpdatedAt.IsZero() {
			user.ObjectMeta.UpdatedAt = ruser.UpdatedAt
		}
		if ruser.Password != "" {
			currentPasswordHash = ruser.Password
		}
	}

	// 如果缓存返回的数据不包含最新的版本信息，补打一条直连查询
	expectedUpdatedAt := user.UpdatedAt
	if ruser != nil && !ruser.UpdatedAt.IsZero() {
		expectedUpdatedAt = ruser.UpdatedAt
	}
	if (expectedUpdatedAt.IsZero() || currentPasswordHash == "" || user.ID == 0) && u.Store != nil {
		latest, latestErr := u.Store.Users().Get(ctx, user.Name, metav1.GetOptions{}, u.Options)
		if latestErr != nil {
			log.Warnf("补查用户最新元数据失败: username=%s err=%v", user.Name, latestErr)
		} else if latest != nil {
			if user.ID == 0 && latest.ID != 0 {
				user.ID = latest.ID
				user.ObjectMeta.ID = latest.ID
			}
			if user.InstanceID == "" && latest.InstanceID != "" {
				user.InstanceID = latest.InstanceID
				user.ObjectMeta.InstanceID = latest.InstanceID
			}
			if expectedUpdatedAt.IsZero() && !latest.UpdatedAt.IsZero() {
				expectedUpdatedAt = latest.UpdatedAt
				user.ObjectMeta.UpdatedAt = latest.UpdatedAt
			}
			if currentPasswordHash == "" && latest.Password != "" {
				currentPasswordHash = latest.Password
			}
		}
	}

	// 更新数据库（带乐观锁，冲突时回表刷新版本）
	maxRetries := changePasswordRetryLimit
	if u.Options != nil && u.Options.RedisOptions != nil && u.Options.RedisOptions.MaxRetries > 0 {
		if u.Options.RedisOptions.MaxRetries < maxRetries {
			maxRetries = u.Options.RedisOptions.MaxRetries
		}
	}
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var updateErr error
	var appliedUpdatedAt time.Time
	for attempt := 1; attempt <= maxRetries; attempt++ {
		appliedUpdatedAt = time.Now()
		updateStart := time.Now()
		updateErr = u.Store.Users().UpdatePasswordWithVersion(ctx, user.ID, user.Name, user.Password, expectedUpdatedAt, appliedUpdatedAt)
		if elapsed := time.Since(updateStart); elapsed > slowStoreCallWarningThreshold {
			log.Warnf("UpdatePasswordWithVersion耗时较长: username=%s elapsed=%s", user.Name, elapsed)
		}
		if updateErr == nil {
			spanDetails["db_update"] = "success"
			user.UpdatedAt = appliedUpdatedAt
			break
		}

		if errors.IsCode(updateErr, code.ErrResourceConflict) {
			if u.Store == nil {
				break
			}
			latestStart := time.Now()
			latest, latestErr := u.Store.Users().Get(ctx, user.Name, metav1.GetOptions{}, u.Options)
			if elapsed := time.Since(latestStart); elapsed > slowStoreCallWarningThreshold {
				log.Warnf("Get耗时较长: username=%s elapsed=%s", user.Name, elapsed)
			}
			if latestErr != nil {
				if isRetryableError(latestErr) && attempt < maxRetries {
					time.Sleep(time.Duration(100*attempt) * time.Millisecond)
					continue
				}
				updateErr = latestErr
				break
			}
			if latest == nil {
				updateErr = errors.WithCode(code.ErrUserNotFound, "用户不存在")
				break
			}
			// 如果密码已经被其他流程更新，则直接返回冲突给调用方
			if currentPasswordHash != "" && latest.Password != "" && latest.Password != currentPasswordHash {
				log.Warnf("检测到真正的密码并发修改: username=%s attempt=%d expected_timestamp=%s latest_timestamp=%s", user.Name, attempt, expectedUpdatedAt.Format(time.RFC3339Nano), latest.UpdatedAt.Format(time.RFC3339Nano))
				break
			}
			// 更新版本信息后重试
			expectedUpdatedAt = latest.UpdatedAt
			currentPasswordHash = latest.Password
			if user.ID == 0 && latest.ID != 0 {
				user.ID = latest.ID
				user.ObjectMeta.ID = latest.ID
			}
			if user.InstanceID == "" && latest.InstanceID != "" {
				user.InstanceID = latest.InstanceID
				user.ObjectMeta.InstanceID = latest.InstanceID
			}
			log.Debugf("改密冲突重试: username=%s attempt=%d new_expected_timestamp=%s", user.Name, attempt, expectedUpdatedAt.Format(time.RFC3339Nano))
			if attempt < maxRetries {
				time.Sleep(time.Duration(100*attempt) * time.Millisecond)
			}
			continue
		}

		if !isRetryableError(updateErr) || attempt == maxRetries {
			break
		}
		log.Debugf("改密重试: username=%s attempt=%d reason=%v", user.Name, attempt, updateErr)
		time.Sleep(time.Duration(100*attempt) * time.Millisecond)
	}

	if updateErr != nil {
		spanDetails["db_update"] = "failed"
		return updateErr
	}

	// 刷新缓存，避免后续登录命中旧密码
	asyncRetries := 1
	if u.Options != nil && u.Options.RedisOptions != nil && u.Options.RedisOptions.MaxRetries > 0 {
		asyncRetries = u.Options.RedisOptions.MaxRetries
	}
	asyncJob := &changePasswordAsyncJob{
		Username:   user.Name,
		UserID:     user.ID,
		User:       cloneUserSnapshot(user),
		Claims:     cloneClaims(claims),
		MaxRetries: asyncRetries,
	}
	u.scheduleChangePasswordAsync(ctx, asyncJob)
	spanDetails["post_change_tasks"] = "queued"

	log.Infof("用户%s修改密码成功，异步执行后续清理", user.Name)
	return nil
}

type changePasswordAsyncJob struct {
	Username   string
	UserID     uint64
	User       *v1.User
	Claims     *jwtvalidator.CustomClaims
	MaxRetries int
}

func (u *UserService) scheduleChangePasswordAsync(parent context.Context, job *changePasswordAsyncJob) {
	if u == nil || job == nil {
		return
	}
	baseCtx := context.Background()
	if parent != nil {
		if reqID := parent.Value("requestID"); reqID != nil {
			baseCtx = context.WithValue(baseCtx, "requestID", reqID)
		}
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("异步执行改密后置任务 panic: %v\n%s", r, debug.Stack())
			}
		}()

		timeout := 10 * time.Second
		if parent != nil {
			if deadline, ok := parent.Deadline(); ok {
				remaining := time.Until(deadline)
				if remaining > 0 && remaining < timeout {
					timeout = remaining
				}
			}
		}
		asyncCtx, cancel := context.WithTimeout(baseCtx, timeout)
		defer cancel()

		u.runChangePasswordAsyncTasks(asyncCtx, job)
	}()
}

func (u *UserService) runChangePasswordAsyncTasks(ctx context.Context, job *changePasswordAsyncJob) {
	if job == nil {
		return
	}

	if job.User != nil {
		if err := u.setUserCache(ctx, job.Username, job.User); err != nil {
			log.Warnf("异步刷新用户缓存失败: username=%s err=%v", job.Username, err)
		}
	}

	if job.MaxRetries < 1 {
		job.MaxRetries = 1
	}
	if _, err := util.RetryWithBackoff(job.MaxRetries, isRetryableError, func() (interface{}, error) {
		if err := u.forceLogoutAllDevices(ctx, job.UserID); err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, nil
			}
			return nil, err
		}
		return nil, nil
	}); err != nil {
		log.Warnf("异步清理用户会话失败: username=%s err=%v", job.Username, err)
	}

	if err := u.blacklistAccessToken(ctx, job.Claims); err != nil {
		log.Warnf("异步写入访问令牌黑名单失败: username=%s err=%v", job.Username, err)
	}

	if err := u.recordPasswordChangeVersion(ctx, job.UserID); err != nil {
		log.Warnf("异步记录密码版本标记失败: username=%s err=%v", job.Username, err)
	}
}

func cloneUserSnapshot(user *v1.User) *v1.User {
	if user == nil {
		return nil
	}
	cloned := *user
	cloned.ObjectMeta = user.ObjectMeta
	if user.ObjectMeta.Extend != nil {
		copied := make(metav1.Extend, len(user.ObjectMeta.Extend))
		for k, v := range user.ObjectMeta.Extend {
			copied[k] = v
		}
		cloned.ObjectMeta.Extend = copied
	}
	return &cloned
}

func cloneClaims(claims *jwtvalidator.CustomClaims) *jwtvalidator.CustomClaims {
	if claims == nil {
		return nil
	}
	copied := *claims
	return &copied
}

func (u *UserService) recordPasswordChangeVersion(ctx context.Context, userID uint64) error {
	if userID == 0 {
		return nil
	}

	key := authkeys.PasswordVersionKey(strconv.FormatUint(userID, 10))
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	if err := u.Redis.SetKey(ctx, key, timestamp, 0); err != nil {
		return errors.WithCode(code.ErrDatabase, "记录密码版本标记失败: %v", err)
	}
	return nil
}

func (u *UserService) blacklistAccessToken(ctx context.Context, claims *jwtvalidator.CustomClaims) error {
	if claims == nil {
		return nil
	}

	if claims.ID == "" || claims.UserID == "" {
		log.Warnf("访问令牌缺少jti或user_id，跳过黑名单写入")
		return nil
	}

	blacklistKey := authkeys.BlacklistKey(u.Options.JwtOptions.Blacklist_key_prefix, claims.UserID, claims.ID)
	//fullBlacklistKey := authkeys.WithGenericPrefix(blacklistKey)

	var ttl time.Duration
	if claims.ExpiresAt != nil {
		ttl = time.Until(claims.ExpiresAt.Time) + time.Hour
	}
	if ttl <= 0 {
		ttl = u.Options.JwtOptions.Timeout + time.Hour
	}
	if ttl <= 0 {
		ttl = time.Hour
	}

	if err := u.Redis.SetKey(ctx, blacklistKey, "1", ttl); err != nil {
		return errors.WithCode(code.ErrDatabase, "写入访问令牌黑名单失败: %v", err)
	}
	return nil
}

func (u *UserService) forceLogoutAllDevices(ctx context.Context, userID uint64) error {
	userIDStr := strconv.FormatUint(userID, 10)
	userSessionsKey := authkeys.UserSessionsKey(userIDStr)
	refreshPrefix := authkeys.WithGenericPrefix(authkeys.RefreshTokenPrefix(userIDStr))

	luaScript := `
		-- KEYS[1]: 用户会话集合完整键名
		-- ARGV[1]: Refresh Token 键前缀（含哈希标签，以冒号结尾）

		local userSessionsKey = KEYS[1]
		local refreshTokenPrefix = ARGV[1]

		redis.log(redis.LOG_NOTICE, "开始清理用户会话: " .. userSessionsKey)

		local tokens = redis.call('SMEMBERS', userSessionsKey)
		redis.log(redis.LOG_NOTICE, "找到 " .. #tokens .. " 个refresh token")

		for _, token in ipairs(tokens) do
			local rtKey = refreshTokenPrefix .. token
			redis.call('DEL', rtKey)
			redis.log(redis.LOG_NOTICE, "已删除refresh token: " .. rtKey)
		end

		local delResult = redis.call('DEL', userSessionsKey)
		redis.log(redis.LOG_NOTICE, "删除用户会话集合结果: " .. delResult)

		return tokens
	`

	result, err := u.Redis.Eval(ctx, luaScript,
		[]string{
			userSessionsKey,
		},
		[]interface{}{
			refreshPrefix,
		},
	)

	if err != nil {
		log.Errorf("Lua脚本执行失败: %v", err)
		if errors.Is(err, redis.Nil) {
			log.Warnf("用户%s没有活跃会话", userIDStr)
			return redis.Nil
		}
		return errors.WithCode(code.ErrDatabase, "清理用户令牌失败: %v", err)
	}

	refreshTokens := decodeRedisStringArray(result)
	if len(refreshTokens) == 0 {
		return redis.Nil
	}

	if err := u.recordRevokedSessions(ctx, userID, refreshTokens); err != nil {
		log.Warnf("记录被撤销的session失败: %v", err)
	}

	return nil
}

func decodeRedisStringArray(val interface{}) []string {
	switch v := val.(type) {
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return v
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		return nil
	}
}

func (u *UserService) recordRevokedSessions(ctx context.Context, userID uint64, refreshTokens []string) error {
	if len(refreshTokens) == 0 {
		return nil
	}

	userIDStr := strconv.FormatUint(userID, 10)
	sessionIDs := make([]string, 0, len(refreshTokens))
	for _, token := range refreshTokens {
		sessionID, err := extractSessionIDFromJWT(token)
		if err != nil {
			log.Debugf("解析refresh token session_id失败: %v", err)
			continue
		}
		if sessionID != "" {
			sessionIDs = append(sessionIDs, sessionID)
		}
	}

	if len(sessionIDs) == 0 {
		return nil
	}

	revokedKey := authkeys.RevokedSessionsKey(userIDStr)

	ttl := u.Options.JwtOptions.MaxRefresh
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	if err := u.Redis.AddToSetWithTTL(ctx, revokedKey, sessionIDs, ttl); err != nil {
		return errors.WithCode(code.ErrDatabase, "记录撤销session失败: %v", err)
	}

	return nil
}

func extractSessionIDFromJWT(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("无效的token格式")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("base64解码失败: %w", err)
	}

	var claims map[string]interface{}
	if err := stdjson.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("JSON解析失败: %w", err)
	}

	if sessionID, ok := claims["session_id"].(string); ok && sessionID != "" {
		return sessionID, nil
	}
	return "", fmt.Errorf("token缺少session_id")
}
