package user

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	storectx "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	apierrors "github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) Get(ctx context.Context, username string, opts metav1.GetOptions, opt *options.Options) (result *v1.User, err error) {

	serviceCtx, serviceSpan := trace.StartSpan(ctx, "user-service", "get_user")
	if serviceCtx != nil {
		ctx = serviceCtx
	}

	cacheKey := u.generateUserCacheKey(username)
	trace.AddRequestTag(ctx, "target_user", username)
	trace.AddRequestTag(ctx, "cache_key", cacheKey)

	spanStatus := "success"
	outcomeStatus := "success"
	outcomeCode := strconv.Itoa(code.ErrSuccess)
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK
	cacheHitLabel := "miss"
	sharedResult := false

	spanDetails := map[string]interface{}{
		"target_user": username,
		"cache_key":   cacheKey,
	}

	if forceCacheRefreshFromContext(ctx) {
		ctx = storectx.WithForcePrimary(ctx)
	}

	if blocked, blkErr := u.isUserBlacklisted(ctx, username); blkErr != nil {
		log.Warnf("黑名单状态查询失败: username=%s err=%v", username, blkErr)
	} else if blocked {
		cacheHitLabel = "blacklist_active"
		spanDetails["blacklist_active"] = true
		trace.AddRequestTag(ctx, "protection_blacklist_active", true)
		result = &v1.User{ObjectMeta: metav1.ObjectMeta{Name: BLACKLIST_SENTINEL}}
		if serviceSpan != nil {
			spanDetails["result_user"] = BLACKLIST_SENTINEL
		}
		return result, nil
	}

	defer func() {
		if err != nil {
			spanStatus = "error"
			outcomeStatus = "error"
			if c := apierrors.GetCode(err); c != 0 {
				outcomeCode = strconv.Itoa(c)
			} else {
				outcomeCode = strconv.Itoa(code.ErrUnknown)
			}
			if msg := apierrors.GetMessage(err); msg != "" {
				outcomeMessage = msg
			}
			if status := apierrors.GetHTTPStatus(err); status != 0 {
				outcomeHTTP = status
			} else {
				outcomeHTTP = http.StatusInternalServerError
			}
		}
		spanDetails["cache_hit"] = cacheHitLabel
		spanDetails["singleflight_shared"] = sharedResult
		if result != nil {
			spanDetails["result_user"] = result.Name
		}
		if serviceSpan != nil {
			trace.EndSpan(serviceSpan, spanStatus, outcomeCode, spanDetails)
		}
		trace.RecordOutcome(ctx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
	}()

	cachedUser, found, cacheErr := u.tryGetFromCache(ctx, username)
	if cacheErr != nil {
		log.Errorf("缓存查询异常，继续流程", "error", cacheErr.Error(), "username", username)
		metrics.CacheErrors.WithLabelValues("query_failed", "get").Inc()
		cacheHitLabel = "error"
	}
	if cacheErr == nil && found {
		switch {
		case cachedUser == nil:
			cacheHitLabel = "null_hit"
		case cachedUser.Name == RATE_LIMIT_PREVENTION:
			cacheHitLabel = "negative_hit"
			trace.AddRequestTag(ctx, "protection_negative_cache_hit", true)
		case cachedUser.Name == BLACKLIST_SENTINEL:
			cacheHitLabel = "blacklist_hit"
			trace.AddRequestTag(ctx, "protection_blacklist_cache_hit", true)
		default:
			cacheHitLabel = "hit"
			if forceCacheRefreshFromContext(ctx) {
				trace.AddRequestTag(ctx, "cache_positive_bypass", true)
				if refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username); refreshErr != nil {
					log.Warnf("正缓存强制刷新失败: username=%s err=%v", username, refreshErr)
				} else {
					cachedUser = refreshedUser
				}
			}
		}
		result = cachedUser
		return result, nil
	}

	// 缓存未命中，使用singleflight保护数据库查询
	var dbResult interface{}
	dbResult, err, sharedResult = u.group.Do(cacheKey, func() (interface{}, error) {
		return u.getUserFromDBAndSetCache(ctx, username)
	})
	if sharedResult {
		metrics.RequestsMerged.WithLabelValues("get").Inc()
	}
	if err != nil {
		return nil, err
	}

	if dbResult == nil {
		return nil, nil
	}

	result = dbResult.(*v1.User)
	return result, nil
}

// 专门处理缓存查询，不包含降级逻辑
func (u *UserService) tryGetFromCache(ctx context.Context, username string) (*v1.User, bool, error) {
	redisTimeout := u.Options.RedisOptions.Timeout
	if redisTimeout == 0 {
		redisTimeout = 5 * u.Options.RedisOptions.Timeout
	}

	redisCtx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()

	cacheKey := u.generateUserCacheKey(username)
	cachedUser, isCached, err := u.getFromCache(redisCtx, cacheKey)
	if err != nil {
		u.recordCacheError(err, "get_from_cache")
		// 只返回错误，不处理降级
		return nil, false, err
	}
	if isCached && cachedUser != nil && cachedUser.ObjectMeta.Version == 0 {
		trace.AddRequestTag(ctx, "cache_missing_version", true)
		isCached = false
		cachedUser = nil
		if u.Redis != nil {
			_, _ = u.Redis.DeleteKey(redisCtx, cacheKey)
		}
	}

	force := forceCacheRefreshFromContext(ctx)

	if isCached {
		if cachedUser != nil {
			switch cachedUser.Name {
			case RATE_LIMIT_PREVENTION:
				metrics.CacheHits.WithLabelValues("null_hit").Inc()
				trace.AddRequestTag(ctx, "protection_negative_cache_hit", true)
				if force {
					trace.AddRequestTag(ctx, "cache_negative_bypass", true)
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						log.Warnf("负缓存强制刷新失败: username=%s err=%v", username, refreshErr)
					} else if refreshedUser != nil {
						return refreshedUser, true, nil
					}
					return nil, true, nil
				}
				if refreshAllowed, lockKey := u.shouldRefreshNullCache(ctx, username); refreshAllowed {
					defer u.releaseNullCacheRefreshLock(lockKey)
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						log.Warnf("负缓存刷新失败: username=%s err=%v", username, refreshErr)
					} else if refreshedUser != nil {
						return refreshedUser, true, nil
					}
				} else {
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						return refreshedUser, true, nil
					}
				}
				return nil, true, nil
			case BLACKLIST_SENTINEL:
				metrics.CacheHits.WithLabelValues("blacklist_hit").Inc()
				trace.AddRequestTag(ctx, "protection_blacklist_cache_hit", true)
				if force {
					trace.AddRequestTag(ctx, "cache_blacklist_bypass", true)
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						log.Warnf("黑名单缓存强制刷新失败: username=%s err=%v", username, refreshErr)
					} else if refreshedUser != nil {
						return refreshedUser, true, nil
					}
					return nil, true, nil
				}
				return cachedUser, true, nil
			default:
				metrics.CacheHits.WithLabelValues("hit").Inc()
				return cachedUser, true, nil
			}
		}
		metrics.CacheHits.WithLabelValues("null_hit").Inc()
		return nil, true, nil // 空值缓存命中
	}

	// 缓存中没有记录
	metrics.CacheHits.WithLabelValues("no_record").Inc()
	return nil, false, nil
}

// 记录缓存错误的辅助方法
func (u *UserService) recordCacheError(err error, operation string) {
	errorType := "unknown"

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		errorType = "timeout"
	case errors.Is(err, context.Canceled):
		errorType = "cancelled"
	case errors.Is(err, redis.Nil):
		errorType = "key_not_found"
		return // key不存在是正常情况，不记录为错误
	default:
		// 检查是否是网络错误
		var netErr net.Error
		if errors.As(err, &netErr) {
			if netErr.Timeout() {
				errorType = "network_timeout"
			} else {
				errorType = "network_error"
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			errorType = "connection_refused"
		} else if strings.Contains(err.Error(), "authentication") {
			errorType = "authentication_failed"
		}
	}

	// 使用 WithLabelValues 来记录
	metrics.CacheErrors.WithLabelValues(errorType, operation).Inc()
}
