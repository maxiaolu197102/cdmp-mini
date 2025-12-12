package user

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

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

	blacklistStart := time.Now()
	blocked, blkErr := u.isUserBlacklisted(ctx, username)
	u.recordUserCreateStep(ctx, "get_check_blacklist", "protection", username, time.Since(blacklistStart), blkErr)
	if blkErr != nil {
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

	cacheStart := time.Now()
	cachedUser, found, cacheErr := u.tryGetFromCache(ctx, username)
	u.recordUserCreateStep(ctx, "get_try_cache", "cache", username, time.Since(cacheStart), cacheErr)
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
	dbStart := time.Now()
	dbResult, err, sharedResult = u.group.Do(cacheKey, func() (interface{}, error) {
		return u.getUserFromDBAndSetCache(ctx, username)
	})
	u.recordUserCreateStep(ctx, "get_fetch_db", "database", username, time.Since(dbStart), err)
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
// 返回值：用户对象（可能为 nil）、是否命中缓存、错误信息.这里返回的是 (*v1.User, bool, error)。第二个布尔值 true 表示“缓存层处理过该用户名”——即便取出来的是 sentinel（负缓存或黑名单）或空值，也算命中了缓存；只有真正没有这个 key 时才会返回 false。这样调用层就知道：true 代表“本次请求已经得到缓存结论，不需要继续打 DB”，哪怕结论是 “nil/黑名单/负缓存”。
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
	// 处理版本为0的缓存，视为无效缓存
	// 读到的结构来自 Redis 序列化的 v1.User，其中 ObjectMeta.Version 按约定只要是合法写入就会带上数据库版本号或至少是 >0。如果版本字段是 0，意味着序列化时缺了版本信息（早期 bug 或序列化被截断），无法判断缓存里的数据是否对应当前 DB 状态，因此被视为“坏缓存”。为了避免把这种来源不明的数据返回给业务，就直接标记 cache_missing_version=true、删除该 key 并让后续逻辑走正常 miss/回源流程。
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
			/*
				防护触发：当 getUserFromDBAndSetCache 查库返回 code.ErrUserNotFound 时，会调用 handleProtectionForMiss。这个函数用 NegativeCounterKey(username) 统计“某个用户名在保护窗口内被查询却不存在”的次数；当次数 ≥ NegativeCacheThreshold（在 ServerRunOptions 里配置，例如 5 次/60 秒）时，就调用 cacheNullValue 把该用户名的缓存写成哨兵值 RATE_LIMIT_PREVENTION，即打上负缓存标记（见 user_service.go 约 699、1365 行）。
				删除补偿：执行删除流程后，cleanupUserStateForDelete 在清理正/联系缓存后也会调用 cacheNullValue(ctx, username, 0)（delete_service.go ~370 行），确保刚删除的用户短时间内被 GET/创建再查时直接命中负缓存，避免重复落库或立刻被重建。
				特性说明：这两个入口都会写同一个 Redis key user:<username>，TTL 默认 45s+抖动，可配置；负缓存是逐用户名独立的，不会互相污染。forceRefresh=true 时会无条件回源刷新；force=false 时只有拿到 shouldRefreshNullCache 锁的请求才会去确认，从而“偶尔刷新一次”防止雪崩。

			*/
			case RATE_LIMIT_PREVENTION:
				metrics.CacheHits.WithLabelValues("null_hit").Inc()
				trace.AddRequestTag(ctx, "protection_negative_cache_hit", true)
				// 强制刷新负缓存
				if force {
					trace.AddRequestTag(ctx, "cache_negative_bypass", true)
					//	强制刷新负缓存
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						log.Warnf("负缓存强制刷新失败: username=%s err=%v", username, refreshErr)
					} else if refreshedUser != nil {
						return refreshedUser, true, nil
					}
					return nil, true, nil
				}
				//如果成功获取到刷新所需的锁，则查库
				if refreshAllowed, lockKey := u.shouldRefreshNullCache(ctx, username); refreshAllowed {
					defer u.releaseNullCacheRefreshLock(lockKey)
					refreshedUser, refreshErr := u.refreshUserCacheFromDB(ctx, username)
					if refreshErr != nil {
						log.Warnf("负缓存刷新失败: username=%s err=%v", username, refreshErr)
					} else if refreshedUser != nil {
						return refreshedUser, true, nil
					}
				} else {
					trace.AddRequestTag(ctx, "cache_negative_refresh_skipped", true)
					metrics.CacheHits.WithLabelValues("negative_refresh_skipped").Inc()
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
