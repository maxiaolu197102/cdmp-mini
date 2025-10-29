package common

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

// 优化的本地限流器
type localRateLimiter struct {
	sync.RWMutex
	counters map[string]*localCounter
}

type localCounter struct {
	count    int64
	lastTime time.Time
}

var (
	localLimiter = &localRateLimiter{
		counters: make(map[string]*localCounter),
	}

	// 定期清理过期的本地计数器
	cleanupTicker = time.NewTicker(5 * time.Minute)
)

func init() {
	go cleanupExpiredCounters()
}

func cleanupExpiredCounters() {
	for range cleanupTicker.C {
		localLimiter.Lock()
		now := time.Now()
		for key, counter := range localLimiter.counters {
			if now.Sub(counter.lastTime) > 10*time.Minute {
				delete(localLimiter.counters, key)
			}
		}
		localLimiter.Unlock()
	}
}

// 优化的Lua脚本 - 使用INCR和EXPIRE组合
const optimizedLuaScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

-- 使用INCR，如果key不存在会自动创建为1
local current = redis.call('INCR', key)

-- 如果是第一次设置，设置过期时间
if current == 1 then
    redis.call('EXPIRE', key, window)
end

-- 如果超过限制，返回剩余时间
if current > limit then
    local ttl = redis.call('TTL', key)
    return {1, ttl}
else
    return {0, limit - current}
end
`

// buildRedisKey 构建完整的Redis键（处理前缀）
func buildRedisKey(redisCluster *storage.RedisCluster, key string) string {
	if redisCluster.KeyPrefix != "" {
		return redisCluster.KeyPrefix + key
	}
	return key
}

// LoginRateLimiter 优化的登录限流中间件
func LoginRateLimiter(redisCluster *storage.RedisCluster, limit int, window time.Duration) gin.HandlerFunc {
	return loginRateLimiter(redisCluster, func() (int, time.Duration) {
		return limit, window
	})
}

// LoginRateLimiterWithProvider 支持动态调整的登录限流中间件
func LoginRateLimiterWithProvider(redisCluster *storage.RedisCluster, provider func() (int, time.Duration)) gin.HandlerFunc {
	return loginRateLimiter(redisCluster, provider)
}

func loginRateLimiter(redisCluster *storage.RedisCluster, provider func() (int, time.Duration)) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, window := provider()
		if limit <= 0 {
			c.Next()
			return
		}
		if window <= 0 {
			window = time.Minute
		}
		windowSec := int64(window.Seconds())

		identifier := "ip:" + c.ClientIP()
		rateLimitKey := buildRedisKey(redisCluster, "ratelimit:login:"+identifier)

		// 第一层：本地内存限流（按IP）
		if !localRateCheck(identifier, limit, window) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100209,
				"message": "请求过于频繁，请稍后再试（本地限流）",
				"data":    nil,
			})
			return
		}

		// 第二层：Redis限流（增加超时时间到200ms）
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		result, err := redisCluster.GetClient().Eval(ctx, optimizedLuaScript,
			[]string{rateLimitKey},
			limit, windowSec,
		).Result()

		if err != nil {
			handleRedisError(c, err, identifier, limit)
			return
		}

		// 解析Lua脚本返回结果
		results, ok := result.([]interface{})
		if !ok || len(results) != 2 {
			log.Errorf("限流Lua脚本返回格式错误: %v", result)
			c.Next() // 格式错误时放行
			return
		}

		limited, _ := results[0].(int64)
		remaining, _ := results[1].(int64)

		if limited == 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100209,
				"message": "请求过于频繁，请稍后再试",
				"data": gin.H{
					"limit":       limit,
					"window":      window.String(),
					"retry_after": formatSecondsLabel(remaining),
				},
			})
			return
		}

		// 允许通过
		c.Next()
	}
}

// 本地限流检查
func localRateCheck(identifier string, limit int, window time.Duration) bool {
	localLimiter.Lock()
	defer localLimiter.Unlock()

	now := time.Now()
	counter, exists := localLimiter.counters[identifier]

	if !exists {
		localLimiter.counters[identifier] = &localCounter{
			count:    1,
			lastTime: now,
		}
		return true
	}

	// 检查时间窗口
	if now.Sub(counter.lastTime) > window {
		// 重置计数器
		counter.count = 1
		counter.lastTime = now
		return true
	}

	// 增加计数
	counter.count++
	counter.lastTime = now

	// 本地限制可以比Redis宽松一些，避免误限流
	localLimit := limit * 2
	return counter.count <= int64(localLimit)
}

// 统一的Redis错误处理
func handleRedisError(c *gin.Context, err error, identifier string, limit int) {
	if errors.Is(err, context.DeadlineExceeded) {
		log.Warnf("Redis限流操作超时，降级处理: %v", err)
		// 超时时使用更严格的本地限流
		if !strictLocalRateCheck(identifier, limit) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100209,
				"message": "系统繁忙，请稍后再试（降级模式）",
				"data":    nil,
			})
			return
		}
		// 超时但本地检查通过，放行
		c.Next()
		return
	}

	// 其他Redis错误
	log.Errorf("Redis限流错误: %v", err)
	// Redis不可用时，使用宽松的本地限流
	if !lenientLocalRateCheck(identifier, limit) {
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
			"code":    100209,
			"message": "系统繁忙，请稍后再试",
			"data":    nil,
		})
		return
	}
	c.Next()
}

// 严格的本地限流（用于超时降级）
func strictLocalRateCheck(identifier string, limit int) bool {
	localLimiter.Lock()
	defer localLimiter.Unlock()

	counter, exists := localLimiter.counters[identifier]
	if !exists {
		return true
	}

	// 严格模式：使用原始限制
	return counter.count <= int64(limit)
}

// 宽松的本地限流（用于其他错误降级）
func lenientLocalRateCheck(identifier string, limit int) bool {
	localLimiter.Lock()
	defer localLimiter.Unlock()

	counter, exists := localLimiter.counters[identifier]
	if !exists {
		return true
	}

	// 宽松模式：使用2倍限制
	return counter.count <= int64(limit*2)
}

// LoginRateLimiterByUser 按用户ID限流的优化版本
func LoginRateLimiterByUser(redisCluster *storage.RedisCluster, limit int, window time.Duration) gin.HandlerFunc {
	windowSec := int64(window.Seconds())

	return func(c *gin.Context) {
		// 从上下文中获取用户ID
		userID, exists := c.Get("userID")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    100401,
				"message": "用户未认证",
				"data":    nil,
			})
			return
		}

		identifier := buildUserIdentifier(userID)
		rateLimitKey := buildRedisKey(redisCluster, "ratelimit:login:"+identifier)

		// 本地限流检查
		if !localRateCheck(identifier, limit, window) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100209,
				"message": "操作过于频繁，请稍后再试",
				"data":    nil,
			})
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		result, err := redisCluster.GetClient().Eval(ctx, optimizedLuaScript,
			[]string{rateLimitKey},
			limit, windowSec,
		).Result()

		if err != nil {
			handleRedisError(c, err, identifier, limit)
			return
		}

		results, ok := result.([]interface{})
		if !ok || len(results) != 2 {
			log.Errorf("用户限流Lua脚本返回格式错误: %v", result)
			c.Next()
			return
		}

		limited, _ := results[0].(int64)
		if limited == 1 {
			remaining, _ := results[1].(int64)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100209,
				"message": "操作过于频繁，请稍后再试",
				"data": gin.H{
					"retry_after": formatSecondsLabel(remaining),
				},
			})
			return
		}

		c.Next()
	}
}

// CleanupRateLimit 清理限流数据
func CleanupRateLimit(redisCluster *storage.RedisCluster) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pattern := buildRedisKey(redisCluster, "ratelimit:login:*")
	keys, err := redisCluster.GetClient().Keys(ctx, pattern).Result()
	if err != nil {
		log.Errorf("清理限流key失败: %v", err)
		return
	}

	if len(keys) > 0 {
		_, err = redisCluster.GetClient().Del(ctx, keys...).Result()
		if err != nil {
			log.Errorf("删除限流key失败: %v", err)
			return
		}
	}

	// 同时清理本地计数器
	localLimiter.Lock()
	localLimiter.counters = make(map[string]*localCounter)
	localLimiter.Unlock()

}

type stringer interface {
	String() string
}

func formatSecondsLabel(seconds int64) string {
	if seconds <= 0 {
		return "0秒"
	}
	if seconds >= 60 {
		minutes := (seconds + 59) / 60
		return strconv.FormatInt(minutes, 10) + "分钟"
	}
	return strconv.FormatInt(seconds, 10) + "秒"
}

func buildUserIdentifier(userID interface{}) string {
	const prefix = "user:"
	if userID == nil {
		return prefix
	}
	switch v := userID.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return prefix
		}
		return prefix + trimmed
	case []byte:
		trimmed := strings.TrimSpace(string(v))
		if trimmed == "" {
			return prefix
		}
		return prefix + trimmed
	case int:
		return prefix + strconv.Itoa(v)
	case int8:
		return prefix + strconv.FormatInt(int64(v), 10)
	case int16:
		return prefix + strconv.FormatInt(int64(v), 10)
	case int32:
		return prefix + strconv.FormatInt(int64(v), 10)
	case int64:
		return prefix + strconv.FormatInt(v, 10)
	case uint:
		return prefix + strconv.FormatUint(uint64(v), 10)
	case uint8:
		return prefix + strconv.FormatUint(uint64(v), 10)
	case uint16:
		return prefix + strconv.FormatUint(uint64(v), 10)
	case uint32:
		return prefix + strconv.FormatUint(uint64(v), 10)
	case uint64:
		return prefix + strconv.FormatUint(v, 10)
	case float32:
		return prefix + strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return prefix + strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		if v {
			return prefix + "true"
		}
		return prefix + "false"
	case stringer:
		trimmed := strings.TrimSpace(v.String())
		if trimmed == "" {
			return prefix
		}
		return prefix + trimmed
	default:
		return prefix + fmt.Sprintf("%v", v)
	}
}
