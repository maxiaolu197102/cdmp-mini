package common

import (
	"context"
	"crypto/sha1"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

// WriteRateLimiter 提供写操作（create/delete/force）级别的分布式限流。
// 设计要点：
// - 第一层本地内存快速检测（避免 Redis 问题时短路）
// - 第二层使用 Redis Lua 原子 INCR+EXPIRE，短超时以保证低延迟
// - Redis 超时或错误时使用降级策略（采用严格或宽松本地限流）
func WriteRateLimiter(redisCluster *storage.RedisCluster, limit int, window time.Duration) gin.HandlerFunc {
	windowSec := int64(window.Seconds())

	return func(c *gin.Context) {
		// 尝试读取全局动态限流配置（优先），短超时
		ctxg, cancelg := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancelg()
		globalKey := buildRedisKey(redisCluster, "ratelimit:write:global_limit")
		if redisCluster != nil && redisCluster.GetClient() != nil {
			if val, err := redisCluster.GetClient().Get(ctxg, globalKey).Result(); err == nil {
				// 解析成整数
				if gLimit, perr := parseInt(val); perr == nil && gLimit > 0 {
					limit = gLimit
				}
			}

			// parseInt helper moved to file scope
		}

		// 标识使用 API 路径 + 优先使用 Authorization token（若存在）作为粒度，否则使用客户端 IP
		idPart := c.ClientIP()
		if auth := c.GetHeader("Authorization"); strings.TrimSpace(auth) != "" {
			ah := strings.TrimSpace(auth)
			if strings.HasPrefix(strings.ToLower(ah), "bearer ") {
				ah = strings.TrimSpace(ah[7:])
			}
			if ah != "" {
				// 使用 token 的 sha1 前缀作为标识，避免在 Redis key 中放置长 token
				h := sha1.Sum([]byte(ah))
				idPart = fmt.Sprintf("%x", h)[:16]
			}
		}
		identifier := "write:" + idPart + ":" + c.FullPath()
		rateLimitKey := buildRedisKey(redisCluster, "ratelimit:write:"+identifier)

		// 本地快速检查（复用 localRateCheck 保持一致策略）
		if !localRateCheck(identifier, limit, window) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100429,
				"message": "写操作过于频繁，请稍后再试（本地限流）",
				"data":    nil,
			})
			metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "local_rate").Inc()
			return
		}

		// Redis 限流，短超时
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		result, err := redisCluster.GetClient().Eval(ctx, optimizedLuaScript,
			[]string{rateLimitKey},
			limit, windowSec,
		).Result()

		if err != nil {
			// 降级到严格本地限流
			log.Warnf("Redis限流失败，降级本地处理: %v", err)
			if !strictLocalRateCheck(identifier, limit) {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    100429,
					"message": "系统繁忙，请稍后再试（降级）",
					"data":    nil,
				})
				metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "redis_timeout").Inc()
				return
			}
			c.Next()
			return
		}

		// 解析返回
		arr, ok := result.([]interface{})
		if !ok || len(arr) != 2 {
			log.Warnf("限流Lua返回格式异常: %v", result)
			c.Next()
			return
		}
		limited, _ := arr[0].(int64)
		remaining, _ := arr[1].(int64)

		if limited == 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    100429,
				"message": "写操作过于频繁，请稍后再试",
				"data": gin.H{
					"retry_after": fmt.Sprintf("%d秒", remaining),
				},
			})
			metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "redis_limit").Inc()
			return
		}

		c.Next()
	}
}

func parseInt(s string) (int, error) {
	var v int
	_, err := fmt.Sscanf(s, "%d", &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}
