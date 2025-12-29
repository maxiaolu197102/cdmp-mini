package common

import (
	"context"
	"crypto/sha1"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/bizid"
	codepkg "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	redis "github.com/redis/go-redis/v9"
)

// WriteRateLimiter 提供写操作（create/delete/force）级别的分布式限流。
// 设计要点：
// - 第一层本地内存快速检测（避免 Redis 问题时短路）
// - 第二层使用 Redis 计数（INCR + EXPIRE）配合客户端“事后判断”，短超时以保证低延迟
// - 支持路径级全局兜底阈值（global limit）、Biz 维度和 Token/IP 粒度三层限流
// - Redis 超时或错误时使用降级策略（采用严格或宽松本地限流）
func WriteRateLimiter(redisCluster *storage.RedisCluster, limit int, globalLimit int, globalFactor float64, window time.Duration) gin.HandlerFunc {
	windowSec := int64(window.Seconds())

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		spanCtx, span := trace.StartSpan(ctx, "middleware", "write-rate-limit")
		if spanCtx != nil {
			c.Request = c.Request.WithContext(spanCtx)
		}
		status := "success"
		spanCode := "0"
		spanDetails := map[string]interface{}{
			"path":         c.FullPath(),
			"window":       window.String(),
			"limit_config": limit,
			"globalLimit":  globalLimit,
		}
		defer func() {
			trace.EndSpan(span, status, spanCode, spanDetails)
		}()
		trace.AddRequestTag(spanCtx, "write_limit_path", c.FullPath())
		trace.AddRequestTag(spanCtx, "write_limit_window", window.String())
		// 初始记录配置中的 limit，后续如有 override 会再写入有效值
		trace.AddRequestTag(spanCtx, "write_limit_limit_config", limit)
		trace.AddRequestTag(spanCtx, "write_limit_global_limit", globalLimit)

		// 尝试读取写限流的“基础阈值覆盖”（override），短超时：
		// - Redis key ratelimit:write:global_limit 存的是「per-identifier 写限流 limit 的全局覆盖值」，而不是计数桶；
		// - 读取到有效数值后，会直接覆盖本次请求的 limit，后续本地/Redis 判定都基于新的 limit；
		// - 使用请求上下文作为父 ctx，确保请求被取消时该调用也能及时结束。
		ctxg, cancelg := context.WithTimeout(c.Request.Context(), 150*time.Millisecond)
		defer cancelg()
		globalOverrideKey := buildRedisKey(redisCluster, "ratelimit:write:global_limit")
		if redisCluster != nil && redisCluster.GetClient() != nil {
			if val, err := redisCluster.GetClient().Get(ctxg, globalOverrideKey).Result(); err == nil {
				if gLimit, perr := parseInt(val); perr == nil && gLimit > 0 {
					// 记录覆盖前后的 limit，确保 span 与 RequestContext.Extra 中看到的都是“生效后的”值
					spanDetails["limit_before_override"] = limit
					limit = gLimit
					spanDetails["limit"] = limit
					// 记录覆盖 key，便于排查
					spanDetails["limit_override_key"] = globalOverrideKey
					// 记录生效的 limit 和 override key
					trace.AddRequestTag(spanCtx, "write_limit_limit_effective", limit)
					trace.AddRequestTag(spanCtx, "write_limit_override_key", globalOverrideKey)
				}
			}
		}
		// 计算全局限流阈值
		effectiveGlobal := globalLimit
		if effectiveGlobal <= 0 && globalFactor > 0 {
			effectiveGlobal = int(float64(limit) * globalFactor)
		}
		if limit <= 0 && effectiveGlobal <= 0 {
			spanDetails["decision"] = "pass_disabled"
			c.Next()
			return
		}

		// 计算当前时间所在的窗口编号，用于与 Redis 对齐固定窗口：
		// windowID = floor(unixSeconds / windowSeconds)。
		now := time.Now()
		wid := windowIDForTime(now, window)

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
		authType := "anonymous"
		if auth := c.GetHeader("Authorization"); strings.TrimSpace(auth) != "" {
			authType = "authenticated"
		}

		// 解析业务维度（BizID）：通过路由信息解析出当前请求所属的业务，用于后续业务级限流/统计。
		// 业务级限流的阈值通过 bizid.SetBizLimit 在进程内配置或后续由表/配置装载；当 bizLimit<=0 时跳过业务级限流。
		bizKeyLabel := ""
		var bizIdentifier string
		bizLimit := 0
		if biz := bizid.ResolveBizByRoute(c.Request.Method, c.FullPath()); biz != nil && !biz.Deprecated {
			bizKeyLabel = biz.Key
			spanDetails["biz_key"] = biz.Key
			spanDetails["biz_name"] = biz.Name
			trace.AddRequestTag(spanCtx, "write_biz_key", biz.Key)
			if l := bizid.GetBizLimit(biz.Key); l > 0 {
				bizLimit = l
				bizIdentifier = "writebiz:" + biz.Key + ":" + idPart
				// 记录 Biz 维度的基础配置阈值
				spanDetails["biz_limit_config"] = bizLimit
				trace.AddRequestTag(spanCtx, "write_biz_limit_config", bizLimit)
				// Biz 维度同样支持通过 Redis 进行阈值覆盖，key 形如：ratelimit:write:biz_limit:<bizKey>
				if redisCluster != nil && redisCluster.GetClient() != nil {
					bizOverrideKey := buildRedisKey(redisCluster, fmt.Sprintf("ratelimit:write:biz_limit:%s", biz.Key))
					if val, err := redisCluster.GetClient().Get(ctxg, bizOverrideKey).Result(); err == nil {
						if bLimit, perr := parseInt(val); perr == nil && bLimit > 0 {
							spanDetails["biz_limit_before_override"] = bizLimit
							bizLimit = bLimit
							spanDetails["biz_limit"] = bizLimit
							spanDetails["biz_limit_override_key"] = bizOverrideKey
							trace.AddRequestTag(spanCtx, "write_biz_limit_effective", bizLimit)
							trace.AddRequestTag(spanCtx, "write_biz_limit_override_key", bizOverrideKey)
						}
					}
				}
			}
		}
		// Redis 计数 key 采用 windowID 分桶：同一窗口内使用相同的 key，不再依赖 TTL 的精确过期时间表达窗口边界。
		idKeyBase := "ratelimit:write:" + identifier
		globalKeyBase := "ratelimit:write:global:" + c.FullPath()
		rateLimitKey := buildRedisKey(redisCluster, fmt.Sprintf("%s:%d", idKeyBase, wid))
		globalPathKey := buildRedisKey(redisCluster, fmt.Sprintf("%s:%d", globalKeyBase, wid))
		// 业务级 Redis 计数 key：按 BizID + token/IP + 窗口分桶，实现跨多条路径的业务级分布式限流。
		bizRedisKey := rateLimitKey
		if bizIdentifier != "" {
			bizKeyBase := "ratelimit:" + bizIdentifier
			bizRedisKey = buildRedisKey(redisCluster, fmt.Sprintf("%s:%d", bizKeyBase, wid))
		}

		// 本地快速检查（复用 localRateCheck 保持一致策略）
		if !localRateCheck(identifier, limit, window) {
			status = "error"
			spanCode = "429_local_rate"
			spanDetails["decision"] = "block_local"
			spanDetails["identifier"] = identifier
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    codepkg.ErrRateLimitExceeded,
				"message": "写操作过于频繁，请稍后再试（本地限流）",
				"data":    nil,
			})
			// 记录本地限流事件
			metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "local_rate").Inc()
			// 记录本地限流请求
			metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_local_rate", authType, bizKeyLabel).Inc()
			return
		}
		// 业务级本地限流：当为某个 BizID 配置了 bizLimit 时，对同一 token/IP 在该业务下的总写入进行统一限流。
		if bizIdentifier != "" && bizLimit > 0 {
			if !localRateCheck(bizIdentifier, bizLimit, window) {
				status = "error"
				spanCode = "429_local_biz"
				spanDetails["decision"] = "block_local_biz"
				spanDetails["identifier"] = bizIdentifier
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    codepkg.ErrRateLimitExceeded,
					"message": "[写限流]该业务写操作过于频繁，请稍后再试（业务级本地限流）",
					"data":    nil,
				})
				metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "local_biz").Inc()
				metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_local_biz", authType, bizKeyLabel).Inc()
				return
			}
		}
		if effectiveGlobal > 0 {
			globalLocalID := "write:global:" + c.FullPath()
			if !localRateCheck(globalLocalID, effectiveGlobal, window) {
				status = "error"
				spanCode = "429_local_global"
				spanDetails["decision"] = "block_local_global"
				spanDetails["identifier"] = globalLocalID
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    codepkg.ErrRateLimitExceeded,
					"message": "[写限流]写操作过于频繁，请稍后再试（全局本地限流）",
					"data":    nil,
				})
				metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "local_global").Inc()
				metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_local_global", authType, bizKeyLabel).Inc()
				return
			}
		}

		// Redis 限流，短超时；继承请求 ctx，避免请求结束后仍然占用资源
		ctx, cancel := context.WithTimeout(c.Request.Context(), 200*time.Millisecond)
		defer cancel()

		client := redisCluster.GetClient()
		if client == nil {
			log.Warn("WriteRateLimiter: redis client is nil, fallback to strict local limiting")
			if !strictLocalRateCheck(identifier, limit) {
				status = "error"
				spanCode = "429_redis_unavailable"
				spanDetails["decision"] = "block_local_strict"
				spanDetails["identifier"] = identifier
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    codepkg.ErrRateLimitExceeded,
					"message": "[写限流]系统繁忙，请稍后再试（降级-本地备用方案）",
					"data":    nil,
				})
				metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "redis_timeout").Inc()
				metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_redis_unavailable", authType, bizKeyLabel).Inc()
				return
			}
			// 记录降级放行事件
			metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "allowed_degraded_local", authType, bizKeyLabel).Inc()
			c.Next()
			return
		}

		// 通过 pipeline 一次性对全局 / 业务 / 标识三个维度进行 INCR+EXPIRE，然后在客户端按顺序做“事后判断”。
		var globalIncr, bizIncr, idIncr *redis.IntCmd
		ttlDuration := time.Duration(windowSec*2) * time.Second // TTL 仅用于 GC，不再承载精确窗口边界
		_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			if effectiveGlobal > 0 {
				globalIncr = pipe.Incr(ctx, globalPathKey)
				pipe.Expire(ctx, globalPathKey, ttlDuration)
			}
			if bizLimit > 0 && bizIdentifier != "" {
				bizIncr = pipe.Incr(ctx, bizRedisKey)
				pipe.Expire(ctx, bizRedisKey, ttlDuration)
			}
			if limit > 0 {
				idIncr = pipe.Incr(ctx, rateLimitKey)
				pipe.Expire(ctx, rateLimitKey, ttlDuration)
			}
			return nil
		})

		if err != nil {
			// 降级到严格本地限流
			log.Warnf("Redis限流失败，降级本地处理: %v", err)
			spanDetails["redis_error"] = err.Error()
			if !strictLocalRateCheck(identifier, limit) {
				status = "error"
				spanCode = "429_redis_timeout"
				spanDetails["decision"] = "block_local_strict"
				spanDetails["identifier"] = identifier
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    codepkg.ErrRateLimitExceeded,
					"message": "[写限流]系统繁忙，请稍后再试（降级）",
					"data":    nil,
				})
				metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), "redis_timeout").Inc()
				metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_redis_timeout", authType, bizKeyLabel).Inc()
				return
			}
			metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "allowed_degraded_local", authType, bizKeyLabel).Inc()
			c.Next()
			return
		}

		var (
			currentGlobal int64
			currentBiz    int64
			currentID     int64
		)
		if globalIncr != nil {
			currentGlobal = globalIncr.Val()
		}
		if bizIncr != nil {
			currentBiz = bizIncr.Val()
		}
		if idIncr != nil {
			currentID = idIncr.Val()
		}

		limited := false
		retryAfter := int64(0)
		scope := "ok"

		// 先验全局，再验业务级，再验标识级，保持与原 Lua 逻辑顺序一致。
		if effectiveGlobal > 0 && currentGlobal > int64(effectiveGlobal) {
			limited = true
			scope = "global"
			// 估算 retry-after 秒数 基于当前窗口长度和 Redis TTL
			// 避免每次请求都额外打一条 TTL，这里只在已经判定超限的维度上查询 TTL；
			// 若 TTL 查询失败，则退回使用 windowSec 作为一个保守估计。
			//用户端可基于该值决定重试时间。
			retryAfter = estimateRetryAfterSeconds(ctx, client, globalPathKey, windowSec)
		} else if bizLimit > 0 && currentBiz > int64(bizLimit) {
			limited = true
			scope = "biz"
			retryAfter = estimateRetryAfterSeconds(ctx, client, bizRedisKey, windowSec)
		} else if limit > 0 && currentID > int64(limit) {
			limited = true
			scope = "identifier"
			retryAfter = estimateRetryAfterSeconds(ctx, client, rateLimitKey, windowSec)
		}

		if limited {
			status = "error"
			spanCode = "429_redis_limit"
			spanDetails["decision"] = "block_redis"
			spanDetails["identifier"] = identifier
			spanDetails["scope"] = scope
			spanDetails["retry_after"] = retryAfter
			message := "写操作过于频繁，请稍后再试"
			if scope == "global" {
				message = "写操作过于频繁，请稍后再试（全局兜底）"
			} else if scope == "biz" {
				message = "[写限流]该业务写操作过于频繁，请稍后再试（业务级分布式限流）"
			}
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    codepkg.ErrRateLimitExceeded,
				"message": message,
				"data": gin.H{
					"retry_after": fmt.Sprintf("%d秒", retryAfter),
					"scope":       scope,
				},
			})
			label := "redis_limit"
			if scope == "global" {
				label = "redis_limit_global"
			} else if scope == "biz" {
				label = "redis_limit_biz"
			}
			metrics.WriteLimiterTotal.WithLabelValues(c.FullPath(), label).Inc()
			metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "blocked_"+label, authType, bizKeyLabel).Inc()
			return
		}

		spanDetails["decision"] = "pass"
		spanDetails["identifier"] = identifier
		// 记录允许通过的请求(成功量)，按 authType 和 bizKey 维度细划分
		metrics.WriteLimiterRequestsTotal.WithLabelValues(c.FullPath(), "allowed", authType, bizKeyLabel).Inc()

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

// estimateRetryAfterSeconds 根据当前窗口长度和 Redis 中的 TTL 估算 retry-after 秒数。
// 为避免每次请求都额外打一条 TTL，这里只在已经判定超限的维度上查询 TTL；
// 若 TTL 查询失败，则退回使用 windowSec 作为一个保守估计。
func estimateRetryAfterSeconds(ctx context.Context, client redis.Cmdable, key string, windowSec int64) int64 {
	if client == nil || key == "" {
		if windowSec > 0 {
			return windowSec
		}
		return 0
	}
	ttl, err := client.TTL(ctx, key).Result()
	if err != nil || ttl <= 0 {
		if windowSec > 0 {
			return windowSec
		}
		return 0
	}
	seconds := int64(ttl.Seconds())
	if seconds <= 0 && windowSec > 0 {
		return windowSec
	}
	return seconds
}
