package common

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

// - 背压数据源协调器
// LagProtectOptions controls how LagProtectMiddleware evaluates backpressure state.
type LagProtectOptions struct {
	// Coordinator provides backpressure samples sourced from pending lease metrics.
	Coordinator *usercache.PendingCoordinator
	// RejectLevel defines the minimum backpressure level that should trigger rejection.
	// Defaults to usercache.BackpressureSevere when unset.
	//背压触发阈值
	RejectLevel usercache.BackpressureLevel
	// SampleInterval deduplicates expensive Redis reads by caching the sampled depth locally.
	// Defaults to 200ms when unset or non-positive.
	SampleInterval time.Duration
	// FailOnError decides whether to fail closed (HTTP 429) when sampling fails.
	// By default the middleware is fail-open to avoid cascading outages.
	FailOnError bool
}

type lagProtectSample struct {
	at     time.Time
	sample usercache.BackpressureSample
}

// LagProtectMiddleware evaluates pending queue depth via PendingCoordinator and
// returns 429 when backpressure exceeds configured thresholds.
func LagProtectMiddleware(opts LagProtectOptions) gin.HandlerFunc {
	coord := opts.Coordinator
	// 路由阶段只采样队列深度，不启用心跳检测（HeartbeatTimeout=0）；心跳检测仅在租约阶段执行。
	if coord == nil {
		// No coordinator available, the middleware should be a no-op.
		return func(c *gin.Context) { c.Next() }
	}

	rejectLevel := opts.RejectLevel
	if rejectLevel == "" {
		rejectLevel = usercache.BackpressureSevere
	}

	sampleInterval := opts.SampleInterval
	if sampleInterval <= 0 {
		sampleInterval = 200 * time.Millisecond
	}

	component := coord.ComponentName()
	if component == "" {
		component = "lag_protect"
	}
	/*
		coord := opts.Coordinator：PendingCoordinator 实例引用，用来实时采样队列/背压（调用 SampleBackpressure）。
		• cached lagProtectSample：一个本地缓存结构，保存上次采样的结果和时间戳（at + sample）。
		• 关联：请求进来时先看缓存是否过期（SampleInterval）；过期则用 coord 重新采样并更新 cached；未过期则直接复用 cached.sample，减少 Redis 读取和采样开销。
	*/
	var (
		mu      sync.Mutex
		cached  lagProtectSample
		failErr = opts.FailOnError
	)

	return func(c *gin.Context) {
		//1. 初始上下文（空）→ 2. 认证中间件（auto.AuthFunc()）注入 "operator": "admin" → 3. 滞后保护中间件（lagProtect）注入 BackpressureSample → 4. Trace 中间件注入 trace_id → 最终形成嵌套的 valueCtx 链
		ctx := c.Request.Context()
		username := strings.TrimSpace(c.GetString(UsernameKey))
		now := time.Now()

		mu.Lock()
		cachedSample := cached
		useCache := username == ""
		//单用户请求时不使用缓存，确保每个用户都能独立采样其队列深度
		if useCache {
			//：缓存未初始化（at为零值） 或 缓存已过期（当前时间-缓存时间 ≥ 采样间隔）
			if cachedSample.at.IsZero() || now.Sub(cachedSample.at) >= sampleInterval {
				mu.Unlock()
				liveSample := coord.SampleBackpressure(ctx, "")
				cachedSample = lagProtectSample{at: now, sample: liveSample}
				mu.Lock()
				cached = cachedSample
			}
			mu.Unlock()
		} else {
			mu.Unlock()
			liveSample := coord.SampleBackpressure(ctx, username)
			cachedSample = lagProtectSample{at: now, sample: liveSample}
		}

		sample := cachedSample.sample
		if _, ok := usercache.BackpressureSampleFromContext(ctx); !ok {
			ctx = usercache.ContextWithBackpressureSample(ctx, sample)
			c.Request = c.Request.WithContext(ctx)
		}

		if sample.GlobalErr != nil {
			log.Warnw("lag protect sampling failed", "component", component, "error", sample.GlobalErr, "path", c.FullPath())
			if failErr {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    code.ErrRateLimitExceeded,
					"message": "[队列延迟限流]系统暂时无法确认写入背压状态，请稍后重试。",
				})
				return
			}
			c.Next()
			return
		}

		trace.AddRequestTag(ctx, "lag_protect_level", string(sample.AggregateLevel))
		trace.AddRequestTag(ctx, "lag_protect_depth", sample.GlobalDepth)

		c.Header("X-Pending-Backpressure", string(sample.AggregateLevel))
		c.Header("X-Pending-Queue-Depth", strconv.FormatInt(sample.GlobalDepth, 10))

		if sample.ShouldReject(rejectLevel) {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(component, "http_backpressure_reject").Inc()
			}

			retryAfter := "1"
			if sampleInterval >= time.Second {
				secs := int(sampleInterval / time.Second)
				if secs > 1 {
					retryAfter = strconv.Itoa(secs)
				}
			}
			c.Header("Retry-After", retryAfter)

			depth := sample.MaxDepth()
			message := fmt.Sprintf("队列深度 %d 超过保护阈值，系统处于背压状态，请稍后再试。", depth)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    code.ErrRateLimitExceeded,
				"message": message,
			})
			return
		}

		c.Next()
	}
}
