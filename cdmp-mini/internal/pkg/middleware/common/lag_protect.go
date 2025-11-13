package common

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

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
	at    time.Time
	depth int64
	level usercache.BackpressureLevel
	err   error
}

// LagProtectMiddleware evaluates pending queue depth via PendingCoordinator and
// returns 429 when backpressure exceeds configured thresholds.
func LagProtectMiddleware(opts LagProtectOptions) gin.HandlerFunc {
	coord := opts.Coordinator
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

	var (
		mu      sync.Mutex
		cached  lagProtectSample
		failErr = opts.FailOnError
	)

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		now := time.Now()

		mu.Lock()
		sample := cached
		if sample.at.IsZero() || now.Sub(sample.at) >= sampleInterval {
			mu.Unlock()
			depth, level, err := coord.SampleQueueDepth(ctx)
			sample = lagProtectSample{
				at:    now,
				depth: depth,
				level: level,
				err:   err,
			}
			mu.Lock()
			cached = sample
		}
		mu.Unlock()

		if sample.err != nil {
			log.Warnw("lag protect sampling failed", "component", component, "error", sample.err, "path", c.FullPath())
			if failErr {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"code":    429,
					"message": "系统暂时无法确认写入背压状态，请稍后重试。",
				})
				return
			}
			c.Next()
			return
		}

		// Propagate sampling information for tracing & observability.
		trace.AddRequestTag(ctx, "lag_protect_level", string(sample.level))
		trace.AddRequestTag(ctx, "lag_protect_depth", sample.depth)

		c.Header("X-Pending-Backpressure", string(sample.level))
		c.Header("X-Pending-Queue-Depth", strconv.FormatInt(sample.depth, 10))

		if shouldRejectBackpressure(sample.level, rejectLevel) {
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

			message := fmt.Sprintf("队列深度 %d 超过保护阈值，系统处于背压状态，请稍后再试。", sample.depth)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    429,
				"message": message,
			})
			return
		}

		c.Next()
	}
}

func shouldRejectBackpressure(current, threshold usercache.BackpressureLevel) bool {
	return backpressureRank(current) >= backpressureRank(threshold)
}

func backpressureRank(level usercache.BackpressureLevel) int {
	switch level {
	case usercache.BackpressureSevere:
		return 2
	case usercache.BackpressureElevated:
		return 1
	default:
		return 0
	}
}
