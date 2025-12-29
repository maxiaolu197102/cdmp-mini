package common

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

// TrafficHooks bundles the shared write path guards (coordinator + lag protect + limiter)
// so each service can plug them in via configuration instead of rewriting wiring code.
type TrafficHooks struct {
	Coordinator *usercache.PendingCoordinator
	LagProtect  gin.HandlerFunc
	WriteLimit  gin.HandlerFunc
}

// TrafficHookConfig 描述如何装配通用流量护栏（协调器 + 滞后保护 + 写限流）。
type TrafficHookConfig struct {
	// 可选：直接传入已构建好的协调器；为空则按配置+Redis 生成。
	Coordinator *usercache.PendingCoordinator
	// 协调器模板：当 Coordinator 为空时，用它生成实例。
	CoordinatorConfig *usercache.PendingCoordinatorConfig
	// 服务级 Pending 统计 key 前缀（用于全局活跃计数）。
	PendingMetricsKey string
	// 服务级用户深度统计 key 前缀（用于用户粒度队列深度）。
	UserMetricsPrefix string
	// Redis 后端（协调器/限流器依赖）。
	Redis *storage.RedisCluster
	// 组件标识，用于指标/日志标签以及默认 key 前缀。
	Component string

	// 写限流：标识粒度（token/IP+路径）阈值，<=0 表示关闭。
	WriteLimit int
	// 写限流：路径级全局兜底阈值，避免单实例/单身份绕过；<=0 表示关闭。
	WriteLimitGlobal int
	// 写限流：当未显式配置全局阈值时，用标识阈值乘以该系数推导全局阈值（典型 2~5）。
	WriteLimitGlobalFactor float64
	// 写限流窗口，固定窗口大小；常用 1min。
	WriteWindow time.Duration

	// 滞后保护配置（内部会自动注入协调器）。
	LagProtect LagProtectOptions
}


// NewTrafficHooks 构建一个可复用的包，包含 PendingCoordinator、滞后保护中间件和写入限制器中间件。调用者可以在路由组之间复用相同的钩子。
// 如果提供了 Coordinator，则使用它；否则，如果提供了 CoordinatorConfig 和 Redis，则根据默认值创建一个新的协调器。
// 写入限制器使用提供的 Redis 实例和阈值参数进行配置。
// 滞后保护中间件使用提供的协调器进行配置。
// 这样，调用者可以轻松地为不同的服务路由组设置一致的流量控制和保护机制，而无需重复编写相同的逻辑。
func NewTrafficHooks(cfg TrafficHookConfig) TrafficHooks {
	coord := cfg.Coordinator
	if coord == nil && cfg.Redis != nil {
		var base usercache.PendingCoordinatorConfig
		source := cfg.CoordinatorConfig
		if source != nil {
			cloned := *source
			base = cloned
		} else if cfg.Component != "" {
			base = usercache.DefaultPendingCoordinatorConfig(cfg.Component)
		}
		if cfg.Component != "" && base.Component == "" {
			base.Component = cfg.Component
		}
		if cfg.PendingMetricsKey != "" {
			base.MetricsKey = cfg.PendingMetricsKey
		}
		if cfg.UserMetricsPrefix != "" {
			base.UserMetricsPrefix = cfg.UserMetricsPrefix
		}
		if base.Component != "" {
			coord = usercache.NewPendingCoordinator(cfg.Redis, base)
		}
	}

	lagOpts := cfg.LagProtect
	lagOpts.Coordinator = coord
	lagProtect := LagProtectMiddleware(lagOpts)

	window := cfg.WriteWindow
	if window <= 0 {
		window = time.Minute
	}

	writeLimit := func(c *gin.Context) { c.Next() }
	if cfg.Redis != nil {
		writeLimit = WriteRateLimiter(cfg.Redis, cfg.WriteLimit, cfg.WriteLimitGlobal, cfg.WriteLimitGlobalFactor, window)
	}

	return TrafficHooks{
		Coordinator: coord,
		LagProtect:  lagProtect,
		WriteLimit:  writeLimit,
	}
}
