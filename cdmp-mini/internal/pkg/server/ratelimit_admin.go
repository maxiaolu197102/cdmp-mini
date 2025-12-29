package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	apiserveropts "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	coreerrors "github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/redis/go-redis/v9"
)

// 查询当前限流配置
func RegisterRateLimitAdminHandlers(rg *gin.RouterGroup, redisCluster *storage.RedisCluster, opts *apiserveropts.Options) {
	// /ratelimit/write 管理接口：读写的是「写限流 per-identifier 基础阈值的全局覆盖值」，而不是 Redis 计数桶。
	// 该值会被 WriteRateLimiter 读取，用来覆盖每次请求的 limit（从而间接影响路径级全局阈值 effectiveGlobal）。
	rg.GET("/ratelimit/write", func(c *gin.Context) {
		if !isLocalOrDebug(c, opts) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		key := ""
		metaKey := ""
		if redisCluster != nil {
			key = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit")
			metaKey = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit:meta")
		} else {
			key = "ratelimit:write:global_limit"
			metaKey = "ratelimit:write:global_limit:meta"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		if redisCluster == nil {
			// No Redis configured — return configured default write limit if available
			core.WriteResponse(c, nil, gin.H{"value": opts.ServerRunOptions.WriteRateLimit, "ttl_seconds": 0, "source": "no_redis"})
			return
		}
		// Prefer an explicit availability check so we return a clear no_redis response
		if err := redisCluster.Up(); err != nil {
			// Redis exists but is down — return configured default so admins get a meaningful number
			log.Warnf("redis up check failed for keys %s / %s: %v", key, metaKey, err)
			core.WriteResponse(c, nil, gin.H{"value": opts.ServerRunOptions.WriteRateLimit, "ttl_seconds": 0, "source": "no_redis"})
			return
		}

		client := redisCluster.GetClient()
		val, err := client.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// key not found — return configured default so callers see the effective limit
				core.WriteResponse(c, nil, gin.H{"value": opts.ServerRunOptions.WriteRateLimit, "ttl_seconds": 0, "source": "default"})
				return
			}
			core.WriteResponse(c, nil, gin.H{"value": nil, "error": err.Error()})
			return
		}
		ttl, _ := client.TTL(ctx, key).Result()
		source := "unknown"
		if metaVal, err := client.Get(ctx, metaKey).Result(); err == nil {
			source = metaVal
		}
		core.WriteResponse(c, nil, gin.H{"value": val, "ttl_seconds": int(ttl.Seconds()), "source": source})
	})

	type setReq struct {
		Value int `json:"value" binding:"required,min=1"`
		TTL   int `json:"ttl_seconds" binding:"omitempty,min=1"`
	}
	// 设置写限流基础阈值覆盖（全局配置），影响所有 token/IP + path 的 per-identifier limit
	rg.POST("/ratelimit/write", func(c *gin.Context) {
		// If AdminToken is set, require it. Otherwise fall back to local/debug only.
		if opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else {
			if !isLocalOrDebug(c, opts) {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
		}
		var req setReq
		if err := c.ShouldBindJSON(&req); err != nil {
			core.WriteResponse(c, err, nil)
			return
		}
		key := ""
		metaKey := ""
		if redisCluster != nil {
			key = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit")
			metaKey = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit:meta")
		} else {
			key = "ratelimit:write:global_limit"
			metaKey = "ratelimit:write:global_limit:meta"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if redisCluster == nil {
			log.Warn("redisCluster is nil when trying to set ratelimit")
			core.WriteResponse(c, nil, gin.H{"result": "no_redis"})
			return
		}
		if err := redisCluster.Up(); err != nil {
			log.Warnf("redis up check failed when setting keys %s / %s: %v", key, metaKey, err)
			core.WriteResponse(c, nil, gin.H{"result": "no_redis"})
			return
		}
		if req.TTL > 0 {
			if err := redisCluster.GetClient().Set(ctx, key, req.Value, time.Duration(req.TTL)*time.Second).Err(); err != nil {
				core.WriteResponse(c, err, nil)
				return
			}
			// store source info with same TTL
			source := requesterSource(c, opts)
			_ = redisCluster.GetClient().Set(ctx, metaKey, source, time.Duration(req.TTL)*time.Second).Err()
		} else {
			if err := redisCluster.GetClient().Set(ctx, key, req.Value, 0).Err(); err != nil {
				core.WriteResponse(c, err, nil)
				return
			}
			source := requesterSource(c, opts)
			_ = redisCluster.GetClient().Set(ctx, metaKey, source, 0).Err()
		}
		log.Debugf("Set global write ratelimit=%d ttl=%d", req.Value, req.TTL)
		core.WriteResponse(c, nil, gin.H{"result": "ok"})
	})

	// DELETE handler to remove the写限流基础阈值覆盖 key，让系统回退到配置文件中的默认 limit
	rg.DELETE("/ratelimit/write", func(c *gin.Context) {
		// auth same as POST
		if opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else {
			if !isLocalOrDebug(c, opts) {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
		}
		key := ""
		metaKey := ""
		if redisCluster != nil {
			key = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit")
			metaKey = fmt.Sprintf("%s%s", redisCluster.KeyPrefix, "ratelimit:write:global_limit:meta")
		} else {
			key = "ratelimit:write:global_limit"
			metaKey = "ratelimit:write:global_limit:meta"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if redisCluster == nil {
			log.Warn("redisCluster is nil when trying to delete ratelimit keys")
			core.WriteResponse(c, nil, gin.H{"result": "no_redis"})
			return
		}
		if err := redisCluster.Up(); err != nil {
			log.Warnf("redis up check failed when deleting keys %s / %s: %v", key, metaKey, err)
			core.WriteResponse(c, nil, gin.H{"result": "no_redis"})
			return
		}
		// delete both key and meta
		if err := redisCluster.GetClient().Del(ctx, key).Err(); err != nil {
			core.WriteResponse(c, err, nil)
			return
		}
		_ = redisCluster.GetClient().Del(ctx, metaKey).Err()
		core.WriteResponse(c, nil, gin.H{"result": "deleted"})
	})
}

type loginLimitRequest struct {
	Value int `json:"value" binding:"required"`
}

func RegisterLoginLimitHandlers(rg *gin.RouterGroup, srv *GenericAPIServer, opts *apiserveropts.Options) {
	if rg == nil || srv == nil {
		return
	}
	rg.GET("/ratelimit/login", func(c *gin.Context) {
		if opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else if !isLocalOrDebug(c, opts) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		current := int(srv.loginLimit.Load())
		if current <= 0 {
			current = opts.ServerRunOptions.LoginRateLimit
		}
		core.WriteResponse(c, nil, gin.H{
			"value":     current,
			"window":    opts.ServerRunOptions.LoginWindow.String(),
			"effective": current > 0,
		})
	})

	rg.POST("/ratelimit/login", func(c *gin.Context) {
		if opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else if !isLocalOrDebug(c, opts) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		var req loginLimitRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			core.WriteResponse(c, err, nil)
			return
		}
		if req.Value < 0 {
			core.WriteResponse(c, coreerrors.WithCode(code.ErrInvalidParameter, "限流值不能为负数"), nil)
			return
		}
		srv.loginLimit.Store(int64(req.Value))
		core.WriteResponse(c, nil, gin.H{"result": "ok", "value": req.Value})
	})

	rg.DELETE("/ratelimit/login", func(c *gin.Context) {
		if opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else if !isLocalOrDebug(c, opts) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		defaultLimit := opts.ServerRunOptions.LoginRateLimit
		srv.loginLimit.Store(int64(defaultLimit))
		core.WriteResponse(c, nil, gin.H{"result": "reset", "value": defaultLimit})
	})
}

func isLocalOrDebug(c *gin.Context, opts *apiserveropts.Options) bool {
	if opts.ServerRunOptions.Mode != "release" {
		return true
	}
	ip := c.ClientIP()
	return ip == "127.0.0.1" || ip == "::1" || ip == "192.168.10.8"
}

// requesterSource returns a short string describing who set the value: prefer token if present, else client IP
func requesterSource(c *gin.Context, opts *apiserveropts.Options) string {
	if opts.ServerRunOptions.AdminToken != "" {
		provided := c.GetHeader("X-Admin-Token")
		if provided != "" && provided == opts.ServerRunOptions.AdminToken {
			return "token"
		}
	}
	return c.ClientIP()
}
