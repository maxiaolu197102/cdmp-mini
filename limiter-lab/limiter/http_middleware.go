package limiter

import (
	"crypto/sha1"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Metrics 是一个极简的指标接口，方便在中间件中打点。
//
// 真实项目里会使用 Prometheus 等，这里只保留 Inc 语义方便单测。
type Metrics interface {
	Inc(path, reason string)
}

// OverrideProvider 用于根据 path 返回一个动态覆盖阈值（若存在）。
//
// 这对应生产代码里的 "global_override_key" 读取逻辑，在 lab 中通过内存实现和单测驱动。
type OverrideProvider interface {
	LimitFor(path string) (limit int, ok bool)
}

// HTTPOptions 用于配置中间件的高级能力：
//   - KeyFunc：自定义标识粒度 key（默认：token/IP + path）；
//   - GlobalKeyFunc：自定义全局 key（默认：按 path）；
//   - GlobalLimiter：可选的全局固定窗口限流器；
//   - Metrics：可选的指标钩子；
//   - PathFunc：自定义 path 提取逻辑（默认：r.URL.Path）。
type HTTPOptions struct {
	KeyFunc       func(r *http.Request) string
	GlobalKeyFunc func(r *http.Request) string
	GlobalLimiter Limiter
	Metrics       Metrics
	PathFunc      func(r *http.Request) string
	OverrideProvider OverrideProvider
}

// HTTPMiddleware 是简化版入口，保留原有签名，内部委托给带 options 的实现。
//
// 注意：默认 key 生成规则已经升级为 token/IP + path，
// 与生产 WriteRateLimiter 更加接近。
func HTTPMiddleware(l Limiter, keyFunc func(r *http.Request) string) func(next http.Handler) http.Handler {
	opts := &HTTPOptions{}
	if keyFunc != nil {
		opts.KeyFunc = keyFunc
	}
	return NewHTTPMiddleware(l, nil, nil, opts)
}

// NewHTTPMiddleware 返回一个更接近生产语义的写限流中间件：
//   - 先按「标识粒度」（token/IP + path）限流；
//   - 若配置了 GlobalLimiter，再按「路径级全局」限流；
//   - 若配置了 Metrics，则按 path + reason 打点。
func NewHTTPMiddleware(l Limiter, global Limiter, m Metrics, opts *HTTPOptions) func(next http.Handler) http.Handler {
	if opts == nil {
		opts = &HTTPOptions{}
	}

	// 默认提取 path。
	pathFunc := opts.PathFunc
	if pathFunc == nil {
		pathFunc = func(r *http.Request) string {
			return r.URL.Path
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := pathFunc(r)

			// 标识粒度 key：优先使用用户自定义，其次使用默认规则（token/IP + path）。
			var idKey string
			if opts.KeyFunc != nil {
				idKey = opts.KeyFunc(r)
			} else {
				idKey = buildIdentifierKey(r)
			}

			// 动态覆盖当前 path 的有效阈值（若存在 OverrideProvider 且底层限流器支持）。
			limitOverride := 0
			if opts.OverrideProvider != nil {
				if lmt, ok := opts.OverrideProvider.LimitFor(path); ok && lmt > 0 {
					limitOverride = lmt
				}
			}

			allowed := false
			if oc, ok := l.(OverrideCapable); ok && limitOverride > 0 {
				allowed = oc.AllowWithOverride(idKey, limitOverride)
			} else {
				allowed = l.Allow(idKey)
			}

			if !allowed {
				if m != nil {
					m.Inc(path, "identifier")
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("too many requests"))
				return
			}

			// 路径级全局限流（可选）。
			if global != nil {
				var gKey string
				if opts.GlobalKeyFunc != nil {
					gKey = opts.GlobalKeyFunc(r)
				} else {
					gKey = buildGlobalKey(r)
				}

				if !global.Allow(gKey) {
					if m != nil {
						m.Inc(path, "global")
					}
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte("too many requests (global)"))
					return
				}
			}

			if m != nil {
				m.Inc(path, "pass")
			}

			next.ServeHTTP(w, r)
		})
	}
}

// buildIdentifierKey 尽量复用生产中的“token/IP + path”逻辑：
//   - 优先从 Authorization: Bearer <token> 获取 token；
//   - 使用 token 的 sha1 前缀作为 ID，避免长 token 直接入 key；
//   - 若无 token，则退回到 RemoteAddr；
//   - 最终格式："write:" + idPart + ":" + path。
func buildIdentifierKey(r *http.Request) string {
	path := r.URL.Path
	idPart := r.RemoteAddr
	if auth := r.Header.Get("Authorization"); strings.TrimSpace(auth) != "" {
		ah := strings.TrimSpace(auth)
		if strings.HasPrefix(strings.ToLower(ah), "bearer ") {
			ah = strings.TrimSpace(ah[7:])
		}
		if ah != "" {
			h := sha1.Sum([]byte(ah))
			idPart = fmt.Sprintf("%x", h)[:16]
		}
	}
	return "write:" + idPart + ":" + path
}

// buildGlobalKey 仅使用 path 作为全局 key。
func buildGlobalKey(r *http.Request) string {
	path := r.URL.Path
	return "write:global:" + path
}

// RedisScriptMiddleware 是一个额外的中间件层，用于在 HTTP 请求中模拟
// 生产环境下通过 Lua 脚本进行的 Redis 限流逻辑。
//
// 每个请求会：
//   1. 基于 Authorization/IP + path 构造 idKey；
//   2. 基于 path 构造 globalKey；
//   3. 调用 EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)；
//   4. 若被限流，则直接返回 429；否则继续调用下游 handler。
func RedisScriptMiddleware(store RedisLikeStore, globalLimit, idLimit int, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			idKey := buildIdentifierKey(r)
			globalKey := buildGlobalKey(r)

			limited, retryAfter, scope := EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
			if limited {
				// 简化版响应：仅演示 scope 与 retry_after 语义。
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprintf(w, "limited by %s, retry_after=%d", scope, retryAfter)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

