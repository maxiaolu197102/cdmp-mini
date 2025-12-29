package practice

import "net/http"

// HTTPMiddleware 是基于 Limiter 的 HTTP 中间件骨架：
//   - 从请求中提取 key（例如 UserID 或 IP）；
//   - 调用 limiter.Allow(key) 判断是否允许；
//   - 超限时返回 429 Too Many Requests。
//
// 验收：
//   - 当 limiter.Allow 返回 true 时，请求应继续传递到 next，状态码由下游 handler 决定；
//   - 当 limiter.Allow 返回 false 时，应直接返回 429，且下游 handler 不被调用（可在测试中统计调用次数）。
// TODO: 按 checklist 完成实现逻辑。
func HTTPMiddleware(l Limiter, keyFunc func(r *http.Request) string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// TODO: 实现 key 提取 + 调用 limiter + 429 响应
			next.ServeHTTP(w, r)
		})
	}
}
