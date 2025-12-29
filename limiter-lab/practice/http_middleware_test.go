package practice

import "testing"

// TestHTTPMiddleware_FixedWindow_TooManyRequests：
//   - 使用 httptest 模拟多次 HTTP 请求；
//   - 期望前 N 次 200，第 N+1 次 429；
//   - 统计下游 handler 实际被调用次数。
//
// 验收：
//   - 根据 limit 构造请求序列：前 limit 次响应应为 200，之后的请求应为 429；
//   - 下游 handler 实际被调用次数应恰好等于成功通过的请求次数，不会在被限流时继续执行。
// TODO: 结合 fixedwindow.go 中的实现，自己编写测试逻辑后删除 t.Skip。
func TestHTTPMiddleware_FixedWindow_TooManyRequests(t *testing.T) {
	t.Skip("TODO: 实现 HTTP 中间件测试，并删除本行")
}
