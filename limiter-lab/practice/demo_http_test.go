package practice

import "testing"

// TestDemo_LocalLimiterAndRedisScript：
//   - 组合本地 FixedWindowLimiter 的 HTTPMiddleware + Redis 脚本中间件；
//   - 演示一次请求依次经过本地限流和 Redis 脚本两层检查；
//   - 本地层 limit 设得较大，主要由 Redis 层决定是否被限流。
//
// 验收：
//   - 构造一个集成测试，其中本地 limiter 总是放行，而 Redis 脚本在达到 globalLimit 或 idLimit 后返回限流；
//   - 通过断言 HTTP 返回码、下游 handler 调用次数以及 Redis 端计数/TTL，验证整条链路 "本地层 → 脚本层 → handler" 按预期工作。
// 建议在完成前面几个练习后，再回来补这个 Demo 测试。
func TestDemo_LocalLimiterAndRedisScript(t *testing.T) {
t.Skip("TODO: 在完成前面练习后，实现完整 HTTP Demo 测试，并删除本行")
}

