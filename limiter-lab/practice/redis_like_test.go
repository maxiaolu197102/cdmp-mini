package practice

import "testing"

// TestEvalWriteLimiterScript_IdentifierLimit：
//   - 只设置 idLimit，验证标识粒度限流的行为；
//   - 前几次通过，第 N+1 次被限流，跨窗口后重新计数。
//
// 验收：
//   - 使用 fakeClock 控制时间推进，验证在同一窗口内前 N 次 limited=false，第 N+1 次开始 limited=true 且 scope="identifier"；
//   - 推进时间跨过一个完整窗口后，再次请求应恢复为 limited=false，计数从头开始。
// TODO: 自己设计 fakeClock + 内存 RedisLikeStore，并补充断言后删除 t.Skip。
func TestEvalWriteLimiterScript_IdentifierLimit(t *testing.T) {
	t.Skip("TODO: 实现标识粒度脚本测试，并删除本行")
}

// TestEvalWriteLimiterScript_GlobalLimit：
//   - 同时设置 globalLimit 和 idLimit，验证全局优先生效；
//   - 前几次通过，第 N+1 次被 scope="global" 限流。
//
// 验收：
//   - 构造多个并发/顺序请求，确保在同一窗口内，达到 globalLimit 后，即便是新 identifier 也会被 limited=true 且 scope="global"；
//   - 验证 retryAfter 与全局 key 的 TTL 对齐，窗口结束后请求重新放行。
// TODO: 自己设计时间推进和断言后删除 t.Skip。
func TestEvalWriteLimiterScript_GlobalLimit(t *testing.T) {
	t.Skip("TODO: 实现全局脚本测试，并删除本行")
}
