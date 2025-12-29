package practice

import "time"

// RedisLikeStore 抽象出一组接近 Redis 的基本操作：
//   - Incr: 自增计数并返回最新值；
//   - Expire: 设置过期时间；
//   - TTL: 返回剩余有效期。
//
// 验收：
//   - 同一 key 连续 Incr，计数单调递增；
//   - 调用 Expire 后，TTL 应接近设置的窗口值，并随时间递减，过期后可视为 key 不存在或重新开始计数；
//   - 足以支撑 EvalWriteLimiterScript 的单元测试中对 TTL/计数的断言。
// TODO: 按 checklist 实现一个内存版 Store（可参考 values/expires + Clock）。
type RedisLikeStore interface {
	Incr(key string) int64
	Expire(key string, window time.Duration)
	TTL(key string) time.Duration
}

// EvalWriteLimiterScript 在 Go 中模拟 Lua 脚本：
//   - 参数含义与生产代码中的脚本一致；
//   - 返回 limited / retryAfter / scope 三元组。
//
// 验收：
//   - 仅设置 idLimit 时：前几次调用返回 limited=false，超过阈值后 limited=true 且 scope="identifier"，retryAfter 与剩余 TTL 对齐；
//   - 同时设置 globalLimit 和 idLimit 时：先由 globalLimit 决定是否限流，limited=true 时 scope="global"，并能通过 redis_like_test.go 中的两组测试用例。
// TODO: 参考文档 2.1.3 状态图和 Lua 注释，按步骤完成实现。
func EvalWriteLimiterScript(store RedisLikeStore, globalKey, idKey string, globalLimit, idLimit int, window time.Duration) (limited bool, retryAfter int64, scope string) {
	// TODO: 实现脚本语义
	return false, 0, "ok"
}
