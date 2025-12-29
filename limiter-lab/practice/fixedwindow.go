package practice

import "time"

// Limiter 是练习用的限流接口：给定 key，返回当前请求是否允许。
//
// 按练习顺序建议先只依赖 window/limit/counts，后续再慢慢引入 Clock/Store 抽象。
type Limiter interface {
	Allow(key string) bool
}

// FixedWindowLimiter 使用固定时间窗口 + 计数的方式做限流。
//
// 验收：同一 key 在一个窗口内最多通过 limit 次；不同 key 之间的计数互不影响；
// 窗口结束后，旧计数不再生效（可以在测试中通过推进时间或等待验证）。
// TODO: 按 checklist 补充需要的字段（window、limit、windowStart、counts、锁等）。
type FixedWindowLimiter struct {
	// TODO: 补充字段
}

// NewFixedWindowLimiter 创建一个新的固定窗口限流器。
// limit 表示在一个窗口内，同一个 key 允许的最大通过次数。

// 验收：正常参数下返回的 limiter 能通过 fixedwindow_test.go 中的单 key/多 key 测试；
// 对于 limit<=0 或 window<=0 的输入，你可以选择 panic 或返回 nil，但行为要自洽并在测试中覆盖。
// TODO: 完成构造逻辑。
func NewFixedWindowLimiter(limit int, window time.Duration) *FixedWindowLimiter {
	// TODO: 实现构造函数
	return nil
}

// Allow 是对外暴露的限流入口。

// 验收：结合 TestFixedWindow_SingleKey/TestFixedWindow_MultiKey，验证：
//   - 单 key 在窗口内前 limit 次返回 true，第 limit+1 次开始返回 false；
//   - 不同 key 彼此独立，互不影响计数；
//   - 新窗口开始后，同一 key 重新从 0 计数。
// TODO: 按固定窗口算法完成实现。
func (l *FixedWindowLimiter) Allow(key string) bool {
	// TODO: 实现限流逻辑
	return false
}

// 下面这两个类型和构造函数用于后续步骤的「可测试抽象」练习，
// 你可以在完成最初版本后再回到这里补充实现。

// Clock 抽象当前时间，便于在测试中通过 fakeClock 精确控制时间线。
type Clock interface {
	Now() time.Time
}

// Store 抽象每个 key 的计数存储，后续可以换成 Redis/其它实现。
type Store interface {
	Get(key string) int
	Set(key string, count int)
	Reset()
}

// NewFixedWindowLimiterWithStore 是可选构造，用于在后续步骤中
// 注入自定义 Clock/Store（例如 fakeClock + 内存 Store）。
// 初始可以留空实现，等做到对应练习步骤时再回来补齐。
func NewFixedWindowLimiterWithStore(limit int, window time.Duration, clk Clock, store Store, start time.Time) *FixedWindowLimiter {
	// 验收：可以在测试中注入 fakeClock 和自定义 Store，通过精确控制时间推进，
	// 验证窗口滚动时计数重置、不同 key 的计数独立等行为，与基础版本语义保持一致。
	// TODO: 在做 Clock/Store 抽象练习时实现
	return nil
}
