package limiter

import (
	"sync"
	"time"
)

// Clock 把时间抽象出来，便于在单测里通过 fakeClock 精确控制时间。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Store 把计数存储抽象出来，后续可以替换为 Redis / 其它实现。
//
// 当前阶段只需要支持：按 key 取计数、写计数，以及在窗口切换时整体重置。
type Store interface {
	Get(key string) int
	Set(key string, count int)
	Reset()
}

// inMemoryStore 是最简单的内存实现，用 map 记录每个 key 的计数。
type inMemoryStore struct {
	counts map[string]int
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{counts: make(map[string]int)}
}

func (s *inMemoryStore) Get(key string) int {
	return s.counts[key]
}

func (s *inMemoryStore) Set(key string, count int) {
	s.counts[key] = count
}

func (s *inMemoryStore) Reset() {
	s.counts = make(map[string]int)
}

// Limiter 是一个最简单的限流接口：给定 key，返回当前请求是否允许。
type Limiter interface {
	Allow(key string) bool
}

// OverrideCapable 表示支持在调用时指定「覆盖阈值」的限流器。
//
// 在实验环境中我们用它来模拟生产中的 "global_override_key" 动态调整能力。
type OverrideCapable interface {
	AllowWithOverride(key string, limit int) bool
}

// FixedWindowLimiter 使用「固定时间窗口 + 计数」做限流。
//
// 特点：
//   - 使用 Clock / Store 抽象时间和存储，便于测试与替换实现；
//   - 每个 key 都在当前窗口里维护一个计数；
//   - 当 now - windowStart >= window 时，整体窗口被重置。
type FixedWindowLimiter struct {
	mu          sync.Mutex
	window      time.Duration
	limit       int
	windowStart time.Time
	clock       Clock
	store       Store
}

// NewFixedWindowLimiter 创建一个新的固定窗口限流器。
// limit 表示在一个窗口内，同一个 key 允许的最大通过次数。
func NewFixedWindowLimiter(limit int, window time.Duration) *FixedWindowLimiter {
	if limit <= 0 {
		panic("limit must be > 0")
	}

	clk := realClock{}
	now := clk.Now()

	return &FixedWindowLimiter{
		window:      window,
		limit:       limit,
		windowStart: now,
		clock:       clk,
		store:       newInMemoryStore(),
	}
}

// newFixedWindowLimiterForTest 仅用于单测，允许注入自定义 clock / store / 初始时间。
func newFixedWindowLimiterForTest(limit int, window time.Duration, clk Clock, store Store, start time.Time) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		window:      window,
		limit:       limit,
		windowStart: start,
		clock:       clk,
		store:       store,
	}
}

// Allow 是对外暴露的限流入口，会通过 Clock 决定当前时间。
func (l *FixedWindowLimiter) Allow(key string) bool {
	return l.allowInternal(key, 0)
}

// AllowWithOverride 在调用时允许传递一个覆盖阈值 limitOverride（>0 生效）。
//
// 为了简化实验，其它状态（当前窗口、计数）仍然共用，只在比较阈值时使用覆盖值。
func (l *FixedWindowLimiter) AllowWithOverride(key string, limitOverride int) bool {
	return l.allowInternal(key, limitOverride)
}

// allowInternal 是共享实现：当 overrideLimit>0 时使用覆盖阈值，否则使用内部 limit 字段。
func (l *FixedWindowLimiter) allowInternal(key string, overrideLimit int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()

	// 窗口切换：如果当前时间已经超过了窗口长度，就整体重置计数。
	if now.Sub(l.windowStart) >= l.window {
		l.windowStart = now
		l.store.Reset()
	}

	limit := l.limit
	if overrideLimit > 0 {
		limit = overrideLimit
	}

	c := l.store.Get(key) + 1
	if c > limit {
		return false
	}

	l.store.Set(key, c)
	return true
}
