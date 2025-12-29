package limiter

import (
	"sync"
	"time"
)

// RedisLikeStore 抽象出一组接近 Redis 的基本操作，用于在 lab 里模拟 Lua 脚本语义。
//
// 这里只保留 writeLimiterLuaScript 所需的最小子集：INCR / EXPIRE / TTL。
type RedisLikeStore interface {
	Incr(key string) int64
	Expire(key string, window time.Duration)
	TTL(key string) time.Duration
}

// inMemoryRedisStore 是 RedisLikeStore 的内存实现。
//
// 注意：
//   - 通过 Clock 控制时间，便于单测。
//   - 过期后再访问会视为 key 已删除，重新开始计数。
type inMemoryRedisStore struct {
	mu      sync.Mutex
	clock   Clock
	values  map[string]int64
	expires map[string]time.Time
}

func newInMemoryRedisStore(clk Clock) *inMemoryRedisStore {
	return &inMemoryRedisStore{
		clock:   clk,
		values:  make(map[string]int64),
		expires: make(map[string]time.Time),
	}
}

// Incr 模拟 Redis INCR：
//   - 若 key 已过期，先删除再从 0 开始；
//   - 返回自增后的值。
func (s *inMemoryRedisStore) Incr(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.evictIfExpired(key)

	v := s.values[key] + 1
	s.values[key] = v
	return v
}

// Expire 模拟 Redis EXPIRE：设置过期时间为当前时间 + window。
func (s *inMemoryRedisStore) Expire(key string, window time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	s.expires[key] = now.Add(window)
}

// TTL 模拟 Redis TTL：返回 key 剩余的过期时间；若不存在或已过期，则返回 0。
func (s *inMemoryRedisStore) TTL(key string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evictIfExpired(key) {
		return 0
	}

	exp, ok := s.expires[key]
	if !ok {
		return 0
	}

	now := s.clock.Now()
	if !exp.After(now) {
		delete(s.values, key)
		delete(s.expires, key)
		return 0
	}

	d := exp.Sub(now)
	// 向下取整到秒，防止测试中因为纳秒级别产生抖动。
	return time.Duration(int64(d.Seconds())) * time.Second
}

// evictIfExpired 在内部使用：若 key 已过期则删除，并返回 true；否则返回 false。
func (s *inMemoryRedisStore) evictIfExpired(key string) bool {
	exp, ok := s.expires[key]
	if !ok {
		return false
	}
	now := s.clock.Now()
	if !exp.After(now) {
		delete(s.values, key)
		delete(s.expires, key)
		return true
	}
	return false
}

// EvalWriteLimiterScript 在 Go 中模拟生产环境下的 writeLimiterLuaScript 逻辑（简化版）。
//
// 为了保持 lab 的专注度，这里暂时只模拟「全局 + 标识粒度」两个维度，不引入业务级维度；
// 生产代码中额外的 BizID 维度由真实 Lua 脚本负责，lab 主要关注算法语义本身。
//
// 参数含义：
//   - globalKey: 路径级全局计数 key
//   - idKey: 标识粒度计数 key（token/ip + path）
//   - globalLimit: 全局阈值（<=0 则跳过全局判断）
//   - idLimit: 标识阈值（<=0 则跳过标识判断）
//   - window: 窗口长度
//
// 返回值：
//   - limited: 是否被限流
//   - retryAfter: 若被限流，距离窗口结束的秒数；未被限流时表示剩余可用配额（对齐 Lua 返回的第二个元素语义）
//   - scope: "global" / "identifier" / "ok"
func EvalWriteLimiterScript(store RedisLikeStore, globalKey, idKey string, globalLimit, idLimit int, window time.Duration) (limited bool, retryAfter int64, scope string) {
	var nowGlobal, nowID int64

	// 全局限流逻辑
	if globalLimit > 0 {
		nowGlobal = store.Incr(globalKey)
		if nowGlobal == 1 {
			store.Expire(globalKey, window)
		}
		if nowGlobal > int64(globalLimit) {
			ttl := store.TTL(globalKey)
			return true, int64(ttl.Seconds()), "global"
		}
	}

	// 标识粒度限流逻辑
	if idLimit > 0 {
		nowID = store.Incr(idKey)
		if nowID == 1 {
			store.Expire(idKey, window)
		}
		if nowID > int64(idLimit) {
			ttl := store.TTL(idKey)
			return true, int64(ttl.Seconds()), "identifier"
		}
	}

	// 未被限流时，Lua 脚本会返回剩余配额（针对标识粒度）。
	remaining := int64(0)
	if idLimit > 0 {
		remaining = int64(idLimit) - nowID
	}
	return false, remaining, "ok"
}
