package practice

import "time"

// RedisClientLike 抽象 PendingCoordinator 需要的最小 Redis 能力，
// 避免在练习中直接依赖具体 Redis 客户端。
//
// TODO: 按需扩展方法签名（例如 SetNX、Eval 等）。
type RedisClientLike interface {
SetNX(key string, value interface{}, ttl time.Duration) (bool, error)
TTL(key string) (time.Duration, error)
}

// EvalPendingScript 模拟 PendingCoordinator 使用的 Redis 脚本或原子操作语义，
// 你可以用 Go 代码表达 Lua 的逻辑。
//
// 验收：
//   - 并发下同一 key 只能有一个调用认为“成功获取租约”；
//   - TTL 行为与文档状态机一致：过期前视为持有，过期后视为无租约。
// TODO: 在完成内存版后，再实现 Redis 语义练习。
func EvalPendingScript(client RedisClientLike, key string, ttl time.Duration) (acquired bool, err error) {
// TODO: 实现 Redis 原子语义或其近似行为
return false, nil
}

