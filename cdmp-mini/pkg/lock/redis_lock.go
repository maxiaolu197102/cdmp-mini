package lock

import (
	"context"
	"errors"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"

	uuid "github.com/satori/go.uuid"
)

var (
	ErrLockAcquisitionFailed = errors.New("lock acquisition failed")
	ErrLockNotHeld           = errors.New("lock not held by this client")
)

// RedisDistributedLock 基于Redis的分布式锁
type RedisDistributedLock struct {
	storage *storage.RedisCluster
	key     string
	value   string
	timeout time.Duration
}

// NewRedisLock 创建新的Redis分布式锁实例
func NewRedisLock(storage *storage.RedisCluster, key string, timeout time.Duration) *RedisDistributedLock {
	return &RedisDistributedLock{
		storage: storage,
		key:     key,
		value:   uuid.Must(uuid.NewV4()).String(),
		timeout: timeout,
	}
}

// Acquire 获取分布式锁
func (l *RedisDistributedLock) Acquire(ctx context.Context) (bool, error) {
	// 修复：补充ctx参数，匹配SetNX方法签名
	success, err := l.storage.SetNX(ctx, l.key, l.value, l.timeout)
	if err != nil {
		log.Errorf("Failed to acquire lock: %v", err)
		return false, err
	}

	return success, nil
}

// Release 释放分布式锁（使用Lua脚本确保原子性）
func (l *RedisDistributedLock) Release(ctx context.Context) error {
	luaScript := `
    if redis.call("get", KEYS[1]) == tostring(ARGV[1]) then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
`

	// 修复：补充ctx参数，匹配Eval方法签名
	result, err := l.storage.Eval(ctx, luaScript, []string{l.key}, []interface{}{l.value})
	if err != nil {
		log.Errorf("Failed to release lock: %v", err)
		return err
	}

	// 检查删除操作是否成功
	if deleted, ok := result.(int64); ok && deleted == 1 {
		return nil
	}

	return ErrLockNotHeld
}

// TryAcquire 尝试获取锁，带有重试机制
func (l *RedisDistributedLock) TryAcquire(ctx context.Context, maxRetries int, retryInterval time.Duration) (bool, error) {
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
			acquired, err := l.Acquire(ctx)
			if err != nil {
				return false, err
			}
			if acquired {
				return true, nil
			}

			// 等待后重试
			time.Sleep(retryInterval)
		}
	}
	return false, ErrLockAcquisitionFailed
}

// GetValue 获取锁的唯一值（用于调试）
func (l *RedisDistributedLock) GetValue() string {
	return l.value
}

// GetKey 获取锁的键名（用于调试）
func (l *RedisDistributedLock) GetKey() string {
	return l.key
}

// Renew 续约分布式锁
func (l *RedisDistributedLock) Renew(ctx context.Context) (bool, error) {
	// 续约逻辑：仅当锁存在（值匹配）时，重置超时时间（原子操作）
	luaScript := `
	if redis.call("get", KEYS[1]) == tostring(ARGV[1]) then
		return redis.call("expire", KEYS[1], ARGV[2])
	else
		return 0
	end
`
	// 修复：补充ctx参数，匹配Eval方法签名
	result, err := l.storage.Eval(ctx, luaScript, []string{l.key}, []interface{}{l.value, int(l.timeout.Seconds())})
	if err != nil {
		log.Errorf("Failed to renew lock: %v", err)
		return false, err
	}

	// 解析结果（expire 成功返回 1，失败返回 0）
	if renewed, ok := result.(int64); ok && renewed == 1 {
		return true, nil
	}
	return false, nil
}
