// store/retry/retry.go
package db

import (
	"context"
	"math/rand"
	"time"
)

// RetryConfig 重试配置
type RetryConfig struct {
	MaxAttempts   int              // 最大尝试次数（包括首次尝试）
	InitialDelay  time.Duration    // 初始延迟
	MaxDelay      time.Duration    // 最大延迟（用于指数退避上限）
	BackoffFactor float64          // 退避因子（通常为2.0）
	Jitter        bool             // 是否添加随机抖动
	IsRetryable   func(error) bool // 判断错误是否可重试
}

// DefaultRetryConfig 默认重试配置
var DefaultRetryConfig = RetryConfig{
	MaxAttempts:   3,
	InitialDelay:  100 * time.Millisecond,
	MaxDelay:      1 * time.Second,
	BackoffFactor: 2.0,
	Jitter:        true,
	IsRetryable: func(err error) bool {
		// 默认重试超时、临时性错误
		return isTimeoutError(err) || isTemporaryError(err)
	},
}

// Do 执行操作并在失败时重试
func Do(ctx context.Context, config RetryConfig, operation func(ctx context.Context) error) error {
	var attempt int
	var lastErr error

	for attempt = 0; attempt < config.MaxAttempts; attempt++ {
		// 为本次尝试创建派生上下文
		attemptCtx, cancel := createAttemptContext(ctx, config, attempt)

		// 执行操作
		lastErr = operation(attemptCtx)
		cancel() // 立即取消本次尝试的上下文

		// 如果成功或不可重试的错误，直接返回
		if lastErr == nil || (config.IsRetryable != nil && !config.IsRetryable(lastErr)) {
			return lastErr
		}

		// 如果是最后一次尝试，不再等待
		if attempt == config.MaxAttempts-1 {
			break
		}

		// 计算下次重试的延迟时间
		delay := calculateDelay(config, attempt)

		// 等待延迟时间或上下文取消
		select {
		//case <-ctx.Done():
		//	return ctx.Err() // 父上下文已取消
		case <-time.After(delay):
			// 继续下一次尝试
		}
	}

	return lastErr
}

// createAttemptContext 为单次尝试创建上下文
func createAttemptContext(parent context.Context, config RetryConfig, attempt int) (context.Context, context.CancelFunc) {
	// 计算本次尝试的超时时间
	// 可以根据尝试次数动态调整，或者使用固定超时
	timeout := calculateAttemptTimeout(config, attempt)

	return context.WithTimeout(parent, timeout)
}

// calculateAttemptTimeout 计算单次尝试的超时时间
func calculateAttemptTimeout(config RetryConfig, attempt int) time.Duration {
	// 策略1: 固定超时（简单）
	// return config.InitialDelay * 2

	// 策略2: 随尝试次数增加超时（推荐）
	baseTimeout := config.InitialDelay * time.Duration(attempt+1)
	if baseTimeout > config.MaxDelay {
		baseTimeout = config.MaxDelay
	}
	return baseTimeout
}

// calculateDelay 计算重试延迟
func calculateDelay(config RetryConfig, attempt int) time.Duration {
	// 指数退避
	delay := config.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * config.BackoffFactor)
		if delay > config.MaxDelay {
			delay = config.MaxDelay
			break
		}
	}

	// 添加抖动（避免重试风暴）
	if config.Jitter {
		jitter := time.Duration(rand.Int63n(int64(delay / 2))) // 最多50%的抖动
		if rand.Intn(2) == 0 {
			delay = delay + jitter
		} else {
			delay = delay - jitter
		}
	}

	return delay
}

// 示例错误判断函数（需要根据实际数据库驱动实现）
func isTimeoutError(err error) bool {
	// 实现根据具体数据库驱动判断超时错误
	return false
}

func isTemporaryError(err error) bool {
	// 实现判断临时性错误
	return false
}
