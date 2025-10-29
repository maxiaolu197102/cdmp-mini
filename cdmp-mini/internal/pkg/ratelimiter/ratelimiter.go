package ratelimiter

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiterController 提供生产端动态限速能力
// 可通过 NewRateLimiterController 创建并启动

type RateLimiterController struct {
	limiter      *rate.Limiter
	mu           sync.RWMutex
	stopCh       chan struct{}
	statsFunc    func() (total, fail int)
	minRate      float64
	maxRate      float64
	adjustPeriod time.Duration
}

// NewRateLimiterController 创建并启动动态速率控制器
// statsFunc: 返回(totalRequests, failCount)
func NewRateLimiterController(initRate, minRate, maxRate float64, adjustPeriod time.Duration, statsFunc func() (int, int)) *RateLimiterController {
	rlc := &RateLimiterController{
		limiter:      rate.NewLimiter(rate.Limit(initRate), int(initRate)),
		stopCh:       make(chan struct{}),
		statsFunc:    statsFunc,
		minRate:      minRate,
		maxRate:      maxRate,
		adjustPeriod: adjustPeriod,
	}
	go rlc.run()
	return rlc
}

// Wait 在生产前调用，实现限速
func (r *RateLimiterController) Wait(ctx context.Context) error {
	r.mu.RLock()
	limiter := r.limiter
	r.mu.RUnlock()
	return limiter.Wait(ctx)
}

// Allow 尝试立即获取令牌，失败则直接返回 false，避免长时间阻塞
func (r *RateLimiterController) Allow() bool {
	r.mu.RLock()
	limiter := r.limiter
	r.mu.RUnlock()
	return limiter.Allow()
}

// SetRate 手动调整速率
func (r *RateLimiterController) SetRate(newRate float64) {
	r.mu.Lock()
	r.limiter.SetLimit(rate.Limit(newRate))
	r.limiter.SetBurst(int(newRate))
	r.mu.Unlock()
}

// Stop 停止动态调整
func (r *RateLimiterController) Stop() {
	close(r.stopCh)
}

// run 定期监控并动态调整速率
func (r *RateLimiterController) run() {
	ticker := time.NewTicker(r.adjustPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			total, fail := r.statsFunc()
			failRate := 0.0
			if total > 0 {
				failRate = float64(fail) / float64(total)
			}
			r.mu.Lock()
			oldLimit := r.limiter.Limit()
			newLimit := oldLimit
			if failRate > 0.10 && oldLimit > rate.Limit(r.minRate) {
				newLimit = oldLimit * 0.8
				if newLimit < rate.Limit(r.minRate) {
					newLimit = rate.Limit(r.minRate)
				}
			} else if failRate < 0.02 && oldLimit < rate.Limit(r.maxRate) {
				newLimit = oldLimit * 1.1
				if newLimit > rate.Limit(r.maxRate) {
					newLimit = rate.Limit(r.maxRate)
				}
			}
			if newLimit != oldLimit {
				r.limiter.SetLimit(newLimit)
				r.limiter.SetBurst(int(newLimit))
				//log.Warnf("[RateLimiter] 动态调整: %.2f -> %.2f req/s (failRate=%.2f%%)", oldLimit, newLimit, failRate*100)
			}
			r.mu.Unlock()
		case <-r.stopCh:
			return
		}
	}
}

// GetRate 获取当前速率
func (r *RateLimiterController) GetRate() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return float64(r.limiter.Limit())
}
