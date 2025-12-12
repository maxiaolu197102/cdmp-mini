package backpressure

import (
	"context"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
)

// DefaultDeadlineSkipThreshold 是将延迟与 deadline 对齐时允许的最小等待窗口，剩余时间低于该值则跳过等待。
const DefaultDeadlineSkipThreshold = 2 * time.Millisecond

// DelayDecision 表示与上下文 deadline 对齐后的等待策略。
type DelayDecision struct {
	Effective time.Duration
	Action    string
	Remaining time.Duration
}

// AlignDelayWithDeadline 根据 ctx.Deadline() 调整请求的延迟，必要时截断或跳过。
func AlignDelayWithDeadline(ctx context.Context, requested, minWait time.Duration) DelayDecision {
	decision := DelayDecision{Effective: requested}
	if ctx == nil || requested <= 0 {
		return decision
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return decision
	}
	remaining := time.Until(deadline)
	decision.Remaining = remaining
	if remaining <= 0 {
		decision.Effective = 0
		decision.Action = "skip"
		return decision
	}
	threshold := minWait
	if threshold <= 0 {
		threshold = DefaultDeadlineSkipThreshold
	}
	if remaining <= threshold {
		decision.Effective = 0
		decision.Action = "skip"
		return decision
	}
	if requested > remaining {
		decision.Effective = remaining
		decision.Action = "truncate"
	}
	return decision
}

// LeadTime 返回从 trace 启动到当前的耗时，如果上下文没有 trace，则回退到 fallbackStart。
func LeadTime(ctx context.Context, fallbackStart time.Time) time.Duration {
	if elapsed := trace.ElapsedSinceStart(ctx); elapsed > 0 {
		return elapsed
	}
	if !fallbackStart.IsZero() {
		return time.Since(fallbackStart)
	}
	return 0
}
