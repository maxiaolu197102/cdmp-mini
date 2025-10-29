package user

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/db"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

type poolStatsProvider interface {
	PoolStats() []db.PoolStats
}

type poolStatsReporter struct {
	provider func() []db.PoolStats
	mu       sync.Mutex
	last     map[string]poolStatSnapshot
}

type poolStatSnapshot struct {
	waitCount    int64
	waitDuration time.Duration
}

func newPoolStatsReporterForFactory(store interfaces.Factory) *poolStatsReporter {
	if store == nil {
		return nil
	}
	provider, ok := store.(poolStatsProvider)
	if !ok {
		return nil
	}
	return newPoolStatsReporter(provider.PoolStats)
}

func newPoolStatsReporter(provider func() []db.PoolStats) *poolStatsReporter {
	if provider == nil {
		return nil
	}
	return &poolStatsReporter{
		provider: provider,
		last:     make(map[string]poolStatSnapshot),
	}
}

func (u *UserService) reportDBPoolStats(ctx context.Context, component string) {
	if u == nil || u.poolReporter == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	u.poolReporter.report(ctx, component)
}

func (r *poolStatsReporter) report(ctx context.Context, component string) {
	if r == nil || r.provider == nil {
		return
	}
	stats := r.provider()
	if len(stats) == 0 {
		return
	}
	component = strings.TrimSpace(component)
	if component == "" {
		component = "user_service"
	}
	pools := make(map[string]map[string]interface{}, len(stats))

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, stat := range stats {
		indexLabel := strconv.Itoa(stat.Index)
		if metrics.DatabasePoolOpenConnections != nil {
			metrics.DatabasePoolOpenConnections.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.OpenConnections))
			metrics.DatabasePoolInUse.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.InUse))
			metrics.DatabasePoolIdle.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.Idle))
			metrics.DatabasePoolWaitCount.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.WaitCount))
			metrics.DatabasePoolWaitDurationSeconds.WithLabelValues(component, stat.Role, indexLabel).Set(stat.Stats.WaitDuration.Seconds())
			metrics.DatabasePoolMaxOpenConnections.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.MaxOpenConnections))
		}

		key := poolSnapshotKey(stat.Role, stat.Index)
		prev := r.last[key]
		waitCountDelta := stat.Stats.WaitCount - prev.waitCount
		waitDurationDelta := stat.Stats.WaitDuration - prev.waitDuration
		if waitCountDelta < 0 {
			waitCountDelta = stat.Stats.WaitCount
		}
		if waitDurationDelta < 0 {
			waitDurationDelta = stat.Stats.WaitDuration
		}
		r.last[key] = poolStatSnapshot{
			waitCount:    stat.Stats.WaitCount,
			waitDuration: stat.Stats.WaitDuration,
		}

		traceKey := tracePoolKey(stat.Role, stat.Index)
		pools[traceKey] = map[string]interface{}{
			"open":               stat.Stats.OpenConnections,
			"in_use":             stat.Stats.InUse,
			"idle":               stat.Stats.Idle,
			"wait_count":         stat.Stats.WaitCount,
			"wait_seconds":       stat.Stats.WaitDuration.Seconds(),
			"max_open":           stat.Stats.MaxOpenConnections,
			"wait_count_delta":   waitCountDelta,
			"wait_seconds_delta": waitDurationDelta.Seconds(),
		}

		fields := []interface{}{
			"component", component,
			"role", stat.Role,
			"index", stat.Index,
			"open", stat.Stats.OpenConnections,
			"in_use", stat.Stats.InUse,
			"idle", stat.Stats.Idle,
			"wait_count_total", stat.Stats.WaitCount,
			"wait_duration_total", stat.Stats.WaitDuration,
			"wait_count_delta", waitCountDelta,
			"wait_duration_delta", waitDurationDelta,
		}
		if waitCountDelta > 0 || waitDurationDelta > 0 {
			log.Infow("数据库连接池等待统计", fields...)
		} else {
			log.Debugw("数据库连接池状态", fields...)
		}
	}

	trace.AddRequestTag(ctx, fmt.Sprintf("db_pool_%s", sanitizeTraceKey(component)), map[string]interface{}{
		"component": component,
		"pools":     pools,
	})
}

func poolSnapshotKey(role string, index int) string {
	if index < 0 {
		return role
	}
	return fmt.Sprintf("%s#%d", role, index)
}

func tracePoolKey(role string, index int) string {
	if index < 0 {
		return role
	}
	return fmt.Sprintf("%s_%d", role, index)
}

func sanitizeTraceKey(component string) string {
	replacer := strings.NewReplacer(" ", "_", "/", "_", ":", "_", ".", "_")
	trimmed := strings.TrimSpace(component)
	if trimmed == "" {
		return "user_service"
	}
	return replacer.Replace(trimmed)
}
