package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

const (
	redisPoolSampleInterval = 5 * time.Second
)

type poolWaitSnapshot struct {
	waitCount    uint64
	waitDuration time.Duration
}

type statsProviderFunc func() poolWaitSnapshot

type redisCommandMetricsHook struct {
	component     string
	node          string
	statsProvider statsProviderFunc
}

func (h *redisCommandMetricsHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *redisCommandMetricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		spanCtx, span := trace.StartSpan(ctx, "redis-client", cmd.FullName())
		before := h.statsProvider()
		start := time.Now()
		err := next(spanCtx, cmd)
		total := time.Since(start)
		after := h.statsProvider()

		queue := computeQueueDuration(before, after)
		service := total - queue
		if service < 0 {
			service = 0
		}

		metrics.ObserveRedisCommandDurations(h.component, h.node, cmd.FullName(), total, queue, service, err)
		if span != nil {
			status := "success"
			if err != nil {
				status = "error"
			}
			details := map[string]interface{}{
				"node":                        h.node,
				"pool_wait_count_delta":       waitCountDelta(before, after),
				"pool_wait_duration_delta_ms": durationMillis(after.waitDuration - before.waitDuration),
				"queue_ms":                    durationMillis(queue),
				"service_ms":                  durationMillis(service),
				"total_ms":                    durationMillis(total),
			}
			if err != nil {
				details["error"] = err.Error()
			}
			trace.EndSpan(span, status, "", details)
		}
		return err
	}
}

func (h *redisCommandMetricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		operation := pipelineOperationName(cmds)
		spanCtx, span := trace.StartSpan(ctx, "redis-client", operation)
		before := h.statsProvider()
		start := time.Now()
		err := next(spanCtx, cmds)
		total := time.Since(start)
		after := h.statsProvider()

		queue := computeQueueDuration(before, after)
		service := total - queue
		if service < 0 {
			service = 0
		}

		metrics.ObserveRedisCommandDurations(h.component, h.node, operation, total, queue, service, err)
		if span != nil {
			status := "success"
			if err != nil {
				status = "error"
			}
			details := map[string]interface{}{
				"node":                        h.node,
				"command_count":               len(cmds),
				"pool_wait_count_delta":       waitCountDelta(before, after),
				"pool_wait_duration_delta_ms": durationMillis(after.waitDuration - before.waitDuration),
				"queue_ms":                    durationMillis(queue),
				"service_ms":                  durationMillis(service),
				"total_ms":                    durationMillis(total),
			}
			if err != nil {
				details["error"] = err.Error()
			}
			trace.EndSpan(span, status, "", details)
		}
		return err
	}
}

func computeQueueDuration(before, after poolWaitSnapshot) time.Duration {
	if after.waitCount <= before.waitCount {
		return 0
	}
	diff := after.waitDuration - before.waitDuration
	if diff < 0 {
		return 0
	}
	return diff
}

func waitCountDelta(before, after poolWaitSnapshot) int64 {
	if after.waitCount <= before.waitCount {
		return 0
	}
	return int64(after.waitCount - before.waitCount)
}

func durationMillis(d time.Duration) float64 {
	if d < 0 {
		d = 0
	}
	return float64(d) / float64(time.Millisecond)
}

func pipelineOperationName(cmds []redis.Cmder) string {
	switch len(cmds) {
	case 0:
		return "pipeline"
	case 1:
		return cmds[0].FullName()
	default:
		return fmt.Sprintf("pipeline(%d):%s", len(cmds), cmds[0].FullName())
	}
}

var instrumentedRedisClients sync.Map

func setupRedisInstrumentation(client redis.UniversalClient, config *options.RedisOptions, isCache bool) {
	component := "primary"
	if isCache {
		component = "cache"
	} else if config != nil && config.MasterName != "" {
		component = config.MasterName
	}

	switch c := client.(type) {
	case *redis.ClusterClient:
		instrumentClusterClient(c, component)
	case *redis.Client:
		instrumentStandaloneClient(c, component, deriveNodeLabel(c))
	default:
		log.Debugf("redis metrics instrumentation: unsupported client type %T", client)
	}
}

func instrumentStandaloneClient(client *redis.Client, component, node string) {
	if _, loaded := instrumentedRedisClients.LoadOrStore(client, struct{}{}); loaded {
		return
	}

	client.AddHook(&redisCommandMetricsHook{
		component:     component,
		node:          node,
		statsProvider: makeStatsProvider(client),
	})

	go sampleRedisPool(client, component, node)
}

func instrumentClusterClient(cluster *redis.ClusterClient, component string) {
	install := func(c *redis.Client) {
		instrumentStandaloneClient(c, component, deriveNodeLabel(c))
	}

	cluster.OnNewNode(func(c *redis.Client) {
		install(c)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cluster.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
		install(c)
		return nil
	}); err != nil {
		log.Debugf("redis metrics instrumentation: ForEachShard failed: %v", err)
	}
}

func makeStatsProvider(client *redis.Client) statsProviderFunc {
	return func() poolWaitSnapshot {
		stats := client.PoolStats()
		if stats == nil {
			return poolWaitSnapshot{}
		}
		return poolWaitSnapshot{
			waitCount:    uint64(stats.WaitCount),
			waitDuration: time.Duration(stats.WaitDurationNs),
		}
	}
}

func sampleRedisPool(client *redis.Client, component, node string) {
	ticker := time.NewTicker(redisPoolSampleInterval)
	defer ticker.Stop()

	for range ticker.C {
		stats := client.PoolStats()
		if stats == nil {
			continue
		}
		total := float64(stats.TotalConns)
		idle := float64(stats.IdleConns)
		inUse := total - idle
		snapshot := metrics.RedisPoolSnapshot{
			TotalConns:          total,
			IdleConns:           idle,
			InUseConns:          inUse,
			WaitDurationSeconds: float64(stats.WaitDurationNs) / float64(time.Second),
			WaitCount:           float64(stats.WaitCount),
			Hits:                float64(stats.Hits),
			Misses:              float64(stats.Misses),
			Timeouts:            float64(stats.Timeouts),
		}
		metrics.ObserveRedisPoolStats(component, node, snapshot)
	}
}

func deriveNodeLabel(client *redis.Client) string {
	if client == nil {
		return "unknown"
	}
	opt := client.Options()
	if opt == nil || opt.Addr == "" {
		return "unknown"
	}
	return opt.Addr
}
