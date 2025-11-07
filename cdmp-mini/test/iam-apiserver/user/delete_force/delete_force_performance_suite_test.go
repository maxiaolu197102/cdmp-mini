//go:build perf
// +build perf

package delete_force

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const (
	perfBasePassword     = "InitPassw0rd!"
	maxForceDeleteBatch  = 100
	performanceOutputDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/delete_force"
)

type perfConfig struct {
	Scale        float64
	Heavy        bool
	Long         bool
	LongDuration time.Duration
}

type deleteMode int

const (
	deleteModeSingle deleteMode = iota
	deleteModeBatch
)

type scenarioOptions struct {
	Concurrency int
	BatchSize   int
	Mode        deleteMode
}

type scenarioMetrics struct {
	durations []time.Duration
	httpCodes map[int]int
	bizCodes  map[int]int
	errors    map[string]int
	success   int64
	failure   int64
}

type resourceSnapshot struct {
	Goroutines int
	HeapAlloc  uint64
	HeapInuse  uint64
	NumGC      uint32
}

type workloadCounter struct {
	success int64
	failure int64
	wg      sync.WaitGroup
}

func (w *workloadCounter) add(success bool) {
	if success {
		atomic.AddInt64(&w.success, 1)
		return
	}
	atomic.AddInt64(&w.failure, 1)
}

func (w *workloadCounter) wait() {
	w.wg.Wait()
}

func (w *workloadCounter) snapshot() (int64, int64) {
	return atomic.LoadInt64(&w.success), atomic.LoadInt64(&w.failure)
}

func loadPerfConfig() perfConfig {
	scale := 0.1
	if raw := strings.TrimSpace(os.Getenv("IAM_APISERVER_PERF_SCALE")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			scale = v
		}
	}
	heavy := os.Getenv("IAM_APISERVER_PERF_HEAVY") != ""
	long := os.Getenv("IAM_APISERVER_PERF_LONG") != ""
	longDuration := 4 * time.Hour
	if raw := strings.TrimSpace(os.Getenv("IAM_APISERVER_PERF_LONG_DURATION")); raw != "" {
		if v, err := time.ParseDuration(raw); err == nil && v > 0 {
			longDuration = v
		}
	}
	return perfConfig{Scale: scale, Heavy: heavy, Long: long, LongDuration: longDuration}
}

func (cfg perfConfig) scaledOps(base int) int {
	if base <= 0 {
		return 0
	}
	val := int(math.Ceil(float64(base) * cfg.Scale))
	if val < 1 {
		return 1
	}
	return val
}

func (cfg perfConfig) scaledConcurrency(base int) int {
	if base <= 0 {
		return 1
	}
	val := int(math.Ceil(float64(base) * cfg.Scale))
	if val < 1 {
		val = 1
	}
	return val
}

func TestDeleteForcePerformanceSuite(t *testing.T) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		t.Skip("set IAM_APISERVER_E2E=1 to enable delete-force performance tests")
	}

	env := framework.NewEnv(t)
	env.DisableClientRateLimiter()
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "delete_force_perf")
	defer recorder.Flush(t)

	cfg := loadPerfConfig()

	t.Run("baseline/single_delete", func(t *testing.T) {
		runBaselineSingleDelete(t, env, recorder, cfg)
	})
	t.Run("baseline/batch_efficiency", func(t *testing.T) {
		runBatchComparison(t, env, recorder, cfg)
	})
	t.Run("load/concurrency_ramp", func(t *testing.T) {
		runConcurrencyRamp(t, env, recorder, cfg)
	})
	t.Run("stress/peak_burst", func(t *testing.T) {
		runPeakBurst(t, env, recorder, cfg)
	})
	t.Run("data_volume/tiers", func(t *testing.T) {
		runDataVolumeScenarios(t, env, recorder, cfg)
	})
	t.Run("mixed_workload/read_write", func(t *testing.T) {
		runMixedWorkload(t, env, recorder, cfg)
	})
	t.Run("concurrency/idempotency", func(t *testing.T) {
		runConcurrentIdempotency(t, env, recorder, cfg)
	})
	t.Run("endurance/long_run", func(t *testing.T) {
		runEndurance(t, env, recorder, cfg)
	})
}

func runBaselineSingleDelete(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	count := cfg.scaledOps(120)
	names := createUsers(t, env, "perf_base_", count)
	defer cleanupUsers(t, env, names)

	before := captureRuntimeResources()
	start := time.Now()
	metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: 1, BatchSize: 1, Mode: deleteModeSingle})
	duration := time.Since(start)
	after := captureRuntimeResources()

	addPerformancePoint(recorder, "baseline_single_delete", "单条强制删除基准延迟", metrics, duration, []string{
		resourceDeltaNote(before, after),
		fmt.Sprintf("scale=%.2f", cfg.Scale),
	})
}

func runBatchComparison(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	baseBatches := []int{1, 10, 100, 1000}
	for _, batch := range baseBatches {
		actualBatch := batch
		if actualBatch > 200 && !cfg.Heavy {
			actualBatch = 200
		}
		if actualBatch > maxForceDeleteBatch {
			actualBatch = maxForceDeleteBatch
		}
		ops := cfg.scaledOps(batch * 8)
		if ops < actualBatch {
			ops = actualBatch
		}
		ops = alignToBatch(ops, actualBatch)
		names := createUsers(t, env, fmt.Sprintf("perf_batch_%d_", batch), ops)

		start := time.Now()
		metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: minInt(8, ops/actualBatch), BatchSize: actualBatch, Mode: deleteModeBatch})
		duration := time.Since(start)

		addPerformancePoint(recorder, fmt.Sprintf("batch_delete_%d", batch), fmt.Sprintf("批量删除对比 batch=%d", batch), metrics, duration, []string{
			fmt.Sprintf("batch_size=%d", actualBatch),
			fmt.Sprintf("requests=%d", metrics.totalRequests()),
		})

		cleanupUsers(t, env, names)
	}
}

func runConcurrencyRamp(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	phases := []int{10, 50, 100, 500}
	seen := map[int]struct{}{}
	for _, phase := range phases {
		concurrency := cfg.scaledConcurrency(phase)
		if concurrency > phase && !cfg.Heavy {
			concurrency = phase
		}
		if concurrency < 1 {
			concurrency = 1
		}
		if _, dup := seen[concurrency]; dup {
			continue
		}
		seen[concurrency] = struct{}{}
		ops := cfg.scaledOps(phase * 6)
		if ops < concurrency {
			ops = concurrency
		}
		names := createUsers(t, env, fmt.Sprintf("perf_ramp_%d_", phase), ops)

		start := time.Now()
		metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: concurrency, BatchSize: 1, Mode: deleteModeSingle})
		duration := time.Since(start)

		addPerformancePoint(recorder, fmt.Sprintf("concurrency_%d", phase), fmt.Sprintf("并发删除阶段 target=%d actual=%d", phase, concurrency), metrics, duration, []string{
			fmt.Sprintf("target_concurrency=%d", phase),
			fmt.Sprintf("actual_concurrency=%d", concurrency),
		})

		cleanupUsers(t, env, names)
	}
}

func runPeakBurst(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	base := 120
	if cfg.Heavy {
		base = 200
	}
	concurrency := cfg.scaledConcurrency(int(float64(base) * 3.5))
	if concurrency < 4 {
		concurrency = 4
	}
	ops := cfg.scaledOps(base * 10)
	if ops < concurrency*2 {
		ops = concurrency * 2
	}
	names := createUsers(t, env, "perf_peak_", ops)

	start := time.Now()
	metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: concurrency, BatchSize: 1, Mode: deleteModeSingle})
	duration := time.Since(start)

	addPerformancePoint(recorder, "peak_burst", "短时三倍峰值冲击", metrics, duration, []string{
		fmt.Sprintf("concurrency=%d", concurrency),
		fmt.Sprintf("operations=%d", ops),
	})

	cleanupUsers(t, env, names)
}

func runDataVolumeScenarios(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	tiers := []struct {
		name        string
		baseRecords int
		description string
	}{
		{name: "small", baseRecords: 500, description: "<10万记录"},
		{name: "medium", baseRecords: 2000, description: "10万-100万记录"},
		{name: "large", baseRecords: 6000, description: ">100万记录"},
	}
	for _, tier := range tiers {
		func() {
			ops := cfg.scaledOps(tier.baseRecords)
			if !cfg.Heavy && tier.name == "large" {
				ops = minInt(ops, 2000)
			}
			ops = minInt(ops, 800)

			names := createUsers(t, env, fmt.Sprintf("perf_%s_", tier.name), ops)
			defer cleanupUsers(t, env, names)

			batchSize := maxForceDeleteBatch
			batchCount := (ops + batchSize - 1) / batchSize
			concurrency := minInt(16, batchCount)
			if concurrency < 1 {
				concurrency = 1
			}

			start := time.Now()
			metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: concurrency, BatchSize: batchSize, Mode: deleteModeBatch})
			duration := time.Since(start)

			addPerformancePoint(recorder, fmt.Sprintf("volume_%s", tier.name), fmt.Sprintf("数据量级场景:%s", tier.description), metrics, duration, []string{
				fmt.Sprintf("records=%d", ops),
				tier.description,
			})
		}()
	}
}

func runMixedWorkload(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	deleteOps := cfg.scaledOps(180)
	if deleteOps < 30 {
		deleteOps = 30
	}
	survivors := createUsers(t, env, "perf_mix_sur_", cfg.scaledOps(40))
	deleteTargets := createUsers(t, env, "perf_mix_del_", deleteOps)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryWorkers := cfg.scaledConcurrency(12)
	if queryWorkers > len(survivors) {
		queryWorkers = len(survivors)
	}
	if queryWorkers < 1 {
		queryWorkers = 1
	}
	queryStats := startQueryWorkers(t, ctx, env, survivors, queryWorkers)

	insertCount := cfg.scaledOps(20)
	insertStats, insertedUsers := startInsertWorker(ctx, env, insertCount)

	start := time.Now()
	metrics := executeDeleteScenario(t, env, deleteTargets, scenarioOptions{Concurrency: minInt(24, deleteOps), BatchSize: 1, Mode: deleteModeSingle})
	duration := time.Since(start)

	cancel()
	queryStats.wait()
	insertStats.wait()
	cleanupUsers(t, env, survivors)
	cleanupUsers(t, env, insertedUsers)
	cleanupUsers(t, env, deleteTargets)

	qs, qf := queryStats.snapshot()
	is, ifail := insertStats.snapshot()
	notes := []string{
		fmt.Sprintf("query_success=%d query_fail=%d", qs, qf),
		fmt.Sprintf("insert_success=%d insert_fail=%d", is, ifail),
	}
	addPerformancePoint(recorder, "mixed_read_write", "删除并发查询与插入混合场景", metrics, duration, notes)
}

func runConcurrentIdempotency(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	user := createUsers(t, env, "perf_idem_", 1)
	defer cleanupUsers(t, env, user)

	concurrency := minInt(32, cfg.scaledConcurrency(64))
	if concurrency < 4 {
		concurrency = 4
	}
	ops := concurrency * 4
	names := make([]string, ops)
	for i := range names {
		names[i] = user[0]
	}

	start := time.Now()
	metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: concurrency, BatchSize: 1, Mode: deleteModeSingle})
	duration := time.Since(start)

	addPerformancePoint(recorder, "concurrent_idempotency", "同一用户高并发物理删除幂等等价性", metrics, duration, []string{
		fmt.Sprintf("concurrency=%d", concurrency),
		"expect all requests succeed or return idempotent success",
	})
}

func runEndurance(t *testing.T, env *framework.Env, recorder *framework.Recorder, cfg perfConfig) {
	t.Helper()
	if !cfg.Long {
		t.Skip("set IAM_APISERVER_PERF_LONG=1 to enable endurance scenario")
	}
	opsPerCycle := cfg.scaledOps(240)
	concurrency := cfg.scaledConcurrency(24)
	if concurrency < 2 {
		concurrency = 2
	}
	deadline := time.Now().Add(cfg.LongDuration)
	totalDuration := time.Duration(0)
	aggregate := newScenarioMetrics()

	cycle := 0
	for time.Now().Before(deadline) {
		cycle++
		names := createUsers(t, env, fmt.Sprintf("perf_long_%d_", cycle), opsPerCycle)
		start := time.Now()
		metrics := executeDeleteScenario(t, env, names, scenarioOptions{Concurrency: concurrency, BatchSize: 1, Mode: deleteModeSingle})
		d := time.Since(start)
		totalDuration += d
		aggregate.merge(metrics)
		cleanupUsers(t, env, names)
		if cycle%10 == 0 {
			t.Logf("endurance cycle=%d operations=%d success=%.2f%%", cycle, aggregate.totalRequests(), ratioFloat(float64(aggregate.success), float64(aggregate.totalRequests()))*100)
		}
	}

	addPerformancePoint(recorder, "endurance_long_run", fmt.Sprintf("持续负载运行 %s", cfg.LongDuration), aggregate.snapshot(), totalDuration, []string{
		fmt.Sprintf("cycles=%d", aggregate.cycles),
		fmt.Sprintf("duration=%s", totalDuration),
	})
}

func runBatchDeleteRequest(t *testing.T, env *framework.Env, usernames []string) (*framework.APIResponse, error) {
	t.Helper()
	query := url.Values{}
	for _, name := range usernames {
		query.Add("names", name)
	}
	path := "/v1/users"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return env.AdminRequest(http.MethodDelete, path, nil)
}

func executeDeleteScenario(t *testing.T, env *framework.Env, usernames []string, opts scenarioOptions) scenarioMetrics {
	t.Helper()
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.BatchSize < 1 {
		opts.BatchSize = 1
	}
	taskCh := make(chan []string)
	collector := newScenarioMetrics()
	var wg sync.WaitGroup

	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range taskCh {
				start := time.Now()
				var resp *framework.APIResponse
				var err error
				switch opts.Mode {
				case deleteModeBatch:
					resp, err = runBatchDeleteRequest(t, env, batch)
				default:
					resp, err = env.ForceDeleteUser(batch[0])
				}
				duration := time.Since(start)
				httpStatus, bizCode, success := classifyResponse(resp, err, opts.Mode == deleteModeBatch)
				collector.add(duration, httpStatus, bizCode, success, err)
				if !success && opts.Mode == deleteModeBatch && resp != nil {
					t.Logf("batch delete failure: size=%d http=%d code=%d msg=%s", len(batch), resp.HTTPStatus(), resp.Code, resp.Message)
				}
			}
		}()
	}

	go func() {
		switch opts.Mode {
		case deleteModeBatch:
			for i := 0; i < len(usernames); {
				end := i + opts.BatchSize
				if end > len(usernames) {
					end = len(usernames)
				}
				task := make([]string, end-i)
				copy(task, usernames[i:end])
				taskCh <- task
				i = end
			}
		default:
			for _, name := range usernames {
				taskCh <- []string{name}
			}
		}
		close(taskCh)
	}()

	wg.Wait()
	return collector.snapshot()
}

func classifyResponse(resp *framework.APIResponse, err error, isBatch bool) (int, int, bool) {
	if err != nil || resp == nil {
		return http.StatusInternalServerError, 0, false
	}
	status := resp.HTTPStatus()
	codeValue := resp.Code
	switch {
	case status == http.StatusOK && codeValue == code.ErrSuccess:
		return status, codeValue, true
	case status == http.StatusNoContent && !isBatch:
		return status, codeValue, true
	case status == http.StatusNotFound:
		return status, code.ErrUserNotFound, true
	default:
		return status, codeValue, false
	}
}

func addPerformancePoint(recorder *framework.Recorder, name, description string, metrics scenarioMetrics, duration time.Duration, notes []string) {
	total := metrics.totalRequests()
	success := int(metrics.success)
	errorCount := int(metrics.failure)
	stats := computeLatencyStats(metrics.durations)
	point := framework.PerformancePoint{
		Scenario:     name,
		Requests:     total,
		SuccessRate:  ratioFloat(float64(success), float64(total)),
		ErrorRate:    ratioFloat(float64(errorCount), float64(total)),
		DurationMS:   duration.Milliseconds(),
		QPS:          qps(total, duration),
		ErrorCount:   errorCount,
		SuccessCount: success,
		Latency:      stats,
		Counters:     metrics.flattenCounters(),
		Notes:        notes,
	}
	recorder.AddPerformance(point)
}

func createUsers(t *testing.T, env *framework.Env, prefix string, count int) []string {
	t.Helper()
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		spec := env.NewUserSpec(prefix, perfBasePassword)
		resp, err := env.CreateUser(spec)
		if err != nil {
			t.Fatalf("create user %s: %v", spec.Name, err)
		}
		if resp.HTTPStatus() != http.StatusCreated {
			t.Fatalf("create user http=%d code=%d", resp.HTTPStatus(), resp.Code)
		}
		if err := env.WaitForUser(spec.Name, 15*time.Second); err != nil {
			t.Fatalf("wait for user %s: %v", spec.Name, err)
		}
		names = append(names, spec.Name)
	}
	return names
}

func cleanupUsers(t *testing.T, env *framework.Env, names []string) {
	t.Helper()
	seen := map[string]struct{}{}
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		resp, err := env.ForceDeleteUser(name)
		if err != nil {
			t.Fatalf("cleanup user %s: %v", name, err)
		}
		if resp != nil {
			switch resp.HTTPStatus() {
			case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
				// acceptable outcomes
			default:
				t.Fatalf("cleanup user %s unexpected status: %d code=%d", name, resp.HTTPStatus(), resp.Code)
			}
		}
		if err := waitForUserGonePerf(env, name, 30*time.Second); err != nil {
			t.Fatalf("user %s still present after cleanup: %v", name, err)
		}
	}
}

func waitForUserGonePerf(env *framework.Env, username string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), nil)
		if err == nil && resp != nil {
			switch resp.HTTPStatus() {
			case http.StatusNotFound:
				return nil
			case http.StatusOK:
				// keep polling
			default:
				// transient or unexpected, keep retrying until timeout
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("user %s still present after %s", username, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func captureRuntimeResources() resourceSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return resourceSnapshot{
		Goroutines: runtime.NumGoroutine(),
		HeapAlloc:  ms.HeapAlloc,
		HeapInuse:  ms.HeapInuse,
		NumGC:      ms.NumGC,
	}
}

func resourceDeltaNote(before, after resourceSnapshot) string {
	deltaAlloc := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	deltaGoroutines := after.Goroutines - before.Goroutines
	deltaGC := int(after.NumGC) - int(before.NumGC)
	return fmt.Sprintf("heap_delta=%s goroutines_delta=%d gc_delta=%d", formatBytes(deltaAlloc), deltaGoroutines, deltaGC)
}

func formatBytes(delta int64) string {
	sign := ""
	if delta < 0 {
		sign = "-"
		delta = -delta
	}
	units := []string{"B", "KB", "MB", "GB"}
	value := float64(delta)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	return fmt.Sprintf("%s%.2f%s", sign, value, units[unit])
}

func startQueryWorkers(t *testing.T, ctx context.Context, env *framework.Env, usernames []string, concurrency int) *workloadCounter {
	t.Helper()
	stats := &workloadCounter{}
	token := env.AdminTokenOrFail(t)
	for i := 0; i < concurrency; i++ {
		stats.wg.Add(1)
		go func(offset int) {
			defer stats.wg.Done()
			idx := offset
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				username := usernames[idx%len(usernames)]
				resp, err := env.AuthorizedRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), token, nil)
				if err == nil && resp != nil && resp.HTTPStatus() == http.StatusOK {
					stats.add(true)
				} else {
					stats.add(false)
				}
				idx++
				time.Sleep(20 * time.Millisecond)
			}
		}(i)
	}
	return stats
}

func startInsertWorker(ctx context.Context, env *framework.Env, count int) (*workloadCounter, []string) {
	stats := &workloadCounter{}
	created := make([]string, 0, count)
	stats.wg.Add(1)
	go func() {
		defer stats.wg.Done()
		for i := 0; i < count; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			spec := env.NewUserSpec("perf_mix_new_", perfBasePassword)
			resp, err := env.CreateUser(spec)
			if err == nil && resp.HTTPStatus() == http.StatusCreated {
				created = append(created, spec.Name)
				stats.add(true)
			} else {
				stats.add(false)
				continue
			}
			_ = env.WaitForUser(spec.Name, 10*time.Second)
			time.Sleep(30 * time.Millisecond)
		}
	}()
	return stats, created
}

func computeLatencyStats(samples []time.Duration) framework.LatencyStats {
	if len(samples) == 0 {
		return framework.LatencyStats{}
	}
	values := make([]float64, len(samples))
	sum := 0.0
	for i, d := range samples {
		ms := float64(d) / float64(time.Millisecond)
		values[i] = ms
		sum += ms
	}
	sort.Float64s(values)
	avg := sum / float64(len(values))
	return framework.LatencyStats{
		MinMS: values[0],
		MaxMS: values[len(values)-1],
		AvgMS: avg,
		P50MS: percentile(values, 0.50),
		P90MS: percentile(values, 0.90),
		P95MS: percentile(values, 0.95),
		P99MS: percentile(values, 0.99),
	}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	rank := p * float64(len(values)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return values[lower]
	}
	weight := rank - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

func ratioFloat(success, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return success / total
}

func qps(total int, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(total) / duration.Seconds()
}

func alignToBatch(total, batch int) int {
	if batch <= 1 {
		return total
	}
	remainder := total % batch
	if remainder == 0 {
		return total
	}
	return total + batch - remainder
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newScenarioMetrics() *scenarioMetricsCollector {
	return &scenarioMetricsCollector{
		httpCodes: make(map[int]int),
		bizCodes:  make(map[int]int),
		errors:    make(map[string]int),
	}
}

type scenarioMetricsCollector struct {
	mu        sync.Mutex
	durations []time.Duration
	httpCodes map[int]int
	bizCodes  map[int]int
	errors    map[string]int
	success   int64
	failure   int64
	cycles    int
}

func (c *scenarioMetricsCollector) add(duration time.Duration, httpCode, bizCode int, success bool, err error) {
	c.mu.Lock()
	c.durations = append(c.durations, duration)
	if httpCode != 0 {
		c.httpCodes[httpCode]++
	}
	if bizCode != 0 {
		c.bizCodes[bizCode]++
	}
	if err != nil {
		msg := err.Error()
		if len(msg) > 120 {
			msg = msg[:120]
		}
		c.errors[msg]++
	}
	c.mu.Unlock()
	if success {
		atomic.AddInt64(&c.success, 1)
	} else {
		atomic.AddInt64(&c.failure, 1)
	}
}

func (c *scenarioMetricsCollector) snapshot() scenarioMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	durations := append([]time.Duration(nil), c.durations...)
	httpCodes := copyIntMap(c.httpCodes)
	bizCodes := copyIntMap(c.bizCodes)
	errors := copyStrMap(c.errors)
	return scenarioMetrics{
		durations: durations,
		httpCodes: httpCodes,
		bizCodes:  bizCodes,
		errors:    errors,
		success:   atomic.LoadInt64(&c.success),
		failure:   atomic.LoadInt64(&c.failure),
	}
}

func (c *scenarioMetricsCollector) merge(metrics scenarioMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.durations = append(c.durations, metrics.durations...)
	mergeIntMap(c.httpCodes, metrics.httpCodes)
	mergeIntMap(c.bizCodes, metrics.bizCodes)
	mergeStrMap(c.errors, metrics.errors)
	atomic.AddInt64(&c.success, metrics.success)
	atomic.AddInt64(&c.failure, metrics.failure)
	c.cycles++
}

func (m scenarioMetrics) totalRequests() int {
	return int(m.success + m.failure)
}

func (m scenarioMetrics) flattenCounters() map[string]int {
	counters := make(map[string]int, len(m.httpCodes)+len(m.bizCodes)+len(m.errors))
	for code, count := range m.httpCodes {
		counters["http_"+strconv.Itoa(code)] = count
	}
	for code, count := range m.bizCodes {
		counters["code_"+strconv.Itoa(code)] = count
	}
	limit := 5
	idx := 0
	for _, count := range m.errors {
		label := fmt.Sprintf("error_%d", idx)
		counters[label] = count
		if idx++; idx >= limit {
			break
		}
	}
	return counters
}

func copyIntMap(src map[int]int) map[int]int {
	dst := make(map[int]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyStrMap(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mergeIntMap(dst, src map[int]int) {
	for k, v := range src {
		dst[k] += v
	}
}

func mergeStrMap(dst, src map[string]int) {
	for k, v := range src {
		dst[k] += v
	}
}

func (c *scenarioMetricsCollector) totalRequests() int {
	return int(atomic.LoadInt64(&c.success) + atomic.LoadInt64(&c.failure))
}
