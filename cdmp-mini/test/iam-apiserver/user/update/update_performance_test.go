package update

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const perfTestDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/update"

type perfStats struct {
	success   int
	failure   int
	duration  time.Duration
	latencies []time.Duration
}

func extractUserVersion(resp *framework.APIResponse) (uint64, error) {
	if resp == nil || len(resp.Data) == 0 {
		return 0, fmt.Errorf("empty response data")
	}
	var payload struct {
		UpdateUser struct {
			Metadata struct {
				Version uint64 `json:"version"`
			} `json:"metadata"`
		} `json:"update_user"`
	}
	if err := json.Unmarshal(resp.Data, &payload); err != nil {
		return 0, err
	}
	return payload.UpdateUser.Metadata.Version, nil
}

func TestUpdatePerformance(t *testing.T) {
	env := framework.NewEnv(t)
	env.DisableClientRateLimiter()
	outputDir := env.EnsureOutputDir(t, perfTestDir)
	recorder := framework.NewRecorder(t, outputDir, "update")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping performance tests")
	}

	const password = "InitPassw0rd!"

	const (
		baselineIterations     = 20
		parallelWorkerCount    = 20
		parallelIterationCount = 10
		batchUserCount         = 20
		batchIterationCount    = 10
		gradientIterationCount = 5
		stressWorkerCount      = 200
		stressBurstFactor      = 5
	)

	putRequest := func(env *framework.Env, spec framework.UserSpec, worker, iter int, version *uint64) error {
		if version == nil {
			return fmt.Errorf("version pointer is nil")
		}
		payload := map[string]any{
			"metadata": map[string]string{"name": spec.Name},
			"nickname": fmt.Sprintf("perf_%d_%d", worker, iter),
			"email":    fmt.Sprintf("%s-%d@example.com", spec.Name, iter),
			"version":  *version,
		}
		resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), payload)
		if err != nil {
			return err
		}
		if resp.HTTPStatus() != http.StatusOK {
			return fmt.Errorf("unexpected status=%d", resp.HTTPStatus())
		}
		if v, err := extractUserVersion(resp); err == nil && v > 0 {
			*version = v
		}
		return nil
	}

	patchRequest := func(env *framework.Env, spec framework.UserSpec, worker, iter int, _ *uint64) error {
		payload := map[string]any{
			"nickname": fmt.Sprintf("patch_perf_%d_%d", worker, iter),
		}
		resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/api/users/%s/profile", spec.Name), payload)
		if err != nil {
			return err
		}
		if resp.HTTPStatus() != http.StatusOK {
			return fmt.Errorf("unexpected status=%d", resp.HTTPStatus())
		}
		return nil
	}

	parallelScenarios := []struct {
		name        string
		description string
		workers     int
		iterations  int
		request     func(env *framework.Env, spec framework.UserSpec, worker, iter int, version *uint64) error
	}{
		{
			name:        "update_baseline_put",
			description: "PUT 单记录延迟基线",
			workers:     1,
			iterations:  baselineIterations,
			request:     putRequest,
		},
		{
			name:        "patch_baseline_profile",
			description: "PATCH 单记录延迟基线",
			workers:     1,
			iterations:  baselineIterations,
			request:     patchRequest,
		},
		{
			name:        "update_parallel_put",
			description: "PUT 更新昵称+邮箱吞吐",
			workers:     parallelWorkerCount,
			iterations:  parallelIterationCount,
			request:     putRequest,
		},
		{
			name:        "patch_parallel_profile",
			description: "PATCH 更新昵称吞吐",
			workers:     parallelWorkerCount,
			iterations:  parallelIterationCount,
			request:     patchRequest,
		},
	}

	for _, scenario := range parallelScenarios {
		stats, err := runParallelScenario(t, env, scenario.name+"_", password, scenario.workers, scenario.iterations, scenario.request)
		if err != nil {
			if errors.Is(err, framework.ErrUserVersionColumnMissing) {
				t.Skip("backend missing user version column; skipping performance tests")
			}
			t.Fatalf("seed parallel scenario %s failed: %v", scenario.name, err)
		}
		total := scenario.workers * scenario.iterations
		durationSec := stats.duration.Seconds()
		qps := float64(total)
		if durationSec > 0 {
			qps = float64(total) / durationSec
		}
		successRate := float64(stats.success) / float64(total)
		errorRate := float64(stats.failure) / float64(total)
		recorder.AddPerformance(framework.PerformancePoint{
			Scenario:     scenario.name,
			Requests:     total,
			SuccessRate:  successRate,
			ErrorRate:    errorRate,
			DurationMS:   stats.duration.Milliseconds(),
			QPS:          qps,
			ErrorCount:   stats.failure,
			SuccessCount: stats.success,
			Latency:      buildLatencyStats(stats.latencies),
			Notes:        []string{scenario.description},
		})
	}

	gradientLevels := []int{10, 50, 100, 200, 500}
	extendedPerfEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("IAM_APISERVER_PERF_EXTENDED")), "1")

	t.Run("concurrency_gradient", func(t *testing.T) {
		if !extendedPerfEnabled {
			t.Skip("set IAM_APISERVER_PERF_EXTENDED=1 to enable concurrency gradient profiling")
		}
		for _, level := range gradientLevels {
			stats, err := runParallelScenario(t, env, fmt.Sprintf("patch_gradient_%d_", level), password, level, gradientIterationCount, patchRequest)
			if err != nil {
				if errors.Is(err, framework.ErrUserVersionColumnMissing) {
					t.Skip("backend missing user version column; skipping performance tests")
				}
				var seedErr *userSeedError
				if errors.As(err, &seedErr) && seedErr.Code == code.ErrValidation {
					t.Logf("skip concurrency level %d due to validation during seed: %v", level, err)
					continue
				}
				t.Fatalf("seed concurrency level %d failed: %v", level, err)
			}
			total := level * gradientIterationCount
			durationSec := stats.duration.Seconds()
			qps := float64(total)
			if durationSec > 0 {
				qps = float64(total) / durationSec
			}
			successRate := float64(stats.success) / float64(total)
			errorRate := float64(stats.failure) / float64(total)
			recorder.AddPerformance(framework.PerformancePoint{
				Scenario:     fmt.Sprintf("patch_concurrency_%d", level),
				Requests:     total,
				SuccessRate:  successRate,
				ErrorRate:    errorRate,
				DurationMS:   stats.duration.Milliseconds(),
				QPS:          qps,
				ErrorCount:   stats.failure,
				SuccessCount: stats.success,
				Latency:      buildLatencyStats(stats.latencies),
				Notes: []string{
					fmt.Sprintf("PATCH 并发场景 (workers=%d iterations=%d)", level, gradientIterationCount),
				},
			})
		}
	})

	t.Run("stress_burst", func(t *testing.T) {
		if !extendedPerfEnabled {
			t.Skip("set IAM_APISERVER_PERF_EXTENDED=1 to enable stress burst profiling")
		}
		stats, err := runParallelScenario(t, env, "patch_stress_", password, stressWorkerCount, stressBurstFactor, patchRequest)
		if err != nil {
			if errors.Is(err, framework.ErrUserVersionColumnMissing) {
				t.Skip("backend missing user version column; skipping performance tests")
			}
			t.Fatalf("seed stress burst failed: %v", err)
		}
		total := stressWorkerCount * stressBurstFactor
		durationSec := stats.duration.Seconds()
		qps := float64(total)
		if durationSec > 0 {
			qps = float64(total) / durationSec
		}
		successRate := float64(stats.success) / float64(total)
		errorRate := float64(stats.failure) / float64(total)
		recorder.AddPerformance(framework.PerformancePoint{
			Scenario:     "patch_stress_burst",
			Requests:     total,
			SuccessRate:  successRate,
			ErrorRate:    errorRate,
			DurationMS:   stats.duration.Milliseconds(),
			QPS:          qps,
			ErrorCount:   stats.failure,
			SuccessCount: stats.success,
			Latency:      buildLatencyStats(stats.latencies),
			Notes: []string{
				fmt.Sprintf("压力测试峰值 (workers=%d burst=%d)", stressWorkerCount, stressBurstFactor),
			},
		})
	})

	dataProfileEnv := strings.ToLower(strings.TrimSpace(os.Getenv("IAM_APISERVER_PERF_DATA")))
	volumes := []struct {
		name       string
		users      int
		iterations int
		note       string
	}{
		{name: "small", users: 50, iterations: 5, note: "<10万记录，索引效率验证"},
		{name: "medium", users: 200, iterations: 3, note: "10万-100万记录，分页更新"},
		{name: "large", users: 1000, iterations: 2, note: ">100万记录，分批策略"},
	}

	t.Run("data_volume_profiles", func(t *testing.T) {
		if dataProfileEnv == "" {
			t.Skip("set IAM_APISERVER_PERF_DATA=small|medium|large|all to enable data volume profiling")
		}
		for _, volume := range volumes {
			if dataProfileEnv != "all" && dataProfileEnv != volume.name {
				continue
			}
			stats := runBatchScenario(t, env, fmt.Sprintf("batch_%s_", volume.name), password, volume.users, volume.iterations)
			total := volume.iterations
			durationSec := stats.duration.Seconds()
			qps := float64(total)
			if durationSec > 0 {
				qps = float64(total) / durationSec
			}
			successRate := float64(stats.success) / float64(total)
			errorRate := float64(stats.failure) / float64(total)
			recorder.AddPerformance(framework.PerformancePoint{
				Scenario:     fmt.Sprintf("patch_batch_%s", volume.name),
				Requests:     total,
				SuccessRate:  successRate,
				ErrorRate:    errorRate,
				DurationMS:   stats.duration.Milliseconds(),
				QPS:          qps,
				ErrorCount:   stats.failure,
				SuccessCount: stats.success,
				Latency:      buildLatencyStats(stats.latencies),
				Notes:        []string{volume.note},
			})
		}
	})

	batchStats := runBatchScenario(t, env, "patch_batch_", password, batchUserCount, batchIterationCount)
	total := batchIterationCount
	durationSec := batchStats.duration.Seconds()
	qps := float64(total)
	if durationSec > 0 {
		qps = float64(total) / durationSec
	}
	successRate := float64(batchStats.success) / float64(total)
	errorRate := float64(batchStats.failure) / float64(total)
	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:     "patch_batch_condition",
		Requests:     total,
		SuccessRate:  successRate,
		ErrorRate:    errorRate,
		DurationMS:   batchStats.duration.Milliseconds(),
		QPS:          qps,
		ErrorCount:   batchStats.failure,
		SuccessCount: batchStats.success,
		Latency:      buildLatencyStats(batchStats.latencies),
		Notes:        []string{"PATCH 批量条件更新请求耗时"},
	})
}

type userSeedError struct {
	User    string
	HTTP    int
	Code    int
	Message string
}

func (e *userSeedError) Error() string {
	return fmt.Sprintf("create user %s failed: http=%d code=%d message=%s", e.User, e.HTTP, e.Code, e.Message)
}

func runParallelScenario(t *testing.T, env *framework.Env, prefix, password string, workers, iterations int, request func(env *framework.Env, spec framework.UserSpec, worker, iter int, version *uint64) error) (perfStats, error) {
	t.Helper()
	specs := make([]framework.UserSpec, workers)
	versions := make([]uint64, workers)
	created := make([]bool, workers)
	for i := range specs {
		spec := env.NewUserSpec(fmt.Sprintf("%s%d_", prefix, i), password)
		specs[i] = spec
		resp, err := env.CreateUser(spec)
		if err != nil {
			return perfStats{}, fmt.Errorf("create user %s: %w", spec.Name, err)
		}
		if resp.HTTPStatus() != http.StatusCreated {
			return perfStats{}, &userSeedError{User: spec.Name, HTTP: resp.HTTPStatus(), Code: resp.Code, Message: resp.Message}
		}
		if waitErr := env.WaitForUser(spec.Name, 15*time.Second); waitErr != nil {
			return perfStats{}, fmt.Errorf("wait user %s: %w", spec.Name, waitErr)
		}
		versions[i] = 1
		created[i] = true
	}
	defer func() {
		for i, spec := range specs {
			if created[i] {
				env.ForceDeleteUserIgnore(spec.Name)
			}
		}
	}()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		perf perfStats
	)

	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			spec := specs[idx]
			for j := 0; j < iterations; j++ {
				reqStart := time.Now()
				if err := request(env, spec, idx, j, &versions[idx]); err != nil {
					mu.Lock()
					perf.failure++
					mu.Unlock()
					continue
				}
				elapsed := time.Since(reqStart)
				mu.Lock()
				perf.success++
				perf.latencies = append(perf.latencies, elapsed)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	perf.duration = time.Since(start)
	return perf, nil
}

func runBatchScenario(t *testing.T, env *framework.Env, prefix, password string, users, iterations int) perfStats {
	t.Helper()
	specs := make([]framework.UserSpec, users)
	for i := range specs {
		specs[i] = env.NewUserSpec(fmt.Sprintf("%s%d_", prefix, i), password)
		specs[i].IsAdmin = 0
		env.CreateUserAndWait(t, specs[i], 15*time.Second)
	}
	defer func() {
		for _, spec := range specs {
			env.ForceDeleteUserIgnore(spec.Name)
		}
	}()

	targets := make([]string, 0, len(specs)/2)
	for i, spec := range specs {
		if i%2 == 0 {
			targets = append(targets, spec.Name)
		}
	}
	if len(targets) == 0 {
		targets = append(targets, specs[0].Name)
	}

	start := time.Now()
	var perf perfStats
	for iter := 0; iter < iterations; iter++ {
		reqStart := time.Now()
		payload := map[string]any{
			"updates": map[string]any{
				"isAdmin": 1,
			},
			"conditions": map[string]any{
				"name": map[string]any{
					"in": targets,
				},
			},
		}
		resp, err := env.AdminRequest(http.MethodPatch, "/api/users", payload)
		if err != nil || resp.HTTPStatus() != http.StatusOK {
			perf.failure++
			continue
		}
		perf.success++
		perf.latencies = append(perf.latencies, time.Since(reqStart))
	}
	perf.duration = time.Since(start)
	return perf
}

func buildLatencyStats(samples []time.Duration) framework.LatencyStats {
	if len(samples) == 0 {
		return framework.LatencyStats{}
	}
	values := make([]float64, len(samples))
	var sum float64
	for i, d := range samples {
		ms := float64(d) / float64(time.Millisecond)
		values[i] = ms
		sum += ms
	}
	sort.Float64s(values)
	return framework.LatencyStats{
		MinMS: values[0],
		MaxMS: values[len(values)-1],
		AvgMS: sum / float64(len(values)),
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
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	pos := p * float64(len(values)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return values[lower]
	}
	weight := pos - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}
