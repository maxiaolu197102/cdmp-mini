package get

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const performanceOutputDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/get"

type getScenario struct {
	Name              string
	Category          string
	Description       string
	Concurrency       int
	TotalRequests     int
	Warmup            func(t *testing.T, env *framework.Env, data *perfDataset)
	Setup             func(t *testing.T, env *framework.Env, data *perfDataset) any
	Action            func(getRequest) getOutcome
	TearDown          func(t *testing.T, env *framework.Env, data *perfDataset, state any)
	Notes             []string
	ThinkTime         time.Duration
	CacheHitThreshold time.Duration
}

type getRequest struct {
	T                 *testing.T
	Env               *framework.Env
	Dataset           *perfDataset
	State             any
	WorkerID          int
	Attempt           int
	CacheHitThreshold time.Duration
}

type getOutcome struct {
	Success    bool
	HTTPStatus int
	Code       int
	Duration   time.Duration
	CacheHit   bool
	Err        error
	Flags      map[string]int
}

type getReport struct {
	duration    time.Duration
	success     int
	failure     int
	durations   []time.Duration
	cacheHits   int
	httpStatus  map[int]int
	codes       map[int]int
	flags       map[string]int
	sampleError []string
}

type perfUser struct {
	Spec   framework.UserSpec
	Tokens *framework.AuthTokens
}

type perfDataset struct {
	Base    perfUser
	Hot     []perfUser
	cleanup []string
}

func TestGetPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "get_performance")
	defer recorder.Flush(t)

	dataset := preparePerfDataset(t, env)
	t.Cleanup(func() { dataset.cleanupAll(env) })

	scenarios := []getScenario{
		{
			Name:          "single_point_baseline",
			Category:      "性能基准测试",
			Description:   "单点性能: 单线程重复查询",
			Concurrency:   1,
			TotalRequests: 50,
			Action:        baselineReadAction,
			Notes:         []string{"单点性能"},
		},
		{
			Name:          "cold_start_first_hit",
			Category:      "性能基准测试",
			Description:   "冷启动首次查询延迟",
			Concurrency:   1,
			TotalRequests: 1,
			Setup:         coldStartSetup,
			Action:        coldStartAction,
			TearDown:      coldStartTearDown,
			Notes:         []string{"冷缓存首访问"},
		},
		{
			Name:          "hot_cache_probe",
			Category:      "性能基准测试",
			Description:   "热数据查询性能",
			Concurrency:   16,
			TotalRequests: 160,
			Warmup:        warmBaseCache,
			Action:        baselineReadAction,
			Notes:         []string{"热缓存验证"},
		},
		{
			Name:          "index_hit_validation",
			Category:      "性能基准测试",
			Description:   "索引命中率近似验证 (基于响应时延)",
			Concurrency:   32,
			TotalRequests: 320,
			Warmup:        warmBaseCache,
			Action:        baselineReadAction,
			Notes:         []string{"索引命中率"},
		},
		{
			Name:          "low_concurrency_stability",
			Category:      "性能基准测试",
			Description:   "低并发(20) 连续读取稳定性",
			Concurrency:   20,
			TotalRequests: 400,
			Action:        multiUserReadAction,
			Notes:         []string{"低并发稳定性"},
		},
		{
			Name:              "high_concurrency_throughput",
			Category:          "性能基准测试",
			Description:       "高并发(128) 吞吐量测量",
			Concurrency:       128,
			TotalRequests:     1280,
			Warmup:            warmBaseCache,
			Action:            baselineReadAction,
			Notes:             []string{"高并发吞吐"},
			CacheHitThreshold: 80 * time.Millisecond,
		},
		{
			Name:              "peak_concurrency_resilience",
			Category:          "性能基准测试",
			Description:       "峰值并发(512) 降级策略观察",
			Concurrency:       512,
			TotalRequests:     2048,
			Warmup:            warmBaseCache,
			Action:            baselineReadAction,
			Notes:             []string{"峰值并发"},
			CacheHitThreshold: 120 * time.Millisecond,
		},
		{
			Name:          "connection_pool_efficiency",
			Category:      "性能基准测试",
			Description:   "连接池效率 (64 并发, 热点轮询)",
			Concurrency:   64,
			TotalRequests: 1024,
			Action:        multiUserReadAction,
			Notes:         []string{"连接池利用率"},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			report := runGetScenario(t, env, dataset, sc)
			recorder.AddPerformance(buildPerformancePoint(sc, report))
			if report.success == 0 {
				sample := ""
				if len(report.sampleError) > 0 {
					sample = report.sampleError[0]
				}
				t.Fatalf("scenario %s produced no successful requests (failures=%d sample=%s)", sc.Name, report.failure, sample)
			}
		})
	}
}

func runGetScenario(t *testing.T, env *framework.Env, dataset *perfDataset, sc getScenario) getReport {
	t.Helper()

	if sc.Warmup != nil {
		sc.Warmup(t, env, dataset)
	}

	state := any(nil)
	if sc.Setup != nil {
		state = sc.Setup(t, env, dataset)
	}

	report := getReport{
		durations:  make([]time.Duration, 0, sc.TotalRequests),
		httpStatus: make(map[int]int),
		codes:      make(map[int]int),
		flags:      make(map[string]int),
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex

	start := time.Now()
	for worker := 0; worker < sc.Concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for attempt := range jobs {
				req := getRequest{
					T:                 t,
					Env:               env,
					Dataset:           dataset,
					State:             state,
					WorkerID:          workerID,
					Attempt:           attempt,
					CacheHitThreshold: sc.CacheHitThreshold,
				}
				outcome := sc.Action(req)

				mu.Lock()
				if outcome.Duration > 0 {
					report.durations = append(report.durations, outcome.Duration)
				}
				if outcome.Success {
					report.success++
				} else {
					report.failure++
					if outcome.Err != nil && len(report.sampleError) < 3 {
						report.sampleError = append(report.sampleError, outcome.Err.Error())
					}
				}
				if outcome.CacheHit {
					report.cacheHits++
				}
				report.httpStatus[outcome.HTTPStatus]++
				report.codes[outcome.Code]++
				if outcome.Flags != nil {
					for k, v := range outcome.Flags {
						report.flags[k] += v
					}
				}
				mu.Unlock()

				if sc.ThinkTime > 0 {
					time.Sleep(sc.ThinkTime)
				}
			}
		}(worker)
	}

	for i := 0; i < sc.TotalRequests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	report.duration = time.Since(start)

	if sc.TearDown != nil {
		sc.TearDown(t, env, dataset, state)
	}

	return report
}

func buildPerformancePoint(sc getScenario, report getReport) framework.PerformancePoint {
	total := report.success + report.failure
	if total == 0 {
		total = 1
	}
	duration := report.duration
	if duration <= 0 {
		duration = time.Millisecond
	}

	latency := computeLatencyStats(report.durations)
	successRate := float64(report.success) / float64(total)
	errorRate := 1 - successRate
	qps := float64(total) / duration.Seconds()

	counters := map[string]int{
		"cache_hits":   report.cacheHits,
		"cache_checks": total,
		"http_200":     report.httpStatus[http.StatusOK],
		"http_401":     report.httpStatus[http.StatusUnauthorized],
		"http_429":     report.httpStatus[http.StatusTooManyRequests],
		"failures":     report.failure,
	}

	notes := append([]string{sc.Description}, sc.Notes...)
	if total > 0 {
		hitRate := (float64(report.cacheHits) / float64(total)) * 100
		notes = append(notes, fmt.Sprintf("cache_hit_rate=%.2f%%", hitRate))
	}
	if len(report.sampleError) > 0 {
		notes = append(notes, fmt.Sprintf("sample_error=%s", report.sampleError[0]))
	}

	return framework.PerformancePoint{
		Scenario:     fmt.Sprintf("%s/%s", sc.Category, sc.Name),
		Requests:     total,
		SuccessRate:  successRate,
		ErrorRate:    errorRate,
		DurationMS:   duration.Milliseconds(),
		QPS:          qps,
		ErrorCount:   report.failure,
		SuccessCount: report.success,
		Latency:      latency,
		Counters:     counters,
		Notes:        notes,
	}
}

func computeLatencyStats(samples []time.Duration) framework.LatencyStats {
	if len(samples) == 0 {
		return framework.LatencyStats{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	total := time.Duration(0)
	for _, d := range sorted {
		total += d
	}
	avg := time.Duration(int64(total) / int64(len(sorted)))

	return framework.LatencyStats{
		MinMS: durationToMilliseconds(sorted[0]),
		MaxMS: durationToMilliseconds(sorted[len(sorted)-1]),
		AvgMS: durationToMilliseconds(avg),
		P50MS: durationToMilliseconds(percentileDuration(sorted, 0.50)),
		P90MS: durationToMilliseconds(percentileDuration(sorted, 0.90)),
		P95MS: durationToMilliseconds(percentileDuration(sorted, 0.95)),
		P99MS: durationToMilliseconds(percentileDuration(sorted, 0.99)),
	}
}

func percentileDuration(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if quantile <= 0 {
		return sorted[0]
	}
	if quantile >= 1 {
		return sorted[len(sorted)-1]
	}
	position := quantile * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	weight := position - float64(lower)
	delta := sorted[upper] - sorted[lower]
	return sorted[lower] + time.Duration(weight*float64(delta))
}

func durationToMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func preparePerfDataset(t *testing.T, env *framework.Env) *perfDataset {
	t.Helper()
	data := &perfDataset{}
	data.Base = data.newUser(t, env, "getperf_base_", true, nil)
	data.Hot = append(data.Hot, data.Base)
	for i := 0; i < 4; i++ {
		idx := i
		user := data.newUser(t, env, fmt.Sprintf("getperf_hot_%02d_", idx), true, nil)
		data.Hot = append(data.Hot, user)
	}
	return data
}

func (d *perfDataset) newUser(t *testing.T, env *framework.Env, prefix string, needToken bool, mutate func(*framework.UserSpec)) perfUser {
	t.Helper()
	spec := env.NewUserSpec(prefix, datasetPassword)
	if mutate != nil {
		mutate(&spec)
	}
	env.CreateUserAndWait(t, spec, 5*time.Second)
	d.cleanup = append(d.cleanup, spec.Name)
	user := perfUser{Spec: spec}
	if needToken {
		user.Tokens = env.LoginOrFail(t, spec.Name, spec.Password)
	}
	return user
}

func (d *perfDataset) cleanupAll(env *framework.Env) {
	seen := make(map[string]struct{}, len(d.cleanup))
	for i := len(d.cleanup) - 1; i >= 0; i-- {
		name := d.cleanup[i]
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		env.ForceDeleteUserIgnore(name)
	}
}

func baselineReadAction(req getRequest) getOutcome {
	token := req.Env.AdminTokenOrFail(req.T)
	return callGet(req, token, req.Dataset.Base.Spec.Name)
}

func multiUserReadAction(req getRequest) getOutcome {
	if len(req.Dataset.Hot) == 0 {
		return baselineReadAction(req)
	}
	idx := req.Attempt % len(req.Dataset.Hot)
	target := req.Dataset.Hot[idx]
	token := ""
	if target.Tokens != nil {
		token = target.Tokens.AccessToken
	} else {
		token = req.Env.AdminTokenOrFail(req.T)
	}
	return callGet(req, token, target.Spec.Name)
}

func coldStartSetup(t *testing.T, env *framework.Env, data *perfDataset) any {
	user := data.newUser(t, env, "getperf_cold_", false, nil)
	return &user
}

func coldStartAction(req getRequest) getOutcome {
	record, _ := req.State.(*perfUser)
	token := req.Env.AdminTokenOrFail(req.T)
	if record == nil {
		return callGet(req, token, req.Dataset.Base.Spec.Name)
	}
	return callGet(req, token, record.Spec.Name)
}

func coldStartTearDown(t *testing.T, env *framework.Env, data *perfDataset, state any) {
	record, _ := state.(*perfUser)
	if record == nil {
		return
	}
	env.ForceDeleteUserIgnore(record.Spec.Name)
}

func warmBaseCache(t *testing.T, env *framework.Env, data *perfDataset) {
	token := env.AdminTokenOrFail(t)
	for i := 0; i < 3; i++ {
		_, _ = env.GetUser(token, data.Base.Spec.Name)
		time.Sleep(5 * time.Millisecond)
	}
}

func callGet(req getRequest, token, username string) getOutcome {
	start := time.Now()
	resp, err := req.Env.GetUser(token, username)
	duration := time.Since(start)
	outcome := getOutcome{
		Duration: duration,
		Flags:    map[string]int{"requests": 1},
	}
	if err != nil {
		outcome.Err = err
		outcome.Flags["failure"]++
		return outcome
	}
	outcome.HTTPStatus = resp.HTTPStatus()
	outcome.Code = resp.Code
	outcome.Success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess
	threshold := req.CacheHitThreshold
	if threshold <= 0 {
		threshold = cacheHitThreshold
	}
	if outcome.Success && duration <= threshold {
		outcome.CacheHit = true
		outcome.Flags["cache_hit"]++
	}
	if !outcome.Success {
		outcome.Flags["failure"]++
	}
	outcome.Flags[fmt.Sprintf("http_%d", outcome.HTTPStatus)]++
	outcome.Flags[fmt.Sprintf("code_%d", outcome.Code)]++
	return outcome
}
