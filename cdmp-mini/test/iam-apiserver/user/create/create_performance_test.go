package create

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net/http"
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
	perfUsernamePrefix    = "createperf"
	cacheDefaultThreshold = 50 * time.Millisecond
	waitDefaultTimeout    = 15 * time.Second
)

const perfOutputDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/create"

type loadPattern string

type scenarioCategory string

type slaTargets struct {
	Avg         time.Duration
	P95         time.Duration
	P99         time.Duration
	SuccessRate float64
}

type scenarioOptions struct {
	WaitForReady      time.Duration
	SkipWaitForReady  bool
	CacheChecks       int
	CacheHitThreshold time.Duration
	EnforceSLA        bool
	AllowDegradation  bool
	SkipInShort       bool
	SLATargets        slaTargets
	AvailabilityGoal  float64
	AspirationalTPS   float64
	Notes             []string
}

type workloadStage struct {
	Name        string
	Requests    int
	Duration    time.Duration
	Concurrency int
	ThinkTime   time.Duration
	Pattern     loadPattern
	DelayAfter  time.Duration
}

type performanceScenario struct {
	Name        string
	Category    scenarioCategory
	Description string
	Pattern     loadPattern
	Stages      []workloadStage
	Options     scenarioOptions
	Generator   userGenerator
}

type userVariant struct {
	Spec           framework.UserSpec
	NameBucket     string
	PasswordBucket string
	EmailBucket    string
	PhoneBucket    string
	AdditionalTag  string
}

type userGenerator func(seq uint64) userVariant

type operationOutcome struct {
	success      bool
	apiDuration  time.Duration
	waitDuration time.Duration
	cacheHits    int
	cacheChecks  int
	err          error
	variant      userVariant
	created      bool
	degraded     bool
}

type stageSummary struct {
	Name        string
	Pattern     loadPattern
	Duration    time.Duration
	Requests    int
	Success     int
	Failure     int
	Concurrency int
	Degraded    int
}

type scenarioResult struct {
	scenario        performanceScenario
	mu              sync.Mutex
	seq             atomic.Uint64
	seqBase         uint64
	start           time.Time
	end             time.Time
	durations       []time.Duration
	waitDurations   []time.Duration
	success         int
	failure         int
	degraded        int
	errors          []string
	cacheHits       int
	cacheChecks     int
	cleanup         map[string]cleanupState
	nameBuckets     map[string]int
	passwordBuckets map[string]int
	emailBuckets    map[string]int
	phoneBuckets    map[string]int
	stageSummaries  []stageSummary
}

type cleanupTask struct {
	Name        string
	RequireWait bool
	Degraded    bool
}

type cleanupState struct {
	RequireWait bool
	Degraded    bool
}

type scenarioMetrics struct {
	Scenario        performanceScenario
	Requests        int
	Success         int
	Failure         int
	Degraded        int
	Duration        time.Duration
	Avg             time.Duration
	P50             time.Duration
	P90             time.Duration
	P95             time.Duration
	P99             time.Duration
	Min             time.Duration
	Max             time.Duration
	WaitAvg         time.Duration
	WaitP95         time.Duration
	QPS             float64
	TPS             float64
	SuccessRate     float64
	DegradeRate     float64
	CacheHits       int
	CacheChecks     int
	CacheHitRate    float64
	StageSummaries  []stageSummary
	NameBuckets     map[string]int
	PasswordBuckets map[string]int
	EmailBuckets    map[string]int
	PhoneBuckets    map[string]int
	PeakConcurrency int
	Cleanup         []string
}

const (
	patternUniform loadPattern = "uniform"
	patternSpike   loadPattern = "spike"
	patternRamp    loadPattern = "ramp"
)

const (
	categoryBaseline    scenarioCategory = "baseline"
	categoryStress      scenarioCategory = "stress"
	categorySpecialized scenarioCategory = "specialized"
)

var defaultSLATarget = slaTargets{
	Avg:         230 * time.Millisecond,
	P95:         320 * time.Millisecond,
	P99:         420 * time.Millisecond,
	SuccessRate: 0.995,
}

func (o scenarioOptions) normalized() scenarioOptions {
	if o.WaitForReady <= 0 {
		o.WaitForReady = waitDefaultTimeout
	}
	if o.CacheHitThreshold <= 0 {
		o.CacheHitThreshold = cacheDefaultThreshold
	}
	if o.SLATargets.Avg == 0 {
		o.SLATargets.Avg = defaultSLATarget.Avg
	}
	if o.SLATargets.P95 == 0 {
		o.SLATargets.P95 = defaultSLATarget.P95
	}
	if o.SLATargets.P99 == 0 {
		o.SLATargets.P99 = defaultSLATarget.P99
	}
	if o.SLATargets.SuccessRate == 0 {
		o.SLATargets.SuccessRate = defaultSLATarget.SuccessRate
	}
	if o.AvailabilityGoal == 0 {
		o.AvailabilityGoal = o.SLATargets.SuccessRate
	}
	return o
}

func newScenarioResult(sc performanceScenario) *scenarioResult {
	return &scenarioResult{
		scenario:        sc,
		cleanup:         make(map[string]cleanupState),
		nameBuckets:     make(map[string]int),
		passwordBuckets: make(map[string]int),
		emailBuckets:    make(map[string]int),
		phoneBuckets:    make(map[string]int),
		seqBase:         uint64(time.Now().UnixNano() % 1_000_000),
	}
}

func (r *scenarioResult) nextSequence() uint64 {
	return r.seqBase + (r.seq.Add(1) - 1)
}

func (r *scenarioResult) record(outcome operationOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durations = append(r.durations, outcome.apiDuration)
	if outcome.waitDuration > 0 {
		r.waitDurations = append(r.waitDurations, outcome.waitDuration)
	}
	if outcome.degraded {
		r.degraded++
		state := r.cleanup[outcome.variant.Spec.Name]
		state.Degraded = true
		state.RequireWait = true
		r.cleanup[outcome.variant.Spec.Name] = state
	} else if outcome.success {
		r.success++
	} else {
		r.failure++
		if outcome.err != nil {
			r.errors = append(r.errors, outcome.err.Error())
		}
	}
	if outcome.created {
		requireWait := r.scenario.Options.SkipWaitForReady
		state := r.cleanup[outcome.variant.Spec.Name]
		if requireWait {
			state.RequireWait = true
		}
		state.Degraded = false
		r.cleanup[outcome.variant.Spec.Name] = state
	}
	if outcome.cacheChecks > 0 {
		r.cacheChecks += outcome.cacheChecks
		r.cacheHits += outcome.cacheHits
	}
	r.nameBuckets[outcome.variant.NameBucket]++
	r.passwordBuckets[outcome.variant.PasswordBucket]++
	r.emailBuckets[outcome.variant.EmailBucket]++
	r.phoneBuckets[outcome.variant.PhoneBucket]++
}

func (r *scenarioResult) snapshot() (success int, failure int, degraded int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.success, r.failure, r.degraded
}

func (r *scenarioResult) addStageSummary(summary stageSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stageSummaries = append(r.stageSummaries, summary)
}

func (r *scenarioResult) cleanupList() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.cleanup))
	for name := range r.cleanup {
		out = append(out, name)
	}
	return out
}

func (r *scenarioResult) cleanupTasks() []cleanupTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	tasks := make([]cleanupTask, 0, len(r.cleanup))
	for name, state := range r.cleanup {
		tasks = append(tasks, cleanupTask{
			Name:        name,
			RequireWait: state.RequireWait,
			Degraded:    state.Degraded,
		})
	}
	return tasks
}

func (r *scenarioResult) errorsSnapshot(limit int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 || limit >= len(r.errors) {
		return append([]string(nil), r.errors...)
	}
	return append([]string(nil), r.errors[:limit]...)
}

func (r *scenarioResult) latencySamples() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]time.Duration, len(r.durations))
	copy(copied, r.durations)
	return copied
}

func (r *scenarioResult) waitSamples() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]time.Duration, len(r.waitDurations))
	copy(copied, r.waitDurations)
	return copied
}

func (r *scenarioResult) metrics() scenarioMetrics {
	samples := r.latencySamples()
	waitSamples := r.waitSamples()
	success, failure, degraded := r.snapshot()
	total := success + failure + degraded
	duration := r.end.Sub(r.start)
	stats := computeLatencyStats(samples)
	waitStats := computeLatencyStats(waitSamples)
	peak := 0
	for _, stage := range r.stageSummaries {
		if stage.Concurrency > peak {
			peak = stage.Concurrency
		}
	}
	var qps, tps, cacheRate float64
	if duration > 0 {
		qps = float64(total) / duration.Seconds()
		tps = float64(success+degraded) / duration.Seconds()
	}
	if r.cacheChecks > 0 {
		cacheRate = float64(r.cacheHits) / float64(r.cacheChecks)
	}
	successRate := 0.0
	degradeRate := 0.0
	if total > 0 {
		successRate = float64(success+degraded) / float64(total)
		degradeRate = float64(degraded) / float64(total)
	}
	return scenarioMetrics{
		Scenario:        r.scenario,
		Requests:        total,
		Success:         success,
		Failure:         failure,
		Degraded:        degraded,
		Duration:        duration,
		Avg:             stats.avg,
		P50:             stats.p50,
		P90:             stats.p90,
		P95:             stats.p95,
		P99:             stats.p99,
		Min:             stats.min,
		Max:             stats.max,
		WaitAvg:         waitStats.avg,
		WaitP95:         waitStats.p95,
		QPS:             qps,
		TPS:             tps,
		SuccessRate:     successRate,
		DegradeRate:     degradeRate,
		CacheHits:       r.cacheHits,
		CacheChecks:     r.cacheChecks,
		CacheHitRate:    cacheRate,
		StageSummaries:  append([]stageSummary(nil), r.stageSummaries...),
		NameBuckets:     cloneCounter(r.nameBuckets),
		PasswordBuckets: cloneCounter(r.passwordBuckets),
		EmailBuckets:    cloneCounter(r.emailBuckets),
		PhoneBuckets:    cloneCounter(r.phoneBuckets),
		PeakConcurrency: peak,
		Cleanup:         r.cleanupList(),
	}
}

type latencySnapshot struct {
	min time.Duration
	max time.Duration
	avg time.Duration
	p50 time.Duration
	p90 time.Duration
	p95 time.Duration
	p99 time.Duration
}

func computeLatencyStats(samples []time.Duration) latencySnapshot {
	if len(samples) == 0 {
		return latencySnapshot{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, v := range sorted {
		total += v
	}
	avg := time.Duration(int64(total) / int64(len(sorted)))
	return latencySnapshot{
		min: sorted[0],
		max: sorted[len(sorted)-1],
		avg: avg,
		p50: percentile(sorted, 0.50),
		p90: percentile(sorted, 0.90),
		p95: percentile(sorted, 0.95),
		p99: percentile(sorted, 0.99),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	rank := p * float64(len(sorted)-1)
	idx := int(math.Round(rank))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func cloneCounter(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func runPerformanceScenario(t *testing.T, env *framework.Env, recorder *framework.Recorder, sc performanceScenario) scenarioMetrics {
	t.Helper()
	env.AdminTokenOrFail(t)
	sc.Options = sc.Options.normalized()
	res := newScenarioResult(sc)
	t.Cleanup(func() {
		tasks := res.cleanupTasks()
		standard := make([]cleanupTask, 0, len(tasks))
		degraded := make([]string, 0, len(tasks))
		for _, task := range tasks {
			if task.Degraded {
				if task.Name != "" {
					degraded = append(degraded, task.Name)
				}
				continue
			}
			standard = append(standard, task)
		}
		followup := parallelCleanup(t, env, standard, sc.Options)
		if len(followup) > 0 {
			degraded = append(degraded, followup...)
		}
		if len(degraded) > 0 {
			cleanupDegradedUsers(t, env, degraded, sc.Options.WaitForReady)
		}
	})
	res.start = time.Now()
	for _, stage := range sc.Stages {
		startSuccess, startFailure, startDegraded := res.snapshot()
		stageStart := time.Now()
		executeStage(env, res, sc, stage)
		successAfter, failureAfter, degradedAfter := res.snapshot()
		summary := stageSummary{
			Name:        stage.Name,
			Pattern:     stage.Pattern,
			Duration:    time.Since(stageStart),
			Requests:    (successAfter + failureAfter + degradedAfter) - (startSuccess + startFailure + startDegraded),
			Success:     successAfter - startSuccess,
			Failure:     failureAfter - startFailure,
			Degraded:    degradedAfter - startDegraded,
			Concurrency: stage.Concurrency,
		}
		res.addStageSummary(summary)
		if stage.DelayAfter > 0 {
			time.Sleep(stage.DelayAfter)
		}
	}
	res.end = time.Now()

	metrics := res.metrics()
	if metrics.Success == 0 && metrics.Degraded == 0 {
		errs := res.errorsSnapshot(5)
		if len(errs) > 0 {
			t.Logf("sample errors: %s", strings.Join(errs, "; "))
		}
	} else if metrics.Failure > 0 {
		errs := res.errorsSnapshot(3)
		if len(errs) > 0 {
			t.Logf("sample errors: %s", strings.Join(errs, "; "))
		}
	}
	notes := evaluateExpectations(t, sc, metrics)
	point := framework.PerformancePoint{
		Scenario:     fmt.Sprintf("%s/%s", sc.Category, sc.Name),
		Requests:     metrics.Requests,
		SuccessRate:  metrics.SuccessRate,
		ErrorRate:    1 - metrics.SuccessRate,
		DurationMS:   metrics.Duration.Milliseconds(),
		QPS:          metrics.QPS,
		ErrorCount:   metrics.Failure,
		SuccessCount: metrics.Success,
		Latency: framework.LatencyStats{
			MinMS: durationToMS(metrics.Min),
			MaxMS: durationToMS(metrics.Max),
			AvgMS: durationToMS(metrics.Avg),
			P50MS: durationToMS(metrics.P50),
			P90MS: durationToMS(metrics.P90),
			P95MS: durationToMS(metrics.P95),
			P99MS: durationToMS(metrics.P99),
		},
		Counters: map[string]int{
			"cache_hits":       metrics.CacheHits,
			"cache_checks":     metrics.CacheChecks,
			"peak_concurrency": metrics.PeakConcurrency,
			"stages":           len(metrics.StageSummaries),
			"degraded":         metrics.Degraded,
		},
		Notes: append(notes, formatDistributionNotes(metrics)...),
	}
	point.Notes = append(point.Notes, fmt.Sprintf("TPS=%.2f", metrics.TPS))
	if metrics.CacheChecks > 0 {
		point.Notes = append(point.Notes, fmt.Sprintf("Cache hit rate=%.2f%%", metrics.CacheHitRate*100))
	}
	if metrics.Degraded > 0 {
		point.Notes = append(point.Notes, fmt.Sprintf("Degraded requests=%d (%.2f%%)", metrics.Degraded, metrics.DegradeRate*100))
	}
	if metrics.WaitAvg > 0 {
		point.Notes = append(point.Notes, fmt.Sprintf("Mean wait-for-ready=%s", metrics.WaitAvg))
	}
	recorder.AddPerformance(point)
	return metrics
}

func parallelCleanup(t testing.TB, env *framework.Env, tasks []cleanupTask, opts scenarioOptions) []string {
	t.Helper()
	if len(tasks) == 0 {
		return nil
	}
	workerCount := runtime.NumCPU()
	if workerCount < 2 {
		workerCount = 2
	}
	if workerCount > 16 {
		workerCount = 16
	}
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}
	if workerCount == 0 {
		workerCount = 1
	}
	jobs := make(chan cleanupTask)
	var wg sync.WaitGroup
	waitTimeout := opts.WaitForReady
	if waitTimeout <= 0 {
		waitTimeout = waitDefaultTimeout
	}
	var followupMu sync.Mutex
	followup := make([]string, 0)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if task.Name == "" {
					continue
				}
				var waitErr error
				if task.RequireWait {
					if err := env.WaitForUser(task.Name, waitTimeout); err != nil {
						t.Logf("cleanup wait for user %s failed: %v", task.Name, err)
						waitErr = err
					}
				}
				needFollow := false
				deleted, err := attemptForceDelete(env, task.Name)
				if err != nil {
					t.Logf("cleanup delete %s failed: %v", task.Name, err)
					needFollow = true
				} else if !deleted {
					needFollow = true
				} else if waitErr != nil {
					// Wait failed but delete succeeded; nothing further to do.
					waitErr = nil
				}
				if needFollow {
					followupMu.Lock()
					followup = append(followup, task.Name)
					followupMu.Unlock()
				}
			}
		}()
	}
	for _, task := range tasks {
		if task.Name == "" {
			continue
		}
		jobs <- task
	}
	close(jobs)
	wg.Wait()
	return followup
}

func cleanupDegradedUsers(t testing.TB, env *framework.Env, names []string, waitHint time.Duration) {
	t.Helper()
	if len(names) == 0 {
		return
	}
	pending := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		pending[name] = struct{}{}
	}
	if len(pending) == 0 {
		return
	}
	deadline := time.Now().Add(resolveDegradedCleanupDeadline(waitHint))
	time.Sleep(2 * time.Second)
	for len(pending) > 0 {
		if time.Now().After(deadline) {
			break
		}
		for name := range pending {
			exists, err := degradedUserExists(env, name)
			if err != nil {
				t.Logf("degraded cleanup probe for %s failed: %v", name, err)
				continue
			}
			if !exists {
				delete(pending, name)
				continue
			}
			deleted, err := attemptForceDelete(env, name)
			if err != nil {
				t.Logf("degraded cleanup delete %s failed: %v", name, err)
				continue
			}
			if deleted {
				delete(pending, name)
			}
		}
		if len(pending) == 0 {
			break
		}
		time.Sleep(1500 * time.Millisecond)
	}
	for name := range pending {
		t.Logf("cleanup degraded user %s timed out; manual cleanup may be required", name)
	}
}

func resolveDegradedCleanupDeadline(waitHint time.Duration) time.Duration {
	base := 2 * time.Minute
	if waitHint <= 0 {
		waitHint = waitDefaultTimeout
	}
	factor := waitHint * 6
	if factor > base {
		base = factor
	}
	return base
}

func degradedUserExists(env *framework.Env, name string) (bool, error) {
	resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", name), nil)
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, fmt.Errorf("nil response while probing user %s", name)
	}
	switch resp.HTTPStatus() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		if resp.Code == code.ErrUserNotFound {
			return false, nil
		}
		msg := strings.ToLower(resp.Message)
		errMsg := strings.ToLower(resp.Error)
		if strings.Contains(msg, "not found") || strings.Contains(errMsg, "not found") {
			return false, nil
		}
		return false, fmt.Errorf("unexpected status %d while probing user %s", resp.HTTPStatus(), name)
	}
}

func attemptForceDelete(env *framework.Env, name string) (bool, error) {
	resp, err := env.ForceDeleteUser(name)
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, fmt.Errorf("nil response while deleting user %s", name)
	}
	switch resp.HTTPStatus() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected delete status %d for user %s", resp.HTTPStatus(), name)
	}
}

func executeStage(env *framework.Env, res *scenarioResult, sc performanceScenario, stage workloadStage) {
	concurrency := stage.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	var wg sync.WaitGroup
	if stage.Requests > 0 {
		jobs := make(chan uint64)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for seq := range jobs {
					variant := sc.Generator(seq)
					outcome := executeRequest(env, variant, sc.Options)
					res.record(outcome)
					if stage.ThinkTime > 0 {
						time.Sleep(stage.ThinkTime)
					}
				}
			}()
		}
		for i := 0; i < stage.Requests; i++ {
			jobs <- res.nextSequence()
		}
		close(jobs)
		wg.Wait()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), stage.Duration)
	defer cancel()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				seq := res.nextSequence()
				variant := sc.Generator(seq)
				outcome := executeRequest(env, variant, sc.Options)
				res.record(outcome)
				if stage.ThinkTime > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(stage.ThinkTime):
					}
				}
			}
		}()
	}
	wg.Wait()
}

func executeRequest(env *framework.Env, variant userVariant, options scenarioOptions) operationOutcome {
	start := time.Now()
	resp, err := env.CreateUser(variant.Spec)
	apiDuration := time.Since(start)
	outcome := operationOutcome{variant: variant, apiDuration: apiDuration}
	if err != nil {
		outcome.err = err
		return outcome
	}
	if resp == nil {
		outcome.err = fmt.Errorf("unexpected status: nil response (user=%s phone=%s)", variant.Spec.Name, variant.Spec.Phone)
		return outcome
	}
	if resp.Code == code.ErrKafkaFailed {
		outcome.degraded = true
		return outcome
	}
	if resp.HTTPStatus() != http.StatusCreated {
		outcome.err = fmt.Errorf("unexpected status: %d code=%d msg=%s (user=%s phone=%s)", resp.HTTPStatus(), resp.Code, strings.TrimSpace(resp.Message), variant.Spec.Name, variant.Spec.Phone)
		return outcome
	}
	outcome.created = true

	waitStart := time.Now()
	if !options.SkipWaitForReady {
		if err := env.WaitForUser(variant.Spec.Name, options.WaitForReady); err != nil {
			outcome.err = fmt.Errorf("wait for user: %w", err)
			return outcome
		}
		outcome.waitDuration = time.Since(waitStart)
	}
	outcome.success = true

	if options.CacheChecks > 0 {
		hits := 0
		totalChecks := options.CacheChecks

		// 预先通过一次查询填充缓存，避免把首个冷启动请求计入命中统计。
		_, _ = env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", variant.Spec.Name), nil)

		for i := 0; i < totalChecks; i++ {
			fetchStart := time.Now()
			resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", variant.Spec.Name), nil)
			fetchDuration := time.Since(fetchStart)
			if err == nil && resp != nil && resp.HTTPStatus() == http.StatusOK && fetchDuration <= options.CacheHitThreshold {
				hits++
			}
		}

		outcome.cacheChecks = totalChecks
		outcome.cacheHits = hits
	}
	return outcome
}

func evaluateExpectations(t *testing.T, sc performanceScenario, metrics scenarioMetrics) []string {
	options := sc.Options
	notes := []string{sc.Description, fmt.Sprintf("Load pattern=%s", sc.Pattern)}
	if metrics.Degraded > 0 {
		if !options.AllowDegradation {
			t.Errorf("%s: observed %d degraded requests but degradation is disabled", sc.Name, metrics.Degraded)
		} else {
			notes = append(notes, fmt.Sprintf("degraded=%d (%.2f%%)", metrics.Degraded, metrics.DegradeRate*100))
		}
	}
	if options.EnforceSLA && !options.AllowDegradation {
		if metrics.SuccessRate < options.SLATargets.SuccessRate {
			t.Errorf("%s: success rate %.4f below target %.4f", sc.Name, metrics.SuccessRate, options.SLATargets.SuccessRate)
		}
		if metrics.Avg > options.SLATargets.Avg {
			t.Logf("%s latency breakdown: avg=%s p95=%s p99=%s min=%s max=%s wait_avg=%s success=%d failure=%d", sc.Name, metrics.Avg, metrics.P95, metrics.P99, metrics.Min, metrics.Max, metrics.WaitAvg, metrics.Success, metrics.Failure)
			t.Errorf("%s: average latency %s above target %s", sc.Name, metrics.Avg, options.SLATargets.Avg)
		}
		if metrics.P95 > options.SLATargets.P95 {
			t.Logf("%s latency breakdown: avg=%s p95=%s p99=%s min=%s max=%s wait_avg=%s", sc.Name, metrics.Avg, metrics.P95, metrics.P99, metrics.Min, metrics.Max, metrics.WaitAvg)
			t.Errorf("%s: p95 latency %s above target %s", sc.Name, metrics.P95, options.SLATargets.P95)
		}
		if metrics.P99 > options.SLATargets.P99 {
			t.Logf("%s latency breakdown: avg=%s p95=%s p99=%s min=%s max=%s wait_avg=%s", sc.Name, metrics.Avg, metrics.P95, metrics.P99, metrics.Min, metrics.Max, metrics.WaitAvg)
			t.Errorf("%s: p99 latency %s above target %s", sc.Name, metrics.P99, options.SLATargets.P99)
		}
	} else {
		if metrics.SuccessRate < options.SLATargets.SuccessRate {
			notes = append(notes, fmt.Sprintf("success rate %.4f below goal %.4f", metrics.SuccessRate, options.SLATargets.SuccessRate))
		}
		if metrics.P99 > options.SLATargets.P99 {
			notes = append(notes, fmt.Sprintf("p99=%s above goal %s", metrics.P99, options.SLATargets.P99))
		}
	}
	if options.AspirationalTPS > 0 && metrics.TPS < options.AspirationalTPS {
		notes = append(notes, fmt.Sprintf("TPS %.2f below aspiration %.2f", metrics.TPS, options.AspirationalTPS))
	}
	notes = append(notes, options.Notes...)
	return notes
}

func formatDistributionNotes(metrics scenarioMetrics) []string {
	nameDist := summariseBuckets(metrics.NameBuckets, "username")
	passDist := summariseBuckets(metrics.PasswordBuckets, "password")
	emailDist := summariseBuckets(metrics.EmailBuckets, "email")
	phoneDist := summariseBuckets(metrics.PhoneBuckets, "phone")
	out := []string{nameDist, passDist, emailDist, phoneDist}
	return out
}

func summariseBuckets(buckets map[string]int, label string) string {
	if len(buckets) == 0 {
		return fmt.Sprintf("%s distribution unavailable", label)
	}
	pairs := make([]string, 0, len(buckets))
	for k, v := range buckets {
		pairs = append(pairs, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(pairs)
	return fmt.Sprintf("%s distribution: %s", label, strings.Join(pairs, ", "))
}

func durationToMS(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(d) / float64(time.Millisecond)
}

func baselineSerialScenario() performanceScenario {
	return performanceScenario{
		Name:        "baseline_serial",
		Category:    categoryBaseline,
		Description: "单用户串行创建 - 基础性能基准",
		Pattern:     patternUniform,
		Generator:   newDefaultGenerator("serial"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			SLATargets: slaTargets{
				Avg: 250 * time.Millisecond,
				P95: 350 * time.Millisecond,
				P99: 400 * time.Millisecond,
			},
			Notes: []string{"串行创建测得平均延迟约220-230ms，设定宽松基线"},
		},
		Stages: []workloadStage{
			{
				Name:        "serial_warmup",
				Requests:    15,
				Concurrency: 1,
				ThinkTime:   20 * time.Millisecond,
				Pattern:     patternUniform,
			},
		},
	}
}

func baselineConcurrentScenario() performanceScenario {
	return performanceScenario{
		Name:        "baseline_concurrent",
		Category:    categoryBaseline,
		Description: "多用户并发创建 - 模拟真实并发场景",
		Pattern:     patternUniform,
		Generator:   newDefaultGenerator("parallel"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			WaitForReady:     15 * time.Second,
			SLATargets: slaTargets{
				Avg:         280 * time.Millisecond,
				P95:         460 * time.Millisecond,
				P99:         620 * time.Millisecond,
				SuccessRate: 0.999,
			},
			Notes: []string{
				"目标对齐登录压测并发，生产端需快速拉满吞吐",
				"SLA 基于近期基准结果: avg~277ms, p95~453ms, p99~599ms",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "parallel_batch",
				Requests:    2048,
				Concurrency: 64,
				Pattern:     patternUniform,
			},
		},
	}
}

func baselineSustainedScenario() performanceScenario {
	return performanceScenario{
		Name:        "baseline_sustained",
		Category:    categoryBaseline,
		Description: "持续负载测试 - 长时间运行检测内存泄漏",
		Pattern:     patternUniform,
		Generator:   newDefaultGenerator("sustained"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			SkipInShort:      true,
			WaitForReady:     45 * time.Second,
			Notes: []string{
				"持续负载周期约30s，用于观察内存与资源走势",
				"取消ThinkTime，关注高并发持续压力下的稳定性",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "steady_state",
				Duration:    30 * time.Second,
				Concurrency: 32,
				Pattern:     patternUniform,
				DelayAfter:  2 * time.Second,
			},
		},
	}
}

func stressSpikeScenario() performanceScenario {
	return performanceScenario{
		Name:        "stress_spike",
		Category:    categoryStress,
		Description: "峰值流量测试 - 短时间内突发高并发",
		Pattern:     patternSpike,
		Generator:   newDefaultGenerator("spike"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			WaitForReady:     45 * time.Second,
			AspirationalTPS:  800,
			Notes:            []string{"突发负载集中在单阶段"},
		},
		Stages: []workloadStage{
			{
				Name:        "spike_burst",
				Requests:    4096,
				Concurrency: 256,
				Pattern:     patternSpike,
			},
		},
	}
}

func stressRampScenario() performanceScenario {
	return performanceScenario{
		Name:        "stress_ramp",
		Category:    categoryStress,
		Description: "容量规划测试 - 逐步增加负载直到系统极限",
		Pattern:     patternRamp,
		Generator:   newDefaultGenerator("ramp"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			WaitForReady:     45 * time.Second,
			AspirationalTPS:  1000,
			Notes:            []string{"分阶段递增并发量"},
		},
		Stages: []workloadStage{
			{Name: "ramp_l1", Requests: 1024, Concurrency: 64, Pattern: patternRamp},
			{Name: "ramp_l2", Requests: 2048, Concurrency: 128, Pattern: patternRamp},
			{Name: "ramp_l3", Requests: 3072, Concurrency: 256, Pattern: patternRamp},
			{Name: "ramp_l4", Requests: 4096, Concurrency: 512, Pattern: patternRamp},
		},
	}
}

func stressDestructiveScenario() performanceScenario {
	return performanceScenario{
		Name:        "stress_overload",
		Category:    categoryStress,
		Description: "破坏性测试 - 超过系统最大处理能力",
		Pattern:     patternSpike,
		Generator:   newDefaultGenerator("overload"),
		Options: scenarioOptions{
			AllowDegradation: true,
			EnforceSLA:       false,
			SkipWaitForReady: true,
			WaitForReady:     60 * time.Second,
			SkipInShort:      true,
			Notes: []string{
				"预期观察降级策略和排队行为，允许出现失败",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "overload_burst",
				Requests:    8192,
				Concurrency: 512,
				Pattern:     patternSpike,
				DelayAfter:  3 * time.Second,
			},
		},
	}
}

func specializedDBPoolScenario() performanceScenario {
	return performanceScenario{
		Name:        "db_connection_pool",
		Category:    categorySpecialized,
		Description: "数据库连接池测试 - 验证连接池配置合理性",
		Pattern:     patternUniform,
		Generator:   newDefaultGenerator("dbpool"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			WaitForReady:     30 * time.Second,
			Notes: []string{
				"并发数接近连接池阈值，关注等待时间",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "pool_pressure",
				Requests:    2048,
				Concurrency: 96,
				Pattern:     patternUniform,
			},
		},
	}
}

func specializedCacheScenario() performanceScenario {
	return performanceScenario{
		Name:        "cache_hit_validation",
		Category:    categorySpecialized,
		Description: "缓存命中率测试 - 测量缓存对性能的影响",
		Pattern:     patternUniform,
		Generator:   newDefaultGenerator("cache"),
		Options: scenarioOptions{
			EnforceSLA:        true,
			SkipWaitForReady:  false,
			WaitForReady:      30 * time.Second,
			CacheChecks:       5,
			CacheHitThreshold: 50 * time.Millisecond,
			Notes: []string{
				"等待服务端写入后多次读取同一用户，验证缓存命中是否按预期发生",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "cache_probe",
				Requests:    1024,
				Concurrency: 48,
				Pattern:     patternUniform,
			},
		},
	}
}

func specializedExtremeDataScenario() performanceScenario {
	return performanceScenario{
		Name:        "extreme_payloads",
		Category:    categorySpecialized,
		Description: "极端数据测试 - 最小/最大长度、特殊字符、重复数据",
		Pattern:     patternUniform,
		Generator:   newExtremeGenerator("extreme"),
		Options: scenarioOptions{
			EnforceSLA:       true,
			SkipWaitForReady: true,
			WaitForReady:     30 * time.Second,
			Notes: []string{
				"数据分布覆盖最小/最大长度、特殊字符、重复手机号",
			},
		},
		Stages: []workloadStage{
			{
				Name:        "extreme_batch",
				Requests:    1024,
				Concurrency: 32,
				Pattern:     patternUniform,
			},
		},
	}
}

func newDefaultGenerator(scenario string) userGenerator {
	nameBuckets := []struct {
		label  string
		length int
	}{
		{label: "len_12", length: 12},
		{label: "len_28", length: 28},
		{label: "len_45", length: 45},
	}
	passwordBuckets := []struct {
		label  string
		format string
	}{
		{label: "simple", format: "SimplePass%02d!"},
		{label: "complex", format: "Compl3x!Pass#%02d"},
	}
	emailDomains := []string{"example.com", "corp.internal", "custom-domain.test"}
	phoneBuckets := []struct {
		label string
	}{{label: "domestic"}, {label: "international"}}
	seed := time.Now().UnixNano() + int64(scenarioPhoneSlot(scenario))*7919
	if seed < 0 {
		seed = -seed
	}
	nameSalt := encodeSequenceSuffix(int(seed % 46656))
	for len(nameSalt) < 3 {
		nameSalt = "0" + nameSalt
	}
	phoneSalt := uint64((seed / 37) % 1_000_000)
	mapper := newSequenceMapper(seed)

	return func(seq uint64) userVariant {
		idx := int(seq)
		nameInfo := nameBuckets[idx%len(nameBuckets)]
		passwordInfo := passwordBuckets[idx%len(passwordBuckets)]
		emailDomain := emailDomains[idx%len(emailDomains)]
		phoneInfo := phoneBuckets[idx%len(phoneBuckets)]

		userName := buildUsername(fmt.Sprintf("%s_%s", perfUsernamePrefix, scenario), nameInfo.length, idx, nameSalt, mapper)
		password := fmt.Sprintf(passwordInfo.format, idx%100)
		phone := buildPhone(scenario, phoneInfo.label, seq, phoneSalt)
		spec := framework.UserSpec{
			Name:     userName,
			Nickname: "性能测试用户",
			Password: password,
			Email:    fmt.Sprintf("%s@%s", userName, emailDomain),
			Phone:    phone,
			Status:   1,
			IsAdmin:  0,
		}
		variant := userVariant{
			Spec:           spec,
			NameBucket:     nameInfo.label,
			PasswordBucket: passwordInfo.label,
			EmailBucket:    emailDomain,
			PhoneBucket:    phoneInfo.label,
		}
		seqSuffix := encodeSequenceSuffix(idx)
		if idx%4 == 3 && len(spec.Name) > 5 {
			protect := len(seqSuffix)
			if protect > len(spec.Name) {
				protect = len(spec.Name)
			}
			spec.Name = injectSpecialRune(spec.Name, idx, protect)
			spec.Email = fmt.Sprintf("%s@%s", spec.Name, emailDomain)
			variant.Spec = spec
			variant.AdditionalTag = "special_char"
		}
		return variant
	}
}

func newExtremeGenerator(scenario string) userGenerator {
	lengths := []int{3, 45, 30, 10}
	specials := []rune{'-', '.', '_'}
	seed := time.Now().UnixNano() + int64(scenarioPhoneSlot(scenario))*7919
	if seed < 0 {
		seed = -seed
	}
	nameSalt := encodeSequenceSuffix(int(seed % 46656))
	for len(nameSalt) < 3 {
		nameSalt = "0" + nameSalt
	}
	phoneSalt := uint64((seed / 37) % 1_000_000)
	mapper := newSequenceMapper(seed)
	return func(seq uint64) userVariant {
		idx := int(seq)
		length := lengths[idx%len(lengths)]
		userName := buildUsername(fmt.Sprintf("%s_%s_ext", perfUsernamePrefix, scenario), length, idx, nameSalt, mapper)
		if length > 4 {
			userName = sprinkleSpecials(userName, specials[idx%len(specials)])
		}
		password := fmt.Sprintf("X%s@%02d", strings.Repeat("x", length%8+4), idx%100)
		emailDomain := []string{"max-length.domain", "custom-extreme.test"}[idx%2]
		phoneBucket := "extreme_domestic"
		if idx%2 == 1 {
			phoneBucket = "extreme_international"
		}
		phone := buildPhone(scenario, phoneBucket, seq, phoneSalt)
		spec := framework.UserSpec{
			Name:     userName,
			Nickname: "极端数据用户",
			Password: password,
			Email:    fmt.Sprintf("%s@%s", userName, emailDomain),
			Phone:    phone,
			Status:   1,
			IsAdmin:  0,
		}
		return userVariant{
			Spec:           spec,
			NameBucket:     fmt.Sprintf("len_%d", length),
			PasswordBucket: "extreme",
			EmailBucket:    emailDomain,
			PhoneBucket:    "extreme",
			AdditionalTag:  "extreme_case",
		}
	}
}

func buildUsername(prefix string, desired int, seq int, salt string, mapper *sequenceMapper) string {
	if desired < 3 {
		desired = 3
	}
	if desired > 45 {
		desired = 45
	}
	prefixClean := sanitizeAlphaNum(prefix)
	if prefixClean == "" {
		prefixClean = "user"
	}
	saltClean := sanitizeAlphaNum(salt)
	if saltClean == "" {
		saltClean = "s0"
	}
	prefixWithSalt := prefixClean + saltClean
	seqWithSalt := seq + saltSequenceOffset(saltClean)
	seqPart := encodeSequenceSuffix(seqWithSalt)
	seqPart = mapper.decorateString(seqPart)
	if desired <= len(seqPart) {
		suffix := seqPart[len(seqPart)-desired:]
		bytes := []byte(suffix)
		if len(bytes) == 0 {
			bytes = []byte{'u', '0', '1'}
		}
		if !isAlphaNum(bytes[0]) {
			bytes[0] = 'u'
		}
		if !isAlphaNum(bytes[len(bytes)-1]) {
			bytes[len(bytes)-1] = 'z'
		}
		return string(bytes)
	}
	remaining := desired - len(seqPart)
	var builder strings.Builder
	if remaining > 1 {
		required := remaining - 1 // account for separator
		prefixPart := prefixWithSalt
		if len(prefixPart) > required {
			prefixPart = prefixPart[:required]
		} else if len(prefixPart) < required {
			prefixPart += deterministicPadding(required-len(prefixPart), seq)
		}
		builder.WriteString(prefixPart)
		builder.WriteByte('_')
	} else {
		prefixPart := prefixWithSalt
		if len(prefixPart) > remaining {
			prefixPart = prefixPart[:remaining]
		} else if len(prefixPart) < remaining {
			prefixPart += deterministicPadding(remaining-len(prefixPart), seq)
		}
		builder.WriteString(prefixPart)
	}
	builder.WriteString(seqPart)
	name := builder.String()
	bytes := []byte(name)
	if len(bytes) == 0 {
		bytes = []byte{'u', '0', '1'}
	}
	if !isAlphaNum(bytes[0]) {
		bytes[0] = 'u'
	}
	if !isAlphaNum(bytes[len(bytes)-1]) {
		bytes[len(bytes)-1] = 'z'
	}
	return string(bytes)
}

func encodeSequenceSuffix(seq int) string {
	seqPart := strings.ToLower(strconv.FormatInt(int64(seq), 36))
	if len(seqPart) < 2 {
		seqPart = strings.Repeat("0", 2-len(seqPart)) + seqPart
	}
	return seqPart
}

func sanitizeAlphaNum(input string) string {
	var b strings.Builder
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if isAlphaNum(ch) || ch == '-' || ch == '_' || ch == '.' {
			b.WriteByte(ch)
			continue
		}
		b.WriteByte(alphaForIndex(i))
	}
	return strings.ToLower(b.String())
}

func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

func alphaForIndex(i int) byte {
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	return alphabet[i%len(alphabet)]
}

func deterministicPadding(length int, seed int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	var b strings.Builder
	for i := 0; i < length; i++ {
		idx := (seed + i) % len(alphabet)
		b.WriteByte(alphabet[idx])
	}
	return b.String()
}

func saltSequenceOffset(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % 46656)
}

type sequenceMapper struct {
	lookup [256]byte
}

func newSequenceMapper(seed int64) *sequenceMapper {
	alphabet := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	perm := make([]byte, len(alphabet))
	copy(perm, alphabet)
	r := rand.New(rand.NewSource(seed))
	for i := len(perm) - 1; i > 0; i-- {
		j := int(r.Int63n(int64(i + 1)))
		perm[i], perm[j] = perm[j], perm[i]
	}
	mapper := &sequenceMapper{}
	for i := 0; i < 256; i++ {
		mapper.lookup[i] = byte(i)
	}
	for idx, ch := range alphabet {
		mapper.lookup[ch] = perm[idx]
	}
	return mapper
}

func (m *sequenceMapper) decorateString(s string) string {
	if m == nil || s == "" {
		return s
	}
	bytes := []byte(s)
	m.apply(bytes)
	return string(bytes)
}

func (m *sequenceMapper) apply(bytes []byte) {
	if m == nil {
		return
	}
	for i := range bytes {
		bytes[i] = m.lookup[bytes[i]]
	}
}

func injectSpecialRune(name string, seed int, protectedSuffix int) string {
	if len(name) < 4 {
		return name
	}
	specials := []byte{'-', '_', '.'}
	limit := len(name) - protectedSuffix
	if limit <= 1 {
		return name
	}
	idx := (seed % (limit - 1)) + 1
	bytes := []byte(name)
	bytes[idx] = specials[seed%len(specials)]
	return string(bytes)
}

func sprinkleSpecials(name string, special rune) string {
	bytes := []rune(name)
	if len(bytes) > 4 {
		bytes[len(bytes)/2] = special
	}
	return string(bytes)
}

func buildPhone(scenario string, bucket string, seq uint64, salt uint64) string {
	slot := scenarioPhoneSlot(scenario)
	uniqueSeq := mixPhoneSequence(seq, salt, slot, bucket)
	prefix := derivePhonePrefix(slot, salt, bucket)
	return fmt.Sprintf("%03d%08d", prefix, uniqueSeq)
}

func mixPhoneSequence(seq, salt, slot uint64, bucket string) uint64 {
	base := seq + salt*104729 + slot*65537
	if strings.Contains(bucket, "international") {
		base += 50_000_000
	}
	return base % 100_000_000
}

func derivePhonePrefix(slot, salt uint64, bucket string) int {
	value := (slot + salt) % 70
	if strings.Contains(bucket, "international") {
		return 200 + int((slot+salt)%200)
	}
	return 130 + int(value)
}

func scenarioPhoneSlot(scenario string) uint64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(scenario))
	return uint64(h.Sum32() % 100)
}

func TestDefaultGeneratorUniqueness(t *testing.T) {
	t.Parallel()
	gen := newDefaultGenerator("parallel")
	seen := make(map[string]struct{})
	phones := make(map[string]struct{})
	for i := uint64(0); i < 512; i++ {
		variant := gen(i)
		if _, ok := seen[variant.Spec.Name]; ok {
			t.Fatalf("duplicate username %q for seq %d", variant.Spec.Name, i)
		}
		seen[variant.Spec.Name] = struct{}{}
		if _, ok := phones[variant.Spec.Phone]; ok {
			t.Fatalf("duplicate phone %q for seq %d", variant.Spec.Phone, i)
		}
		phones[variant.Spec.Phone] = struct{}{}
	}
}
func TestCreatePerformance(t *testing.T) {
	env := framework.NewEnv(t)                                // 1. 创建测试环境
	outputDir := env.EnsureOutputDir(t, perfOutputDir)        // 2. 准备输出目录
	recorder := framework.NewRecorder(t, outputDir, "create") // 3. 创建结果记录器
	defer recorder.Flush(t)

	scenarios := []performanceScenario{
		baselineSerialScenario(),
		baselineConcurrentScenario(),
		baselineSustainedScenario(),
		stressSpikeScenario(),
		stressRampScenario(),
		stressDestructiveScenario(),
		specializedDBPoolScenario(),
		specializedCacheScenario(),
		specializedExtremeDataScenario(),
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			if sc.Options.SkipInShort && testing.Short() {
				t.Skip("scenario skipped in short mode")
			}
			metrics := runPerformanceScenario(t, env, recorder, sc)
			if metrics.Requests == 0 {
				t.Fatalf("scenario %s produced no requests", sc.Name)
			}
			if !sc.Options.AllowDegradation && sc.Options.EnforceSLA && metrics.Success == 0 && metrics.Degraded == 0 {
				t.Fatalf("scenario %s had zero successful requests", sc.Name)
			}
		})
	}
}
