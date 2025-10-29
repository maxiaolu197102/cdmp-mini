package login

import (
	"encoding/json"
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

type loadScenario struct {
	Name          string
	Concurrency   int
	TotalRequests int
	Setup         func(t *testing.T, env *framework.Env) any
	Action        func(req loadRequest) loadOutcome
	TearDown      func(t *testing.T, env *framework.Env, state any)
}

type loadRequest struct {
	T        *testing.T
	Env      *framework.Env
	State    any
	WorkerID int
	Attempt  int
}

type loadOutcome struct {
	Success    bool
	HTTPStatus int
	Code       int
	Err        error
	Flags      map[string]bool
}

type loadReport struct {
	duration  time.Duration
	latencies []time.Duration
	success   int
	failure   int
	counters  map[string]int
}

type loginUserState struct {
	username string
	password string
}

type refreshSession struct {
	spec   framework.UserSpec
	tokens *framework.AuthTokens
}

type refreshState struct {
	sessions []refreshSession
}

type mixedSession struct {
	spec   framework.UserSpec
	tokens *framework.AuthTokens
}

type mixedState struct {
	sessions []mixedSession
}

const performanceOutputDir = "/home/mxl/cdmp-mini/test/iam-apiserver/user/login"

func TestLoginPerformance_Concurrent(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "login_concurrent")
	defer recorder.Flush(t)

	baseSpec := env.NewUserSpec("loginperf_", "InitPassw0rd!")
	env.CreateUserAndWait(t, baseSpec, 5*time.Second)
	defer env.ForceDeleteUserIgnore(baseSpec.Name)

	if resp, err := env.SetLoginRateLimit(100000); err == nil && resp != nil && resp.HTTPStatus() == http.StatusOK {
		defer env.ResetLoginRateLimit()
	}

	state := &loginUserState{username: baseSpec.Name, password: baseSpec.Password}
	levels := []int{50, 100, 200, 500}

	for _, level := range levels {
		scenarioName := fmt.Sprintf("login_concurrent_%d", level)
		report := runLoadScenario(t, env, loadScenario{
			Name:          scenarioName,
			Concurrency:   level,
			TotalRequests: level * 4,
			Setup: func(t *testing.T, env *framework.Env) any {
				return state
			},
			Action: loginSuccessAction,
		})
		recorder.AddPerformance(buildPerformancePoint(scenarioName, report))
	}
}

func TestLoginPerformance_Errors(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "login_errors")
	defer recorder.Flush(t)

	spec := env.NewUserSpec("loginerror_", "InitPassw0rd!")
	env.CreateUserAndWait(t, spec, 5*time.Second)
	defer env.ForceDeleteUserIgnore(spec.Name)

	if resp, err := env.SetLoginRateLimit(5); err == nil && resp != nil && resp.HTTPStatus() == http.StatusOK {
		defer env.ResetLoginRateLimit()
	}

	state := &loginUserState{username: spec.Name, password: spec.Password}
	scenario := loadScenario{
		Name:          "login_error_pressure",
		Concurrency:   100,
		TotalRequests: 1000,
		Setup: func(t *testing.T, env *framework.Env) any {
			return state
		},
		Action: errorPasswordAction,
	}

	report := runLoadScenario(t, env, scenario)
	recorder.AddPerformance(buildPerformancePoint(scenario.Name, report))
}

func TestLoginPerformance_TokenRefresh(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "login_refresh")
	defer recorder.Flush(t)

	const (
		concurrency       = 100
		requestsPerWorker = 10
	)

	report := runLoadScenario(t, env, loadScenario{
		Name:          "login_refresh_parallel",
		Concurrency:   concurrency,
		TotalRequests: concurrency * requestsPerWorker,
		Setup:         func(t *testing.T, env *framework.Env) any { return setupRefreshState(t, env, concurrency) },
		Action:        tokenRefreshAction,
		TearDown:      teardownRefreshState,
	})

	recorder.AddPerformance(buildPerformancePoint("login_refresh_parallel", report))
}

func TestLoginPerformance_MixedTraffic(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, performanceOutputDir)
	recorder := framework.NewRecorder(t, outputDir, "login_mixed")
	defer recorder.Flush(t)

	const (
		concurrency       = 100
		requestsPerWorker = 10
	)

	if resp, err := env.SetLoginRateLimit(90000); err == nil && resp != nil && resp.HTTPStatus() == http.StatusOK {
		defer env.ResetLoginRateLimit()
	}

	report := runLoadScenario(t, env, loadScenario{
		Name:          "login_mixed_traffic",
		Concurrency:   concurrency,
		TotalRequests: concurrency * requestsPerWorker,
		Setup:         func(t *testing.T, env *framework.Env) any { return setupMixedState(t, env, concurrency) },
		Action:        mixedTrafficAction,
		TearDown:      teardownMixedState,
	})

	recorder.AddPerformance(buildPerformancePoint("login_mixed_traffic", report))
}

func runLoadScenario(t *testing.T, env *framework.Env, cfg loadScenario) loadReport {
	t.Helper()

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.TotalRequests <= 0 {
		return loadReport{}
	}

	state := any(nil)
	if cfg.Setup != nil {
		state = cfg.Setup(t, env)
	}

	var (
		mu        sync.Mutex
		success   int
		failure   int
		latencies = make([]time.Duration, 0, cfg.TotalRequests)
		counters  = make(map[string]int)
	)

	jobs := make(chan int)
	var wg sync.WaitGroup

	start := time.Now()
	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for attempt := range jobs {
				req := loadRequest{T: t, Env: env, State: state, WorkerID: workerID, Attempt: attempt}
				attemptStart := time.Now()
				outcome := cfg.Action(req)
				elapsed := time.Since(attemptStart)

				mu.Lock()
				latencies = append(latencies, elapsed)
				if outcome.Success {
					success++
				} else {
					failure++
				}
				for k, v := range outcome.Flags {
					if v {
						counters[k]++
					}
				}
				mu.Unlock()
			}
		}(worker)
	}

	for i := 0; i < cfg.TotalRequests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	duration := time.Since(start)

	if cfg.TearDown != nil {
		cfg.TearDown(t, env, state)
	}

	return loadReport{
		duration:  duration,
		latencies: latencies,
		success:   success,
		failure:   failure,
		counters:  counters,
	}
}

func buildPerformancePoint(name string, report loadReport) framework.PerformancePoint {
	total := report.success + report.failure
	if total == 0 {
		return framework.PerformancePoint{Scenario: name}
	}

	duration := report.duration
	if duration <= 0 {
		duration = time.Millisecond
	}

	successRate := ratio(report.success, total)
	errorRate := ratio(report.failure, total)
	qps := float64(total) / duration.Seconds()

	latStats := computeLatencyStats(report.latencies)
	notes := summarizeCounters(report.counters)

	return framework.PerformancePoint{
		Scenario:     name,
		Requests:     total,
		SuccessRate:  successRate,
		ErrorRate:    errorRate,
		DurationMS:   duration.Milliseconds(),
		QPS:          qps,
		ErrorCount:   report.failure,
		SuccessCount: report.success,
		Latency:      latStats,
		Counters:     report.counters,
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

	min := sorted[0]
	max := sorted[len(sorted)-1]

	avg := float64(total) / float64(len(sorted))

	return framework.LatencyStats{
		MinMS: durationToMilliseconds(min),
		MaxMS: durationToMilliseconds(max),
		AvgMS: avg / float64(time.Millisecond),
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
	interpolation := position - float64(lower)
	delta := sorted[upper] - sorted[lower]
	return sorted[lower] + time.Duration(interpolation*float64(delta))
}

func durationToMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func summarizeCounters(counters map[string]int) []string {
	if len(counters) == 0 {
		return nil
	}
	keys := make([]string, 0, len(counters))
	for k := range counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	notes := make([]string, 0, len(keys))
	for _, k := range keys {
		notes = append(notes, fmt.Sprintf("%s=%d", k, counters[k]))
	}
	return notes
}

func ratio(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total)
}

func loginSuccessAction(req loadRequest) loadOutcome {
	state := req.State.(*loginUserState)
	payload := map[string]string{
		"username": state.username,
		"password": state.password,
	}
	resp, err := req.Env.AuthorizedRequest(http.MethodPost, "/login", "", payload)
	status, respCode := responseInfo(resp)
	success := err == nil && resp != nil && status == http.StatusOK && respCode == code.ErrSuccess

	flags := map[string]bool{
		"login_attempt": true,
	}
	if status == http.StatusTooManyRequests {
		flags["limit_triggered"] = true
	}
	if respCode == code.ErrAccountLocked {
		flags["account_locked"] = true
	}
	if success {
		flags["login_success"] = true
	} else {
		flags["login_failure"] = true
	}

	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func errorPasswordAction(req loadRequest) loadOutcome {
	state := req.State.(*loginUserState)
	payload := map[string]string{
		"username": state.username,
		"password": state.password + "_wrong",
	}
	resp, err := req.Env.AuthorizedRequest(http.MethodPost, "/login", "", payload)
	status, respCode := responseInfo(resp)

	passwordIncorrect := status == http.StatusUnauthorized && respCode == code.ErrPasswordIncorrect
	limitTriggered := status == http.StatusTooManyRequests
	accountLocked := respCode == code.ErrAccountLocked

	success := passwordIncorrect || limitTriggered || accountLocked
	flags := map[string]bool{
		"login_attempt":       true,
		"password_incorrect":  passwordIncorrect,
		"limit_triggered":     limitTriggered,
		"account_locked":      accountLocked,
		"login_failure":       !success,
		"expected_error_path": success,
	}

	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func setupRefreshState(t *testing.T, env *framework.Env, concurrency int) *refreshState {
	t.Helper()

	sessions := make([]refreshSession, concurrency)
	for i := 0; i < concurrency; i++ {
		spec := env.NewUserSpec("refreshperf_", "InitPassw0rd!")
		env.CreateUserAndWait(t, spec, 5*time.Second)
		tokens := env.LoginOrFail(t, spec.Name, spec.Password)
		sessions[i] = refreshSession{spec: spec, tokens: tokens}
	}
	return &refreshState{sessions: sessions}
}

func tokenRefreshAction(req loadRequest) loadOutcome {
	state := req.State.(*refreshState)
	session := &state.sessions[req.WorkerID%len(state.sessions)]

	resp, err := req.Env.Refresh(session.tokens.AccessToken, session.tokens.RefreshToken)
	status, respCode := responseInfo(resp)
	success := err == nil && resp != nil && status == http.StatusOK && respCode == code.ErrSuccess

	if success {
		updateTokensFromResponse(session.tokens, resp.Data)
	}

	flags := map[string]bool{
		"refresh_attempt": true,
	}
	if success {
		flags["refresh_success"] = true
	} else {
		flags["refresh_failure"] = true
		if status == http.StatusUnauthorized {
			flags["unauthorized"] = true
		}
	}

	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func teardownRefreshState(t *testing.T, env *framework.Env, state any) {
	t.Helper()
	refreshState, ok := state.(*refreshState)
	if !ok || refreshState == nil {
		return
	}
	for _, session := range refreshState.sessions {
		env.ForceDeleteUserIgnore(session.spec.Name)
	}
}

func setupMixedState(t *testing.T, env *framework.Env, concurrency int) *mixedState {
	t.Helper()
	sessions := make([]mixedSession, concurrency)
	for i := 0; i < concurrency; i++ {
		spec := env.NewUserSpec("mixedperf_", "InitPassw0rd!")
		env.CreateUserAndWait(t, spec, 5*time.Second)
		tokens := env.LoginOrFail(t, spec.Name, spec.Password)
		sessions[i] = mixedSession{spec: spec, tokens: tokens}
	}
	return &mixedState{sessions: sessions}
}

func mixedTrafficAction(req loadRequest) loadOutcome {
	state := req.State.(*mixedState)
	session := &state.sessions[req.WorkerID%len(state.sessions)]

	step := req.Attempt % 10
	switch step {
	case 0, 1, 2, 3:
		return performLoginAttempt(req, session)

	case 4:
		return performRefreshAttempt(req, session)

	case 5:
		return performLogoutAttempt(req, session)

	case 6, 8, 9:
		return performLoginAttempt(req, session)

	case 7:
		return performWrongPasswordAttempt(req, session)

	default:
		return performLoginAttempt(req, session)
	}
}

func performLoginAttempt(req loadRequest, session *mixedSession) loadOutcome {
	tokens, resp, err := req.Env.Login(session.spec.Name, session.spec.Password)
	status, respCode := responseInfo(resp)
	success := err == nil && resp != nil && status == http.StatusOK && respCode == code.ErrSuccess
	flags := map[string]bool{
		"login_attempt": true,
	}
	if success {
		session.tokens = tokens
		flags["login_success"] = true
	} else {
		flags["login_failure"] = true
		if status == http.StatusTooManyRequests {
			flags["limit_triggered"] = true
		}
		if respCode == code.ErrAccountLocked {
			flags["account_locked"] = true
		}
		if respCode == code.ErrPasswordIncorrect {
			flags["password_incorrect"] = true
		}
	}
	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func performWrongPasswordAttempt(req loadRequest, session *mixedSession) loadOutcome {
	payload := map[string]string{
		"username": session.spec.Name,
		"password": session.spec.Password + "_wrong",
	}
	resp, err := req.Env.AuthorizedRequest(http.MethodPost, "/login", "", payload)
	status, respCode := responseInfo(resp)
	passwordIncorrect := status == http.StatusUnauthorized && respCode == code.ErrPasswordIncorrect
	limitTriggered := status == http.StatusTooManyRequests
	accountLocked := respCode == code.ErrAccountLocked
	success := passwordIncorrect || limitTriggered || accountLocked
	flags := map[string]bool{
		"login_attempt":       true,
		"password_incorrect":  passwordIncorrect,
		"limit_triggered":     limitTriggered,
		"account_locked":      accountLocked,
		"expected_error_path": success,
	}
	if !success {
		flags["login_failure"] = true
	}
	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func performRefreshAttempt(req loadRequest, session *mixedSession) loadOutcome {
	if session.tokens == nil {
		flags := map[string]bool{
			"refresh_attempt": true,
			"refresh_failure": true,
			"missing_tokens":  true,
		}
		return loadOutcome{Success: false, Flags: flags}
	}
	resp, err := req.Env.Refresh(session.tokens.AccessToken, session.tokens.RefreshToken)
	status, respCode := responseInfo(resp)
	success := err == nil && resp != nil && status == http.StatusOK && respCode == code.ErrSuccess
	flags := map[string]bool{"refresh_attempt": true}
	if success {
		updateTokensFromResponse(session.tokens, resp.Data)
		flags["refresh_success"] = true
	} else {
		flags["refresh_failure"] = true
		if status == http.StatusUnauthorized {
			flags["unauthorized"] = true
		}
	}
	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func performLogoutAttempt(req loadRequest, session *mixedSession) loadOutcome {
	if session.tokens == nil {
		flags := map[string]bool{
			"logout_attempt": true,
			"logout_failure": true,
			"missing_tokens": true,
		}
		return loadOutcome{Success: false, Flags: flags}
	}
	resp, err := req.Env.Logout(session.tokens.AccessToken, session.tokens.RefreshToken)
	status, respCode := responseInfo(resp)
	success := err == nil && resp != nil && status == http.StatusOK && respCode == code.ErrSuccess
	flags := map[string]bool{"logout_attempt": true}
	if success {
		flags["logout_success"] = true
		session.tokens = nil
	} else {
		flags["logout_failure"] = true
		if status == http.StatusUnauthorized {
			flags["unauthorized"] = true
		}
	}
	return loadOutcome{Success: success, HTTPStatus: status, Code: respCode, Err: err, Flags: flags}
}

func teardownMixedState(t *testing.T, env *framework.Env, state any) {
	t.Helper()
	mixedState, ok := state.(*mixedState)
	if !ok || mixedState == nil {
		return
	}
	for _, session := range mixedState.sessions {
		env.ForceDeleteUserIgnore(session.spec.Name)
	}
}

func updateTokensFromResponse(tokens *framework.AuthTokens, data []byte) {
	if tokens == nil || len(data) == 0 {
		return
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	if payload.AccessToken != "" {
		tokens.AccessToken = payload.AccessToken
	}
	if payload.RefreshToken != "" {
		tokens.RefreshToken = payload.RefreshToken
	}
}

func responseInfo(resp *framework.APIResponse) (int, int) {
	if resp == nil {
		return 0, 0
	}
	return resp.HTTPStatus(), resp.Code
}
