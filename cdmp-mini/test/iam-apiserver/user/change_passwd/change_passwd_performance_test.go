package changepasswd

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

type perfScenario struct {
	name          string
	description   string
	workers       int
	iterations    int
	sessionFanout bool
	wrongOld      bool
	stagger       time.Duration
	userPrefix    string
}

func TestChangePasswordPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	env.DisableClientRateLimiter()
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "change_password")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	callChangePassword := func(token, username string, payload map[string]string) (*framework.APIResponse, error) {
		path := fmt.Sprintf("/v1/users/%s/change-password", username)
		body := map[string]string{}
		for k, v := range payload {
			body[k] = v
		}
		return env.AuthorizedRequest(http.MethodPut, path, token, body)
	}

	scenarios := []perfScenario{
		{
			name:        "baseline_serial",
			description: "单线程顺序修改密码，验证基本性能",
			workers:     1,
			iterations:  5,
			userPrefix:  "perf_bs",
		},
		{
			name:          "parallel_burst",
			description:   "8 并发线程快速改密，验证多会话下线",
			workers:       8,
			iterations:    3,
			sessionFanout: true,
			userPrefix:    "perf_pb",
		},
		{
			name:        "stress_wrong_old",
			description: "高并发错误旧密码应被快速拒绝",
			workers:     4,
			iterations:  3,
			wrongOld:    true,
			userPrefix:  "perf_sw",
		},
		{
			name:        "paced_endurance",
			description: "低并发持续改密，验证系统稳定性",
			workers:     2,
			iterations:  5,
			stagger:     100 * time.Millisecond,
			userPrefix:  "perf_pe",
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			point, err := runPerfScenario(env, callChangePassword, basePassword, scenario)
			if err != nil {
				t.Fatalf("scenario %s failed: %v", scenario.name, err)
			}
			recorder.AddPerformance(point)
		})
	}
}

func runPerfScenario(env *framework.Env, call func(string, string, map[string]string) (*framework.APIResponse, error), basePassword string, scenario perfScenario) (framework.PerformancePoint, error) {
	var (
		wg            sync.WaitGroup
		mu            sync.Mutex
		successCount  int
		errorCount    int
		durations     []time.Duration
		failureCounts = make(map[string]int)
		fatalErr      error
	)

	totalRequests := scenario.workers * scenario.iterations
	start := time.Now()

	for w := 0; w < scenario.workers; w++ {
		worker := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < scenario.iterations; i++ {
				mu.Lock()
				if fatalErr != nil {
					mu.Unlock()
					return
				}
				mu.Unlock()

				duration, ok, category, err := executePerfIteration(env, call, basePassword, scenario, worker, i)
				if err != nil {
					mu.Lock()
					if fatalErr == nil {
						fatalErr = err
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				if ok {
					successCount++
					durations = append(durations, duration)
				} else {
					errorCount++
					if category == "" {
						category = "unexpected_failure"
					}
					failureCounts[category]++
				}
				mu.Unlock()

				if scenario.stagger > 0 {
					time.Sleep(scenario.stagger)
				}
			}
		}()
	}

	wg.Wait()

	if fatalErr != nil {
		return framework.PerformancePoint{}, fatalErr
	}

	totalDuration := time.Since(start)
	latency := computeLatencyStats(durations)
	qps := 0.0
	if totalDuration > 0 {
		qps = float64(totalRequests) / totalDuration.Seconds()
	}

	point := framework.PerformancePoint{
		Scenario:     scenario.name,
		Requests:     totalRequests,
		SuccessRate:  ratio(successCount, totalRequests),
		ErrorRate:    ratio(errorCount, totalRequests),
		DurationMS:   totalDuration.Milliseconds(),
		QPS:          qps,
		ErrorCount:   errorCount,
		SuccessCount: successCount,
		Latency:      latency,
		Counters:     failureCounts,
		Notes:        []string{scenario.description},
	}

	return point, nil
}

func waitUntil(timeout, interval time.Duration, fn func() (bool, error)) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not satisfied within %s", timeout)
		}
		time.Sleep(interval)
	}
}

func waitUnauthorizedGetUser(env *framework.Env, token, username string, timeout time.Duration) error {
	return waitUntil(timeout, 150*time.Millisecond, func() (bool, error) {
		resp, err := env.GetUser(token, username)
		if err != nil {
			return false, err
		}
		return resp.HTTPStatus() != http.StatusOK, nil
	})
}

func waitRefreshRejected(env *framework.Env, accessToken, refreshToken string, timeout time.Duration) error {
	return waitUntil(timeout, 150*time.Millisecond, func() (bool, error) {
		resp, err := env.Refresh(accessToken, refreshToken)
		if err != nil {
			return false, err
		}
		return resp.HTTPStatus() != http.StatusOK, nil
	})
}

func executePerfIteration(env *framework.Env, call func(string, string, map[string]string) (*framework.APIResponse, error), basePassword string, scenario perfScenario, worker, iteration int) (time.Duration, bool, string, error) {
	username := perfUserPrefix(scenario.userPrefix, scenario.name, worker, iteration)
	spec := env.NewUserSpec(username, basePassword)
	resp, err := env.CreateUser(spec)
	if err != nil {
		return 0, false, "create_user_error", fmt.Errorf("create user %s: %w", spec.Name, err)
	}
	if resp == nil || resp.HTTPStatus() != http.StatusCreated {
		status := 0
		code := 0
		if resp != nil {
			status = resp.HTTPStatus()
			code = resp.Code
		}
		return 0, false, fmt.Sprintf("create_http_%d_code_%d", status, code), nil
	}
	defer env.ForceDeleteUserIgnore(spec.Name)

	// 创建链路存在异步缓存刷新，这里放宽等待时间避免误判失败
	if err := env.WaitForUser(spec.Name, 20*time.Second); err != nil {
		return 0, false, "wait_user_failed", fmt.Errorf("wait for user %s: %w", spec.Name, err)
	}

	primaryTokens, _, err := env.Login(spec.Name, basePassword)
	if err != nil {
		return 0, false, "login_initial_failed", fmt.Errorf("login user %s: %w", spec.Name, err)
	}

	var secondaryTokens *framework.AuthTokens
	if scenario.sessionFanout {
		secondaryTokens, _, err = env.Login(spec.Name, basePassword)
		if err != nil {
			return 0, false, "secondary_login_failed", fmt.Errorf("secondary session login %s: %w", spec.Name, err)
		}
	}

	newPassword := fmt.Sprintf("Perf%06d!", time.Now().UnixNano()%1000000)
	oldPassword := basePassword
	if scenario.wrongOld {
		oldPassword = "WrongPass@123"
	}

	start := time.Now()
	changeResp, err := call(primaryTokens.AccessToken, spec.Name, map[string]string{
		"oldPassword": oldPassword,
		"newPassword": newPassword,
	})
	duration := time.Since(start)
	if err != nil {
		return duration, false, "change_request_error", fmt.Errorf("change password request: %w", err)
	}
	if changeResp == nil {
		return duration, false, "change_no_response", nil
	}

	if scenario.wrongOld {
		if changeResp.HTTPStatus() != http.StatusUnauthorized || changeResp.Code != code.ErrPasswordIncorrect {
			return duration, false, fmt.Sprintf("unexpected_http_%d_code_%d", changeResp.HTTPStatus(), changeResp.Code), nil
		}
		if _, _, err := env.Login(spec.Name, basePassword); err != nil {
			return duration, false, "post_check_login_failed", fmt.Errorf("login original password after failure: %w", err)
		}
		return duration, true, "", nil
	}

	if changeResp.HTTPStatus() != http.StatusOK || changeResp.Code != code.ErrSuccess {
		return duration, false, fmt.Sprintf("unexpected_http_%d_code_%d", changeResp.HTTPStatus(), changeResp.Code), nil
	}

	if err := waitUnauthorizedGetUser(env, primaryTokens.AccessToken, spec.Name, 5*time.Second); err != nil {
		return duration, false, "primary_token_not_revoked", fmt.Errorf("wait primary token revocation: %w", err)
	}

	if err := waitRefreshRejected(env, primaryTokens.AccessToken, primaryTokens.RefreshToken, 5*time.Second); err != nil {
		return duration, false, "primary_refresh_not_revoked", fmt.Errorf("wait primary refresh revocation: %w", err)
	}

	if _, _, err := env.Login(spec.Name, newPassword); err != nil {
		return duration, false, "login_new_failed", fmt.Errorf("login new password: %w", err)
	}

	if _, oldResp, err := env.Login(spec.Name, basePassword); err == nil {
		return duration, false, "old_password_still_valid", nil
	} else if oldResp != nil && oldResp.Code != code.ErrPasswordIncorrect {
		return duration, false, fmt.Sprintf("old_login_code_%d", oldResp.Code), nil
	}

	if scenario.sessionFanout && secondaryTokens != nil {
		if err := waitUnauthorizedGetUser(env, secondaryTokens.AccessToken, spec.Name, 5*time.Second); err != nil {
			return duration, false, "secondary_token_not_revoked", fmt.Errorf("wait secondary token revocation: %w", err)
		}
		if err := waitRefreshRejected(env, secondaryTokens.AccessToken, secondaryTokens.RefreshToken, 5*time.Second); err != nil {
			return duration, false, "secondary_refresh_not_revoked", fmt.Errorf("wait secondary refresh revocation: %w", err)
		}
	}

	return duration, true, "", nil
}

const maxUsernamePrefixLen = 20

func perfUserPrefix(custom, fallback string, worker, iteration int) string {
	base := custom
	if base == "" {
		base = fallback
	}
	suffix := fmt.Sprintf("_w%d_i%d_", worker, iteration)
	maxBaseLen := maxUsernamePrefixLen - len(suffix)
	if maxBaseLen < 0 {
		maxBaseLen = 0
	}
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
	}
	if base == "" {
		base = "perf"
	}
	return base + suffix
}

func ratio(count, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(count) / float64(total)
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
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	index := p * float64(len(values)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	if lower == upper {
		return values[lower]
	}
	frac := index - float64(lower)
	return values[lower] + (values[upper]-values[lower])*frac
}
