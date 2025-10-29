package jwt

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func TestJWTPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "jwt")
	defer recorder.Flush(t)

	const (
		workers    = 5
		iterations = 5
		basePass   = "InitPassw0rd!"
	)

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		successFlows    int
		failureFlows    int
		refreshAttempts int
		refreshSuccess  int
		logoutAttempts  int
		logoutSuccess   int
	)

	recordFlow := func(ok bool) {
		mu.Lock()
		if ok {
			successFlows++
		} else {
			failureFlows++
		}
		mu.Unlock()
	}

	increment := func(target *int) {
		mu.Lock()
		*target++
		mu.Unlock()
	}

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				spec := env.NewUserSpec(fmt.Sprintf("jwtperf_%d_", worker), basePass)

				created := false

				resp, err := env.CreateUser(spec)
				if err != nil || resp.HTTPStatus() != http.StatusCreated {
					recordFlow(false)
					continue
				}
				created = true

				if err := env.WaitForUser(spec.Name, 5*time.Second); err != nil {
					recordFlow(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				tokens, _, err := env.Login(spec.Name, basePass)
				if err != nil {
					recordFlow(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				increment(&refreshAttempts)
				refreshResp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil || refreshResp.HTTPStatus() != http.StatusOK || refreshResp.Code != code.ErrSuccess {
					recordFlow(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}
				increment(&refreshSuccess)

				increment(&logoutAttempts)
				logoutResp, err := env.Logout(tokens.AccessToken, tokens.RefreshToken)
				if err != nil || logoutResp.HTTPStatus() != http.StatusOK || logoutResp.Code != code.ErrSuccess {
					recordFlow(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}
				increment(&logoutSuccess)

				recordFlow(true)
				env.ForceDeleteUserIgnore(spec.Name)
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	mu.Lock()
	localFlowSuccess := successFlows
	localFlowFailure := failureFlows
	localRefreshAttempts := refreshAttempts
	localRefreshSuccess := refreshSuccess
	localLogoutAttempts := logoutAttempts
	localLogoutSuccess := logoutSuccess
	mu.Unlock()

	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}

	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "jwt_login_refresh_parallel",
		Requests:    localRefreshAttempts,
		SuccessRate: ratio(localRefreshSuccess, localRefreshAttempts),
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(localRefreshAttempts), seconds),
		ErrorCount:  localRefreshAttempts - localRefreshSuccess,
	})

	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "jwt_logout_parallel",
		Requests:    localLogoutAttempts,
		SuccessRate: ratio(localLogoutSuccess, localLogoutAttempts),
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(localLogoutAttempts), seconds),
		ErrorCount:  localLogoutAttempts - localLogoutSuccess,
	})

	localFlowAttempts := localFlowSuccess + localFlowFailure
	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "jwt_full_flow",
		Requests:    localFlowAttempts,
		SuccessRate: ratio(localFlowSuccess, localFlowAttempts),
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(localFlowAttempts), seconds),
		ErrorCount:  localFlowFailure,
	})
}

func ratio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func ratioFloat(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}
