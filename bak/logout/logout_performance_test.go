package logout

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

func TestLogoutPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "logout")
	defer recorder.Flush(t)

	const (
		workers    = 5
		iterations = 5
		basePass   = "InitPassw0rd!"
	)

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		successCount int
		failureCount int
	)

	record := func(ok bool) {
		mu.Lock()
		if ok {
			successCount++
		} else {
			failureCount++
		}
		mu.Unlock()
	}

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				spec := env.NewUserSpec(fmt.Sprintf("logoutperf_%d_", worker), basePass)

				created := false
				resp, err := env.CreateUser(spec)
				if err != nil || resp.HTTPStatus() != http.StatusCreated {
					record(false)
					continue
				}
				created = true

				if err := env.WaitForUser(spec.Name, 5*time.Second); err != nil {
					record(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				tokens, _, err := env.Login(spec.Name, basePass)
				if err != nil {
					record(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				logoutResp, err := env.Logout(tokens.AccessToken, tokens.RefreshToken)
				if err != nil || logoutResp.HTTPStatus() != http.StatusOK || logoutResp.Code != code.ErrSuccess {
					record(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				secondResp, err := env.Logout(tokens.AccessToken, tokens.RefreshToken)
				if err == nil && secondResp.HTTPStatus() == http.StatusOK && secondResp.Code == code.ErrSuccess {
					record(false)
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				record(true)
				env.ForceDeleteUserIgnore(spec.Name)
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	total := workers * iterations
	successRate := ratio(successCount, total)
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}

	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "logout_parallel",
		Requests:    total,
		SuccessRate: successRate,
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(total), seconds),
		ErrorCount:  failureCount,
	})
}

func ratio(success, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(success) / float64(total)
}

func ratioFloat(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}
