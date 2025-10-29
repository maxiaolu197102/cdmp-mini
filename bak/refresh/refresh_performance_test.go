package refresh

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func TestRefreshPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "refresh")
	defer recorder.Flush(t)

	const (
		workers    = 5
		iterations = 5
		basePass   = "InitPassw0rd!"
	)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		attempts  int
		successes int
	)

	recordSuccess := func() {
		mu.Lock()
		successes++
		mu.Unlock()
	}

	recordAttempt := func() {
		mu.Lock()
		attempts++
		mu.Unlock()
	}

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				recordAttempt()
				spec := env.NewUserSpec(fmt.Sprintf("refreshperf_%d_", worker), basePass)

				created := false
				resp, err := env.CreateUser(spec)
				if err != nil || resp.HTTPStatus() != http.StatusCreated {
					continue
				}
				created = true

				if err := env.WaitForUser(spec.Name, 5*time.Second); err != nil {
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				tokens, _, err := env.Login(spec.Name, basePass)
				if err != nil {
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				refreshResp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil || refreshResp.HTTPStatus() != http.StatusOK || refreshResp.Code != code.ErrSuccess {
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				var data struct {
					AccessToken  string `json:"access_token"`
					RefreshToken string `json:"refresh_token"`
				}
				if len(refreshResp.Data) > 0 {
					if err := json.Unmarshal(refreshResp.Data, &data); err != nil {
						if created {
							env.ForceDeleteUserIgnore(spec.Name)
						}
						continue
					}
				}
				if data.AccessToken == "" || data.RefreshToken == "" {
					if created {
						env.ForceDeleteUserIgnore(spec.Name)
					}
					continue
				}

				recordSuccess()
				env.Logout(tokens.AccessToken, tokens.RefreshToken)
				env.ForceDeleteUserIgnore(spec.Name)
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}

	mu.Lock()
	localAttempts := attempts
	localSuccesses := successes
	mu.Unlock()

	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "refresh_parallel",
		Requests:    localAttempts,
		SuccessRate: ratio(localSuccesses, localAttempts),
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(localAttempts), seconds),
		ErrorCount:  localAttempts - localSuccesses,
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
