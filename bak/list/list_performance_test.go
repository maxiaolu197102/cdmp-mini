package list

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func TestListPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "list")
	defer recorder.Flush(t)

	const (
		workers    = 6
		iterations = 6
		basePass   = "InitPassw0rd!"
	)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		users   []framework.UserSpec
		success int
		failure int
	)

	for i := 0; i < workers; i++ {
		spec := env.NewUserSpec(fmt.Sprintf("listperf_%d_", i), basePass)
		env.CreateUserAndWait(t, spec, 5*time.Second)
		users = append(users, spec)
	}
	defer func() {
		for _, spec := range users {
			env.ForceDeleteUserIgnore(spec.Name)
		}
	}()

	record := func(ok bool) {
		mu.Lock()
		if ok {
			success++
		} else {
			failure++
		}
		mu.Unlock()
	}

	start := time.Now()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				idx := (worker*iterations + i) % len(users)
				spec := users[idx]

				query := url.Values{}
				query.Set("fieldSelector", "name="+spec.Name)
				query.Set("limit", "1")

				resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+query.Encode(), nil)
				if err != nil || resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
					record(false)
					continue
				}

				if len(resp.Data) > 0 {
					var items []publicUser
					if err := json.Unmarshal(resp.Data, &items); err != nil {
						record(false)
						continue
					}
					if len(items) == 0 || items[0].Username != spec.Name {
						record(false)
						continue
					}
				}

				record(true)
			}
		}(worker)
	}

	wg.Wait()
	duration := time.Since(start)
	total := workers * iterations
	successRate := ratio(success, total)
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}

	recorder.AddPerformance(framework.PerformancePoint{
		Scenario:    "list_parallel",
		Requests:    total,
		SuccessRate: successRate,
		DurationMS:  duration.Milliseconds(),
		QPS:         ratioFloat(float64(total), seconds),
		ErrorCount:  failure,
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
