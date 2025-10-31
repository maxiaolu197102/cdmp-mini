package list

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

type perfScenario struct {
	name        string
	description string
	run         func(t *testing.T, env *framework.Env, data *listDataset, load []userRecord) (framework.PerformancePoint, error)
}

func TestListPerformance(t *testing.T) {
	env := framework.NewEnv(t)
	env.DisableClientRateLimiter()
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "list")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	data := newListDataset(t, env, basePassword)
	defer data.cleanupAll(env)

	loadPrefix := fmt.Sprintf("load%d", time.Now().UnixNano())
	loadUsers := data.createBatch(t, env, "list_perf_load_", 12, func(idx int, spec *framework.UserSpec) {
		spec.Email = fmt.Sprintf("%s-%02d@example.com", loadPrefix, idx)
	})

	scenarios := []perfScenario{
		{
			name:        "baseline_serial",
			description: "串行请求校验稳定性",
			run: func(t *testing.T, env *framework.Env, data *listDataset, _ []userRecord) (framework.PerformancePoint, error) {
				requests := 40
				durations := make([]time.Duration, 0, requests)
				codeCounts := make(map[int]int)
				success := 0
				failure := 0
				start := time.Now()
				for i := 0; i < requests; i++ {
					values := url.Values{}
					values.Set("name", data.Primary.Spec.Name)
					values.Set("limit", "1")
					reqStart := time.Now()
					users, resp, err := listUsersWithAdmin(t, env, values)
					elapsed := time.Since(reqStart)
					durations = append(durations, elapsed)
					if err != nil {
						failure++
						continue
					}
					codeCounts[resp.Code]++
					if resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Primary.Spec.Name {
						success++
					} else {
						failure++
					}
				}
				total := time.Since(start)
				point := buildPerformancePoint("baseline_serial", requests, success, failure, durations, total, codeCounts, nil)
				point.Notes = append(point.Notes, fmt.Sprintf("requests=%d", requests))
				point.Notes = append(point.Notes, "query=name")
				return point, nil
			},
		},
		{
			name:        "parallel_mixed_filters",
			description: "多过滤组合并发读取",
			run: func(t *testing.T, env *framework.Env, data *listDataset, _ []userRecord) (framework.PerformancePoint, error) {
				profiles := []struct {
					label    string
					values   func() url.Values
					validate func([]publicUser, *framework.APIResponse) bool
				}{
					{
						label: "by_name",
						values: func() url.Values {
							v := url.Values{}
							v.Set("name", data.Primary.Spec.Name)
							v.Set("limit", "1")
							return v
						},
						validate: func(users []publicUser, resp *framework.APIResponse) bool {
							return resp != nil && resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Primary.Spec.Name
						},
					},
					{
						label: "disabled_status",
						values: func() url.Values {
							v := url.Values{}
							v.Set("name", data.MultiDisabled.Spec.Name)
							v.Set("status", "0")
							return v
						},
						validate: func(users []publicUser, resp *framework.APIResponse) bool {
							return resp != nil && resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.MultiDisabled.Spec.Name
						},
					},
					{
						label: "email_like",
						values: func() url.Values {
							v := url.Values{}
							v.Set("email[like]", data.MultiEmailPrefix)
							v.Set("status", "0,1")
							return v
						},
						validate: func(users []publicUser, resp *framework.APIResponse) bool {
							return resp != nil && resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) >= 1
						},
					},
					{
						label: "phone_like",
						values: func() url.Values {
							v := url.Values{}
							v.Set("phone[like]", data.ContactPhonePrefix)
							v.Set("limit", "5")
							return v
						},
						validate: func(users []publicUser, resp *framework.APIResponse) bool {
							if resp == nil || resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
								return false
							}
							for _, u := range users {
								if u.Username == data.Contact.Spec.Name {
									return true
								}
							}
							return false
						},
					},
				}
				workers := 6
				iterations := 10
				totalRequests := workers * iterations
				durations := make([]time.Duration, 0, totalRequests)
				codeCounts := make(map[int]int)
				profileCounts := make(map[string]int)
				success := 0
				failure := 0
				start := time.Now()
				var mu sync.Mutex
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := 0; w < workers; w++ {
					worker := w
					go func() {
						defer wg.Done()
						for i := 0; i < iterations; i++ {
							profile := profiles[(worker*iterations+i)%len(profiles)]
							vals := profile.values()
							reqStart := time.Now()
							users, resp, err := listUsersWithAdmin(t, env, vals)
							elapsed := time.Since(reqStart)
							mu.Lock()
							durations = append(durations, elapsed)
							profileCounts["profile_"+profile.label]++
							if err != nil {
								failure++
							} else {
								codeCounts[resp.Code]++
								if profile.validate(users, resp) {
									success++
								} else {
									failure++
								}
							}
							mu.Unlock()
						}
					}()
				}
				wg.Wait()
				total := time.Since(start)
				point := buildPerformancePoint("parallel_mixed_filters", totalRequests, success, failure, durations, total, codeCounts, profileCounts)
				point.Notes = append(point.Notes, fmt.Sprintf("workers=%d iterations=%d", workers, iterations))
				return point, nil
			},
		},
		{
			name:        "pagination_window_scan",
			description: "多页扫描分页一致性",
			run: func(t *testing.T, env *framework.Env, data *listDataset, _ []userRecord) (framework.PerformancePoint, error) {
				ordered := make([]userRecord, len(data.Pagination))
				copy(ordered, data.Pagination)
				sort.Slice(ordered, func(i, j int) bool {
					return ordered[i].Snapshot.ID > ordered[j].Snapshot.ID
				})
				cycles := 4
				requests := cycles * len(ordered)
				durations := make([]time.Duration, 0, requests)
				codeCounts := make(map[int]int)
				success := 0
				failure := 0
				start := time.Now()
				for c := 0; c < cycles; c++ {
					for offset := range ordered {
						values := url.Values{}
						values.Set("status", "1")
						values.Set("email[like]", data.PaginationEmailPrefix)
						values.Set("limit", "1")
						values.Set("offset", strconv.Itoa(offset))
						reqStart := time.Now()
						users, resp, err := listUsersWithAdmin(t, env, values)
						elapsed := time.Since(reqStart)
						durations = append(durations, elapsed)
						if err != nil {
							failure++
							continue
						}
						codeCounts[resp.Code]++
						expected := ordered[offset].Spec.Name
						if resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == expected {
							success++
						} else {
							failure++
						}
					}
				}
				total := time.Since(start)
				extra := map[string]int{"cycles": cycles, "pages": len(ordered)}
				point := buildPerformancePoint("pagination_window_scan", requests, success, failure, durations, total, codeCounts, extra)
				point.Notes = append(point.Notes, fmt.Sprintf("pages=%d cycles=%d", len(ordered), cycles))
				return point, nil
			},
		},
		{
			name:        "invalid_parameter_resilience",
			description: "非法参数快速失败",
			run: func(t *testing.T, env *framework.Env, _ *listDataset, _ []userRecord) (framework.PerformancePoint, error) {
				invalids := []struct {
					label  string
					values url.Values
				}{
					{label: "status", values: url.Values{"status": []string{"abc"}}},
					{label: "time", values: url.Values{"createdAt[gte]": []string{"2024-13-01"}}},
					{label: "extend", values: url.Values{"extend..illegal": []string{"foo"}}},
					{label: "offset", values: url.Values{"offset": []string{"-1"}}},
				}
				repetitions := 3
				requests := repetitions * len(invalids)
				durations := make([]time.Duration, 0, requests)
				codeCounts := make(map[int]int)
				extra := make(map[string]int)
				success := 0
				failure := 0
				start := time.Now()
				for r := 0; r < repetitions; r++ {
					for _, inv := range invalids {
						reqStart := time.Now()
						resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+inv.values.Encode(), nil)
						elapsed := time.Since(reqStart)
						durations = append(durations, elapsed)
						extra["invalid_"+inv.label]++
						if err != nil {
							failure++
							continue
						}
						codeCounts[resp.Code]++
						if resp.HTTPStatus() == http.StatusBadRequest {
							success++
						} else {
							failure++
						}
					}
				}
				total := time.Since(start)
				point := buildPerformancePoint("invalid_parameter_resilience", requests, success, failure, durations, total, codeCounts, extra)
				point.Notes = append(point.Notes, fmt.Sprintf("variants=%d", len(invalids)))
				return point, nil
			},
		},
		{
			name:        "load_user_parallel",
			description: "批量用户并发读取",
			run: func(t *testing.T, env *framework.Env, data *listDataset, load []userRecord) (framework.PerformancePoint, error) {
				if len(load) == 0 {
					return framework.PerformancePoint{}, fmt.Errorf("no load users prepared")
				}
				workers := 8
				iterations := 8
				totalRequests := workers * iterations
				durations := make([]time.Duration, 0, totalRequests)
				codeCounts := make(map[int]int)
				success := 0
				failure := 0
				extra := map[string]int{"load_users": len(load)}
				var mu sync.Mutex
				start := time.Now()
				var wg sync.WaitGroup
				wg.Add(workers)
				for w := 0; w < workers; w++ {
					worker := w
					go func() {
						defer wg.Done()
						for i := 0; i < iterations; i++ {
							idx := (worker*iterations + i) % len(load)
							spec := load[idx]
							values := url.Values{}
							values.Set("name", spec.Spec.Name)
							values.Set("status", "0,1")
							values.Set("limit", "1")
							reqStart := time.Now()
							users, resp, err := listUsersWithAdmin(t, env, values)
							elapsed := time.Since(reqStart)
							mu.Lock()
							durations = append(durations, elapsed)
							if err != nil {
								failure++
							} else {
								codeCounts[resp.Code]++
								if resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == spec.Spec.Name {
									success++
								} else {
									failure++
								}
							}
							mu.Unlock()
						}
					}()
				}
				wg.Wait()
				total := time.Since(start)
				point := buildPerformancePoint("load_user_parallel", totalRequests, success, failure, durations, total, codeCounts, extra)
				point.Notes = append(point.Notes, fmt.Sprintf("workers=%d iterations=%d", workers, iterations))
				return point, nil
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			point, err := sc.run(t, env, data, loadUsers)
			if err != nil {
				t.Fatalf("performance scenario %s failed: %v", sc.name, err)
			}
			point.Scenario = sc.name
			point.Notes = append(point.Notes, sc.description)
			recorder.AddPerformance(point)
		})
	}
}

func buildPerformancePoint(name string, requests, success, failure int, samples []time.Duration, total time.Duration, codeCounts map[int]int, extraCounters map[string]int) framework.PerformancePoint {
	point := framework.PerformancePoint{
		Scenario:     name,
		Requests:     requests,
		SuccessRate:  ratioFloat(success, requests),
		ErrorRate:    ratioFloat(failure, requests),
		DurationMS:   total.Milliseconds(),
		QPS:          computeQPS(requests, total),
		ErrorCount:   failure,
		SuccessCount: success,
		Latency:      computeLatencyStats(samples),
	}
	counters := map[string]int{
		"success": success,
		"error":   failure,
	}
	for code, count := range codeCounts {
		counters[fmt.Sprintf("code_%d", code)] = count
	}
	for k, v := range extraCounters {
		counters[k] = v
	}
	point.Counters = counters
	return point
}

func computeLatencyStats(samples []time.Duration) framework.LatencyStats {
	if len(samples) == 0 {
		return framework.LatencyStats{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, s := range sorted {
		total += s
	}
	avg := total / time.Duration(len(sorted))
	return framework.LatencyStats{
		MinMS: toMillis(sorted[0]),
		MaxMS: toMillis(sorted[len(sorted)-1]),
		AvgMS: toMillis(avg),
		P50MS: percentile(sorted, 0.5),
		P90MS: percentile(sorted, 0.9),
		P95MS: percentile(sorted, 0.95),
		P99MS: percentile(sorted, 0.99),
	}
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return toMillis(sorted[0])
	}
	if p >= 1 {
		return toMillis(sorted[len(sorted)-1])
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return toMillis(sorted[idx])
}

func toMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func ratioFloat(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func computeQPS(requests int, duration time.Duration) float64 {
	if requests == 0 || duration <= 0 {
		return 0
	}
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(requests) / seconds
}
