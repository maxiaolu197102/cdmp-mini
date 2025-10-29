package framework

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type CaseResult struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Success     bool            `json:"success"`
	HTTPStatus  int             `json:"http_status,omitempty"`
	Code        int             `json:"code,omitempty"`
	Message     string          `json:"message,omitempty"`
	Error       string          `json:"error,omitempty"`
	DurationMS  int64           `json:"duration_ms"`
	Checks      map[string]bool `json:"checks,omitempty"`
	Notes       []string        `json:"notes,omitempty"`
}

type LatencyStats struct {
	MinMS float64 `json:"min_ms"`
	MaxMS float64 `json:"max_ms"`
	AvgMS float64 `json:"avg_ms"`
	P50MS float64 `json:"p50_ms"`
	P90MS float64 `json:"p90_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
}

type PerformancePoint struct {
	Scenario     string         `json:"scenario"`
	Requests     int            `json:"requests"`
	SuccessRate  float64        `json:"success_rate"`
	ErrorRate    float64        `json:"error_rate"`
	DurationMS   int64          `json:"duration_ms"`
	QPS          float64        `json:"qps"`
	ErrorCount   int            `json:"error_count"`
	SuccessCount int            `json:"success_count,omitempty"`
	Latency      LatencyStats   `json:"latency"`
	Counters     map[string]int `json:"counters,omitempty"`
	Notes        []string       `json:"notes,omitempty"`
}

type Recorder struct {
	mu       sync.Mutex
	caseFile string
	perfFile string
	cases    []CaseResult
	perf     []PerformancePoint
}

func NewRecorder(t *testing.T, outputDir string, name string) *Recorder {
	t.Helper()
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("prepare output dir: %w", err))
	}
	caseFile := filepath.Join(outputDir, fmt.Sprintf("%s_cases.json", name))
	perfFile := filepath.Join(outputDir, fmt.Sprintf("%s_perf.json", name))
	return &Recorder{caseFile: caseFile, perfFile: perfFile}
}

func (r *Recorder) AddCase(result CaseResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cases = append(r.cases, result)
}

func (r *Recorder) AddPerformance(point PerformancePoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.perf = append(r.perf, point)
}

func (r *Recorder) Flush(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.cases) > 0 {
		if err := writeJSON(r.caseFile, r.cases); err != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("write case results: %w", err))
		}
	}
	if len(r.perf) > 0 {
		if err := writeJSON(r.perfFile, r.perf); err != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("write perf results: %w", err))
		}
	}
}

func writeJSON(path string, data any) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
