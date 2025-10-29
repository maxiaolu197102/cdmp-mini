package framework

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

type CaseDiffStatus string

const (
	CaseAdded   CaseDiffStatus = "added"
	CaseRemoved CaseDiffStatus = "removed"
	CaseChanged CaseDiffStatus = "changed"
)

type CaseDiff struct {
	File     string         `json:"file"`
	Name     string         `json:"name"`
	Status   CaseDiffStatus `json:"status"`
	Baseline *CaseResult    `json:"baseline,omitempty"`
	Current  *CaseResult    `json:"current,omitempty"`
}

type PerformanceDiff struct {
	File             string            `json:"file"`
	Scenario         string            `json:"scenario"`
	Baseline         *PerformancePoint `json:"baseline,omitempty"`
	Current          *PerformancePoint `json:"current,omitempty"`
	DeltaSuccessRate float64           `json:"delta_success_rate"`
	DeltaQPS         float64           `json:"delta_qps"`
}

type ComparisonSummary struct {
	CaseDiffs        []CaseDiff        `json:"case_diffs,omitempty"`
	PerformanceDiffs []PerformanceDiff `json:"performance_diffs,omitempty"`
}

func (s *ComparisonSummary) IsEmpty() bool {
	return len(s.CaseDiffs) == 0 && len(s.PerformanceDiffs) == 0
}

func (s *ComparisonSummary) HasRegression() bool {
	for _, diff := range s.CaseDiffs {
		switch diff.Status {
		case CaseRemoved:
			return true
		case CaseChanged:
			if diff.Baseline != nil && diff.Baseline.Success && diff.Current != nil && !diff.Current.Success {
				return true
			}
		}
	}
	for _, diff := range s.PerformanceDiffs {
		if diff.DeltaSuccessRate < 0 || diff.DeltaQPS < 0 {
			return true
		}
	}
	return false
}

func (s *ComparisonSummary) PrintReport(w io.Writer) {
	if s.IsEmpty() {
		fmt.Fprintln(w, "✅ 没有检测到结果差异")
		return
	}

	if len(s.CaseDiffs) > 0 {
		fmt.Fprintln(w, "📋 用例结果差异：")
		for _, diff := range s.CaseDiffs {
			switch diff.Status {
			case CaseAdded:
				fmt.Fprintf(w, "  ➕ [%s] 新增用例 %s (HTTP=%d Code=%d)\n", diff.File, diff.Name, diff.Current.HTTPStatus, diff.Current.Code)
			case CaseRemoved:
				fmt.Fprintf(w, "  ➖ [%s] 缺失用例 %s (HTTP=%d Code=%d)\n", diff.File, diff.Name, diff.Baseline.HTTPStatus, diff.Baseline.Code)
			case CaseChanged:
				fmt.Fprintf(w, "  🔄 [%s] 用例 %s 发生变化: HTTP %d→%d, Code %d→%d, Success %v→%v\n",
					diff.File,
					diff.Name,
					diff.Baseline.HTTPStatus,
					diff.Current.HTTPStatus,
					diff.Baseline.Code,
					diff.Current.Code,
					diff.Baseline.Success,
					diff.Current.Success,
				)
			}
		}
	}

	if len(s.PerformanceDiffs) > 0 {
		fmt.Fprintln(w, "📈 性能指标差异：")
		for _, diff := range s.PerformanceDiffs {
			fmt.Fprintf(w, "  [%s] 场景 %s : ΔSuccessRate=%.2f%% ΔQPS=%.2f\n",
				diff.File,
				diff.Scenario,
				diff.DeltaSuccessRate*100,
				diff.DeltaQPS,
			)
		}
	}
}

type collectorResult struct {
	cases map[string]map[string]CaseResult
	perfs map[string]map[string]PerformancePoint
}

func CompareResults(baselineDir, currentDir string) (*ComparisonSummary, error) {
	baseline, err := collectResults(baselineDir)
	if err != nil {
		return nil, err
	}
	current, err := collectResults(currentDir)
	if err != nil {
		return nil, err
	}

	summary := &ComparisonSummary{}

	caseFiles := unionKeys(baseline.cases, current.cases)
	for _, file := range caseFiles {
		baseCases := baseline.cases[file]
		currCases := current.cases[file]
		caseNames := unionCaseKeys(baseCases, currCases)
		for _, name := range caseNames {
			baseCase, hasBase := baseCases[name]
			currCase, hasCurr := currCases[name]
			switch {
			case hasBase && !hasCurr:
				bc := baseCase
				summary.CaseDiffs = append(summary.CaseDiffs, CaseDiff{File: file, Name: name, Status: CaseRemoved, Baseline: &bc})
			case !hasBase && hasCurr:
				cc := currCase
				summary.CaseDiffs = append(summary.CaseDiffs, CaseDiff{File: file, Name: name, Status: CaseAdded, Current: &cc})
			case hasBase && hasCurr:
				if caseChanged(baseCase, currCase) {
					bc := baseCase
					cc := currCase
					summary.CaseDiffs = append(summary.CaseDiffs, CaseDiff{File: file, Name: name, Status: CaseChanged, Baseline: &bc, Current: &cc})
				}
			}
		}
	}

	perfFiles := unionKeys(baseline.perfs, current.perfs)
	for _, file := range perfFiles {
		basePerf := baseline.perfs[file]
		currPerf := current.perfs[file]
		scenarios := unionPerfKeys(basePerf, currPerf)
		for _, scenario := range scenarios {
			basePoint, hasBase := basePerf[scenario]
			currPoint, hasCurr := currPerf[scenario]
			switch {
			case hasBase && !hasCurr:
				bp := basePoint
				summary.PerformanceDiffs = append(summary.PerformanceDiffs, PerformanceDiff{
					File:     file,
					Scenario: scenario,
					Baseline: &bp,
				})
			case !hasBase && hasCurr:
				cp := currPoint
				summary.PerformanceDiffs = append(summary.PerformanceDiffs, PerformanceDiff{
					File:     file,
					Scenario: scenario,
					Current:  &cp,
				})
			case hasBase && hasCurr:
				deltaSuccess := currPoint.SuccessRate - basePoint.SuccessRate
				deltaQPS := currPoint.QPS - basePoint.QPS
				if !almostEqual(deltaSuccess, 0) || !almostEqual(deltaQPS, 0) {
					bp := basePoint
					cp := currPoint
					summary.PerformanceDiffs = append(summary.PerformanceDiffs, PerformanceDiff{
						File:             file,
						Scenario:         scenario,
						Baseline:         &bp,
						Current:          &cp,
						DeltaSuccessRate: deltaSuccess,
						DeltaQPS:         deltaQPS,
					})
				}
			}
		}
	}

	sort.SliceStable(summary.CaseDiffs, func(i, j int) bool {
		if summary.CaseDiffs[i].File == summary.CaseDiffs[j].File {
			return summary.CaseDiffs[i].Name < summary.CaseDiffs[j].Name
		}
		return summary.CaseDiffs[i].File < summary.CaseDiffs[j].File
	})
	sort.SliceStable(summary.PerformanceDiffs, func(i, j int) bool {
		if summary.PerformanceDiffs[i].File == summary.PerformanceDiffs[j].File {
			return summary.PerformanceDiffs[i].Scenario < summary.PerformanceDiffs[j].Scenario
		}
		return summary.PerformanceDiffs[i].File < summary.PerformanceDiffs[j].File
	})

	return summary, nil
}

func collectResults(root string) (*collectorResult, error) {
	result := &collectorResult{cases: map[string]map[string]CaseResult{}, perfs: map[string]map[string]PerformancePoint{}}
	if root == "" {
		return result, nil
	}
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return result, nil
	} else if err != nil {
		return nil, err
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.Base(path)
		rel = filepath.ToSlash(rel)
		switch {
		case strings.HasSuffix(name, "_cases.json"):
			entries, err := loadCaseResults(path)
			if err != nil {
				return fmt.Errorf("load case results %s: %w", path, err)
			}
			if _, ok := result.cases[rel]; !ok {
				result.cases[rel] = make(map[string]CaseResult)
			}
			for _, entry := range entries {
				result.cases[rel][entry.Name] = entry
			}
		case strings.HasSuffix(name, "_perf.json"):
			entries, err := loadPerfResults(path)
			if err != nil {
				return fmt.Errorf("load performance results %s: %w", path, err)
			}
			if _, ok := result.perfs[rel]; !ok {
				result.perfs[rel] = make(map[string]PerformancePoint)
			}
			for _, entry := range entries {
				result.perfs[rel][entry.Scenario] = entry
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func loadCaseResults(path string) ([]CaseResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results []CaseResult
	if err := jsoniter.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func loadPerfResults(path string) ([]PerformancePoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results []PerformancePoint
	if err := jsoniter.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func unionKeys[K comparable, V any](a, b map[K]V) []K {
	keys := make(map[K]struct{})
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	out := make([]K, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]) < fmt.Sprint(out[j])
	})
	return out
}

func unionCaseKeys(m1, m2 map[string]CaseResult) []string {
	if m1 == nil {
		m1 = map[string]CaseResult{}
	}
	if m2 == nil {
		m2 = map[string]CaseResult{}
	}
	return unionKeys(m1, m2)
}

func unionPerfKeys(m1, m2 map[string]PerformancePoint) []string {
	if m1 == nil {
		m1 = map[string]PerformancePoint{}
	}
	if m2 == nil {
		m2 = map[string]PerformancePoint{}
	}
	return unionKeys(m1, m2)
}

func caseChanged(base, current CaseResult) bool {
	if base.Success != current.Success {
		return true
	}
	if base.HTTPStatus != current.HTTPStatus || base.Code != current.Code {
		return true
	}
	if base.DurationMS != current.DurationMS {
		return true
	}
	if base.Message != current.Message {
		return true
	}
	return false
}

func almostEqual(a, b float64) bool {
	const eps = 1e-6
	return math.Abs(a-b) <= eps
}
