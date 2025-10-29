package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type snapshot struct {
	Enabled      bool    `json:"enabled"`
	StartingRate float64 `json:"starting_rate"`
	MinRate      float64 `json:"min_rate"`
	MaxRate      float64 `json:"max_rate"`
	AdjustPeriod string  `json:"adjust_period"`
	StatsSource  string  `json:"stats_source,omitempty"`
}

type snapshotResult struct {
	File     string
	Snapshot snapshot
}

func main() {
	root := flag.String("root", "test/iam-apiserver/user", "root directory to scan for rate_limiter_snapshot.json")
	strict := flag.Bool("strict", true, "fail when snapshots differ or contain invalid values")
	flag.Parse()

	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[ratelimitcheck] IAM_APISERVER_E2E not set, skipping verification")
		return
	}

	results, err := collectSnapshots(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ratelimitcheck] %v\n", err)
		os.Exit(1)
	}
	if len(results) == 0 {
		fmt.Fprintf(os.Stderr, "[ratelimitcheck] no rate_limiter_snapshot.json files found under %s\n", *root)
		if *strict {
			os.Exit(1)
		}
		return
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].File < results[j].File
	})

	fmt.Println("[ratelimitcheck] discovered snapshots:")
	for _, r := range results {
		fmt.Printf("  - %s\n", r.File)
	}

	var (
		baseline    = results[0].Snapshot
		mismatches  []string
		validations []string
	)

	if err := validateSnapshot(baseline); err != nil {
		validations = append(validations, fmt.Sprintf("%s: %v", results[0].File, err))
	}

	for i := 1; i < len(results); i++ {
		other := results[i]
		if err := validateSnapshot(other.Snapshot); err != nil {
			validations = append(validations, fmt.Sprintf("%s: %v", other.File, err))
		}
		if !reflect.DeepEqual(baseline, other.Snapshot) {
			mismatches = append(mismatches, diffSnapshot(baseline, other))
		}
	}

	printSnapshotSummary(baseline)

	if len(validations) > 0 {
		fmt.Println("[ratelimitcheck] validation issues:")
		for _, v := range validations {
			fmt.Printf("  * %s\n", v)
		}
	}

	if len(mismatches) > 0 {
		fmt.Println("[ratelimitcheck] mismatch detected:")
		for _, m := range mismatches {
			fmt.Printf("  * %s\n", m)
		}
	}

	if (*strict && (len(mismatches) > 0 || len(validations) > 0)) || (len(mismatches) > 0 && len(validations) > 0) {
		os.Exit(1)
	}
}

func collectSnapshots(root string) ([]snapshotResult, error) {
	var results []snapshotResult
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.EqualFold(d.Name(), "rate_limiter_snapshot.json") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var snap snapshot
		if err := json.Unmarshal(content, &snap); err != nil {
			return fmt.Errorf("decode %s: %w", path, err)
		}
		results = append(results, snapshotResult{File: path, Snapshot: snap})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func validateSnapshot(s snapshot) error {
	if !s.Enabled {
		// Disabled limiter is acceptable but nothing to validate
		return nil
	}
	if s.StartingRate <= 0 {
		return errors.New("starting_rate must be positive")
	}
	if s.MinRate <= 0 {
		return errors.New("min_rate must be positive")
	}
	if s.MaxRate <= 0 {
		return errors.New("max_rate must be positive")
	}
	if s.MinRate > s.StartingRate {
		return fmt.Errorf("min_rate %.2f greater than starting_rate %.2f", s.MinRate, s.StartingRate)
	}
	if s.MaxRate < s.StartingRate {
		return fmt.Errorf("max_rate %.2f lower than starting_rate %.2f", s.MaxRate, s.StartingRate)
	}
	if strings.TrimSpace(s.AdjustPeriod) == "" {
		return errors.New("adjust_period must not be empty")
	}
	return nil
}

func diffSnapshot(baseline snapshot, other snapshotResult) string {
	var diffs []string
	if baseline.Enabled != other.Snapshot.Enabled {
		diffs = append(diffs, fmt.Sprintf("enabled %v -> %v", baseline.Enabled, other.Snapshot.Enabled))
	}
	if baseline.StartingRate != other.Snapshot.StartingRate {
		diffs = append(diffs, fmt.Sprintf("starting_rate %.2f -> %.2f", baseline.StartingRate, other.Snapshot.StartingRate))
	}
	if baseline.MinRate != other.Snapshot.MinRate {
		diffs = append(diffs, fmt.Sprintf("min_rate %.2f -> %.2f", baseline.MinRate, other.Snapshot.MinRate))
	}
	if baseline.MaxRate != other.Snapshot.MaxRate {
		diffs = append(diffs, fmt.Sprintf("max_rate %.2f -> %.2f", baseline.MaxRate, other.Snapshot.MaxRate))
	}
	if baseline.AdjustPeriod != other.Snapshot.AdjustPeriod {
		diffs = append(diffs, fmt.Sprintf("adjust_period %s -> %s", baseline.AdjustPeriod, other.Snapshot.AdjustPeriod))
	}
	if baseline.StatsSource != other.Snapshot.StatsSource {
		diffs = append(diffs, fmt.Sprintf("stats_source %q -> %q", baseline.StatsSource, other.Snapshot.StatsSource))
	}
	if len(diffs) == 0 {
		return fmt.Sprintf("%s differs but no fields detected", other.File)
	}
	return fmt.Sprintf("%s: %s", other.File, strings.Join(diffs, ", "))
}

func printSnapshotSummary(s snapshot) {
	fmt.Println("[ratelimitcheck] baseline snapshot:")
	fmt.Printf("  enabled      : %v\n", s.Enabled)
	fmt.Printf("  starting_rate: %.2f\n", s.StartingRate)
	fmt.Printf("  min_rate     : %.2f\n", s.MinRate)
	fmt.Printf("  max_rate     : %.2f\n", s.MaxRate)
	fmt.Printf("  adjust_period: %s\n", s.AdjustPeriod)
	if s.StatsSource != "" {
		fmt.Printf("  stats_source : %s\n", s.StatsSource)
	}
}
