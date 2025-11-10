package usercache

import (
	"encoding/json"
	"strings"
)

type pendingMarkerMetadata struct {
	Status       string `json:"status"`
	Degraded     bool   `json:"degraded"`
	Backpressure string `json:"backpressure"`
}

// PendingMarkerIsDegraded returns true when the pending marker payload indicates a degraded state.
func PendingMarkerIsDegraded(raw string) (bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false, nil
	}
	if trimmed[0] != '{' {
		if strings.EqualFold(trimmed, "degraded") {
			return true, nil
		}
		return false, nil
	}

	var marker pendingMarkerMetadata
	if err := json.Unmarshal([]byte(trimmed), &marker); err != nil {
		return false, err
	}
	status := strings.ToLower(strings.TrimSpace(marker.Status))
	if status == "degraded" || marker.Degraded {
		return true, nil
	}
	level := strings.ToLower(strings.TrimSpace(marker.Backpressure))
	if level != "" && level != "none" {
		return true, nil
	}
	return false, nil
}
