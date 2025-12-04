package usercache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

func newTestCoordinator(cfg PendingCoordinatorConfig) *PendingCoordinator {
	cfg.CalibrationInterval = 0
	coord := NewPendingCoordinator(&storage.RedisCluster{}, cfg)
	return coord
}

func TestPendingAcquireViaLuaSuccess(t *testing.T) {
	metrics.PendingLeaseLuaAttempts.Reset()
	cfg := PendingCoordinatorConfig{
		Component:                 "lua_success",
		LeaseTTL:                  2 * time.Second,
		BackpressureWindow:        time.Second,
		MetricsKey:                "user:pending:active:test",
		UserBackpressureWindow:    time.Second,
		UserBackpressureSoftLimit: 4,
		UserBackpressureHardLimit: 8,
	}
	coord := newTestCoordinator(cfg)
	defer coord.decInstanceActive()
	loadCalled := false
	coord.scriptLoader = func(ctx context.Context, script string) (string, error) {
		loadCalled = true
		if script != pendingAcquireLua {
			t.Fatalf("unexpected script body")
		}
		return "sha1", nil
	}
	var capturedArgs []interface{}
	var capturedKeys []string
	coord.luaExecutor = func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
		if sha != "sha1" {
			t.Fatalf("unexpected sha %s", sha)
		}
		capturedArgs = append([]interface{}{}, args...)
		capturedKeys = append([]string{}, keys...)
		return []interface{}{
			int64(1),
			int64(1),
			int64(0),
			"none",
			"none",
			"none",
			"{}",
		}, nil
	}
	username := "alice"
	key := PendingCreateKey(username)
	snapshot := pendingLeaseSnapshot{Status: "pending", State: string(PendingStateLease), OwnerID: "owner", Username: username}
	ctx := context.Background()
	outcome, err := coord.pendingAcquireViaLua(ctx, username, key, snapshot)
	if err != nil {
		t.Fatalf("pendingAcquireViaLua returned error: %v", err)
	}
	if !loadCalled {
		t.Fatalf("expected script loader to be called")
	}
	if outcome == nil || !outcome.created {
		t.Fatalf("expected created outcome")
	}
	if outcome.queueDepth != 1 {
		t.Fatalf("expected queue depth 1, got %d", outcome.queueDepth)
	}
	if len(capturedArgs) != 11 {
		t.Fatalf("unexpected arg length %d", len(capturedArgs))
	}
	if len(capturedKeys) != 3 {
		t.Fatalf("unexpected key length %d", len(capturedKeys))
	}
	for idx, key := range capturedKeys {
		if !strings.Contains(key, pendingHashTag) {
			t.Fatalf("expected key %d to include pending hash tag, got %s", idx, key)
		}
	}
	expectedTTL := cfg.LeaseTTL.Milliseconds()
	if ttl := capturedArgs[0]; ttl != expectedTTL {
		t.Fatalf("expected ttl %d, got %v", expectedTTL, ttl)
	}
	value := testutil.ToFloat64(metrics.PendingLeaseLuaAttempts.WithLabelValues(coord.component, "acquired"))
	if value != 1 {
		t.Fatalf("expected lua acquired metric 1, got %f", value)
	}
}

func TestPendingAcquireViaLuaReloadsAfterNoscript(t *testing.T) {
	metrics.PendingLeaseLuaAttempts.Reset()
	cfg := PendingCoordinatorConfig{
		Component:              "lua_reload",
		LeaseTTL:               1500 * time.Millisecond,
		BackpressureWindow:     time.Second,
		MetricsKey:             "user:pending:active:reload",
		UserBackpressureWindow: time.Second,
	}
	coord := newTestCoordinator(cfg)
	defer coord.decInstanceActive()
	loadCalls := 0
	coord.scriptLoader = func(ctx context.Context, script string) (string, error) {
		loadCalls++
		if loadCalls == 1 {
			return "sha1", nil
		}
		return "sha2", nil
	}
	evalCalls := 0
	coord.luaExecutor = func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
		evalCalls++
		if evalCalls == 1 {
			if sha != "sha1" {
				t.Fatalf("first eval expected sha1, got %s", sha)
			}
			return nil, errors.New("NOSCRIPT No matching script")
		}
		if sha != "sha2" {
			t.Fatalf("second eval expected sha2, got %s", sha)
		}
		return []interface{}{
			int64(1),
			int64(1),
			int64(1),
			"none",
			"none",
			"none",
			"{}",
		}, nil
	}
	snapshot := pendingLeaseSnapshot{Status: "pending", State: string(PendingStateLease), OwnerID: "owner", Username: "bob"}
	ctx := context.Background()
	outcome, err := coord.pendingAcquireViaLua(ctx, "bob", PendingCreateKey("bob"), snapshot)
	if err != nil {
		t.Fatalf("pendingAcquireViaLua returned error: %v", err)
	}
	if outcome == nil || !outcome.created {
		t.Fatalf("expected created outcome after reload")
	}
	if loadCalls != 2 {
		t.Fatalf("expected 2 script loads, got %d", loadCalls)
	}
	if evalCalls != 2 {
		t.Fatalf("expected 2 eval calls, got %d", evalCalls)
	}
	if sha, ok := coord.pendingAcquireScriptSHA.Load().(string); !ok || sha != "sha2" {
		t.Fatalf("expected cached sha2, got %v", coord.pendingAcquireScriptSHA.Load())
	}
	value := testutil.ToFloat64(metrics.PendingLeaseLuaAttempts.WithLabelValues(coord.component, "acquired"))
	if value != 1 {
		t.Fatalf("expected lua acquired metric 1, got %f", value)
	}
}

func TestPendingAcquireViaLuaUnavailableRecordsMetric(t *testing.T) {
	metrics.PendingLeaseLuaAttempts.Reset()
	cfg := PendingCoordinatorConfig{
		Component:              "lua_unavailable",
		LeaseTTL:               time.Second,
		BackpressureWindow:     time.Second,
		MetricsKey:             "user:pending:active:unavailable",
		UserBackpressureWindow: time.Second,
	}
	coord := newTestCoordinator(cfg)
	coord.scriptLoader = func(ctx context.Context, script string) (string, error) {
		return "sha1", nil
	}
	coord.luaExecutor = func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
		return nil, errAcquireLuaUnavailable
	}
	snapshot := pendingLeaseSnapshot{Status: "pending", State: string(PendingStateLease), OwnerID: "owner", Username: "eve"}
	ctx := context.Background()
	_, err := coord.pendingAcquireViaLua(ctx, "eve", PendingCreateKey("eve"), snapshot)
	if !errors.Is(err, errAcquireLuaUnavailable) {
		t.Fatalf("expected errAcquireLuaUnavailable, got %v", err)
	}
	value := testutil.ToFloat64(metrics.PendingLeaseLuaAttempts.WithLabelValues(coord.component, "unavailable"))
	if value != 1 {
		t.Fatalf("expected lua unavailable metric 1, got %f", value)
	}
}
