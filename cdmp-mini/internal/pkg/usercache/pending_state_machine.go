package usercache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

type PendingStateValue string

const (
	PendingStateUnknown  PendingStateValue = "unknown"
	PendingStateLease    PendingStateValue = "lease"
	PendingStateReleased PendingStateValue = "released"
	PendingStateExpired  PendingStateValue = "expired"
)

type BackpressureLevel string

const (
	BackpressureNone     BackpressureLevel = "none"
	BackpressureElevated BackpressureLevel = "elevated"
	BackpressureSevere   BackpressureLevel = "severe"
)

type LeaseMetadata struct {
	Username        string
	RequestID       string
	Operator        string
	ClientIP        string
	LegacyRequestID string
}

type PendingCoordinatorConfig struct {
	LeaseTTL              time.Duration
	ObserveTimeout        time.Duration
	BackpressureWindow    time.Duration
	BackpressureSoftLimit int
	BackpressureHardLimit int
	MetricsKey            string
}

type PendingCoordinator struct {
	redis *storage.RedisCluster
	cfg   PendingCoordinatorConfig
}

type pendingLeaseSnapshot struct {
	Status          string `json:"status"`
	Degraded        bool   `json:"degraded,omitempty"`
	State           string `json:"state"`
	OwnerID         string `json:"owner_id"`
	Version         int64  `json:"version"`
	LeaseExpiresAt  string `json:"lease_expires_at"`
	AcquireAt       string `json:"acquire_at"`
	UpdatedAt       string `json:"updated_at"`
	QueueDepth      int64  `json:"queue_depth,omitempty"`
	Backpressure    string `json:"backpressure,omitempty"`
	Username        string `json:"username,omitempty"`
	RequestID       string `json:"request_id,omitempty"`
	Operator        string `json:"operator,omitempty"`
	ClientIP        string `json:"client_ip,omitempty"`
	LegacyRequestID string `json:"legacy_request_id,omitempty"`
}

type PendingLease struct {
	Username       string
	OwnerID        string
	Version        int64
	AcquireAt      time.Time
	LeaseExpiresAt time.Time
	QueueDepth     int64
	Backpressure   BackpressureLevel
	Metadata       LeaseMetadata
}

type AcquireResult struct {
	Lease         *PendingLease
	SetNXDuration time.Duration
}

type PendingState struct {
	Exists           bool
	State            PendingStateValue
	LeaseOwner       string
	Version          int64
	TTL              time.Duration
	LeaseExpiresAt   time.Time
	Backpressure     BackpressureLevel
	QueueDepth       int64
	Raw              string
	RedisGetDuration time.Duration
	RedisTTLDur      time.Duration
}

type AcquireFailureReason string

const (
	AcquireFailureConflict     AcquireFailureReason = "conflict"
	AcquireFailureBackpressure AcquireFailureReason = "backpressure"
)

type AcquireError struct {
	Reason     AcquireFailureReason
	State      *PendingState
	QueueDepth int64
}

var (
	errLeaseConflict = errors.New("pending lease conflict")
	errBackpressure  = errors.New("pending backpressure triggered")
)

var (
	ErrPendingLeaseConflict = errLeaseConflict
	ErrPendingBackpressure  = errBackpressure
)

func (e *AcquireError) Error() string {
	if e == nil {
		return "pending lease error"
	}
	switch e.Reason {
	case AcquireFailureBackpressure:
		return fmt.Sprintf("pending lease rejected by backpressure (depth=%d)", e.QueueDepth)
	case AcquireFailureConflict:
		return "pending lease already active"
	default:
		return "pending lease error"
	}
}

func (e *AcquireError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Reason {
	case AcquireFailureConflict:
		return errLeaseConflict
	case AcquireFailureBackpressure:
		return errBackpressure
	default:
		return nil
	}
}

func (e *AcquireError) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	if target == errLeaseConflict || target == ErrPendingLeaseConflict {
		return e.Reason == AcquireFailureConflict
	}
	if target == errBackpressure || target == ErrPendingBackpressure {
		return e.Reason == AcquireFailureBackpressure
	}
	return false
}

func NewPendingCoordinator(redis *storage.RedisCluster, cfg PendingCoordinatorConfig) *PendingCoordinator {
	if redis == nil {
		return nil
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Minute
	}
	if cfg.ObserveTimeout <= 0 {
		cfg.ObserveTimeout = 700 * time.Millisecond
	}
	if cfg.BackpressureWindow <= 0 {
		cfg.BackpressureWindow = 5 * time.Second
	}
	if cfg.BackpressureSoftLimit <= 0 {
		cfg.BackpressureSoftLimit = 1000
	}
	if cfg.BackpressureHardLimit <= 0 {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit + 500
	}
	if cfg.BackpressureHardLimit < cfg.BackpressureSoftLimit {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit
	}
	if cfg.MetricsKey == "" {
		cfg.MetricsKey = "user:pending:active"
	}

	applyCoordinatorEnvOverrides(&cfg)

	return &PendingCoordinator{redis: redis, cfg: cfg}
}

func applyCoordinatorEnvOverrides(cfg *PendingCoordinatorConfig) {
	if cfg == nil {
		return
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_LEASE_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.LeaseTTL = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_BACKPRESSURE_WINDOW")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.BackpressureWindow = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_SOFT_LIMIT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			cfg.BackpressureSoftLimit = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_HARD_LIMIT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			cfg.BackpressureHardLimit = parsed
		}
	}
	if cfg.BackpressureHardLimit < cfg.BackpressureSoftLimit {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit
	}
}

func (c *PendingCoordinator) Acquire(ctx context.Context, username string, meta LeaseMetadata) (*AcquireResult, error) {
	if c == nil || c.redis == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, errors.New("username required")
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return nil, errors.New("pending key empty")
	}

	now := time.Now().UTC()
	ownerID := uuid.New().String()
	expiresAt := now.Add(c.cfg.LeaseTTL)
	snapshot := pendingLeaseSnapshot{
		Status:          "pending",
		State:           string(PendingStateLease),
		OwnerID:         ownerID,
		Version:         now.UnixNano(),
		LeaseExpiresAt:  expiresAt.Format(time.RFC3339Nano),
		AcquireAt:       now.Format(time.RFC3339Nano),
		UpdatedAt:       now.Format(time.RFC3339Nano),
		Username:        trimmed,
		RequestID:       strings.TrimSpace(meta.RequestID),
		Operator:        strings.TrimSpace(meta.Operator),
		ClientIP:        strings.TrimSpace(meta.ClientIP),
		LegacyRequestID: strings.TrimSpace(meta.LegacyRequestID),
	}

	payload, err := json.Marshal(&snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal pending snapshot: %w", err)
	}

	opCtx, cancel := c.newOpContext(ctx)
	setNXStart := time.Now()
	created, setNXErr := c.redis.SetNX(opCtx, key, string(payload), c.cfg.LeaseTTL)
	cancel()
	setNXDuration := time.Since(setNXStart)
	metrics.RecordRedisOperation("pending_lease_setnx", setNXDuration.Seconds(), setNXErr)
	if setNXErr != nil {
		return nil, setNXErr
	}
	if !created {
		state, observeErr := c.Observe(ctx, trimmed)
		if observeErr != nil {
			return nil, observeErr
		}
		return nil, &AcquireError{Reason: AcquireFailureConflict, State: state}
	}

	queueDepth := c.redis.IncrememntWithExpire(ctx, c.cfg.MetricsKey, int64(c.cfg.BackpressureWindow.Seconds()))
	level := c.classifyBackpressure(queueDepth)
	snapshot.QueueDepth = queueDepth
	snapshot.Backpressure = string(level)
	if level != BackpressureNone {
		snapshot.Status = "degraded"
		snapshot.Degraded = true
	}
	snapshot.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

	if updatedPayload, marshalErr := json.Marshal(&snapshot); marshalErr == nil {
		updateCtx, updateCancel := c.newOpContext(ctx)
		if err := c.redis.SetKey(updateCtx, key, string(updatedPayload), c.cfg.LeaseTTL); err != nil {
			log.Warnw("update pending snapshot failed", "username", trimmed, "error", err)
		}
		updateCancel()
	} else {
		log.Warnw("marshal pending snapshot with queue depth failed", "username", trimmed, "error", marshalErr)
	}

	if level == BackpressureSevere {
		if _, releaseErr := c.Release(ctx, trimmed, ownerID); releaseErr != nil {
			log.Warnw("rollback pending lease after severe backpressure failed", "username", trimmed, "error", releaseErr)
		}
		state := &PendingState{
			Exists:       true,
			State:        PendingStateValue(snapshot.State),
			LeaseOwner:   ownerID,
			Version:      snapshot.Version,
			Backpressure: level,
			QueueDepth:   queueDepth,
			Raw:          string(payload),
		}
		return nil, &AcquireError{Reason: AcquireFailureBackpressure, State: state, QueueDepth: queueDepth}
	}

	lease := &PendingLease{
		Username:       trimmed,
		OwnerID:        ownerID,
		Version:        snapshot.Version,
		AcquireAt:      now,
		LeaseExpiresAt: expiresAt,
		QueueDepth:     queueDepth,
		Backpressure:   level,
		Metadata:       meta,
	}

	return &AcquireResult{Lease: lease, SetNXDuration: setNXDuration}, nil
}

func (c *PendingCoordinator) Release(ctx context.Context, username string, ownerID string) (time.Duration, error) {
	if c == nil || c.redis == nil {
		return 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return 0, nil
	}

	deleteCtx, cancel := c.newOpContext(ctx)
	deleteStart := time.Now()
	deleted, err := c.deleteKeyWithOwner(deleteCtx, key, ownerID)
	cancel()
	deleteDuration := time.Since(deleteStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_delete", deleteDuration.Seconds(), metricErr)
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil {
		return deleteDuration, err
	}

	if deleted {
		if _, decErr := c.safeDecrementActive(ctx); decErr != nil {
			return deleteDuration, decErr
		}
	}
	return deleteDuration, nil
}

func (c *PendingCoordinator) Observe(ctx context.Context, username string) (*PendingState, error) {
	result := &PendingState{State: PendingStateUnknown}
	if c == nil || c.redis == nil {
		return result, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return result, nil
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return result, nil
	}

	getCtx, cancel := c.newOpContext(ctx)
	getStart := time.Now()
	raw, err := c.redis.GetKey(getCtx, key)
	cancel()
	getDuration := time.Since(getStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_get", getDuration.Seconds(), metricErr)
	result.RedisGetDuration = getDuration
	if errors.Is(err, redis.Nil) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	result.Exists = true
	result.Raw = raw
	var snapshot pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr == nil {
		result.State = PendingStateValue(snapshot.State)
		result.LeaseOwner = snapshot.OwnerID
		result.Version = snapshot.Version
		result.Backpressure = BackpressureLevel(snapshot.Backpressure)
		result.QueueDepth = snapshot.QueueDepth
		if snapshot.LeaseExpiresAt != "" {
			if expiresAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.LeaseExpiresAt); parseErr == nil {
				result.LeaseExpiresAt = expiresAt
			}
		}
		if snapshot.Degraded && result.Backpressure == BackpressureNone {
			result.Backpressure = BackpressureElevated
		}
	}

	ttlCtx, ttlCancel := c.newOpContext(ctx)
	ttlStart := time.Now()
	ttlSeconds, ttlErr := c.redis.GetExp(ttlCtx, key)
	ttlCancel()
	ttlDuration := time.Since(ttlStart)
	ttlMetricErr := ttlErr
	if ttlErr == storage.ErrKeyNotFound {
		ttlMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_ttl", ttlDuration.Seconds(), ttlMetricErr)
	result.RedisTTLDur = ttlDuration
	if ttlErr == nil && ttlSeconds > 0 {
		result.TTL = time.Duration(ttlSeconds) * time.Second
	}
	if ttlErr != nil && ttlErr != storage.ErrKeyNotFound {
		return result, ttlErr
	}
	return result, nil
}

func (c *PendingCoordinator) deleteKeyWithOwner(ctx context.Context, key, ownerID string) (bool, error) {
	if ownerID == "" {
		return c.redis.DeleteKey(ctx, key)
	}
	raw, err := c.redis.GetKey(ctx, key)
	if errors.Is(err, redis.Nil) {
		return false, redis.Nil
	}
	if err != nil {
		return false, err
	}
	var snapshot pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr == nil {
		if snapshot.OwnerID != "" && snapshot.OwnerID != ownerID {
			return false, nil
		}
	}
	return c.redis.DeleteKey(ctx, key)
}

func (c *PendingCoordinator) classifyBackpressure(depth int64) BackpressureLevel {
	if depth >= int64(c.cfg.BackpressureHardLimit) {
		return BackpressureSevere
	}
	if depth >= int64(c.cfg.BackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

const decrementActiveLua = `
local current = redis.call("GET", KEYS[1])
if not current then
    return 0
end
current = tonumber(current)
if not current then
    redis.call("DEL", KEYS[1])
    return 0
end
if current <= 1 then
    redis.call("DEL", KEYS[1])
    return 0
end
local newVal = redis.call("DECR", KEYS[1])
if not newVal then
    redis.call("DEL", KEYS[1])
    return 0
end
if newVal < 0 then
    redis.call("DEL", KEYS[1])
    return 0
end
return newVal
`

func (c *PendingCoordinator) safeDecrementActive(ctx context.Context) (int64, error) {
	evalCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	result, err := c.redis.Eval(evalCtx, decrementActiveLua, []string{c.cfg.MetricsKey}, nil)
	if err != nil {
		return 0, err
	}
	switch v := result.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		parsed, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected decrement result type %T", result)
	}
}

func (c *PendingCoordinator) newOpContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := c.cfg.ObserveTimeout
	if timeout <= 0 {
		timeout = 700 * time.Millisecond
	}
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}
