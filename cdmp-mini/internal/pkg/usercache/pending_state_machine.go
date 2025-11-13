package usercache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
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

var backpressureGaugeValues = map[BackpressureLevel]float64{
	BackpressureNone:     0,
	BackpressureElevated: 1,
	BackpressureSevere:   2,
}

// BackpressureDelayBucket describes a queue depth threshold and the delay to apply when it is reached.
type BackpressureDelayBucket struct {
	Depth int
	Delay time.Duration
}

// BackpressureDelayProfile groups delay buckets for each backpressure level so that producers and consumers respond consistently.
type BackpressureDelayProfile struct {
	Elevated []BackpressureDelayBucket
	Severe   []BackpressureDelayBucket
}

func (p *BackpressureDelayProfile) ensureDefaults(soft, hard int, elevatedBase, elevatedMax, severeBase, severeMax time.Duration) {
	if soft <= 0 {
		soft = 1
	}
	if hard <= 0 {
		hard = soft
	}
	if elevatedBase <= 0 {
		elevatedBase = 20 * time.Millisecond
	}
	if elevatedMax <= 0 {
		elevatedMax = elevatedBase
	}
	if elevatedMax < elevatedBase {
		elevatedMax = elevatedBase
	}
	if severeBase <= 0 {
		severeBase = 80 * time.Millisecond
	}
	if severeMax <= 0 {
		severeMax = severeBase
	}
	if severeMax < severeBase {
		severeMax = severeBase
	}
	if len(p.Elevated) == 0 {
		p.Elevated = []BackpressureDelayBucket{
			{Depth: soft, Delay: elevatedBase},
			{Depth: maxInt(soft*2, soft+1), Delay: elevatedBase + (elevatedMax-elevatedBase)/2},
			{Depth: maxInt(soft*4, soft+2), Delay: elevatedMax},
		}
	}
	if len(p.Severe) == 0 {
		p.Severe = []BackpressureDelayBucket{
			{Depth: hard, Delay: severeBase},
			{Depth: maxInt(hard*2, hard+1), Delay: severeBase + (severeMax-severeBase)/2},
			{Depth: maxInt(hard*4, hard+2), Delay: severeMax},
		}
	}
	sort.Slice(p.Elevated, func(i, j int) bool { return p.Elevated[i].Depth < p.Elevated[j].Depth })
	sort.Slice(p.Severe, func(i, j int) bool { return p.Severe[i].Depth < p.Severe[j].Depth })
}

func (p BackpressureDelayProfile) delay(level BackpressureLevel, depth int64) time.Duration {
	switch level {
	case BackpressureElevated:
		return pickDelay(p.Elevated, depth)
	case BackpressureSevere:
		return pickDelay(p.Severe, depth)
	default:
		return 0
	}
}

func pickDelay(buckets []BackpressureDelayBucket, depth int64) time.Duration {
	var result time.Duration
	if depth < 0 {
		depth = 0
	}
	for _, bucket := range buckets {
		if bucket.Depth <= 0 || bucket.Delay <= 0 {
			continue
		}
		if depth >= int64(bucket.Depth) && bucket.Delay > result {
			result = bucket.Delay
		}
	}
	return result
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

type LeaseMetadata struct {
	Username        string
	RequestID       string
	Operator        string
	ClientIP        string
	LegacyRequestID string
}

//背压配置

type PendingCoordinatorConfig struct {
	LeaseTTL                 time.Duration            //租约有效期（控制单个操作的最大处理时间）
	ObserveTimeout           time.Duration            //观察超时时间（控制读取当前状态的最大等待时间）
	BackpressureWindow       time.Duration            //背压评估窗口
	BackpressureSoftLimit    int                      //软限制（达到该值开始应用背压）
	BackpressureHardLimit    int                      //硬限制（达到该值触发严重背压429）
	MetricsKey               string                   //指标键
	Component                string                   //组件标识（用于区分不同服务）
	LogLeaseEvents           bool                     //是否记录租约事件日志
	ReleaseRetention         time.Duration            //正常释放租约状态保留时间
	ExpiredRetention         time.Duration            //过期租约状态保留时间
	ExpiredGracePeriod       time.Duration            //过期宽限期
	ElevatedDelayBase        time.Duration            //基础延迟（背压升高时）
	ElevatedDelayMax         time.Duration            //最大延迟（背压升高时）
	SevereDelayBase          time.Duration            //基础延迟（严重背压时）
	SevereDelayMax           time.Duration            //最大延迟（严重背压时）
	BackpressureDelayProfile BackpressureDelayProfile //延迟曲线配置
}

/*
数据源抽象：PendingCoordinator 是背压数据的统一入口
指标来源：从 "pending lease metrics"（待处理租约指标）获取背压样本
业务上下文：在用户缓存场景中，"pending lease" 可能指：
等待处理的缓存更新操作
未完成的分布式锁获取
缓存击穿导致的积压请求
设计意义：
将数据采集与决策逻辑解耦
支持不同的背压信号源实现
便于测试和模拟
*/
type PendingCoordinator struct {
	redis     *storage.RedisCluster
	cfg       PendingCoordinatorConfig //背压配置
	component string                   //组件标识（用于区分不同服务）
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
	ReleasedAt      string `json:"released_at,omitempty"`
	ReleasedBy      string `json:"released_by,omitempty"`
	ExpiredAt       string `json:"expired_at,omitempty"`
	ExpiredReason   string `json:"expired_reason,omitempty"`
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
	ReleasedAt       time.Time
	Username         string
	ExpiredAt        time.Time
	ExpiredReason    string
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
	ErrPendingLeaseConflict      = errLeaseConflict
	ErrPendingBackpressure       = errBackpressure
	ErrPendingLeaseOwnerMismatch = errors.New("pending lease owner mismatch")
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
	if cfg.ReleaseRetention <= 0 {
		cfg.ReleaseRetention = 3 * time.Second
	}
	if cfg.ExpiredRetention <= 0 {
		cfg.ExpiredRetention = 30 * time.Second
	}
	if cfg.ExpiredGracePeriod <= 0 {
		cfg.ExpiredGracePeriod = 2 * time.Second
	}
	if cfg.ElevatedDelayBase <= 0 {
		cfg.ElevatedDelayBase = 20 * time.Millisecond
	}
	if cfg.ElevatedDelayMax <= 0 {
		cfg.ElevatedDelayMax = 45 * time.Millisecond
	}
	if cfg.SevereDelayBase <= 0 {
		cfg.SevereDelayBase = 80 * time.Millisecond
	}
	if cfg.SevereDelayMax <= 0 {
		cfg.SevereDelayMax = 150 * time.Millisecond
	}

	applyCoordinatorEnvOverrides(&cfg)

	if cfg.ElevatedDelayBase <= 0 {
		cfg.ElevatedDelayBase = 20 * time.Millisecond
	}
	if cfg.ElevatedDelayMax <= 0 {
		cfg.ElevatedDelayMax = cfg.ElevatedDelayBase
	}
	if cfg.ElevatedDelayMax < cfg.ElevatedDelayBase {
		cfg.ElevatedDelayMax = cfg.ElevatedDelayBase
	}
	if cfg.SevereDelayBase <= 0 {
		cfg.SevereDelayBase = 80 * time.Millisecond
	}
	if cfg.SevereDelayMax <= 0 {
		cfg.SevereDelayMax = cfg.SevereDelayBase
	}
	if cfg.SevereDelayMax < cfg.SevereDelayBase {
		cfg.SevereDelayMax = cfg.SevereDelayBase
	}

	cfg.BackpressureDelayProfile.ensureDefaults(cfg.BackpressureSoftLimit, cfg.BackpressureHardLimit, cfg.ElevatedDelayBase, cfg.ElevatedDelayMax, cfg.SevereDelayBase, cfg.SevereDelayMax)

	component := strings.TrimSpace(cfg.Component)
	if component == "" {
		component = "pending_coordinator"
	}
	cfg.Component = component

	return &PendingCoordinator{redis: redis, cfg: cfg, component: component}
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
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_RELEASE_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ReleaseRetention = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_EXPIRED_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ExpiredRetention = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_EXPIRED_GRACE")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
			cfg.ExpiredGracePeriod = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_ELEVATED")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ElevatedDelayBase = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_ELEVATED_MAX")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ElevatedDelayMax = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_SEVERE")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.SevereDelayBase = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_SEVERE_MAX")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.SevereDelayMax = parsed
		}
	}
	if cfg.BackpressureHardLimit < cfg.BackpressureSoftLimit {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit
	}
}

const releasedStateRetryLimit = 3

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

	for attempt := 0; attempt < releasedStateRetryLimit+1; attempt++ {
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
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "setnx_error").Inc()
			}
			c.logLeaseEvent("error", "pending lease setnx failed", "username", trimmed, "error", setNXErr)
			return nil, setNXErr
		}
		if !created {
			state, observeErr := c.Observe(ctx, trimmed)
			if observeErr != nil {
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(c.component, "observe_error").Inc()
				}
				c.logLeaseEvent("error", "pending lease observe failed", "username", trimmed, "error", observeErr)
				return nil, observeErr
			}
			if c.shouldPromoteExpired(state) {
				if _, promoteErr := c.promoteExpired(ctx, trimmed, state); promoteErr != nil {
					return nil, promoteErr
				}
				updatedState, refreshedErr := c.Observe(ctx, trimmed)
				if refreshedErr == nil {
					state = updatedState
				} else {
					c.logLeaseEvent("debug", "pending lease observe after expire promotion failed", "username", trimmed, "error", refreshedErr)
				}
			}
			if state != nil && state.State == PendingStateReleased {
				if cleanupErr := c.cleanupReleasedState(ctx, trimmed); cleanupErr != nil {
					c.logLeaseEvent("warn", "failed to cleanup released pending lease", "username", trimmed, "error", cleanupErr)
					return nil, cleanupErr
				}
				c.logLeaseEvent("debug", "removed released pending lease before retry", "username", trimmed, "attempt", attempt+1)
				if attempt < releasedStateRetryLimit {
					continue
				}
			}
			if state != nil && state.State == PendingStateExpired {
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_conflict_expired").Inc()
				}
				expiredAt := ""
				if !state.ExpiredAt.IsZero() {
					expiredAt = state.ExpiredAt.Format(time.RFC3339Nano)
				}
				fields := []interface{}{"username", trimmed, "queue_depth", state.QueueDepth, "backpressure", string(state.Backpressure)}
				if expiredAt != "" {
					fields = append(fields, "expired_at", expiredAt)
				}
				if state.ExpiredReason != "" {
					fields = append(fields, "expired_reason", state.ExpiredReason)
				}
				c.logLeaseEvent("warn", "pending lease acquisition blocked by expired state", fields...)
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: state.QueueDepth}
			}
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_conflict").Inc()
			}
			queueDepth := int64(0)
			level := BackpressureNone
			owner := ""
			if state != nil {
				queueDepth = state.QueueDepth
				owner = state.LeaseOwner
				if state.Backpressure != "" {
					level = state.Backpressure
				}
				c.recordQueueDepthMetrics(queueDepth, level)
			}
			c.logLeaseEvent("info", "pending lease acquire conflict", "username", trimmed, "owner", owner, "queue_depth", queueDepth, "backpressure", string(level))
			return nil, &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: queueDepth}
		}

		queueDepth := c.redis.IncrememntWithExpire(ctx, c.cfg.MetricsKey, int64(c.cfg.BackpressureWindow.Seconds()))
		level := c.classifyBackpressure(queueDepth)
		c.recordQueueDepthMetrics(queueDepth, level)

		snapshot.QueueDepth = queueDepth
		snapshot.Backpressure = string(level)
		if level != BackpressureNone {
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
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "backpressure_reject").Inc()
			}
			c.logLeaseEvent("warn", "pending lease rejected by severe backpressure", "username", trimmed, "owner", ownerID, "queue_depth", queueDepth, "backpressure", string(level))
			if _, releaseErr := c.Release(ctx, trimmed, ownerID); releaseErr != nil {
				log.Warnw("rollback pending lease after severe backpressure failed", "username", trimmed, "error", releaseErr)
			}
			state := &PendingState{
				Exists:       true,
				State:        PendingStateValue(snapshot.State),
				LeaseOwner:   ownerID,
				Version:      snapshot.Version,
				Username:     trimmed,
				Backpressure: level,
				QueueDepth:   queueDepth,
				Raw:          string(payload),
			}
			return nil, &AcquireError{Reason: AcquireFailureBackpressure, State: state, QueueDepth: queueDepth}
		}

		fields := []interface{}{"username", trimmed, "owner", ownerID, "queue_depth", queueDepth, "backpressure", string(level), "lease_ttl_ms", c.cfg.LeaseTTL.Milliseconds()}
		if snapshot.RequestID != "" {
			fields = append(fields, "request_id", snapshot.RequestID)
		}
		if snapshot.Operator != "" {
			fields = append(fields, "operator", snapshot.Operator)
		}
		if snapshot.ClientIP != "" {
			fields = append(fields, "client_ip", snapshot.ClientIP)
		}
		if level != BackpressureNone {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "backpressure_elevated").Inc()
			}
			c.logLeaseEvent("warn", "pending lease acquired under backpressure", fields...)
		} else {
			c.logLeaseEvent("info", "pending lease acquired", fields...)
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_success").Inc()
		}
		if metrics.PendingLeaseActiveGauge != nil {
			metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Inc()
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

	return nil, errors.New("pending lease acquisition retries exhausted")
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
	deleted, snapshot, err := c.deleteKeyWithOwner(deleteCtx, key, ownerID)
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

	var holdDuration time.Duration
	if snapshot != nil && snapshot.AcquireAt != "" {
		if acquiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.AcquireAt); parseErr == nil {
			holdDuration = time.Since(acquiredAt)
			if holdDuration < 0 {
				holdDuration = 0
			}
		} else {
			c.logLeaseEvent("debug", "failed to parse pending lease acquire timestamp", "username", trimmed, "owner", ownerID, "error", parseErr)
		}
	}

	if !deleted {
		if snapshot != nil && snapshot.OwnerID != "" && ownerID != "" && snapshot.OwnerID != ownerID {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_owner_mismatch").Inc()
			}
			c.logLeaseEvent("warn", "pending lease release skipped due to owner mismatch", "username", trimmed, "owner", ownerID, "current_owner", snapshot.OwnerID)
			return deleteDuration, ErrPendingLeaseOwnerMismatch
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_miss").Inc()
		}
		c.logLeaseEvent("debug", "pending lease release no-op", "username", trimmed, "owner", ownerID)
		return deleteDuration, nil
	}

	if metrics.PendingLeaseActiveGauge != nil {
		metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Dec()
	}
	remaining, decErr := c.safeDecrementActive(ctx)
	if decErr != nil {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_decrement_error").Inc()
		}
		c.logLeaseEvent("error", "pending lease decrement failed", "username", trimmed, "owner", ownerID, "error", decErr)
		return deleteDuration, decErr
	}
	newLevel := c.classifyBackpressure(remaining)
	c.recordQueueDepthMetrics(remaining, newLevel)
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_success").Inc()
	}
	if holdDuration > 0 && metrics.PendingLeaseHoldDuration != nil {
		metrics.PendingLeaseHoldDuration.WithLabelValues(c.component, "success").Observe(holdDuration.Seconds())
	}

	initialDepth := int64(0)
	if snapshot != nil {
		initialDepth = snapshot.QueueDepth
	}
	fields := []interface{}{"username", trimmed, "owner", ownerID, "initial_queue_depth", initialDepth, "remaining_queue_depth", remaining, "backpressure", string(newLevel)}
	if holdDuration > 0 {
		fields = append(fields, "hold_ms", holdDuration.Milliseconds())
	}
	c.logLeaseEvent("info", "pending lease released", fields...)

	if err := c.writeReleaseSnapshot(ctx, trimmed, snapshot, ownerID, remaining); err != nil {
		c.logLeaseEvent("warn", "write release snapshot failed", "username", trimmed, "error", err)
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
	result.Username = trimmed
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
		if snapshot.Username != "" {
			result.Username = snapshot.Username
		}
		if snapshot.LeaseExpiresAt != "" {
			if expiresAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.LeaseExpiresAt); parseErr == nil {
				result.LeaseExpiresAt = expiresAt
			}
		}
		if snapshot.ReleasedAt != "" {
			if releasedAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.ReleasedAt); parseErr == nil {
				result.ReleasedAt = releasedAt
			}
		}
		if snapshot.ExpiredAt != "" {
			if expiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.ExpiredAt); parseErr == nil {
				result.ExpiredAt = expiredAt
			}
		}
		if snapshot.ExpiredReason != "" {
			result.ExpiredReason = snapshot.ExpiredReason
		}
		if snapshot.Degraded && result.Backpressure == BackpressureNone {
			result.Backpressure = BackpressureElevated
		}
	}
	if result.Exists {
		c.recordQueueDepthMetrics(result.QueueDepth, result.Backpressure)
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "observe").Inc()
		}
		c.logLeaseEvent("debug", "pending lease observed", "username", trimmed, "owner", result.LeaseOwner, "queue_depth", result.QueueDepth, "backpressure", string(result.Backpressure))
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

func (c *PendingCoordinator) cleanupReleasedState(ctx context.Context, username string) error {
	if c == nil || c.redis == nil {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	cleanupCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	_, err := c.redis.DeleteKey(cleanupCtx, key)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "cleanup_released").Inc()
	}
	return nil
}

func (c *PendingCoordinator) cleanupExpiredState(ctx context.Context, username string) error {
	if c == nil || c.redis == nil {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	cleanupCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	_, err := c.redis.DeleteKey(cleanupCtx, key)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "cleanup_expired").Inc()
	}
	return nil
}

func (c *PendingCoordinator) CleanupExpired(ctx context.Context, username string) error {
	return c.cleanupExpiredState(ctx, username)
}

func (c *PendingCoordinator) promoteExpired(ctx context.Context, username string, state *PendingState) (bool, error) {
	if c == nil || c.redis == nil || state == nil {
		return false, nil
	}
	var snapshot pendingLeaseSnapshot
	if err := json.Unmarshal([]byte(state.Raw), &snapshot); err != nil {
		c.logLeaseEvent("debug", "failed to decode pending lease snapshot before expire", "username", username, "error", err)
		return false, err
	}
	holdDuration := time.Duration(0)
	if snapshot.AcquireAt != "" {
		if acquiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.AcquireAt); parseErr == nil {
			holdDuration = time.Since(acquiredAt)
			if holdDuration < 0 {
				holdDuration = 0
			}
		}
	}
	if metrics.PendingLeaseActiveGauge != nil {
		metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Dec()
	}
	remaining, decErr := c.safeDecrementActive(ctx)
	if decErr != nil {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "expire_decrement_error").Inc()
		}
		c.logLeaseEvent("error", "pending lease decrement failed while marking expired", "username", username, "error", decErr)
		remaining = state.QueueDepth
	}
	newLevel := c.classifyBackpressure(remaining)
	c.recordQueueDepthMetrics(remaining, newLevel)
	if holdDuration > 0 && metrics.PendingLeaseHoldDuration != nil {
		metrics.PendingLeaseHoldDuration.WithLabelValues(c.component, "expired").Observe(holdDuration.Seconds())
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "expire_promote").Inc()
	}
	if err := c.writeExpiredSnapshot(ctx, username, &snapshot, remaining); err != nil {
		return false, err
	}
	fields := []interface{}{"username", username, "queue_depth", state.QueueDepth, "remaining_queue_depth", remaining}
	if holdDuration > 0 {
		fields = append(fields, "hold_ms", holdDuration.Milliseconds())
	}
	c.logLeaseEvent("warn", "pending lease expired without release", fields...)
	return true, nil
}

func (c *PendingCoordinator) shouldPromoteExpired(state *PendingState) bool {
	if c == nil || state == nil {
		return false
	}
	if state.State != PendingStateLease {
		return false
	}
	now := time.Now()
	if !state.LeaseExpiresAt.IsZero() {
		expiry := state.LeaseExpiresAt.Add(c.cfg.ExpiredGracePeriod)
		if now.After(expiry) {
			return true
		}
	}
	if state.TTL <= 0 {
		return true
	}
	return false
}

func (c *PendingCoordinator) writeReleaseSnapshot(ctx context.Context, username string, snapshot *pendingLeaseSnapshot, owner string, remaining int64) error {
	if c == nil || c.redis == nil {
		return nil
	}
	ttl := c.releaseRetentionTTL()
	if ttl <= 0 {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	now := time.Now().UTC()
	released := pendingLeaseSnapshot{
		Status:         "completed",
		State:          string(PendingStateReleased),
		OwnerID:        "",
		Version:        now.UnixNano(),
		LeaseExpiresAt: now.Add(ttl).Format(time.RFC3339Nano),
		AcquireAt:      "",
		UpdatedAt:      now.Format(time.RFC3339Nano),
		QueueDepth:     remaining,
		Backpressure:   "",
		Username:       username,
		ReleasedAt:     now.Format(time.RFC3339Nano),
		ReleasedBy:     owner,
	}
	if snapshot != nil {
		if snapshot.RequestID != "" {
			released.RequestID = snapshot.RequestID
		}
		if snapshot.Operator != "" {
			released.Operator = snapshot.Operator
		}
		if snapshot.ClientIP != "" {
			released.ClientIP = snapshot.ClientIP
		}
		if snapshot.LegacyRequestID != "" {
			released.LegacyRequestID = snapshot.LegacyRequestID
		}
		if snapshot.AcquireAt != "" {
			released.AcquireAt = snapshot.AcquireAt
		}
		if snapshot.Backpressure != "" {
			released.Backpressure = snapshot.Backpressure
		}
	}

	payload, err := json.Marshal(&released)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKey(writeCtx, key, string(payload), ttl); err != nil {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "write_released").Inc()
	}
	c.logLeaseEvent("debug", "pending lease release snapshot written", "username", username, "ttl_ms", ttl.Milliseconds())
	return nil
}

func (c *PendingCoordinator) releaseRetentionTTL() time.Duration {
	if c == nil {
		return 0
	}
	ttl := c.cfg.ReleaseRetention
	if ttl <= 0 {
		return 0
	}
	if ttl > c.cfg.LeaseTTL {
		return c.cfg.LeaseTTL
	}
	return ttl
}

func (c *PendingCoordinator) writeExpiredSnapshot(ctx context.Context, username string, snapshot *pendingLeaseSnapshot, remaining int64) error {
	if c == nil || c.redis == nil {
		return nil
	}
	ttl := c.expiredRetentionTTL()
	if ttl <= 0 {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	now := time.Now().UTC()
	expired := pendingLeaseSnapshot{
		Status:         "failed",
		State:          string(PendingStateExpired),
		OwnerID:        "",
		Version:        now.UnixNano(),
		LeaseExpiresAt: now.Add(ttl).Format(time.RFC3339Nano),
		AcquireAt:      "",
		UpdatedAt:      now.Format(time.RFC3339Nano),
		QueueDepth:     remaining,
		Backpressure:   "",
		Username:       username,
		ExpiredAt:      now.Format(time.RFC3339Nano),
		ExpiredReason:  "lease_timeout",
	}
	if snapshot != nil {
		if snapshot.RequestID != "" {
			expired.RequestID = snapshot.RequestID
		}
		if snapshot.Operator != "" {
			expired.Operator = snapshot.Operator
		}
		if snapshot.ClientIP != "" {
			expired.ClientIP = snapshot.ClientIP
		}
		if snapshot.LegacyRequestID != "" {
			expired.LegacyRequestID = snapshot.LegacyRequestID
		}
		if snapshot.AcquireAt != "" {
			expired.AcquireAt = snapshot.AcquireAt
		}
		if snapshot.Backpressure != "" {
			expired.Backpressure = snapshot.Backpressure
		}
		if snapshot.QueueDepth > 0 && remaining == 0 {
			expired.QueueDepth = snapshot.QueueDepth
		}
	}

	payload, err := json.Marshal(&expired)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKey(writeCtx, key, string(payload), ttl); err != nil {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "write_expired").Inc()
	}
	c.logLeaseEvent("warn", "pending lease marked as expired", "username", username, "ttl_ms", ttl.Milliseconds())
	return nil
}

func (c *PendingCoordinator) expiredRetentionTTL() time.Duration {
	if c == nil {
		return 0
	}
	ttl := c.cfg.ExpiredRetention
	if ttl <= 0 {
		return 0
	}
	maxRetention := 2 * c.cfg.LeaseTTL
	if maxRetention <= 0 {
		maxRetention = c.cfg.LeaseTTL
	}
	if maxRetention > 0 && ttl > maxRetention {
		return maxRetention
	}
	return ttl
}

func (c *PendingCoordinator) deleteKeyWithOwner(ctx context.Context, key, ownerID string) (bool, *pendingLeaseSnapshot, error) {
	raw, err := c.redis.GetKey(ctx, key)
	if errors.Is(err, redis.Nil) {
		return false, nil, redis.Nil
	}
	if err != nil {
		return false, nil, err
	}
	var snapshot pendingLeaseSnapshot
	var snapshotPtr *pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr == nil {
		snapshotPtr = &snapshot
		if ownerID != "" && snapshot.OwnerID != "" && snapshot.OwnerID != ownerID {
			return false, snapshotPtr, nil
		}
	} else {
		c.logLeaseEvent("debug", "failed to decode pending lease snapshot before delete", "key", key, "error", decodeErr)
	}
	deleted, deleteErr := c.redis.DeleteKey(ctx, key)
	return deleted, snapshotPtr, deleteErr
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

func (c *PendingCoordinator) logLeaseEvent(severity, message string, fields ...interface{}) {
	if c == nil {
		return
	}
	args := []interface{}{"component", c.component}
	if len(fields) > 0 {
		args = append(args, fields...)
	}
	switch severity {
	case "debug":
		if c.cfg.LogLeaseEvents {
			log.Debugw(message, args...)
		}
	case "info":
		if c.cfg.LogLeaseEvents {
			log.Infow(message, args...)
		} else {
			log.Debugw(message, args...)
		}
	case "warn":
		log.Warnw(message, args...)
	case "error":
		log.Errorw(message, args...)
	default:
		if c.cfg.LogLeaseEvents {
			log.Infow(message, args...)
		} else {
			log.Debugw(message, args...)
		}
	}
}

func backpressureValue(level BackpressureLevel) float64 {
	if value, ok := backpressureGaugeValues[level]; ok {
		return value
	}
	return 0
}

func (c *PendingCoordinator) recordQueueDepthMetrics(queueDepth int64, level BackpressureLevel) {
	if metrics.PendingLeaseQueueDepth != nil {
		metrics.PendingLeaseQueueDepth.WithLabelValues(c.component).Set(float64(queueDepth))
	}
	if metrics.PendingLeaseBackpressureLevel != nil {
		metrics.PendingLeaseBackpressureLevel.WithLabelValues(c.component).Set(backpressureValue(level))
	}
}

func (c *PendingCoordinator) SampleQueueDepth(ctx context.Context) (int64, BackpressureLevel, error) {
	if c == nil || c.redis == nil || strings.TrimSpace(c.cfg.MetricsKey) == "" {
		return 0, BackpressureNone, nil
	}
	sampleCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	raw, err := c.redis.GetKey(sampleCtx, c.cfg.MetricsKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, BackpressureNone, nil
		}
		return 0, BackpressureNone, err
	}
	depth, parseErr := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if parseErr != nil {
		return 0, BackpressureNone, parseErr
	}
	if depth < 0 {
		depth = 0
	}
	level := c.classifyBackpressure(depth)
	return depth, level, nil
}

// BackpressureDelay returns the recommended sleep duration for the supplied level/depth pair.
func (c *PendingCoordinator) BackpressureDelay(level BackpressureLevel, depth int64) time.Duration {
	if c == nil {
		return 0
	}
	return c.cfg.BackpressureDelayProfile.delay(level, depth)
}

func (c *PendingCoordinator) ComponentName() string {
	if c == nil {
		return ""
	}
	return c.component
}

func (c *PendingCoordinator) ListExpired(ctx context.Context, limit int) ([]*PendingState, error) {
	if c == nil || c.redis == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 128
	}
	keys := c.redis.GetKeys(ctx, PendingCreatePrefix())
	if len(keys) == 0 {
		return nil, nil
	}
	result := make([]*PendingState, 0, limit)
	for _, key := range keys {
		if len(result) >= limit {
			break
		}
		if !strings.HasPrefix(key, PendingCreatePrefix()) {
			continue
		}
		snapshotKey := strings.TrimSpace(strings.TrimPrefix(key, PendingCreatePrefix()))
		if snapshotKey == "" {
			continue
		}
		state, err := c.Observe(ctx, snapshotKey)
		if err != nil {
			c.logLeaseEvent("debug", "failed to observe pending lease during expired scan", "username", snapshotKey, "error", err)
			continue
		}
		if state != nil && state.Exists && state.State == PendingStateExpired {
			if strings.TrimSpace(state.Username) == "" {
				state.Username = snapshotKey
			}
			result = append(result, state)
		}
	}
	return result, nil
}
