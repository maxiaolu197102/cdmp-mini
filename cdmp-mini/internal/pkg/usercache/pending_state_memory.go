package usercache

import (
	"context"
	"errors"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
)

const defaultMemoryPendingShardCount = 32

type memoryLeaseShard struct {
	mu     sync.RWMutex
	leases map[string]*memoryLeaseEntry
}

type memoryPendingCoordinator struct {
	cfg         PendingCoordinatorConfig
	component   string
	shards      []memoryLeaseShard
	shardMask   int
	profile     atomic.Value
	activeCount atomic.Int64
}

type memoryLeaseEntry struct {
	lease         *PendingLease
	metadata      LeaseMetadata
	state         PendingStateValue
	releasedAt    time.Time
	expiredAt     time.Time
	expiredReason string
	lastHeartbeat time.Time
}

func newMemoryPendingCoordinator(cfg PendingCoordinatorConfig, component string) *memoryPendingCoordinator {
	cfg.BackpressureDelayProfile.ensureDefaults(cfg.BackpressureSoftLimit, cfg.BackpressureHardLimit, cfg.ElevatedDelayBase, cfg.ElevatedDelayMax, cfg.SevereDelayBase, cfg.SevereDelayMax)
	shardCount := defaultMemoryPendingShardCount
	if shardCount < 1 {
		shardCount = 1
	}
	// enforce power-of-two to simplify mask computation
	shardCount = nextPowerOfTwo(shardCount)
	m := &memoryPendingCoordinator{
		cfg:       cfg,
		component: component,
		shards:    make([]memoryLeaseShard, shardCount),
		shardMask: shardCount - 1,
	}
	for i := range m.shards {
		m.shards[i].leases = make(map[string]*memoryLeaseEntry)
	}
	m.profile.Store(cfg.BackpressureDelayProfile.clone())
	return m
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

func (m *memoryPendingCoordinator) shardFor(username string) *memoryLeaseShard {
	if m == nil || len(m.shards) == 0 {
		return nil
	}
	idx := m.shardIndex(username)
	return &m.shards[idx]
}

func (m *memoryPendingCoordinator) shardIndex(key string) int {
	if len(m.shards) == 0 {
		return 0
	}
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	value := int(h.Sum32())
	if value < 0 {
		value = -value
	}
	if m.shardMask > 0 {
		return value & m.shardMask
	}
	return value % len(m.shards)
}

func (m *memoryPendingCoordinator) currentProfile() BackpressureDelayProfile {
	if m == nil {
		return BackpressureDelayProfile{}
	}
	if v := m.profile.Load(); v != nil {
		if profile, ok := v.(BackpressureDelayProfile); ok {
			return profile
		}
	}
	return m.cfg.BackpressureDelayProfile
}

func (m *memoryPendingCoordinator) UpdateBackpressureProfile(profile BackpressureDelayProfile) {
	if m == nil {
		return
	}
	profile.ensureDefaults(m.cfg.BackpressureSoftLimit, m.cfg.BackpressureHardLimit, m.cfg.ElevatedDelayBase, m.cfg.ElevatedDelayMax, m.cfg.SevereDelayBase, m.cfg.SevereDelayMax)
	cloned := profile.clone()
	m.cfg.BackpressureDelayProfile = cloned
	m.profile.Store(cloned)
}

func (m *memoryPendingCoordinator) Acquire(_ context.Context, username string, meta LeaseMetadata) (*AcquireResult, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, errors.New("username required")
	}

	now := time.Now().UTC()
	shard := m.shardFor(trimmed)
	if shard == nil {
		return nil, errors.New("pending coordinator unavailable")
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	m.pruneShardLocked(shard, now)

	if entry, exists := shard.leases[trimmed]; exists {
		m.maybeExpireLocked(entry, now)
		switch entry.state {
		case PendingStateLease:
			return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.activeCount.Load()}
		case PendingStateReleased:
			if m.cfg.ReleaseRetention <= 0 || now.Sub(entry.releasedAt) < m.cfg.ReleaseRetention {
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.activeCount.Load()}
			}
			delete(shard.leases, trimmed)
		case PendingStateExpired:
			expiry := entry.expiredAt
			if expiry.IsZero() && entry.lease != nil {
				expiry = entry.lease.LeaseExpiresAt
			}
			if m.cfg.ExpiredRetention <= 0 || now.Sub(expiry) < m.cfg.ExpiredRetention {
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.activeCount.Load()}
			}
			delete(shard.leases, trimmed)
		default:
			delete(shard.leases, trimmed)
		}
	}

	active := m.activeCount.Load()
	if m.cfg.BackpressureHardLimit > 0 && active >= int64(m.cfg.BackpressureHardLimit) {
		state := &PendingState{QueueDepth: active, Backpressure: BackpressureSevere, State: PendingStateUnknown}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(m.component, "memory_backpressure_reject").Inc()
		}
		return nil, &AcquireError{Reason: AcquireFailureBackpressure, State: state, QueueDepth: active}
	}

	ownerID := uuid.NewString()
	lease := &PendingLease{
		Username:       trimmed,
		OwnerID:        ownerID,
		AcquireAt:      now,
		LeaseExpiresAt: now.Add(m.cfg.LeaseTTL),
		HeartbeatAt:    now,
		QueueDepth:     active + 1,
		Backpressure:   m.classifyBackpressure(active + 1),
		Metadata:       meta,
	}

	entry := &memoryLeaseEntry{lease: lease, metadata: meta, state: PendingStateLease, lastHeartbeat: now}
	shard.leases[trimmed] = entry
	m.activeCount.Add(1)
	m.emitActiveGauge()

	return &AcquireResult{Lease: lease}, nil
}

func (m *memoryPendingCoordinator) Release(_ context.Context, username, ownerID string) (time.Duration, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}

	now := time.Now().UTC()
	shard := m.shardFor(trimmed)
	if shard == nil {
		return 0, nil
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.leases[trimmed]
	if !exists {
		return 0, nil
	}
	if entry.lease != nil && ownerID != "" && entry.lease.OwnerID != "" && entry.lease.OwnerID != ownerID {
		return 0, ErrPendingLeaseOwnerMismatch
	}

	duration := time.Duration(0)
	if entry.lease != nil && !entry.lease.AcquireAt.IsZero() {
		duration = now.Sub(entry.lease.AcquireAt)
		entry.lease.LeaseExpiresAt = now
	}
	if entry.state == PendingStateLease {
		entry.state = PendingStateReleased
		entry.releasedAt = now
		m.activeCount.Add(-1)
	} else {
		entry.state = PendingStateReleased
		entry.releasedAt = now
	}
	entry.releasedAt = now
	m.emitActiveGauge()
	return duration, nil
}

func (m *memoryPendingCoordinator) Heartbeat(_ context.Context, username, ownerID string) error {
	trimmed := strings.TrimSpace(username)
	trimmedOwner := strings.TrimSpace(ownerID)
	if trimmed == "" || trimmedOwner == "" {
		return nil
	}
	now := time.Now().UTC()
	shard := m.shardFor(trimmed)
	if shard == nil {
		return ErrPendingLeaseConflict
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, exists := shard.leases[trimmed]
	if !exists || entry == nil || entry.state != PendingStateLease || entry.lease == nil {
		return ErrPendingLeaseConflict
	}
	if entry.lease.OwnerID != "" && entry.lease.OwnerID != trimmedOwner {
		return ErrPendingLeaseOwnerMismatch
	}
	entry.lastHeartbeat = now
	entry.lease.HeartbeatAt = now
	if m.cfg.LeaseTTL > 0 {
		entry.lease.LeaseExpiresAt = now.Add(m.cfg.LeaseTTL)
	}
	entry.expiredReason = ""
	return nil
}

func (m *memoryPendingCoordinator) Observe(_ context.Context, username string) (*PendingState, error) {
	result := &PendingState{State: PendingStateUnknown}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return result, nil
	}

	now := time.Now().UTC()
	shard := m.shardFor(trimmed)
	if shard == nil {
		return result, nil
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	m.pruneShardLocked(shard, now)
	entry, exists := shard.leases[trimmed]
	if !exists {
		return result, nil
	}

	return m.buildStateLocked(trimmed, entry, now), nil
}

// SampleQueueDepth 返回当前实例的待审批队列深度及对应的背压等级。

func (m *memoryPendingCoordinator) SampleQueueDepth(_ context.Context) (int64, BackpressureLevel, error) {
	now := time.Now().UTC()

	m.pruneAllShards(now)
	depth := m.activeCount.Load()
	level := m.classifyBackpressure(depth)
	m.emitActiveGauge()
	if metrics.PendingLeaseQueueDepth != nil {
		metrics.PendingLeaseQueueDepth.WithLabelValues(m.component).Set(float64(depth))
	}
	if metrics.PendingLeaseBackpressureLevel != nil {
		metrics.PendingLeaseBackpressureLevel.WithLabelValues(m.component).Set(backpressureValue(level))
	}
	return depth, level, nil
}

func (m *memoryPendingCoordinator) SampleUserQueueDepth(_ context.Context, username string) (int64, BackpressureLevel, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, BackpressureNone, nil
	}

	now := time.Now().UTC()
	shard := m.shardFor(trimmed)
	if shard == nil {
		return 0, BackpressureNone, nil
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	m.pruneShardLocked(shard, now)
	entry, exists := shard.leases[trimmed]
	if !exists || entry.state != PendingStateLease {
		return 0, BackpressureNone, nil
	}
	return 1, m.classifyUserBackpressure(1), nil
}

func (m *memoryPendingCoordinator) ListExpired(_ context.Context, limit int) ([]*PendingState, error) {
	now := time.Now().UTC()
	if limit <= 0 {
		limit = 64
	}
	result := make([]*PendingState, 0, limit)
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		m.pruneShardLocked(shard, now)
		for username, entry := range shard.leases {
			if len(result) >= limit {
				break
			}
			if entry.state != PendingStateExpired {
				continue
			}
			result = append(result, m.buildStateLocked(username, entry, now))
		}
		shard.mu.Unlock()
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *memoryPendingCoordinator) pruneAllShards(now time.Time) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		m.pruneShardLocked(shard, now)
		shard.mu.Unlock()
	}
}

// pruneShardLocked 会清理给定分片上已释放和已过期的租约条目，调用方负责持有 shard 的写锁。
func (m *memoryPendingCoordinator) pruneShardLocked(shard *memoryLeaseShard, now time.Time) {
	if shard == nil {
		return
	}
	for username, entry := range shard.leases {
		m.maybeExpireLocked(entry, now)
		switch entry.state {
		case PendingStateReleased:
			if m.cfg.ReleaseRetention > 0 && now.Sub(entry.releasedAt) >= m.cfg.ReleaseRetention {
				delete(shard.leases, username)
			}
		case PendingStateExpired:
			expiry := entry.expiredAt
			if expiry.IsZero() && entry.lease != nil {
				expiry = entry.lease.LeaseExpiresAt
			}
			if m.cfg.ExpiredRetention > 0 && now.Sub(expiry) >= m.cfg.ExpiredRetention {
				delete(shard.leases, username)
			}
		}
	}
}

// maybeExpireLocked 会根据当前时间检查给定的条目是否应标记为已过期。
func (m *memoryPendingCoordinator) maybeExpireLocked(entry *memoryLeaseEntry, now time.Time) {
	if entry == nil || entry.state != PendingStateLease || entry.lease == nil {
		return
	}
	if timeout := m.cfg.HeartbeatTimeout; timeout > 0 {
		last := entry.lastHeartbeat
		if last.IsZero() {
			last = entry.lease.AcquireAt
		}
		if !last.IsZero() && now.Sub(last) >= timeout {
			entry.state = PendingStateExpired
			entry.expiredAt = now
			entry.expiredReason = "heartbeat_timeout"
			m.activeCount.Add(-1)
			m.emitActiveGauge()
			return
		}
	}
	// 核心判定：当前时间 > （租约理论到期时间 + 宽限期）
	if now.After(entry.lease.LeaseExpiresAt.Add(m.cfg.ExpiredGracePeriod)) {
		entry.state = PendingStateExpired
		entry.expiredAt = entry.lease.LeaseExpiresAt
		entry.expiredReason = "lease_timeout"
		m.activeCount.Add(-1)
		m.emitActiveGauge()
	}
}

func (m *memoryPendingCoordinator) emitActiveGauge() {
	if metrics.PendingLeaseActiveGauge == nil {
		return
	}
	metrics.PendingLeaseActiveGauge.WithLabelValues(m.component).Set(float64(m.activeCount.Load()))
}

func (m *memoryPendingCoordinator) classifyBackpressure(depth int64) BackpressureLevel {
	return classifyDepthWithProfile(depth, m.currentProfile(), m.cfg.BackpressureSoftLimit, m.cfg.BackpressureHardLimit)
}

func (m *memoryPendingCoordinator) classifyUserBackpressure(depth int64) BackpressureLevel {
	profile := m.currentProfile()
	return classifyDepthWithProfile(depth, profile, m.cfg.UserBackpressureSoftLimit, m.cfg.UserBackpressureHardLimit)
}

func (m *memoryPendingCoordinator) buildStateLocked(username string, entry *memoryLeaseEntry, now time.Time) *PendingState {
	depth := m.activeCount.Load()
	state := &PendingState{
		Exists:             true,
		Username:           username,
		State:              entry.state,
		QueueDepth:         depth,
		InstanceQueueDepth: depth,
	}

	state.Backpressure = m.classifyBackpressure(state.QueueDepth)
	state.InstanceBackpressure = state.Backpressure
	if entry.state == PendingStateLease {
		state.UserQueueDepth = 1
		state.UserBackpressure = m.classifyUserBackpressure(1)
	}

	if entry.lease != nil {
		state.LeaseOwner = entry.lease.OwnerID
		state.LeaseExpiresAt = entry.lease.LeaseExpiresAt
		if state.LeaseExpiresAt.After(now) {
			state.TTL = state.LeaseExpiresAt.Sub(now)
		}
		if entry.lease.QueueDepth > 0 && state.QueueDepth == 0 {
			state.QueueDepth = entry.lease.QueueDepth
			state.Backpressure = entry.lease.Backpressure
		}
		if !entry.lease.HeartbeatAt.IsZero() {
			state.HeartbeatAt = entry.lease.HeartbeatAt
		}
	}
	if state.HeartbeatAt.IsZero() && !entry.lastHeartbeat.IsZero() {
		state.HeartbeatAt = entry.lastHeartbeat
	}

	if entry.state == PendingStateReleased {
		state.ReleasedAt = entry.releasedAt
	}
	if entry.state == PendingStateExpired {
		state.ExpiredAt = entry.expiredAt
		if entry.expiredReason != "" {
			state.ExpiredReason = entry.expiredReason
		} else {
			state.ExpiredReason = "timeout"
		}
	}
	return state
}
