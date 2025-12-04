package usercache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
)

type memoryPendingCoordinator struct {
	cfg       PendingCoordinatorConfig
	component string
	mu        sync.Mutex
	leases    map[string]*memoryLeaseEntry
}

type memoryLeaseEntry struct {
	lease         *PendingLease
	metadata      LeaseMetadata
	state         PendingStateValue
	releasedAt    time.Time
	expiredAt     time.Time
	expiredReason string
}

func newMemoryPendingCoordinator(cfg PendingCoordinatorConfig, component string) *memoryPendingCoordinator {
	return &memoryPendingCoordinator{
		cfg:       cfg,
		component: component,
		leases:    make(map[string]*memoryLeaseEntry),
	}
}

func (m *memoryPendingCoordinator) Acquire(_ context.Context, username string, meta LeaseMetadata) (*AcquireResult, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, errors.New("username required")
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneLocked(now)

	if entry, exists := m.leases[trimmed]; exists {
		m.maybeExpireLocked(entry, now)
		switch entry.state {
		case PendingStateLease:
			return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.countActiveLocked()}
		case PendingStateReleased:
			if m.cfg.ReleaseRetention <= 0 || now.Sub(entry.releasedAt) < m.cfg.ReleaseRetention {
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.countActiveLocked()}
			}
			delete(m.leases, trimmed)
		case PendingStateExpired:
			expiry := entry.expiredAt
			if expiry.IsZero() && entry.lease != nil {
				expiry = entry.lease.LeaseExpiresAt
			}
			if m.cfg.ExpiredRetention <= 0 || now.Sub(expiry) < m.cfg.ExpiredRetention {
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: m.buildStateLocked(trimmed, entry, now), QueueDepth: m.countActiveLocked()}
			}
			delete(m.leases, trimmed)
		default:
			delete(m.leases, trimmed)
		}
	}

	active := m.countActiveLocked()
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
		QueueDepth:     active + 1,
		Backpressure:   m.classifyBackpressureLocked(active + 1),
		Metadata:       meta,
	}

	entry := &memoryLeaseEntry{lease: lease, metadata: meta, state: PendingStateLease}
	m.leases[trimmed] = entry
	m.emitActiveGaugeLocked()

	return &AcquireResult{Lease: lease}, nil
}

func (m *memoryPendingCoordinator) Release(_ context.Context, username, ownerID string) (time.Duration, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.leases[trimmed]
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
	entry.state = PendingStateReleased
	entry.releasedAt = now
	m.emitActiveGaugeLocked()
	return duration, nil
}

func (m *memoryPendingCoordinator) Observe(_ context.Context, username string) (*PendingState, error) {
	result := &PendingState{State: PendingStateUnknown}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return result, nil
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneLocked(now)
	entry, exists := m.leases[trimmed]
	if !exists {
		return result, nil
	}

	return m.buildStateLocked(trimmed, entry, now), nil
}

func (m *memoryPendingCoordinator) SampleQueueDepth(_ context.Context) (int64, BackpressureLevel, error) {
	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneLocked(now)
	depth := m.countActiveLocked()
	level := m.classifyBackpressureLocked(depth)
	m.emitActiveGaugeLocked()
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

	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneLocked(now)
	entry, exists := m.leases[trimmed]
	if !exists || entry.state != PendingStateLease {
		return 0, BackpressureNone, nil
	}
	return 1, m.classifyUserBackpressureLocked(1), nil
}

func (m *memoryPendingCoordinator) ListExpired(_ context.Context, limit int) ([]*PendingState, error) {
	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneLocked(now)
	if limit <= 0 {
		limit = 64
	}
	result := make([]*PendingState, 0, limit)
	for username, entry := range m.leases {
		if len(result) >= limit {
			break
		}
		if entry.state != PendingStateExpired {
			continue
		}
		result = append(result, m.buildStateLocked(username, entry, now))
	}
	return result, nil
}

func (m *memoryPendingCoordinator) pruneLocked(now time.Time) {
	for username, entry := range m.leases {
		m.maybeExpireLocked(entry, now)
		switch entry.state {
		case PendingStateReleased:
			if m.cfg.ReleaseRetention > 0 && now.Sub(entry.releasedAt) >= m.cfg.ReleaseRetention {
				delete(m.leases, username)
			}
		case PendingStateExpired:
			expiry := entry.expiredAt
			if expiry.IsZero() && entry.lease != nil {
				expiry = entry.lease.LeaseExpiresAt
			}
			if m.cfg.ExpiredRetention > 0 && now.Sub(expiry) >= m.cfg.ExpiredRetention {
				delete(m.leases, username)
			}
		}
	}
}

func (m *memoryPendingCoordinator) maybeExpireLocked(entry *memoryLeaseEntry, now time.Time) {
	if entry == nil || entry.state != PendingStateLease || entry.lease == nil {
		return
	}
	if now.After(entry.lease.LeaseExpiresAt.Add(m.cfg.ExpiredGracePeriod)) {
		entry.state = PendingStateExpired
		entry.expiredAt = entry.lease.LeaseExpiresAt
		if entry.expiredReason == "" {
			entry.expiredReason = "timeout"
		}
	}
}

func (m *memoryPendingCoordinator) countActiveLocked() int64 {
	var count int64
	for _, entry := range m.leases {
		if entry.state == PendingStateLease {
			count++
		}
	}
	return count
}

func (m *memoryPendingCoordinator) emitActiveGaugeLocked() {
	if metrics.PendingLeaseActiveGauge == nil {
		return
	}
	metrics.PendingLeaseActiveGauge.WithLabelValues(m.component).Set(float64(m.countActiveLocked()))
}

func (m *memoryPendingCoordinator) classifyBackpressureLocked(depth int64) BackpressureLevel {
	if m.cfg.BackpressureHardLimit > 0 && depth >= int64(m.cfg.BackpressureHardLimit) {
		return BackpressureSevere
	}
	if m.cfg.BackpressureSoftLimit > 0 && depth >= int64(m.cfg.BackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

func (m *memoryPendingCoordinator) classifyUserBackpressureLocked(depth int64) BackpressureLevel {
	if m.cfg.UserBackpressureHardLimit > 0 && depth >= int64(m.cfg.UserBackpressureHardLimit) {
		return BackpressureSevere
	}
	if m.cfg.UserBackpressureSoftLimit > 0 && depth >= int64(m.cfg.UserBackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

func (m *memoryPendingCoordinator) buildStateLocked(username string, entry *memoryLeaseEntry, now time.Time) *PendingState {
	state := &PendingState{
		Exists:             true,
		Username:           username,
		State:              entry.state,
		QueueDepth:         m.countActiveLocked(),
		InstanceQueueDepth: m.countActiveLocked(),
	}

	state.Backpressure = m.classifyBackpressureLocked(state.QueueDepth)
	state.InstanceBackpressure = state.Backpressure
	if entry.state == PendingStateLease {
		state.UserQueueDepth = 1
		state.UserBackpressure = m.classifyUserBackpressureLocked(1)
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
