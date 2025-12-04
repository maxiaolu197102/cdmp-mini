package redisdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

// Config configures the Redis/DB backed RequestStateStore implementation.
type Config struct {
	Redis     *storage.RedisCluster
	DB        *gorm.DB
	KeyPrefix string
	TTL       time.Duration
	Clock     func() time.Time
}

// Store persists operation state records to Redis for quick lookups and mirrors
// them to the relational database for durability.
type Store struct {
	redis       redisAdapter
	db          *gorm.DB
	keyPrefix   string
	ttl         time.Duration
	now         func() time.Time
	migrateOnce sync.Once
	migrateErr  error
}

// NewStore constructs a Redis/DB backed RequestStateStore.
func NewStore(cfg Config) (*Store, error) {
	if cfg.Redis == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	store := &Store{
		redis:     &clusterAdapter{cluster: cfg.Redis},
		db:        cfg.DB,
		keyPrefix: strings.TrimSuffix(cfg.KeyPrefix, ":"),
		ttl:       ttl,
		now:       clock,
	}
	if cfg.DB != nil {
		if err := store.ensureMigrated(); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// Upsert stores or replaces the operation state using the provided envelope.
func (s *Store) Upsert(ctx context.Context, env *operation.OperationEnvelope, state operation.OperationState) error {
	if env == nil {
		return fmt.Errorf("operation envelope is required")
	}
	if env.ID == "" {
		return fmt.Errorf("operation id is required")
	}

	record := operationStateRecord{
		OperationID: env.ID,
		Kind:        string(env.Kind),
		Resource:    env.Resource,
		State:       string(state),
		Attempts:    0,
		LastError:   "",
		UpdatedAt:   s.now(),
	}
	if err := s.persistRecord(ctx, &record); err != nil {
		return err
	}
	return s.cacheState(ctx, record.toRequestState())
}

// Advance transitions the operation state if it currently matches the expected value.
func (s *Store) Advance(ctx context.Context, operationID string, from, to operation.OperationState) error {
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}
	if err := s.ensureMigrated(); err != nil {
		return err
	}

	now := s.now()
	if s.db != nil {
		updates := map[string]interface{}{
			"state":      string(to),
			"updated_at": now,
		}
		if to != operation.StateFailed {
			updates["last_error"] = ""
		}
		if to == operation.StateQueued {
			updates["attempts"] = 0
		}
		tx := s.db.WithContext(ctx).
			Model(&operationStateRecord{}).
			Where("operation_id = ? AND state = ?", operationID, string(from)).
			Updates(updates)
		if tx.Error != nil {
			return tx.Error
		}
		if tx.RowsAffected == 0 {
			var count int64
			countErr := s.db.WithContext(ctx).
				Model(&operationStateRecord{}).
				Where("operation_id = ?", operationID).
				Count(&count).Error
			if countErr != nil {
				return countErr
			}
			if count == 0 {
				return fmt.Errorf("operation %s not found", operationID)
			}
			return operation.ErrStateConflict
		}
	}

	current, err := s.fetchState(ctx, operationID)
	if err != nil {
		return err
	}
	if current == nil {
		current = &operation.RequestState{OperationID: operationID}
	}
	current.State = to
	if to != operation.StateFailed {
		current.LastError = ""
	}
	if to == operation.StateQueued {
		current.Attempts = 0
	}
	current.UpdatedAt = now.UnixMilli()
	return s.cacheState(ctx, current)
}

// RecordFailure tracks a failed attempt and stores the last error message.
func (s *Store) RecordFailure(ctx context.Context, operationID string, attempt int, reason string) error {
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}
	if err := s.ensureMigrated(); err != nil {
		return err
	}

	now := s.now()
	if s.db != nil {
		tx := s.db.WithContext(ctx).
			Model(&operationStateRecord{}).
			Where("operation_id = ?", operationID).
			Updates(map[string]interface{}{
				"state":      string(operation.StateFailed),
				"attempts":   attempt,
				"last_error": truncateReason(reason),
				"updated_at": now,
			})
		if tx.Error != nil {
			return tx.Error
		}
		if tx.RowsAffected == 0 {
			return fmt.Errorf("operation %s not found", operationID)
		}
	}

	current, err := s.fetchState(ctx, operationID)
	if err != nil {
		return err
	}
	if current == nil {
		current = &operation.RequestState{OperationID: operationID}
	}
	current.State = operation.StateFailed
	current.Attempts = attempt
	current.LastError = reason
	current.UpdatedAt = now.UnixMilli()
	return s.cacheState(ctx, current)
}

// Get retrieves the persisted state for an operation.
func (s *Store) Get(ctx context.Context, operationID string) (*operation.RequestState, error) {
	if operationID == "" {
		return nil, fmt.Errorf("operation id is required")
	}
	if cached, err := s.fetchState(ctx, operationID); err == nil && cached != nil {
		return cached, nil
	} else if err != nil {
		return nil, err
	}

	if s.db == nil {
		return nil, fmt.Errorf("operation %s not found", operationID)
	}

	var record operationStateRecord
	err := s.db.WithContext(ctx).
		Where("operation_id = ?", operationID).
		Take(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("operation %s not found", operationID)
		}
		return nil, err
	}

	state := record.toRequestState()
	if cacheErr := s.cacheState(ctx, state); cacheErr != nil {
		log.Debugw("cache operation state failed", "operation", operationID, "error", cacheErr)
	}
	return state, nil
}

func (s *Store) persistRecord(ctx context.Context, record *operationStateRecord) error {
	if err := s.ensureMigrated(); err != nil {
		return err
	}
	if s.db == nil {
		return nil
	}
	record.CreatedAt = record.UpdatedAt
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "operation_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"kind":       record.Kind,
			"resource":   record.Resource,
			"state":      record.State,
			"attempts":   record.Attempts,
			"last_error": record.LastError,
			"updated_at": record.UpdatedAt,
		}),
	}).Create(record).Error
}

type redisAdapter interface {
	GetKey(ctx context.Context, key string) (string, error)
	SetKey(ctx context.Context, key, value string, ttl time.Duration) error
}

type clusterAdapter struct {
	cluster *storage.RedisCluster
}

func (c *clusterAdapter) GetKey(ctx context.Context, key string) (string, error) {
	return c.cluster.GetKey(ctx, key)
}

func (c *clusterAdapter) SetKey(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.cluster.SetKey(ctx, key, value, ttl)
}

func (s *Store) fetchState(ctx context.Context, operationID string) (*operation.RequestState, error) {
	key := s.persistentKey(operationID)
	raw, err := s.redis.GetKey(ctx, key)
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, storage.ErrKeyNotFound) {
			return nil, nil
		}
		if errors.Is(err, storage.ErrRedisIsDown) {
			return nil, nil
		}
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var state operation.RequestState
	if decodeErr := json.Unmarshal([]byte(raw), &state); decodeErr != nil {
		return nil, decodeErr
	}
	return &state, nil
}

func (s *Store) cacheState(ctx context.Context, state *operation.RequestState) error {
	if state == nil {
		return nil
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.redis.SetKey(ctx, s.persistentKey(state.OperationID), string(payload), s.ttl)
}

func (s *Store) persistentKey(operationID string) string {
	if s.keyPrefix == "" {
		return fmt.Sprintf("operation_state:%s", operationID)
	}
	return fmt.Sprintf("%s:operation_state:%s", s.keyPrefix, operationID)
}

func (s *Store) ensureMigrated() error {
	if s.db == nil {
		return nil
	}
	s.migrateOnce.Do(func() {
		s.migrateErr = s.db.AutoMigrate(&operationStateRecord{})
	})
	return s.migrateErr
}

func truncateReason(reason string) string {
	const limit = 2048
	if len(reason) <= limit {
		return reason
	}
	return reason[:limit]
}

type operationStateRecord struct {
	OperationID string    `gorm:"column:operation_id;primaryKey"`
	Kind        string    `gorm:"column:kind"`
	Resource    string    `gorm:"column:resource"`
	State       string    `gorm:"column:state"`
	Attempts    int       `gorm:"column:attempts"`
	LastError   string    `gorm:"column:last_error"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

func (operationStateRecord) TableName() string {
	return "operation_states"
}

func (r operationStateRecord) toRequestState() *operation.RequestState {
	return &operation.RequestState{
		OperationID: r.OperationID,
		Kind:        operation.OperationKind(r.Kind),
		Resource:    r.Resource,
		State:       operation.OperationState(r.State),
		Attempts:    r.Attempts,
		LastError:   r.LastError,
		UpdatedAt:   r.UpdatedAt.UnixMilli(),
	}
}

var _ operation.RequestStateStore = (*Store)(nil)
