package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
)

// Store is a simple in-memory RequestStateStore implementation intended for
// unit tests and local development.
type Store struct {
	mu     sync.RWMutex
	states map[string]*operation.RequestState
}

// NewStore constructs an empty in-memory state store.
func NewStore() *Store {
	return &Store{states: make(map[string]*operation.RequestState)}
}

// Upsert stores or replaces the operation state using the provided envelope.
func (s *Store) Upsert(_ context.Context, env *operation.OperationEnvelope, state operation.OperationState) error {
	if env == nil {
		return fmt.Errorf("operation envelope is required")
	}
	if env.ID == "" {
		return fmt.Errorf("operation id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	if existing, ok := s.states[env.ID]; ok {
		existing.Kind = env.Kind
		existing.Resource = env.Resource
		existing.State = state
		if state != operation.StateFailed {
			existing.LastError = ""
		}
		if state == operation.StateQueued {
			existing.Attempts = 0
		}
		existing.UpdatedAt = now
		return nil
	}

	s.states[env.ID] = &operation.RequestState{
		OperationID: env.ID,
		Kind:        env.Kind,
		Resource:    env.Resource,
		State:       state,
		Attempts:    0,
		LastError:   "",
		UpdatedAt:   now,
	}
	return nil
}

// Advance transitions the operation state if it currently matches the expected value.
func (s *Store) Advance(_ context.Context, operationID string, from, to operation.OperationState) error {
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.states[operationID]
	if !ok {
		return fmt.Errorf("operation %s not found", operationID)
	}
	if current.State != from {
		return operation.ErrStateConflict
	}

	current.State = to
	if to != operation.StateFailed {
		current.LastError = ""
	}
	if to == operation.StateQueued {
		current.Attempts = 0
	}
	current.UpdatedAt = time.Now().UnixMilli()
	return nil
}

// RecordFailure tracks a failed attempt and stores the last error message.
func (s *Store) RecordFailure(_ context.Context, operationID string, attempt int, reason string) error {
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[operationID]
	if !ok {
		return fmt.Errorf("operation %s not found", operationID)
	}

	state.State = operation.StateFailed
	state.Attempts = attempt
	state.LastError = reason
	state.UpdatedAt = time.Now().UnixMilli()
	return nil
}

// Get retrieves the persisted state for an operation.
func (s *Store) Get(_ context.Context, operationID string) (*operation.RequestState, error) {
	if operationID == "" {
		return nil, fmt.Errorf("operation id is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.states[operationID]
	if !ok {
		return nil, fmt.Errorf("operation %s not found", operationID)
	}

	copy := *state
	return &copy, nil
}

var _ operation.RequestStateStore = (*Store)(nil)
