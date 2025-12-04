package operation

import (
	"context"
	"time"
)

// QueueCoordinator manages the lifecycle of queued operations before they are
// dispatched to workers. Implementations should be safe for concurrent use and
// may coordinate with Redis or other distributed stores.
type QueueCoordinator interface {
	Enqueue(ctx context.Context, env *OperationEnvelope) (*QueueTicket, error)
	Poll(ctx context.Context, ticketID string) (*QueueStatus, error)
	Cancel(ctx context.Context, ticketID string) error
	Dequeue(ctx context.Context) (*QueueItem, error)
	Ack(ctx context.Context, operationID string) error
	Requeue(ctx context.Context, item *QueueItem, delay time.Duration) error
}

// RequestStateStore persists the canonical state machine for each operation so
// that API queries, compensators, and reconciliation jobs share a single source
// of truth.
type RequestStateStore interface {
	Upsert(ctx context.Context, env *OperationEnvelope, state OperationState) error
	Advance(ctx context.Context, operationID string, from, to OperationState) error
	RecordFailure(ctx context.Context, operationID string, attempt int, reason string) error
	Get(ctx context.Context, operationID string) (*RequestState, error)
}

// OperationExecutor encapsulates business logic for a specific CRUD action.
// Each resource can provide its own implementation and reuse the generic queue
// orchestration.
type OperationExecutor interface {
	Prepare(ctx context.Context, env *OperationEnvelope) error
	Execute(ctx context.Context, env *OperationEnvelope) (*OperationResult, error)
	Compensate(ctx context.Context, env *OperationEnvelope) (*OperationResult, error)
}

// RequestState represents the persisted status record returned by the state store.
type RequestState struct {
	OperationID string
	Kind        OperationKind
	Resource    string
	State       OperationState
	Attempts    int
	LastError   string
	UpdatedAt   int64 // unix millis for compact storage across Redis/JSON
}
