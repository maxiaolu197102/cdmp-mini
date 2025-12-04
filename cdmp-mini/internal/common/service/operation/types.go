package operation

import (
	"errors"
	"time"
)

// OperationKind enumerates CRUD-style actions supported by the async pipeline.
// These values map directly to Kafka topics, queue buckets, and compensator logic.
type OperationKind string

const (
	OperationCreate OperationKind = "create"
	OperationUpdate OperationKind = "update"
	OperationDelete OperationKind = "delete"
	OperationBatch  OperationKind = "batch"
	OperationQuery  OperationKind = "query"
)

// OperationState captures the lifecycle state of a request within the async system.
// Transitions are enforced by RequestStateStore implementations.
type OperationState string

const (
	StateQueued       OperationState = "queued"
	StateExecuting    OperationState = "executing"
	StateCompleted    OperationState = "completed"
	StateFailed       OperationState = "failed"
	StateCompensating OperationState = "compensating"
	StateCompensated  OperationState = "compensated"
)

// OperationEnvelope is the canonical representation of a CRUD request while it travels
// through the queue, execution, retry, and compensation stages.
type OperationEnvelope struct {
	ID             string            // globally unique operation identifier (UUID / ULID)
	Kind           OperationKind     // CRUD action being performed
	Resource       string            // logical resource name, e.g. "users"
	Tenant         string            // optional tenant or namespace segregation key
	TraceID        string            // distributed trace correlation id
	IdempotencyKey string            // stable dedupe token provided by caller
	Headers        map[string]string // loose key/value metadata propagated end to end
	SubmittedAt    time.Time         // client-visible enqueue timestamp
	Deadline       time.Time         // optional hard deadline for queue/execution
	Payload        []byte            // serialized business payload (JSON/Proto/etc)
	CompensateHint []byte            // optional serialized hint for reverse action
}

// OperationResult expresses the outcome of executing or compensating a request.
type OperationResult struct {
	OperationID         string // mirrors OperationEnvelope.ID
	State               OperationState
	CompletedAt         time.Time
	Attempt             int
	Error               error
	RetryAfter          time.Duration // >0 indicates the request should be requeued after the delay
	Fatal               bool          // true means no further retries should be attempted
	TriggerCompensation bool          // request compensation processing when true
}

// QueueTicket is returned to the caller after enqueueing a request. It allows
// position tracking, cancellation, or fast requeue when clients reconnect.
type QueueTicket struct {
	TicketID        string
	OperationID     string
	QueuePosition   int64
	EstimatedWait   time.Duration
	IssuedAt        time.Time
	Degraded        bool
	BackpressureTag string
}

// QueueStatus provides lightweight polling information without exposing the
// entire OperationState record.
type QueueStatus struct {
	OperationID string
	State       OperationState
	Position    int64
	Remaining   time.Duration
	Message     string
}

// QueueItem wraps an operation while it resides inside a coordinator's
// internal queue. Attempts tracks how many times the item has been dequeued.
type QueueItem struct {
	Envelope    *OperationEnvelope
	Attempts    int
	AvailableAt time.Time
}

// ErrQueueEmpty indicates that no items are currently ready for processing.
var ErrQueueEmpty = errors.New("operation queue empty")

// ErrStateConflict indicates the persisted state did not match the expected
// value during a state transition.
var ErrStateConflict = errors.New("operation state conflict")
