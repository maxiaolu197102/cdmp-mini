package operation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
)

// Pipeline orchestrates the lifecycle of queued CRUD operations. A Pipeline
// instance is typically created per resource (e.g. users) with its dedicated
// executor implementation.
type Pipeline struct {
	queue QueueCoordinator
	state RequestStateStore
	exec  OperationExecutor
	opts  pipelineOptions
}

type pipelineOptions struct {
	maxAttempts  int
	retryBackoff func(attempt int) time.Duration
}

const (
	headerOperationPhase = "operation-phase"
	phaseExecute         = "execute"
	phaseCompensate      = "compensate"
)

// PipelineConfig configures a new Pipeline instance. The QueueCoordinator and
// RequestStateStore are mandatory; options may be nil to use defaults.
type PipelineConfig struct {
	QueueCoordinator QueueCoordinator
	StateStore       RequestStateStore
	Executor         OperationExecutor
	MaxAttempts      int
	RetryBackoff     func(attempt int) time.Duration
}

// NewPipeline builds a new operation pipeline using the provided configuration.
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	if cfg.QueueCoordinator == nil {
		return nil, fmt.Errorf("queue coordinator is required")
	}
	if cfg.StateStore == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if cfg.Executor == nil {
		return nil, fmt.Errorf("operation executor is required")
	}

	opts := pipelineOptions{
		maxAttempts:  defaultMaxAttempts(cfg.MaxAttempts),
		retryBackoff: defaultRetryBackoff(cfg.RetryBackoff),
	}

	return &Pipeline{
		queue: cfg.QueueCoordinator,
		state: cfg.StateStore,
		exec:  cfg.Executor,
		opts:  opts,
	}, nil
}

// Submit registers an operation for asynchronous execution. The envelope will
// be assigned an ID and submission timestamp if they are not already set.
func (p *Pipeline) Submit(ctx context.Context, env *OperationEnvelope) (*QueueTicket, error) {
	if env == nil {
		return nil, fmt.Errorf("operation envelope is required")
	}
	if strings.TrimSpace(env.ID) == "" {
		env.ID = idutil.GetUUID36("")
	}
	if env.SubmittedAt.IsZero() {
		env.SubmittedAt = time.Now()
	}
	if env.Headers == nil {
		env.Headers = make(map[string]string)
	}
	markStateStoreWrite(ctx)

	stateStart := time.Now()
	if err := p.state.Upsert(ctx, env, StateQueued); err != nil {
		fmt.Printf("Submit Upsert duration=%v\n", time.Since(stateStart))
		return nil, fmt.Errorf("persist queued state: %w", err)
	}
	fmt.Printf("Submit Upsert duration=%v\n", time.Since(stateStart))

	queueStart := time.Now()
	ticket, err := p.queue.Enqueue(ctx, env)
	if err != nil {
		fmt.Printf("Submit Enqueue duration=%v\n", time.Since(queueStart))
		return nil, fmt.Errorf("enqueue operation: %w", err)
	}
	fmt.Printf("Submit Enqueue duration=%v\n", time.Since(queueStart))

	return ticket, nil
}

// ProcessOnce consumes a single operation from the queue and drives it through
// execute/compensate paths. Callers typically run this inside worker goroutines.
func (p *Pipeline) ProcessOnce(ctx context.Context) (err error) {
	item, err := p.queue.Dequeue(ctx)
	if err != nil {
		return err
	}
	if item == nil || item.Envelope == nil {
		return fmt.Errorf("dequeue returned empty item")
	}

	workerID := WorkerIDFromContext(ctx)
	opts := trace.Options{
		TraceID:   item.Envelope.TraceID,
		Service:   "operation-pipeline",
		Component: "operation-worker",
		Operation: string(item.Envelope.Kind),
		Phase:     trace.PhaseAsync,
		RequestID: item.Envelope.Headers["requestID"],
		Path:      item.Envelope.Resource,
	}
	if startedCtx, _ := trace.Start(ctx, opts); startedCtx != nil {
		ctx = startedCtx
	}
	defer func() {
		status := "success"
		codeStr := "0"
		message := "operation processed"
		if err != nil {
			status = "error"
			codeStr = err.Error()
			message = err.Error()
		}
		trace.RecordOutcome(ctx, codeStr, message, status, 0)
		trace.Complete(ctx)
	}()

	workerCtx, span := trace.StartSpan(ctx, "operation-pipeline", "operation_worker")
	if workerCtx != nil {
		ctx = workerCtx
		trace.AddRequestTag(ctx, "operation_id", item.Envelope.ID)
		trace.AddRequestTag(ctx, "worker_id", workerID)
		trace.AddRequestTag(ctx, "attempt", item.Attempts+1)
		trace.AddRequestTag(ctx, "operation_kind", string(item.Envelope.Kind))
		trace.AddRequestTag(ctx, "operation_resource", item.Envelope.Resource)
	}
	defer func() {
		if span != nil {
			status := "success"
			codeStr := "0"
			if err != nil {
				status = "error"
				codeStr = err.Error()
			}
			trace.EndSpan(span, status, codeStr, map[string]interface{}{
				"operation_id":       item.Envelope.ID,
				"worker_id":          workerID,
				"attempt":            item.Attempts + 1,
				"operation_kind":     string(item.Envelope.Kind),
				"operation_resource": item.Envelope.Resource,
			})
		}
	}()

	switch operationPhase(item.Envelope) {
	case phaseCompensate:
		return p.processCompensation(ctx, item)
	default:
		return p.processExecution(ctx, item)
	}
}

func (p *Pipeline) processExecution(ctx context.Context, item *QueueItem) error {
	env := item.Envelope
	attempt := item.Attempts + 1

	if err := p.advanceToExecuting(ctx, env.ID); err != nil {
		_ = p.queue.Ack(ctx, env.ID)
		return fmt.Errorf("advance to executing: %w", err)
	}

	if err := p.exec.Prepare(ctx, env); err != nil {
		p.handleFailure(ctx, item, attempt, fmt.Errorf("prepare: %w", err))
		return err
	}

	res, execErr := p.exec.Execute(ctx, env)
	if execErr != nil {
		if res == nil {
			res = &OperationResult{OperationID: env.ID, State: StateFailed, Error: execErr}
		} else if res.Error == nil {
			res.Error = execErr
		}
	}

	if res == nil {
		res = &OperationResult{OperationID: env.ID, State: StateFailed, Error: fmt.Errorf("executor returned nil result")}
	}
	res.Attempt = attempt

	if res.Error == nil && res.State == StateCompleted {
		markStateStoreWrite(ctx)
		if err := p.state.Advance(ctx, env.ID, StateExecuting, StateCompleted); err != nil {
			log.Warnw("advance completed failed", "operation", env.ID, "error", err)
		}
		if err := p.queue.Ack(ctx, env.ID); err != nil {
			return fmt.Errorf("ack queue: %w", err)
		}
		return nil
	}

	return p.handleExecutionResult(ctx, item, res)
}

func (p *Pipeline) processCompensation(ctx context.Context, item *QueueItem) error {
	env := item.Envelope
	attempt := item.Attempts + 1

	res, execErr := p.exec.Compensate(ctx, env)
	if execErr != nil {
		if res == nil {
			res = &OperationResult{OperationID: env.ID, State: StateFailed, Error: execErr}
		} else if res.Error == nil {
			res.Error = execErr
		}
	}

	if res == nil {
		res = &OperationResult{OperationID: env.ID, State: StateFailed, Error: fmt.Errorf("compensator returned nil result")}
	}
	res.Attempt = attempt

	if res.Error == nil && res.State == StateCompensated {
		markStateStoreWrite(ctx)
		if err := p.state.Advance(ctx, env.ID, StateCompensating, StateCompensated); err != nil {
			log.Warnw("advance compensated failed", "operation", env.ID, "error", err)
		}
		if err := p.queue.Ack(ctx, env.ID); err != nil {
			return fmt.Errorf("ack queue: %w", err)
		}
		return nil
	}

	if res.Error != nil {
		log.Warnw("compensation attempt failed", "operation", env.ID, "attempt", attempt, "error", res.Error)
	} else {
		log.Warnw("compensation returned non-success state", "operation", env.ID, "attempt", attempt, "state", res.State)
	}

	if res.Fatal || attempt >= p.opts.maxAttempts {
		markStateStoreWrite(ctx)
		if err := p.state.Advance(ctx, env.ID, StateCompensating, StateFailed); err != nil && !errors.Is(err, ErrStateConflict) {
			log.Warnw("mark compensation failed", "operation", env.ID, "error", err)
		}
		if err := p.queue.Ack(ctx, env.ID); err != nil {
			return fmt.Errorf("ack queue: %w", err)
		}
		if res.Error != nil {
			return res.Error
		}
		return fmt.Errorf("operation %s compensation failed", env.ID)
	}

	delay := res.RetryAfter
	if delay <= 0 {
		delay = p.opts.retryBackoff(attempt)
	}
	if delay < 0 {
		delay = 0
	}

	item.Attempts = attempt
	item.AvailableAt = time.Now().Add(delay)
	if err := p.queue.Requeue(ctx, item, delay); err != nil {
		return fmt.Errorf("requeue compensation: %w", err)
	}
	if res.Error != nil {
		return res.Error
	}
	return fmt.Errorf("operation %s compensation pending retry", env.ID)
}

func (p *Pipeline) scheduleCompensation(ctx context.Context, env *OperationEnvelope) error {
	cloned := cloneEnvelope(env)
	if cloned == nil {
		return fmt.Errorf("operation envelope is nil")
	}
	if cloned.Headers == nil {
		cloned.Headers = make(map[string]string)
	}
	cloned.Headers[headerOperationPhase] = phaseCompensate
	cloned.SubmittedAt = time.Now()

	queueItem := &QueueItem{
		Envelope:    cloned,
		Attempts:    0,
		AvailableAt: time.Now(),
	}
	return p.queue.Requeue(ctx, queueItem, 0)
}

func (p *Pipeline) handleExecutionResult(ctx context.Context, item *QueueItem, res *OperationResult) error {
	reason := "execution failed"
	if res.Error != nil {
		reason = res.Error.Error()
	}
	if err := p.handleFailure(ctx, item, res.Attempt, errors.New(reason)); err != nil {
		return err
	}

	// Fatal errors do not requeue.
	if res.Fatal || res.Attempt >= p.opts.maxAttempts {
		markStateStoreWrite(ctx)
		if err := p.state.Advance(ctx, res.OperationID, StateExecuting, StateFailed); err != nil {
			log.Warnw("mark failed state", "operation", res.OperationID, "error", err)
		}
		if res.TriggerCompensation {
			if err := p.state.Advance(ctx, res.OperationID, StateFailed, StateCompensating); err != nil {
				log.Warnw("mark compensating", "operation", res.OperationID, "error", err)
			} else if schedErr := p.scheduleCompensation(ctx, item.Envelope); schedErr != nil {
				log.Warnw("schedule compensation failed", "operation", res.OperationID, "error", schedErr)
			}
		}
		if err := p.queue.Ack(ctx, res.OperationID); err != nil {
			return fmt.Errorf("ack queue: %w", err)
		}
		if res.Error != nil {
			return res.Error
		}
		return fmt.Errorf("operation %s failed", res.OperationID)
	}

	delay := res.RetryAfter
	if delay <= 0 {
		delay = p.opts.retryBackoff(res.Attempt)
	}
	if delay < 0 {
		delay = 0
	}

	markStateStoreWrite(ctx)
	if err := p.state.Advance(ctx, res.OperationID, StateFailed, StateQueued); err != nil && !errors.Is(err, ErrStateConflict) {
		log.Warnw("return to queued state", "operation", res.OperationID, "error", err)
	}

	item.Attempts = res.Attempt
	item.AvailableAt = time.Now().Add(delay)
	if err := p.queue.Requeue(ctx, item, delay); err != nil {
		return fmt.Errorf("requeue operation: %w", err)
	}
	if res.Error != nil {
		return res.Error
	}
	return fmt.Errorf("operation %s failed", res.OperationID)
}

func (p *Pipeline) handleFailure(ctx context.Context, item *QueueItem, attempt int, err error) error {
	if err == nil {
		return nil
	}
	reason := err.Error()
	if reason == "" {
		reason = "unknown error"
	}
	markStateStoreWrite(ctx)
	if recordErr := p.state.RecordFailure(ctx, item.Envelope.ID, attempt, reason); recordErr != nil {
		log.Warnw("record failure", "operation", item.Envelope.ID, "error", recordErr)
	}
	return nil
}

func (p *Pipeline) advanceToExecuting(ctx context.Context, operationID string) error {
	markStateStoreWrite(ctx)
	if err := p.state.Advance(ctx, operationID, StateQueued, StateExecuting); err != nil {
		if errors.Is(err, ErrStateConflict) {
			markStateStoreWrite(ctx)
			return p.state.Advance(ctx, operationID, StateFailed, StateExecuting)
		}
		return err
	}
	return nil
}

func cloneEnvelope(env *OperationEnvelope) *OperationEnvelope {
	if env == nil {
		return nil
	}
	cloned := *env
	if env.Headers != nil {
		headers := make(map[string]string, len(env.Headers))
		for k, v := range env.Headers {
			headers[k] = v
		}
		cloned.Headers = headers
	}
	if env.Payload != nil {
		cloned.Payload = append([]byte(nil), env.Payload...)
	}
	if env.CompensateHint != nil {
		cloned.CompensateHint = append([]byte(nil), env.CompensateHint...)
	}
	return &cloned
}

func operationPhase(env *OperationEnvelope) string {
	if env == nil || env.Headers == nil {
		return phaseExecute
	}
	if phase := env.Headers[headerOperationPhase]; phase != "" {
		return phase
	}
	return phaseExecute
}

func defaultMaxAttempts(v int) int {
	if v <= 0 {
		return 3
	}
	return v
}

func defaultRetryBackoff(custom func(int) time.Duration) func(int) time.Duration {
	if custom != nil {
		return custom
	}
	return func(attempt int) time.Duration {
		if attempt <= 1 {
			return 250 * time.Millisecond
		}
		delay := time.Duration(1<<uint(min(attempt-1, 6))) * 250 * time.Millisecond
		if delay > 30*time.Second {
			return 30 * time.Second
		}
		return delay
	}
}

func markStateStoreWrite(ctx context.Context) {
	trace.AddRequestTag(ctx, "request_state_store_write", true)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
