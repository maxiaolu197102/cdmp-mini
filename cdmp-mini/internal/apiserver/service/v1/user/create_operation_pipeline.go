package user

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdjson "encoding/json"
	stdErrors "errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation/queue/memory"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation/queue/redisqueue"
	memorystate "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation/state"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation/state/redisdb"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

const (
	pendingOwnerHeader          = "user.pending.owner"
	pendingBackendHeader        = "user.pending.backend"
	defaultOperationWorkerCount = 4
	maxOperationWorkerCount     = 16
	queueIdleBackoff            = 150 * time.Millisecond
	queueErrorBackoff           = 500 * time.Millisecond
	operationResourceUsers      = "users"
)

type userOperationExecutor struct {
	service   *UserService
	mu        sync.RWMutex
	executors map[operation.OperationKind]operation.OperationExecutor
}

func newUserOperationExecutor(service *UserService) *userOperationExecutor {
	return &userOperationExecutor{
		service:   service,
		executors: make(map[operation.OperationKind]operation.OperationExecutor),
	}
}

func (e *userOperationExecutor) Register(kind operation.OperationKind, exec operation.OperationExecutor) error {
	if exec == nil {
		return fmt.Errorf("operation executor is nil for kind %q", kind)
	}
	if kind == "" {
		kind = operation.OperationCreate
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.executors == nil {
		e.executors = make(map[operation.OperationKind]operation.OperationExecutor)
	}
	if _, exists := e.executors[kind]; exists {
		return nil
	}
	e.executors[kind] = exec
	return nil
}

func (e *userOperationExecutor) executorFor(kind operation.OperationKind) (operation.OperationExecutor, error) {
	if kind == "" {
		kind = operation.OperationCreate
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if exec := e.executors[kind]; exec != nil {
		return exec, nil
	}
	return nil, fmt.Errorf("operation executor for kind %q not registered", kind)
}

func (e *userOperationExecutor) resolveExecutor(env *operation.OperationEnvelope) (operation.OperationExecutor, error) {
	if env == nil {
		return nil, fmt.Errorf("operation envelope is nil")
	}
	return e.executorFor(env.Kind)
}

func (e *userOperationExecutor) Prepare(ctx context.Context, env *operation.OperationEnvelope) error {
	exec, err := e.resolveExecutor(env)
	if err != nil {
		return err
	}
	return exec.Prepare(ctx, env)
}

func (e *userOperationExecutor) Execute(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	exec, err := e.resolveExecutor(env)
	if err != nil {
		opID := ""
		if env != nil {
			opID = env.ID
		}
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true, Error: err}, err
	}
	return exec.Execute(ctx, env)
}

func (e *userOperationExecutor) Compensate(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	exec, err := e.resolveExecutor(env)
	if err != nil {
		opID := ""
		if env != nil {
			opID = env.ID
		}
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true, Error: err}, err
	}
	return exec.Compensate(ctx, env)
}

type operationEnvelopeKey struct{}

func withOperationEnvelope(ctx context.Context, env *operation.OperationEnvelope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if env == nil {
		return ctx
	}
	return context.WithValue(ctx, operationEnvelopeKey{}, env)
}

func operationEnvelopeFromContext(ctx context.Context) (*operation.OperationEnvelope, bool) {
	if ctx == nil {
		return nil, false
	}
	env, ok := ctx.Value(operationEnvelopeKey{}).(*operation.OperationEnvelope)
	return env, ok
}

func (u *UserService) ensureOperationPipeline() error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}

	var initErr error
	u.operationPipelineOnce.Do(func() {
		u.initCreatePipeline()

		queue := u.operationQueue
		if queue == nil {
			queue = u.buildQueueCoordinator()
		}

		stateStore := u.operationStateStore
		if stateStore == nil {
			stateStore = u.buildRequestStateStore()
		}
		if stateStore == nil {
			initErr = fmt.Errorf("state store unavailable")
			return
		}

		execRegistry := newUserOperationExecutor(u)
		if regErr := execRegistry.Register(operation.OperationCreate, &userCreateOperationExecutor{service: u}); regErr != nil {
			initErr = regErr
			return
		}
		if regErr := execRegistry.Register(operation.OperationUpdate, &userUpdateOperationExecutor{service: u}); regErr != nil {
			initErr = regErr
			return
		}
		if regErr := execRegistry.Register(operation.OperationDelete, &userDeleteOperationExecutor{service: u}); regErr != nil {
			initErr = regErr
			return
		}
		if regErr := execRegistry.Register(operation.OperationBatch, &userBatchPatchOperationExecutor{service: u}); regErr != nil {
			initErr = regErr
			return
		}

		pipeline, err := operation.NewPipeline(operation.PipelineConfig{
			QueueCoordinator: queue,
			StateStore:       stateStore,
			Executor:         execRegistry,
			MaxAttempts:      3,
		})
		if err != nil {
			initErr = err
			return
		}

		u.operationQueue = queue
		u.operationStateStore = stateStore
		u.operationExecutor = execRegistry
		u.operationPipeline = pipeline

		if workerErr := u.startOperationWorkers(); workerErr != nil {
			initErr = workerErr
		}
	})

	if initErr != nil {
		return initErr
	}
	if u.operationExecutor == nil {
		return fmt.Errorf("operation executor registry not initialized")
	}
	return nil
}

func (u *UserService) registerOperationExecutor(kind operation.OperationKind, exec operation.OperationExecutor) error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}
	if exec == nil {
		return fmt.Errorf("operation executor is nil")
	}
	if err := u.ensureOperationPipeline(); err != nil {
		return err
	}
	return u.operationExecutor.Register(kind, exec)
}

func (u *UserService) startOperationWorkers() error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}
	if u.operationPipeline == nil {
		return fmt.Errorf("operation pipeline not initialized")
	}
	if u.operationWorkersCancel != nil {
		return nil
	}

	workerCount := deriveOperationWorkerCount()
	ctx, cancel := context.WithCancel(context.Background())
	u.operationWorkersCancel = cancel
	u.operationWorkersWG.Add(workerCount)

	fallbackActive := false
	if _, ok := u.operationQueue.(*memory.Coordinator); ok {
		fallbackActive = true
	}
	metrics.SetOperationQueueFallback(operationResourceUsers, fallbackActive)

	for i := 0; i < workerCount; i++ {
		workerID := i + 1
		workerCtx := operation.ContextWithWorkerID(ctx, workerID)
		go u.runOperationWorker(workerCtx, workerID)
	}

	log.Infow("user operation workers started", "component", "user_service", "workers", workerCount)
	return nil
}

func (u *UserService) processOperationInlineWithBudget(ctx context.Context, budget time.Duration) error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}
	if budget <= 0 {
		return nil
	}
	if u.operationPipeline == nil {
		return fmt.Errorf("operation pipeline not initialized")
	}

	inlineCtx, cancel := context.WithCancelCause(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- u.operationPipeline.ProcessOnce(inlineCtx)
	}()

	timer := time.NewTimer(budget)
	defer func() {
		cancel(nil)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case err := <-resultCh:
		if err == nil {
			return nil
		}
		if stdErrors.Is(err, context.DeadlineExceeded) || stdErrors.Is(err, context.Canceled) {
			return nil
		}
		if stdErrors.Is(err, operation.ErrQueueEmpty) {
			return nil
		}
		return err
	case <-ctx.Done():
		cancel(ctx.Err())
		return nil
	case <-timer.C:
		cancel(context.DeadlineExceeded)
		return nil
	}
}

func deriveOperationWorkerCount() int {
	cpu := runtime.NumCPU()
	if cpu <= 0 {
		return defaultOperationWorkerCount
	}
	count := cpu / 2
	if count < 1 {
		count = 1
	}
	if count > maxOperationWorkerCount {
		count = maxOperationWorkerCount
	}
	return count
}

func (u *UserService) runOperationWorker(ctx context.Context, id int) {
	defer u.operationWorkersWG.Done()

	for {
		select {
		case <-ctx.Done():
			metrics.RecordOperationWorkerIteration(operationResourceUsers, "cancelled", 0)
			log.Debugw("user operation worker exiting", "component", "user_service", "worker", id, "reason", ctx.Err())
			return
		default:
		}

		iterationStart := time.Now()
		err := u.operationPipeline.ProcessOnce(ctx)
		duration := time.Since(iterationStart)

		if err == nil {
			metrics.RecordOperationWorkerIteration(operationResourceUsers, "success", duration)
			continue
		}

		if stdErrors.Is(err, operation.ErrQueueEmpty) {
			metrics.RecordOperationWorkerIteration(operationResourceUsers, "empty", duration)
			if !sleepWithContext(ctx, queueIdleBackoff) {
				log.Debugw("operation worker idle exit", "component", "user_service", "worker", id)
				return
			}
			continue
		}

		metrics.RecordOperationWorkerIteration(operationResourceUsers, "error", duration)
		log.Warnw("process user operation failed", "component", "user_service", "worker", id, "error", err)
		if !sleepWithContext(ctx, queueErrorBackoff) {
			log.Debugw("operation worker error exit", "component", "user_service", "worker", id)
			return
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (u *UserService) buildRequestStateStore() operation.RequestStateStore {
	if u == nil {
		return memorystate.NewStore()
	}

	var writeDB *gorm.DB
	if factory, ok := u.Store.(interface{ WriteDB() *gorm.DB }); ok {
		writeDB = factory.WriteDB()
	}

	if u.Redis != nil {
		store, err := redisdb.NewStore(redisdb.Config{
			Redis:     u.Redis,
			DB:        writeDB,
			KeyPrefix: "user",
			TTL:       24 * time.Hour,
		})
		if err != nil {
			log.Warnw("init redis request state store failed", "error", err)
		} else {
			return store
		}
	}

	return memorystate.NewStore()
}

func (u *UserService) buildQueueCoordinator() operation.QueueCoordinator {
	if u == nil {
		return memory.NewCoordinator()
	}

	if u.Redis != nil {
		coord, err := redisqueue.NewCoordinator(redisqueue.Config{
			Redis:               u.Redis,
			KeyPrefix:           "user",
			TicketTTL:           24 * time.Hour,
			PayloadTTL:          48 * time.Hour,
			Clock:               time.Now,
			MaxInflightDuration: 30 * time.Second,
			ReclaimBatchSize:    32,
		})
		if err != nil {
			log.Warnw("init redis queue coordinator failed", "error", err)
		} else {
			return coord
		}
	}

	return memory.NewCoordinator()
}

func (u *UserService) buildOperationEnvelope(ctx context.Context, kind operation.OperationKind, operationID, idempotencyKey string, payload interface{}, headers map[string]string) (*operation.OperationEnvelope, error) {
	trimmedID := strings.TrimSpace(operationID)
	if trimmedID == "" {
		return nil, fmt.Errorf("operation id is empty")
	}
	if payload == nil {
		return nil, fmt.Errorf("operation payload is nil")
	}
	if kind == "" {
		kind = operation.OperationCreate
	}

	body, err := stdjson.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode operation payload: %w", err)
	}

	env := &operation.OperationEnvelope{
		ID:          trimmedID,
		Kind:        kind,
		Resource:    operationResourceUsers,
		Headers:     make(map[string]string),
		Payload:     body,
		TraceID:     "",
		Tenant:      "",
		Deadline:    time.Time{},
		SubmittedAt: time.Time{},
	}

	if trimmedKey := strings.TrimSpace(idempotencyKey); trimmedKey != "" {
		env.IdempotencyKey = trimmedKey
	}

	for k, v := range headers {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		env.Headers[key] = v
	}

	if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
		env.TraceID = traceID
	}
	if requestID := ctx.Value("requestID"); requestID != nil {
		env.Headers["requestID"] = fmt.Sprint(requestID)
	}
	if deadline, ok := ctx.Deadline(); ok {
		env.Deadline = deadline
	}

	return env, nil
}

func (u *UserService) buildUserOperationEnvelope(ctx context.Context, kind operation.OperationKind, operationID string, user *v1.User) (*operation.OperationEnvelope, error) {
	if user == nil {
		return nil, fmt.Errorf("user payload is nil")
	}

	return u.buildOperationEnvelope(ctx, kind, operationID, strings.TrimSpace(user.Name), user, nil)
}

func updateOperationID(username string) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	return trimmed + ":update"
}

func deleteOperationID(username string, force bool) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	suffix := ":delete"
	if force {
		suffix += ":force"
	}
	return trimmed + suffix
}

type batchConditionFingerprint struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func batchOperationID(update *v1.User) (string, error) {
	if update == nil {
		return "", fmt.Errorf("batch update request is nil")
	}
	if update.Patch == nil {
		return "", fmt.Errorf("batch update missing patch spec")
	}
	if len(update.Conditions) == 0 {
		return "", fmt.Errorf("batch update missing conditions")
	}

	conds := make([]batchConditionFingerprint, 0, len(update.Conditions))
	for rawKey, rawValue := range update.Conditions {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}
		conds = append(conds, batchConditionFingerprint{
			Key:   key,
			Value: normalizeConditionValue(rawValue),
		})
	}
	if len(conds) == 0 {
		return "", fmt.Errorf("batch update missing usable conditions")
	}

	sort.Slice(conds, func(i, j int) bool {
		if conds[i].Key == conds[j].Key {
			return conds[i].Value < conds[j].Value
		}
		return conds[i].Key < conds[j].Key
	})

	patchBytes, err := stdjson.Marshal(update.Patch)
	if err != nil {
		return "", fmt.Errorf("marshal batch patch: %w", err)
	}

	fingerprintPayload := struct {
		Conditions []batchConditionFingerprint `json:"conditions"`
		Patch      string                      `json:"patch"`
		Command    string                      `json:"command,omitempty"`
	}{
		Conditions: conds,
		Patch:      string(patchBytes),
	}
	if command := strings.TrimSpace(string(update.Command)); command != "" {
		fingerprintPayload.Command = command
	}

	payloadBytes, err := stdjson.Marshal(fingerprintPayload)
	if err != nil {
		return "", fmt.Errorf("marshal batch fingerprint payload: %w", err)
	}

	sum := sha256.Sum256(payloadBytes)
	return fmt.Sprintf("batch:%x", sum[:16]), nil
}

func normalizeConditionValue(raw stdjson.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(trimmed)))
	if err := stdjson.Compact(buf, trimmed); err == nil {
		return buf.String()
	}
	return string(trimmed)
}

type userCreateOperationExecutor struct {
	service *UserService
}

func (e *userCreateOperationExecutor) Prepare(_ context.Context, env *operation.OperationEnvelope) error {
	if env == nil {
		return fmt.Errorf("operation envelope is nil")
	}
	if env.Payload == nil {
		return fmt.Errorf("operation payload is empty")
	}
	return nil
}

func (e *userCreateOperationExecutor) Execute(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	if e == nil || e.service == nil {
		opID := ""
		if env != nil {
			opID = env.ID
		}
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true}, fmt.Errorf("user service not initialized")
	}
	ctx = withOperationEnvelope(ctx, env)

	var user v1.User
	if err := stdjson.Unmarshal(env.Payload, &user); err != nil {
		return &operation.OperationResult{OperationID: env.ID, State: operation.StateFailed, Fatal: true, Error: err}, err
	}

	if err := e.service.createPipeline.Execute(ctx, &user); err != nil {
		fatal := classifyCreateError(err)
		triggerCompensation := false
		if env != nil && env.Headers != nil && strings.TrimSpace(env.Headers[pendingOwnerHeader]) != "" {
			triggerCompensation = true
		}
		res := &operation.OperationResult{
			OperationID:         env.ID,
			State:               operation.StateFailed,
			Error:               err,
			Fatal:               fatal,
			TriggerCompensation: triggerCompensation,
		}
		return res, err
	}

	if env != nil {
		e.service.stopPendingHeartbeatSession(env.ID)
	}

	return &operation.OperationResult{
		OperationID: env.ID,
		State:       operation.StateCompleted,
		CompletedAt: time.Now(),
	}, nil
}

func classifyCreateError(err error) bool {
	if err == nil {
		return false
	}
	switch errors.GetCode(err) {
	case code.ErrInvalidParameter, code.ErrUserAlreadyExist, code.ErrEncrypt:
		return true
	default:
		return false
	}
}

func (e *userCreateOperationExecutor) Compensate(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	start := time.Now()
	outcome := "success"
	releaseOutcome := "skipped"
	compDetails := map[string]interface{}{}
	var errs []error

	result := &operation.OperationResult{
		OperationID: "",
		State:       operation.StateCompensated,
		CompletedAt: time.Now(),
	}
	if env != nil {
		result.OperationID = env.ID
	}

	compCtxRoot, compRootSpan := trace.StartSpan(ctx, "user-service", "compensate_create")
	if compCtxRoot != nil {
		ctx = compCtxRoot
	}
	defer func() {
		status := "success"
		codeStr := strconv.Itoa(code.ErrSuccess)
		if outcome != "success" {
			status = "error"
			codeStr = outcome
		}
		compDetails["release_outcome"] = releaseOutcome
		compDetails["error_count"] = len(errs)
		compDetails["duration_ms"] = time.Since(start).Milliseconds()
		if result != nil && result.OperationID != "" {
			compDetails["operation_id"] = result.OperationID
		}
		trace.EndSpan(compRootSpan, status, codeStr, compDetails)
	}()

	defer func() {
		metrics.ObserveOperationCompensation(operationResourceUsers, outcome, time.Since(start))
	}()

	if e == nil || e.service == nil {
		opID := result.OperationID
		if opID == "" && env != nil {
			opID = env.ID
		}
		fatalErr := fmt.Errorf("user service not initialized")
		outcome = "fatal"
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true, Error: fatalErr}, fatalErr
	}

	if env != nil {
		defer e.service.stopPendingHeartbeatSession(env.ID)
	}
	if env == nil {
		outcome = "noop"
		return result, nil
	}

	username := strings.TrimSpace(env.ID)
	if username == "" {
		outcome = "noop"
		return result, nil
	}

	ownerID := ""
	backendHint := ""
	if env.Headers != nil {
		ownerID = strings.TrimSpace(env.Headers[pendingOwnerHeader])
		backendHint = strings.TrimSpace(env.Headers[pendingBackendHeader])
	}
	compDetails["username"] = username
	compDetails["owner"] = ownerID
	compDetails["backend_hint"] = backendHint

	userSnapshot := &v1.User{}
	userSnapshot.Name = username
	if len(env.Payload) > 0 {
		if err := stdjson.Unmarshal(env.Payload, userSnapshot); err != nil {
			decodeErr := fmt.Errorf("decode operation payload: %w", err)
			outcome = "fatal"
			result.State = operation.StateFailed
			result.Error = decodeErr
			result.Fatal = true
			return result, decodeErr
		}
	}

	compCtx, compSpan := trace.StartSpan(ctx, "user-service", "compensate_pending_lease")
	if compCtx != nil {
		ctx = compCtx
	}
	releaseStart := time.Now()
	if _, releaseErr := e.service.releasePendingLease(ctx, username, ownerID, backendHint); releaseErr != nil {
		releaseOutcome = "error"
		errs = append(errs, fmt.Errorf("release pending lease: %w", releaseErr))
		trace.EndSpan(compSpan, "error", strconv.Itoa(code.ErrUnknown), map[string]interface{}{
			"username":      username,
			"owner":         ownerID,
			"backend_hint":  backendHint,
			"duration_ms":   time.Since(releaseStart).Milliseconds(),
			"retry_attempt": 0,
		})
	} else {
		releaseOutcome = "released"
		trace.EndSpan(compSpan, "success", strconv.Itoa(code.ErrSuccess), map[string]interface{}{
			"username":      username,
			"owner":         ownerID,
			"backend_hint":  backendHint,
			"duration_ms":   time.Since(releaseStart).Milliseconds(),
			"retry_attempt": 0,
		})
	}

	if (strings.TrimSpace(userSnapshot.Email) == "" && strings.TrimSpace(userSnapshot.Phone) == "") || strings.TrimSpace(userSnapshot.Name) == "" {
		fetched, fetchErr := e.service.fetchUserSnapshot(ctx, username)
		if fetchErr != nil {
			if !errors.IsCode(fetchErr, code.ErrUserNotFound) {
				errs = append(errs, fmt.Errorf("fetch user snapshot: %w", fetchErr))
			}
		} else if fetched != nil {
			userSnapshot = fetched
		}
	}

	e.service.normalizeUserContacts(userSnapshot)

	if handlerErr := e.service.runCompensationHandlers(ctx, userSnapshot); handlerErr != nil {
		errs = append(errs, handlerErr)
	}

	if len(errs) > 0 {
		outcome = "error"
		combined := stdErrors.Join(errs...)
		result.State = operation.StateFailed
		result.Error = combined
		result.RetryAfter = time.Second
		return result, combined
	}

	result.CompletedAt = time.Now()
	return result, nil
}

func (u *UserService) releasePendingLease(ctx context.Context, username, ownerID, backendHint string) (time.Duration, error) {
	if u == nil {
		return 0, fmt.Errorf("user service is nil")
	}
	trimmedUser := strings.TrimSpace(username)
	trimmedOwner := strings.TrimSpace(ownerID)
	if trimmedUser == "" || trimmedOwner == "" {
		return 0, nil
	}
	attempts := u.pendingCoordinatorOrder(backendHint)
	if len(attempts) == 0 {
		return 0, nil
	}
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	timeout := u.redisOpTimeout()
	var (
		duration time.Duration
		firstErr error
	)
	for _, coord := range attempts {
		if coord == nil {
			continue
		}
		releaseCtx := baseCtx
		var cancel context.CancelFunc
		if timeout > 0 {
			releaseCtx, cancel = context.WithTimeout(baseCtx, timeout)
		}
		d, err := coord.Release(releaseCtx, trimmedUser, trimmedOwner)
		if cancel != nil {
			cancel()
		}
		duration = d
		component := coord.Component()
		if err == nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(component, "release_compensation_success").Inc()
			}
			return duration, nil
		}
		if stdErrors.Is(err, usercache.ErrPendingLeaseOwnerMismatch) {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(component, "release_compensation_owner_mismatch").Inc()
			}
			continue
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(component, "release_compensation_error").Inc()
		}
		log.Warnw("release pending lease failed", "component", component, "username", trimmedUser, "owner", trimmedOwner, "backend_hint", backendHint, "error", err)
		if firstErr == nil {
			firstErr = err
		}
	}
	return duration, firstErr
}

// compensationHandlers returns the rollback steps executed during create compensation.
func (u *UserService) compensationHandlers() []func(context.Context, *v1.User) error {
	return []func(context.Context, *v1.User) error{
		u.compensateDeleteUserRecord,
		u.compensateEvictUserCache,
		u.compensateClearContactCaches,
	}
}

func (u *UserService) runCompensationHandlers(ctx context.Context, user *v1.User) error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}
	handlers := u.compensationHandlers()
	var errs []error
	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		if err := handler(ctx, user); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return stdErrors.Join(errs...)
}

func (u *UserService) fetchUserSnapshot(ctx context.Context, username string) (*v1.User, error) {
	if u == nil {
		return nil, fmt.Errorf("user service is nil")
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, nil
	}
	if u.Store == nil {
		return nil, fmt.Errorf("user store not initialized")
	}
	store := u.userStoreReadOnly()
	if store == nil {
		store = u.Store.Users()
	}
	if store == nil {
		return nil, fmt.Errorf("user store unavailable")
	}
	dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
	defer cancel()
	return store.Get(dbCtx, trimmed, metav1.GetOptions{}, u.Options)
}

func (u *UserService) compensateDeleteUserRecord(ctx context.Context, user *v1.User) error {
	if u == nil {
		return fmt.Errorf("user service is nil")
	}
	if user == nil {
		return nil
	}
	trimmed := strings.TrimSpace(user.Name)
	if trimmed == "" {
		return nil
	}
	if u.Store == nil {
		return fmt.Errorf("user store not initialized")
	}
	store := u.Store.Users()
	if store == nil {
		return fmt.Errorf("user store unavailable")
	}
	deleteCtx, cancel := u.newDBContext(ctx, u.contactRefreshTimeout())
	defer cancel()
	if err := store.DeleteForce(deleteCtx, trimmed, metav1.DeleteOptions{Unscoped: true}, u.Options); err != nil {
		if errors.IsCode(err, code.ErrUserNotFound) {
			return nil
		}
		return fmt.Errorf("delete user record: %w", err)
	}
	return nil
}

func (u *UserService) compensateEvictUserCache(ctx context.Context, user *v1.User) error {
	if u == nil || u.Redis == nil || user == nil {
		return nil
	}
	username := strings.TrimSpace(user.Name)
	if username == "" {
		return nil
	}
	key := u.generateUserCacheKey(username)
	if key == "" {
		return nil
	}
	if err := u.deleteRedisKey(ctx, key); err != nil {
		return fmt.Errorf("evict user cache: %w", err)
	}
	return nil
}

func (u *UserService) compensateClearContactCaches(ctx context.Context, user *v1.User) error {
	if u == nil || u.Redis == nil || user == nil {
		return nil
	}
	var errs []error
	if emailKey := u.generateEmailCacheKey(user.Email); emailKey != "" {
		if err := u.deleteRedisKey(ctx, emailKey); err != nil {
			errs = append(errs, fmt.Errorf("evict email cache: %w", err))
		}
	}
	if phoneKey := u.generatePhoneCacheKey(user.Phone); phoneKey != "" {
		if err := u.deleteRedisKey(ctx, phoneKey); err != nil {
			errs = append(errs, fmt.Errorf("evict phone cache: %w", err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return stdErrors.Join(errs...)
}

func (u *UserService) deleteRedisKey(ctx context.Context, key string) error {
	if u == nil || u.Redis == nil || key == "" {
		return nil
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	if _, err := u.Redis.DeleteKey(redisCtx, key); err != nil && !stdErrors.Is(err, redis.Nil) {
		return err
	}
	return nil
}
