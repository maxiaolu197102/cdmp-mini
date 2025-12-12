package user

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type userDeleteOperationExecutor struct {
	service *UserService
}

func (e *userDeleteOperationExecutor) Prepare(_ context.Context, env *operation.OperationEnvelope) error {
	if env == nil {
		return fmt.Errorf("operation envelope is nil")
	}
	if len(env.Payload) == 0 {
		return fmt.Errorf("operation payload is empty")
	}
	return nil
}

func (e *userDeleteOperationExecutor) Execute(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	if e == nil || e.service == nil {
		opID := ""
		if env != nil {
			opID = env.ID
		}
		fatalErr := fmt.Errorf("user service not initialized")
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true, Error: fatalErr}, fatalErr
	}
	ctx = withOperationEnvelope(ctx, env)

	var payload userDeleteOperationPayload
	if err := stdjson.Unmarshal(env.Payload, &payload); err != nil {
		decodeErr := fmt.Errorf("decode delete payload: %w", err)
		return &operation.OperationResult{OperationID: env.ID, State: operation.StateFailed, Fatal: true, Error: decodeErr}, decodeErr
	}
	stopHeartbeat := e.service.startHeartbeatFromEnvelope(env, payload.Username)
	defer stopHeartbeat()

	if err := e.service.processUserDelete(ctx, &payload); err != nil {
		fatal := classifyDeleteError(err)
		res := &operation.OperationResult{
			OperationID: env.ID,
			State:       operation.StateFailed,
			Error:       err,
			Fatal:       fatal,
		}
		return res, err
	}

	return &operation.OperationResult{
		OperationID: env.ID,
		State:       operation.StateCompleted,
		CompletedAt: time.Now(),
	}, nil
}

func (e *userDeleteOperationExecutor) Compensate(_ context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	result := &operation.OperationResult{
		OperationID: "",
		State:       operation.StateCompensated,
		CompletedAt: time.Now(),
	}
	if env != nil {
		result.OperationID = env.ID
	}
	return result, nil
}

func classifyDeleteError(err error) bool {
	if err == nil {
		return false
	}
	switch errors.GetCode(err) {
	case code.ErrInvalidParameter, code.ErrUserNotFound:
		return true
	default:
		return false
	}
}
