package user

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type userUpdateOperationExecutor struct {
	service *UserService
}

func (e *userUpdateOperationExecutor) Prepare(_ context.Context, env *operation.OperationEnvelope) error {
	if env == nil {
		return fmt.Errorf("operation envelope is nil")
	}
	if len(env.Payload) == 0 {
		return fmt.Errorf("operation payload is empty")
	}
	return nil
}

func (e *userUpdateOperationExecutor) Execute(ctx context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
	if e == nil || e.service == nil {
		opID := ""
		if env != nil {
			opID = env.ID
		}
		fatalErr := fmt.Errorf("user service not initialized")
		return &operation.OperationResult{OperationID: opID, State: operation.StateFailed, Fatal: true, Error: fatalErr}, fatalErr
	}
	ctx = withOperationEnvelope(ctx, env)

	var user v1.User
	if err := stdjson.Unmarshal(env.Payload, &user); err != nil {
		decodeErr := fmt.Errorf("decode update payload: %w", err)
		return &operation.OperationResult{OperationID: env.ID, State: operation.StateFailed, Fatal: true, Error: decodeErr}, decodeErr
	}

	if err := e.service.processUserUpdate(ctx, &user); err != nil {
		fatal := classifyUpdateError(err)
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

func (e *userUpdateOperationExecutor) Compensate(_ context.Context, env *operation.OperationEnvelope) (*operation.OperationResult, error) {
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

func classifyUpdateError(err error) bool {
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
