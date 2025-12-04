package user

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// awaitOperationState polls the request state store until the operation reaches
// a terminal state or the caller's context times out. translate allows callers
// to map persisted error messages to service-specific error codes.
func (u *UserService) awaitOperationState(ctx context.Context, operationID string, translate func(string) error) error {
	if u == nil || u.operationStateStore == nil {
		return errors.WithCode(code.ErrServerBusy, "操作状态未知，请稍后重试")
	}

	trimmedID := strings.TrimSpace(operationID)
	if trimmedID == "" {
		return errors.WithCode(code.ErrServerBusy, "操作状态未知，请稍后重试")
	}

	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}

	timeout := u.operationAwaitTimeout()
	if deadline, hasDeadline := waitCtx.Deadline(); hasDeadline {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.WithCode(code.ErrServerBusy, "操作处理中，请稍后查询")
		}
		if remaining < timeout {
			timeout = remaining
		}
	}

	waitCtx, cancel := context.WithTimeout(waitCtx, timeout)
	defer cancel()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		stateCtx, stateCancel := context.WithTimeout(waitCtx, 250*time.Millisecond)
		state, err := u.operationStateStore.Get(stateCtx, trimmedID)
		stateCancel()

		if err == nil {
			switch state.State {
			case operation.StateCompleted:
				return nil
			case operation.StateFailed:
				if translate == nil {
					translate = func(msg string) error {
						message := strings.TrimSpace(msg)
						if message == "" {
							return errors.WithCode(code.ErrServerBusy, "操作失败")
						}
						return errors.WithCode(code.ErrServerBusy, "%s", message)
					}
				}
				return translate(state.LastError)
			}
		} else {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if waitCtx.Err() != nil {
					return errors.WithCode(code.ErrServerBusy, "操作处理中，请稍后查询")
				}
				log.Debugw("查询操作状态超时", "component", "user_service", "operation", trimmedID)
			} else {
				log.Warnw("查询操作状态失败", "component", "user_service", "operation", trimmedID, "error", err)
			}
		}

		select {
		case <-waitCtx.Done():
			return errors.WithCode(code.ErrServerBusy, "操作处理中，请稍后查询")
		case <-ticker.C:
		}
	}
}

func (u *UserService) operationAwaitTimeout() time.Duration {
	defaultTimeout := 2 * time.Second
	if u != nil && u.Options != nil && u.Options.ServerRunOptions != nil && u.Options.ServerRunOptions.CtxTimeout > 0 {
		configured := u.Options.ServerRunOptions.CtxTimeout
		if configured < defaultTimeout {
			if configured <= 0 {
				return defaultTimeout
			}
			return configured
		}
	}
	return defaultTimeout
}

var operationErrorCodePattern = regexp.MustCompile(`\[code:\s*(\d+)\]`)

func translateOperationFailureWithFallback(raw, fallback string) error {
	message := strings.TrimSpace(raw)
	if fallback == "" {
		fallback = "操作失败"
	}
	if message == "" {
		return errors.WithCode(code.ErrServerBusy, "%s", fallback)
	}

	codeVal := code.ErrServerBusy
	if matches := operationErrorCodePattern.FindStringSubmatch(message); len(matches) == 2 {
		if parsed, err := strconv.Atoi(matches[1]); err == nil && parsed > 0 {
			codeVal = parsed
		}
	}

	if idx := strings.Index(message, "]"); idx >= 0 {
		message = strings.TrimSpace(message[idx+1:])
		if strings.HasPrefix(message, "[http:") {
			if httpIdx := strings.Index(message, "]"); httpIdx >= 0 {
				message = strings.TrimSpace(message[httpIdx+1:])
			}
		}
	}

	if message == "" {
		message = fallback
	}

	return errors.WithCode(codeVal, "%s", message)
}

func translateCreateOperationFailure(raw string) error {
	return translateOperationFailureWithFallback(raw, "用户创建失败")
}

func translateUpdateOperationFailure(raw string) error {
	return translateOperationFailureWithFallback(raw, "用户更新失败")
}

func translateDeleteOperationFailure(raw string) error {
	return translateOperationFailureWithFallback(raw, "用户删除失败")
}

func translateBatchOperationFailure(raw string) error {
	return translateOperationFailureWithFallback(raw, "批量更新失败")
}
