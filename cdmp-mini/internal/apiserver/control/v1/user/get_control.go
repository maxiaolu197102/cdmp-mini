package user

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	sru "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/user"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserController) Get(ctx *gin.Context) {
	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	username := ctx.Param("name")

	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "get_user")
	if controllerCtx == nil {
		controllerCtx = traceCtx
	} else {
		ctx.Request = ctx.Request.WithContext(controllerCtx)
	}
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "get_user")
	trace.AddRequestTag(controllerCtx, "target_user", username)

	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]interface{}{
		"request_id":  ctx.Request.Header.Get("X-Request-ID"),
		"operator":    operator,
		"target_user": username,
	}
	defer func() {
		if controllerSpan != nil {
			trace.EndSpan(controllerSpan, controllerStatus, controllerCode, controllerDetails)
		}
	}()

	outcomeStatus := "success"
	outcomeCode := strconv.Itoa(code.ErrSuccess)
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK

	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.get"
		event.ResourceType = "user"
		event.ResourceID = username
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "get", "http", func() error {
		if errs := validation.IsQualifiedName(username); len(errs) > 0 {
			errMsg := strings.Join(errs, ":")
			log.Errorf("用户名参数校验失败:", "error", errMsg)
			err := errors.WithCode(code.ErrValidation, "用户名不合法:%s", errMsg)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}

		c := controllerCtx
		if _, hasDeadline := c.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			requestTimeout := u.options.ServerRunOptions.CtxTimeout
			if requestTimeout == 0 {
				requestTimeout = 30 * time.Second
			}
			c, cancel = context.WithTimeout(c, requestTimeout)
			defer cancel()
		}

		user, err := u.srv.Users().Get(c, username, metav1.GetOptions{}, u.options)
		if err != nil {
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		if user == nil {
			err := errors.WithCode(code.ErrUserNotFound, "用户不存在")
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		if user.Name == sru.RATE_LIMIT_PREVENTION || user.Name == sru.BLACKLIST_SENTINEL {
			err := errors.WithCode(code.ErrUserNotFound, "用户不存在或已删除")
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}

		publicUser := v1.ConvertToPublicUser(user)
		successData := gin.H{
			"get":            publicUser.Username,
			"operator":       operator,
			"operation_time": time.Now().Format(time.RFC3339),
			"operation_type": "retrieve",
		}
		controllerDetails["result_user"] = publicUser.Username
		core.WriteResponse(ctx, nil, successData)
		auditLog("success", "")
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		outcomeStatus = "error"
		outcomeCode = strconv.Itoa(errors.GetCode(err))
		if outcomeCode == "0" {
			outcomeCode = strconv.Itoa(code.ErrUnknown)
		}
		outcomeMessage = errors.GetMessage(err)
		outcomeHTTP = errors.GetHTTPStatus(err)
		controllerStatus = "error"
		controllerCode = outcomeCode
	}

	trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
}
