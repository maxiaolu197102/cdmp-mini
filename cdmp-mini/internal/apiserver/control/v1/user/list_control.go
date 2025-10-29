package user

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/fields"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserController) List(ctx *gin.Context) {

	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)

	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "list_users")
	if controllerCtx == nil {
		controllerCtx = traceCtx
	} else {
		ctx.Request = ctx.Request.WithContext(controllerCtx)
	}
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "list_users")

	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]interface{}{
		"request_id": ctx.Request.Header.Get("X-Request-ID"),
		"operator":   operator,
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

	auditLog := func(outcome string, err error, meta map[string]any) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.list"
		event.ResourceType = "user"
		event.ResourceID = "collection"
		event.Actor = operator
		event.Outcome = outcome
		if err != nil {
			event.ErrorMessage = err.Error()
		}
		if len(meta) > 0 {
			event.Metadata = meta
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "list", "http", func() error {
		var opts metav1.ListOptions
		if err := ctx.ShouldBindQuery(&opts); err != nil {
			errWrap := errors.WithCode(code.ErrBind, "传入的参数错误")
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errWrap))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errWrap)
			outcomeHTTP = errors.GetHTTPStatus(errWrap)
			core.WriteResponse(ctx, errWrap, nil)
			auditLog("fail", errWrap, map[string]any{"stage": "bind"})
			return errWrap
		}

		// 尝试从 fieldSelector 中解析用户名，并记录在链路中
		if opts.FieldSelector != "" {
			if controllerDetails != nil {
				controllerDetails["field_selector"] = opts.FieldSelector
			}
			if selector, err := fields.ParseSelector(opts.FieldSelector); err == nil {
				if username, ok := selector.RequiresExactMatch("name"); ok {
					trace.AddRequestTag(controllerCtx, "target_user", username)
					controllerDetails["target_user"] = username
				}
			}
		}
		if opts.Limit != nil {
			controllerDetails["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			controllerDetails["offset"] = *opts.Offset
		}

		errs := u.validateListOptions(&opts)
		if len(errs) > 0 {
			errDetails := make(map[string]string, len(errs))
			for _, fieldErr := range errs {
				errDetails[fieldErr.Field] = fieldErr.ErrorBody()
			}
			detailStr := fmt.Sprintf("参数错误:%+v", errDetails)
			errValidate := errors.WrapC(nil, code.ErrInvalidParameter, "%s", detailStr)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errValidate))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errValidate)
			outcomeHTTP = errors.GetHTTPStatus(errValidate)
			core.WriteResponse(ctx, errValidate, nil)
			auditLog("fail", errValidate, map[string]any{"stage": "validate"})
			return errValidate
		}

		requestCtx := controllerCtx
		if _, hasDeadline := requestCtx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeout := u.options.ServerRunOptions.CtxTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			requestCtx, cancel = context.WithTimeout(requestCtx, timeout)
			defer cancel()
		}

		userList, err := u.srv.Users().List(requestCtx, opts, u.options)
		if err != nil {
			errWrap := errors.WrapC(err, code.ErrInternal, "%s", errors.GetMessage(err))
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errWrap))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errWrap)
			outcomeHTTP = errors.GetHTTPStatus(errWrap)
			core.WriteResponse(ctx, errWrap, nil)
			auditLog("fail", errWrap, map[string]any{"stage": "service"})
			return errWrap
		}

		var publicUsers []*v1.PublicUser
		if len(userList.Items) > 0 {
			for _, user := range userList.Items {
				publicUser := v1.ConvertToPublicUser(user)
				publicUsers = append(publicUsers, publicUser)
			}
		}

		controllerDetails["returned_count"] = len(publicUsers)
		core.WriteResponse(ctx, nil, publicUsers)

		meta := map[string]any{
			"returned_count": len(publicUsers),
		}
		if opts.Limit != nil {
			meta["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			meta["offset"] = *opts.Offset
		}
		if opts.LabelSelector != "" {
			meta["label_selector"] = opts.LabelSelector
		}
		if opts.FieldSelector != "" {
			meta["field_selector"] = opts.FieldSelector
		}
		auditLog("success", nil, meta)
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
