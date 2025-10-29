package user

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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

type patchProfileRequest struct {
	Updates *v1.UserPatchSpec `json:"updates"`
	Version *uint64           `json:"version,omitempty"`
}

type batchPatchRequest struct {
	Updates    *v1.UserPatchSpec `json:"updates"`
	Conditions v1.UserConditions `json:"conditions"`
}

func (u *UserController) PatchProfile(ctx *gin.Context) {
	username := strings.TrimSpace(ctx.Param("name"))
	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "patch_user_profile")
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "patch_user_profile")
	trace.AddRequestTag(controllerCtx, "target_user", username)
	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]any{
		"request_id":  ctx.Request.Header.Get("X-Request-ID"),
		"target_user": username,
		"operator":    operator,
	}
	defer func() {
		trace.EndSpan(controllerSpan, controllerStatus, controllerCode, controllerDetails)
	}()

	outcomeStatus := "success"
	outcomeCode := strconv.Itoa(code.ErrSuccess)
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK
	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.patch"
		event.ResourceType = "user"
		event.ResourceID = username
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "patch_user_profile", "http", func() error {
		var req patchProfileRequest
		if err := ctx.ShouldBindJSON(&req); err != nil {
			bindErr := errors.WithCode(code.ErrBind, "%s", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(bindErr))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(bindErr)
			outcomeHTTP = errors.GetHTTPStatus(bindErr)
			core.WriteResponse(ctx, bindErr, nil)
			auditLog("fail", bindErr.Error())
			return bindErr
		}
		if strings.TrimSpace(username) == "" {
			err := errors.WithCode(code.ErrValidation, "用户名不能为空")
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
		if errs := validation.IsQualifiedName(username); len(errs) > 0 {
			errMsg := strings.Join(errs, ":")
			err := errors.WithCode(code.ErrValidation, "用户名不合法: %s", errMsg)
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
		if isEmptyPatchSpec(req.Updates) {
			err := errors.WithCode(code.ErrInvalidParameter, "缺少更新字段")
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
		if req.Version != nil && *req.Version == 0 {
			err := errors.WithCode(code.ErrInvalidParameter, "版本号不合法")
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

		payload := &v1.User{
			ObjectMeta: metav1.ObjectMeta{Name: username},
			Command:    v1.UserUpdateCommandPatch,
			Patch:      req.Updates,
		}
		if req.Version != nil {
			expected := *req.Version
			payload.ExpectedVersion = &expected
		}

		trace.AddRequestTag(controllerCtx, "patch_fields", patchFieldSummary(req.Updates))
		if payload.ExpectedVersion != nil {
			trace.AddRequestTag(controllerCtx, "requested_version", *payload.ExpectedVersion)
		}

		c := ctx.Request.Context()
		if _, hasDeadline := c.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeout := u.options.ServerRunOptions.CtxTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			c, cancel = context.WithTimeout(c, timeout)
			defer cancel()
		}

		if err := u.srv.Users().Update(c, payload, metav1.UpdateOptions{}, u.options); err != nil {
			log.Errorf("[control] 用户局部更新失败: username=%s, error=%v", username, err)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "-1" {
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

		successData := gin.H{
			"username": username,
			"command":  payload.Command,
			"code":     code.ErrSuccess,
		}
		if payload.ExpectedVersion != nil {
			successData["expectedVersion"] = *payload.ExpectedVersion
		}
		controllerDetails["updated_fields"] = patchFieldSummary(req.Updates)
		outcomeMessage = "success"
		awaitTimeout := 30 * time.Second
		if u.options != nil && u.options.ServerRunOptions != nil && u.options.ServerRunOptions.CtxTimeout > 0 {
			awaitTimeout = u.options.ServerRunOptions.CtxTimeout
		}
		trace.ExpectAsync(controllerCtx, time.Now().Add(awaitTimeout))
		core.WriteResponse(ctx, nil, successData)
		auditLog("success", "")
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		outcomeStatus = "error"
		outcomeCode = strconv.Itoa(errors.GetCode(err))
		if outcomeCode == "-1" {
			outcomeCode = strconv.Itoa(code.ErrUnknown)
		}
		outcomeMessage = errors.GetMessage(err)
		outcomeHTTP = errors.GetHTTPStatus(err)
		controllerStatus = "error"
		controllerCode = outcomeCode
	}

	trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
}

func (u *UserController) PatchCollection(ctx *gin.Context) {
	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "patch_user_batch")
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "patch_user_batch")
	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]any{
		"request_id": ctx.Request.Header.Get("X-Request-ID"),
		"operator":   operator,
	}
	defer func() {
		trace.EndSpan(controllerSpan, controllerStatus, controllerCode, controllerDetails)
	}()

	outcomeStatus := "success"
	outcomeCode := strconv.Itoa(code.ErrSuccess)
	outcomeMessage := ""
	outcomeHTTP := http.StatusAccepted
	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.patch_batch"
		event.ResourceType = "user"
		event.ResourceID = "*"
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "patch_user_batch", "http", func() error {
		var req batchPatchRequest
		if err := ctx.ShouldBindJSON(&req); err != nil {
			bindErr := errors.WithCode(code.ErrBind, "%s", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(bindErr))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(bindErr)
			outcomeHTTP = errors.GetHTTPStatus(bindErr)
			core.WriteResponse(ctx, bindErr, nil)
			auditLog("fail", bindErr.Error())
			return bindErr
		}
		if isEmptyPatchSpec(req.Updates) {
			err := errors.WithCode(code.ErrInvalidParameter, "缺少更新字段")
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
		if len(req.Conditions) == 0 {
			err := errors.WithCode(code.ErrInvalidParameter, "缺少批量更新条件")
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

		trace.AddRequestTag(controllerCtx, "batch_condition_count", len(req.Conditions))
		trace.AddRequestTag(controllerCtx, "batch_patch_fields", patchFieldSummary(req.Updates))
		controllerDetails["conditions"] = len(req.Conditions)
		controllerDetails["updated_fields"] = patchFieldSummary(req.Updates)

		c := ctx.Request.Context()
		if _, hasDeadline := c.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeout := u.options.ServerRunOptions.CtxTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			c, cancel = context.WithTimeout(c, timeout)
			defer cancel()
		}

		payload := &v1.User{
			Command:    v1.UserUpdateCommandBatch,
			Patch:      req.Updates,
			Conditions: req.Conditions,
		}

		if err := u.srv.Users().BatchPatch(c, payload, u.options); err != nil {
			log.Errorf("[control] 批量条件更新失败: error=%v", err)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "-1" {
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

		successData := gin.H{
			"command":        payload.Command,
			"conditions":     len(req.Conditions),
			"updated_fields": patchFieldSummary(req.Updates),
			"code":           code.ErrSuccess,
		}
		outcomeHTTP = http.StatusAccepted
		outcomeMessage = "accepted"
		awaitTimeout := 30 * time.Second
		if u.options != nil && u.options.ServerRunOptions != nil && u.options.ServerRunOptions.CtxTimeout > 0 {
			awaitTimeout = u.options.ServerRunOptions.CtxTimeout
		}
		trace.ExpectAsync(controllerCtx, time.Now().Add(awaitTimeout))
		core.WriteResponse(ctx, nil, successData)
		auditLog("success", "")
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		outcomeStatus = "error"
		outcomeCode = strconv.Itoa(errors.GetCode(err))
		if outcomeCode == "-1" {
			outcomeCode = strconv.Itoa(code.ErrUnknown)
		}
		outcomeMessage = errors.GetMessage(err)
		outcomeHTTP = errors.GetHTTPStatus(err)
		controllerStatus = "error"
		controllerCode = outcomeCode
	}

	trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
}

func isEmptyPatchSpec(spec *v1.UserPatchSpec) bool {
	if spec == nil {
		return true
	}
	if spec.Status != nil || spec.Nickname != nil || spec.Email != nil || spec.Phone != nil || spec.IsAdmin != nil || spec.Password != nil || spec.LoginedAt != nil {
		return false
	}
	if spec.Extend != nil && !isEmptyExtendPatch(spec.Extend) {
		return false
	}
	if spec.Metadata != nil && spec.Metadata.Extend != nil && !isEmptyExtendPatch(spec.Metadata.Extend) {
		return false
	}
	return true
}

func isEmptyExtendPatch(spec *v1.ExtendPatchSpec) bool {
	if spec == nil {
		return true
	}
	if len(spec.Merge) > 0 || len(spec.Replace) > 0 || len(spec.Remove) > 0 {
		return false
	}
	return true
}

func patchFieldSummary(spec *v1.UserPatchSpec) []string {
	result := make([]string, 0, 6)
	if spec == nil {
		return result
	}
	if spec.Status != nil {
		result = append(result, "status")
	}
	if spec.Nickname != nil {
		result = append(result, "nickname")
	}
	if spec.Email != nil {
		result = append(result, "email")
	}
	if spec.Phone != nil {
		result = append(result, "phone")
	}
	if spec.IsAdmin != nil {
		result = append(result, "isAdmin")
	}
	if spec.Password != nil {
		result = append(result, "password")
	}
	if spec.LoginedAt != nil {
		result = append(result, "loginedAt")
	}
	if spec.Extend != nil && !isEmptyExtendPatch(spec.Extend) {
		result = append(result, "extend")
	}
	if spec.Metadata != nil && spec.Metadata.Extend != nil && !isEmptyExtendPatch(spec.Metadata.Extend) {
		result = append(result, "metadata.extend")
	}
	return result
}
