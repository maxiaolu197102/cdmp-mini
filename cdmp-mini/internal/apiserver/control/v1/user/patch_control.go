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
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type profileUpdateRequest struct {
	Nickname  *string               `json:"nickname,omitempty"`
	Status    *int                  `json:"status,omitempty"`
	IsAdmin   *int                  `json:"isAdmin,omitempty"`
	LoginedAt *time.Time            `json:"loginedAt,omitempty"`
	Extend    *v1.ExtendPatchSpec   `json:"extend,omitempty"`
	Metadata  *v1.MetadataPatchSpec `json:"metadata,omitempty"`
	Version   *uint64               `json:"version,omitempty"`
}

type passwordPatchRequest struct {
	Password *string `json:"password,omitempty"`
	Version  *uint64 `json:"version,omitempty"`
}

type emailPatchRequest struct {
	Email   *string `json:"email,omitempty"`
	Version *uint64 `json:"version,omitempty"`
}

type phonePatchRequest struct {
	Phone   *string `json:"phone,omitempty"`
	Version *uint64 `json:"version,omitempty"`
}

type batchPatchRequest struct {
	Updates    *v1.UserPatchSpec `json:"updates"`
	Conditions v1.UserConditions `json:"conditions"`
}

type targetedPatchOptions struct {
	TraceAction string
	Operation   string
	AuditAction string
	Build       func(*gin.Context) (*v1.UserPatchSpec, *uint64, error)
}

func (u *UserController) UpdateProfile(ctx *gin.Context) {
	u.handleTargetedPatch(ctx, targetedPatchOptions{
		TraceAction: "update_user_profile",
		Operation:   "patch_user_profile",
		AuditAction: "user.profile_update",
		Build:       buildProfilePatch,
	})
}

func (u *UserController) PatchPassword(ctx *gin.Context) {
	u.handleTargetedPatch(ctx, targetedPatchOptions{
		TraceAction: "patch_user_password",
		Operation:   "patch_user_password",
		AuditAction: "user.password_patch",
		Build:       buildPasswordPatch,
	})
}

func (u *UserController) PatchEmail(ctx *gin.Context) {
	u.handleTargetedPatch(ctx, targetedPatchOptions{
		TraceAction: "patch_user_email",
		Operation:   "patch_user_email",
		AuditAction: "user.email_patch",
		Build:       buildEmailPatch,
	})
}

func (u *UserController) PatchPhone(ctx *gin.Context) {
	u.handleTargetedPatch(ctx, targetedPatchOptions{
		TraceAction: "patch_user_phone",
		Operation:   "patch_user_phone",
		AuditAction: "user.phone_patch",
		Build:       buildPhonePatch,
	})
}

func (u *UserController) handleTargetedPatch(ctx *gin.Context, opts targetedPatchOptions) {
	username := strings.TrimSpace(ctx.Param("name"))
	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", opts.TraceAction)
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", opts.TraceAction)
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
		event.Action = opts.AuditAction
		event.ResourceType = "user"
		event.ResourceID = username
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", opts.Operation, "http", func() error {
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

		spec, version, buildErr := opts.Build(ctx)
		if buildErr != nil {
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(buildErr))
			if controllerCode == "-1" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(buildErr)
			outcomeHTTP = errors.GetHTTPStatus(buildErr)
			core.WriteResponse(ctx, buildErr, nil)
			auditLog("fail", buildErr.Error())
			return buildErr
		}

		if spec == nil || isEmptyPatchSpec(spec) {
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

		if spec.Password != nil {
			hashCfg := auth.HashConfig{}
			if u.options != nil && u.options.ServerRunOptions != nil {
				hashCfg = u.options.ServerRunOptions.HashConfig()
			}
			hashed, hashErr := auth.EncryptWithConfig(*spec.Password, hashCfg)
			if hashErr != nil {
				encErr := errors.WithCode(code.ErrEncrypt, "用户密码加密失败")
				controllerStatus = "error"
				controllerCode = strconv.Itoa(errors.GetCode(encErr))
				outcomeStatus = "error"
				outcomeCode = controllerCode
				outcomeMessage = errors.GetMessage(encErr)
				outcomeHTTP = errors.GetHTTPStatus(encErr)
				core.WriteResponse(ctx, encErr, nil)
				auditLog("fail", encErr.Error())
				return encErr
			}
			spec.Password = &hashed
		}

		payload := &v1.User{
			ObjectMeta: metav1.ObjectMeta{Name: username},
			Command:    v1.UserUpdateCommandPatch,
			Patch:      spec,
		}
		if version != nil {
			expected := *version
			payload.ExpectedVersion = &expected
		}

		fields := patchFieldSummary(spec)
		if len(fields) > 0 {
			trace.AddRequestTag(controllerCtx, "patch_fields", fields)
			controllerDetails["updated_fields"] = fields
		}
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
			"username":      username,
			"command":       payload.Command,
			"code":          code.ErrSuccess,
			"updatedFields": fields,
		}
		if payload.ExpectedVersion != nil {
			successData["expectedVersion"] = *payload.ExpectedVersion
		}
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

func buildProfilePatch(ctx *gin.Context) (*v1.UserPatchSpec, *uint64, error) {
	var req profileUpdateRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		return nil, nil, errors.WithCode(code.ErrBind, "%s", err.Error())
	}
	if req.Version != nil && *req.Version == 0 {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "版本号不合法")
	}
	spec := &v1.UserPatchSpec{
		Status:    req.Status,
		Nickname:  req.Nickname,
		IsAdmin:   req.IsAdmin,
		LoginedAt: req.LoginedAt,
		Extend:    req.Extend,
		Metadata:  req.Metadata,
	}
	return spec, req.Version, nil
}

func buildPasswordPatch(ctx *gin.Context) (*v1.UserPatchSpec, *uint64, error) {
	var req passwordPatchRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		return nil, nil, errors.WithCode(code.ErrBind, "%s", err.Error())
	}
	if req.Password == nil {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "缺少密码字段")
	}
	password := strings.TrimSpace(*req.Password)
	if password == "" {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "密码不能为空")
	}
	if req.Version != nil && *req.Version == 0 {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "版本号不合法")
	}
	spec := &v1.UserPatchSpec{
		Password: &password,
	}
	return spec, req.Version, nil
}

func buildEmailPatch(ctx *gin.Context) (*v1.UserPatchSpec, *uint64, error) {
	var req emailPatchRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		return nil, nil, errors.WithCode(code.ErrBind, "%s", err.Error())
	}
	if req.Email == nil {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "缺少邮箱字段")
	}
	email := strings.TrimSpace(*req.Email)
	if req.Version != nil && *req.Version == 0 {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "版本号不合法")
	}
	spec := &v1.UserPatchSpec{
		Email: &email,
	}
	return spec, req.Version, nil
}

func buildPhonePatch(ctx *gin.Context) (*v1.UserPatchSpec, *uint64, error) {
	var req phonePatchRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		return nil, nil, errors.WithCode(code.ErrBind, "%s", err.Error())
	}
	if req.Phone == nil {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "缺少手机号字段")
	}
	phone := strings.TrimSpace(*req.Phone)
	if req.Version != nil && *req.Version == 0 {
		return nil, nil, errors.WithCode(code.ErrInvalidParameter, "版本号不合法")
	}
	spec := &v1.UserPatchSpec{
		Phone: &phone,
	}
	return spec, req.Version, nil
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
	outcomeHTTP := http.StatusOK
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
		outcomeHTTP = http.StatusOK
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
