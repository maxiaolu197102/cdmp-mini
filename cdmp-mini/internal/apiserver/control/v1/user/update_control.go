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

type updateUserRequest struct {
	Nickname  *string    `json:"nickname"`
	Email     *string    `json:"email"`
	Phone     *string    `json:"phone"`
	Status    *int       `json:"status"`
	IsAdmin   *int       `json:"isAdmin"`
	LoginedAt *time.Time `json:"loginedAt"`
	Version   *uint64    `json:"version"`
}

func (u *UserController) Update(ctx *gin.Context) {
	username := ctx.Param("name") // 从URL路径获取，清晰明确

	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "update_user")
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "update_user")
	trace.AddRequestTag(controllerCtx, "target_user", username)
	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]interface{}{
		"request_id":  ctx.Request.Header.Get("X-Request-ID"),
		"target_user": username,
		"operator":    operator,
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
		event.Action = "user.update"
		event.ResourceType = "user"
		event.ResourceID = username
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}
	err := metrics.MonitorBusinessOperation("user_service", "update_user", "http", func() error {

		var req updateUserRequest
		if err := ctx.ShouldBindJSON(&req); err != nil {
			errBind := errors.WithCode(code.ErrBind, "%s", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errBind))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errBind)
			outcomeHTTP = errors.GetHTTPStatus(errBind)
			core.WriteResponse(ctx, errBind, nil)
			auditLog("fail", errBind.Error())
			return errBind
		}
		if req.Nickname != nil {
			trace.AddRequestTag(controllerCtx, "requested_nickname", *req.Nickname)
		}
		if req.Email != nil {
			trace.AddRequestTag(controllerCtx, "requested_email", *req.Email)
		}
		if req.Phone != nil {
			trace.AddRequestTag(controllerCtx, "requested_phone", *req.Phone)
		}
		if req.Status != nil {
			trace.AddRequestTag(controllerCtx, "requested_status", *req.Status)
		}
		if req.IsAdmin != nil {
			trace.AddRequestTag(controllerCtx, "requested_is_admin", *req.IsAdmin)
		}
		if req.Version != nil {
			trace.AddRequestTag(controllerCtx, "requested_version", *req.Version)
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
			log.Warnf("[control] 用户名校验失败: username=%s, error=%s", username, errMsg)
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

		c := ctx.Request.Context()
		// 使用HTTP请求的超时配置，而不是Redis超时
		if _, hasDeadline := c.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			// 使用ServerRunOptions中的请求超时时间
			requestTimeout := u.options.ServerRunOptions.CtxTimeout
			if requestTimeout == 0 {
				requestTimeout = 30 * time.Second // 默认30秒
			}
			c, cancel = context.WithTimeout(c, requestTimeout)
			defer cancel()
		}
		//查询现有用户
		existingUser, err := u.srv.Users().Get(c, username, metav1.GetOptions{}, u.options)
		if err != nil {
			log.Errorf("[control] 用户更新 service 层查询失败: username=%s, error=%v", username, err)
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
		if existingUser == nil {
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
		//  安全防护：检查是否是防刷标记
		if existingUser.Name == sru.RATE_LIMIT_PREVENTION || existingUser.Name == sru.BLACKLIST_SENTINEL {
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

		// ✅ 只更新非nil的字段
		if req.Nickname != nil {
			existingUser.Nickname = strings.TrimSpace(*req.Nickname)
		}
		if req.Email != nil {
			existingUser.Email = strings.TrimSpace(*req.Email)
		}
		if req.Phone != nil {
			existingUser.Phone = strings.TrimSpace(*req.Phone)
		}
		if req.LoginedAt != nil {
			existingUser.LoginedAt = req.LoginedAt.UTC()
		}
		if req.Status != nil {
			if *req.Status != 0 && *req.Status != 1 {
				err := errors.WithCode(code.ErrValidation, "状态值必须为0或1")
				core.WriteResponse(ctx, err, nil)
				auditLog("fail", err.Error())
				return err
			}
			existingUser.Status = *req.Status
		}
		if req.IsAdmin != nil {
			if *req.IsAdmin != 0 && *req.IsAdmin != 1 {
				err := errors.WithCode(code.ErrValidation, "管理员状态必须为0或1")
				core.WriteResponse(ctx, err, nil)
				auditLog("fail", err.Error())
				return err
			}
			existingUser.IsAdmin = *req.IsAdmin
		}
		if req.Version == nil || *req.Version == 0 {
			err := errors.WithCode(code.ErrInvalidParameter, "缺少版本号version")
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		if existingUser.ObjectMeta.Version != *req.Version {
			err := errors.WithCode(code.ErrResourceConflict, "用户数据版本不匹配")
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		expected := *req.Version
		existingUser.ExpectedVersion = &expected
		existingUser.Command = v1.UserUpdateCommandFull

		if errs := existingUser.ValidateUpdate(); len(errs) != 0 {
			err := errors.WithCode(code.ErrValidation, "%s", errs.ToAggregate().Error())
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

		// 传入正确的对象（existingUser）
		if err := u.srv.Users().Update(c, existingUser,
			metav1.UpdateOptions{}, u.options); err != nil {
			log.Errorf("[control] 用户更新 service 层失败: username=%s, error=%v", username, err)
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

		// 构建成功数据
		successData := gin.H{
			"update_user":    existingUser,
			"operator":       common.GetUsername(ctx.Request.Context()),
			"operation_time": time.Now().Format(time.RFC3339),
			"operation_type": "create",
			"code":           code.ErrSuccess,
		}

		summaryFields := []string{}
		if req.Nickname != nil {
			summaryFields = append(summaryFields, "nickname")
		}
		if req.Email != nil {
			summaryFields = append(summaryFields, "email")
		}
		if req.Phone != nil {
			summaryFields = append(summaryFields, "phone")
		}
		if req.Status != nil {
			summaryFields = append(summaryFields, "status")
		}
		if req.IsAdmin != nil {
			summaryFields = append(summaryFields, "is_admin")
		}
		if req.LoginedAt != nil {
			summaryFields = append(summaryFields, "logined_at")
		}
		controllerDetails["updated_fields"] = summaryFields
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
