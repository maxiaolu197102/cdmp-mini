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

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserController) Delete(ctx *gin.Context) {

	
}

func (u *UserController) ForceDelete(ctx *gin.Context) {

	// 校验：提取并检查待删除用户名（参数无效场景）
	traceCtx := ctx.Request.Context()
	operator := common.GetUsername(traceCtx)
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "delete_user")
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.AddRequestTag(controllerCtx, "controller", "delete_user")
	trace.AddRequestTag(controllerCtx, "target_user", ctx.Param("name"))
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

	deleteUsername := ctx.Param("name")
	controllerDetails["target_user"] = deleteUsername
	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.delete"
		event.ResourceType = "user"
		event.ResourceID = deleteUsername
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}
	err := metrics.MonitorBusinessOperation("user_service", "delete_user", "http", func() error {
		if errs := validation.IsQualifiedName(deleteUsername); len(errs) > 0 {
			errsMsg := strings.Join(errs, ":")
			log.Warnf("[control] 用户名不合法: username=%s, error=%s", deleteUsername, errsMsg)
			err := errors.WithCode(code.ErrInvalidParameter, "用户名不合法:%s", errsMsg)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			outcomeStatus := "error"
			outcomeCode := controllerCode
			outcomeMessage := errors.GetMessage(err)
			outcomeHTTP := errors.GetHTTPStatus(err)
			trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
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

		rawDelErr := u.srv.Users().Delete(
			c,
			deleteUsername,
			true, // force=true：强制删除
			metav1.DeleteOptions{Unscoped: true},
			u.options,
		)
		if rawDelErr != nil {
			log.Errorf("[control] 用户强制删除 service 层失败: username=%s, error=%v", deleteUsername, rawDelErr)
			err := errors.WrapC(
				rawDelErr,
				code.ErrInternalServer,
				"用户[%s]强制删除失败，请稍后重试",
				deleteUsername,
			)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "-1" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus := "error"
			outcomeCode := controllerCode
			outcomeMessage := errors.GetMessage(err)
			outcomeHTTP := errors.GetHTTPStatus(err)
			trace.RecordOutcome(traceCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
			core.WriteResponse(ctx, err, nil)
			auditLog("fail", err.Error())
			return err
		}

		//成功场景：返回 RESTful 标准 204 No Content（无响应体）

		// 构建成功数据
		successData := gin.H{
			"delete_user":    deleteUsername,
			"operator":       operator,
			"operation_time": time.Now().Format(time.RFC3339),
			"operation_type": "delete",
		}

		controllerDetails["operator"] = operator
		awaitTimeout := 30 * time.Second
		if u.options != nil && u.options.ServerRunOptions != nil && u.options.ServerRunOptions.CtxTimeout > 0 {
			awaitTimeout = u.options.ServerRunOptions.CtxTimeout
		}
		trace.ExpectAsync(controllerCtx, time.Now().Add(awaitTimeout))
		core.WriteResponse(ctx, nil, successData)
		auditLog("success", "")
		trace.RecordOutcome(controllerCtx, strconv.Itoa(code.ErrSuccess), "success", "success", http.StatusOK)
		return nil
	})

	if err != nil && controllerStatus == "success" {
		controllerStatus = "error"
		controllerCode = strconv.Itoa(errors.GetCode(err))
		if controllerCode == "-1" {
			controllerCode = strconv.Itoa(code.ErrUnknown)
		}
		trace.RecordOutcome(controllerCtx, controllerCode, errors.GetMessage(err), "error", errors.GetHTTPStatus(err))
	}
}
