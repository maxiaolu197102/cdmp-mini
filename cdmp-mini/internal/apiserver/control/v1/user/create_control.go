// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package user

import (
	"context"
	"fmt"
	"io"
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
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"

	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func (u *UserController) Create(ctx *gin.Context) {
	// 从Gin上下文获取中间件设置的信息
	operator := common.GetUsername(ctx.Request.Context())
	traceCtx := ctx.Request.Context()
	trace.SetOperator(traceCtx, operator)
	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "create_user")
	ctx.Request = ctx.Request.WithContext(controllerCtx)
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "create_user")
	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]interface{}{
		"request_id": ctx.Request.Header.Get("X-Request-ID"),
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
	auditBase := func(outcome, message string) {
		event := audit.BuildEventFromRequest(ctx.Request)
		event.Action = "user.create"
		event.ResourceType = "user"
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(ctx, event)
	}
	err := metrics.MonitorBusinessOperation("user_service", "get", "http", func() error {
		body, err := io.ReadAll(ctx.Request.Body)
		if err != nil {
			log.Errorw("读取请求体失败", "requestID", ctx.Request.Header.Get("X-Request-ID"), "error", err)
			errBind := errors.WithCode(code.ErrBind, "读取请求体失败:%v", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errBind))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errBind)
			outcomeHTTP = errors.GetHTTPStatus(errBind)
			core.WriteResponse(ctx, errBind, nil)
			auditBase("fail", errBind.Error())
			return errBind
		}

		var r v1.User
		if err := json.Unmarshal(body, &r); err != nil {
			log.Errorw("请求体绑定结构体失败", "requestID", ctx.Request.Header.Get("X-Request-ID"), "error", err)
			errBind := errors.WithCode(code.ErrBind, "参数绑定失败:%v", err.Error())
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errBind))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errBind)
			outcomeHTTP = errors.GetHTTPStatus(errBind)
			core.WriteResponse(ctx, errBind, nil)
			auditBase("fail", errBind.Error())
			return errBind
		}

		trace.AddRequestTag(controllerCtx, "requested_username", r.Name)
		trace.AddRequestTag(controllerCtx, "requested_email", r.Email)
		trace.AddRequestTag(controllerCtx, "requested_phone", r.Phone)

		var statusPayload struct {
			Status *int `json:"status"`
		}
		if err := json.Unmarshal(body, &statusPayload); err != nil {
			log.Warnf("解析status字段失败: username=%s, err=%v", r.Name, err)
		}
		if statusPayload.Status != nil {
			r.Status = *statusPayload.Status
		} else if r.Status == 0 {
			r.Status = 1
		}

		username := r.Name

		if strings.TrimSpace(username) == "" {
			err := errors.WithCode(code.ErrValidation, "用户名不能为空")
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			core.WriteResponse(ctx, err, nil)
			auditBase("fail", err.Error())
			return err
		}

		validationErrs := r.Validate()
		if len(validationErrs) > 0 {
			errDetails := make(map[string]string, len(validationErrs))
			for _, fieldErr := range validationErrs {
				errDetails[fieldErr.Field] = fieldErr.ErrorBody()
			}
			detailsStr := fmt.Sprintf("参数校验失败: %+v", errDetails)
			log.Warnf("[control] 参数校验失败: username=%s, detail=%s", username, detailsStr)
			err := errors.WrapC(nil, code.ErrValidation, "%s", detailsStr)
			core.WriteResponse(ctx, err, nil)
			auditBase("fail", err.Error())
			return err
		}
		r.LoginedAt = time.Now()

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

		if err := u.srv.Users().Create(c, &r,
			metav1.CreateOptions{}, u.options); err != nil {
			log.Errorf("[control] 用户创建 service 层失败: username=%s, error=%v", r.Name, err)
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
			auditBase("fail", err.Error())
			return err
		}
		publicUser := v1.ConvertToPublicUser(&r)

		// 构建成功数据
		successData := gin.H{
			"create_user":    publicUser.Username,
			"operator":       operator,
			"operation_time": time.Now().Format(time.RFC3339),
			"operation_type": "create",
			"code":           code.ErrSuccess,
		}
		controllerDetails["created_user"] = publicUser.Username
		outcomeMessage = "success"
		awaitTimeout := 30 * time.Second
		if u.options != nil && u.options.ServerRunOptions != nil && u.options.ServerRunOptions.CtxTimeout > 0 {
			awaitTimeout = u.options.ServerRunOptions.CtxTimeout
		}
		trace.ExpectAsync(controllerCtx, time.Now().Add(awaitTimeout))

		core.WriteResponse(ctx, nil, successData)
		auditBase("success", "")
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		// 未在上游显式设置，兜底使用错误信息
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
