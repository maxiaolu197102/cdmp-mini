// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

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
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator/jwtvalidator"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// ChangePasswordRequest defines the ChangePasswordRequest data format.
type ChangePasswordRequest struct {
	// Old password.
	// Required: true
	OldPassword string `json:"oldPassword" binding:"omitempty"`

	// New password.
	// Required: true
	NewPassword string `json:"newPassword" binding:"password"`
}

// ChangePassword change the user's password by the user identifier.
func (u *UserController) ChangePassword(c *gin.Context) {
	log.L(c).Info("开始进行密码修改.")
	traceCtx := c.Request.Context()
	operator := common.GetUsername(traceCtx)
	username := c.Param("name")
	trace.SetOperator(traceCtx, operator)

	controllerCtx, controllerSpan := trace.StartSpan(traceCtx, "user-controller", "change_password")
	if controllerCtx != nil {
		c.Request = c.Request.WithContext(controllerCtx)
	}
	trace.SetOperator(controllerCtx, operator)
	trace.AddRequestTag(controllerCtx, "controller", "change_password")
	trace.AddRequestTag(controllerCtx, "target_user", username)

	controllerStatus := "success"
	controllerCode := strconv.Itoa(code.ErrSuccess)
	controllerDetails := map[string]any{
		"request_id":  c.Request.Header.Get("X-Request-ID"),
		"operator":    operator,
		"target_user": username,
	}
	defer func() {
		if controllerSpan != nil {
			trace.EndSpan(controllerSpan, controllerStatus, controllerCode, controllerDetails)
		}
	}()

	outcomeStatus := "success"
	outcomeCode := controllerCode
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK
	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(c.Request)
		event.Action = "user.change_password"
		event.ResourceType = "user"
		event.ResourceID = username
		event.Actor = operator
		event.Outcome = outcome
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(c, event)
	}

	err := metrics.MonitorBusinessOperation("user_service", "change_password", "http", func() error {
		var r ChangePasswordRequest

		if err := c.ShouldBindJSON(&r); err != nil {
			errBind := errors.WithCode(code.ErrBind, "ChangePasswordRequest结构体转换错误%s", err.Error())
			core.WriteResponse(c, errBind, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errBind))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errBind)
			outcomeHTTP = errors.GetHTTPStatus(errBind)
			auditLog("fail", errBind.Error())
			return errBind
		}

		if strings.TrimSpace(r.OldPassword) == "" || strings.TrimSpace(r.NewPassword) == "" {
			errEmpty := errors.WithCode(code.ErrInvalidParameter, "%s", "旧密码和新密码不能为空")
			core.WriteResponse(c, errEmpty, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errEmpty))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errEmpty)
			outcomeHTTP = errors.GetHTTPStatus(errEmpty)
			auditLog("fail", errEmpty.Error())
			return errEmpty
		}

		if r.OldPassword == r.NewPassword {
			errSame := errors.WithCode(code.ErrInvalidParameter, "%s", "旧密码和新密码不能相同")
			core.WriteResponse(c, errSame, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errSame))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errSame)
			outcomeHTTP = errors.GetHTTPStatus(errSame)
			auditLog("fail", errSame.Error())
			return errSame
		}

		if err := validation.IsValidPassword(r.NewPassword); err != nil {
			errInvalid := errors.WithCode(code.ErrInvalidParameter, "%s", "新密码不符合要求")
			core.WriteResponse(c, errInvalid, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errInvalid))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errInvalid)
			outcomeHTTP = errors.GetHTTPStatus(errInvalid)
			auditLog("fail", errInvalid.Error())
			return errInvalid
		}

		user, err := u.srv.Users().Get(c, c.Param("name"), metav1.GetOptions{}, u.options)
		if err != nil {
			core.WriteResponse(c, err, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			auditLog("fail", err.Error())
			return err
		}

		if err := user.Compare(r.OldPassword); err != nil {
			errCompare := errors.WithCode(code.ErrPasswordIncorrect, "%s", err.Error())
			core.WriteResponse(c, errCompare, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(errCompare))
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(errCompare)
			outcomeHTTP = errors.GetHTTPStatus(errCompare)
			auditLog("fail", errCompare.Error())
			return errCompare
		}

		hashCfg := auth.HashConfig{}
		if u.options != nil && u.options.ServerRunOptions != nil {
			hashCfg = u.options.ServerRunOptions.HashConfig()
		}
		user.Password, _ = auth.EncryptWithConfig(r.NewPassword, hashCfg)
		token := c.GetHeader("Authorization")
		claims, err := jwtvalidator.ValidateToken(token, u.options.JwtOptions.Key)
		if err != nil {
			core.WriteResponse(c, err, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			auditLog("fail", err.Error())
			return err
		}

		changeCtx := controllerCtx
		if changeCtx == nil {
			changeCtx = c.Request.Context()
		}
		if _, hasDeadline := changeCtx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeout := u.options.ServerRunOptions.CtxTimeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			changeCtx, cancel = context.WithTimeout(changeCtx, timeout)
			defer cancel()
		}

		if err := u.srv.Users().ChangePassword(changeCtx, user, claims, u.options); err != nil {
			core.WriteResponse(c, err, nil)
			controllerStatus = "error"
			controllerCode = strconv.Itoa(errors.GetCode(err))
			if controllerCode == "0" {
				controllerCode = strconv.Itoa(code.ErrUnknown)
			}
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			auditLog("fail", err.Error())
			return err
		}

		core.WriteResponse(c, nil, nil)
		controllerDetails["change_result"] = "success"
		auditLog("success", "")
		return nil
	})

	if err != nil && outcomeStatus == "success" {
		// 未在内部设置错误信息，则在此兜底
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
