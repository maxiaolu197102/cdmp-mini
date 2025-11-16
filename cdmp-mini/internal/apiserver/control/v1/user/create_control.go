package user

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	createcontrol "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/control/create"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// 使用 jsoniter 库来替换 Go 标准库的 encoding/json
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Create 处理用户创建的 HTTP 请求入口
// 利用通用控制层管道完成参数解析、校验、调用 service 层以及响应输出；
// 同时保留审计、链路追踪与指标上报，确保行为与历史实现一致。
func (u *UserController) Create(ctx *gin.Context) {
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

	handler := u.ensureCreateHandler()
	if handler == nil {
		err := errors.WithCode(code.ErrServerBusy, "创建控制流程未初始化")
		controllerStatus = "error"
		controllerCode = strconv.Itoa(errors.GetCode(err))
		outcomeStatus = "error"
		outcomeCode = controllerCode
		outcomeMessage = errors.GetMessage(err)
		outcomeHTTP = errors.GetHTTPStatus(err)
		if outcomeHTTP == 0 {
			outcomeHTTP = http.StatusInternalServerError
		}
		core.WriteResponse(ctx, err, nil)
		auditBase("fail", err.Error())
		trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
		return
	}

	execErr := metrics.MonitorBusinessOperation("user_service", "create", "http", func() error {
		result, err := handler.Execute(ctx)
		if err != nil {
			codeVal := errors.GetCode(err)
			if codeVal == 0 {
				codeVal = code.ErrUnknown
			}
			controllerStatus = "error"
			controllerCode = strconv.Itoa(codeVal)
			outcomeStatus = "error"
			outcomeCode = controllerCode
			outcomeMessage = errors.GetMessage(err)
			outcomeHTTP = errors.GetHTTPStatus(err)
			if outcomeHTTP == 0 {
				outcomeHTTP = http.StatusInternalServerError
			}
			auditBase("fail", err.Error())
			return err
		}

		outcomeMessage = "success"
		outcomeCode = strconv.Itoa(code.ErrSuccess)
		outcomeHTTP = http.StatusOK
		auditBase("success", "")

		if result.Entity != nil {
			controllerDetails["created_user"] = result.Entity.Name
			trace.AddRequestTag(controllerCtx, "created_user", result.Entity.Name)
		}

		awaitTimeout := u.createAwaitTimeout()
		if awaitTimeout > 0 {
			trace.ExpectAsync(controllerCtx, time.Now().Add(awaitTimeout))
		}
		return nil
	})

	if execErr != nil && outcomeStatus == "success" {
		codeVal := errors.GetCode(execErr)
		if codeVal == 0 {
			codeVal = code.ErrUnknown
		}
		outcomeStatus = "error"
		outcomeCode = strconv.Itoa(codeVal)
		outcomeMessage = errors.GetMessage(execErr)
		outcomeHTTP = errors.GetHTTPStatus(execErr)
		if outcomeHTTP == 0 {
			outcomeHTTP = http.StatusInternalServerError
		}
		controllerStatus = "error"
		controllerCode = outcomeCode
	}

	trace.RecordOutcome(controllerCtx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
}

func (u *UserController) ensureCreateHandler() *createcontrol.Handler[*v1.User] {
	if u == nil {
		return nil
	}
	if u.createHandler != nil {
		return u.createHandler
	}
	cfg := createcontrol.HandlerConfig[*v1.User]{
		Name:           "users",
		ReadBody:       u.readCreateRequestBody,
		Decode:         u.decodeCreateUser,
		Enhance:        u.enhanceCreateUser,
		Validate:       u.validateCreateUserEntity,
		Prepare:        u.prepareCreateUserEntity,
		WithTimeout:    u.createUserWithTimeout,
		InvokeService:  u.invokeCreateUserService,
		SuccessPayload: u.buildCreateUserResponse,
		ResponseWriter: u.writeCreateUserResponse,
	}
	u.createHandler = createcontrol.NewHandler[*v1.User](cfg)
	return u.createHandler
}

func (u *UserController) readCreateRequestBody(ctx *gin.Context) ([]byte, error) {
	if ctx == nil || ctx.Request == nil || ctx.Request.Body == nil {
		return nil, errors.WithCode(code.ErrBind, "请求体为空")
	}
	data, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.Errorw("读取请求体失败", "requestID", ctx.Request.Header.Get("X-Request-ID"), "error", err)
		return nil, errors.WithCode(code.ErrBind, "读取请求体失败:%v", err.Error())
	}
	//创建可重复读取的缓冲区
	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(data))
	return data, nil
}

func (u *UserController) decodeCreateUser(ctx *gin.Context, body []byte) (*v1.User, error) {
	var user v1.User
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, &user); err != nil {
		log.Errorw("请求体绑定结构体失败", "requestID", ctx.Request.Header.Get("X-Request-ID"), "error", err)
		return nil, errors.WithCode(code.ErrBind, "参数绑定失败:%v", err.Error())
	}
	return &user, nil
}

func (u *UserController) enhanceCreateUser(ctx *gin.Context, user *v1.User, body []byte) error {
	trace.AddRequestTag(ctx.Request.Context(), "requested_username", user.Name)
	trace.AddRequestTag(ctx.Request.Context(), "requested_email", user.Email)
	trace.AddRequestTag(ctx.Request.Context(), "requested_phone", user.Phone)

	var statusPayload struct {
		Status *int `json:"status"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &statusPayload); err != nil {
			log.Warnf("解析status字段失败: username=%s, err=%v", user.Name, err)
		}
	}
	if statusPayload.Status != nil {
		user.Status = *statusPayload.Status
	} else if user.Status == 0 {
		user.Status = 1
	}
	return nil
}

func (u *UserController) validateCreateUserEntity(ctx *gin.Context, user *v1.User) error {
	if user == nil {
		return errors.WithCode(code.ErrValidation, "用户实体为空")
	}
	username := strings.TrimSpace(user.Name)
	if username == "" {
		return errors.WithCode(code.ErrValidation, "用户名不能为空")
	}

	validationErrs := user.Validate()
	if len(validationErrs) == 0 {
		return nil
	}

	errDetails := make(map[string]string, len(validationErrs))
	for _, fieldErr := range validationErrs {
		errDetails[fieldErr.Field] = fieldErr.ErrorBody()
	}
	detailStr := fmt.Sprintf("参数校验失败: %+v", errDetails)
	log.Warnf("[control] 参数校验失败: username=%s, detail=%s", username, detailStr)
	return errors.WrapC(nil, code.ErrValidation, "%s", detailStr)
}

func (u *UserController) prepareCreateUserEntity(ctx *gin.Context, user *v1.User) error {
	if user == nil {
		return errors.WithCode(code.ErrValidation, "用户实体为空")
	}
	user.LoginedAt = time.Now()
	return nil
}

func (u *UserController) createUserWithTimeout(ctx *gin.Context, _ *v1.User) (context.Context, context.CancelFunc, error) {
	if ctx == nil || ctx.Request == nil {
		return nil, nil, nil
	}
	requestCtx := ctx.Request.Context()
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	if _, hasDeadline := requestCtx.Deadline(); hasDeadline {
		return requestCtx, nil, nil
	}

	timeout := u.createAwaitTimeout()
	newCtx, cancel := context.WithTimeout(requestCtx, timeout)
	return newCtx, cancel, nil
}

func (u *UserController) invokeCreateUserService(ctx context.Context, user *v1.User) error {
	if u == nil || u.srv == nil {
		return errors.WithCode(code.ErrServerBusy, "用户服务未初始化")
	}
	return u.srv.Users().Create(ctx, user, metav1.CreateOptions{}, u.options)
}

func (u *UserController) buildCreateUserResponse(ctx *gin.Context, user *v1.User) (interface{}, error) {
	publicUser := v1.ConvertToPublicUser(user)
	response := gin.H{
		"create_user":    publicUser.Username,
		"operator":       common.GetUsername(ctx.Request.Context()),
		"operation_time": time.Now().Format(time.RFC3339),
		"operation_type": "create",
		"code":           code.ErrSuccess,
	}
	return response, nil
}

func (u *UserController) writeCreateUserResponse(ctx *gin.Context, err error, payload interface{}) {
	core.WriteResponse(ctx, err, payload)
}

func (u *UserController) createAwaitTimeout() time.Duration {
	if u != nil && u.options != nil && u.options.ServerRunOptions != nil && u.options.ServerRunOptions.CtxTimeout > 0 {
		return u.options.ServerRunOptions.CtxTimeout
	}
	return 30 * time.Second
}
