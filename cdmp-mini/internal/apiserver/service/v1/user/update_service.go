package user

import (
	"context"
	stdErrors "errors"
	"strconv"
	"strings"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) Update(ctx context.Context, user *v1.User, opts metav1.UpdateOptions, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "update_submit")
	username := ""
	if user != nil {
		username = user.Name
	}
	trace.AddRequestTag(ctx, "username", username)
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanStatus := "success"
	defer func() {
		if err != nil {
			spanStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, spanStatus, businessCode, map[string]interface{}{
			"username": username,
		})
	}()

	if user == nil {
		err = errors.WithCode(code.ErrInvalidParameter, "用户信息为空")
		return err
	}

	_ = opts
	_ = opt

	mode := u.decideOperationMode(ctx, operation.OperationUpdate, user.Name)
	trace.AddRequestTag(ctx, "operation_mode", mode.String())

	if mode == OperationModeSync {
		err = u.processUserUpdate(ctx, user)
		return err
	}

	if err = u.ensureOperationPipeline(); err != nil {
		log.Errorw("初始化更新操作管道失败", "error", err)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	if regErr := u.registerOperationExecutor(operation.OperationUpdate, &userUpdateOperationExecutor{service: u}); regErr != nil {
		log.Errorw("注册用户更新执行器失败", "error", regErr)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	operationID := updateOperationID(user.Name)
	if strings.TrimSpace(operationID) == "" {
		err = errors.WithCode(code.ErrInvalidParameter, "用户名为空")
		return err
	}

	env, buildErr := u.buildUserOperationEnvelope(ctx, operation.OperationUpdate, operationID, user)
	if buildErr != nil {
		log.Errorw("构建用户更新操作包失败", "username", user.Name, "error", buildErr)
		err = errors.WithCode(code.ErrInvalidParameter, "构建请求失败: %v", buildErr)
		return err
	}

	if env.Headers == nil {
		env.Headers = make(map[string]string)
	}
	if pendingOwner := strings.TrimSpace(env.Headers[pendingOwnerHeader]); pendingOwner != "" {
		env.Headers[pendingOwnerHeader] = pendingOwner
	}
	if pendingBackend := strings.TrimSpace(env.Headers[pendingBackendHeader]); pendingBackend != "" {
		env.Headers[pendingBackendHeader] = pendingBackend
	}

	ticket, submitErr := u.operationPipeline.Submit(ctx, env)
	if submitErr != nil {
		log.Errorw("提交用户更新操作失败", "operation", operationID, "error", submitErr)
		err = errors.WithCode(code.ErrServerBusy, "提交更新请求失败")
		return err
	}
	if ticket != nil && strings.TrimSpace(ticket.OperationID) != "" {
		operationID = ticket.OperationID
	}

	if procErr := u.operationPipeline.ProcessOnce(ctx); procErr != nil {
		if stdErrors.Is(procErr, operation.ErrQueueEmpty) {
			return u.awaitOperationState(ctx, operationID, translateUpdateOperationFailure)
		}
		log.Warnw("同步处理用户更新操作失败", "operation", operationID, "error", procErr)
		return errors.WithCode(code.ErrServerBusy, "更新请求处理中，请稍后查询")
	}

	return nil
}

func (u *UserService) processUserUpdate(ctx context.Context, user *v1.User) (err error) {
	if u == nil {
		return errors.WithCode(code.ErrServerBusy, "用户服务未初始化")
	}
	if user == nil {
		return errors.WithCode(code.ErrInvalidParameter, "用户信息为空")
	}

	updateCtx, span := trace.StartSpan(ctx, "user-service", "update")
	trace.AddRequestTag(updateCtx, "username", user.Name)
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanStatus := "success"
	defer func() {
		if err != nil {
			spanStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, spanStatus, businessCode, map[string]interface{}{
			"username": user.Name,
		})
	}()

	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	checkCtx, checkSpan := trace.StartSpan(updateCtx, "user-service", "check_user_exist")
	ruser, existErr := u.checkUserExist(checkCtx, user.Name, true)
	spanStatusCheck := "success"
	spanCodeCheck := strconv.Itoa(code.ErrSuccess)
	if existErr != nil {
		log.Warnf("查询用户%s checkUserExist方法返回错误, 可能是系统繁忙, 将忽略是否存在的检查: %v", user.Name, existErr)
		spanStatusCheck = "error"
		if c := errors.GetCode(existErr); c != 0 {
			spanCodeCheck = strconv.Itoa(c)
		} else {
			spanCodeCheck = strconv.Itoa(code.ErrUnknown)
		}
	}
	if ruser != nil && (ruser.Name == RATE_LIMIT_PREVENTION || ruser.Name == BLACKLIST_SENTINEL) {
		err = errors.WithCode(code.ErrUserNotFound, "用户不存在,无法更新")
		spanStatusCheck = "error"
		spanCodeCheck = strconv.Itoa(code.ErrUserNotFound)
	}
	trace.EndSpan(checkSpan, spanStatusCheck, spanCodeCheck, map[string]interface{}{
		"username": user.Name,
	})
	if err != nil {
		return err
	}

	if u.Producer == nil {
		log.Errorf("生产者转换错误")
		err = errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
		return err
	}

	sendCtx, sendSpan := trace.StartSpan(updateCtx, "user-service", "producer_send_update")
	trace.AddRequestTag(sendCtx, "username", user.Name)
	errKafka := u.Producer.SendUserUpdateMessage(sendCtx, user)
	sendStatus := "success"
	sendCode := strconv.Itoa(code.ErrSuccess)
	if errKafka != nil {
		log.Errorf("requestID=%v: 生产者消息发送失败 username=%s, err=%v", updateCtx.Value("requestID"), user.Name, errKafka)
		sendStatus = "error"
		if c := errors.GetCode(errKafka); c != 0 {
			sendCode = strconv.Itoa(c)
		} else {
			sendCode = strconv.Itoa(code.ErrUnknown)
		}
		err = errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
	}
	trace.EndSpan(sendSpan, sendStatus, sendCode, map[string]interface{}{
		"username": user.Name,
	})
	if errKafka != nil {
		return err
	}

	return nil
}
