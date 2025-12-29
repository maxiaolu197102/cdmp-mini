package user

import (
	"context"
	stdErrors "errors"
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type userDeleteOperationPayload struct {
	Username string               `json:"username"`
	Force    bool                 `json:"force"`
	Options  metav1.DeleteOptions `json:"options"`
}

func (u *UserService) DeleteCollection(ctx context.Context, username []string, force bool, opts metav1.DeleteOptions, opt *options.Options) error {
	ctx = WithBatchLookupCache(ctx)
	trace.AddRequestTag(ctx, "batch_lookup_cache", "enabled")

	//检查用户是否存在

	//判断用户是否存在
	for _, name := range username {
		verifyCtx := WithVerifyUserGone(ctx)
		ruser, err := u.checkUserExist(verifyCtx, name, true)
		if err != nil {
			log.Debugw("batch delete check failed", "username", name, "error", err)
			continue
		}
		if ruser == nil || ruser.Name == RATE_LIMIT_PREVENTION || ruser.Name == BLACKLIST_SENTINEL {
			continue
		}
		_ = u.Delete(ctx, name, true, opts, opt)
	}
	return nil
}

func (u *UserService) Delete(ctx context.Context, username string, force bool, opts metav1.DeleteOptions, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "delete_submit")
	trimmedName := strings.TrimSpace(username)
	trace.AddRequestTag(ctx, "username", trimmedName)
	trace.AddRequestTag(ctx, "delete_force", force)
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
			"username": trimmedName,
			"force":    force,
		})
	}()

	if trimmedName == "" {
		err = errors.WithCode(code.ErrInvalidParameter, "用户名为空")
		return err
	}

	_ = opt

	payload := userDeleteOperationPayload{
		Username: trimmedName,
		Force:    force,
		Options:  opts,
	}

	mode := u.decideOperationMode(ctx, operation.OperationDelete, trimmedName)
	trace.AddRequestTag(ctx, "operation_mode", mode.String())

	if mode == OperationModeSync {
		return u.processUserDelete(ctx, &payload)
	}

	if err = u.ensureOperationPipeline(); err != nil {
		log.Errorw("初始化删除操作管道失败", "username", trimmedName, "error", err)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	if regErr := u.registerOperationExecutor(operation.OperationDelete, &userDeleteOperationExecutor{service: u}); regErr != nil {
		log.Errorw("注册用户删除执行器失败", "username", trimmedName, "error", regErr)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	opID := deleteOperationID(trimmedName, force)
	if strings.TrimSpace(opID) == "" {
		err = errors.WithCode(code.ErrInvalidParameter, "删除请求缺少操作ID")
		return err
	}

	env, buildErr := u.buildOperationEnvelope(ctx, operation.OperationDelete, opID, trimmedName, payload, nil)
	if buildErr != nil {
		log.Errorw("构建用户删除操作包失败", "username", trimmedName, "error", buildErr)
		err = errors.WithCode(code.ErrInvalidParameter, "构建请求失败: %v", buildErr)
		return err
	}

	// 队列进入降级态时，直接拒绝入队，交由调用方重试，避免在服务端隐式切换到不可靠队列实现。
	if u.isRedisDegradeActive() {
		log.Errorw("用户删除操作队列处于降级状态，拒绝入队", "operation", opID)
		err = errors.WithCode(code.ErrServerBusy, "删除队列暂不可用，请稍后重试")
		return err
	}

	ticket, submitErr := u.operationPipeline.Submit(ctx, env)
	if submitErr != nil {
		log.Errorw("提交用户删除操作失败", "operation", opID, "error", submitErr)
		err = errors.WithCode(code.ErrServerBusy, "提交删除请求失败")
		return err
	}
	if ticket != nil && strings.TrimSpace(ticket.OperationID) != "" {
		opID = ticket.OperationID
	}

	if procErr := u.operationPipeline.ProcessOnce(ctx); procErr != nil {
		if stdErrors.Is(procErr, operation.ErrQueueEmpty) {
			return u.awaitOperationState(ctx, opID, translateDeleteOperationFailure)
		}
		log.Warnw("同步处理用户删除操作失败", "operation", opID, "error", procErr)
		return errors.WithCode(code.ErrServerBusy, "删除请求处理中，请稍后查询")
	}

	return nil
}

func (u *UserService) processUserDelete(ctx context.Context, payload *userDeleteOperationPayload) (err error) {
	if u == nil {
		return errors.WithCode(code.ErrServerBusy, "用户服务未初始化")
	}
	if payload == nil {
		return errors.WithCode(code.ErrInvalidParameter, "删除请求为空")
	}

	ctx = WithBatchLookupCache(ctx)
	username := strings.TrimSpace(payload.Username)
	if username == "" {
		return errors.WithCode(code.ErrInvalidParameter, "用户名为空")
	}

	deleteCtx, span := trace.StartSpan(ctx, "user-service", "delete")
	trace.AddRequestTag(deleteCtx, "username", username)
	trace.AddRequestTag(deleteCtx, "delete_force", payload.Force)
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
			"force":    payload.Force,
		})
	}()

	//检查用户是否存在
	checkCtx, checkSpan := trace.StartSpan(deleteCtx, "user-service", "check_user_exist")
	checkCtx = WithVerifyUserGone(checkCtx)
	checkStart := time.Now()
	ruser, existErr := u.checkUserExist(checkCtx, username, true)
	spanStatusCheck := "success"
	spanCodeCheck := strconv.Itoa(code.ErrSuccess)
	notFound := false

	if existErr != nil {
		if isUserNotFoundErr(existErr) {
			notFound = true
			trace.AddRequestTag(deleteCtx, "check_exist_result", "not_found")
		} else {
			log.Warnf("查询用户%s checkUserExist方法返回错误: %v", username, existErr)
			spanStatusCheck = "error"
			if c := errors.GetCode(existErr); c != 0 {
				spanCodeCheck = strconv.Itoa(c)
			} else {
				spanCodeCheck = strconv.Itoa(code.ErrUnknown)
			}
			err = existErr
		}
	}
	if err == nil {
		if ruser == nil {
			notFound = true
		} else if ruser.Name == RATE_LIMIT_PREVENTION || ruser.Name == BLACKLIST_SENTINEL {
			notFound = true
		}
	}
	if notFound {
		spanStatusCheck = "success"
		spanCodeCheck = strconv.Itoa(code.ErrSuccess)
	}

	trace.EndSpan(checkSpan, spanStatusCheck, spanCodeCheck, map[string]interface{}{
		"username":  username,
		"not_found": notFound,
	})
	u.recordUserCreateStep(deleteCtx, "delete_check_user_exist", "username", username, time.Since(checkStart), existErr)
	if err != nil {
		return err
	}

	if notFound {
		if !payload.Force {
			err = errors.WithCode(code.ErrUserNotFound, "用户不存在,无法删除")
			return err
		}
		trace.AddRequestTag(deleteCtx, "delete_idempotent_skip", "true")
		return nil
	}

	existingUser := ruser
	if payload.Force {
		if u.Producer == nil {
			log.Errorf("生产者转换错误")
			return errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
		}
		sendCtx, sendSpan := trace.StartSpan(deleteCtx, "user-service", "producer_send_delete")
		trace.AddRequestTag(sendCtx, "username", username)
		sendStart := time.Now()
		sendErr := u.Producer.SendDeleteMessage(sendCtx, username)
		sendStatus := "success"
		sendCode := strconv.Itoa(code.ErrSuccess)
		if sendErr != nil {
			log.Errorf("requestID=%s: 生产者消息发送失败 username=%s, err=%v", deleteCtx.Value("requestID"), username, sendErr)
			sendStatus = "error"
			if c := errors.GetCode(sendErr); c != 0 {
				sendCode = strconv.Itoa(c)
			} else {
				sendCode = strconv.Itoa(code.ErrUnknown)
			}
			err = errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
		}
		sendDuration := time.Since(sendStart)
		trace.EndSpan(sendSpan, sendStatus, sendCode, map[string]interface{}{
			"username": username,
		})
		u.recordUserCreateStep(deleteCtx, "kafka_send_delete_user", "kafka", username, sendDuration, sendErr)
		if sendErr != nil {
			return err
		}
		return nil
	}

	if existingUser == nil {
		fetchStart := time.Now()
		fetched, fetchErr := u.fetchUserSnapshot(deleteCtx, username)
		u.recordUserCreateStep(deleteCtx, "delete_fetch_snapshot", "database", username, time.Since(fetchStart), fetchErr)
		if fetchErr != nil {
			log.Warnw("获取用户快照失败", "username", username, "error", fetchErr)
		} else if fetched != nil {
			existingUser = fetched
		}
	}

	if u.Producer == nil {
		log.Errorf("生产者转换错误")
		return errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
	}

	sendCtx, sendSpan := trace.StartSpan(deleteCtx, "user-service", "producer_send_delete")
	trace.AddRequestTag(sendCtx, "username", username)
	sendStart := time.Now()
	sendErr := u.Producer.SendDeleteMessage(sendCtx, username)
	sendStatus := "success"
	sendCode := strconv.Itoa(code.ErrSuccess)
	if sendErr != nil {
		log.Errorf("requestID=%s: 生产者消息发送失败 username=%s, err=%v", deleteCtx.Value("requestID"), username, sendErr)
		sendStatus = "error"
		if c := errors.GetCode(sendErr); c != 0 {
			sendCode = strconv.Itoa(c)
		} else {
			sendCode = strconv.Itoa(code.ErrUnknown)
		}
		err = errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
	}
	sendDuration := time.Since(sendStart)
	trace.EndSpan(sendSpan, sendStatus, sendCode, map[string]interface{}{
		"username": username,
		"force":    false,
	})
	u.recordUserCreateStep(deleteCtx, "kafka_send_delete_user", "kafka", username, sendDuration, sendErr)
	if sendErr != nil {
		return err
	}

	cleanupStart := time.Now()
	cleanupErr := u.cleanupUserStateForDelete(deleteCtx, username, existingUser)
	if cleanupErr != nil {
		trace.AddRequestTag(deleteCtx, "delete_cleanup_error", cleanupErr.Error())
		log.Warnw("删除用户清理缓存失败", "username", username, "error", cleanupErr)
	} else {
		trace.AddRequestTag(deleteCtx, "delete_cleanup_success", true)
	}
	u.recordUserCreateStep(deleteCtx, "delete_cleanup_state", "cache", username, time.Since(cleanupStart), cleanupErr)

	return nil
}

func isUserNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	visited := map[error]struct{}{}
	current := err

	for current != nil {
		if errors.IsCode(current, code.ErrUserNotFound) {
			return true
		}
		visited[current] = struct{}{}

		var next error
		if cause := errors.Cause(current); cause != nil && cause != current {
			next = cause
		} else if unwrapped := stdErrors.Unwrap(current); unwrapped != nil && unwrapped != current {
			next = unwrapped
		}

		if next == nil {
			break
		}
		if _, seen := visited[next]; seen {
			break
		}
		current = next
	}

	return false
}

func (u *UserService) cleanupUserStateForDelete(ctx context.Context, username string, user *v1.User) error {
	if u == nil {
		return nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil
	}

	if user == nil {
		user = &v1.User{}
	}
	if strings.TrimSpace(user.Name) == "" {
		user.Name = trimmed
	}

	u.normalizeUserContacts(user)

	var errs []error
	if err := u.compensateEvictUserCache(ctx, user); err != nil {
		errs = append(errs, err)
	}
	if err := u.compensateClearContactCaches(ctx, user); err != nil {
		errs = append(errs, err)
	}
	if err := u.cacheNullValue(ctx, trimmed, 0); err != nil {
		errs = append(errs, err)
	}

	if len(errs) == 0 {
		return nil
	}
	return stdErrors.Join(errs...)
}
