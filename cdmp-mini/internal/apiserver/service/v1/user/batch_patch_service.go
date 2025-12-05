package user

import (
	"context"
	stdErrors "errors"
	"strconv"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// BatchPatch 将批量补丁请求提交到异步操作管道。
func (u *UserService) BatchPatch(ctx context.Context, update *v1.User, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "batch_patch_submit")
	_ = opt

	businessCode := strconv.Itoa(code.ErrSuccess)
	spanStatus := "success"
	conditionCount := 0
	defer func() {
		if err != nil {
			spanStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, spanStatus, businessCode, map[string]any{
			"conditions": conditionCount,
		})
	}()

	if update == nil || update.Patch == nil || len(update.Conditions) == 0 {
		err = errors.WithCode(code.ErrInvalidParameter, "批量更新缺少更新内容或条件")
		return err
	}
	conditionCount = len(update.Conditions)
	trace.AddRequestTag(ctx, "batch_conditions", conditionCount)

	update.Command = v1.UserUpdateCommandBatch

	opID, idErr := batchOperationID(update)
	if idErr != nil {
		log.Errorw("计算批量操作指纹失败", "error", idErr)
		err = errors.WithCode(code.ErrInvalidParameter, "批量更新缺少必要信息: %v", idErr)
		return err
	}
	trace.AddRequestTag(ctx, "batch_operation_id", opID)
	update.Name = opID

	if err = u.ensureOperationPipeline(); err != nil {
		log.Errorw("初始化批量操作管道失败", "error", err)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	if regErr := u.registerOperationExecutor(operation.OperationBatch, &userBatchPatchOperationExecutor{service: u}); regErr != nil {
		log.Errorw("注册用户批量更新执行器失败", "error", regErr)
		err = errors.WithCode(code.ErrServerBusy, "异步管道不可用")
		return err
	}

	headers := map[string]string{
		"batch.operation_id": opID,
	}
	if conditionCount > 0 {
		headers["batch.conditions"] = strconv.Itoa(conditionCount)
	}
	headers["idempotency.key"] = opID

	env, buildErr := u.buildOperationEnvelope(ctx, operation.OperationBatch, opID, opID, update, headers)
	if buildErr != nil {
		log.Errorw("构建批量更新操作包失败", "operation", opID, "error", buildErr)
		err = errors.WithCode(code.ErrInvalidParameter, "构建请求失败: %v", buildErr)
		return err
	}

	ticket, submitErr := u.operationPipeline.Submit(ctx, env)
	if submitErr != nil {
		log.Errorw("提交批量更新操作失败", "operation", opID, "error", submitErr)
		err = errors.WithCode(code.ErrServerBusy, "提交批量更新请求失败")
		return err
	}
	if ticket != nil && ticket.OperationID != "" {
		opID = ticket.OperationID
	}

	if procErr := u.operationPipeline.ProcessOnce(ctx); procErr != nil {
		if stdErrors.Is(procErr, operation.ErrQueueEmpty) {
			return u.awaitOperationState(ctx, opID, translateBatchOperationFailure)
		}
		log.Warnw("同步处理批量更新操作失败", "operation", opID, "error", procErr)
		return errors.WithCode(code.ErrServerBusy, "批量更新请求处理中，请稍后查询")
	}

	return nil
}

func (u *UserService) processUserBatchPatch(ctx context.Context, update *v1.User) (err error) {
	if u == nil {
		return errors.WithCode(code.ErrServerBusy, "用户服务未初始化")
	}
	if update == nil {
		return errors.WithCode(code.ErrInvalidParameter, "批量更新请求为空")
	}

	processCtx, span := trace.StartSpan(ctx, "user-service", "batch_patch")
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanStatus := "success"
	conditionCount := len(update.Conditions)
	defer func() {
		if err != nil {
			spanStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, spanStatus, businessCode, map[string]any{
			"conditions": conditionCount,
		})
	}()

	if update.Patch == nil || len(update.Conditions) == 0 {
		err = errors.WithCode(code.ErrInvalidParameter, "批量更新缺少更新内容或条件")
		return err
	}

	if u.Producer == nil {
		log.Errorf("批量更新失败: Kafka 生产者未初始化")
		err = errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
		return err
	}

	update.Email = usercache.NormalizeEmail(update.Email)
	update.Phone = usercache.NormalizePhone(update.Phone)
	update.Command = v1.UserUpdateCommandBatch

	if errKafka := u.Producer.SendUpdateMessage(processCtx, update); errKafka != nil {
		log.Errorf("批量更新消息发送失败: %v", errKafka)
		err = errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
		return err
	}

	return nil
}
