package user

import (
	"context"
	"strconv"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// BatchPatch 将批量补丁请求通过 Kafka 发送给异步消费端。
func (u *UserService) BatchPatch(ctx context.Context, update *v1.User, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "batch_patch")
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

	if u.Producer == nil {
		log.Errorf("批量更新失败: Kafka 生产者未初始化")
		err = errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
		return err
	}

	update.Email = usercache.NormalizeEmail(update.Email)
	update.Phone = usercache.NormalizePhone(update.Phone)
	update.Command = v1.UserUpdateCommandBatch

	if errKafka := u.Producer.SendUserUpdateMessage(ctx, update); errKafka != nil {
		log.Errorf("批量更新消息发送失败: %v", errKafka)
		err = errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
		return err
	}

	return nil
}
