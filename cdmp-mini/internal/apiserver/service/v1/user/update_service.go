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
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) Update(ctx context.Context, user *v1.User, opts metav1.UpdateOptions, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "update")
	trace.AddRequestTag(ctx, "username", user.Name)
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

	//判断用户是否存在
	checkCtx, checkSpan := trace.StartSpan(ctx, "user-service", "check_user_exist")
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
	// 发送到Kafka
	sendCtx, sendSpan := trace.StartSpan(ctx, "user-service", "producer_send_update")
	trace.AddRequestTag(sendCtx, "username", user.Name)
	errKafka := u.Producer.SendUserUpdateMessage(sendCtx, user)
	sendStatus := "success"
	sendCode := strconv.Itoa(code.ErrSuccess)
	if errKafka != nil {
		log.Errorf("requestID=%v: 生产者消息发送失败 username=%s, err=%v", ctx.Value("requestID"), user.Name, errKafka)
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
