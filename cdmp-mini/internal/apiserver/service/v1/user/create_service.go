package user

import (
	"context"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/userctx"

	"strconv"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) Create(ctx context.Context, user *v1.User, opts metav1.CreateOptions, opt *options.Options) (err error) {
	ctx, span := trace.StartSpan(ctx, "user-service", "create")
	ctx = userctx.WithCreateState(ctx)
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

	// 统一规整邮箱和手机号，确保后续索引和缓存命中
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	// 对密码进行加密，避免在控制层重复执行
	passwordStart := time.Now()
	if user.Password != "" {
		hashCfg := auth.HashConfig{}
		if u != nil && u.Options != nil && u.Options.ServerRunOptions != nil {
			hashCfg = u.Options.ServerRunOptions.HashConfig()
		}
		hashed, hashErr := auth.EncryptWithConfig(user.Password, hashCfg)
		u.recordUserCreateStep(ctx, "encrypt_password", "password", user.Name, time.Since(passwordStart), hashErr)
		if hashErr != nil {
			log.Errorf("用户密码加密失败: username=%s, err=%v", user.Name, hashErr)
			return errors.WithCode(code.ErrEncrypt, "用户密码加密失败")
		}
		user.Password = hashed
	} else {
		u.recordUserCreateStep(ctx, "encrypt_password", "password", user.Name, time.Since(passwordStart), nil)
	}

	u.ensureContactCacheReady()

	var (
		contactsErr       error
		existingUser      *v1.User
		existErr          error
		contactsDuration  time.Duration
		existenceDuration time.Duration
	)

	contactCtx, contactSpan := trace.StartSpan(ctx, "user-service", "ensure_contacts_unique")
	contactsStart := time.Now()
	contactHits, usernamePreflighted, errEnsure := u.ensureContactUniqueness(contactCtx, user)
	contactsDuration = time.Since(contactsStart)
	contactsErr = errEnsure
	contactStatus := "success"
	contactCode := strconv.Itoa(code.ErrSuccess)
	if contactsErr != nil {
		contactStatus = "error"
		if c := errors.GetCode(contactsErr); c != 0 {
			contactCode = strconv.Itoa(c)
		} else {
			contactCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	trace.EndSpan(contactSpan, contactStatus, contactCode, map[string]interface{}{
		"username":    user.Name,
		"duration_ms": contactsDuration.Milliseconds(),
	})

	if contactsErr != nil {
		err = contactsErr
		u.recordUserCreateStep(ctx, "ensure_contacts_unique", "all", user.Name, contactsDuration, contactsErr)
		return err
	}

	if contactHits != nil {
		if existing := contactHits["username"]; existing != nil {
			existingUser = existing
		}
	}

	if existingUser == nil && usernamePreflighted {
		u.recordUserCreateStep(ctx, "check_user_exist", "username", user.Name, 0, nil)
		trace.AddRequestTag(ctx, "username_preflight_verified", true)
	} else if existingUser == nil {
		checkCtx, checkSpan := trace.StartSpan(ctx, "user-service", "check_user_exist")
		existenceStart := time.Now()
		ruser, errCheck := u.checkUserExist(checkCtx, user.Name, false)
		existenceDuration = time.Since(existenceStart)
		existErr = errCheck
		if errCheck == nil {
			existingUser = ruser
		}
		status := "success"
		codeStr := strconv.Itoa(code.ErrSuccess)
		if errCheck != nil {
			status = "error"
			if c := errors.GetCode(errCheck); c != 0 {
				codeStr = strconv.Itoa(c)
			} else {
				codeStr = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(checkSpan, status, codeStr, map[string]interface{}{
			"username":    user.Name,
			"duration_ms": existenceDuration.Milliseconds(),
		})
	} else {
		u.recordUserCreateStep(ctx, "check_user_exist", "username", user.Name, 0, nil)
	}

	u.recordUserCreateStep(ctx, "ensure_contacts_unique", "all", user.Name, contactsDuration, contactsErr)
	if existingUser == nil {
		u.recordUserCreateStep(ctx, "check_user_exist", "username", user.Name, existenceDuration, existErr)
	}
	if existErr != nil {
		log.Warnf("查询用户%s checkUserExist方法返回错误, 可能是系统繁忙, 将忽略是否存在的检查, 放行该用户: %v", user.Name, existErr)
	}
	if existingUser != nil && existingUser.Name != RATE_LIMIT_PREVENTION {
		log.Warnf("用户%s已经存在,无法创建", user.Name)
		err = errors.WithCode(code.ErrUserAlreadyExist, "用户已经存在")
		return err
	}

	pendingStart := time.Now()
	pendingCtx, pendingSpan := trace.StartSpan(ctx, "user-service", "mark_pending_create")
	trace.AddRequestTag(pendingCtx, "username", user.Name)
	markerCreated, markerRefreshed, pendingTTL, setNXDuration, refreshDuration, pendingErr := u.markUserPendingCreate(pendingCtx, user.Name)
	pendingDuration := time.Since(pendingStart)
	pendingStatus := "success"
	pendingCode := strconv.Itoa(code.ErrSuccess)
	if pendingErr != nil {
		pendingStatus = "error"
		if c := errors.GetCode(pendingErr); c != 0 {
			pendingCode = strconv.Itoa(c)
		} else {
			pendingCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	trace.EndSpan(pendingSpan, pendingStatus, pendingCode, map[string]interface{}{
		"username":         user.Name,
		"duration_ms":      pendingDuration.Milliseconds(),
		"marker_new":       markerCreated,
		"marker_refresh":   markerRefreshed,
		"marker_ttl_ms":    pendingTTL.Milliseconds(),
		"redis_setnx_ms":   setNXDuration.Milliseconds(),
		"redis_refresh_ms": refreshDuration.Milliseconds(),
	})
	u.recordUserCreateStep(ctx, "mark_pending_create", "redis", user.Name, pendingDuration, pendingErr)
	trace.AddRequestTag(ctx, "pending_marker_new", markerCreated)
	if markerRefreshed {
		trace.AddRequestTag(ctx, "pending_marker_refreshed", true)
	}
	if pendingTTL > 0 {
		trace.AddRequestTag(ctx, "pending_marker_ttl_ms", pendingTTL.Milliseconds())
	}
	if pendingErr != nil {
		return pendingErr
	}

	if u.Producer == nil {
		log.Errorf("生产者转换错误")
		err = errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
		return err
	}
	// 发送到Kafka
	sendStart := time.Now()
	sendCtx, sendSpan := trace.StartSpan(ctx, "user-service", "producer_send_create")
	trace.AddRequestTag(sendCtx, "username", user.Name)
	errKafka := u.Producer.SendUserCreateMessage(sendCtx, user)
	u.recordUserCreateStep(ctx, "kafka_send_create_user", "kafka", user.Name, time.Since(sendStart), errKafka)
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
