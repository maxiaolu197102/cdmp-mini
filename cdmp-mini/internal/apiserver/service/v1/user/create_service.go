package user

import (
	"context"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	createpipeline "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/create"
	operation "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/userctx"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

const operationInlineProcessBudget = 150 * time.Millisecond

// Create 处理用户创建请求的主流程
//
// 调用通用创建管道完成字段归一化、唯一性校验、pending 标记和 Kafka 发送；
// 通过 createPipeline 提供的钩子保持与历史逻辑一致，实现可配置的表级复用模式。
//
// param ctx: 上下文，需传入调用方请求上下文，包含 trace、超时时间等控制信息。
// param user: 待创建的用户实体，要求姓名、密码、邮箱/手机在进入函数前已做基本校验。
// param opts: 标准创建选项，目前未使用，保留兼容性。
// param opt: 服务运行期配置选项，目前未使用，保留兼容性。
//
// returns: 若创建成功返回 nil；任一步骤失败则返回携带业务码的错误。
func (u *UserService) Create(ctx context.Context, user *v1.User, opts metav1.CreateOptions, opt *options.Options) error {
	if u == nil {
		return errors.WithCode(code.ErrServerBusy, "用户服务未初始化")
	}
	if user == nil {
		return errors.WithCode(code.ErrInvalidParameter, "用户信息为空")
	}

	// 保留参数以兼容接口签名
	_ = opts
	_ = opt

	mode := u.decideOperationMode(ctx, operation.OperationCreate, user.Name)
	trace.AddRequestTag(ctx, "operation_mode", mode.String())

	if mode == OperationModeSync {
		if u.createPipeline == nil {
			u.initCreatePipeline()
		}
		if u.createPipeline == nil {
			log.Errorw("create pipeline missing in sync mode", "component", "user_service")
			return errors.WithCode(code.ErrServerBusy, "同步创建流程不可用")
		}
		if err := u.createPipeline.Execute(ctx, user); err != nil {
			return err
		}
		return nil
	}

	if err := u.ensureOperationPipeline(); err != nil {
		log.Errorw("初始化创建操作管道失败", "error", err)
		return errors.WithCode(code.ErrServerBusy, "异步管道不可用")
	}

	env, err := u.buildUserOperationEnvelope(ctx, operation.OperationCreate, user.Name, user)
	if err != nil {
		log.Errorw("构建操作包失败", "error", err)
		return errors.WithCode(code.ErrInvalidParameter, "构建请求失败: %v", err)
	}

	if _, err := u.operationPipeline.Submit(ctx, env); err != nil {
		log.Errorw("提交用户创建操作失败", "operation", env.ID, "error", err)
		return errors.WithCode(code.ErrServerBusy, "提交创建请求失败")
	}

	if inlineErr := u.processOperationInlineWithBudget(ctx, operationInlineProcessBudget); inlineErr != nil {
		log.Warnw("inline processing create operation failed", "operation", env.ID, "error", inlineErr)
		return errors.WithCode(code.ErrServerBusy, "创建请求已入队，请稍后查询状态")
	}

	return nil
}

// initCreatePipeline 根据用户表规则初始化通用创建管道。
//
// note: 该函数幂等，重复调用不会重新分配管道实例。
func (u *UserService) initCreatePipeline() {
	if u == nil || u.createPipeline != nil {
		return
	}

	cfg := createpipeline.PipelineConfig[*v1.User]{
		Name:              "users",
		Begin:             u.createBeginHook,
		Normalize:         u.normalizeUserForCreate,
		BeforeUnique:      u.prepareUserForCreate,
		EnsureUnique:      u.ensureUserUnique,
		ResolveExistence:  u.resolveUserExistence,
		HandleExisting:    u.handleUserExisting,
		MarkPending:       u.markUserPendingForCreate,
		AfterPending:      u.afterUserPending,
		SendCreateMessage: u.sendUserCreateMessage,
	}

	u.createPipeline = createpipeline.NewPipeline[*v1.User](cfg)
}

// createBeginHook 创建顶层链路追踪并返回收尾函数。
//
// param ctx: 调用方上下文。
// param user: 当前待创建的用户。
//
// returns: 带有创建状态的新上下文及结束函数。
func (u *UserService) createBeginHook(ctx context.Context, user *v1.User) (context.Context, func(error)) {
	spanCtx, span := trace.StartSpan(ctx, "user-service", "create")
	// createState adds a per-request degraded flag; it remains false until MarkCreateDegraded marks the request.
	spanCtx = userctx.WithCreateState(spanCtx)
	trace.AddRequestTag(spanCtx, "username", user.Name)

	businessCode := strconv.Itoa(code.ErrSuccess)
	spanStatus := "success"

	end := func(execErr error) {
		if execErr != nil {
			spanStatus = "error"
			if c := errors.GetCode(execErr); c != 0 {
				businessCode = strconv.Itoa(c)
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, spanStatus, businessCode, map[string]interface{}{
			"username": user.Name,
		})
	}

	return spanCtx, end
}

// normalizeUserForCreate 统一规整联系方式字段。
//
// param user: 当前待创建的用户。
func (u *UserService) normalizeUserForCreate(user *v1.User) {
	if user == nil {
		return
	}
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
}

// prepareUserForCreate 在唯一性校验前执行密码加密和缓存预热。
//
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
//
// returns: 成功返回 nil，失败时返回 ErrEncrypt 或相关错误。
func (u *UserService) prepareUserForCreate(ctx context.Context, user *v1.User) error {
	if user == nil {
		return errors.WithCode(code.ErrInvalidParameter, "用户信息为空")
	}

	passwordStart := time.Now()
	if user.Password != "" {
		hashCfg := auth.HashConfig{}
		if u.Options != nil && u.Options.ServerRunOptions != nil {
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
	// 预热缓存
	u.ensureContactCacheReady()
	return nil
}

// ensureUserUnique 用户唯一性检查
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
//
// returns: 返回预检结果与错误信息。
func (u *UserService) ensureUserUnique(ctx context.Context, user *v1.User) (createpipeline.PreflightResult[*v1.User], error) {
	result := createpipeline.PreflightResult[*v1.User]{
		Conflicts: make(map[string]*v1.User),
	}

	ensureCtx, span := trace.StartSpan(ctx, "user-service", "ensure_contacts_unique")
	start := time.Now()
	conflicts, usernameChecked, err := u.ensureContactUniqueness(ensureCtx, user)
	duration := time.Since(start)

	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	if err != nil {
		status = "error"
		if c := errors.GetCode(err); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrUnknown)
		}
	}

	trace.EndSpan(span, status, codeStr, map[string]interface{}{
		"username":    user.Name,
		"duration_ms": duration.Milliseconds(),
	})
	u.recordUserCreateStep(ctx, "ensure_contacts_unique", "all", user.Name, duration, err)

	if err != nil {
		return result, err
	}

	if conflicts != nil {
		result.Conflicts = conflicts
	}
	result.UsernameChecked = usernameChecked
	return result, nil
}

// resolveUserExistence 处理用户名存在性的兜底查询。
//
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
// param preflight: 唯一性预检结果。
//
// returns: 若已存在返回实体指针，未命中返回 nil，异常时返回 error。
func (u *UserService) resolveUserExistence(ctx context.Context, user *v1.User, preflight createpipeline.PreflightResult[*v1.User]) (*v1.User, error) {
	if preflight.Conflicts != nil {
		if existing := preflight.Conflicts["username"]; existing != nil {
			return existing, nil
		}
	}

	if preflight.UsernameChecked {
		u.recordUserCreateStep(ctx, "check_user_exist", "username", user.Name, 0, nil)
		trace.AddRequestTag(ctx, "username_preflight_verified", true)
		return nil, nil
	}

	checkCtx, span := trace.StartSpan(ctx, "user-service", "check_user_exist")
	start := time.Now()
	existing, err := u.checkUserExist(checkCtx, user.Name, false)
	duration := time.Since(start)

	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	if err != nil {
		status = "error"
		if c := errors.GetCode(err); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrUnknown)
		}
	}

	trace.EndSpan(span, status, codeStr, map[string]interface{}{
		"username":    user.Name,
		"duration_ms": duration.Milliseconds(),
	})

	u.recordUserCreateStep(ctx, "check_user_exist", "username", user.Name, duration, err)
	if err != nil {
		log.Warnf("查询用户%s checkUserExist方法返回错误, 可能是系统繁忙, 将忽略是否存在的检查, 放行该用户: %v", user.Name, err)
		return nil, nil
	}

	return existing, nil
}

// handleUserExisting 在确认实体已存在时返回业务冲突错误。
//
// param user: 当前待创建的用户。
// param existing: 已存在的用户实体。
//
// returns: 冲突时返回 ErrUserAlreadyExist，其他情况返回 nil。
func (u *UserService) handleUserExisting(user *v1.User, existing *v1.User) error {
	if existing == nil {
		return nil
	}
	if existing.Name == RATE_LIMIT_PREVENTION {
		return nil
	}

	log.Warnf("用户%s已经存在,无法创建", user.Name)
	return errors.WithCode(code.ErrUserAlreadyExist, "用户已经存在")
}

// markUserPendingForCreate 写入 pending 占位并返回占位信息。
//
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
//
// returns: PendingResult 描述占位详情。
func (u *UserService) markUserPendingForCreate(ctx context.Context, user *v1.User) (createpipeline.PendingResult, error) {
	pendingCtx, span := trace.StartSpan(ctx, "user-service", "mark_pending_create")
	trace.AddRequestTag(pendingCtx, "username", user.Name)

	start := time.Now()
	//建立create状态
	created, refreshed, ttl, setNXDuration, refreshDuration, ownerID, backend, err := u.markUserPendingCreate(pendingCtx, user.Name)
	duration := time.Since(start)

	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	if err != nil {
		status = "error"
		if c := errors.GetCode(err); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrUnknown)
		}
	}

	trace.EndSpan(span, status, codeStr, map[string]interface{}{
		"username":         user.Name,
		"duration_ms":      duration.Milliseconds(),
		"marker_new":       created,
		"marker_refresh":   refreshed,
		"marker_ttl_ms":    ttl.Milliseconds(),
		"redis_setnx_ms":   setNXDuration.Milliseconds(),
		"redis_refresh_ms": refreshDuration.Milliseconds(),
	})
	u.recordUserCreateStep(ctx, "mark_pending_create", "redis", user.Name, duration, err)

	if err != nil {
		trace.AddRequestTag(ctx, "pending_marker_error", err.Error())
		if shouldDegradeForError(err) {
			u.markCreateDegraded(ctx, redisDegradeReasonPlaceholder, "username", user.Name, "error", err.Error())
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues("user_service", "acquire_degraded").Inc()
			}
			return createpipeline.PendingResult{}, nil
		}
		return createpipeline.PendingResult{}, err
	}

	return createpipeline.PendingResult{
		Created:         created,
		Refreshed:       refreshed,
		TTL:             ttl,
		SetDuration:     setNXDuration,
		RefreshDuration: refreshDuration,
		OwnerID:         ownerID,
		Backend:         backend,
	}, nil
}

// afterUserPending 在 pending 成功后写入 Trace 标签。
//
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
// param pending: 占位结果元信息。
func (u *UserService) afterUserPending(ctx context.Context, user *v1.User, pending createpipeline.PendingResult) {
	trace.AddRequestTag(ctx, "pending_marker_new", pending.Created)
	if pending.Refreshed {
		trace.AddRequestTag(ctx, "pending_marker_refreshed", true)
	}
	if pending.TTL > 0 {
		trace.AddRequestTag(ctx, "pending_marker_ttl_ms", pending.TTL.Milliseconds())
	}
	if pending.Backend != "" {
		trace.AddRequestTag(ctx, "pending_backend", pending.Backend)
	}
	if pending.OwnerID != "" {
		trace.AddRequestTag(ctx, "pending_lease_owner", pending.OwnerID)
		if env, ok := operationEnvelopeFromContext(ctx); ok && env != nil {
			if env.Headers == nil {
				env.Headers = make(map[string]string)
			}
			env.Headers[pendingOwnerHeader] = pending.OwnerID
			if pending.Backend != "" {
				env.Headers[pendingBackendHeader] = pending.Backend
			}
		}
	}
}

// sendUserCreateMessage 将创建事件发送至 Kafka。
//
// param ctx: 链路上下文。
// param user: 当前待创建的用户。
//
// returns: 发送失败时返回 ErrKafkaFailed。
func (u *UserService) sendUserCreateMessage(ctx context.Context, user *v1.User) error {
	if u.Producer == nil {
		log.Errorf("生产者转换错误")
		return errors.WithCode(code.ErrKafkaFailed, "Kafka生产者未初始化")
	}

	sendStart := time.Now()
	sendCtx, span := trace.StartSpan(ctx, "user-service", "producer_send_create")
	trace.AddRequestTag(sendCtx, "username", user.Name)

	errKafka := u.Producer.SendCreateMessage(sendCtx, user)
	u.recordUserCreateStep(ctx, "kafka_send_create_user", "kafka", user.Name, time.Since(sendStart), errKafka)

	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	if errKafka != nil {
		log.Errorf("requestID=%v: 生产者消息发送失败 username=%s, err=%v", ctx.Value("requestID"), user.Name, errKafka)
		status = "error"
		if c := errors.GetCode(errKafka); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrUnknown)
		}
	}

	trace.EndSpan(span, status, codeStr, map[string]interface{}{
		"username": user.Name,
	})

	if errKafka != nil {
		return errors.WithCode(code.ErrKafkaFailed, "kafka生产者消息发送失败")
	}

	return nil
}
