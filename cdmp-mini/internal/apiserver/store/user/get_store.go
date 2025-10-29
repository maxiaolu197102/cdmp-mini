package user

import (
	"context"
	"strconv"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/db"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	apierrors "github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// Get 查询用户（按用户名）- 生产级大并发版本
func (u *Users) Get(ctx context.Context, username string,
	opts metav1.GetOptions, opt *options.Options) (*v1.User, error) {

	storeCtx, storeSpan := trace.StartSpan(ctx, "user-store", "get_user")
	if storeCtx != nil {
		ctx = storeCtx
	}
	trace.AddRequestTag(ctx, "target_user", username)

	spanStatus := "success"
	spanCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": username,
	}
	defer func() {
		if storeSpan != nil {
			trace.EndSpan(storeSpan, spanStatus, spanCode, spanDetails)
		}
	}()

	// 设置包含重试时间预算的超时上下文
	totalCtx, cancel := u.createTimeoutContext(ctx, opt.ServerRunOptions.CtxTimeout, 3)
	defer cancel()

	var resultUser *v1.User

	// 配置生产级重试策略
	queryConfig := db.RetryConfig{
		MaxAttempts:   opt.MysqlOptions.MaxRetryAttempts, // 最多重试2次（总共3次尝试）
		InitialDelay:  opt.MysqlOptions.InitialDelay,     // 初始延迟
		MaxDelay:      opt.MysqlOptions.MaxDelay,         // 大并发下适当增加最大延迟
		BackoffFactor: opt.MysqlOptions.BackoffFactor,
		Jitter:        opt.MysqlOptions.Jitter,
		IsRetryable:   u.isRetryableError,
	}

	// 执行带重试的数据库查询
	err := db.Do(totalCtx, queryConfig, func(attemptCtx context.Context) error {
		attemptStart := time.Now()
		user, innerErr := u.executeSingleGet(attemptCtx, username)
		if innerErr != nil {
			return innerErr
		}
		resultUser = user
		logger.Debugf("单次查询尝试成功%v,%v",
			"attempt_cost_ms", time.Since(attemptStart).Milliseconds())
		return nil
	})

	if err != nil {
		handledErr := u.handleGetError(err)
		spanStatus = "error"
		if handledErr != nil {
			if c := apierrors.GetCode(handledErr); c != 0 {
				spanCode = strconv.Itoa(c)
				if c == code.ErrUserNotFound {
					spanStatus = "degraded"
					spanDetails["reason"] = "user_not_found"
				}
			}
		}
		return nil, handledErr
	}

	spanDetails["cached_user"] = resultUser != nil
	if resultUser != nil {
		spanDetails["result_id"] = resultUser.ID
	}
	return resultUser, nil
}
