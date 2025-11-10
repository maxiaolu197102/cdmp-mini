package user

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"gorm.io/gorm"
)

func (u *Users) Delete(ctx context.Context, username string, opts metav1.DeleteOptions, opt *options.Options) error {
	return nil
}

func (u *Users) DeleteForce(ctx context.Context, username string, opts metav1.DeleteOptions, opt *options.Options) error {
	storeCtx, span := trace.StartSpan(ctx, "user-store", "delete_force")
	if storeCtx != nil {
		ctx = storeCtx
	}
	trace.AddRequestTag(ctx, "target_user", username)
	trace.AddRequestTag(ctx, "delete_force", true)

	logger := log.L(ctx).WithValues(
		"store", "user",
		"method", "DeleteForce",
		"username", username,
		"unscoped", opts.Unscoped,
	)

	spanStatus := "success"
	spanCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": username,
		"unscoped": opts.Unscoped,
	}
	defer func() {
		if span != nil {
			trace.EndSpan(span, spanStatus, spanCode, spanDetails)
		}
	}()

	if u == nil || u.db == nil {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrDatabase)
		err := errors.WithCode(code.ErrDatabase, "用户存储未初始化")
		logger.Errorw("DeleteForce: 存储未初始化", "error", err)
		return err
	}
	if strings.TrimSpace(username) == "" {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrInvalidParameter)
		err := errors.WithCode(code.ErrInvalidParameter, "用户名不能为空")
		logger.Errorw("DeleteForce: 参数校验失败", "error", err)
		return err
	}

	start := time.Now()
	var assocDeleted map[string]int64
	var userRows int64

	txnErr := u.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		assocStart := time.Now()
		deleted, err := u.deleteUserAssociations(ctx, tx, username)
		if err != nil {
			return err
		}
		assocDeleted = deleted
		logger.Debugw("DeleteForce: 关联数据已删除", "duration_ms", time.Since(assocStart).Milliseconds(), "details", assocDeleted)

		userStart := time.Now()
		rows, err := u.deleteUserMainData(ctx, tx, username, opts)
		if err != nil {
			return err
		}
		userRows = rows
		logger.Debugw("DeleteForce: 主体数据已删除", "duration_ms", time.Since(userStart).Milliseconds(), "rows", userRows)
		return nil
	})

	if txnErr != nil {
		spanStatus = "error"
		if c := errors.GetCode(txnErr); c != 0 {
			spanCode = strconv.Itoa(c)
		} else {
			spanCode = strconv.Itoa(code.ErrDatabase)
		}
		logger.Errorw("DeleteForce: 删除失败", "error", txnErr)
		return txnErr
	}

	spanDetails["assoc_deleted"] = assocDeleted
	spanDetails["user_rows"] = userRows
	spanDetails["duration_ms"] = time.Since(start).Milliseconds()

	logger.Infow("DeleteForce: 用户删除完成",
		"assoc_deleted", assocDeleted,
		"user_rows", userRows,
		"total_duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

func (u *Users) deleteUserAssociations(ctx context.Context, tx *gorm.DB, username string) (map[string]int64, error) {
	spanCtx, span := trace.StartSpan(ctx, "user-store", "delete_force_associations")
	if spanCtx != nil {
		ctx = spanCtx
	}
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{"username": username}
	beginAll := time.Now()
	defer func() {
		spanDetails["duration_ms"] = time.Since(beginAll).Milliseconds()
		if span != nil {
			trace.EndSpan(span, status, codeStr, spanDetails)
		}
	}()

	logger := log.L(ctx).WithValues("operation", "delete_user_associations", "username", username)
	if tx == nil {
		err := errors.WithCode(code.ErrDatabase, "事务未初始化")
		logger.Errorw("删除关联数据失败", "error", err)
		status = "error"
		codeStr = strconv.Itoa(errors.GetCode(err))
		return nil, err
	}

	targets := []struct {
		name  string
		model any
	}{
		{name: "policy", model: &v1.Policy{}},
		{name: "secret", model: &v1.Secret{}},
	}

	deleted := make(map[string]int64, len(targets))
	for _, target := range targets {
		tableLogger := logger.WithValues("table", target.name)
		begin := time.Now()
		result := tx.WithContext(ctx).Where("username = ?", username).Delete(target.model)
		if result.Error != nil {
			err := errors.WithCode(code.ErrDatabase, "删除%s失败: %v", target.name, result.Error)
			tableLogger.Errorw("删除失败", "error", err)
			tagDeleteLockWait(ctx, result.Error, "assoc", target.name)
			status = "error"
			codeStr = strconv.Itoa(errors.GetCode(err))
			return nil, err
		}
		deleted[target.name] = result.RowsAffected
		tableLogger.Debugw("删除完成", "rows", result.RowsAffected, "duration_ms", time.Since(begin).Milliseconds())
		spanDetails[target.name+"_rows"] = result.RowsAffected
	}

	return deleted, nil
}

func (u *Users) deleteUserMainData(ctx context.Context, tx *gorm.DB, username string, opts metav1.DeleteOptions) (int64, error) {
	spanCtx, span := trace.StartSpan(ctx, "user-store", "delete_force_main")
	if spanCtx != nil {
		ctx = spanCtx
	}
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": username,
		"unscoped": opts.Unscoped,
	}
	defer func() {
		if span != nil {
			trace.EndSpan(span, status, codeStr, spanDetails)
		}
	}()

	logger := log.L(ctx).WithValues("operation", "delete_user_main", "username", username, "unscoped", opts.Unscoped)
	if tx == nil {
		err := errors.WithCode(code.ErrDatabase, "事务未初始化")
		logger.Errorw("删除用户主体失败", "error", err)
		status = "error"
		codeStr = strconv.Itoa(errors.GetCode(err))
		return 0, err
	}

	executor := tx.WithContext(ctx)
	if opts.Unscoped {
		executor = executor.Unscoped()
	}

	begin := time.Now()
	result := executor.Where("name = ?", username).Delete(&v1.User{})
	duration := time.Since(begin).Milliseconds()

	if result.Error != nil {
		err := errors.WithCode(code.ErrDatabase, "删除用户失败: %v", result.Error)
		logger.Errorw("删除失败", "error", err, "duration_ms", duration)
		tagDeleteLockWait(ctx, result.Error, "main", "user")
		status = "error"
		codeStr = strconv.Itoa(errors.GetCode(err))
		spanDetails["duration_ms"] = duration
		return 0, err
	}

	if result.RowsAffected == 0 {
		logger.Infow("未找到用户,跳过删除", "duration_ms", duration)
		spanDetails["duration_ms"] = duration
		return 0, nil
	}

	logger.Infow("用户主体删除完成", "rows", result.RowsAffected, "duration_ms", duration)
	spanDetails["duration_ms"] = duration
	spanDetails["rows"] = result.RowsAffected
	return result.RowsAffected, nil
}

func (u *Users) DeleteCollection(ctx context.Context, usernames []string, opts metav1.DeleteOptions, opt *options.Options) error {
	return nil
}

func tagDeleteLockWait(ctx context.Context, err error, stage string, resource string) {
	if ctx == nil || err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "lock wait") || strings.Contains(msg, "deadlock") || strings.Contains(msg, "lock timeout") {
		trace.AddRequestTag(ctx, "lock_wait_detected", true)
		if stage != "" {
			trace.AddRequestTag(ctx, "lock_wait_stage", stage)
		}
		if resource != "" {
			trace.AddRequestTag(ctx, "lock_wait_resource", resource)
		}
	}
}
