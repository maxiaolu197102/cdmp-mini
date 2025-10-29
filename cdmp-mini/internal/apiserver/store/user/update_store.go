package user

import (
	"context"
	"strconv"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *Users) Update(ctx context.Context, user *v1.User, opts metav1.UpdateOptions, opt *options.Options) error {
	storeCtx, span := trace.StartSpan(ctx, "user-store", "update_user")
	if storeCtx != nil {
		ctx = storeCtx
	}
	trace.AddRequestTag(ctx, "target_user", user.Name)

	spanStatus := "success"
	spanCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"username": user.Name,
	}
	defer func() {
		if span != nil {
			trace.EndSpan(span, spanStatus, spanCode, spanDetails)
		}
	}()

	if err := u.db.WithContext(ctx).Model(&v1.User{}).Where("name = ?", user.Name).Updates(user).Error; err != nil {
		spanStatus = "error"
		if c := errors.GetCode(err); c != 0 {
			spanCode = strconv.Itoa(c)
		}
		return err
	}
	return nil
}

func (u *Users) UpdatePasswordWithVersion(ctx context.Context, userID uint64, username string, hashedPassword string, expectedUpdatedAt, newUpdatedAt time.Time) error {
	storeCtx, span := trace.StartSpan(ctx, "user-store", "update_password_version")
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
		if span != nil {
			trace.EndSpan(span, spanStatus, spanCode, spanDetails)
		}
	}()

	log.Debugw("UpdatePasswordWithVersion", "userID", userID, "expected_updated_at", expectedUpdatedAt.Format(time.RFC3339Nano), "new_updated_at", newUpdatedAt.Format(time.RFC3339Nano))

	updates := map[string]interface{}{
		"password":  hashedPassword,
		"updatedAt": newUpdatedAt,
	}

	builder := u.db.WithContext(ctx).Model(&v1.User{}).Where("id = ?", userID)
	if !expectedUpdatedAt.IsZero() {
		builder = builder.Where("updatedAt = ?", expectedUpdatedAt)
	}

	tx := builder.Updates(updates)
	if tx.Error != nil {
		spanStatus = "error"
		if c := errors.GetCode(tx.Error); c != 0 {
			spanCode = strconv.Itoa(c)
		}
		return errors.WithCode(code.ErrDatabase, "更新用户密码失败: %v", tx.Error)
	}
	if tx.RowsAffected == 0 {
		spanStatus = "error"
		spanCode = strconv.Itoa(code.ErrResourceConflict)
		return errors.WithCode(code.ErrResourceConflict, "用户数据已发生变化, 请重试")
	}
	spanDetails["rows_affected"] = tx.RowsAffected
	return nil
}
