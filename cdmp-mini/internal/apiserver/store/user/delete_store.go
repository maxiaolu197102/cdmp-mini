package user

import (
	"context"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (u *Users) Delete(ctx context.Context, username string, opts metav1.DeleteOptions, opt *options.Options) error {
	return nil
}

func (u *Users) DeleteForce(ctx context.Context, username string, opts metav1.DeleteOptions, opt *options.Options) error {
	// 	logger := log.L(ctx).WithValues(
	// 		"method", "DeleteForce",
	// 		"unscoped", true,
	// 	)

	// 	startTime := time.Now()
	// 	logger.Infow("存储层:开始用户删除操作", "start_time", startTime)

	// 	err := u.executeInTransaction(ctx, logger, func(tx *gorm.DB) error {
	// 		// 1. 删除关联数据
	// 		assocStart := time.Now()
	// 		if err := u.deleteUserAssociations(tx, username, opts); err != nil {
	// 			return err
	// 		}
	// 		logger.Infow("存储层:关联数据删除完成", "duration_ms", time.Since(assocStart).Milliseconds())

	// 		// 2. 删除用户主体数据
	// 		mainStart := time.Now()
	// 		if err := u.deleteUserMainData(tx, username, opts); err != nil {
	// 			return err
	// 		}
	// 		logger.Infow("存储层:主体数据删除完成", "duration_ms", time.Since(mainStart).Milliseconds())

	// 		return nil
	// 	})

	// 	totalDuration := time.Since(startTime)
	// 	if err != nil {
	// 		logger.Errorw("存储层:用户删除失败", "error", err, "total_duration_ms", totalDuration.Milliseconds(), "status", "failed")
	// 		return err
	// 	}

	// 	//logger.Infow("用户删除成功", "total_duration_ms", totalDuration.Milliseconds(), "status", "success")
	// 	return nil
	// }

	// // deleteUserAssociations 删除用户关联数据（修复tx参数使用）
	// func (u *Users) deleteUserAssociations(tx *gorm.DB, username string, opts metav1.DeleteOptions) error {
	// 	logger := log.L(context.Background()).WithValues("operation", "delete_user_associations", "username", username)

	// 	// 使用事务tx
	// 	//pol := NewPolices(&datastore{DB: tx}) // 根据实际情况使用 db 或 DB

	// 	policyStart := time.Now()

	// 	result := tx.Where("username = ?", username).Delete(&v1.Policy{})
	// 	if result.Error != nil {
	// 		policyDuration := time.Since(policyStart)
	// 		logger.Errorw("存储层:删除用户策略失败", "error", result.Error, "policy_duration_ms", policyDuration.Milliseconds())
	// 		return errors.Wrap(result.Error, "存储层:删除用户策略失败")
	// 	}

	// 	policyDuration := time.Since(policyStart)
	// 	logger.Debugw("存储层:用户策略删除完成", "policy_duration_ms", policyDuration.Milliseconds())

	// 	return nil
	// }

	// // deleteUserMainData 删除用户主体数据（带计时）
	// func (u *Users) deleteUserMainData(tx *gorm.DB, username string, opts metav1.DeleteOptions) error {
	// 	logger := log.L(context.Background()).WithValues(
	// 		"operation", "delete_user_main_data",
	// 		"username", username,
	// 		"unscoped", opts.Unscoped,
	// 	)

	// 	db := tx
	// 	if opts.Unscoped {
	// 		db = db.Unscoped()
	// 		logger.Debug("存储层:使用硬删除模式")
	// 	}

	// 	// 记录删除操作时间
	// 	deleteStart := time.Now()
	// 	result := db.Where("name = ?", username).Delete(&v1.User{})
	// 	deleteDuration := time.Since(deleteStart)

	// 	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
	// 		logger.Errorw("存储层:删除用户失败",
	// 			"error", result.Error,
	// 			"delete_duration_ms", deleteDuration.Milliseconds(),
	// 		)
	// 		return errors.WithCode(code.ErrDatabase, result.Error.Error())
	// 	}

	// 	if result.RowsAffected == 0 {
	// 		logger.Warnw("存储层:未找到要删除的用户",
	// 			"delete_duration_ms", deleteDuration.Milliseconds(),
	// 		)
	// 		return errors.WithCode(code.ErrUserNotFound, "存储层:用户不存在")
	// 	}

	// 	logger.Infow("存储层:用户主体数据删除成功",
	// 		"rows_affected", result.RowsAffected,
	// 		"delete_duration_ms", deleteDuration.Milliseconds(),
	// 	)

	return nil
}

func (u *Users) DeleteCollection(ctx context.Context, usernames []string, opts metav1.DeleteOptions, opt *options.Options) error {
	return nil
}
