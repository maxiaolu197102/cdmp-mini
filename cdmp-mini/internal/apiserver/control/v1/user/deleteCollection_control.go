package user

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserController) DeleteCollection(c *gin.Context) {

	var usernames []string

	// 方式1：优先使用标准数组格式 ?names=user1&names=user2
	if namesArray := c.QueryArray("names"); len(namesArray) > 0 {
		usernames = namesArray
	} else if namesStr := c.Query("names"); namesStr != "" {
		usernames = strings.Split(namesStr, ",")
		// 清理空格
		for i, name := range usernames {
			usernames[i] = strings.TrimSpace(name)
		}
	} else if nameArray := c.QueryArray("name"); len(nameArray) > 0 {
		usernames = nameArray
	} else if nameStr := c.Query("name"); nameStr != "" {
		usernames = strings.Split(nameStr, ",")
		for i, name := range usernames {
			usernames[i] = strings.TrimSpace(name)
		}
	}

	operator := common.GetUsername(c.Request.Context())
	auditLog := func(outcome, message string) {
		event := audit.BuildEventFromRequest(c.Request)
		event.Action = "user.delete_collection"
		event.ResourceType = "user"
		event.ResourceID = fmt.Sprintf("batch:%d", len(usernames))
		event.Actor = operator
		event.Outcome = outcome
		if len(usernames) > 0 {
			event.Metadata = map[string]any{"usernames": usernames}
		}
		if message != "" {
			event.ErrorMessage = message
		}
		submitAudit(c, event)
	}

	if len(usernames) == 0 {
		err := errors.WithCode(code.ErrInvalidParameter,
			"请通过以下方式传入用户名列表：\n"+
				"1. ?names=user1&names=user2（推荐）\n"+
				"2. ?names=user1,user2,user3")
		core.WriteResponse(c, err, nil)
		auditLog("fail", err.Error())
		return
	}
	// 数量限制
	const maxBatchDelete = 100
	if len(usernames) > maxBatchDelete {
		err := errors.WithCode(code.ErrReachMaxCount, "一次最多删除%d个用户", maxBatchDelete)
		core.WriteResponse(c, err, nil)
		auditLog("fail", err.Error())
		return
	}
	metrics.MonitorBusinessOperation("user_service", "deleteCollection", "http", func() error {
		// 分离合法和非法的用户名
		var validUsernames []string
		var invalidUsers = make(map[string]string)

		for _, username := range usernames {
			if errs := validation.IsQualifiedName(username); len(errs) > 0 {
				invalidUsers[username] = strings.Join(errs, ",")
			} else {
				validUsernames = usernames
			}
			// 如果有非法用户名，记录警告但不停止操作
			if len(invalidUsers) > 0 {
				log.Warnf("批量删除中检测到非法用户名: %v", invalidUsers)
			}
		} // 只对合法用户名执行删除
		if len(validUsernames) == 0 {
			err := errors.WithCode(code.ErrInvalidParameter,
				"所有用户名格式都不合法: %v", invalidUsers)
			core.WriteResponse(c, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		if err := u.srv.Users().DeleteCollection(c, validUsernames, true, metav1.DeleteOptions{}, u.options); err != nil {
			core.WriteResponse(c, err, nil)
			auditLog("fail", err.Error())
			return err
		}
		// 返回详细结果
		response := gin.H{
			"message": fmt.Sprintf("批量删除完成，成功%d个，跳过%d个",
				len(validUsernames)-len(invalidUsers), len(invalidUsers)),
			"deleted": validUsernames,
		}
		if len(invalidUsers) > 0 {
			response["skipped"] = invalidUsers
			response["note"] = "部分用户名格式不合法，已自动跳过"
		}

		core.WriteResponse(c, nil, response)
		auditLog("partial_success", fmt.Sprintf("跳过%d个非法用户名", len(invalidUsers)))
		return nil
	})
}
