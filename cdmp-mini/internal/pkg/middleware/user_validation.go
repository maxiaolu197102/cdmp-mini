// Copyright 2020 Lingfei Kong <colin404@foxmail.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package middleware

import (
	"net/http"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// 建议：用常量定义路由路径，避免硬编码（提升扩展性）
const (
	RouteUsersCreate = "/v1/users"
	// 查看用户详情  更新用户信息  删除用户 修改密码
	RouteUsersDetail         = "/v1/users/:name"                 // 用户详情（查/改/删）
	RouteUsersChangePassword = "/v1/users/:name/change-password" // 修改密码
)

// Validation 权限校验中间件：确保用户有对应资源的操作权限
func Validation(opt *options.Options) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 调用 isAdmin 检查当前用户是否为管理员
		err := isAdmin(c)

		// 区分错误类型处理
		if err != nil {
			errCode := errors.GetCode(err)
			switch errCode {
			// 非管理员错误（允许进入后续路径权限判断）
			case code.ErrNotAdministrator, code.ErrPermissionDenied:
				// 执行非管理员权限校验逻辑
				if !checkNormalUserPermission(c) {
					c.Abort()
					return // 校验失败，已通过 core.WriteResponse 响应
				}
				// 权限通过，继续执行
				c.Next()
				return

			// 其他错误（未登录、用户不存在、数据库错误等，直接阻断）
			case code.ErrUnauthorized, code.ErrUserNotFound, code.ErrDatabase, code.ErrDatabaseTimeout:
				core.WriteResponse(c, err, nil)
				c.Abort()
				return

			// 未知错误
			default:
				core.WriteResponse(c, errors.WithCode(code.ErrInternalServer, "权限校验异常"), nil)
				c.Abort()
				return
			}
		} else {
			// 管理员：执行管理员特殊限制后放行
			// // 管理员特殊限制（如禁止删除超级管理员）
			// if c.Request.Method == http.MethodDelete && isSuperAdmin(c, opt) {
			// 	core.WriteResponse(c, errors.WithCode(code.ErrPermissionDenied, "超级管理员不允许删除自己"), nil)
			// 	c.Abort()
			// 	return
			// }
			// 管理员权限校验通过，放行
			c.Next()
			return
		}
	}
}

// checkNormalUserPermission 非管理员用户的路径权限校验
func checkNormalUserPermission(c *gin.Context) bool {
	fullPath := c.FullPath()
	currentUser := getUsernameFromCtx(c)
	targetUser := c.Param("name")

	switch fullPath {
	case RouteUsersDetail, RouteUsersChangePassword:
		// 非管理员仅允许操作自己的账号
		if currentUser != targetUser {
			core.WriteResponse(c,
				errors.WithCode(code.ErrPermissionDenied, "非管理员仅允许操作自己的账号，不允许修改他人信息"),
				nil,
			)
			c.Abort()
			return false
		}

		// 禁止非管理员删除自己
		if c.Request.Method == http.MethodDelete {
			core.WriteResponse(c,
				errors.WithCode(code.ErrPermissionDenied, "非管理员不允许删除自己的账号（需联系管理员）"),
				nil,
			)
			c.Abort()
			return false
		}

		// 允许操作：查看详情、更新信息、修改密码等
		return true

	case RouteUsersCreate:
		// 非管理员仅允许创建用户（POST方法）
		//if c.Request.Method != http.MethodPost {
		core.WriteResponse(c,
			errors.WithCode(code.ErrPermissionDenied, "普通用户无权限创建用户，请联系管理员操作"),
			nil,
		)
		c.Abort()
		return false
	default:
		// 其他未定义路径：非管理员禁止访问
		core.WriteResponse(c,
			errors.WithCode(code.ErrPermissionDenied, "非管理员无权限访问此接口"),
			nil,
		)
		c.Abort()
		return false
	}
}

// isAdmin 判断当前用户是否为管理员（纯判断逻辑，不处理响应）
func isAdmin(c *gin.Context) error {

	// 从 claims 获取用户状态和角色
	claims := jwt.ExtractClaims(c)
	username, _ := claims["username"].(string)
	isadmin, _ := claims["isadmin"].(string)

	if username == "" {
		return errors.WithCode(code.ErrUnauthorized, "判断管理员失败：未获取到当前用户名")
	}

	// 判断是否为管理员
	if isadmin != "1" {
		return errors.WithCode(code.ErrNotAdministrator, "当前用户（%s）非管理员", username)
	}

	return nil
}

// getUsernameFromCtx 从上下文中安全获取当前用户名
func getUsernameFromCtx(c *gin.Context) string {
	usernameVal, exists := c.Get(common.UsernameKey)
	if !exists {
		return ""
	}
	username, ok := usernameVal.(string)
	if !ok {
		return ""
	}
	return username
}

// isSuperAdmin 判断当前用户是否为超级管理员（示例实现）
// func isSuperAdmin(c *gin.Context, opt *options.Options) bool {
// 	username := getUsernameFromCtx(c)
// 	if username == "root" { // 假设 root 为超级管理员
// 		return true
// 	}
// 	// 实际场景可从数据库查询 is_super 字段
// 	_, err := interfaces.Client().Users().Get(c, username, metav1.GetOptions{}, opt)
// 	if err != nil {
// 		return false
// 	}
// 	return true
// }
