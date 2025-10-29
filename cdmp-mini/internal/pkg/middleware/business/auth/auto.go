/*
这个包是IAM项目的认证策略模块，实现了自动选择认证方式的智能策略。以下是包摘要：

包功能
自动认证: 根据Authorization头部自动选择Basic或Bearer认证

策略路由: 将认证请求路由到相应的具体认证策略

统一接口: 提供统一的Gin中间件认证接口

核心结构体
AutoStrategy
自动认证策略结构体：

go

	type AutoStrategy struct {
	    basic middleware.AuthStrategy  // Basic认证策略
	    jwt   middleware.AuthStrategy  // JWT Bearer认证策略
	}

主要方法
NewAutoStrategy(basic, jwt middleware.AuthStrategy) AutoStrategy
创建自动认证策略实例

AuthFunc() gin.HandlerFunc
生成Gin认证中间件函数，实现：

解析头部: 解析Authorization头部信息

策略选择: 根据"Basic"或"Bearer"选择对应策略

错误处理: 处理无效的认证头部格式

认证执行: 调用具体策略的认证方法

认证流程
检查Authorization头部是否存在且格式正确

根据认证类型选择策略：

# Basic → Basic认证策略

# Bearer → JWT认证策略

执行选择的认证策略

如果认证失败，返回相应的错误响应

错误处理
ErrInvalidAuthHeader: Authorization头部格式错误

ErrSignatureInvalid: 无法识别的认证类型

统一使用core.WriteResponse返回错误信息

设计特点
策略模式: 使用策略模式实现认证算法的灵活切换

装饰器模式: 包装具体认证策略，提供统一接口

开闭原则: 易于扩展新的认证策略类型

中间件兼容: 完全兼容Gin中间件接口标准

使用场景
API网关的统一认证入口

支持多种认证方式的微服务

需要向后兼容不同认证协议的系统

这个包作为认证系统的智能路由层，为IAM项目提供了灵活、可扩展的多认证策略支持。
*/
package auth

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	middleware "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

const authHeaderCount = 2

type AutoStrategy struct {
	basicStrategy middleware.AuthStrategy
	jwtStrategy   middleware.AuthStrategy
}

func NewAutoStrategy(basic, jwt middleware.AuthStrategy) AutoStrategy {
	return AutoStrategy{
		basicStrategy: basic,
		jwtStrategy:   jwt,
	}
}

// AuthFunc 生成 Gin 中间件函数（核心认证逻辑）
func (a AutoStrategy) AuthFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		//log.Errorf("c.path", c.FullPath())
		// 关键：在所有认证逻辑前添加/login白名单，直接放行
		if c.FullPath() == "/login" {
			// 打印日志便于调试（生产环境可移除）

			c.Next() // 直接进入后续处理器（如createAuthHandler）
			return
		}

		// 初始化 AuthOperator 并设置到上下文
		operator := middleware.AuthOperator{}
		c.Set("AuthOperator", &operator)

		// 第一步：获取 Authorization 头并判断是否为空
		authHeader := c.Request.Header.Get("Authorization")
		if authHeader == "" {
			err := errors.WithCode(
				code.ErrMissingHeader,
				"缺少Authorization头，支持格式：\n1. Basic认证：Authorization: Basic {base64(username:password)}\n2. JWT认证：Authorization: Bearer {jwt-token}",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 第二步：分割授权头并校验格式
		authParts := strings.SplitN(authHeader, " ", authHeaderCount)
		if len(authParts) != authHeaderCount || strings.TrimSpace(authParts[0]) == "" || strings.TrimSpace(authParts[1]) == "" {
			err := errors.WithCode(
				code.ErrInvalidAuthHeader,
				"授权头格式无效，正确格式：\n- Basic认证：Authorization: Basic {base64编码的用户名:密码}\n- JWT认证：Authorization: Bearer {完整JWT令牌}",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}
		authScheme := strings.TrimSpace(authParts[0])

		// 第三步：选择认证策略
		switch authScheme {
		case "Basic":
			operator.SetAuthStrategy(a.basicStrategy)
		case "Bearer":
			operator.SetAuthStrategy(a.jwtStrategy)
		default:
			err := errors.WithCode(
				code.ErrTokenInvalid,
				"未识别的认证方式，仅支持两种：\n1. Basic认证（用户名密码）\n2. Bearer认证（JWT令牌）",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 第四步：执行具体认证逻辑（规避 nil 风险）
		authFunc := operator.AuthFunc()
		if authFunc == nil {
			err := errors.WithCode(
				code.ErrInternalServer,
				"认证策略初始化异常，无法执行认证",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}
		authFunc(c)
		if c.IsAborted() {
			return
		}

		// 第五步：获取并校验用户名

		username := operator.GetUsername()
		if username == "" {
			err := errors.WithCode(
				code.ErrUnauthorized,
				"认证流程异常：未获取到用户名",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 第六步：同步用户名到上下文
		c.Set(common.UsernameKey, username)
		newCtx := context.WithValue(c.Request.Context(), common.KeyUsername, username)

		c.Request = c.Request.WithContext(newCtx)

		c.Next()

	}
}
