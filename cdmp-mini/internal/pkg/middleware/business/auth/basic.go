/*
这个包是IAM项目的Basic认证策略实现模块，提供了基于用户名和密码的基础认证功能。以下是包摘要：

包功能
Basic认证: 实现HTTP Basic认证标准

凭证验证: 通过回调函数验证用户名和密码

中间件集成: 提供Gin中间件认证接口

核心结构体
BasicStrategy
Basic认证策略结构体：

go
type BasicStrategy struct {
    compare func(username string, password string) bool // 凭证验证回调函数
}
主要方法
NewBasicStrategy(compare func()) BasicStrategy
创建Basic认证策略实例，需要传入验证函数

AuthFunc() gin.HandlerFunc
生成Gin认证中间件函数

认证流程
解析头部: 检查Authorization头部格式是否为"Basic"

解码凭证: Base64解码认证凭证

分割凭证: 分割用户名和密码（以冒号分隔）

验证凭证: 调用compare函数验证用户名密码

设置上下文: 认证成功后设置用户名到Gin上下文

错误处理: 任何步骤失败都返回认证错误

技术细节
编码格式: 使用Base64标准编码解码

分隔符: 用户名密码以冒号:分隔

错误类型: 统一返回ErrSignatureInvalid错误

上下文键: 使用middleware.UsernameKey存储用户名

设计特点
依赖注入: 通过回调函数注入验证逻辑，与具体存储解耦

标准兼容: 完全遵循HTTP Basic认证标准

灵活验证: compare函数可以连接数据库、LDAP或其他认证源

中间件友好: 完全兼容Gin中间件模式

安全考虑
使用Base64编码（非加密），建议配合HTTPS使用

验证函数应实现密码安全比较（防时序攻击）

使用场景
内部系统API认证

简单的用户名密码认证需求

与其他认证策略组合使用（如AutoStrategy）

这个包提供了简单而标准的Basic认证实现，为IAM项目提供了基础的认证能力。


*/

package auth

import (
	"encoding/base64"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	middleware "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

type BasicStrategy struct {
	compare func(username string, password string) bool
}

func NewBasicStrategy(compare func(username string, password string) bool) BasicStrategy {
	return BasicStrategy{compare: compare}
}

// 修复后的 AuthFunc
func (b BasicStrategy) AuthFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 第一步：获取并校验 Authorization 头格式（Basic 前缀）
		authHeader := c.Request.Header.Get("Authorization")
		if authHeader == "" {
			// 场景1：缺少授权头 → 用 ErrMissingHeader（而非 ErrInvalidAuthHeader）
			err := errors.WithCode(
				code.ErrMissingHeader,
				"Basic认证缺少Authorization头，正确格式：Authorization: Basic {base64(username:password)}",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 分割 "Basic " 和 payload（最多分割2部分）
		authParts := strings.SplitN(authHeader, " ", 2)
		if len(authParts) != 2 || strings.TrimSpace(authParts[0]) != "Basic" {
			// 场景2：授权头格式错误（非 Basic 前缀）→ 用 ErrInvalidAuthHeader
			err := errors.WithCode(
				code.ErrInvalidAuthHeader,
				"Basic认证头格式无效，正确格式：Authorization: Basic {base64(username:password)}",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 2. 第二步：Base64 解码 payload（不忽略错误）
		payload, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(authParts[1]))
		if decodeErr != nil {
			// 场景3：Base64 解码失败 → 用新增的 ErrBase64DecodeFail
			err := errors.WithCode(
				code.ErrBase64DecodeFail,
				"Basic认证 payload解码失败：%v（请确保是 username:password 的Base64编码）",
				decodeErr,
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 3. 第三步：校验 payload 格式（必须含冒号分隔用户名和密码）
		userPassPair := strings.SplitN(string(payload), ":", 2)
		if len(userPassPair) != 2 || strings.TrimSpace(userPassPair[0]) == "" || strings.TrimSpace(userPassPair[1]) == "" {
			// 场景4：payload 无冒号/用户名/密码为空 → 用新增的 ErrInvalidBasicPayload
			err := errors.WithCode(
				code.ErrInvalidBasicPayload,
				"Basic认证 payload格式无效：需用冒号分隔非空用户名和密码（如 username:password）",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}
		username := strings.TrimSpace(userPassPair[0])
		password := strings.TrimSpace(userPassPair[1])

		// 4. 第四步：验证用户名密码（修正逻辑反置问题）
		if !b.compare(username, password) { // 验证失败（compare返回false）才返回错误
			// 场景5：密码不正确 → 用 ErrPasswordIncorrect（而非 
			err := errors.WithCode(
				code.ErrPasswordIncorrect,
				"Basic认证失败：用户名或密码不正确",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 5. 第五步：设置上下文（避免 MustGet  panic）
		// 先判断 AuthOperator 是否存在，再类型转换
		operatorVal, ok := c.Get("AuthOperator")
		if !ok {
			err := errors.WithCode(
				code.ErrInternalServer,
				"Basic认证：上下文缺失 AuthOperator（上游中间件未设置）",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}
		operator, ok := operatorVal.(*middleware.AuthOperator)
		if !ok {
			err := errors.WithCode(
				code.ErrInternalServer,
				"Basic认证：AuthOperator 类型错误（预期 *middleware.AuthOperator）",
			)
			core.WriteResponse(c, err, nil)
			c.Abort()
			return
		}

		// 6. 认证通过：设置用户名到上下文和 AuthOperator
		c.Set(common.UsernameKey, username)
		operator.SetUsername(username)
		c.Next()
	}
}
