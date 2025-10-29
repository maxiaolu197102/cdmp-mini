/*
这个包是IAM项目的JWT认证策略实现模块，提供了基于JWT令牌的Bearer认证功能。以下是包摘要：

包功能
JWT认证: 实现JWT Bearer令牌认证

中间件集成: 包装gin-jwt中间件提供统一接口

受众验证: 定义特定的JWT受众字段值

核心常量
AuthzAudience
go
const AuthzAudience = "iam.authz.marmotedu.com" // JWT受众字段值
用于验证JWT令牌的受众(audience)声明，确保令牌为IAM授权系统颁发

核心结构体
JWTStrategy
JWT认证策略结构体：

go

	type JWTStrategy struct {
	    ginjwt.GinJWTMiddleware // 嵌入gin-jwt中间件
	}

主要方法
NewJWTStrategy(gjwt ginjwt.GinJWTMiddleware) JWTStrategy
创建JWT认证策略实例，包装现有的gin-jwt中间件

AuthFunc() gin.HandlerFunc
生成Gin认证中间件函数，直接委托给底层gin-jwt中间件

设计特点
适配器模式: 包装第三方gin-jwt库，提供统一的AuthStrategy接口

最小封装: 仅进行简单的接口适配，保持gin-jwt的全部功能

接口一致性: 实现middleware.AuthStrategy接口，与BasicStrategy等保持一致

受众验证: 通过常量定义确保令牌的目标受众正确

技术特性
Bearer认证: 遵循HTTP Bearer Token认证标准

完整的JWT支持: 继承gin-jwt的全部功能（签发、验证、刷新等）

中间件兼容: 完全兼容Gin中间件生态系统

使用场景
RESTful API的令牌认证

需要长期会话管理的应用

与其他认证策略组合使用（如AutoStrategy）

依赖关系
gin-jwt: 基于appleboy/gin-jwt.v2库实现核心JWT功能

统一接口: 符合项目内部的middleware.AuthStrategy接口

这个包通过适配器模式将强大的gin-jwt库集成到IAM认证体系中，提供了生产级的JWT认证解决方案。
*/
package auth

import (
	ginjwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
	middleware "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/business"
)

const AuthzAudience = "https://github.com/maxiaolu1981/cretem"

type JWTStrategy struct {
	ginjwt.GinJWTMiddleware
}

var _ middleware.AuthStrategy = &JWTStrategy{}

func NewJWTStrategy(gjwt ginjwt.GinJWTMiddleware) JWTStrategy {
	return JWTStrategy{gjwt}
}

func (j JWTStrategy) AuthFunc() gin.HandlerFunc {
	return j.MiddlewareFunc()
}
