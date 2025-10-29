/*
这个包是IAM项目的API服务器核心实现模块，负责服务器的创建、配置和生命周期管理。以下是包摘要：

包功能
服务器创建: 构建HTTP和gRPC双服务实例

配置管理: 处理通用配置和额外配置的构建

生命周期管理: 实现优雅启动和关闭流程

依赖初始化: 初始化数据库、缓存等基础设施

核心结构体
apiServer
主服务器结构，包含：

gs - 优雅关闭管理器

redisOptions - Redis配置选项

gRPCAPIServer - gRPC服务器实例

genericAPIServer - HTTP REST API服务器实例

preparedAPIServer
准备就绪的服务器包装器

ExtraConfig
gRPC服务器额外配置：

地址、最大消息大小

服务器证书

数据库选项

主要函数
createAPIServer(cfg *config.Config)
创建API服务器实例，包括：

初始化优雅关闭管理器

构建通用配置和额外配置

创建HTTP和gRPC服务器实例

PrepareRun() preparedAPIServer
准备运行阶段：

初始化路由

初始化Redis存储

注册关闭回调（关闭MySQL、gRPC、HTTP服务）

Run() error
启动服务器：

启动gRPC服务器（goroutine）

启动优雅关闭管理器

启动HTTP REST API服务器（阻塞式）

配置构建
buildGenericConfig()
构建HTTP服务器通用配置：

服务器运行选项

功能特性选项

安全服务配置

非安全服务配置

buildExtraConfig()
构建gRPC服务器额外配置

特色功能
双协议支持: 同时支持HTTP REST和gRPC协议

优雅关闭: 使用shutdown管理器实现平滑关闭

健康检查: 包含Redis连接健康检查

安全传输: gRPC使用TLS证书加密

服务发现: gRPC支持反射服务

依赖管理
MySQL存储: 通过工厂模式获取数据库实例

Redis缓存: 异步连接Redis并进行健康监测

缓存服务: 注册gRPC缓存服务

信号处理: POSIX信号优雅关闭管理

这个包是IAM API服务器的核心实现，提供了完整的高可用、安全的微服务架构基础。
*/
package apiserver

import (
	"context"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server"

	_ "github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator"
)

type apiServer struct {
	genericAPIServer *server.GenericAPIServer
	//redis            redis.UniversalClient // 新增字段
	options *options.Options
}

func newApiServer(opts *options.Options) (*apiServer, error) {

	genericAPIServer, err := server.NewGenericAPIServer(opts)
	if err != nil {
		return nil, err
	}

	return &apiServer{
		genericAPIServer: genericAPIServer,
		options:          opts,
	}, nil
}

func (a *apiServer) run(ctx context.Context) error {
	return a.genericAPIServer.Run(ctx)
}
