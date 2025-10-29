/*
这个包是IAM项目的API服务器启动模块，提供了服务器运行的核心入口函数。以下是包摘要：
1.包功能
服务器启动: 提供API服务器的启动入口
生命周期管理: 控制服务器的准备和运行流程
2.核心函数
Run(cfg *config.Config) error
主运行函数，负责创建和启动API服务器
3.执行流程
创建服务器: 调用 createAPIServer(cfg) 创建服务器实例
错误处理: 如果创建失败，返回错误信息
准备运行: 调用 server.PrepareRun() 进行运行前准备
启动服务: 调用 PrepareRun().Run() 启动服务器并阻塞运行
4.设计特点
简洁入口: 提供清晰的服务器启动入口点
错误传播: 正确处理并返回启动过程中的错误
永不退出: 设计为长期运行的服务，正常情况下不应退出
生命周期管理: 分离准备阶段和运行阶段，确保服务正确初始化
5.依赖关系
配置输入: 依赖 config.Config 结构体提供所有配置参数
服务器创建: 依赖 createAPIServer() 函数创建具体的服务器实例
运行框架: 依赖服务器实例的 PrepareRun() 和 Run() 方法
这个包作为API服务器的启动入口，封装了服务器的创建、准备和运行流程，为上层应用提供了简单清晰的启动接口。
*/
package apiserver

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
)

func Run(opts *options.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server, err := newApiServer(opts)
	if err != nil {
		return err
	}

	if err := server.run(ctx); err != nil {
		if err == context.Canceled {
			return nil
		}
		return err
	}
	return nil
}
