/*
这是一个简洁而强大的 Go 语言应用程序框架接口定义包，提供了构建命令行应用的核心接口和类型定义。

核心接口定义
1. CliOptions 接口
配置选项管理接口，任何配置选项结构体都需要实现：
go

	type CliOptions interface {
	    Flags() cliFlag.NamedFlagSets    // 返回命名的标志集
	    Validate() []error               // 验证配置有效性
	}

2. CompleteableOptions 接口
这个接口主要用于解决以下场景：
配置验证 - 在应用启动前验证配置的完整性
默认值设置 - 为未设置的配置项提供合理的默认值
依赖解析 - 解析配置项之间的依赖关系
后期处理 - 进行一些只能在运行时完成的处理
go

	type CompleteableOptions interface {
	    Complete() error  // 完成配置的最终处理
	}

3. RunFunc 类型
应用程序运行函数类型：
go
type RunFunc func(basename string) error
basename: 应用程序的基础名称
返回 error: 执行结果错误

4. Option 类型
函数式选项模式的类型定义：
go
type Option func(app *App)
用于通过函数调用配置 App 实例。
设计模式
函数式选项模式 (Functional Options)
允许通过一系列配置函数来构建对象：
go
app := NewApp("myapp",

	WithOptions(myOptions),
	WithRunFunc(myRunFunc),
	WithDescription("我的应用"),

)
接口分离原则
CliOptions: 负责标志定义和验证
CompleteableOptions: 负责配置后期处理（可选）
清晰的职责分离，易于扩展和维护
典型实现示例
CliOptions 实现
go

	type ServerOptions struct {
	    Port int
	    Host string
	}
	func (o *ServerOptions) Flags() cliFlag.NamedFlagSets {
	    nfs := cliFlag.NamedFlagSets{}
	    fs := nfs.FlagSet("Server")
	    fs.IntVar(&o.Port, "port", 8080, "服务器端口")
	    fs.StringVar(&o.Host, "host", "localhost", "服务器主机")
	    return nfs
	}

	func (o *ServerOptions) Validate() []error {
	    var errs []error
	    if o.Port < 1 || o.Port > 65535 {
	        errs = append(errs, fmt.Errorf("端口必须在1-65535之间"))
	    }
	    return errs
	}

CompleteableOptions 实现
go

	func (o *ServerOptions) Complete() error {
	    if o.Host == "" {
	        o.Host = getDefaultHost()
	    }
	    return nil
	}

架构优势
松耦合: 接口定义使核心框架与具体实现解耦
可测试性: 每个接口都可以单独测试
可扩展性: 易于添加新的配置选项类型
一致性: 统一的配置管理模式
灵活性: 支持可选的功能扩展（CompleteableOptions）
使用流程
定义选项: 实现 CliOptions 接口
创建应用: 使用 NewApp 和选项函数
配置验证: 框架自动调用 Validate()
配置完成: 如果实现 CompleteableOptions，调用 Complete()
执行应用: 调用 RunFunc 运行应用程序
这个包提供了一个简洁而强大的框架基础，使得构建复杂的命令行应用程序变得更加规范和可维护。
*/
package app

import (
	cliFlag "github.com/maxiaolu1981/cretem/nexuscore/component-base/cli/flag"
)

// 命令行选项接口
type CliOptions interface {
	Flags() cliFlag.NamedFlagSets
	Validate() []error
}

// 选项模式接口
type Option func(a *App)

// 应用程序运行函数类型
type Runfunc func(basename string) error

// 完成配置接口
type CompleteableOptions interface {
	Complete()
}
