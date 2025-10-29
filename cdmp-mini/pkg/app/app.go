/*
这是一个功能强大的 Go 语言命令行应用程序框架包，提供了完整的 CLI 应用构建和管理功能。以下是详细分析：
1.核心结构
App 结构体
应用程序主结构，包含：
basename - 基础命令名称
name - 应用名称
description - 应用描述
options - 命令行选项接口
noVersion - 是否禁用版本显示
noConfig - 是否禁用配置
silence - 是否静默模式
cmd - cobra.Command 实例
runFunc - 运行函数
args - 参数验证函数
2.设计模式
选项模式 (Functional Options)
通过 Option 函数类型实现灵活的配置：
go
type Option func(app *App)
// 各种配置选项函数
WithDescription(), WithOptions(), WithRunFunc(), etc.
主要功能
1. 命令行构建 (buildCommand)
创建 cobra.Command 实例
设置使用说明和帮助模板
添加标志集和配置选项
设置自定义帮助函数
2. 命令运行 (runCommand)
完整的命令执行流程：
打印工作目录
显示命令行标志
绑定 Viper 配置
应用选项规则
执行运行函数
3. 选项验证 (applyOptionRules)
两级验证机制：
Complete() - 完成配置（如果实现 CompleteableOptions）
# Validate() - 验证配置有效性
特色功能
自定义帮助模板
go
var usageTemplate = fmt.Sprintf(...) // 彩色输出的帮助模板
配置管理
Viper 集成：支持多种配置源绑定
标志排序：自动排序命令行标志
命名标志集：分组管理相关标志
版本管理
go
verflag.AddFlags() // 添加版本标志
version.Get().PrintVersionWithLog() // 打印版本信息
4.使用示例
创建应用
go
app := NewApp("iam-apiserver", "IAM API Server",

	WithDescription("身份访问管理API服务器"),
	WithOptions(options),
	WithRunFunc(runServer),
	WithDefaultValidArgs(),

)
5.运行应用
go

	if err := app.Run(); err != nil {
	    // 错误处理
	}

6.配置选项接口
CliOptions 接口
应用选项需要实现的接口：
go

	type CliOptions interface {
	    Flags() cliFlag.NamedFlagSets
	    Validate() []error
	}

CompleteableOptions 接口（可选）
go

	type CompleteableOptions interface {
	    Complete() error
	}

7.输出美化
彩色输出
使用 fatih/color 包提供彩色输出：
progressMessage - 绿色进度提示
错误信息 - 红色高亮
帮助信息 - 青色标题
结构化输出
分章节显示帮助信息
自动适应终端宽度
清晰的标志分组
错误处理
聚合错误
使用 errors.NewAggregate() 处理多个验证错误
优雅退出
详细的错误信息输出
适当的退出码设置
8.设计优势
高度可配置：通过选项模式灵活定制应用行为
生产就绪：包含完整的配置验证和管理
用户体验：精美的帮助信息和彩色输出
扩展性强：清晰的接口设计，易于添加新功能
标准化：遵循 Go 语言和 cobra 的最佳实践
这个包提供了一个企业级的命令行应用程序框架，特别适合构建复杂的微服务和管理工具，具有完善的配置管理、错误处理和用户交互功能。
*/
package app

import (
	"fmt"
	"os"

	"github.com/fatih/color"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"

	cliflag "github.com/maxiaolu1981/cretem/nexuscore/component-base/cli/flag"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/term"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/prettyprint"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/version"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/version/verflag"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	progressMessage = color.GreenString("==>")
)

type App struct {
	basename    string
	name        string
	description string
	noVersion   bool
	noConfig    bool
	silence     bool
	runFunc     Runfunc
	command     *cobra.Command
	args        cobra.PositionalArgs
	options     CliOptions
}

func WithOptions(opts *options.Options) Option {
	return func(a *App) {
		a.options = opts
	}
}

func WithDescription(description string) Option {
	return func(a *App) {
		a.description = description
	}
}
func WithNoVersion() Option {
	return func(a *App) {
		a.noVersion = true
	}
}
func WithNoConfig() Option {
	return func(a *App) {
		a.noConfig = true
	}
}
func WithSilence() Option {
	return func(a *App) {
		a.silence = true
	}
}
func WithArgs(args cobra.PositionalArgs) Option {
	return func(a *App) {
		a.args = args
	}
}
func WithRunFunc(runFunc Runfunc) Option {
	return func(a *App) {
		a.runFunc = runFunc
	}
}
func WithDefaultValidArgs() Option {
	return func(a *App) {
		a.args = func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return errors.WithCode(code.ErrValidation, "参数验证失败:该命令不接受任何参数\n"+
					"使用方法:%s\n"+
					"实际提供了%d个参数:%v\n", cmd.UseLine(),
					len(args),
					args)
			}
			return nil
		}
	}
}

func NewApp(basename, name string, opts ...Option) (*App, error) {
	app := &App{
		basename: basename,
		name:     name,
	}
	for _, o := range opts {
		o(app)
	}

	if err := app.buildCommand(); err != nil {
		return nil, err
	}

	return app, nil
}

func (a *App) buildCommand() error {
	cmd := &cobra.Command{
		Use:           a.basename,
		Short:         a.name,
		Long:          a.description,
		Args:          a.args,
		SilenceUsage:  true,
		SilenceErrors: true,
		PreRunE:       a.persistentPreRun,
	}
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)
	cliflag.InitFlags(cmd.Flags())

	if a.runFunc != nil {
		cmd.RunE = a.runCommand
	}

	var namedFlagSets cliflag.NamedFlagSets
	if a.options != nil {

		namedFlagSets = a.options.Flags()
		fs := cmd.Flags()
		for _, fss := range namedFlagSets.FlagSets {
			fs.AddFlagSet(fss)
		}
	}

	if !a.noVersion {
		verflag.AddFlags(namedFlagSets.FlagSet("global"))
	}
	if !a.noConfig {
		addConfigFlag(a.basename, namedFlagSets.FlagSet("global"))
	}

	addHelpCommandFlag(cmd.Name(), namedFlagSets.FlagSet("global"))

	addCmdTemplate(cmd, namedFlagSets)
	a.command = cmd
	return nil
}

func (a *App) runCommand(cmd *cobra.Command, args []string) error {
	pwd, _ := os.Getwd()
	log.Debugf("%s开始在[%s]运行%s", progressMessage, pwd, a.name)
	if a.runFunc != nil {
		err := a.runFunc(a.basename)
		if err != nil {
			return err
		}
	}
	return nil
}

func addCmdTemplate(cmd *cobra.Command, namedFlagSets cliflag.NamedFlagSets) {
	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStderr(), namedFlagSets, cols)

		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStdout(), namedFlagSets, cols)
	})
}

func (a *App) applyOptionRules() error {
	if completeableOptions, ok := a.options.(CompleteableOptions); ok {
		completeableOptions.Complete()
	}
	if a.options != nil {
		if errs := a.options.Validate(); len(errs) > 0 {
			return errors.NewAggregate(errs)
		}
	}

	return nil
}

func (a *App) Run() {
	if err := a.command.Execute(); err != nil {
		fmt.Printf("error=%v %v\n", progressMessage, err)
	}
}

func (a *App) persistentPreRun(cmd *cobra.Command, args []string) error {
	// 绑定 Viper
	if !a.noConfig {
		if err := viper.BindPFlags(cmd.Flags()); err != nil {
			return err
		}
	}
	//解组到结构体
	if !a.noConfig && a.options != nil {
		if err := viper.Unmarshal(a.options); err != nil {
			return err
		}
	}

	// 应用选项规则
	if a.options != nil {
		if err := a.applyOptionRules(); err != nil {
			return err
		}
	}

	// 打印调试信息
	if !a.silence {
		//prettyprint.PrintFlags(cmd.Flags())
		if !a.noVersion {
			version.Get().PrintVersionWithLog()
		}
		if !a.noConfig {
			//	prettyprint.PrintConfig()
		}
		prettyprint.Print("配置内容", a.options)
	}

	return nil
}
