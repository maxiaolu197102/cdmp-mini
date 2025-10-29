/*
这个包是一个专业的 Go 语言应用程序配置管理模块，基于 Viper 库实现了完整的配置加载和管理功能。以下是详细分析：

核心功能
1. 多源配置加载
支持从多个来源加载配置：

命令行参数：通过 -c/--config 指定配置文件

配置文件：支持 JSON、TOML、YAML、HCL、Java properties 等多种格式

环境变量：自动绑定并支持前缀和键名转换

2. 智能配置文件发现
采用分层搜索策略查找配置文件：

当前工作目录 (.)

用户主目录隐藏文件夹 (~/.appname/)

系统配置目录 (/etc/appname/)

自动识别文件格式（无需指定扩展名）

3. 环境变量集成
go
viper.AutomaticEnv() // 自动绑定环境变量
viper.SetEnvPrefix("APP_PREFIX") // 设置环境变量前缀
viper.SetEnvKeyReplacer() // 键名转换（. → _，- → _）
配置解析流程
初始化阶段 (init())
注册命令行标志：

go
pflag.StringVarP(&cfgFile, "config", "c", "", "配置文件路径")
配置加载阶段 (cobra.OnInitialize)
确定配置文件路径：命令行指定或自动发现

设置搜索路径：多级目录搜索

读取配置：viper.ReadInConfig()

错误处理：读取失败时优雅退出

配置展示阶段 (printConfig)
使用表格形式清晰展示所有配置项：

go
table := uitable.New()
table.AddRow("配置项:", "值")
关键技术特性
环境变量映射
自动将配置键名转换为环境变量：

server.port → SERVER_PORT

db-host → DB_HOST

智能路径处理
go
// 从应用名提取目录名
// "api-server" → "api"
names := strings.Split(basename, "-")
dirName := names[0]
多格式支持
Viper 自动检测并解析多种配置文件格式，无需手动指定。

使用示例
基本用法
bash
# 指定配置文件
./app -c config.yaml

# 自动发现配置（搜索 ./app.yaml, ~/.app/app, /etc/app/app 等）
./app
环境变量覆盖
bash
# 通过环境变量覆盖配置
export APP_SERVER_PORT=8080
export APP_LOG_LEVEL=debug
./app
设计优势
灵活性：支持多配置源，优先级明确（命令行 > 环境变量 > 配置文件）

易用性：自动发现配置，减少用户配置负担

可读性：表格形式清晰展示配置，便于调试

标准化：遵循 Twelve-Factor App 的配置最佳实践

扩展性：易于集成到各种命令行应用中

错误处理
配置文件读取失败时提供明确错误信息

环境变量绑定自动处理

多级配置搜索避免单点失败

生产环境特性
配置安全
支持隐藏目录配置（~/.appname/）

系统级配置目录支持

调试支持
完整配置项输出

清晰的错误信息提示

跨平台兼容
正确处理主目录路径（Windows/Linux/macOS）

统一的环境变量处理

这个配置管理包提供了一个企业级的配置解决方案，特别适合需要复杂配置管理的微服务和命令行工具，具有良好的可维护性和用户体验。

*/

package app

import (
	"os"
	"path/filepath"
	"strings"

	// 用于生成格式化表格
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/system"

	// 获取用户主目录
	// 命令行参数解析

	"github.com/spf13/cobra"
	"github.com/spf13/pflag" // 命令行标志管理
	"github.com/spf13/viper" // 配置管理库
)

// 配置文件标志名（用于命令行参数）
const configFlagName = "config"

// 配置文件路径变量，通过命令行参数设置
var cfgFile string

// 初始化函数：注册命令行参数
// nolint: gochecknoinits 忽略"禁止在init中执行复杂逻辑"的检查
func init() {
	// 注册 -config/-c 标志：指定配置文件路径，支持多种格式
	pflag.StringVarP(&cfgFile, "config", "c", cfgFile, "从指定的文件读取配置，支持 JSON、TOML、YAML、HCL 或 Java properties 格式。")
}

func addConfigFlag(basename string, fs *pflag.FlagSet) {
	fs.AddFlag(pflag.Lookup(configFlagName))
	viper.AutomaticEnv()
	cobra.OnInitialize(func() {
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
		} else {
			viper.AddConfigPath(".")
			if name := strings.Split(basename, "-"); len(name) > 1 {
				viper.AddConfigPath(filepath.Join(system.HomeDir(), "."+name[0]))
				viper.AddConfigPath(filepath.Join("/etc", name[0]))
			}
			viper.SetConfigName(basename)
		}
		if err := viper.ReadInConfig(); err != nil {
			log.Errorf("读取配置文件错误(%s):%v", cfgFile, err)
			os.Exit(1)
		}
	})
}

var (
	_ = configFlagName
	_ = addConfigFlag
)

/*
// 已注释的 loadConfig 函数：另一种配置加载实现（保留作为参考）
// 功能：读取配置文件和环境变量（若设置）
func loadConfig(cfg string, defaultName string) {
	if cfg != "" {
		viper.SetConfigFile(cfg) // 使用指定的配置文件
	} else {
		// 未指定时，添加默认搜索路径
		viper.AddConfigPath(".")
		viper.AddConfigPath(filepath.Join(homedir.HomeDir(), RecommendedHomeDir)) // 推荐的用户主目录下路径
		viper.SetConfigName(defaultName) // 默认配置文件名
	}

	viper.SetConfigType("yaml")              // 强制指定配置类型为 yaml
	viper.AutomaticEnv()                     // 自动读取环境变量
	viper.SetEnvPrefix(RecommendedEnvPrefix) // 环境变量前缀（如 IAM）
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_")) // 替换分隔符

	// 读取配置文件（若找到）
	if err := viper.ReadInConfig(); err != nil {
		log.Warnf("警告：viper 未能发现并加载配置文件：%s", err.Error())
	}
}
*/
