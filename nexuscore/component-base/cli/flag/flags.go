// package flag
// flags:flags.go
// 提供命令行参数（flag）的增强处理功能，基于 spf13/pflag 库扩展，
// 主要实现标志名规范化、标准库 flag 兼容及调试打印功能，确保命令行参数解析的一致性和易用性。
//
// 核心功能包括：
// 1. 标志名规范化：将含下划线 "_" 的标志名自动转换为连字符 "-"，并支持对旧格式的警告提示。
// 2. 跨库兼容：合并标准库 flag 与 pflag 的参数集，实现两者的无缝协同。
// 3. 调试辅助：打印所有解析后的参数及其值，便于验证参数配置正确性。
//
// 适用于需要统一命令行参数格式、兼容多 flag 库或需要调试参数解析的场景。
package flag

import (
	goflag "flag" // 导入标准库的flag包，重命名为goflag以区分
	"strings"     // 用于字符串处理

	"github.com/maxiaolu1981/cretem/nexuscore/log" // 导入自定义日志包
	"github.com/spf13/pflag"                       // 导入第三方pflag包（增强版flag，支持更多功能）
)

// WordSepNormalizeFunc 用于规范化命令行标志（flag）的名称，将包含"_"分隔符的标志名转换为"-"分隔符
// 例如：将"log_level"转换为"log-level"，符合Unix命令行参数的命名习惯
func WordSepNormalizeFunc(f *pflag.FlagSet, name string) pflag.NormalizedName {

	normalized := strings.ReplaceAll(name, "_", ".")
	normalized = strings.ReplaceAll(normalized, "-", ".")
	return pflag.NormalizedName(normalized)
}

// WarnWordSepNormalizeFunc 与WordSepNormalizeFunc功能类似，但会对包含"_"的标志名发出警告
// 用于提示用户旧的命名方式已过时，建议使用"-"分隔符的新方式
func WarnWordSepNormalizeFunc(f *pflag.FlagSet, name string) pflag.NormalizedName {
	if strings.Contains(name, "_") {
		nname := strings.ReplaceAll(name, "_", "-")
		// 输出警告日志：告知用户旧标志名将被弃用，建议使用新标志名
		log.Warnf("%s is DEPRECATED and will be removed in a future version. Use %s instead.", name, nname)

		return pflag.NormalizedName(nname)
	}
	return pflag.NormalizedName(name)
}

// InitFlags 初始化命令行标志集：规范化标志名、合并标准库flag、解析标志
// 作用是统一处理命令行参数，确保标志名格式一致并支持标准库的flag
func InitFlags(flags *pflag.FlagSet) {
	// 设置标志名规范化函数，统一将"_"转换为"-"
	flags.SetNormalizeFunc(WordSepNormalizeFunc)
	// 将标准库goflag的标志集添加到pflag中，实现两者的兼容
	flags.AddGoFlagSet(goflag.CommandLine)
}
