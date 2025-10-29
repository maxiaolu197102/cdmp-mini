// Package verflag
// verflag.go
// 提供版本信息相关的命令行标志（flag）处理功能，
// 支持通过命令行参数（如 --version）触发版本信息的打印并退出程序，
// 支持普通文本和原始JSON两种输出格式，依赖版本信息包和pflag库实现。
package verflag

import (
	"fmt"
	"os"
	"strconv"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/version"
	flag "github.com/spf13/pflag" // 第三方pflag库，用于定义命令行标志
)

// versionValue 自定义命令行标志类型，用于表示版本标志的三种状态
type versionValue int

// 定义版本标志的三种状态常量
const (
	VersionFalse versionValue = 0 // 未指定版本标志（默认）
	VersionTrue  versionValue = 1 // 指定 --version（打印普通版本信息）
	VersionRaw   versionValue = 2 // 指定 --version=raw（打印原始JSON格式版本信息）
)

// strRawVersion 用于标识原始格式的字符串常量
const strRawVersion string = "raw"

// IsBoolFlag 实现pflag.Value接口，标记该标志为布尔类型（支持无参数使用，如--version）
func (v *versionValue) IsBoolFlag() bool {
	return true
}

// Get 实现pflag.Value接口，返回标志的当前值
func (v *versionValue) Get() interface{} {
	return v
}

// Set 实现pflag.Value接口，解析命令行传入的标志值并设置状态
// 支持三种输入：
// - "raw" → 设为VersionRaw（原始格式）
// - "true" → 设为VersionTrue（普通格式）
// - "false" → 设为VersionFalse（不打印）
func (v *versionValue) Set(s string) error {
	if s == strRawVersion {
		*v = VersionRaw
		return nil
	}
	// 解析布尔值
	boolVal, err := strconv.ParseBool(s)
	if boolVal {
		*v = VersionTrue
	} else {
		*v = VersionFalse
	}
	return err
}

// String 实现pflag.Value接口，返回标志的字符串表示
func (v *versionValue) String() string {
	if *v == VersionRaw {
		return strRawVersion
	}
	// 布尔值形式（true/false）
	return fmt.Sprintf("%v", bool(*v == VersionTrue))
}

// Type 实现pflag.Value接口，返回标志的类型名称
func (v *versionValue) Type() string {
	return "version"
}

// VersionVar 定义一个版本标志，绑定到指定的versionValue变量
// 参数：
//   - p: 存储标志值的变量
//   - name: 标志名称（如"version"）
//   - value: 默认值
//   - usage: 标志的帮助信息
func VersionVar(p *versionValue, name string, value versionValue, usage string) {
	*p = value
	flag.Var(p, name, usage)
	// 设置无参数时的默认值（如--version等价于--version=true）
	flag.Lookup(name).NoOptDefVal = "true"
}

// Version 简化版的VersionVar，自动创建versionValue变量并返回
func Version(name string, value versionValue, usage string) *versionValue {
	p := new(versionValue)
	VersionVar(p, name, value, usage)
	return p
}

// 定义全局版本标志的名称和默认实例
const versionFlagName = "version"

var versionFlag = Version(versionFlagName, VersionFalse, "打印版本信息并退出。")

// AddFlags 将全局版本标志添加到指定的标志集，确保多组件共享同一个版本标志
func AddFlags(fs *flag.FlagSet) {
	fs.AddFlag(flag.Lookup(versionFlagName))
}

// PrintAndExitIfRequested 检查版本标志状态，若已指定则打印版本信息并退出程序
// 流程：
// 1. 若标志为VersionRaw → 打印JSON格式的原始版本信息
// 2. 若标志为VersionTrue → 打印人类可读的版本信息
// 3. 打印完成后调用os.Exit(0)退出程序
func PrintAndExitIfRequested() {

	if *versionFlag == VersionRaw {
		fmt.Printf("%#v\n", version.Get()) // 原始格式（适合机器解析）
		os.Exit(0)
	} else if *versionFlag == VersionTrue {
		fmt.Printf("%s\n", version.Get()) // 普通文本格式（适合人类阅读）
		os.Exit(0)
	}
}
