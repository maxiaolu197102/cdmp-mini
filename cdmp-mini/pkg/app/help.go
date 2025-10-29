// Copyright (c) 2025 马晓璐
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,

//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
/*
该包提供了命令行应用的帮助系统支持，主要功能包括：
创建 "help" 子命令，允许用户通过help [命令]的方式查询任意命令的详细帮助
为应用和子命令添加--help/-H标志，支持通过标志快速获取帮助信息
集成颜色输出（使用 color 库）美化帮助提示中的命令名称
基于 cobra 和 pflag 库实现，与主流 Go 命令行框架无缝集成
该模块是命令行应用的基础组件，提升了用户体验，让用户能够方便地获取命令使用方法。
该代码主要实现了命令行应用中 "帮助命令" 及 "帮助标志" 的功能，
核心流程如下：
创建帮助命令（helpCommand）
定义一个名为 "help" 的子命令，用于显示其他命令的帮助信息
当执行[应用名] help [命令名]时，通过c.Root().Find(args)查找目标命令
若找到目标命令，初始化其默认帮助标志并显示帮助信息（cmd.Help()）
若未找到目标命令，提示错误并显示根命令的用法
添加帮助标志（addHelpFlag 与 addHelpCommandFlag）
为应用或命令添加--help（短选项-H）标志
当用户使用--help或-H时，会触发对应命令的帮助信息显示
两个函数分别用于应用级和命令级的帮助标志，提示信息略有不同
*/
package app

import (
	"fmt"
	"strings"

	"github.com/fatih/color" // 用于终端颜色输出
	// Go命令行框架
	"github.com/spf13/pflag" // 命令行标志处理库
)

// 帮助标志的常量定义
const (
	flagHelp          = "help" // 长选项名：--help
	flagHelpShorthand = "h"    // 短选项名：-H
)

// addHelpCommandFlag 为应用的特定命令向指定的标志集(FlagSet)添加帮助标志
// 参数usage是命令的用法字符串，fs是要添加标志的标志集
func addHelpCommandFlag(usage string, fs *pflag.FlagSet) {
	// 从用法字符串中提取命令名（取第一个空格前的部分）并设置为绿色
	cmdName := color.GreenString(strings.Split(usage, " ")[0])

	// 添加--help/-H标志，提示信息为"Help for the [命令名] command."
	fs.BoolP(
		flagHelp,
		flagHelpShorthand,
		false,
		fmt.Sprintf("Help for the %s command.", cmdName),
	)
}
