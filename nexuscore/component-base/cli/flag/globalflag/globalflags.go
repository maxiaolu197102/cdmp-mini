// Copyright (c) 2025 马晓璐
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
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

// package globalflag
// globalflags.go
// 提供全局命令行标志（flag）的注册和管理功能，
// 用于将标准库 flag 中定义的全局标志关联到本地 pflag 标志集，
// 同时确保标志名格式统一（使用连字符 "-" 而非下划线 "_"），
// 避免全局标志泄露到组件的标志集中，适用于多组件命令行工具的参数管理。
package globalflag

import (
	"flag"    // 标准库的flag包，用于处理全局标志
	"fmt"     // 用于格式化字符串
	"strings" // 用于字符串处理

	"github.com/spf13/pflag" // 第三方pflag库，提供增强的命令行标志功能
)

// AddGlobalFlags 显式注册全局标志（如帮助信息）到本地pflag标志集，
// 防止不需要的全局标志泄露到组件的标志集中。
// 参数：
//   - fs: 本地pflag标志集（组件的标志集）
//   - name: 组件/命令名称，用于帮助信息的格式化
func AddGlobalFlags(fs *pflag.FlagSet, name string) {
	// 注册一个 "--help" 或 "-h" 标志，用于显示当前组件的帮助信息
	fs.BoolP("help", "h", false, fmt.Sprintf("帮助 %s", name))
}

// normalize 标准化标志名：将下划线 "_" 替换为连字符 "-"，
// 遵循命令行标志的命名规范（推荐使用连字符）。
func normalize(s string) string {
	return strings.ReplaceAll(s, "_", "-")
}

// Register 将标准库flag中定义的全局标志（globalName）关联到本地pflag标志集，
// 确保本地标志集可以访问全局标志的值，同时统一标志名格式。
// 流程：
// 1. 从标准库flag的全局标志集（flag.CommandLine）中查找名为globalName的标志
// 2. 若找到，将其转换为pflag标志，并标准化名称（替换下划线为连字符）
// 3. 将转换后的标志添加到本地pflag标志集
// 4. 若未找到，触发panic（确保依赖的全局标志已定义）
// 参数：
//   - local: 本地pflag标志集（组件的标志集）
//   - globalName: 标准库flag中定义的全局标志名称
func Register(local *pflag.FlagSet, globalName string) {
	// 从标准库的全局标志集中查找指定名称的标志
	if f := flag.CommandLine.Lookup(globalName); f != nil {
		// 将标准库flag转换为pflag标志
		pflagFlag := pflag.PFlagFromGoFlag(f)
		// 标准化标志名（替换下划线为连字符）
		pflagFlag.Name = normalize(pflagFlag.Name)
		// 将转换后的标志添加到本地标志集
		local.AddFlag(pflagFlag)
	} else {
		// 若全局标志未找到，触发panic（提示标志未定义）
		panic(fmt.Sprintf("failed to find flag in global flagset (flag): %s", globalName))
	}
}
