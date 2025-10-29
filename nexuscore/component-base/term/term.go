// Copyright (c) 2025 马晓璐

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

// Package term
// term.go
// 提供终端相关的工具函数，主要用于获取终端窗口的尺寸（宽度和高度），
// 依赖 moby/term 库实现跨平台的终端信息获取，支持判断输出流是否为终端设备。

package term

import (
	"fmt"
	"io"

	"github.com/moby/term"
	// 引入第三方终端处理库，提供跨平台的终端信息获取能力
)

// TerminalSize 获取当前用户终端的宽度和高度。
// 如果传入的 writer 不是终端设备，返回错误；如果获取尺寸失败，返回宽高为 0 及错误。
// 注意：通常需要传入进程的 stdout（标准输出），传入 stderr（标准错误）可能无法正常工作。
// 参数：
//   - w: 输出流（如 os.Stdout），用于判断是否为终端并获取其文件描述符
//
// 返回值：
//   - 终端宽度（列数）
//   - 终端高度（行数）
//   - 错误信息（非终端设备或获取失败时）
func TerminalSize(w io.Writer) (int, int, error) {
	// 步骤1：获取输出流的文件描述符，并判断是否为终端设备
	// term.GetFdInfo 会返回文件描述符（outFd）和一个布尔值（isTerminal）
	outFd, isTerminal := term.GetFdInfo(w)
	if !isTerminal {
		// 若不是终端设备，返回错误
		return 0, 0, fmt.Errorf("given writer is no terminal")
	}

	// 步骤2：通过文件描述符获取终端窗口尺寸
	// term.GetWinsize 会返回包含宽高信息的 winsize 结构体
	winsize, err := term.GetWinsize(outFd)
	if err != nil {
		// 若获取尺寸失败（如终端不支持），返回错误
		return 0, 0, err
	}

	// 步骤3：将尺寸转换为 int 类型并返回
	return int(winsize.Width), int(winsize.Height), nil
}
