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

// package errors
// stack.go
// 提供增强的错误处理功能，专注于调用栈捕获、存储与格式化输出
// 核心目标是为错误信息附加详细的调用栈上下文，帮助开发者快速定位问题根源。
//
// 整体架构
//
// 包采用分层设计，包含三个核心类型和辅助功能：
// 1. Frame：最底层的栈帧单元，封装单个程序计数器(PC)，提供文件、行号、函数名等信息的解析
// 2. stack：底层存储结构，保存原始调用栈的PC值集合，负责与系统调用栈交互
// 3. StackTrace：高层调用栈表示，由多个Frame组成，提供多样化的格式化输出能力
//
// 各组件关系：
//
//	stack（原始PC存储）→ 通过StackTrace()方法转换为 → StackTrace（Frame集合）
//	Frame是StackTrace的基本组成单元，每个Frame对应一个函数调用点
//
// 核心调用流程
//
//  1. 捕获调用栈：
//     st := errors.Callers()  // 捕获当前调用位置的栈信息，返回*stack
//
//  2. 转换为高层表示：
//     trace := st.StackTrace()  // 将原始PC值转换为StackTrace（[]Frame）
//
//  3. 格式化输出：
//     fmt.Printf("详细调用栈:\n%+v\n", trace)  // 按详细格式打印
//     fmt.Printf("简洁调用栈: %v\n", trace)     // 按简洁格式打印
//
// 格式化能力
//
// 支持多种格式动词（%s/%v/%+v/%#v/%d/%n），满足不同场景需求：
// - 开发调试：%+v 显示完整函数名、文件路径和行号
// - 日志记录：%v 显示简洁的"函数名:行号"列表
// - 信息展示：%s 仅显示函数名列表
// - 序列化：通过MarshalText()方法支持文本序列化
//
// 使用场景
//
// - 错误包装：为自定义错误类型附加调用栈信息
// - 日志增强：在关键操作日志中添加调用栈上下文
// - 调试工具：辅助开发过程中的调用链分析
//
// 注意事项
//
// - 调用栈捕获有一定性能开销，建议在错误路径（而非正常流程）中使用
// - Frame存储的PC值为实际值+1（历史兼容原因），需通过pc()方法获取真实PC
// - 默认最多捕获32层调用栈，可根据需求调整Callers()中的depth常量
package errors

import (
	"fmt"
	"io"
	"path"
	"runtime"
	"strconv"
	"strings"
)

// Frame 表示调用栈中单个栈帧的程序计数器(PC)
// 存储的值为实际PC+1（历史原因），需通过pc()方法获取真实PC
type Frame uintptr

// pc 返回当前栈帧对应的实际程序计数器(PC)值
func (f Frame) pc() uintptr {
	return uintptr(f) - 1
}

// file 获取当前栈帧对应的源代码文件绝对路径
// 示例: "/home/going/cretem/nexuscore/errors/stack_test.go"
// 若无法获取，返回"unknown"
func (f Frame) file() string {
	fn := runtime.FuncForPC(f.pc())
	if fn == nil {
		return "unknown"
	}
	file, _ := fn.FileLine(f.pc())
	return file
}

// line 获取当前栈帧对应的源代码行号
// 若无法获取，返回0
func (f Frame) line() int {
	fn := runtime.FuncForPC(f.pc())
	if fn == nil {
		return 0
	}
	_, line := fn.FileLine(f.pc())
	return line
}

// name 获取当前栈帧对应的函数名（含完整包路径）
// 示例: "github.com/cretem/nexuscore/errors.TestFrame"
// 若无法获取，返回"unknown"
func (f Frame) name() string {
	fn := runtime.FuncForPC(f.pc())
	if fn == nil {
		return "unknown"
	}
	return fn.Name() + "()"
}

// Format 实现fmt.Formatter接口，支持自定义栈帧格式化输出
// 支持的格式动词：
//
//	%s    输出简短函数名（不含包路径）
//	%+s   输出"完整包路径+函数名\n\t完整文件路径"
//	%d    输出行号
//	%n    输出简化的函数名（仅函数名部分）
//	%v    输出"简短函数名:行号"
//	%+v   输出"完整包路径+函数名\n\t完整文件路径:行号"
func (f Frame) Format(s fmt.State, verb rune) {
	switch verb {
	case 's':
		if s.Flag('+') || s.Flag('-') {
			io.WriteString(s, f.name())
			io.WriteString(s, "\n\t")
			io.WriteString(s, f.file())
		} else {
			io.WriteString(s, path.Base(f.name()))
		}

	case 'd':
		io.WriteString(s, strconv.Itoa(f.line()))

	case 'n':
		io.WriteString(s, funcname(f.name()))

	case 'v':
		io.WriteString(s, "\n")
		io.WriteString(s, "{")
		f.Format(s, 's')
		io.WriteString(s, ":")
		io.WriteString(s, strconv.Itoa(f.line()))
		io.WriteString(s, "\n")
		io.WriteString(s, "}")
	}
}

// MarshalText 实现encoding.TextMarshaler接口，用于栈帧信息的文本序列化
// 输出格式："函数名 文件路径:行号"（无换行和制表符）
func (f Frame) MarshalText() ([]byte, error) {
	name := f.name()
	if name == "unknown" {
		return []byte(name), nil
	}
	return []byte(fmt.Sprintf("%s %s:%d", name, f.file(), f.line())), nil
}

// StackTrace 表示完整的调用栈序列，由多个Frame组成
// 顺序为：最内层（最新调用）→ 最外层（最早调用）
type StackTrace []Frame

// Format 实现fmt.Formatter接口，支持自定义调用栈格式化输出
// 支持的格式动词：
//
//	%+v  显示详细信息，每行一个栈帧，包含完整函数名、文件路径和行号
//	%#v  显示Go语法风格的切片表示
//	%v   显示简洁格式的栈帧列表，每个元素为"函数名:行号"
//	%s   显示函数名列表，每个元素为简短函数名
func (f StackTrace) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		switch {
		case s.Flag('+'):
			for _, frame := range f {
				io.WriteString(s, "\n")
				frame.Format(s, verb)
			}
		case s.Flag('#'):
			fmt.Fprintf(s, "%#v", []Frame(f))
		default:
			f.formatSlice(s, verb)
		}
	case 's':
		f.formatSlice(s, verb)
	}
}

// formatSlice 辅助方法，以切片形式格式化输出所有栈帧
// 格式为"[帧1 帧2 ...]"，具体帧格式由verb决定
func (f StackTrace) formatSlice(s fmt.State, verb rune) {
	io.WriteString(s, "[")
	for i, frame := range f {
		if i > 0 {
			io.WriteString(s, " ")
		}
		frame.Format(s, verb)
	}
	io.WriteString(s, "]")
}

// stack 底层存储结构，用于保存原始调用栈的程序计数器(PC)值
type stack []uintptr

// Format 实现fmt.Formatter接口，支持栈信息的格式化输出
// 仅处理%+v格式，输出所有栈帧的详细信息（每行一个栈帧）
func (s *stack) Format(st fmt.State, verb rune) {
	if verb == 'v' && st.Flag('+') || st.Flag('-') {
		for _, pc := range *s {
			Frame(pc).Format(st, 'v')
		}
	}
}

// StackTrace 将底层PC值转换为上层StackTrace类型（Frame集合）
func (s *stack) StackTrace() StackTrace {
	stackTrace := make([]Frame, len(*s))
	for i, pc := range *s {
		stackTrace[i] = Frame(pc)
	}
	return stackTrace
}

// callers 捕获当前程序的调用栈信息，返回存储PC值的stack指针
// 最多捕获32层栈，跳过前3层（自身、调用者、runtime.callers）
func callers() *stack {
	const depth = 32
	pcs := [depth]uintptr{}
	n := runtime.Callers(3, pcs[:])
	st := stack(pcs[:n])
	return &st
}

// funcname 简化函数名，移除包路径前缀
// 例如："github.com/user/pkg/foo.Bar" → "Bar"
func funcname(name string) string {
	// 移除最后一个斜杠之前的包路径部分
	if n := strings.LastIndex(name, "/"); n != -1 {
		name = name[n+1:]
	}
	// 移除包名部分，保留函数名
	if n := strings.Index(name, "."); n > 0 {
		name = name[n+1:]
	}
	return name
}

// ToSlice 将堆栈帧转换为字符串数组（每个元素为一行堆栈）
func (s *stack) ToSlice() []string {
	if s == nil || len(*s) == 0 {
		return nil // 空堆栈返回 nil（配合 omitempty 省略）
	}
	var stackSlice []string
	frames := runtime.CallersFrames(*s)
	for {
		frame, more := frames.Next()
		// 格式化堆栈信息为 "文件名:行号"
		stackSlice = append(stackSlice, fmt.Sprintf("[%s()]%s:%d", frame.Function, frame.File, frame.Line))
		if !more {
			break
		}
	}
	return stackSlice
}
