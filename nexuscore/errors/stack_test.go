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

// Package errors 提供调用栈捕获与格式化功能的测试用例
// 该包包含对 Frame、StackTrace、stack 等类型的完整测试，验证其在不同场景下的行为正确性。
//
// 测试结构分为四类：
// 1. 基础功能测试：验证单个栈帧的属性获取
// 2. 格式化测试：覆盖所有支持的格式动词（%s, %v, %+v 等）
// 3. 边界测试：空调用栈的特殊处理验证
// 4. 调试辅助：提供各种格式的输出示例
package errors

import (
	"bytes"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func init() {}

// TestFrame 验证 Frame 类型的基础功能
// 测试内容包括：
// - pc() 方法：验证程序计数器的转换正确性
// - file() 方法：确保能获取有效的文件路径
// - line() 方法：验证行号的有效性
// - name() 方法：检查函数名的正确性
func TestFrame(t *testing.T) {
	// 捕获当前函数的调用栈帧作为测试对象
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	f := Frame(pcs[0])

	t.Run("pc() 方法验证", func(t *testing.T) {
		expected := uintptr(pcs[0]) - 1
		if f.pc() != expected {
			t.Errorf("pc() 计算错误: 预期 %d, 实际 %d", expected, f.pc())
		}
	})

	t.Run("file() 方法验证", func(t *testing.T) {
		if f.file() == "unknown" {
			t.Error("file() 返回 unknown，无法获取文件路径")
		}
	})

	t.Run("line() 方法验证", func(t *testing.T) {
		if f.line() <= 0 {
			t.Errorf("line() 返回无效行号: %d", f.line())
		}
	})

	t.Run("name() 方法验证", func(t *testing.T) {
		expectedFunc := "TestFrame"
		if !strings.Contains(f.name(), expectedFunc) {
			t.Errorf("name() 预期包含 %q, 实际 %q", expectedFunc, f.name())
		}
	})
}

// TestFrameFormat 验证 Frame 类型的格式化输出
// 覆盖所有支持的格式动词：
//
// 格式动词 | 描述
// --------|------
// %s      | 输出简短函数名（不含包路径）
// %+s     | 输出完整函数名+文件路径
// %d      | 输出行号
// %n      | 输出简化的函数名（仅函数名）
// %v      | 输出"函数名:行号"格式
// %+v     | 输出完整信息（函数名+文件路径+行号）
func TestFrameFormat(t *testing.T) {
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	f := Frame(pcs[0])

	tests := []struct {
		name   string
		format string
		check  func(string) bool
	}{
		{
			name:   "%s 格式（简短函数名）",
			format: "%s",
			check: func(s string) bool {
				return strings.HasSuffix(s, "TestFrameFormat")
			},
		},
		{
			name:   "%+s 格式（完整函数名+文件路径）",
			format: "%+s",
			check: func(s string) bool {
				return strings.Contains(s, "TestFrameFormat") &&
					strings.Contains(s, "stack_test.go")
			},
		},
		{
			name:   "%d 格式（行号）",
			format: "%d",
			check: func(s string) bool {
				return len(s) > 0 && s != "0"
			},
		},
		{
			name:   "%n 格式（简化函数名）",
			format: "%n",
			check: func(s string) bool {
				return s == "TestFrameFormat"
			},
		},
		{
			name:   "%v 格式（函数名:行号）",
			format: "%v",
			check: func(s string) bool {
				parts := strings.Split(s, ":")
				return len(parts) == 2 && parts[1] != "0"
			},
		},
		{
			name:   "%+v 格式（完整信息）",
			format: "%+v",
			check: func(s string) bool {
				return strings.Contains(s, "TestFrameFormat") &&
					strings.Contains(s, "stack_test.go:")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fmt.Sprintf(tt.format, f)
			if !tt.check(result) {
				t.Errorf("格式 %q 输出不符合预期: %q", tt.format, result)
			}
		})
	}
}

// TestFrameMarshalText 验证 Frame 的文本序列化功能
// 确保输出格式为 "<函数名> <文件路径>:<行号>"，
// 实现 encoding.TextMarshaler 接口的规范。
func TestFrameMarshalText(t *testing.T) {
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	f := Frame(pcs[0])

	data, err := f.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText 失败: %v", err)
	}

	text := string(data)
	t.Run("序列化内容验证", func(t *testing.T) {
		if !strings.Contains(text, "TestFrameMarshalText") ||
			!strings.Contains(text, "stack_test.go") {
			t.Errorf("MarshalText 输出不符合预期: %q", text)
		}
	})
}

// createTestStackTrace 辅助函数：创建包含两个栈帧的测试用 StackTrace
// 用于模拟真实调用链，内层栈帧为当前函数，外层栈帧为调用者函数。
func createTestStackTrace() StackTrace {
	var pcs [2]uintptr
	runtime.Callers(0, pcs[:]) // 捕获当前调用栈
	return StackTrace{
		Frame(pcs[0]), // 内层函数栈帧
		Frame(pcs[1]), // 外层函数栈帧
	}
}

// TestStackTraceFormat 验证 StackTrace 类型的格式化输出
// 覆盖四种主要格式：
//
// 格式动词 | 描述
// --------|------
// %v      | 默认切片格式，元素为"函数名:行号"
// %+v     | 详细格式，每行一个栈帧的完整信息
// %#v     | Go 语法切片格式，如 "[]errors.Frame{...}"
// %s      | 函数名列表，元素为简短函数名
func TestStackTraceFormat(t *testing.T) {
	st := createTestStackTrace()
	if len(st) < 2 {
		t.Fatal("测试栈帧数量不足，无法完成测试")
	}
	frame0, frame1 := st[0], st[1]

	tests := []struct {
		name       string
		format     string
		wantSubstr []string
	}{
		{
			name:   "%v 格式（默认切片格式）",
			format: "%v",
			wantSubstr: []string{
				fmt.Sprintf("%s:%d", path.Base(frame0.name()), frame0.line()),
				fmt.Sprintf("%s:%d", path.Base(frame1.name()), frame1.line()),
				"[", "]",
			},
		},
		{
			name:   "%+v 格式（详细信息）",
			format: "%+v",
			wantSubstr: []string{
				frame0.name(),
				frame0.file(),
				strconv.Itoa(frame0.line()),
				frame1.name(),
				frame1.file(),
				strconv.Itoa(frame1.line()),
			},
		},
		{
			name:   "%#v 格式（Go语法切片）",
			format: "%#v",
			wantSubstr: []string{
				"[]errors.Frame{",
				"}",
			},
		},
		{
			name:   "%s 格式（函数名列表）",
			format: "%s",
			wantSubstr: []string{
				path.Base(frame0.name()),
				path.Base(frame1.name()),
				"[", "]",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			fmt.Fprintf(&buf, tt.format, st)
			result := buf.String()

			for _, substr := range tt.wantSubstr {
				if !strings.Contains(result, substr) {
					t.Errorf("格式 %q 缺少子串\n预期: %q\n实际: %q",
						tt.format, substr, result)
				}
			}
		})
	}
}

// TestStackTraceEmpty 验证空 StackTrace 的格式化行为
// 空调用栈（长度为 0 的切片）在不同格式下的输出应符合预期：
// - %v 和 %s 输出 "[]"
// - %+v 输出空字符串
// - %#v 输出 "[]errors.Frame(nil)"
func TestStackTraceEmpty(t *testing.T) {
	var emptyST StackTrace

	tests := []struct {
		name   string
		format string
		want   string
	}{
		{"空栈 %v 格式", "%v", "[]"},
		{"空栈 %+v 格式", "%+v", ""},
		{"空栈 %#v 格式", "%#v", "[]errors.Frame(nil)"},
		{"空栈 %s 格式", "%s", "[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			fmt.Fprintf(&buf, tt.format, emptyST)
			if buf.String() != tt.want {
				t.Errorf("空栈格式 %q 不符\n预期: %q\n实际: %q",
					tt.format, tt.want, buf.String())
			}
		})
	}
}

// TestStackFormat 验证 stack 类型的格式化输出
// stack 作为底层存储类型，仅处理 %+v 格式，输出所有栈帧的详细信息。
// 其他格式下应输出空字符串，空 stack 实例在任何格式下均输出空。
func TestStackFormat(t *testing.T) {
	// 生成包含至少2个栈帧的真实调用栈
	var testStack *stack
	func() {
		func() {
			testStack = callers()
		}()
	}()

	if testStack == nil || len(*testStack) == 0 {
		t.Fatal("未能捕获有效的调用栈，无法进行测试")
	}

	tests := []struct {
		name   string
		format string
		want   func(*testing.T, string)
	}{
		{
			name:   "%+v 格式（详细栈帧信息）",
			format: "%+v",
			want: func(t *testing.T, output string) {
				expectedST := testStack.StackTrace()
				if len(expectedST) == 0 {
					t.Error("转换后的 StackTrace 为空")
					return
				}

				for _, frame := range expectedST {
					expectedSubstr := frame.name() + "\n\t" + frame.file()
					if !strings.Contains(output, expectedSubstr) {
						t.Errorf("输出缺少栈帧信息\n预期: %q\n实际: %q",
							expectedSubstr, output)
					}
				}
			},
		},
		{
			name:   "非 %+v 格式（不处理）",
			format: "%v",
			want: func(t *testing.T, output string) {
				if output != "" {
					t.Errorf("非 %%+v 格式应输出空，实际: %q", output)
				}
			},
		},
		{
			name:   "空 stack 的 %+v 格式",
			format: "%+v",
			want: func(t *testing.T, output string) {
				if output != "" {
					t.Errorf("空 stack 应输出空，实际: %q", output)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s *stack
			if tt.name == "空 stack 的 %+v 格式" {
				emptyStack := stack{}
				s = &emptyStack
			} else {
				s = testStack
			}

			var buf bytes.Buffer
			fmt.Fprintf(&buf, tt.format, s)
			tt.want(t, buf.String())
		})
	}
}

// TestFrameFormat_Print 打印 Frame 各种格式的实际输出（调试用）
// 直观展示不同格式动词下的输出效果，便于开发调试。
func TestFrameFormat_Print(t *testing.T) {
	var pcs = make([]uintptr, 1)
	runtime.Callers(0, pcs[:])
	f := Frame(pcs[0])

	t.Log("===== Frame 格式输出示例 =====")
	t.Logf("%%s (简短函数名):\n  %s", f)
	t.Logf("%%+s (完整函数名+文件路径):\n  %+s", f)
	t.Logf("%%v (函数名:行号):\n  %v", f)
	t.Logf("%%+v (完整详细格式):\n  %+v", f)
	t.Logf("%%d (行号):\n  %d", f)
	t.Logf("%%n (简化函数名):\n  %n", f)

	if text, err := f.MarshalText(); err == nil {
		t.Logf("MarshalText() (文本序列化):\n  %s", text)
	}
	t.Log("==============================")
}

// TestStackTrace_Print 打印 StackTrace 各种格式的实际输出（调试用）
// 展示完整调用栈在不同格式下的表现，辅助理解格式化规则。
func TestStackTrace_Print(t *testing.T) {
	stackTrace := callers().StackTrace()

	t.Log("===== StackTrace 格式输出示例 =====")
	t.Logf("%%s (函数名列表):\n  %s", stackTrace)
	t.Logf("%%v (函数名:行号列表):\n  %v", stackTrace)
	t.Logf("%%#v (Go语法切片):\n  %#v", stackTrace)
	t.Logf("%%+v (详细栈信息):\n  %+v", stackTrace)
	t.Log("===================================")
}

// TestStack_Print 打印 stack 类型的格式输出（调试用）
// 展示底层 stack 类型的格式化效果，主要验证 %+v 格式的详细输出。
func TestStack_Print(t *testing.T) {
	stack := callers()

	t.Log("===== stack 格式输出示例 =====")
	t.Logf("%%+v (详细栈信息):\n  %+v", stack)
	t.Log("=============================")
}
