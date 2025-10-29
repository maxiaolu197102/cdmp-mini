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

//go:build !jsoniter
// +build !jsoniter

//package json
// json.go
// 对标准库 encoding/json 进行简单封装，导出常用的JSON序列化/反序列化函数和类型，
// 提供统一的JSON处理接口，便于后续在不修改调用代码的情况下扩展JSON功能（如替换序列化库）。

package json

import "encoding/json" // 导入标准库的JSON处理包

// RawMessage 导出为当前包的类型，等价于标准库的 json.RawMessage，
// 用于表示未解析的JSON原始字节数据（如延迟解析场景）
//type RawMessage = json.RawMessage

var (
	// Marshal 导出标准库的 json.Marshal 函数，用于将Go数据结构序列化为JSON字节流
	Marshal = json.Marshal

	// Unmarshal 导出标准库的 json.Unmarshal 函数，用于将JSON字节流反序列化为Go数据结构
	Unmarshal = json.Unmarshal

	// MarshalIndent 导出标准库的 json.MarshalIndent 函数，用于生成带缩进的格式化JSON字符串
	MarshalIndent = json.MarshalIndent

	// NewDecoder 导出标准库的 json.NewDecoder 函数，创建从输入流读取并解析JSON的解码器
	NewDecoder = json.NewDecoder

	// NewEncoder 导出标准库的 json.NewEncoder 函数，创建向输出流写入JSON的编码器
	NewEncoder = json.NewEncoder
)
