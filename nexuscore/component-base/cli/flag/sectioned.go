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

// Package flag
// sectioned.go
// 提供命名标志集（NamedFlagSets）的管理与格式化输出功能，
// 支持按名称组织多个pflag.FlagSet并按顺序打印为分节的帮助信息，
// 适用于需要将命令行参数分类展示的场景（如按功能模块划分参数）
package flag

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/pflag" // 引入pflag库，用于处理命令行标志
)

// NamedFlagSets 用于按名称存储多个标志集（FlagSet），并记录添加顺序
// 适用于将不同类别的标志分组管理（如"global"、"server"、"client"等分组）
type NamedFlagSets struct {
	Order    []string                  // 标志集名称的有序列表，保证打印顺序
	FlagSets map[string]*pflag.FlagSet // 映射：标志集名称 → 对应的pflag.FlagSet实例
}

// FlagSet 获取指定名称的标志集，若不存在则创建并添加到有序列表中
// 参数：
//   - name: 标志集的名称（如"global"、"database"）
//
// 返回：
//   - 对应的*pflag.FlagSet实例（新创建或已存在的）
func (nfs *NamedFlagSets) FlagSet(name string) *pflag.FlagSet {
	// 初始化映射（首次调用时）
	if nfs.FlagSets == nil {
		nfs.FlagSets = map[string]*pflag.FlagSet{}
	}
	// 若标志集不存在，则创建并添加到有序列表
	if _, ok := nfs.FlagSets[name]; !ok {
		nfs.FlagSets[name] = pflag.NewFlagSet(name, pflag.ExitOnError) // 创建新标志集
		nfs.Order = append(nfs.Order, name)                            // 记录顺序
	}
	return nfs.FlagSets[name]
}

// PrintSections 将命名标志集中的所有标志按分组打印为分节的帮助信息，支持按指定列宽换行
// 参数：
//   - w: 输出目标（如os.Stdout）
//   - fss: 要打印的NamedFlagSets实例
//   - cols: 最大列宽（0表示不换行）
func PrintSections(w io.Writer, fss NamedFlagSets, cols int) {
	// 按添加顺序遍历所有标志集
	for _, name := range fss.Order {
		fs := fss.FlagSets[name]
		// 跳过无标志的空集
		if !fs.HasFlags() {
			continue
		}

		// 创建临时标志集，用于格式化输出（避免修改原标志集）
		wideFS := pflag.NewFlagSet("", pflag.ExitOnError)
		wideFS.AddFlagSet(fs) // 复制原标志集的所有标志

		// 处理列宽：若cols>24，添加临时标志辅助计算换行（内部实现细节）
		var zzz string
		if cols > 24 {
			zzz = strings.Repeat("z", cols-24)               // 生成填充字符串
			wideFS.Int(zzz, 0, strings.Repeat("z", cols-24)) // 添加临时标志
		}

		// 构建分节输出内容
		var buf bytes.Buffer
		// 打印分组标题（首字母大写，如"Global flags:"）
		fmt.Fprintf(&buf, "\n%s flags:\n\n%s", strings.ToUpper(name[:1])+name[1:], wideFS.FlagUsagesWrapped(cols))

		// 处理换行格式：移除临时标志产生的多余内容
		if cols > 24 {
			// 找到临时标志的位置，截断多余内容
			i := strings.Index(buf.String(), zzz)
			lines := strings.Split(buf.String()[:i], "\n")
			// 打印除最后一行外的内容（最后一行为临时标志的信息）
			fmt.Fprint(w, strings.Join(lines[:len(lines)-1], "\n"))
			fmt.Fprintln(w)
		} else {
			// 列宽不足时直接输出完整内容
			fmt.Fprint(w, buf.String())
		}
	}
}
