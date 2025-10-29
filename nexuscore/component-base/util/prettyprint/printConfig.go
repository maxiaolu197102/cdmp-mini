package prettyprint

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/maxiaolu1981/cretem/nexuscore/component-base/term"
	"github.com/spf13/viper"
)

// printConfig 以结构化方式打印所有配置项信息
func PrintConfig() error {
	keys := viper.AllKeys()
	if len(keys) == 0 {
		return nil // 没有配置项时不打印
	}

	// 获取终端宽度
	cols := getTerminalWidth()
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)

	// 写入标题
	title := fmt.Sprintf("打印配置项(%s)", viper.ConfigFileUsed())
	writeTitle(tw, title, cols)

	// 按层级打印配置项
	configMap := buildConfigMap(keys)
	printConfigMap(tw, configMap, "", 0)

	// 刷新输出
	if err := tw.Flush(); err != nil {
		return err
	}

	// 输出结果
	fmt.Print(buf.String())
	fmt.Println()
	return nil
}

// buildConfigMap 将扁平的配置键转换为层级映射
func buildConfigMap(keys []string) map[string]interface{} {
	root := make(map[string]interface{})
	for _, key := range keys {
		parts := strings.Split(key, ".")
		current := root

		// 逐级创建嵌套映射
		for i, part := range parts {
			if i == len(parts)-1 {
				// 最后一级，存储配置值
				current[part] = viper.Get(key)
			} else {
				// 非最后一级，确保映射存在
				if _, exists := current[part]; !exists {
					current[part] = make(map[string]interface{})
				}
				// 进入下一级
				current = current[part].(map[string]interface{})
			}
		}
	}
	return root
}

// printConfigMap 递归打印配置映射
func printConfigMap(tw *tabwriter.Writer, configMap map[string]interface{}, prefix string, depth int) {
	for key, value := range configMap {
		fullKey := prefix + key
		nestedMap, isNested := value.(map[string]interface{})

		if isNested {
			// 嵌套配置项，递归打印
			fmt.Fprintf(tw, "%s:\n", fullKey)
			printConfigMap(tw, nestedMap, prefix+"  ", depth+1)
		} else {
			// 普通配置项，直接打印键值对
			fmt.Fprintf(tw, "%s:\t%v\n", fullKey, value)
		}
	}
	// 同级配置项之间添加空行分隔
	if depth == 0 {
		fmt.Fprintln(tw)
	}
}

// getTerminalWidth 获取终端宽度（复用之前的实现）
func getTerminalWidth() int {
	if os.Stdout == nil {
		return 120
	}
	cols, _, err := term.TerminalSize(os.Stdout)
	if err != nil {
		return 120
	}
	if cols < 40 {
		return 80
	}
	if cols > 300 {
		return 120
	}
	return cols
}

// writeTitle 写入标题（复用之前的实现）
func writeTitle(tw *tabwriter.Writer, title string, cols int) {
	fmt.Fprintln(tw, strings.Repeat("=", cols))
	titleLen := len(title)
	if titleLen < cols {
		padding := (cols - titleLen) / 2
		fmt.Fprint(tw, strings.Repeat(" ", padding))
	}
	fmt.Fprintf(tw, "%s\n", title)
	fmt.Fprintln(tw, strings.Repeat("=", cols))
	fmt.Fprintln(tw)
}
