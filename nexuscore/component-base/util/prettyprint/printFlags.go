package prettyprint

import (
	"bytes"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/pflag"
)

// PrintFlags 打印标志集中所有已设置的标志及其值（调试用）
// 遍历所有标志并通过Debug级别日志输出，便于开发和调试时确认参数是否正确解析
// PrintFlags 以结构化方式打印pflag.FlagSet中的所有标志
func PrintFlags(flags *pflag.FlagSet) error {
	if flags == nil {
		return fmt.Errorf("flagset is nil")
	}
	flags.SortFlags = true
	// 获取终端宽度
	cols := getTerminalWidth()
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)

	// 写入标题
	writeTitle(tw, "Flags", cols)

	// 遍历所有标志并打印
	flags.VisitAll(func(flag *pflag.Flag) {
		printFlag(tw, flag, "", 0)
	})

	// 刷新输出
	if err := tw.Flush(); err != nil {
		return err
	}

	// 输出结果
	fmt.Print(buf.String())
	return nil
}

// printFlag 打印单个flag的详细信息
func printFlag(tw *tabwriter.Writer, flag *pflag.Flag, prefix string, depth int) {
	// 基础前缀
	basePrefix := prefix + flag.Name
	fmt.Fprintf(tw, "%s:\n", basePrefix)

	// 子项前缀（缩进）
	childPrefix := prefix + "  "

	// 打印flag的详细属性
	fmt.Fprintf(tw, "%sValue:\t%v\n", childPrefix, flag.Value)
	fmt.Fprintf(tw, "%sDefault:\t%v\n", childPrefix, flag.DefValue)
	fmt.Fprintf(tw, "%sShorthand:\t%q\n", childPrefix, flag.Shorthand)
	fmt.Fprintf(tw, "%sUsage:\t%s\n", childPrefix, flag.Usage)
	fmt.Fprintf(tw, "%sChanged:\t%t\n", childPrefix, flag.Changed)

	// 如果有 shorthand，显示组合形式
	if flag.Shorthand != "" {
		fmt.Fprintf(tw, "%sForm:\t-%s, --%s\n", childPrefix, flag.Shorthand, flag.Name)
	} else {
		fmt.Fprintf(tw, "%sForm:\t--%s\n", childPrefix, flag.Name)
	}

	// 空行分隔不同flag
	fmt.Fprintln(tw)
}
