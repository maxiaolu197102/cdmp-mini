/*
包摘要
该包（package middleware）提供了基于 Gin 框架的日志中间件（Logger），用于记录 HTTP 请求的关键信息（如状态码、客户端 IP、处理耗时等）。它支持自定义日志格式、输出目标和忽略路径，同时适配终端彩色输出，是跟踪请求处理情况的核心组件。
核心流程
日志配置初始化：通过 Logger 系列函数（Logger、LoggerWithFormatter 等）初始化日志中间件，支持自定义格式、输出目标（如文件）和需忽略的路径。
请求处理前准备：中间件在请求到达时记录开始时间，存储请求路径和原始查询参数。
请求处理与日志生成：请求经过业务处理器后，中间件计算处理耗时，收集状态码、客户端 IP 等信息，通过格式化函数生成日志内容。
日志输出：根据配置的输出目标（默认标准输出）和终端环境（是否支持彩色），输出格式化后的日志。
*/
package common

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mattn/go-isatty" // 用于判断输出设备是否为终端（支持彩色输出）
)

// defaultLogFormatter 是日志中间件默认使用的日志格式化函数
var defaultLogFormatter = func(param gin.LogFormatterParams) string {
	var statusColor, methodColor, resetColor string
	// 若输出支持彩色，则为状态码和方法名添加颜色
	if param.IsOutputColor() {
		statusColor = param.StatusCodeColor() // 状态码颜色（如 200 为绿色，500 为红色）
		methodColor = param.MethodColor()     // HTTP 方法颜色（如 GET 为蓝色）
		resetColor = param.ResetColor()       // 重置颜色
	}

	// 若请求耗时超过 1 分钟，截断为秒级（避免显示过多小数）
	if param.Latency > time.Minute {
		param.Latency = param.Latency - param.Latency%time.Second
	}

	// 格式化日志字符串：状态码、客户端 IP、耗时、方法、路径、错误信息
	return fmt.Sprintf("%s%3d%s - [%s] \"%v %s%s%s %s\" %s",
		statusColor, param.StatusCode, resetColor, // 带颜色的状态码
		param.ClientIP,                        // 客户端 IP 地址
		param.Latency,                         // 请求处理耗时
		methodColor, param.Method, resetColor, // 带颜色的 HTTP 方法
		param.Path,         // 请求路径（含查询参数）
		param.ErrorMessage, // 错误信息（若有）
	)
}

// Logger 创建一个日志中间件，默认将日志写入 gin.DefaultWriter（标准输出）
func Logger() gin.HandlerFunc {
	return LoggerWithConfig(GetLoggerConfig(nil, nil, nil))
}

// LoggerWithFormatter 创建一个日志中间件，使用指定的日志格式化函数
func LoggerWithFormatter(f gin.LogFormatter) gin.HandlerFunc {
	return LoggerWithConfig(gin.LoggerConfig{
		Formatter: f,
	})
}

// LoggerWithWriter 创建一个日志中间件，将日志写入指定的输出流（如文件），并可指定忽略的路径
// 示例：os.Stdout（标准输出）、打开的文件、网络套接字等
func LoggerWithWriter(out io.Writer, notlogged ...string) gin.HandlerFunc {
	return LoggerWithConfig(gin.LoggerConfig{
		Output:    out,
		SkipPaths: notlogged,
	})
}

// LoggerWithConfig 根据配置创建日志中间件，支持自定义格式、输出目标和忽略路径
func LoggerWithConfig(conf gin.LoggerConfig) gin.HandlerFunc {
	// 若未指定格式化函数，使用默认格式
	formatter := conf.Formatter
	if formatter == nil {
		formatter = defaultLogFormatter
	}

	// 若未指定输出目标，使用 Gin 默认输出（标准输出）
	out := conf.Output
	if out == nil {
		out = gin.DefaultWriter
	}

	// 需要忽略日志的路径（如健康检查接口）
	notlogged := conf.SkipPaths

	// 判断输出设备是否为终端（用于决定是否启用彩色输出）
	isTerm := true
	if w, ok := out.(*os.File); !ok || os.Getenv("TERM") == "dumb" ||
		(!isatty.IsTerminal(w.Fd()) && !isatty.IsCygwinTerminal(w.Fd())) {
		isTerm = false
	}

	// 若为终端，强制启用控制台彩色输出
	if isTerm {
		gin.ForceConsoleColor()
	}

	// 将需忽略的路径转换为 map，便于快速查询
	var skip map[string]struct{}
	if length := len(notlogged); length > 0 {
		skip = make(map[string]struct{}, length)
		for _, path := range notlogged {
			skip[path] = struct{}{}
		}
	}

	// 返回中间件函数
	return func(c *gin.Context) {
		// 1. 记录请求开始时间
	
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery // 原始查询参数（如 ?id=1&name=test）

		// 2. 处理请求（执行后续中间件和业务处理器）
		c.Next()

		// 3. 若路径不在忽略列表中，则生成并输出日志
		if _, ok := skip[path]; !ok {
		

			// 拼接路径和查询参数（如 /api/users?id=1）
			if raw != "" {
				path = path + "?" + raw
			}
	

			// 生成日志并输出（原代码注释了输出逻辑，实际使用时需补充）
			//log.L(c).Debug(formatter(param))
		}
	}
}
