package version

import (
	"bytes"
	"fmt"
	"os" // 关键：确保导入 os 包（解决 os.Stdout 未定义问题）
	"runtime"
	"strings"
	"text/tabwriter"

	"github.com/mattn/go-isatty"

	// 替换为项目实际的 JSON 包路径
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/json"
	"github.com/maxiaolu1981/cretem/nexuscore/log"
	// 替换为项目实际的日志包路径
)

// 全局版本元数据（编译时通过 -ldflags 注入）
var (
	GitVersion   = "v0.0.0-master+$Format:%h$"
	BuildDate    = "1970-01-01T00:00:00Z"
	GitCommit    = "$Format:%H$"
	GitTreeState = ""
)

// Info 封装程序版本相关的所有元数据
type Info struct {
	GitVersion   string `json:"gitVersion"`
	GitCommit    string `json:"gitCommit"`
	GitTreeState string `json:"gitTreeState"`
	BuildDate    string `json:"buildDate"`
	GoVersion    string `json:"goVersion"`
	Compiler     string `json:"compiler"`
	Platform     string `json:"platform"`
}

// Get 获取完整的版本信息实例
func Get() Info {
	return Info{
		GitVersion:   GitVersion,
		GitCommit:    GitCommit,
		GitTreeState: GitTreeState,
		BuildDate:    BuildDate,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// ToJSON 将版本信息序列化为带缩进的 JSON 字符串
func (info Info) ToJSON() string {
	s, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		log.Warnf("marshal version info to JSON failed: %v", err)
		return fmt.Sprintf(`{"error":"%v"}`, err)
	}
	return string(s)
}

// GetAdaptiveString 生成自适应宽度的版本信息字符串（核心修正：os.Stdout 空值判断）
func (info Info) GetAdaptiveString() (string, error) {
	// 1. 初始化默认宽度（80字符）
	cols := 80

	// 2. 关键修正：先判断 os.Stdout 是否为 nil（避免空指针 panic）
	if os.Stdout != nil {
		// 断言 os.Stdout 为 *os.File（标准输出本质是文件类型）
		// 第 69 行左右的修正代码：
		if os.Stdout != nil { // 先判断非空，避免空指针
			// 直接使用 os.Stdout 的 Fd() 方法（*os.File 有 Fd() 方法），无需多余断言
			if isatty.IsTerminal(os.Stdout.Fd()) {
				// 调用 getTerminalCols 时直接传入 os.Stdout（已确定是 *os.File）
				// if terminalCols, err := getTerminalCols(os.Stdout); err == nil && terminalCols >= 40 {
				// 	cols = terminalCols
				// }
			}
		}
	}

	// 3. 用缓冲区承接 tabwriter 输出
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', tabwriter.DiscardEmptyColumns)
	tw.Init(&buf, 0, cols, 2, ' ', tabwriter.DiscardEmptyColumns)

	// 4. 写入标题和分隔线
	title := "打印版本信息"
	titleLen := len(title)
	if titleLen < cols {
		padding := (cols - titleLen) / 2
		fmt.Fprint(tw, strings.Repeat(" ", padding))
	}
	fmt.Fprintf(tw, "%s\n", title)
	fmt.Fprint(tw, strings.Repeat("-", cols)+"\n\n")

	// 5. 写入版本键值对
	versionRows := []struct {
		Key   string
		Value string
	}{
		{"Version", info.GitVersion},
		{"Git Commit", info.GitCommit},
		{"Git Tree State", info.GitTreeState},
		{"Build Date", info.BuildDate},
		{"Go Version", info.GoVersion},
		{"Compiler", info.Compiler},
		{"Platform", info.Platform},
		{"Description", "基于 Gin 框架的 API 服务，支持 JWT 认证与版本化接口管理"},
	}
	for _, row := range versionRows {
		fmt.Fprintf(tw, "%s\t%s\n", row.Key+":", row.Value)
	}

	// 6. 刷新 tabwriter
	if err := tw.Flush(); err != nil {
		return "", fmt.Errorf("flush tabwriter failed: %w", err)
	}
	return buf.String(), nil
}

// PrintVersionWithLog 用日志打印版本信息
func (info Info) PrintVersionWithLog() {
	versionStr, err := info.GetAdaptiveString()
	if err != nil {
		log.Errorf("generate adaptive version string failed: %v", err)
		return
	}
	log.Infof("Program version information:\n%s", versionStr)
}

// CheckVersionAndExit 检查版本参数并打印退出
func CheckVersionAndExit() {
	// 关键修正：遍历 os.Args 前先判断是否为 nil（避免空指针）
	if os.Args == nil {
		return
	}
	for _, arg := range os.Args {
		switch arg {
		case "--version", "--version=true":
			Get().PrintVersionWithLog()
			os.Exit(0)
		case "--version=raw":
			log.Infof("Raw version information:\n%s", Get().ToJSON())
			os.Exit(0)
		}
	}
}
