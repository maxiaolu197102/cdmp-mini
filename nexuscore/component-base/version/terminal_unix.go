//go:build linux || darwin
// +build linux darwin

package version

import (
	"fmt"
	"os"

	// 关键：使用 golang.org/x/sys/unix 替代标准库 syscall
	"golang.org/x/sys/unix"
)

// getTerminalCols 获取 Unix 系统（Linux/macOS）的终端宽度
func getTerminalCols(f *os.File) (int, error) {
	// 1. 调用 unix.IoctlGetWinsize 获取终端窗口信息
	// 函数作用：专门用于获取终端的宽高（封装了 ioctl + TIOCGWINSZ 逻辑）
	// 参数1：文件描述符（f.Fd() 是终端的文件描述符）
	// 参数2：请求码（unix.TIOCGWINSZ 表示“获取窗口大小”）
	winsize, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil {
		return 0, fmt.Errorf("unix: failed to get terminal size: %w", err)
	}

	// 2. 返回终端列数（即宽度）
	return int(winsize.Col), nil
}
