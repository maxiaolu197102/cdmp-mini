package main

import (
	"math/rand"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver"
	_ "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	// 限制为 CPU 核心数的一半或更少
	//	maxProcs := runtime.NumCPU() / 2
	//	if maxProcs < 1 {
	//		maxProcs = 1
	//	}
	//	runtime.GOMAXPROCS(maxProcs)

	// 或者直接设置为固定值
	// runtime.GOMAXPROCS(8)

	//debug.SetGCPercent(100)
	//debug.SetMemoryLimit(4 << 30) // 增加到 4GB

	//version.CheckVersionAndExit()
	apiserver.NewApp("iam-apiserver").Run()

}
