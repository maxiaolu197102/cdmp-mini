package main

import (
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver"
	_ "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
)

func main() {
	apiserver.NewApp("iam-apiserver").Run()

}
