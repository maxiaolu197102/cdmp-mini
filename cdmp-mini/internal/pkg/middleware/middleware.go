package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
)

func InstallMiddlewares(engine *gin.Engine, opt *options.Options) error {

	// 安装通用中间件
	commonMiddlewares := common.GetMiddlewareStack(opt)
	for _, mw := range commonMiddlewares {
		engine.Use(mw)
	}

	// 安装业务中间件（根据配置决定）

	return nil
}
