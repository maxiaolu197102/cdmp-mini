package apiserver

import (
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/app"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

const commandDesc = `IAM API 服务器用于验证和配置 API 对象的数据，这些对象包括用户、策略、密钥等。该 API 服务器通过处理 REST 操作来实现对这些 API 对象的管理。`

func NewApp(basename string) *app.App {
	opt := options.NewOptions()
	application, _ := app.NewApp(basename, "api server",
		app.WithOptions(opt),
		app.WithDefaultValidArgs(),
		app.WithRunFunc(run(opt)),
		app.WithDescription(commandDesc),
		app.WithNoConfig(),
	)

	return application
}

func run(opt app.CliOptions) app.Runfunc {
	return func(basename string) error {
		opt, ok := opt.(*options.Options)
		if !ok {
			log.Fatal("转换Options错误")

		}
		log.Init(opt.Log)
		defer log.Flush()
		return Run(opt)
	}
}
