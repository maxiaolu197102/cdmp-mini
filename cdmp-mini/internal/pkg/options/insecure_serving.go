package options

import (
	"net"
	"os"
	"strconv"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/spf13/pflag"
)

type InsecureServingOptions struct {
	BindAddress string `json:"bind-address" mapstructure:"bind-address"`
	BindPort    int    `json:"bind-port"    mapstructure:"bind-port"`
}

func NewInsecureServingOptions() *InsecureServingOptions {

	return &InsecureServingOptions{
		BindAddress: "0.0.0.0",
		BindPort:    8088,
	}
}

func (i *InsecureServingOptions) Complete() {
	i.loadFromEnv()

	if i.BindAddress == "" {
		i.BindAddress = "0.0.0.0"
	}
	if i.BindPort < 0 || i.BindPort > 65535 {
		i.BindPort = 8080
	}
}

func (i *InsecureServingOptions) Validate() []error {
	var errs = []error{}
	if i.BindAddress == "" {
		errs = append(errs, errors.WithCode(code.ErrValidation, "绑定的地址不能为空"))
	}
	if net.ParseIP(i.BindAddress) == nil {
		errs = append(errs, errors.WithCode(code.ErrValidation, "无效的ip地址%s", i.BindAddress))
	}

	if i.BindPort > 0 {
		if i.BindAddress == "" {
			errs = append(errs, errors.WithCode(code.ErrValidation, "启用端口时需要指定绑定地址"))
		} else {
			address := net.JoinHostPort(i.BindAddress, strconv.Itoa(i.BindPort))
			if _, err := net.ResolveTCPAddr("tcp", address); err != nil {
				errs = append(errs, errors.WithCode(code.ErrValidation, "地址+端口组合无效%v", err))
			}
		}
	}
	return errs
}

func (i *InsecureServingOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&i.BindAddress, "insecure.bind-address", "b", i.BindAddress, "用于监听 --insecure.bind-port（不安全绑定端口）的 IP 地址（若需监听所有 IPv4 接口，设为 0.0.0.0；若需监听所有 IPv6 接口，设为 ::）")
	fs.IntVarP(&i.BindPort, "insecure.bind-port", "p", i.BindPort, "用于提供未加密、未认证访问的端口。默认假设已配置防火墙规则，确保该端口无法从部署机器外部访问，且 IAM 公网地址的 443 端口会被代理到该端口（默认配置下由 nginx 实现此代理）。设为 0 可禁用该端口。")
}

func (i *InsecureServingOptions) loadFromEnv() {
	// 使用与 Bash 脚本一致的环境变量名
	if envAddr := os.Getenv("IAM_APISERVER_INSECURE_BIND_ADDRESS"); envAddr != "" {
		i.BindAddress = envAddr
	}
	if envPort := os.Getenv("IAM_APISERVER_INSECURE_BIND_PORT"); envPort != "" {
		if port, err := strconv.Atoi(envPort); err == nil && port >= 0 && port <= 65535 {
			i.BindPort = port
		}
	}

}
