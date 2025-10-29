package options

import (
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	cliFlag "github.com/maxiaolu1981/cretem/nexuscore/component-base/cli/flag"
)

type Options struct {
	InsecureServingOptions *options.InsecureServingOptions `json:"insecure" mapstructure:"insecure"`
	JwtOptions             *options.JwtOptions             `json:"jwt"      mapstructure:"jwt"`
	MysqlOptions           *options.MySQLOptions           `json:"mysql"    mapstructure:"mysql"`
	ServerRunOptions       *options.ServerRunOptions       `json:"server"   mapstructure:"server"`
	Log                    *log.Options                    `json:"log"      mapstructure:"log"`
	RedisOptions           *options.RedisOptions           `json:"redis"    mapstructure:"redis"`
	MetaOptions            *options.MetaOptions            `json:"metoptions" mapstructure:"metoptions"`
	KafkaOptions           *options.KafkaOptions           `json:"kafkaoptions" mapstructure:"kafkaoptions"`
	AuditOptions           *options.AuditOptions           `json:"audit" mapstructure:"audit"`
}

func NewOptions() *Options {
	return &Options{
		InsecureServingOptions: options.NewInsecureServingOptions(),
		JwtOptions:             options.NewJwtOptions(),
		MysqlOptions:           options.NewMySQLOptions(),
		ServerRunOptions:       options.NewServerRunOptions(),
		Log:                    log.NewOptions(),
		RedisOptions:           options.NewRedisOptions(),
		MetaOptions:            options.NewMetaOptions(),
		KafkaOptions:           options.NewKafkaOptions(),
		AuditOptions:           options.NewAuditOptions(),
	}
}

func (o *Options) Complete() {
	o.InsecureServingOptions.Complete()
	o.JwtOptions.Complete()
	o.ServerRunOptions.Complete()
	o.MysqlOptions.Complete()
	o.Log.Complete()
	o.MetaOptions.Complete()
	o.KafkaOptions.Complete()
	o.AuditOptions.Complete()

}

func (o *Options) Validate() []error {
	var errs []error
	errs = append(errs, o.InsecureServingOptions.Validate()...)
	errs = append(errs, o.JwtOptions.Validate()...)
	errs = append(errs, o.MysqlOptions.Validate()...)
	errs = append(errs, o.ServerRunOptions.Validate()...)
	errs = append(errs, o.Log.Validate()...)
	errs = append(errs, o.RedisOptions.Validate()...)
	errs = append(errs, o.MetaOptions.Validate()...)
	errs = append(errs, o.KafkaOptions.Validate()...)
	errs = append(errs, o.AuditOptions.Validate()...)

	return errs
}

func (o *Options) Flags() (fss cliFlag.NamedFlagSets) {
	o.InsecureServingOptions.AddFlags(fss.FlagSet("insecure serving"))
	o.MysqlOptions.AddFlags(fss.FlagSet("mysql"))
	o.JwtOptions.AddFlags(fss.FlagSet("jwt"))
	o.ServerRunOptions.AddFlags(fss.FlagSet("server"))
	o.Log.AddFlags(fss.FlagSet("log"))
	o.RedisOptions.AddFlags(fss.FlagSet("redis"))
	o.MetaOptions.AddFlags(fss.FlagSet("meta"))
	o.KafkaOptions.AddFlags(fss.FlagSet("kafka"))
	o.AuditOptions.AddFlags(fss.FlagSet("audit"))

	return fss
}
