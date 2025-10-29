package user

import (
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	service "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
)

type UserController struct {
	srv      service.ServiceManager
	options  *options.Options
	Producer producer.MessageProducer
}

// NewUserController creates a user handler.
func NewUserController(store interfaces.Factory,
	redis *storage.RedisCluster,
	options *options.Options,
	producer producer.MessageProducer) (*UserController, error) {

	s, err := service.NewService(store,
		redis, options, producer)
	if err != nil {
		return nil, err
	}
	return &UserController{
		srv:      s,
		options:  options,
		Producer: producer,
	}, nil
}

// BusinessValidateListOptions 业务层验证函数
func (u *UserController) validateListOptions(opts *v1.ListOptions) field.ErrorList {

	return validation.ValidateListOptionsBase(opts)
}
