package v1

import (
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"

	policy "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/policy"
	secret "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/secret"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/user"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

type ServiceSrv struct {
	Store    interfaces.Factory
	Redis    *storage.RedisCluster
	Options  *options.Options
	producer producer.MessageProducer
}

type ServiceManager interface {
	Users() user.UserSrv
	Secrets() secret.SecretSrv
	Policies() policy.PolicySrv
}

func (s *ServiceSrv) Users() user.UserSrv {
	return NewUsers(s)
}

func (s *ServiceSrv) Secrets() secret.SecretSrv {
	return NewSecrets(s)
}

func (s *ServiceSrv) Policies() policy.PolicySrv {
	return NewPolicies(s)
}

func NewUsers(s *ServiceSrv) *user.UserService {
	return &user.UserService{
		Store:    s.Store,
		Redis:    s.Redis,
		Options:  s.Options,
		Producer: s.producer,
	}
}
func NewSecrets(s *ServiceSrv) *secret.SecretService {
	return &secret.SecretService{
		Store:   s.Store,
		Redis:   s.Redis,
		Options: s.Options,
	}
}

func NewPolicies(s *ServiceSrv) *policy.PolicService {
	return &policy.PolicService{
		Store:   s.Store,
		Redis:   s.Redis,
		Options: s.Options,
	}
}

func NewService(store interfaces.Factory,
	redis *storage.RedisCluster,
	options *options.Options,
	producer producer.MessageProducer) (ServiceManager, error) {

	s := &ServiceSrv{
		Store:    store,
		Redis:    redis,
		Options:  options,
		producer: producer,
	}
	return s, nil
}
