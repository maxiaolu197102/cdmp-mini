package user

import (
	"context"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

// Create 创建用户
func (u *Users) Create(ctx context.Context, user *v1.User, opts metav1.CreateOptions, opt *options.Options) error {
	return nil
}
