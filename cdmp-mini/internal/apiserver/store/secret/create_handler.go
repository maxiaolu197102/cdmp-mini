package secret

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (s *Secret) Create(ctx context.Context, secret *v1.Secret, opts metav1.CreateOptions) error {
	return nil
}
