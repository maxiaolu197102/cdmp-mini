package secret

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (s *Secret) List(ctx context.Context, username string, opts metav1.ListOptions) (*v1.SecretList, error) {
	return nil, nil
}
