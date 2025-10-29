package policy

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (p *Policy) Get(ctx context.Context, username string, name string, opts metav1.GetOptions) (*v1.Policy, error) {
	return nil, nil
}
