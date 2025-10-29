package policy

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (p *Policy) Update(ctx context.Context, policy *v1.Policy, opts metav1.UpdateOptions) error {
	return nil
}
