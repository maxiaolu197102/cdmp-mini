package policy

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (p *PolicService) List(ctx context.Context, username string, opts metav1.ListOptions) (*v1.PolicyList, error) {
	return nil, nil
}
