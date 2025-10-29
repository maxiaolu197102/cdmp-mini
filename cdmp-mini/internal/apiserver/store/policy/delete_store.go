package policy

import (
	"context"

	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (p *Policy) Delete(ctx context.Context, username string, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (p *Policy) DeleteCollection(ctx context.Context, username string, names []string, opts metav1.DeleteOptions) error {
	return nil
}
