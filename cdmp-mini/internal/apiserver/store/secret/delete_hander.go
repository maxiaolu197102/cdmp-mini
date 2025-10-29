package secret

import (
	"context"

	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

func (s *Secret) Delete(ctx context.Context, username, secretID string, opts metav1.DeleteOptions) error {
	return nil
}
func (s *Secret) DeleteCollection(ctx context.Context, username string, secretIDs []string, opts metav1.DeleteOptions) error {
	return nil
}
