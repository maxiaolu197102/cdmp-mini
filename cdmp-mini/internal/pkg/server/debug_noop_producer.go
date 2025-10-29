package server

import (
	"context"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
)

type noopProducer struct{}

func newNoopProducer() producer.MessageProducer {
	return &noopProducer{}
}

func (n *noopProducer) SendUserCreateMessage(ctx context.Context, user *v1.User) error {
	return nil
}

func (n *noopProducer) SendUserUpdateMessage(ctx context.Context, user *v1.User) error {
	return nil
}

func (n *noopProducer) SendUserDeleteMessage(ctx context.Context, username string) error {
	return nil
}

func (n *noopProducer) Close() error {
	return nil
}
