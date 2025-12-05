package server

import (
	"context"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
)

type noopProducer struct{}

func newNoopProducer() producer.MessageProducer[*v1.User, string] {
	return &noopProducer{}
}

func (n *noopProducer) SendCreateMessage(ctx context.Context, user *v1.User) error {
	return nil
}

func (n *noopProducer) SendUpdateMessage(ctx context.Context, user *v1.User) error {
	return nil
}

func (n *noopProducer) SendDeleteMessage(ctx context.Context, username string) error {
	return nil
}

func (n *noopProducer) Close() error {
	return nil
}
