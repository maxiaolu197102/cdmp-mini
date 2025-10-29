// internal/pkg/server/producer/interfaces.go
package producer

import (
	"context"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
)

// MessageProducer 定义通用的消息生产者接口
type MessageProducer interface {
	// SendUserCreateMessage 发送用户创建消息
	SendUserCreateMessage(ctx context.Context, user *v1.User) error

	// SendUserUpdateMessage 发送用户更新消息
	SendUserUpdateMessage(ctx context.Context, user *v1.User) error

	// SendUserDeleteMessage 发送用户删除消息
	SendUserDeleteMessage(ctx context.Context, username string) error

	// Close 关闭生产者连接
	Close() error
}
