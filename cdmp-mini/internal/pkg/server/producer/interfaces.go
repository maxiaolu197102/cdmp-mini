// internal/pkg/server/producer/interfaces.go
package producer

import (
	"context"
)

// MessageProducer 定义通用的消息生产者接口，支持任意业务实体。
//
// T: 业务实体类型，例如 *v1.User。
// K: 实体删除时使用的主键/标识类型，例如 string。
type MessageProducer[T any, K any] interface {
	// SendCreateMessage 发送创建事件。
	SendCreateMessage(ctx context.Context, entity T) error

	// SendUpdateMessage 发送更新事件。
	SendUpdateMessage(ctx context.Context, entity T) error

	// SendDeleteMessage 发送删除事件。
	SendDeleteMessage(ctx context.Context, key K) error

	// Close 关闭生产者连接。
	Close() error
}
