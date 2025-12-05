package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// MessageConsumer 抽象 Kafka 消费者的统一生命周期接口。
type MessageConsumer interface {
	Start(ctx context.Context, ready *sync.WaitGroup)
	Close() error
}

// Registry 负责管理不同业务键下的消费者集合。
type Registry struct {
	mu        sync.RWMutex
	consumers map[string][]MessageConsumer
}

// NewRegistry 创建一个空的消费者注册表。
func NewRegistry() *Registry {
	return &Registry{consumers: make(map[string][]MessageConsumer)}
}

// Register 将消费者实例按业务键追加到注册表中。
func (r *Registry) Register(key string, c MessageConsumer) error {
	if r == nil {
		return fmt.Errorf("registry is nil")
	}
	if key == "" {
		return fmt.Errorf("consumer key cannot be empty")
	}
	if c == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.consumers[key] = append(r.consumers[key], c)
	return nil
}

// List 返回指定业务键下的全部消费者拷贝。
func (r *Registry) List(key string) []MessageConsumer {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	instances := r.consumers[key]
	if len(instances) == 0 {
		return nil
	}

	out := make([]MessageConsumer, 0, len(instances))
	out = append(out, instances...)
	return out
}

// Range 依次遍历所有注册的消费者实例。
func (r *Registry) Range(fn func(key string, c MessageConsumer)) {
	if r == nil || fn == nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for key, list := range r.consumers {
		for _, c := range list {
			if c == nil {
				continue
			}
			fn(key, c)
		}
	}
}

// CloseAll 关闭注册表中所有消费者并清空记录。
func (r *Registry) CloseAll() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for key, list := range r.consumers {
		for idx, c := range list {
			if c == nil {
				continue
			}
			if closeErr := c.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close consumer %s[%d]: %w", key, idx, closeErr))
			}
		}
	}

	r.consumers = make(map[string][]MessageConsumer)
	return err
}
