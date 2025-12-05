package producer

import (
	"errors"
	"fmt"
	"sync"
)

// Registry 管理不同业务对象的消息生产者实例。
type Registry struct {
	mu        sync.RWMutex
	producers map[string]any
	closers   map[string]closer
}

type closer interface {
	Close() error
}

type closeFunc func() error

func (fn closeFunc) Close() error {
	if fn == nil {
		return nil
	}
	return fn()
}

// NewRegistry 创建一个空的生产者注册表。
func NewRegistry() *Registry {
	return &Registry{
		producers: make(map[string]any),
		closers:   make(map[string]closer),
	}
}

// registerRaw 在内部注册生产者实例，负责资源回收处理。
func (r *Registry) registerRaw(key string, producer any, closer closer) error {
	if key == "" {
		return fmt.Errorf("producer key cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	if existingCloser, ok := r.closers[key]; ok && existingCloser != nil {
		if closeErr := existingCloser.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		delete(r.producers, key)
		delete(r.closers, key)
	}

	if producer == nil {
		delete(r.producers, key)
		delete(r.closers, key)
		return err
	}

	r.producers[key] = producer
	r.closers[key] = closer
	return err
}

// RegisterProducer 将指定 key 的生产者注册到表中，必要时覆盖旧实例。
func RegisterProducer[T any, K any](registry *Registry, key string, p MessageProducer[T, K]) error {
	if registry == nil {
		return fmt.Errorf("registry is nil")
	}
	return registry.registerRaw(key, p, closeFunc(func() error {
		if p == nil {
			return nil
		}
		return p.Close()
	}))
}

// GetProducer 返回指定 key 的生产者，并在类型匹配时断言。
func GetProducer[T any, K any](registry *Registry, key string) (MessageProducer[T, K], bool) {
	if registry == nil {
		return nil, false
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	raw, ok := registry.producers[key]
	if !ok {
		return nil, false
	}

	producerTyped, ok := raw.(MessageProducer[T, K])
	if !ok {
		return nil, false
	}
	return producerTyped, true
}

// CloseAll 依次关闭所有注册的生产者实例。
func (r *Registry) CloseAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for key, c := range r.closers {
		if c == nil {
			continue
		}
		if closeErr := c.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close producer %s: %w", key, closeErr))
		}
		delete(r.producers, key)
		delete(r.closers, key)
	}
	return err
}
