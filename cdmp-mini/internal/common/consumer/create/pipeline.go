package create

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// MessageOutcome 表示单条消费消息的执行结果。
//
// param Success: true 表示全部钩子执行完成，false 表示中途失败。
// param Created: 在持久化阶段成功写入实体时为 true。
// param Err: 当出现错误时返回具体错误信息。
type MessageOutcome[T any] struct {
	Success bool
	Created bool
	Err     error
	Entity  T
}

// PipelineConfig 定义批量创建消费者流程的钩子集合。
//
// param Decode: 反序列化 Kafka 消息的函数，必填。
// param OnDecodeError: 解码失败时的回调，允许为 nil。
// param Validate: 业务校验钩子，允许为 nil。
// param OnValidationError: 校验失败时的回调，允许为 nil。
// param Normalize: 字段规范化钩子，允许为 nil。
// param Prepare: 执行持久化前的预处理逻辑，允许为 nil。
// param BeforePersist: 持久化前的补充操作（如埋点），允许为 nil。
// param Persist: 实际写入存储的函数，必填。
// param OnPersistError: 持久化失败时的回调，允许为 nil。
// param AfterSuccess: 持久化成功后的补充操作（如写缓存），允许为 nil。
type PipelineConfig[T any] struct {
	Decode            func(context.Context, kafka.Message) (T, error)
	OnDecodeError     func(context.Context, kafka.Message, error)
	Validate          func(context.Context, kafka.Message, T) error
	OnValidationError func(context.Context, kafka.Message, T, error)
	Normalize         func(T)
	Prepare           func(T) error
	BeforePersist     func(context.Context, kafka.Message, T) error
	Persist           func(context.Context, kafka.Message, T) (bool, error)
	OnPersistError    func(context.Context, kafka.Message, T, error)
	AfterSuccess      func(context.Context, kafka.Message, T, bool) error
}

// Pipeline 封装批量创建消息的通用处理流程。
type Pipeline[T any] struct {
	cfg PipelineConfig[T]
}

// NewPipeline 创建批量创建消费管道实例。
func NewPipeline[T any](cfg PipelineConfig[T]) *Pipeline[T] {
	return &Pipeline[T]{cfg: cfg}
}

// Process 处理单条 Kafka 消息。
//
// param ctx: 上下文，同步透传到所有钩子。
// param msg: Kafka 消息。
//
// returns: MessageOutcome，包含执行状态与是否写入成功。
func (p *Pipeline[T]) Process(ctx context.Context, msg kafka.Message) MessageOutcome[T] {
	var outcome MessageOutcome[T]
	if p == nil {
		return outcome
	}
	if p.cfg.Decode == nil || p.cfg.Persist == nil {
		return outcome
	}

	entity, err := p.cfg.Decode(ctx, msg)
	if err != nil {
		outcome.Err = err
		if p.cfg.OnDecodeError != nil {
			p.cfg.OnDecodeError(ctx, msg, err)
		}
		return outcome
	}
	outcome.Entity = entity

	if p.cfg.Validate != nil {
		if err = p.cfg.Validate(ctx, msg, entity); err != nil {
			outcome.Err = err
			if p.cfg.OnValidationError != nil {
				p.cfg.OnValidationError(ctx, msg, entity, err)
			}
			return outcome
		}
	}

	if p.cfg.Normalize != nil {
		p.cfg.Normalize(entity)
	}

	if p.cfg.Prepare != nil {
		if err = p.cfg.Prepare(entity); err != nil {
			outcome.Err = err
			return outcome
		}
	}

	if p.cfg.BeforePersist != nil {
		if err = p.cfg.BeforePersist(ctx, msg, entity); err != nil {
			outcome.Err = err
			return outcome
		}
	}

	created, err := p.cfg.Persist(ctx, msg, entity)
	if err != nil {
		outcome.Err = err
		if p.cfg.OnPersistError != nil {
			p.cfg.OnPersistError(ctx, msg, entity, err)
		}
		return outcome
	}

	if p.cfg.AfterSuccess != nil {
		if err = p.cfg.AfterSuccess(ctx, msg, entity, created); err != nil {
			outcome.Err = err
			return outcome
		}
	}

	outcome.Success = true
	outcome.Created = created
	return outcome
}
