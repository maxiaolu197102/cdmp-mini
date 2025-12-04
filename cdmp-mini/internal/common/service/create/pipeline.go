package create

import (
	"context"
	"time"
)

// PreflightResult 封装唯一性预检阶段的返回结果。
//
// param Conflicts: 以字段名为键的冲突实体映射，可为空 map 表示未命中。
// param UsernameChecked: 标记用户名是否已在预检阶段确认过，true 时可跳过后续探库。
type PreflightResult[T any] struct {
	Conflicts       map[string]T
	UsernameChecked bool
}

// PendingResult 描述 pending 标记阶段返回的元信息。
//
// param Created: true 表示首次创建占位，false 表示刷新已有占位。
// param Refreshed: true 表示刷新过已有占位。
// param TTL: Redis 占位标记剩余存活时间。
// param SetDuration: 执行 SetNX 或 Acquire 操作的耗时。
// param RefreshDuration: 刷新占位时的耗时，当未刷新时为 0。
type PendingResult struct {
	Created         bool
	Refreshed       bool
	TTL             time.Duration
	SetDuration     time.Duration
	RefreshDuration time.Duration
	OwnerID         string
	Backend         string
}

// PipelineConfig 为通用创建流程提供自定义钩子。
//
// param Name: 资源名称（推荐使用表名），主要用于日志或调试。
// param Begin: 创建流程开始前的回调，返回新的上下文以及结束函数（需负责 Span 收尾等）。
// param Normalize: 用于字段归一化或默认值补全，允许为 nil。
// param BeforeUnique: 在唯一性校验前执行的钩子（如密码加密、缓存预热），允许为 nil。
// param EnsureUnique: 执行唯一性校验并返回预检结果；若返回错误，流程立即终止。
// param ResolveExistence: 在唯一性通过后确认实体是否已存在（如数据库查重），允许为 nil。
// param HandleExisting: 处理已存在实体的回调，通常用于返回 ErrAlreadyExist。
// param MarkPending: 写入 pending 标记的钩子，需保证与 Kafka 消费侧契合。
// param AfterPending: pending 成功后的补充操作，如设置 Trace 标签，允许为 nil。
// param SendCreateMessage: 将创建事件发送至消息系统。
// param AfterSuccess: 全部步骤成功后执行的回调，允许为 nil。
// param OnError: 出现错误时的兜底回调，允许为 nil。
//
// note: 钩子实现需具备幂等性，以便重试或上层容错逻辑调用。
type PipelineConfig[T any] struct {
	Name              string
	Begin             func(ctx context.Context, entity T) (context.Context, func(error))
	Normalize         func(entity T)
	BeforeUnique      func(ctx context.Context, entity T) error
	EnsureUnique      func(ctx context.Context, entity T) (PreflightResult[T], error)
	ResolveExistence  func(ctx context.Context, entity T, preflight PreflightResult[T]) (T, error)
	HandleExisting    func(entity T, existing T) error
	MarkPending       func(ctx context.Context, entity T) (PendingResult, error)
	AfterPending      func(ctx context.Context, entity T, pending PendingResult)
	SendCreateMessage func(ctx context.Context, entity T) error
	AfterSuccess      func(ctx context.Context, entity T) error
	OnError           func(ctx context.Context, entity T, err error)
}

// Pipeline 根据配置执行标准化的创建流程。
//
// note: Pipeline 无状态，可在多 goroutine 中复用。
type Pipeline[T any] struct {
	cfg PipelineConfig[T]
}

// NewPipeline 构建通用创建流程实例。
//
// param cfg: 流程配置，除 EnsureUnique、SendCreateMessage 外均可选。
//
// returns: 返回可复用的 Pipeline 指针。
func NewPipeline[T any](cfg PipelineConfig[T]) *Pipeline[T] {
	return &Pipeline[T]{cfg: cfg}
}

// Execute 执行创建流程。
//
// param ctx: 上下文对象，需传入请求链路的上下文。
// param entity: 待创建的实体；若为指针类型，需确保在钩子中原地修改。
//
// returns: 成功返回 nil，失败返回具体错误。
func (p *Pipeline[T]) Execute(ctx context.Context, entity T) (err error) {
	if p == nil {
		return nil
	}
	if p.cfg.EnsureUnique == nil {
		return nil
	}
	if p.cfg.SendCreateMessage == nil {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	var end func(error)
	if p.cfg.Begin != nil {
		ctx, end = p.cfg.Begin(ctx, entity)
	}
	if end != nil {
		defer end(err)
	}

	defer func() {
		if err != nil && p.cfg.OnError != nil {
			p.cfg.OnError(ctx, entity, err)
		}
	}()

	if p.cfg.Normalize != nil {
		p.cfg.Normalize(entity)
	}

	//密码判断和联系人预热
	if p.cfg.BeforeUnique != nil {
		if err = p.cfg.BeforeUnique(ctx, entity); err != nil {
			return err
		}
	}

	//联系方式唯一性检查并包装返回值
	preflight, err := p.cfg.EnsureUnique(ctx, entity)
	if err != nil {
		return err
	}

	var existing T
	if p.cfg.ResolveExistence != nil {
		existing, err = p.cfg.ResolveExistence(ctx, entity, preflight)
		if err != nil {
			return err
		}
	}

	if p.cfg.HandleExisting != nil {
		if err = p.cfg.HandleExisting(entity, existing); err != nil {
			return err
		}
	}

	var pending PendingResult
	if p.cfg.MarkPending != nil {
		pending, err = p.cfg.MarkPending(ctx, entity)
		if err != nil {
			return err
		}
		if p.cfg.AfterPending != nil {
			p.cfg.AfterPending(ctx, entity, pending)
		}
	}

	if err = p.cfg.SendCreateMessage(ctx, entity); err != nil {
		return err
	}

	if p.cfg.AfterSuccess != nil {
		if err = p.cfg.AfterSuccess(ctx, entity); err != nil {
			return err
		}
	}

	return nil
}
