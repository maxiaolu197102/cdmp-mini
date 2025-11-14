package create

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

// PipelineConfig 定义通用创建消息生产流程的钩子集合。
//
// param Name: 管道名称，用于日志或调试。
// param Operation: 业务操作名称，将透传给指标或 Trace。
// param Topic: 固定的目标 Topic，当 TopicFunc 为空时使用。
// param TopicFunc: 动态计算目标 Topic 的函数，可覆盖 Topic。
// param Begin: 创建前置钩子，允许开启 Trace 或附加标签；返回新的 context 与收尾函数。
// param Wait: 在生成消息前执行的限流函数，允许为 nil。
// param Marshal: 将实体序列化为字节数组的函数，必填。
// param LogPayload: 序列化成功后用于记录 payload 的钩子，允许为 nil。
// param Key: 生成消息分区键的函数，允许为 nil。
// param Headers: 构造消息头的函数，允许为 nil。
// param BuildMessage: 自定义消息构造逻辑，返回 Sarama ProducerMessage，允许为 nil。
// param Attach: 在入队前对消息进行附加处理（如补充 Trace Header、元信息），允许为 nil。
// param Description: 生成入队描述信息的函数，用于日志或降级提示，允许为 nil。
// param Enqueue: 将消息写入 Kafka 的函数，必填。
// param WrapError: 对外返回前的错误包装函数，允许为 nil。
// param Metrics: 发送完成后的指标记录钩子，允许为 nil。
type PipelineConfig[T any] struct {
	Name         string
	Operation    string
	Topic        string
	TopicFunc    func(T) string
	Begin        func(context.Context, T, string, string) (context.Context, func(error))
	Wait         func(context.Context) error
	Marshal      func(T) ([]byte, error)
	LogPayload   func(T, []byte)
	Key          func(T) string
	Headers      func(T, time.Time) []sarama.RecordHeader
	BuildMessage func(T, []byte, time.Time) (*sarama.ProducerMessage, error)
	Attach       func(context.Context, *sarama.ProducerMessage) error
	Description  func(T) string
	Enqueue      func(context.Context, *sarama.ProducerMessage, string) error
	WrapError    func(error) error
	Metrics      func(context.Context, T, string, string, time.Duration, error)
}

// Pipeline 负责串联消息生产流程。
type Pipeline[T any] struct {
	cfg PipelineConfig[T]
}

// NewPipeline 创建通用创建消息生产管道。
func NewPipeline[T any](cfg PipelineConfig[T]) *Pipeline[T] {
	return &Pipeline[T]{cfg: cfg}
}

// Execute 执行消息生产流程。
//
// param ctx: 上下文，通常来自请求链路。
// param entity: 待发送的实体。
//
// returns: 若成功入队返回 nil，否则返回包装后的错误。
func (p *Pipeline[T]) Execute(ctx context.Context, entity T) error {
	if p == nil {
		return nil
	}
	if p.cfg.Marshal == nil || p.cfg.Enqueue == nil {
		return nil
	}

	operation := p.cfg.Operation
	topic := p.resolveTopic(entity)

	var execErr error
	if p.cfg.Begin != nil {
		beginCtx, end := p.cfg.Begin(ctx, entity, operation, topic)
		if beginCtx != nil {
			ctx = beginCtx
		}
		if end != nil {
			defer func() {
				end(execErr)
			}()
		}
	}

	if p.cfg.Wait != nil {
		if err := p.cfg.Wait(ctx); err != nil {
			execErr = p.wrapError(err)
			return execErr
		}
	}

	start := time.Now()

	payload, err := p.cfg.Marshal(entity)
	if err != nil {
		execErr = p.wrapError(err)
		return execErr
	}

	if p.cfg.LogPayload != nil {
		p.cfg.LogPayload(entity, payload)
	}

	now := time.Now()
	msg, err := p.buildMessage(entity, payload, now, topic)
	if err != nil {
		execErr = p.wrapError(err)
		return execErr
	}

	if p.cfg.Attach != nil {
		if err = p.cfg.Attach(ctx, msg); err != nil {
			execErr = p.wrapError(err)
			return execErr
		}
	}

	detail := ""
	if p.cfg.Description != nil {
		detail = p.cfg.Description(entity)
	}

	err = p.cfg.Enqueue(ctx, msg, detail)
	execErr = p.wrapError(err)

	if p.cfg.Metrics != nil {
		p.cfg.Metrics(ctx, entity, msg.Topic, operation, time.Since(start), execErr)
	}

	return execErr
}

func (p *Pipeline[T]) resolveTopic(entity T) string {
	if p.cfg.TopicFunc != nil {
		if topic := p.cfg.TopicFunc(entity); topic != "" {
			return topic
		}
	}
	return p.cfg.Topic
}

func (p *Pipeline[T]) buildMessage(entity T, payload []byte, ts time.Time, topic string) (*sarama.ProducerMessage, error) {
	if p.cfg.BuildMessage != nil {
		return p.cfg.BuildMessage(entity, payload, ts)
	}

	var keyEncoder sarama.Encoder
	if p.cfg.Key != nil {
		key := p.cfg.Key(entity)
		if key != "" {
			keyEncoder = sarama.StringEncoder(key)
		}
	}

	headers := []sarama.RecordHeader(nil)
	if p.cfg.Headers != nil {
		headers = p.cfg.Headers(entity, ts)
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     keyEncoder,
		Value:   sarama.ByteEncoder(payload),
		Headers: headers,
	}
	return msg, nil
}

func (p *Pipeline[T]) wrapError(err error) error {
	if err == nil || p.cfg.WrapError == nil {
		return err
	}
	return p.cfg.WrapError(err)
}
