package server

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	createproducer "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/producer/create"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware/common"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/ratelimiter"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/segmentio/kafka-go"
)

var _ producer.MessageProducer[*v1.User, string] = (*UserProducer)(nil)

type UserProducer struct {
	producer       sarama.AsyncProducer
	kafkaOptions   *options.KafkaOptions
	wg             sync.WaitGroup
	shutdown       chan struct{}
	limiter        *ratelimiter.RateLimiterController
	fallbackDir    string // 新增：降级文件目录
	createPipeline *createproducer.Pipeline[*v1.User]
}

type fallbackMessage struct {
	Topic     string           `json:"topic"`
	Key       string           `json:"key,omitempty"`
	Value     string           `json:"value"`
	Timestamp string           `json:"timestamp"`
	Attempts  int              `json:"attempts"`
	Headers   []fallbackHeader `json:"headers,omitempty"`
}

type fallbackHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type producerSpanKey struct{}

type producerMetadata struct {
	topic         string
	operation     string
	traceID       string
	enqueueStart  time.Time
	enqueueFinish time.Time
	ackAt         time.Time
	attempt       int
	extendedWait  bool
	fallback      bool
	parentSpanID  string
}

func (m *producerMetadata) markEnqueued() {
	if m == nil {
		return
	}
	if m.enqueueStart.IsZero() {
		now := time.Now()
		m.enqueueStart = now
		if m.attempt == 0 {
			m.attempt = 1
		}
		m.extendedWait = false
		m.fallback = false
		return
	}
	m.attempt++
	m.extendedWait = true
}

func attachProducerMetadata(msg *sarama.ProducerMessage, topic, operation, traceID string) *producerMetadata {
	if msg == nil {
		return nil
	}
	if topic == "" {
		topic = "unknown"
	}
	if operation == "" {
		operation = "unknown"
	}
	meta := &producerMetadata{
		topic:     topic,
		operation: operation,
		traceID:   traceID,
	}
	msg.Metadata = meta
	return meta
}

func buildOperationHeaders(operation, channel string, ts time.Time, retryCount int) []sarama.RecordHeader {
	if ts.IsZero() {
		ts = time.Now()
	}
	retryValue := strconv.Itoa(retryCount)
	return []sarama.RecordHeader{
		{Key: []byte(HeaderOperation), Value: []byte(operation)},
		{Key: []byte(HeaderChannel), Value: []byte(channel)},
		{Key: []byte(HeaderOriginalTimestamp), Value: []byte(ts.Format(time.RFC3339))},
		{Key: []byte(HeaderRetryCount), Value: []byte(retryValue)},
	}
}

func resolveChannelFromTopic(topic string) string {
	switch topic {
	case UserOperationTopic:
		return ChannelPrimary
	case UserOperationRetryTopic:
		return ChannelRetry
	case UserOperationCompTopic:
		return ChannelCompensation
	default:
		return ""
	}
}

func (p *UserProducer) recordDeliveryMetrics(msg *sarama.ProducerMessage, err error) {
	if msg == nil {
		return
	}
	meta, ok := msg.Metadata.(*producerMetadata)
	if !ok || meta == nil {
		return
	}
	if meta.enqueueStart.IsZero() {
		meta.enqueueStart = time.Now()
	}
	if meta.enqueueFinish.IsZero() {
		meta.enqueueFinish = meta.enqueueStart
	}
	meta.ackAt = time.Now()

	enqueueWait := meta.enqueueFinish.Sub(meta.enqueueStart)
	if enqueueWait < 0 {
		enqueueWait = 0
	}
	brokerAck := meta.ackAt.Sub(meta.enqueueFinish)
	if brokerAck < 0 {
		brokerAck = 0
	}
	totalDelivery := meta.ackAt.Sub(meta.enqueueStart)
	if totalDelivery < 0 {
		totalDelivery = 0
	}

	metrics.RecordKafkaProducerDelivery(meta.topic, meta.operation, totalDelivery, err)
	metrics.RecordKafkaProducerEnqueueWait(meta.topic, meta.operation, enqueueWait, err)
	metrics.RecordKafkaProducerBrokerAck(meta.topic, meta.operation, brokerAck, err)
	p.emitProducerDeliveryTrace(meta, msg, enqueueWait, brokerAck, totalDelivery, err)
	msg.Metadata = nil
}

func (p *UserProducer) emitProducerDeliveryTrace(meta *producerMetadata, msg *sarama.ProducerMessage, enqueueWait, brokerAck, total time.Duration, sendErr error) {
	if meta == nil {
		return
	}
	traceID := meta.traceID
	if traceID == "" && msg != nil {
		traceID = p.getTraceIDFromHeaders(msg.Headers)
	}
	if traceID == "" {
		return
	}

	operationLabel := meta.operation
	if operationLabel == "" {
		operationLabel = "unknown"
	}

	spanOperation := fmt.Sprintf("broker_ack_%s", operationLabel)
	nowTs := meta.enqueueFinish
	if nowTs.IsZero() {
		nowTs = meta.ackAt
	}
	if nowTs.IsZero() {
		nowTs = time.Now()
	}
	_, ctx := trace.NewDetached(trace.Options{
		TraceID:         traceID,
		Service:         "iam-apiserver",
		Component:       "kafka-producer",
		Operation:       spanOperation,
		Phase:           trace.PhaseAsync,
		Method:          "KAFKA",
		Path:            meta.topic,
		Now:             nowTs,
		DisableLogging:  true,
		ForceLogOnError: true,
	})

	trace.AddRequestTag(ctx, "topic", meta.topic)
	trace.AddRequestTag(ctx, "operation", operationLabel)
	if channel := resolveChannelFromTopic(meta.topic); channel != "" {
		trace.AddRequestTag(ctx, "channel", channel)
	}
	trace.AddRequestTag(ctx, "delivery_latency_ms", float64(total.Milliseconds()))
	trace.AddRequestTag(ctx, "enqueue_wait_ms", float64(enqueueWait.Milliseconds()))
	trace.AddRequestTag(ctx, "broker_ack_ms", float64(brokerAck.Milliseconds()))
	trace.AddRequestTag(ctx, "attempt", meta.attempt)
	trace.AddRequestTag(ctx, "extended_wait", meta.extendedWait)
	trace.AddRequestTag(ctx, "fallback", meta.fallback)
	if msg != nil {
		if msg.Partition >= 0 {
			trace.AddRequestTag(ctx, "partition", msg.Partition)
		}
		if msg.Offset >= 0 {
			trace.AddRequestTag(ctx, "offset", msg.Offset)
		}
		if msg.Key != nil {
			if encodedKey, err := msg.Key.Encode(); err == nil {
				trace.AddRequestTag(ctx, "message_key", string(encodedKey))
			}
		}
	}

	spanCtx, span := trace.StartSpanWithParent(ctx, "kafka-producer", "broker_ack", meta.parentSpanID)
	if span != nil {
		if !meta.enqueueFinish.IsZero() {
			span.StartTime = meta.enqueueFinish
		} else if span.StartTime.IsZero() {
			span.StartTime = nowTs
		}
	}
	if sendErr == nil {
		trace.ExpectAsync(ctx, time.Now().Add(5*time.Second))
	}
	details := map[string]interface{}{
		"topic":             meta.topic,
		"operation":         operationLabel,
		"channel":           resolveChannelFromTopic(meta.topic),
		"latency_ms":        float64(total) / float64(time.Millisecond),
		"enqueue_wait_ms":   float64(enqueueWait) / float64(time.Millisecond),
		"broker_ack_ms":     float64(brokerAck) / float64(time.Millisecond),
		"total_delivery_ms": float64(total) / float64(time.Millisecond),
		"attempt":           meta.attempt,
		"extended_wait":     meta.extendedWait,
		"fallback":          meta.fallback,
		"parent_span_id":    meta.parentSpanID,
	}
	if msg != nil {
		if msg.Partition >= 0 {
			details["partition"] = msg.Partition
		}
		if msg.Offset >= 0 {
			details["offset"] = msg.Offset
		}
		if msg.Key != nil {
			if encodedKey, err := msg.Key.Encode(); err == nil {
				details["message_key"] = string(encodedKey)
			}
		}
	}

	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	message := "broker acknowledged"
	if sendErr != nil {
		status = "error"
		message = sendErr.Error()
		details["error"] = sendErr.Error()
		if c := errors.GetCode(sendErr); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrKafkaFailed)
		}
	}

	trace.EndSpanAt(span, meta.ackAt, status, codeStr, details)
	trace.RecordOutcome(spanCtx, codeStr, message, status, 0)
	trace.Complete(spanCtx)
}

const defaultProducerEnqueueTimeout = 500 * time.Millisecond

const (
	// 放宽异步生产者入队重试的等待范围，避免短时积压直接落盘降级
	extendedEnqueueTimeoutMultiplier = 3
	minExtendedEnqueueTimeout        = 8 * time.Second
	maxExtendedEnqueueTimeout        = 30 * time.Second
)

var errProducerEnqueueTimeout = stderrors.New("producer enqueue timeout")

func injectTraceHeader(ctx context.Context, msg *sarama.ProducerMessage) {
	if msg == nil {
		return
	}
	traceID := strings.TrimSpace(trace.TraceIDFromContext(ctx))
	requestID := strings.TrimSpace(common.GetRequestID(ctx))

	if traceID == "" || traceID == "unknown" {
		traceID = ""
	}
	if traceID == "" {
		if requestID != "" && requestID != "unknown" {
			traceID = requestID
		}
	}
	if traceID == "" {
		traceID = fmt.Sprintf("generated-%d", time.Now().UnixNano())
		trace.AddRequestTag(ctx, "trace_id_generated", true)
	}
	if requestID == "" || requestID == "unknown" {
		requestID = traceID
	}

	setOrUpdate := func(key, val string) {
		val = strings.TrimSpace(val)
		if val == "" {
			return
		}
		for i := range msg.Headers {
			if string(msg.Headers[i].Key) == key {
				msg.Headers[i].Value = []byte(val)
				return
			}
		}
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(val)})
	}

	setOrUpdate(HeaderTraceID, traceID)
	setOrUpdate(HeaderRequestID, requestID)
}

func (p *UserProducer) getEnqueueTimeout() time.Duration {
	if p != nil && p.kafkaOptions != nil && p.kafkaOptions.ProducerEnqueueTimeout > 0 {
		return p.kafkaOptions.ProducerEnqueueTimeout
	}
	return defaultProducerEnqueueTimeout
}

func (p *UserProducer) enqueueWithTimeout(ctx context.Context, msg *sarama.ProducerMessage, wait time.Duration) error {
	if p == nil || p.producer == nil {
		return fmt.Errorf("producer unavailable")
	}
	timeout := wait
	if timeout <= 0 {
		timeout = p.getEnqueueTimeout()
	}
	if timeout <= 0 {
		timeout = defaultProducerEnqueueTimeout
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-p.shutdown:
		return fmt.Errorf("producer shutting down")
	case p.producer.Input() <- msg:
		if meta, ok := msg.Metadata.(*producerMetadata); ok && meta != nil {
			meta.enqueueFinish = time.Now()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errProducerEnqueueTimeout
	}
}

func (p *UserProducer) enqueueOrFallback(ctx context.Context, msg *sarama.ProducerMessage, detail string) error {
	timeouts := []time.Duration{0}
	if extended := p.extendedEnqueueTimeout(); extended > 0 {
		timeouts = append(timeouts, extended)
	}

	var (
		err           error
		attemptsTried int
		lastTimeout   time.Duration
	)

	for idx, wait := range timeouts {
		attemptsTried = idx + 1
		actualTimeout := wait
		if actualTimeout <= 0 {
			actualTimeout = p.getEnqueueTimeout()
		}
		lastTimeout = actualTimeout

		if meta, ok := msg.Metadata.(*producerMetadata); ok && meta != nil {
			if meta.attempt < attemptsTried {
				meta.attempt = attemptsTried
			}
			if idx > 0 {
				meta.extendedWait = true
			}
		}

		err = p.enqueueWithTimeout(ctx, msg, wait)
		if err == nil {
			if idx > 0 {
				log.Infof("Enqueued %s after extended wait %s (attempt %d)", detail, actualTimeout, attemptsTried)
			}
			trace.AddRequestTag(ctx, "retry_attempt", attemptsTried)
			trace.AddRequestTag(ctx, "max_retry", len(timeouts))
			return nil
		}

		if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
			return errors.WithCode(code.ErrKafkaFailed, "context cancelled while enqueuing %s: %v", detail, err)
		}

		if err == errProducerEnqueueTimeout && idx+1 < len(timeouts) {
			log.Warnf("Failed to enqueue %s within %s (attempt %d/%d); retrying with extended timeout %s", detail, actualTimeout, attemptsTried, len(timeouts), timeouts[idx+1])
			if meta, ok := msg.Metadata.(*producerMetadata); ok {
				meta.markEnqueued()
			}
			continue
		}

		// 其他错误或已没有更多重试，跳出进入降级逻辑
		break
	}

	trace.AddRequestTag(ctx, "async_forward_to", "fallback_storage")
	trace.AddRequestTag(ctx, "retry_attempt", attemptsTried)
	trace.AddRequestTag(ctx, "max_retry", len(timeouts))
	if err == errProducerEnqueueTimeout {
		log.Errorf("Failed to enqueue %s within %s after %d attempts. Triggering fallback.", detail, lastTimeout, attemptsTried)
		p.writeToFallbackFile(msg)
		trace.AddRequestTag(ctx, "dlq_produced", true)
		return errors.WithCode(code.ErrKafkaFailed, "producer enqueue timeout after %s, message written to fallback", lastTimeout)
	}

	log.Errorf("Failed to enqueue %s after %d attempts: %v. Triggering fallback.", detail, attemptsTried, err)
	p.writeToFallbackFile(msg)
	trace.AddRequestTag(ctx, "dlq_produced", true)
	return errors.WithCode(code.ErrKafkaFailed, "producer enqueue failed (%v), message written to fallback", err)
}

func (p *UserProducer) extendedEnqueueTimeout() time.Duration {
	base := p.getEnqueueTimeout()
	if base <= 0 {
		base = defaultProducerEnqueueTimeout
	}

	extended := time.Duration(extendedEnqueueTimeoutMultiplier) * base
	if extended < minExtendedEnqueueTimeout {
		extended = minExtendedEnqueueTimeout
	}
	if extended > maxExtendedEnqueueTimeout {
		extended = maxExtendedEnqueueTimeout
	}
	if extended <= base {
		return 0
	}
	return extended
}

func NewUserProducer(
	options *options.KafkaOptions,
	limiter *ratelimiter.RateLimiterController,
	fallbackDir string,
) (*UserProducer, error) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.RequiredAcks(options.RequiredAcks)

	compressionCodec, err := parseCompressionCodec(options.ProducerCompression)
	if err != nil {
		return nil, fmt.Errorf("invalid compression codec: %w", err)
	}
	config.Producer.Compression = compressionCodec

	config.Producer.Flush.Frequency = options.FlushFrequency
	config.Producer.Flush.MaxMessages = options.FlushMaxMessages
	config.Producer.Return.Successes = options.ProducerReturnSuccesses
	config.Producer.Return.Errors = options.ProducerReturnErrors

	if options.ChannelBufferSize > 0 {
		config.ChannelBufferSize = options.ChannelBufferSize
	}

	producer, err := sarama.NewAsyncProducer(options.Brokers, config)
	if err != nil {
		log.Errorf("Failed to create Sarama async producer: %v", err)
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	up := &UserProducer{
		producer:     producer,
		kafkaOptions: options,
		shutdown:     make(chan struct{}),
		limiter:      limiter,
		fallbackDir:  fallbackDir, // 保存降级目录
	}

	up.wg.Add(2)
	go up.handleSuccesses()
	go up.handleErrors()

	if fallbackDir != "" && options.FallbackRetryEnabled {
		up.wg.Add(1)
		go up.runFallbackCompensator()
	}

	up.initCreatePipeline()

	return up, nil
}

func (p *UserProducer) handleSuccesses() {
	defer p.wg.Done()
	successes := p.producer.Successes()
	for {
		select {
		case success, ok := <-successes:
			if !ok {
				return
			}
			if success == nil {
				continue
			}
			p.recordDeliveryMetrics(success, nil)
			log.Debugf("Message sent successfully to topic %s, partition %d, offset %d", success.Topic, success.Partition, success.Offset)
		case <-p.shutdown:
			return
		}
	}
}

func (p *UserProducer) handleErrors() {
	defer p.wg.Done()
	errs := p.producer.Errors()
	for {
		select {
		case errMsg, ok := <-errs:
			if !ok {
				return
			}
			if errMsg != nil {
				p.recordDeliveryMetrics(errMsg.Msg, errMsg.Err)
				log.Errorf("Failed to send message: %v", errMsg.Err)
				p.writeToFallbackFile(errMsg.Msg) // 写入到降级文件
			}
		case <-p.shutdown:
			return
		}
	}
}

// SendCreateMessage 将用户创建事件发送至 Kafka
//
// 封装用户实体并投递到创建主题，附带链路追踪与限流控制，失败时会按配置走降级写文件。
//
// 参数：
//
//	ctx: 请求上下文，携带 trace 与取消信号
//	user: 待发送的用户实体，需包含用户名等基础字段
//
// 返回值：
//
//	error: 发送失败时返回具体错误，nil 表示成功入队
//
// 示例：
//
//	err := producer.SendCreateMessage(ctx, user)
//	if err != nil {
//	    // 处理发送异常
//	}
//
// 注意事项：
//   - 会尊重生产者限流器，可能阻塞等待
//   - 失败时会记录指标并可能落盘到降级文件
//
// 异常情况：
//   - 序列化或发送失败返回对应错误码
//   - 限流被拒绝时返回 ErrRateLimitExceeded
func (p *UserProducer) SendCreateMessage(ctx context.Context, user *v1.User) error {
	if p == nil {
		return nil
	}
	if p.createPipeline == nil {
		p.initCreatePipeline()
	}
	if p.createPipeline == nil {
		return errors.WithCode(code.ErrServerBusy, "create message pipeline 未初始化")
	}
	return p.createPipeline.Execute(ctx, user)
}

func (p *UserProducer) initCreatePipeline() {
	if p == nil || p.createPipeline != nil {
		return
	}

	cfg := createproducer.PipelineConfig[*v1.User]{
		Name:      "user-create",
		Operation: OperationCreate,
		Topic:     UserOperationTopic,
		Begin: func(ctx context.Context, user *v1.User, operation, topic string) (context.Context, func(error)) {
			spanCtx, span := trace.StartSpan(ctx, "kafka-producer", fmt.Sprintf("send_%s", operation))
			if spanCtx != nil {
				ctx = spanCtx
			}
			if user != nil {
				trace.AddRequestTag(ctx, "username", user.Name)
			}
			trace.AddRequestTag(ctx, "topic", topic)
			trace.AddRequestTag(ctx, "operation", operation)
			ctx = context.WithValue(ctx, producerSpanKey{}, span)
			start := time.Now()
			return ctx, func(err error) {
				status := "success"
				codeStr := strconv.Itoa(code.ErrSuccess)
				if err != nil {
					status = "error"
					if c := errors.GetCode(err); c != 0 {
						codeStr = strconv.Itoa(c)
					} else {
						codeStr = strconv.Itoa(code.ErrUnknown)
					}
				}
				username := ""
				if user != nil {
					username = user.Name
				}
				if span != nil {
					trace.EndSpan(span, status, codeStr, map[string]interface{}{
						"username":  username,
						"topic":     topic,
						"operation": operation,
					})
				}
				metrics.RecordKafkaProducerOperation(topic, operation, time.Since(start).Seconds(), err, false)
			}
		},
		Wait: func(ctx context.Context) error {
			if p.limiter == nil {
				return nil
			}
			if err := p.limiter.Wait(ctx); err != nil {
				return errors.WithCode(code.ErrRateLimitExceeded, "producer rate limit exceeded: %v", err)
			}
			return nil
		},
		Marshal: func(user *v1.User) ([]byte, error) {
			data, err := jsonCodec.Marshal(user)
			if err != nil {
				log.Errorf("Failed to marshal user %s for topic %s, operation %s: %v", user.Name, UserOperationTopic, OperationCreate, err)
				return nil, errors.WithCode(code.ErrEncodingJSON, "failed to marshal user message: %v", err)
			}
			return data, nil
		},
		LogPayload: func(user *v1.User, payload []byte) {
			log.Debugw("User message payload", "operation", OperationCreate, "topic", UserOperationTopic, "username", user.Name, "payload", string(payload))
			if strings.HasPrefix(user.Name, "lock_case_") {
				log.Infow("[lock-debug-producer]", "operation", OperationCreate, "topic", UserOperationTopic, "username", user.Name, "payload", string(payload))
				appendLockDebug(fmt.Sprintf("producer|op=%s|user=%s|payload=%s", OperationCreate, user.Name, string(payload)))
			}
		},
		Key: func(user *v1.User) string {
			key := strings.TrimSpace(user.Name)
			if key == "" {
				key = strconv.FormatUint(user.ID, 10)
			}
			return key
		},
		Headers: func(_ *v1.User, ts time.Time) []sarama.RecordHeader {
			return buildOperationHeaders(OperationCreate, ChannelPrimary, ts, 0)
		},
		Attach: func(ctx context.Context, msg *sarama.ProducerMessage) error {
			injectTraceHeader(ctx, msg)
			meta := attachProducerMetadata(msg, msg.Topic, OperationCreate, trace.TraceIDFromContext(ctx))
			if meta != nil {
				if span, ok := ctx.Value(producerSpanKey{}).(*trace.Span); ok && span != nil {
					meta.parentSpanID = span.ID
					msg.Headers = p.updateOrAddHeader(msg.Headers, HeaderParentSpanID, span.ID)
				}
				meta.markEnqueued()
			}
			return nil
		},
		Description: func(user *v1.User) string {
			return fmt.Sprintf("user message operation=%s topic=%s username=%s", OperationCreate, UserOperationTopic, user.Name)
		},
		Enqueue: func(ctx context.Context, msg *sarama.ProducerMessage, detail string) error {
			if detail == "" {
				detail = fmt.Sprintf("user message operation=%s topic=%s", OperationCreate, UserOperationTopic)
			}
			return p.enqueueOrFallback(ctx, msg, detail)
		},
	}

	p.createPipeline = createproducer.NewPipeline[*v1.User](cfg)
}

func (p *UserProducer) SendUpdateMessage(ctx context.Context, user *v1.User) error {
	trace.AddRequestTag(ctx, "username", user.Name)
	log.Debugf("[Producer] SendUpdateMessage: username=%s", user.Name)
	return p.sendUserMessage(ctx, user, OperationUpdate)
}

func (p *UserProducer) SendDeleteMessage(ctx context.Context, username string) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-producer", "send_delete")
	if spanCtx != nil {
		ctx = spanCtx
	}
	if span != nil {
		ctx = context.WithValue(ctx, producerSpanKey{}, span)
	}
	trace.AddRequestTag(ctx, "username", username)
	log.Debugf("[Producer] SendDeleteMessage: username=%s", username)
	deleteData := map[string]interface{}{
		"username":   username,
		"deleted_at": time.Now().Format(time.RFC3339),
	}

	data, err := jsonCodec.Marshal(deleteData)
	if err != nil {
		trace.EndSpan(span, "error", strconv.Itoa(code.ErrEncodingJSON), map[string]interface{}{
			"username": username,
			"error":    err.Error(),
		})
		return errors.WithCode(code.ErrEncodingJSON, "failed to marshal delete message: %v", err)
	}

	now := time.Now()
	msg := &sarama.ProducerMessage{
		Topic:   UserOperationTopic,
		Key:     sarama.StringEncoder(username),
		Value:   sarama.ByteEncoder(data),
		Headers: buildOperationHeaders(OperationDelete, ChannelPrimary, now, 0),
	}

	injectTraceHeader(ctx, msg)
	meta := attachProducerMetadata(msg, msg.Topic, OperationDelete, trace.TraceIDFromContext(ctx))
	if meta != nil {
		if span != nil {
			meta.parentSpanID = span.ID
			msg.Headers = p.updateOrAddHeader(msg.Headers, HeaderParentSpanID, span.ID)
		}
		meta.markEnqueued()
	}
	errSend := p.enqueueOrFallback(ctx, msg, fmt.Sprintf("delete message username=%s topic=%s", username, msg.Topic))
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	if errSend != nil {
		status = "error"
		if c := errors.GetCode(errSend); c != 0 {
			codeStr = strconv.Itoa(c)
		} else {
			codeStr = strconv.Itoa(code.ErrUnknown)
		}
	}
	trace.EndSpan(span, status, codeStr, map[string]interface{}{
		"username": username,
		"topic":    msg.Topic,
	})
	if errSend != nil {
		return errSend
	}
	return nil
}

// 新增：写入降级文件的方法
func (p *UserProducer) writeToFallbackFile(msg *sarama.ProducerMessage) {
	if meta, ok := msg.Metadata.(*producerMetadata); ok && meta != nil {
		meta.fallback = true
	}
	if p.fallbackDir == "" {
		log.Warnf("Fallback directory not configured. Message lost: key=%s", msg.Key)
		return
	}

	// 确保目录存在
	if err := os.MkdirAll(p.fallbackDir, 0755); err != nil {
		log.Errorf("Failed to create fallback directory %s: %v", p.fallbackDir, err)
		return
	}

	// 按天创建文件名
	fileName := fmt.Sprintf("%s.json", time.Now().Format("2006-01-02"))
	filePath := filepath.Join(p.fallbackDir, fileName)

	// 构造要写入的 JSON 对象
	value, _ := msg.Value.Encode()

	var key string
	if msg.Key != nil {
		encodedKey, _ := msg.Key.Encode()
		key = string(encodedKey)
	}

	entry := fallbackMessage{
		Topic:     msg.Topic,
		Key:       key,
		Value:     string(value),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Attempts:  0,
	}

	if len(msg.Headers) > 0 {
		entry.Headers = make([]fallbackHeader, 0, len(msg.Headers))
		for _, header := range msg.Headers {
			entry.Headers = append(entry.Headers, fallbackHeader{
				Key:   string(header.Key),
				Value: string(header.Value),
			})
		}
	}

	// 序列化为 JSON
	jsonData, err := jsonCodec.Marshal(entry)
	if err != nil {
		log.Errorf("Failed to marshal fallback message to JSON: %v", err)
		return
	}

	// 以追加模式打开文件
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open fallback file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	// 写入 JSON 数据，并在线末添加换行符
	if _, err := file.Write(append(jsonData, '\n')); err != nil {
		log.Errorf("Failed to write to fallback file %s: %v", filePath, err)
	}
}

func (p *UserProducer) sendUserMessage(ctx context.Context, user *v1.User, operation string) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-producer", fmt.Sprintf("send_%s", operation))
	if spanCtx != nil {
		ctx = spanCtx
	}
	if span != nil {
		ctx = context.WithValue(ctx, producerSpanKey{}, span)
	}

	topic := UserOperationTopic
	trace.AddRequestTag(ctx, "topic", topic)
	trace.AddRequestTag(ctx, "operation", operation)
	trace.AddRequestTag(ctx, "username", user.Name)

	if p.limiter != nil {
		if err := p.limiter.Wait(ctx); err != nil {
			trace.EndSpan(span, "error", strconv.Itoa(code.ErrRateLimitExceeded), map[string]interface{}{
				"error": err.Error(),
			})
			return errors.WithCode(code.ErrRateLimitExceeded, "producer rate limit exceeded: %v", err)
		}
	}

	start := time.Now()
	var errSend error
	defer func() {
		metrics.RecordKafkaProducerOperation(topic, operation, time.Since(start).Seconds(), errSend, false)
		status := "success"
		codeStr := strconv.Itoa(code.ErrSuccess)
		if errSend != nil {
			status = "error"
			if c := errors.GetCode(errSend); c != 0 {
				codeStr = strconv.Itoa(c)
			} else {
				codeStr = strconv.Itoa(code.ErrUnknown)
			}
		}
		trace.EndSpan(span, status, codeStr, map[string]interface{}{
			"username":  user.Name,
			"topic":     topic,
			"operation": operation,
		})
	}()

	userData, err := jsonCodec.Marshal(user)
	if err != nil {
		errSend = err
		log.Errorf("Failed to marshal user %s for topic %s, operation %s: %v", user.Name, topic, operation, err)
		return errors.WithCode(code.ErrEncodingJSON, "failed to marshal user message: %v", err)
	}
	log.Debugw("User message payload", "operation", operation, "topic", topic, "username", user.Name, "payload", string(userData))
	if strings.HasPrefix(user.Name, "lock_case_") {
		log.Infow("[lock-debug-producer]", "operation", operation, "topic", topic, "username", user.Name, "payload", string(userData))
		appendLockDebug(fmt.Sprintf("producer|op=%s|user=%s|payload=%s", operation, user.Name, string(userData)))
	}

	now := time.Now()
	key := strings.TrimSpace(user.Name)
	if key == "" {
		key = strconv.FormatUint(user.ID, 10)
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(userData),
		Headers: buildOperationHeaders(operation, ChannelPrimary, now, 0),
	}

	injectTraceHeader(ctx, msg)
	meta := attachProducerMetadata(msg, msg.Topic, operation, trace.TraceIDFromContext(ctx))
	if meta != nil {
		if span != nil {
			meta.parentSpanID = span.ID
			msg.Headers = p.updateOrAddHeader(msg.Headers, HeaderParentSpanID, span.ID)
		}
		meta.markEnqueued()
	}
	if err := p.enqueueOrFallback(ctx, msg, fmt.Sprintf("user message operation=%s topic=%s username=%s", operation, topic, user.Name)); err != nil {
		errSend = err
		return err
	}
	return nil
}

// sendToRetryTopic is called by the consumer to send a message to the retry topic.
// It needs to accept a kafka-go message and convert it to a sarama message.
func (p *UserProducer) sendToRetryTopic(ctx context.Context, msg kafka.Message, errorInfo string) error {
	log.Warnf("[Producer] Forwarding to retry topic: key=%s, error=%s", string(msg.Key), errorInfo)

	// Convert kafka.Message to sarama.ProducerMessage
	saramaMsg := &sarama.ProducerMessage{
		Topic: UserOperationRetryTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}

	// Copy and update headers
	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+1)
	for _, h := range msg.Headers {
		headers = append(headers, sarama.RecordHeader{Key: []byte(h.Key), Value: h.Value})
	}
	headers = p.updateOrAddHeader(headers, HeaderRetryError, errorInfo)
	currCount := 0
	for _, h := range headers {
		if strings.EqualFold(string(h.Key), HeaderRetryCount) {
			if v, err := strconv.Atoi(string(h.Value)); err == nil {
				currCount = v
			}
			break
		}
	}
	retryAttempt := currCount + 1
	maxRetry := 0
	if p.kafkaOptions != nil {
		maxRetry = p.kafkaOptions.MaxRetries
	}
	trace.AddRequestTag(ctx, "retry_attempt", retryAttempt)
	trace.AddRequestTag(ctx, "max_retry", maxRetry)
	trace.AddRequestTag(ctx, "dlq_produced", false)
	headers = p.updateOrAddHeader(headers, HeaderChannel, ChannelRetry)
	headers = p.updateOrAddHeader(headers, HeaderRetryCount, strconv.Itoa(retryAttempt))
	saramaMsg.Headers = headers
	traceID := trace.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = p.getTraceIDFromHeaders(headers)
	}
	meta := attachProducerMetadata(saramaMsg, saramaMsg.Topic, p.getOperationFromHeaders(headers), traceID)
	if meta != nil {
		meta.markEnqueued()
	}

	// 使用 select 防止阻塞，并在无法立即发送时触发降级
	if err := p.enqueueWithTimeout(ctx, saramaMsg, 0); err != nil {
		if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("enqueue retry topic cancelled: %w", err)
		}
		if err == errProducerEnqueueTimeout {
			log.Errorf("Retry enqueue timeout for key=%s after %s. Triggering fallback.", string(msg.Key), p.getEnqueueTimeout())
		} else {
			log.Errorf("Failed to enqueue message to retry topic: key=%s error=%v. Triggering fallback.", string(msg.Key), err)
		}
		p.writeToFallbackFile(saramaMsg)
		return fmt.Errorf("enqueue retry topic failed: %w", err)
	}

	log.Debugf("Successfully enqueued message to retry topic for key: %s", string(msg.Key))
	return nil
}

// SendToDeadLetterTopic is called by the consumer to send a message to the dead-letter topic.
// It needs to accept a kafka-go message and convert it to a sarama message.
func (p *UserProducer) SendToDeadLetterTopic(ctx context.Context, msg kafka.Message, errorInfo string) error {
	log.Errorf("[Producer] Forwarding to dead-letter topic: key=%s, error=%s", string(msg.Key), errorInfo)
	retryAttempt := 0
	for _, h := range msg.Headers {
		if strings.EqualFold(string(h.Key), HeaderRetryCount) {
			if v, err := strconv.Atoi(string(h.Value)); err == nil {
				retryAttempt = v
			}
		}
	}
	maxRetry := 0
	if p.kafkaOptions != nil {
		maxRetry = p.kafkaOptions.MaxRetries
	}
	trace.AddRequestTag(ctx, "retry_attempt", retryAttempt)
	trace.AddRequestTag(ctx, "max_retry", maxRetry)
	trace.AddRequestTag(ctx, "dlq_produced", true)

	// Convert kafka.Message to sarama.ProducerMessage
	saramaMsg := &sarama.ProducerMessage{
		Topic: UserDeadLetterTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}

	// Copy and update headers
	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+2)
	for _, h := range msg.Headers {
		headers = append(headers, sarama.RecordHeader{Key: []byte(h.Key), Value: h.Value})
	}
	headers = p.updateOrAddHeader(headers, "deadletter-reason", errorInfo)
	headers = p.updateOrAddHeader(headers, "deadletter-timestamp", time.Now().Format(time.RFC3339))
	saramaMsg.Headers = headers
	traceID := trace.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = p.getTraceIDFromHeaders(headers)
	}
	meta := attachProducerMetadata(saramaMsg, saramaMsg.Topic, p.getOperationFromHeaders(headers), traceID)
	if meta != nil {
		meta.markEnqueued()
	}

	if err := p.enqueueWithTimeout(ctx, saramaMsg, 0); err != nil {
		if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("enqueue dead-letter topic cancelled: %w", err)
		}
		if err == errProducerEnqueueTimeout {
			log.Errorf("Dead-letter enqueue timeout for key=%s after %s. Triggering fallback.", string(msg.Key), p.getEnqueueTimeout())
		} else {
			log.Errorf("Failed to enqueue message to dead-letter topic: key=%s error=%v. Triggering fallback.", string(msg.Key), err)
		}
		p.writeToFallbackFile(saramaMsg)
		return fmt.Errorf("enqueue dead-letter topic failed: %w", err)
	}

	return nil
}

func (p *UserProducer) Close() error {

	close(p.shutdown) // Signal background goroutines to exit

	// Drain any remaining messages
	if p.producer != nil {
		// Note: AsyncClose does not block. The wg.Wait() below will ensure graceful shutdown.
		p.producer.AsyncClose()
	}

	p.wg.Wait() // Wait for goroutines to finish

	return nil
}

func (p *UserProducer) runFallbackCompensator() {
	defer p.wg.Done()
	logger := log.WithValues("component", "fallback-compensator")
	logger.Info("Compensator started")
	ticker := time.NewTicker(p.kafkaOptions.FallbackRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdown:
			logger.Info("Compensator shutting down")
			return
		case <-ticker.C:
			p.processFallbackFiles(logger)
		}
	}
}

func (p *UserProducer) processFallbackFiles(logger log.Logger) {
	if p.fallbackDir == "" {
		return
	}

	files, err := filepath.Glob(filepath.Join(p.fallbackDir, "*.json"))
	if err != nil {
		logger.Errorf("Failed to list fallback files: %v", err)
		return
	}

	if len(files) == 0 {
		return
	}

	sort.Strings(files)

	processed := 0
	maxBatch := p.kafkaOptions.FallbackRetryBatchSize

	for _, filePath := range files {
		if maxBatch > 0 && processed >= maxBatch {
			return
		}

		count, err := p.processFallbackFile(logger, filePath, maxBatch-processed)
		if err != nil {
			logger.Errorf("Failed to process fallback file %s: %v", filePath, err)
		}
		processed += count
	}
}

func (p *UserProducer) processFallbackFile(logger log.Logger, filePath string, remainingQuota int) (int, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	tempPath := filePath + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	defer tempFile.Close()
	writer := bufio.NewWriter(tempFile)
	defer writer.Flush()

	retryMax := p.kafkaOptions.FallbackRetryMaxAttempts
	processed := 0

	for scanner.Scan() {
		if remainingQuota > 0 && processed >= remainingQuota {
			// Copy remaining entries as-is
			if _, err := writer.Write(scanner.Bytes()); err != nil {
				return processed, err
			}
			if _, err := writer.WriteString("\n"); err != nil {
				return processed, err
			}
			continue
		}

		line := scanner.Bytes()
		var entry fallbackMessage
		if err := jsonCodec.Unmarshal(line, &entry); err != nil {
			logger.Errorf("Invalid fallback entry in %s: %v", filePath, err)
			continue
		}

		// Skip if attempts already exceed max retry limit
		if retryMax > 0 && entry.Attempts >= retryMax {
			logger.Warnf("Discarding fallback message after max attempts, topic=%s key=%s", entry.Topic, entry.Key)
			continue
		}

		if err := p.publishFallbackEntry(entry); err != nil {
			entry.Attempts++
			reEncoded, marshalErr := jsonCodec.Marshal(entry)
			if marshalErr != nil {
				logger.Errorf("Failed to re-marshal fallback entry: %v", marshalErr)
				continue
			}
			if _, err := writer.Write(reEncoded); err != nil {
				return processed, err
			}
			if _, err := writer.WriteString("\n"); err != nil {
				return processed, err
			}
			continue
		}

		processed++
	}

	if err := scanner.Err(); err != nil {
		return processed, err
	}

	// Replace original file with temp file
	if err := os.Rename(tempPath, filePath); err != nil {
		return processed, err
	}

	return processed, nil
}

func appendLockDebug(line string) {
	const debugFile = "/tmp/lock_debug.log"
	f, err := os.OpenFile(debugFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line + "\n")
}

func (p *UserProducer) publishFallbackEntry(entry fallbackMessage) error {
	msg := &sarama.ProducerMessage{
		Topic: entry.Topic,
		Value: sarama.ByteEncoder([]byte(entry.Value)),
	}

	if entry.Key != "" {
		msg.Key = sarama.StringEncoder(entry.Key)
	}

	headers := make([]sarama.RecordHeader, 0, len(entry.Headers))
	for _, header := range entry.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: []byte(header.Value),
		})
	}
	if channel := resolveChannelFromTopic(entry.Topic); channel != "" {
		headers = p.updateOrAddHeader(headers, HeaderChannel, channel)
	}
	headers = p.updateOrAddHeader(headers, HeaderRetryCount, strconv.Itoa(entry.Attempts))
	msg.Headers = headers
	meta := attachProducerMetadata(msg, msg.Topic, p.getOperationFromHeaders(headers), p.getTraceIDFromHeaders(headers))
	if meta != nil {
		meta.markEnqueued()
	}

	return p.enqueueWithTimeout(context.Background(), msg, 5*time.Second)
}

func (p *UserProducer) getOperationFromHeaders(headers []sarama.RecordHeader) string {
	for _, h := range headers {
		if string(h.Key) == HeaderOperation {
			return string(h.Value)
		}
	}
	return "unknown"
}

func (p *UserProducer) getTraceIDFromHeaders(headers []sarama.RecordHeader) string {
	for _, h := range headers {
		if strings.EqualFold(string(h.Key), HeaderTraceID) {
			return string(h.Value)
		}
	}
	return ""
}

func (p *UserProducer) updateOrAddHeader(headers []sarama.RecordHeader, key, value string) []sarama.RecordHeader {
	log.Debugf("[Producer] updateOrAddHeader: key=%s, value=%s", key, value)
	if key == "" {
		panic("kafka header key cannot be empty string")
	}

	targetKeyLower := strings.ToLower(key)
	var newHeaders []sarama.RecordHeader
	foundTargetHeader := false

	for _, header := range headers {
		currentHeaderKeyLower := strings.ToLower(string(header.Key))
		if currentHeaderKeyLower == targetKeyLower {
			// Update existing header
			newHeaders = append(newHeaders, sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			})
			foundTargetHeader = true
		} else {
			newHeaders = append(newHeaders, header)
		}
	}

	if !foundTargetHeader {
		newHeaders = append(newHeaders, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	return newHeaders
}

func parseCompressionCodec(codec string) (sarama.CompressionCodec, error) {
	switch strings.ToLower(codec) {
	case "", "none":
		return sarama.CompressionNone, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "zstd":
		return sarama.CompressionZSTD, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("unsupported compression codec %q", codec)
	}
}
