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
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/ratelimiter"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/segmentio/kafka-go"
)

var _ producer.MessageProducer = (*UserProducer)(nil)

type UserProducer struct {
	producer     sarama.AsyncProducer
	kafkaOptions *options.KafkaOptions
	wg           sync.WaitGroup
	shutdown     chan struct{}
	limiter      *ratelimiter.RateLimiterController
	fallbackDir  string // 新增：降级文件目录
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

type producerMetadata struct {
	topic        string
	operation    string
	traceID      string
	enqueuedAt   time.Time
	parentSpanID string
}

func (m *producerMetadata) markEnqueued() {
	if m == nil {
		return
	}
	m.enqueuedAt = time.Now()
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
		topic:        topic,
		operation:    operation,
		traceID:      traceID,
		parentSpanID: "",
	}
	msg.Metadata = meta
	return meta
}

func (p *UserProducer) recordDeliveryMetrics(msg *sarama.ProducerMessage, err error) {
	if msg == nil {
		return
	}
	meta, ok := msg.Metadata.(*producerMetadata)
	if !ok || meta == nil {
		return
	}
	if meta.enqueuedAt.IsZero() {
		return
	}
	deliveryLatency := time.Since(meta.enqueuedAt)
	if deliveryLatency < 0 {
		deliveryLatency = 0
	}
	metrics.RecordKafkaProducerDelivery(meta.topic, meta.operation, deliveryLatency, err)
	p.emitProducerDeliveryTrace(meta, msg, deliveryLatency, err)
	msg.Metadata = nil
}

func (p *UserProducer) emitProducerDeliveryTrace(meta *producerMetadata, msg *sarama.ProducerMessage, latency time.Duration, sendErr error) {
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
	_, ctx := trace.NewDetached(trace.Options{
		TraceID:         traceID,
		Service:         "iam-apiserver",
		Component:       "kafka-producer",
		Operation:       spanOperation,
		Phase:           trace.PhaseAsync,
		Method:          "KAFKA",
		Path:            meta.topic,
		Now:             time.Now(),
		DisableLogging:  true,
		ForceLogOnError: true,
	})

	trace.AddRequestTag(ctx, "topic", meta.topic)
	trace.AddRequestTag(ctx, "operation", operationLabel)
	trace.AddRequestTag(ctx, "delivery_latency_ms", float64(latency.Milliseconds()))
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
	if sendErr == nil {
		// Ack succeeds before the consumer finishes, so keep the collector waiting for the downstream span.
		trace.ExpectAsync(ctx, time.Now().Add(5*time.Second))
	}
	details := map[string]interface{}{
		"topic":          meta.topic,
		"operation":      operationLabel,
		"latency_ms":     float64(latency) / float64(time.Millisecond),
		"parent_span_id": meta.parentSpanID,
	}
	if msg != nil {
		if msg.Partition >= 0 {
			details["partition"] = msg.Partition
		}
		if msg.Offset >= 0 {
			details["offset"] = msg.Offset
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

	trace.EndSpan(span, status, codeStr, details)
	trace.RecordOutcome(spanCtx, codeStr, message, status, 0)
	trace.Complete(spanCtx)
}

const defaultProducerEnqueueTimeout = 500 * time.Millisecond

var errProducerEnqueueTimeout = stderrors.New("producer enqueue timeout")

func injectTraceHeader(ctx context.Context, msg *sarama.ProducerMessage) {
	if msg == nil {
		return
	}
	traceID := trace.TraceIDFromContext(ctx)
	if traceID == "" {
		return
	}
	for i := range msg.Headers {
		if string(msg.Headers[i].Key) == HeaderTraceID {
			msg.Headers[i].Value = []byte(traceID)
			return
		}
	}
	msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(HeaderTraceID), Value: []byte(traceID)})
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errProducerEnqueueTimeout
	}
}

func (p *UserProducer) enqueueOrFallback(ctx context.Context, msg *sarama.ProducerMessage, detail string) error {
	err := p.enqueueWithTimeout(ctx, msg, 0)
	if err == nil {
		return nil
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return errors.WithCode(code.ErrKafkaFailed, "context cancelled while enqueuing %s: %v", detail, err)
	}
	if err == errProducerEnqueueTimeout {
		timeout := p.getEnqueueTimeout()
		log.Errorf("Failed to enqueue %s within %s. Triggering fallback.", detail, timeout)
		trace.AddRequestTag(ctx, "async_forward_to", "fallback_storage")
		p.writeToFallbackFile(msg)
		return errors.WithCode(code.ErrKafkaFailed, "producer enqueue timeout after %s, message written to fallback", timeout)
	}
	log.Errorf("Failed to enqueue %s: %v. Triggering fallback.", detail, err)
	trace.AddRequestTag(ctx, "async_forward_to", "fallback_storage")
	p.writeToFallbackFile(msg)
	return errors.WithCode(code.ErrKafkaFailed, "producer enqueue failed (%v), message written to fallback", err)
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

func (p *UserProducer) SendUserCreateMessage(ctx context.Context, user *v1.User) error {
	trace.AddRequestTag(ctx, "username", user.Name)
	log.Debugf("[Producer] SendUserCreateMessage: username=%s", user.Name)
	return p.sendUserMessage(ctx, user, OperationCreate, UserCreateTopic)
}

func (p *UserProducer) SendUserUpdateMessage(ctx context.Context, user *v1.User) error {
	trace.AddRequestTag(ctx, "username", user.Name)
	log.Debugf("[Producer] SendUserUpdateMessage: username=%s", user.Name)
	return p.sendUserMessage(ctx, user, OperationUpdate, UserUpdateTopic)
}

func (p *UserProducer) SendUserDeleteMessage(ctx context.Context, username string) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-producer", "send_delete")
	if spanCtx != nil {
		ctx = spanCtx
	}
	trace.AddRequestTag(ctx, "username", username)
	log.Debugf("[Producer] SendUserDeleteMessage: username=%s", username)
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

	msg := &sarama.ProducerMessage{
		Topic: UserDeleteTopic,
		Key:   sarama.StringEncoder(username),
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{Key: []byte(HeaderOperation), Value: []byte(OperationDelete)},
			{Key: []byte(HeaderOriginalTimestamp), Value: []byte(time.Now().Format(time.RFC3339))},
			{Key: []byte(HeaderRetryCount), Value: []byte("0")},
		},
	}

	injectTraceHeader(ctx, msg)
	meta := attachProducerMetadata(msg, msg.Topic, OperationDelete, trace.TraceIDFromContext(ctx))
	if meta != nil {
		if span != nil {
			meta.parentSpanID = span.ID
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

func (p *UserProducer) sendUserMessage(ctx context.Context, user *v1.User, operation, topic string) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-producer", fmt.Sprintf("send_%s", operation))
	if spanCtx != nil {
		ctx = spanCtx
	}

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

	now := time.Now()
	key := strings.TrimSpace(user.Name)
	if key == "" {
		key = strconv.FormatUint(user.ID, 10)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(userData),
		Headers: []sarama.RecordHeader{
			{Key: []byte(HeaderOperation), Value: []byte(operation)},
			{Key: []byte(HeaderOriginalTimestamp), Value: []byte(now.Format(time.RFC3339))},
			{Key: []byte(HeaderRetryCount), Value: []byte("0")},
		},
	}

	injectTraceHeader(ctx, msg)
	meta := attachProducerMetadata(msg, msg.Topic, operation, trace.TraceIDFromContext(ctx))
	if meta != nil {
		if span != nil {
			meta.parentSpanID = span.ID
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
		Topic: UserRetryTopic,
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
	headers = p.updateOrAddHeader(headers, HeaderRetryCount, strconv.Itoa(currCount+1))
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
