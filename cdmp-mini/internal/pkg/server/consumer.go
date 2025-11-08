// internal/pkg/server/consumer.go
package server

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/redis/go-redis/v9"

	"github.com/jmoiron/sqlx"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/dbscan"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/db"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type UserConsumer struct {
	readers     []*kafka.Reader
	db          *gorm.DB
	sqlxDB      *sqlx.DB
	redis       *storage.RedisCluster
	producer    *UserProducer
	topic       string
	groupID     string
	instanceID  int // 新增：实例ID
	opts        *options.KafkaOptions
	markerCache *pendingMarkerCache
	// 移除本地保护状态，全部走redis全局key
	// 主控选举相关
	isMaster      bool
	poolReporter  poolStatsReporter
	poolComponent string
	fetcherCount  int
}

type deleteMessage struct {
	Username  string `json:"username"`
	DeletedAt string `json:"deleted_at"`
}

type consumerJob struct {
	msg       kafka.Message
	workerID  int
	readerIdx int
}

type consumerAck struct {
	message   kafka.Message
	workerID  int
	readerIdx int
	err       error
}

type consumerBatchItem struct {
	op        string
	message   kafka.Message
	readerIdx int
}

const cacheNullSentinel = "rate_limit_prevention"

const pendingMarkerCacheWindow = 500 * time.Millisecond
const batchChannelFreeSlotDivisor = 8
const createFastFlushTimeout = 25 * time.Millisecond

type markerCacheEntry struct {
	exists      bool
	value       string
	hasTTL      bool
	expireAt    time.Time
	cacheExpiry time.Time
}

func (e markerCacheEntry) remainingTTL(now time.Time) time.Duration {
	if !e.hasTTL {
		return 0
	}
	remaining := e.expireAt.Sub(now)
	if remaining <= 0 {
		return 0
	}
	return remaining
}

type pendingMarkerCache struct {
	mu      sync.RWMutex
	entries map[string]markerCacheEntry
}

func newPendingMarkerCache() *pendingMarkerCache {
	return &pendingMarkerCache{entries: make(map[string]markerCacheEntry)}
}

func (c *pendingMarkerCache) Get(key string) (markerCacheEntry, bool) {
	if c == nil {
		return markerCacheEntry{}, false
	}
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return markerCacheEntry{}, false
	}
	now := time.Now()
	if now.After(entry.cacheExpiry) || (entry.hasTTL && now.After(entry.expireAt)) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return markerCacheEntry{}, false
	}
	return entry, true
}

func (c *pendingMarkerCache) Set(key string, entry markerCacheEntry) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = entry
	c.mu.Unlock()
}

func (c *pendingMarkerCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

var (
	userMessagePool = sync.Pool{
		New: func() interface{} {
			return &v1.User{}
		},
	}
	deleteMessagePool = sync.Pool{
		New: func() interface{} {
			return &deleteMessage{}
		},
	}
)

const (
	poolStatsReportInterval = 5 * time.Second
	slowDBQueryThreshold    = 200 * time.Millisecond
	dbWriteMaxRetries       = 3
	dbWriteInitialBackoff   = 100 * time.Millisecond
	dbWriteMaxBackoff       = 2 * time.Second
)

type conditionValueKind int

const (
	conditionKindNumeric conditionValueKind = iota
	conditionKindString
	conditionKindTime
)

type conditionFieldMeta struct {
	Column string
	Kind   conditionValueKind
}

var userConditionMeta = map[string]conditionFieldMeta{
	"id":         {Column: "id", Kind: conditionKindNumeric},
	"instanceID": {Column: "instanceID", Kind: conditionKindString},
	"name":       {Column: "name", Kind: conditionKindString},
	"email":      {Column: "email", Kind: conditionKindString},
	"phone":      {Column: "phone", Kind: conditionKindString},
	"status":     {Column: "status", Kind: conditionKindNumeric},
	"isAdmin":    {Column: "isAdmin", Kind: conditionKindNumeric},
	"createdAt":  {Column: "createdAt", Kind: conditionKindTime},
	"updatedAt":  {Column: "updatedAt", Kind: conditionKindTime},
	"loginedAt":  {Column: "loginedAt", Kind: conditionKindTime},
	"version":    {Column: "version", Kind: conditionKindNumeric},
}

type poolStatsReporter struct {
	provider   func() []db.PoolStats
	lastReport atomic.Int64
}

func (r *poolStatsReporter) report(ctx context.Context, component string) {
	if r == nil || r.provider == nil {
		return
	}
	if component = strings.TrimSpace(component); component == "" {
		component = "consumer"
	}
	now := time.Now().UnixNano()
	last := r.lastReport.Load()
	if last != 0 && now-last < poolStatsReportInterval.Nanoseconds() {
		return
	}
	if !r.lastReport.CompareAndSwap(last, now) {
		return
	}
	stats := r.provider()
	if len(stats) == 0 || metrics.DatabasePoolOpenConnections == nil {
		return
	}
	pools := make(map[string]map[string]interface{}, len(stats))
	for _, stat := range stats {
		indexLabel := strconv.Itoa(stat.Index)
		metrics.DatabasePoolOpenConnections.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.OpenConnections))
		metrics.DatabasePoolInUse.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.InUse))
		metrics.DatabasePoolIdle.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.Idle))
		metrics.DatabasePoolWaitCount.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.WaitCount))
		metrics.DatabasePoolWaitDurationSeconds.WithLabelValues(component, stat.Role, indexLabel).Set(stat.Stats.WaitDuration.Seconds())
		metrics.DatabasePoolMaxOpenConnections.WithLabelValues(component, stat.Role, indexLabel).Set(float64(stat.Stats.MaxOpenConnections))
		traceKey := stat.Role
		if stat.Index >= 0 {
			traceKey = fmt.Sprintf("%s_%d", stat.Role, stat.Index)
		}
		pools[traceKey] = map[string]interface{}{
			"open":         stat.Stats.OpenConnections,
			"in_use":       stat.Stats.InUse,
			"idle":         stat.Stats.Idle,
			"wait_count":   stat.Stats.WaitCount,
			"wait_seconds": stat.Stats.WaitDuration.Seconds(),
			"max_open":     stat.Stats.MaxOpenConnections,
		}
	}
	trace.AddRequestTag(ctx, fmt.Sprintf("db_pool_%s", sanitizeTraceKey(component)), map[string]interface{}{
		"component": component,
		"pools":     pools,
	})
}

func sanitizeTraceKey(component string) string {
	replacer := strings.NewReplacer(" ", "_", "/", "_", ":", "_", ".", "_")
	trimmed := strings.TrimSpace(component)
	if trimmed == "" {
		return "consumer"
	}
	return replacer.Replace(trimmed)
}

func NewUserConsumer(opts *options.KafkaOptions, topic, groupID string, instanceIndex int, db *gorm.DB, redis *storage.RedisCluster) *UserConsumer {
	fetcherCount := opts.FetcherCount
	if fetcherCount <= 0 {
		fetcherCount = 1
	}

	readers := make([]*kafka.Reader, 0, fetcherCount)
	for fetcherIdx := 0; fetcherIdx < fetcherCount; fetcherIdx++ {
		groupInstanceID := buildGroupInstanceID(opts.InstanceID, groupID, instanceIndex*fetcherCount+fetcherIdx)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: opts.Brokers,
			Topic:   topic,
			GroupID: groupID,
			// Enable static membership so that the coordinator can track this consumer across restarts.
			GroupInstanceID: groupInstanceID,

			// 优化配置
			MinBytes:      32 * 1024, // 32KB，兼顾延迟与批量度
			MaxBytes:      10e6,      // 10MB
			MaxWait:       time.Millisecond * 100,
			QueueCapacity: 512, // 放大队列容量以喂饱 worker 池

			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,

			// 添加重试配置
			MaxAttempts:    opts.MaxRetries,
			ReadBackoffMin: time.Millisecond * 100,
			ReadBackoffMax: time.Millisecond * 1000,
		})
		readers = append(readers, reader)
	}

	consumer := &UserConsumer{
		readers:       readers,
		db:            db,
		redis:         redis,
		topic:         topic,
		groupID:       groupID,
		opts:          opts,
		poolComponent: groupID,
		// 新增：实例ID赋值
		instanceID:   instanceIndex,
		fetcherCount: fetcherCount,
		markerCache:  newPendingMarkerCache(),
	}
	if sqlCore, err := db.DB(); err != nil {
		log.Errorf("initialize sqlx db failed: %v", err)
	} else {
		consumer.sqlxDB = sqlx.NewDb(sqlCore, "mysql").Unsafe()
	}
	//go consumer.startLagMonitor(context.Background())
	return consumer

}

func (c *UserConsumer) ensureSQLX() (*sqlx.DB, error) {
	if c.sqlxDB != nil {
		return c.sqlxDB, nil
	}
	if c.db == nil {
		return nil, fmt.Errorf("gorm db not initialized")
	}
	sqlCore, err := c.db.DB()
	if err != nil {
		return nil, fmt.Errorf("acquire sql core failed: %w", err)
	}
	c.sqlxDB = sqlx.NewDb(sqlCore, "mysql").Unsafe()
	return c.sqlxDB, nil
}

// parseInstanceID 支持 string->int 转换，若失败则用 hash 兜底
func parseInstanceID(idStr string) int {
	if idStr == "" {
		return 0
	}
	if n, err := strconv.Atoi(idStr); err == nil {
		return n
	}
	// fallback: hash string
	sum := 0
	for _, c := range idStr {
		sum += int(c)
	}
	return sum & 0x7FFFFFFF // 保证正数
}

func buildGroupInstanceID(base, group string, index int) string {
	const maxLen = 249

	baseComponent := sanitizeGroupInstanceComponent(base)
	if baseComponent == "" {
		baseComponent = "consumer"
	}

	groupComponent := sanitizeGroupInstanceComponent(group)
	if groupComponent == "" {
		groupComponent = "group"
	}

	candidate := fmt.Sprintf("%s-%s-%02d", baseComponent, groupComponent, index)
	if len(candidate) > maxLen {
		candidate = candidate[:maxLen]
	}
	return candidate
}

func sanitizeGroupInstanceComponent(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(trimmed))
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '@':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// 消费
func (c *UserConsumer) StartConsuming(ctx context.Context, workerCount int, ready *sync.WaitGroup) {
	ctx, span := trace.StartSpan(ctx, "kafka-consumer", "start_consuming")
	trace.AddRequestTag(ctx, "topic", c.topic)
	trace.AddRequestTag(ctx, "group", c.groupID)
	trace.AddRequestTag(ctx, "reader_count", len(c.readers))
	defer trace.EndSpan(span, "success", "", map[string]interface{}{})

	jobs := make(chan consumerJob, 2048)
	acks := make(chan consumerAck, 2048)

	readyOnce := sync.Once{}
	signalReady := func() {
		if ready != nil {
			readyOnce.Do(func() { ready.Done() })
		}
	}

	ctx, dispatcherSpan := trace.StartSpan(ctx, "kafka-consumer", "dispatch_loop")
	defer func() {
		trace.EndSpan(dispatcherSpan, "success", "", nil)
		signalReady()
	}()

	// worker数量与分区数动态匹配，保证每个分区有独立worker
	var workerWg sync.WaitGroup
	partitionCount := c.opts.DesiredPartitions
	actualWorkerCount := workerCount
	if partitionCount > 0 && workerCount < partitionCount {
		actualWorkerCount = partitionCount
	}
	if actualWorkerCount <= 0 {
		actualWorkerCount = 1
	}

	for i := 0; i < actualWorkerCount; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for job := range jobs {
				operation := c.getOperationFromHeaders(job.msg.Headers)
				messageKey := string(job.msg.Key)
				processStart := time.Now()

				msgCtx, msgSpan := c.startAsyncTraceContext(ctx, job.msg, operation, workerID)
				err := c.processMessageWithRetry(msgCtx, job.msg, 3)

				businessCode := strconv.Itoa(code.ErrSuccess)
				status := "success"
				message := "message processed"
				if forwarded, ok := trace.GetRequestTag(msgCtx, "async_forward_to"); ok {
					status = "degraded"
					businessCode = strconv.Itoa(code.ErrKafkaFailed)
					if target, ok := forwarded.(string); ok && target != "" {
						message = fmt.Sprintf("forwarded to %s", target)
					} else {
						message = "forwarded to fallback"
					}
				}
				if err != nil {
					status = "error"
					if c := errors.GetCode(err); c != 0 {
						businessCode = strconv.Itoa(c)
					} else {
						businessCode = strconv.Itoa(code.ErrUnknown)
					}
					message = err.Error()
				}
				spanDetails := map[string]interface{}{
					"topic":       c.topic,
					"partition":   job.msg.Partition,
					"offset":      job.msg.Offset,
					"worker_id":   workerID,
					"reader_idx":  job.readerIdx,
					"operation":   operation,
					"message_key": messageKey,
				}
				if forwarded, ok := trace.GetRequestTag(msgCtx, "async_forward_to"); ok {
					spanDetails["forward_target"] = forwarded
				}
				if err != nil {
					spanDetails["error"] = err.Error()
				}
				trace.EndSpan(msgSpan, status, businessCode, spanDetails)
				trace.RecordOutcome(msgCtx, businessCode, message, status, 0)
				trace.Complete(msgCtx)

				c.recordConsumerMetrics(operation, messageKey, processStart, err, job.workerID)

				ack := consumerAck{message: job.msg, workerID: job.workerID, readerIdx: job.readerIdx, err: err}
				select {
				case acks <- ack:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		type partitionState struct {
			nextOffset int64
		}
		pending := make(map[int]map[int64]consumerAck)
		states := make(map[int]*partitionState)
		for ack := range acks {
			if ack.err != nil {
				log.Warnf("Fetcher: message processing failed (worker=%d partition=%d offset=%d): %v", ack.workerID, ack.message.Partition, ack.message.Offset, ack.err)
				continue
			}
			partition := ack.message.Partition
			state, exists := states[partition]
			if !exists {
				state = &partitionState{nextOffset: ack.message.Offset}
				states[partition] = state
			}
			if ack.message.Offset < state.nextOffset {
				continue
			}
			if _, ok := pending[partition]; !ok {
				pending[partition] = make(map[int64]consumerAck)
			}
			pending[partition][ack.message.Offset] = ack
			for {
				expected := state.nextOffset
				ready, ok := pending[partition][expected]
				if !ok {
					break
				}
				if err := c.commitWithRetry(ctx, ready.message, ready.workerID, ready.readerIdx); err != nil {
					log.Errorf("Fetcher: 提交偏移失败 partition=%d offset=%d err=%v", partition, ready.message.Offset, err)
					break
				}
				delete(pending[partition], expected)
				state.nextOffset = expected + 1
			}
		}
	}()

	batchCapacity := c.opts.BatchChannelCapacity
	if batchCapacity <= 0 {
		batchCapacity = 1024
	}
	batchCh := make(chan consumerBatchItem, batchCapacity)

	var batchWg sync.WaitGroup
	batchWg.Add(1)
	go func() {
		defer batchWg.Done()
		c.runBatchAggregator(ctx, batchCh, acks)
	}()

	var fetchWg sync.WaitGroup
	for idx, reader := range c.readers {
		if reader == nil {
			continue
		}
		fetchWg.Add(1)
		go func(readerIdx int, r *kafka.Reader) {
			defer fetchWg.Done()
			c.runFetchLoop(ctx, readerIdx, r, jobs, batchCh, actualWorkerCount)
		}(idx, reader)
	}

	signalReady()

	fetchWg.Wait()
	close(batchCh)
	close(jobs)
	workerWg.Wait()
	batchWg.Wait()
	close(acks)
	commitWg.Wait()
}

func (c *UserConsumer) runBatchAggregator(ctx context.Context, batchCh <-chan consumerBatchItem, acks chan<- consumerAck) {
	if batchCh == nil {
		return
	}

	aggCtx, span := trace.StartSpan(ctx, "kafka-consumer", "batch_aggregator")
	trace.AddRequestTag(aggCtx, "topic", c.topic)
	trace.AddRequestTag(aggCtx, "group", c.groupID)
	ctx = aggCtx

	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	var processedCreates, processedDeletes, processedUpdates int

	defer func() {
		details := map[string]any{
			"create_count":    processedCreates,
			"delete_count":    processedDeletes,
			"update_count":    processedUpdates,
			"buffer_capacity": cap(batchCh),
		}
		if err := ctx.Err(); err != nil && status == "success" {
			status = "error"
			statusCode = strconv.Itoa(code.ErrUnknown)
			details["error"] = err.Error()
		}
		trace.EndSpan(span, status, statusCode, details)
	}()

	maxBatchBound := c.opts.MaxDBBatchSize
	if maxBatchBound < 1 {
		maxBatchBound = 100
	}
	minBatchBound := c.opts.MinDBBatchSize
	if minBatchBound < 1 || minBatchBound > maxBatchBound {
		minBatchBound = maxBatchBound / 2
		if minBatchBound < 1 {
			minBatchBound = 1
		}
	}

	minTimeout := c.opts.MinBatchTimeout
	if minTimeout <= 0 {
		minTimeout = 40 * time.Millisecond
	}
	maxTimeout := c.opts.MaxBatchTimeout
	if maxTimeout <= 0 {
		maxTimeout = 200 * time.Millisecond
	}
	if maxTimeout < minTimeout {
		maxTimeout = minTimeout
	}
	baseTimeout := c.opts.BatchTimeout
	if baseTimeout <= 0 {
		baseTimeout = minTimeout
	}
	if baseTimeout < minTimeout {
		baseTimeout = minTimeout
	} else if baseTimeout > maxTimeout {
		baseTimeout = maxTimeout
	}

	currentTimeout := baseTimeout
	ticker := time.NewTicker(currentTimeout)
	defer ticker.Stop()

	currentBatchLimit := maxBatchBound
	batchWorkerID := -1

	ackMessages := func(msgs []kafka.Message, readers []int) bool {
		for i, m := range msgs {
			readerIdx := -1
			if i < len(readers) {
				readerIdx = readers[i]
			}
			select {
			case acks <- consumerAck{message: m, workerID: batchWorkerID, readerIdx: readerIdx, err: nil}:
			case <-ctx.Done():
				status = "error"
				statusCode = strconv.Itoa(code.ErrUnknown)
				return false
			}
		}
		return true
	}

	adjustParams := func(pending int) {
		if pending < 0 {
			pending = 0
		}
		capacity := cap(batchCh)
		targetLimit := maxBatchBound
		targetTimeout := baseTimeout

		if c.opts.LagProtected || (capacity > 0 && pending >= capacity*3/4) {
			targetLimit = minBatchBound
			targetTimeout = minTimeout
		} else if capacity > 0 && pending >= capacity/2 {
			mid := (minBatchBound + maxBatchBound) / 2
			if mid < minBatchBound {
				mid = minBatchBound
			}
			targetLimit = mid
			if baseTimeout > minTimeout {
				targetTimeout = baseTimeout - (baseTimeout-minTimeout)/2
			} else {
				targetTimeout = minTimeout
			}
		} else if capacity > 0 && pending <= capacity/4 && !c.opts.LagProtected {
			extra := baseTimeout + (maxTimeout-baseTimeout)/2
			if extra > maxTimeout {
				extra = maxTimeout
			}
			targetTimeout = extra
			targetLimit = maxBatchBound
		}

		if targetLimit < minBatchBound {
			targetLimit = minBatchBound
		} else if targetLimit > maxBatchBound {
			targetLimit = maxBatchBound
		}

		if targetTimeout < minTimeout {
			targetTimeout = minTimeout
		} else if targetTimeout > maxTimeout {
			targetTimeout = maxTimeout
		}

		if targetLimit != currentBatchLimit {
			currentBatchLimit = targetLimit
		}
		if targetTimeout != currentTimeout {
			ticker.Reset(targetTimeout)
			currentTimeout = targetTimeout
		}
	}

	batchPool := sync.Pool{
		New: func() any {
			return make([]kafka.Message, 0, maxBatchBound)
		},
	}
	getBatch := func(batch []kafka.Message) []kafka.Message {
		if batch != nil {
			return batch
		}
		return batchPool.Get().([]kafka.Message)[:0]
	}
	releaseBatch := func(batch []kafka.Message) []kafka.Message {
		if batch == nil {
			return nil
		}
		for i := range batch {
			batch[i] = kafka.Message{}
		}
		batchPool.Put(batch[:0])
		return nil
	}

	var (
		createBatchMsgs    []kafka.Message
		createBatchReaders []int
		deleteBatchMsgs    []kafka.Message
		deleteBatchReaders []int
		updateBatchMsgs    []kafka.Message
		updateBatchReaders []int
	)

	flush := func() bool {
		if len(createBatchMsgs) > 0 {
			c.batchCreateToDB(ctx, createBatchMsgs)
			if !ackMessages(createBatchMsgs, createBatchReaders) {
				return false
			}
			processedCreates += len(createBatchMsgs)
		}
		createBatchMsgs = releaseBatch(createBatchMsgs)
		createBatchReaders = createBatchReaders[:0]

		if len(deleteBatchMsgs) > 0 {
			c.batchDeleteFromDB(ctx, deleteBatchMsgs)
			if !ackMessages(deleteBatchMsgs, deleteBatchReaders) {
				return false
			}
			processedDeletes += len(deleteBatchMsgs)
		}
		deleteBatchMsgs = releaseBatch(deleteBatchMsgs)
		deleteBatchReaders = deleteBatchReaders[:0]

		if len(updateBatchMsgs) > 0 {
			c.batchUpdateToDB(ctx, updateBatchMsgs)
			if !ackMessages(updateBatchMsgs, updateBatchReaders) {
				return false
			}
			processedUpdates += len(updateBatchMsgs)
		}
		updateBatchMsgs = releaseBatch(updateBatchMsgs)
		updateBatchReaders = updateBatchReaders[:0]

		adjustParams(len(batchCh))
		return true
	}

	for {
		adjustParams(len(batchCh))
		select {
		case bi, ok := <-batchCh:
			if !ok {
				flush()
				return
			}
			switch bi.op {
			case OperationCreate:
				createBatchMsgs = append(getBatch(createBatchMsgs), bi.message)
				createBatchReaders = append(createBatchReaders, bi.readerIdx)
				if len(createBatchMsgs) == 1 && len(batchCh) == 0 {
					fastTimeout := createFastFlushTimeout
					if fastTimeout <= 0 {
						fastTimeout = minTimeout
					}
					if fastTimeout > 0 && currentTimeout > fastTimeout {
						ticker.Reset(fastTimeout)
						currentTimeout = fastTimeout
					}
				}
				if len(createBatchMsgs) >= currentBatchLimit {
					if !flush() {
						return
					}
				}
			case OperationDelete:
				deleteBatchMsgs = append(getBatch(deleteBatchMsgs), bi.message)
				deleteBatchReaders = append(deleteBatchReaders, bi.readerIdx)
				if len(deleteBatchMsgs) >= currentBatchLimit {
					if !flush() {
						return
					}
				}
			case OperationUpdate:
				updateBatchMsgs = append(getBatch(updateBatchMsgs), bi.message)
				updateBatchReaders = append(updateBatchReaders, bi.readerIdx)
				if len(updateBatchMsgs) >= currentBatchLimit {
					if !flush() {
						return
					}
				}
			default:
				// 忽略不支持批量的操作
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		case <-ctx.Done():
			status = "error"
			statusCode = strconv.Itoa(code.ErrUnknown)
			flush()
			return
		}
	}
}

func (c *UserConsumer) runFetchLoop(ctx context.Context, readerIdx int, reader *kafka.Reader, jobs chan<- consumerJob, batchCh chan<- consumerBatchItem, workerCount int) {
	loopCtx, span := trace.StartSpan(ctx, "kafka-consumer", "fetch_loop")
	trace.AddRequestTag(loopCtx, "reader_idx", readerIdx)
	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	processed := 0
	defer func() {
		details := map[string]any{"processed": processed}
		trace.EndSpan(span, status, statusCode, details)
	}()

	if workerCount <= 0 {
		workerCount = 1
	}

	nextWorker := readerIdx % workerCount
	batchNearFull := func() bool {
		if batchCh == nil {
			return true
		}
		capacity := cap(batchCh)
		if capacity <= 0 {
			return false
		}
		current := len(batchCh)
		freeSlots := capacity - current
		margin := capacity / batchChannelFreeSlotDivisor
		if margin < 1 {
			margin = 1
		}
		return freeSlots <= margin
	}

	tryEnqueueCreateBatch := func(item consumerBatchItem) bool {
		if batchCh == nil {
			return false
		}
		if batchNearFull() {
			return false
		}
		select {
		case batchCh <- item:
			return true
		default:
			return false
		}
	}

	for {
		select {
		case <-loopCtx.Done():
			status = "error"
			statusCode = strconv.Itoa(code.ErrUnknown)
			trace.AddRequestTag(loopCtx, "loop_cancelled", true)
			return
		default:
		}

		stats := reader.Stats()
		if stats.Lag == 0 {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-loopCtx.Done():
				status = "error"
				statusCode = strconv.Itoa(code.ErrUnknown)
				trace.AddRequestTag(loopCtx, "loop_cancelled", true)
				return
			}
		}

		var (
			msg      kafka.Message
			fetchErr error
		)
		for retry := 0; retry < c.opts.MaxRetries; retry++ {
			attemptCtx, attemptSpan := trace.StartSpan(loopCtx, "kafka-consumer", "fetch_attempt")
			retryNum := retry + 1
			trace.AddRequestTag(attemptCtx, "attempt", retryNum)
			trace.AddRequestTag(attemptCtx, "reader_idx", readerIdx)
			trace.AddRequestTag(attemptCtx, "lag", stats.Lag)
			msg, fetchErr = reader.FetchMessage(attemptCtx)
			if fetchErr == nil {
				details := map[string]any{
					"attempt":   retryNum,
					"partition": msg.Partition,
					"offset":    msg.Offset,
				}
				trace.EndSpan(attemptSpan, "success", strconv.Itoa(code.ErrSuccess), details)
				processed++
				break
			}
			if stderrs.Is(fetchErr, context.Canceled) || stderrs.Is(fetchErr, context.DeadlineExceeded) {
				log.Warnf("Fetcher %d: 上下文已取消，停止获取消息", readerIdx)
				trace.EndSpan(attemptSpan, "error", strconv.Itoa(code.ErrUnknown), map[string]any{
					"attempt": retryNum,
					"error":   fetchErr.Error(),
				})
				status = "error"
				statusCode = strconv.Itoa(code.ErrUnknown)
				return
			}
			trace.EndSpan(attemptSpan, "error", strconv.Itoa(code.ErrUnknown), map[string]any{
				"attempt": retryNum,
				"error":   fetchErr.Error(),
			})
			log.Warnf("Fetcher %d: 获取消息失败 (重试 %d/%d): %v", readerIdx, retry+1, c.opts.MaxRetries, fetchErr)
			backoff := time.Second * time.Duration(1<<uint(retry))
			select {
			case <-time.After(backoff):
			case <-loopCtx.Done():
				log.Warnf("Fetcher %d: 重试期间上下文取消", readerIdx)
				return
			}
		}
		if fetchErr != nil {
			log.Errorf("Fetcher %d: 获取消息最终失败: %v", readerIdx, fetchErr)
			trace.AddRequestTag(loopCtx, "fetch_failure", fetchErr.Error())
			select {
			case <-time.After(500 * time.Millisecond):
			case <-loopCtx.Done():
				status = "error"
				statusCode = strconv.Itoa(code.ErrUnknown)
				trace.AddRequestTag(loopCtx, "loop_cancelled", true)
				return
			}
			continue
		}

		op := c.getOperationFromHeaders(msg.Headers)
		if op == OperationCreate {
			batchItem := consumerBatchItem{op: op, message: msg, readerIdx: readerIdx}
			if tryEnqueueCreateBatch(batchItem) {
				continue
			}
		}

		job := consumerJob{msg: msg, workerID: nextWorker, readerIdx: readerIdx}
		select {
		case jobs <- job:
		case <-loopCtx.Done():
			return
		case <-time.After(10 * time.Millisecond):
			if op == OperationCreate {
				batchItem := consumerBatchItem{op: op, message: msg, readerIdx: readerIdx}
				if tryEnqueueCreateBatch(batchItem) {
					continue
				}
			}
			if op == OperationDelete || op == OperationUpdate {
				batchItem := consumerBatchItem{op: op, message: msg, readerIdx: readerIdx}
				select {
				case batchCh <- batchItem:
					continue
				default:
					select {
					case jobs <- job:
					case <-loopCtx.Done():
						return
					}
				}
			} else {
				select {
				case jobs <- job:
				case <-loopCtx.Done():
					return
				}
			}
		}

		nextWorker = (nextWorker + 1) % workerCount
	}
}

// 消息调度 - 已弃用
// StartConsuming 已经采用单 fetcher + worker 池的模式替代了旧的并发 Fetch/Commit 实现。
// 保留该函数签名以避免潜在外部引用编译错误，但实现为空。

// 处理消息
// ...old worker and processSingleMessage removed. Use StartConsuming with the new fetcher+worker flow.

// commitWithRetry 尝试提交消息偏移，遇到临时错误会重试
func (c *UserConsumer) commitWithRetry(ctx context.Context, msg kafka.Message, workerID int, readerIdx int) error {
	reader := c.readerForIndex(readerIdx)
	if reader == nil {
		if len(c.readers) == 0 {
			return fmt.Errorf("commit failed: no kafka reader available")
		}
		reader = c.readers[0]
	}

	spanCtx, span := trace.StartSpan(ctx, "kafka-consumer", "commit_offset")
	trace.AddRequestTag(spanCtx, "reader_idx", readerIdx)
	trace.AddRequestTag(spanCtx, "partition", msg.Partition)
	trace.AddRequestTag(spanCtx, "offset", msg.Offset)
	trace.AddRequestTag(spanCtx, "worker_id", workerID)

	maxAttempts := 3
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := reader.CommitMessages(spanCtx, msg); err != nil {
			lastErr = err
			metrics.ConsumerProcessingErrors.WithLabelValues(c.topic, c.groupID, "commit", "commit_error").Inc()
			metrics.ConsumerCommitFailures.WithLabelValues(c.topic, c.groupID, fmt.Sprintf("%d", msg.Partition)).Inc()
			log.Warnf("Worker %d: 提交偏移量失败 (reader=%d 尝试 %d/%d): topic=%s partition=%d offset=%d err=%v",
				workerID, readerIdx, i+1, maxAttempts, msg.Topic, msg.Partition, msg.Offset, err)
			wait := time.Duration(100*(1<<uint(i))) * time.Millisecond
			select {
			case <-time.After(wait):
				continue
			case <-spanCtx.Done():
				trace.EndSpan(span, "error", strconv.Itoa(code.ErrUnknown), map[string]any{"error": spanCtx.Err()})
				return spanCtx.Err()
			}
		} else {
			metrics.ConsumerCommitSuccess.WithLabelValues(c.topic, c.groupID, fmt.Sprintf("%d", msg.Partition)).Inc()
			trace.EndSpan(span, "success", strconv.Itoa(code.ErrSuccess), nil)
			return nil
		}
	}

	trace.EndSpan(span, "error", strconv.Itoa(code.ErrUnknown), map[string]any{"error": lastErr.Error()})
	log.Errorf("Worker %d: 提交偏移量最终失败 (reader=%d): %v", workerID, readerIdx, lastErr)
	return lastErr
}

// 业务处理
func (c *UserConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	operation := c.getOperationFromHeaders(msg.Headers)

	switch operation {
	case OperationCreate:
		return c.processCreateOperation(ctx, msg)
	case OperationUpdate:
		return c.processUpdateOperation(ctx, msg)
	case OperationDelete:
		return c.processDeleteOperation(ctx, msg)
	default:
		log.Errorf("未知操作类型: %s", operation)
		if c.producer != nil {
			return c.producer.SendToDeadLetterTopic(ctx, msg, "UNKNOWN_OPERATION: "+operation)
		}
		return fmt.Errorf("未知操作类型: %s", operation)
	}
}

func (c *UserConsumer) processCreateOperation(ctx context.Context, msg kafka.Message) error {
	user := userMessagePool.Get().(*v1.User)
	defer func() {
		*user = v1.User{}
		userMessagePool.Put(user)
	}()

	if err := decodeUserMessage(msg.Value, user); err != nil {
		return err
	}
	if err := validation.ValidateUserFields(user.Name, user.Nickname, user.Password, user.Email, user.Phone); err != nil {
		return c.sendToDeadLetter(ctx, msg, err.Error())
	}
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
	ensureUserInstanceID(user)

	pendingStart := time.Now()
	markerCtx, markerSpan := trace.StartSpan(ctx, "kafka-consumer", "pending_marker_verify")
	trace.AddRequestTag(markerCtx, "username", user.Name)
	pendingExists, pendingValue, pendingTTL, redisGetDuration, redisTTLDur, pendingErr := c.getPendingCreateMarker(markerCtx, user.Name)
	markerDuration := time.Since(pendingStart)
	pendingStatus := "success"
	pendingCode := strconv.Itoa(code.ErrSuccess)
	if pendingErr != nil {
		pendingStatus = "error"
		if c := errors.GetCode(pendingErr); c != 0 {
			pendingCode = strconv.Itoa(c)
		} else {
			pendingCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	details := map[string]interface{}{
		"username":     user.Name,
		"duration_ms":  markerDuration.Milliseconds(),
		"marker_found": pendingExists,
		"redis_get_ms": redisGetDuration.Milliseconds(),
		"redis_ttl_ms": redisTTLDur.Milliseconds(),
	}
	if pendingValue != "" {
		details["marker_payload_len"] = len(pendingValue)
	}
	if pendingTTL > 0 {
		details["marker_ttl_ms"] = pendingTTL.Milliseconds()
	}
	trace.EndSpan(markerSpan, pendingStatus, pendingCode, details)
	if pendingErr != nil {
		trace.AddRequestTag(ctx, "pending_marker_error", pendingErr.Error())
		return c.sendToRetry(ctx, msg, "PENDING_MARKER_ERROR: "+pendingErr.Error())
	}
	trace.AddRequestTag(ctx, "pending_marker_present", pendingExists)
	pendingDegraded := false
	if pendingExists && pendingValue != "" {
		trace.AddRequestTag(ctx, "pending_marker_value_len", len(pendingValue))
		if degraded, decodeErr := usercache.PendingMarkerIsDegraded(pendingValue); decodeErr != nil {
			trace.AddRequestTag(ctx, "pending_marker_decode_error", decodeErr.Error())
		} else if degraded {
			pendingDegraded = true
			trace.AddRequestTag(ctx, "pending_marker_degraded", true)
		}
	}
	if pendingTTL > 0 {
		trace.AddRequestTag(ctx, "pending_marker_ttl_ms", pendingTTL.Milliseconds())
	}

	markerMissingFallback := false
	if !pendingExists {
		markerMissingFallback = true
		trace.AddRequestTag(ctx, "pending_marker_missing", true)
		existing, err := c.loadUserSnapshotWithTrace(ctx, user.Name, "pending_marker_missing")
		if err != nil {
			return c.sendToRetry(ctx, msg, "CHECK_EXISTING_FAILED: "+err.Error())
		}
		if existing != nil {
			trace.AddRequestTag(ctx, "pending_marker_missing_existing", true)
			if err := c.setUserCache(ctx, existing, nil); err != nil {
				log.Warnf("用户创建消息到达但该用户已存在, 刷新缓存失败: username=%s err=%v", existing.Name, err)
			}
			return nil
		}
		trace.AddRequestTag(ctx, "pending_marker_missing_fallback", true)
		log.Warnf("未检测到用户创建pending标记，降级走数据库兜底: username=%s", user.Name)
	}

	persistStart := time.Now()
	persistCtx, persistSpan := trace.StartSpan(ctx, "kafka-consumer", "persist_user")
	trace.AddRequestTag(persistCtx, "username", user.Name)
	created, err := c.createUserInDB(persistCtx, user, pendingDegraded)
	persistDuration := time.Since(persistStart)
	persistStatus := "success"
	persistCode := strconv.Itoa(code.ErrSuccess)
	if err != nil {
		persistStatus = "error"
		if c := errors.GetCode(err); c != 0 {
			persistCode = strconv.Itoa(c)
		} else {
			persistCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	trace.EndSpan(persistSpan, persistStatus, persistCode, map[string]interface{}{
		"username":    user.Name,
		"duration_ms": persistDuration.Milliseconds(),
		"created":     created,
	})
	if err != nil {
		if markerMissingFallback {
			trace.AddRequestTag(ctx, "pending_marker_missing_create_error", err.Error())
			log.Errorf("pending标记缺失降级插入失败: username=%s err=%v", user.Name, err)
			return nil
		}
		return c.sendToDeadLetter(ctx, msg, "CREATE_DB_ERROR: "+err.Error())
	}
	trace.AddRequestTag(ctx, "create_db_inserted", created)

	if created {
		if err := c.setUserCache(ctx, user, nil); err != nil {
			log.Warnf("用户创建成功但缓存设置失败: username=%s, error=%v", user.Name, err)
		}
	} else {
		trace.AddRequestTag(ctx, "create_duplicate_skip", true)
		existing, err := c.loadUserSnapshotWithTrace(ctx, user.Name, "duplicate_refresh")
		if err == nil && existing != nil {
			if err := c.setUserCache(ctx, existing, nil); err != nil {
				log.Warnf("重复创建消息刷新缓存失败: username=%s err=%v", existing.Name, err)
			}
		}
	}

	clearStart := time.Now()
	clearCtx, clearSpan := trace.StartSpan(ctx, "kafka-consumer", "clear_pending_marker")
	trace.AddRequestTag(clearCtx, "username", user.Name)
	clearRedisDuration, clearErr := c.clearPendingCreateMarker(clearCtx, user.Name)
	clearDuration := time.Since(clearStart)
	clearStatus := "success"
	clearCode := strconv.Itoa(code.ErrSuccess)
	if clearErr != nil {
		clearStatus = "error"
		if c := errors.GetCode(clearErr); c != 0 {
			clearCode = strconv.Itoa(c)
		} else {
			clearCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	trace.EndSpan(clearSpan, clearStatus, clearCode, map[string]interface{}{
		"username":        user.Name,
		"duration_ms":     clearDuration.Milliseconds(),
		"cleared":         clearErr == nil,
		"redis_delete_ms": clearRedisDuration.Milliseconds(),
	})
	if clearErr != nil {
		trace.AddRequestTag(ctx, "pending_marker_clear_failed", true)
		log.Warnf("清理用户创建幂等标记失败: username=%s err=%v", user.Name, clearErr)
	} else {
		trace.AddRequestTag(ctx, "pending_marker_cleared", true)
	}

	return nil
}

// 删除
func (c *UserConsumer) processDeleteOperation(ctx context.Context, msg kafka.Message) error {
	deleteRequest := deleteMessagePool.Get().(*deleteMessage)
	defer func() {
		deleteRequest.Username = ""
		deleteRequest.DeletedAt = ""
		deleteMessagePool.Put(deleteRequest)
	}()

	if err := jsonCodec.Unmarshal(msg.Value, deleteRequest); err != nil {
		return c.sendToDeadLetter(ctx, msg, "UNMARSHAL_ERROR: "+err.Error())
	}

	var (
		userID           uint64
		existingSnapshot *v1.User
		pendingExists    bool
		pendingTTL       time.Duration
		pendingErr       error
	)
	retryCount := 0
	if header := c.getHeaderValue(msg.Headers, HeaderRetryCount); header != "" {
		if parsed, err := strconv.Atoi(header); err == nil {
			retryCount = parsed
		}
	}
	if username := strings.TrimSpace(deleteRequest.Username); username != "" {
		monitorCtx, monitorSpan := trace.StartSpan(ctx, "kafka-consumer", "pending_marker_lookup_delete")
		trace.AddRequestTag(monitorCtx, "username", username)
		var pendingValue string
		var redisGetDuration, redisTTLDur time.Duration
		pendingExists, pendingValue, pendingTTL, redisGetDuration, redisTTLDur, pendingErr = c.getPendingCreateMarker(monitorCtx, username)
		status := "success"
		statusCode := strconv.Itoa(code.ErrSuccess)
		details := map[string]any{
			"username":     username,
			"marker_found": pendingExists,
			"redis_get_ms": redisGetDuration.Milliseconds(),
			"redis_ttl_ms": redisTTLDur.Milliseconds(),
		}
		if pendingTTL > 0 {
			details["marker_ttl_ms"] = pendingTTL.Milliseconds()
		}
		if pendingValue != "" {
			details["marker_payload_len"] = len(pendingValue)
		}
		if pendingErr != nil {
			status = "error"
			if c := errors.GetCode(pendingErr); c != 0 {
				statusCode = strconv.Itoa(c)
			} else {
				statusCode = strconv.Itoa(code.ErrUnknown)
			}
			details["error"] = pendingErr.Error()
		}
		trace.EndSpan(monitorSpan, status, statusCode, details)
		if pendingErr != nil {
			trace.AddRequestTag(ctx, "delete_pending_marker_error", pendingErr.Error())
			log.Warnf("删除消息检查pending标记失败: username=%s err=%v", username, pendingErr)
		} else if pendingExists {
			trace.AddRequestTag(ctx, "delete_pending_marker_exists", true)
			if pendingTTL > 0 {
				trace.AddRequestTag(ctx, "delete_pending_marker_ttl_ms", pendingTTL.Milliseconds())
			}
		}
	}
	if deleteRequest.Username != "" {
		snapshot, err := c.loadUserSnapshot(ctx, deleteRequest.Username)
		if err != nil {
			return c.sendToRetry(ctx, msg, "查询用户失败: "+err.Error())
		}
		if snapshot != nil {
			userID = snapshot.ID
			existingSnapshot = snapshot
		}
	}

	if err := c.deleteUserFromDB(ctx, deleteRequest.Username); err != nil {
		if errors.IsCode(err, code.ErrUserNotFound) || stderrs.Is(err, gorm.ErrRecordNotFound) {
			retryPending := pendingExists || pendingErr != nil
			if retryCount == 0 && retryPending {
				reason := "DELETE_TARGET_NOT_READY"
				if pendingErr != nil {
					reason = "DELETE_TARGET_STATE_UNKNOWN"
				}
				trace.AddRequestTag(ctx, "delete_retry_on_not_found", true)
				if pendingTTL > 0 {
					trace.AddRequestTag(ctx, "delete_retry_pending_ttl_ms", pendingTTL.Milliseconds())
				}
				log.Warnf("Delete message for %s reached before user is ready, scheduling retry", deleteRequest.Username)
				return c.sendToRetry(ctx, msg, reason+": "+err.Error())
			}
			trace.AddRequestTag(ctx, "delete_idempotent_skip", true)
			c.purgeUserState(ctx, deleteRequest.Username, userID, existingSnapshot)
			return nil
		}
		return c.sendToRetry(ctx, msg, "删除用户失败: "+err.Error())
	}

	c.purgeUserState(ctx, deleteRequest.Username, userID, existingSnapshot)

	return nil
}

func (c *UserConsumer) processUpdateOperation(ctx context.Context, msg kafka.Message) error {
	user := userMessagePool.Get().(*v1.User)
	defer func() {
		*user = v1.User{}
		userMessagePool.Put(user)
	}()

	if err := decodeUserMessage(msg.Value, user); err != nil {
		return c.sendToDeadLetter(ctx, msg, "UNMARSHAL_ERROR: "+err.Error())
	}

	command := user.Command
	if strings.TrimSpace(string(command)) == "" {
		command = v1.UserUpdateCommandFull
	}

	switch command {
	case v1.UserUpdateCommandBatch:
		return c.handleBatchPatch(ctx, msg, user)
	case v1.UserUpdateCommandPatch, v1.UserUpdateCommandFull:
		return c.handleSingleUpdate(ctx, msg, user, command)
	default:
		return c.sendToDeadLetter(ctx, msg, "UNKNOWN_UPDATE_COMMAND: "+string(command))
	}
}

func (c *UserConsumer) handleSingleUpdate(ctx context.Context, msg kafka.Message, user *v1.User, command v1.UserUpdateCommand) error {
	if err := validation.ValidateUserFields(user.Name, user.Nickname, user.Password, user.Email, user.Phone); err != nil {
		return c.sendToDeadLetter(ctx, msg, err.Error())
	}

	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	var (
		existingSnapshot *v1.User
		err              error
	)
	if user.ExpectedVersion != nil {
		existingSnapshot, err = c.loadUserSnapshotStrong(ctx, user.Name)
	} else {
		existingSnapshot, err = c.loadUserSnapshot(ctx, user.Name)
	}
	if err != nil {
		if stderrs.Is(err, sql.ErrNoRows) || stderrs.Is(err, gorm.ErrRecordNotFound) {
			return c.sendToRetry(ctx, msg, "UPDATE_TARGET_NOT_READY: "+user.Name)
		}
		return c.sendToRetry(ctx, msg, "查询用户失败: "+err.Error())
	}
	if existingSnapshot == nil {
		return c.sendToRetry(ctx, msg, "UPDATE_TARGET_NOT_READY: "+user.Name)
	}

	existing := *existingSnapshot
	updated := existing
	if strings.TrimSpace(user.InstanceID) != "" {
		updated.InstanceID = user.InstanceID
	}
	updated.ID = existing.ID
	if user.Password != "" {
		updated.Password = user.Password
	}
	if command == v1.UserUpdateCommandFull {
		updated.Nickname = user.Nickname
		updated.Email = user.Email
		updated.Phone = user.Phone
		updated.Status = user.Status
		updated.IsAdmin = user.IsAdmin
	}
	if !user.LoginedAt.IsZero() {
		updated.LoginedAt = user.LoginedAt
	}
	if len(user.Extend) != 0 {
		updated.Extend = user.Extend
	}
	if user.ExtendShadow != "" {
		updated.ExtendShadow = user.ExtendShadow
	}
	if user.Patch != nil {
		if err := user.Patch.Apply(&updated); err != nil {
			return c.sendToDeadLetter(ctx, msg, "APPLY_PATCH_FAILED: "+err.Error())
		}
		// 当 PATCH 未显式包含邮箱/手机号时，保持快照中的值，避免被零值覆盖
		if user.Patch.Email == nil {
			updated.Email = existing.Email
		}
		if user.Patch.Phone == nil {
			updated.Phone = existing.Phone
		}
	}
	if err := v1.EnsureExtendShadow(&updated.ObjectMeta); err != nil {
		return c.sendToDeadLetter(ctx, msg, "SERIALIZE_EXTEND_FAILED: "+err.Error())
	}

	updated.UpdatedAt = time.Now().UTC()
	existingVersion := existing.ObjectMeta.Version
	var (
		expectedVersion *uint64
		expectedValue   uint64
	)
	if user.ExpectedVersion != nil {
		expectedValue = *user.ExpectedVersion
		expectedVersion = &expectedValue
		log.Infow("用户更新乐观锁检查", "username", user.Name, "expected_version", expectedValue, "existing_version", existingVersion, "command", command)
		if existingVersion != expectedValue {
			// 乐观锁冲突直接 ACK，记录监控日志，不进死信队列
			log.Warnf("用户更新命中乐观锁冲突，直接丢弃: username=%s expected_version=%v actual_version=%v", user.Name, expectedValue, existingVersion)
			metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "optimistic_conflict").Inc()
			return nil
		}
	}
	if expectedVersion == nil {
		if command == v1.UserUpdateCommandPatch {
			expectedValue = existingVersion
			expectedVersion = &expectedValue
			log.Warnw("PATCH 更新缺少版本号，使用当前版本兜底", "username", user.Name, "command", command, "current_version", existingVersion)
		} else {
			log.Warnf("用户更新缺少版本号，直接丢弃以避免覆盖: username=%s command=%s current_version=%d", user.Name, command, existingVersion)
			metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "missing_expected_version").Inc()
			trace.AddRequestTag(ctx, "update_missing_version", true)
			return nil
		}
	}
	log.Infow("用户更新版本调试", "username", user.Name, "expected_version", *expectedVersion, "existing_version", existingVersion)
	if strings.HasPrefix(user.Name, "lock_case_") {
		fmt.Printf("[lock-debug] name=%s expected=%d existing=%d command=%s\n", user.Name, *expectedVersion, existingVersion, command)
		appendLockDebug(fmt.Sprintf("consumer|phase=pre-update|user=%s|expected=%d|existing=%d|command=%s", user.Name, *expectedVersion, existingVersion, command))
	}

	updated.ObjectMeta.Version = existingVersion + 1

	if err := c.updateUserInDB(ctx, &updated, expectedVersion); err != nil {
		if isDuplicateKeyDBError(err) {
			// 唯一约束冲突视为幂等或业务性冲突，不应进入重试或死信队列，直接认为处理完成并提交偏移量
			log.Warnf("用户更新命中唯一约束冲突，忽略并提交偏移: username=%s err=%v", updated.Name, err)
			return nil
		}
		if errors.IsCode(err, code.ErrResourceConflict) {
			// 乐观锁冲突直接 ACK，记录监控日志，不再重试
			trace.AddRequestTag(ctx, "update_conflict", true)
			log.Warnf("用户更新命中乐观锁冲突，直接丢弃: username=%s expected_version=%v err=%v", updated.Name, expectedVersion, err)
			metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "optimistic_conflict").Inc()
			return nil
		}
		return c.sendToRetry(ctx, msg, "更新用户失败: "+err.Error())
	}

	if err := c.setUserCache(ctx, &updated, existingSnapshot); err != nil {
		log.Warnf("用户更新成功但缓存刷新失败: username=%s err=%v", updated.Name, err)
	}

	return nil
}

func (c *UserConsumer) handleBatchPatch(ctx context.Context, msg kafka.Message, update *v1.User) error {
	if update.Patch == nil {
		return c.sendToDeadLetter(ctx, msg, "BATCH_PATCH_MISSING_UPDATES")
	}
	if len(update.Conditions) == 0 {
		return c.sendToDeadLetter(ctx, msg, "BATCH_PATCH_MISSING_CONDITIONS")
	}

	whereClause, args, err := buildUserConditionClause(update.Conditions)
	if err != nil {
		return c.sendToDeadLetter(ctx, msg, "INVALID_CONDITIONS: "+err.Error())
	}

	targets, err := c.loadUsersByConditions(ctx, whereClause, args)
	if err != nil {
		return c.sendToRetry(ctx, msg, "查询批量更新目标失败: "+err.Error())
	}
	if len(targets) == 0 {
		log.Warnw("批量更新条件未匹配任何用户", map[string]any{
			"message_key": string(msg.Key),
			"conditions":  update.Conditions,
			"where":       whereClause,
			"args":        args,
		})
		return nil
	}

	const maxLoggedTargets = 10
	loggedTargets := make([]string, 0, len(targets))
	for i := 0; i < len(targets) && i < maxLoggedTargets; i++ {
		loggedTargets = append(loggedTargets, targets[i].Name)
	}
	if len(targets) > maxLoggedTargets {
		loggedTargets = append(loggedTargets, "...")
	}
	log.Infow("批量更新命中用户", "message_key", string(msg.Key), "count", len(targets), "sample_users", loggedTargets)

	var (
		retryErr            error
		unresolvedConflicts int
	)

	const maxConflictResyncAttempts = 3

targetLoop:
	for idx := range targets {
		snapshot := targets[idx]
		conflictUnresolved := false

	attemptLoop:
		for attempt := 1; attempt <= maxConflictResyncAttempts; attempt++ {
			current := snapshot
			patched := current
			if err := update.Patch.Apply(&patched); err != nil {
				return c.sendToDeadLetter(ctx, msg, "APPLY_PATCH_FAILED: "+err.Error())
			}
			patched.Email = usercache.NormalizeEmail(patched.Email)
			patched.Phone = usercache.NormalizePhone(patched.Phone)
			patched.UpdatedAt = time.Now().UTC()
			expected := current.ObjectMeta.Version
			patched.ExpectedVersion = &expected
			patched.ObjectMeta.Version = expected + 1
			if err := v1.EnsureExtendShadow(&patched.ObjectMeta); err != nil {
				return c.sendToDeadLetter(ctx, msg, "SERIALIZE_EXTEND_FAILED: "+err.Error())
			}
			if err := c.updateUserInDB(ctx, &patched, patched.ExpectedVersion); err != nil {
				if isDuplicateKeyDBError(err) {
					log.Warnf("批量更新命中唯一约束冲突，跳过该条: username=%s err=%v", patched.Name, err)
					break attemptLoop
				}
				if errors.IsCode(err, code.ErrResourceConflict) {
					log.Warnf("批量更新版本冲突: username=%s current_version=%d expected_version=%d new_version=%d attempt=%d", current.Name, current.ObjectMeta.Version, expected, patched.ObjectMeta.Version, attempt)
					if attempt == maxConflictResyncAttempts {
						conflictUnresolved = true
						break attemptLoop
					}
					latest, loadErr := c.loadUserSnapshot(ctx, current.Name)
					if loadErr != nil {
						retryErr = fmt.Errorf("批量更新冲突后重新加载用户失败: %w", loadErr)
						break targetLoop
					}
					if latest == nil {
						log.Warnf("批量更新冲突后目标缺失，跳过: username=%s", current.Name)
						break attemptLoop
					}
					snapshot = *latest
					continue attemptLoop
				}
				retryErr = err
				break targetLoop
			}
			if err := c.setUserCache(ctx, &patched, &current); err != nil {
				log.Warnf("批量更新刷新缓存失败: username=%s err=%v", patched.Name, err)
			}
			break attemptLoop
		}

		if retryErr != nil {
			break
		}
		if conflictUnresolved {
			unresolvedConflicts++
		}
	}

	if retryErr != nil {
		return c.sendToRetry(ctx, msg, "批量更新失败: "+retryErr.Error())
	}
	if unresolvedConflicts > 0 {
		log.Warnf("批量更新存在未解决的版本冲突: count=%d", unresolvedConflicts)
		return nil
	}
	return nil
}

func (c *UserConsumer) createUserInDB(ctx context.Context, user *v1.User, markerDegraded bool) (bool, error) {
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	totalStart := time.Now()

	now := time.Now()
	user.CreatedAt = now
	user.UpdatedAt = now

	if strings.TrimSpace(user.ExtendShadow) == "" {
		user.ExtendShadow = user.Extend.String()
	}

	db, err := c.ensureSQLX()
	prepareDuration := time.Since(totalStart)
	trace.AddRequestTag(ctx, "create_prepare_ms", prepareDuration.Milliseconds())
	if err != nil {
		trace.AddRequestTag(ctx, "create_prepare_error", err.Error())
		return false, fmt.Errorf("获取数据库连接失败: %w", err)
	}

	var phoneValue interface{}
	if trimmed := strings.TrimSpace(user.Phone); trimmed != "" {
		phoneValue = trimmed
	}

	version := user.ObjectMeta.Version
	if version == 0 {
		version = 1
	}
	execStart := time.Now()
	res, err := db.ExecContext(ctx,
		"INSERT INTO `user` (instanceID, name, nickname, password, email, phone, status, isAdmin, extendShadow, createdAt, updatedAt, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		user.InstanceID,
		user.Name,
		user.Nickname,
		user.Password,
		user.Email,
		phoneValue,
		user.Status,
		user.IsAdmin,
		user.ExtendShadow,
		now,
		now,
		version,
	)
	createDuration := time.Since(execStart)
	totalDuration := time.Since(totalStart)
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "create_user_db").Observe(createDuration.Seconds())
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "create_user_db_total").Observe(totalDuration.Seconds())
	trace.AddRequestTag(ctx, "create_db_ms", createDuration.Milliseconds())
	trace.AddRequestTag(ctx, "create_total_ms", totalDuration.Milliseconds())
	if err != nil {
		metrics.BusinessFailures.WithLabelValues("consumer", "create_user_db", "db_exec_error").Inc()
		if isDuplicateKeyDBError(err) {
			log.Warnw("检测到用户重复插入，直接忽略", "username", user.Name, "error", err, "prepare_duration", prepareDuration, "db_duration", createDuration, "total_duration", totalDuration)
			trace.AddRequestTag(ctx, "create_db_duplicate", true)
			if markerDegraded {
				trace.AddRequestTag(ctx, "create_degraded_conflict", true)
			}
			metrics.BusinessSuccess.WithLabelValues("consumer", "create_user_db", "duplicate_skip").Inc()
			return false, nil
		}
		return false, fmt.Errorf("数据创建失败: %w", err)
	}

	if insertedID, idErr := res.LastInsertId(); idErr == nil && insertedID > 0 {
		user.ID = uint64(insertedID)
	}
	user.ObjectMeta.Version = version
	metrics.BusinessSuccess.WithLabelValues("consumer", "create_user_db", "db_exec").Inc()
	log.Infow("用户插入完成", "username", user.Name, "prepare_duration", prepareDuration, "db_duration", createDuration, "total_duration", totalDuration, "marker_degraded", markerDegraded, "version", user.ObjectMeta.Version)

	return true, nil
}

// loadUserSnapshot 查询数据库中的用户信息，用于判定重复消息或刷新缓存。

func (c *UserConsumer) loadUserSnapshot(ctx context.Context, username string) (*v1.User, error) {
	return c.loadUserSnapshotInternal(ctx, username, false)
}

func (c *UserConsumer) loadUserSnapshotStrong(ctx context.Context, username string) (*v1.User, error) {
	return c.loadUserSnapshotInternal(ctx, username, true)
}

func (c *UserConsumer) loadUserSnapshotInternal(ctx context.Context, username string, skipCache bool) (*v1.User, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, nil
	}

	if !skipCache && c.redis != nil {
		cacheKey := usercache.UserKey(trimmed)
		if cacheKey != "" {
			start := time.Now()
			value, err := c.redis.GetKey(ctx, cacheKey)
			duration := time.Since(start)
			metricErr := err
			if err == redis.Nil {
				metricErr = nil
			}
			metrics.RecordRedisOperation("user_snapshot_get", duration.Seconds(), metricErr)
			switch {
			case err == redis.Nil:
				metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "cache_miss").Inc()
			case err != nil:
				metrics.BusinessFailures.WithLabelValues("consumer", "load_user_snapshot", "cache_error").Inc()
				log.Warnw("读取用户缓存失败", "username", trimmed, "error", err)
			case value == cacheNullSentinel || strings.TrimSpace(value) == "":
				metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "cache_sentinel").Inc()
			default:
				cached, decodeErr := usercache.Unmarshal([]byte(value))
				if decodeErr != nil {
					metrics.BusinessFailures.WithLabelValues("consumer", "load_user_snapshot", "cache_decode_error").Inc()
					log.Warnw("用户缓存反序列化失败", "username", trimmed, "error", decodeErr)
				} else if cached != nil {
					trace.AddRequestTag(ctx, "snapshot_source", "cache")
					metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "cache_hit").Inc()
					return cached, nil
				}
			}
		}
	}

	db, err := c.ensureSQLX()
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %w", err)
	}

	const query = "SELECT id, instanceID, name, nickname, password, email, phone, status, isAdmin, extendShadow, createdAt, updatedAt, loginedAt, version FROM `user` WHERE name = ? LIMIT 1"
	trace.AddRequestTag(ctx, "snapshot_source", "database")
	start := time.Now()
	rows, err := db.QueryContext(ctx, query, trimmed)
	duration := time.Since(start)
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "load_user_snapshot_db").Observe(duration.Seconds())
	trace.AddRequestTag(ctx, "snapshot_db_ms", duration.Milliseconds())
	if err != nil {
		metrics.BusinessFailures.WithLabelValues("consumer", "load_user_snapshot", "db_query_error").Inc()
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "db_miss").Inc()
		return nil, nil
	}
	var record v1.User
	if _, err := dbscan.ScanUserFullInto(rows, &record); err != nil {
		if stderrs.Is(err, sql.ErrNoRows) {
			metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "db_miss").Inc()
			return nil, nil
		}
		metrics.BusinessFailures.WithLabelValues("consumer", "load_user_snapshot", "db_scan_error").Inc()
		return nil, err
	}
	if err := rows.Err(); err != nil {
		metrics.BusinessFailures.WithLabelValues("consumer", "load_user_snapshot", "db_rows_error").Inc()
		return nil, err
	}
	metrics.BusinessSuccess.WithLabelValues("consumer", "load_user_snapshot", "db_hit").Inc()
	if duration > slowDBQueryThreshold {
		log.Warnw("用户快照查询耗时较长", "username", trimmed, "duration", duration)
	}
	return &record, nil
}

func (c *UserConsumer) loadUserSnapshotWithTrace(ctx context.Context, username, reason string) (*v1.User, error) {
	start := time.Now()
	snapshotCtx, snapshotSpan := trace.StartSpan(ctx, "kafka-consumer", "load_existing_user")
	trace.AddRequestTag(snapshotCtx, "username", username)
	if strings.TrimSpace(reason) != "" {
		trace.AddRequestTag(snapshotCtx, "snapshot_reason", reason)
	}
	existing, err := c.loadUserSnapshot(snapshotCtx, username)
	duration := time.Since(start)
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
	details := map[string]interface{}{
		"username":    username,
		"duration_ms": duration.Milliseconds(),
		"found":       existing != nil,
	}
	if strings.TrimSpace(reason) != "" {
		details["reason"] = reason
	}
	trace.EndSpan(snapshotSpan, status, codeStr, details)
	return existing, err
}

// getPendingCreateMarker 读取 Redis 中的 pending 标记，供消费者做幂等校验。
func (c *UserConsumer) getPendingCreateMarker(ctx context.Context, username string) (bool, string, time.Duration, time.Duration, time.Duration, error) {
	if c.redis == nil {
		return false, "", 0, 0, 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return false, "", 0, 0, 0, nil
	}
	key := usercache.PendingCreateKey(trimmed)
	if key == "" {
		return false, "", 0, 0, 0, nil
	}
	if cached, ok := c.markerCache.Get(trimmed); ok {
		trace.AddRequestTag(ctx, "pending_marker_cache_hit", true)
		now := time.Now()
		return cached.exists, cached.value, cached.remainingTTL(now), 0, 0, nil
	}
	trace.AddRequestTag(ctx, "pending_marker_cache_hit", false)

	getStart := time.Now()
	value, err := c.redis.GetKey(ctx, key)
	getDuration := time.Since(getStart)
	getMetricErr := err
	if err == redis.Nil {
		getMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_marker_get", getDuration.Seconds(), getMetricErr)
	trace.AddRequestTag(ctx, "pending_marker_get_ms", getDuration.Milliseconds())
	if err != nil {
		if err == redis.Nil {
			c.markerCache.Set(trimmed, markerCacheEntry{
				exists:      false,
				hasTTL:      false,
				cacheExpiry: time.Now().Add(pendingMarkerCacheWindow),
			})
			return false, "", 0, getDuration, 0, nil
		}
		trace.AddRequestTag(ctx, "pending_marker_get_error", err.Error())
		return false, "", 0, getDuration, 0, err
	}

	ttlStart := time.Now()
	ttlSeconds, ttlErr := c.redis.GetExp(ctx, key)
	ttlDuration := time.Since(ttlStart)
	ttlMetricErr := ttlErr
	if ttlErr == storage.ErrKeyNotFound {
		ttlMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_marker_ttl", ttlDuration.Seconds(), ttlMetricErr)
	trace.AddRequestTag(ctx, "pending_marker_ttl_lookup_ms", ttlDuration.Milliseconds())
	if ttlErr != nil {
		if ttlErr == storage.ErrKeyNotFound {
			now := time.Now()
			entry := markerCacheEntry{
				exists:      true,
				hasTTL:      false,
				cacheExpiry: now.Add(pendingMarkerCacheWindow),
			}
			c.markerCache.Set(trimmed, entry)
			return true, value, 0, getDuration, ttlDuration, nil
		}
		trace.AddRequestTag(ctx, "pending_marker_ttl_error", ttlErr.Error())
		log.Debugf("获取 pending 标记TTL失败: key=%s err=%v", key, ttlErr)
		now := time.Now()
		entry := markerCacheEntry{
			exists:      true,
			hasTTL:      false,
			cacheExpiry: now.Add(pendingMarkerCacheWindow),
		}
		c.markerCache.Set(trimmed, entry)
		return true, value, 0, getDuration, ttlDuration, nil
	}

	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}
	now := time.Now()
	cacheExpiry := now.Add(pendingMarkerCacheWindow)
	entry := markerCacheEntry{
		exists:      true,
		value:       value,
		hasTTL:      ttl > 0,
		expireAt:    now.Add(ttl),
		cacheExpiry: cacheExpiry,
	}
	if ttl <= 0 {
		entry.hasTTL = false
		entry.expireAt = time.Time{}
	} else if entry.expireAt.Before(cacheExpiry) {
		entry.cacheExpiry = entry.expireAt
	}
	c.markerCache.Set(trimmed, entry)
	return true, value, ttl, getDuration, ttlDuration, nil
}

// clearPendingCreateMarker 在消息处理完成后清理 pending 标记，防止重复请求受阻。
func (c *UserConsumer) clearPendingCreateMarker(ctx context.Context, username string) (time.Duration, error) {
	if c.redis == nil {
		return 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}
	key := usercache.PendingCreateKey(trimmed)
	if key == "" {
		return 0, nil
	}
	c.markerCache.Delete(trimmed)
	deleteStart := time.Now()
	deleted, err := c.redis.DeleteKey(ctx, key)
	deleteDuration := time.Since(deleteStart)
	deleteMetricErr := err
	if err == redis.Nil {
		deleteMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_marker_delete", deleteDuration.Seconds(), deleteMetricErr)
	trace.AddRequestTag(ctx, "pending_marker_clear_ms", deleteDuration.Milliseconds())
	if err == redis.Nil {
		return deleteDuration, nil
	}
	if err != nil {
		trace.AddRequestTag(ctx, "pending_marker_clear_error", err.Error())
		return deleteDuration, err
	}
	trace.AddRequestTag(ctx, "pending_marker_cleared", deleted)
	return deleteDuration, nil
}

func isDuplicateKeyDBError(err error) bool {
	if err == nil {
		return false
	}
	if stderrs.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if stderrs.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	msg := err.Error()
	if strings.Contains(msg, "1062") || strings.Contains(msg, "Duplicate entry") || strings.Contains(strings.ToLower(msg), "unique constraint") {
		return true
	}
	return false
}

func isRetryableDBWriteError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if stderrs.As(err, &mysqlErr) {
		if mysqlErr.Number == 1213 || mysqlErr.Number == 1205 {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "deadlock") || strings.Contains(msg, "lock wait timeout") {
		return true
	}
	return false
}

func nextDBBackoff(attempt int) time.Duration {
	delay := dbWriteInitialBackoff << (attempt - 1)
	if delay > dbWriteMaxBackoff {
		return dbWriteMaxBackoff
	}
	return delay
}

func ensureUserInstanceID(user *v1.User) {
	if user == nil {
		return
	}
	if strings.TrimSpace(user.InstanceID) != "" {
		return
	}
	user.InstanceID = idutil.GetInstanceID(idutil.GetIntID(), "user-")
}

func (c *UserConsumer) deleteUserFromDB(ctx context.Context, username string) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-consumer", "delete_user_db")
	trace.AddRequestTag(spanCtx, "username", username)
	ctx = spanCtx
	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	defer func() {
		trace.EndSpan(span, status, statusCode, map[string]any{"username": username})
	}()
	db, err := c.ensureSQLX()
	if err != nil {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUnknown)
		trace.AddRequestTag(ctx, "delete_db_prepare_error", err.Error())
		return fmt.Errorf("获取数据库连接失败: %w", err)
	}

	res, execErr := db.ExecContext(ctx,
		"DELETE FROM `user` WHERE name = ?",
		username,
	)
	if execErr != nil {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUnknown)
		trace.AddRequestTag(ctx, "delete_db_error", execErr.Error())
		return execErr
	}
	affected, affErr := res.RowsAffected()
	if affErr != nil {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUnknown)
		trace.AddRequestTag(ctx, "delete_db_affect_error", affErr.Error())
		return affErr
	}
	if affected == 0 {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUserNotFound)
		errNotFound := errors.WithCode(code.ErrUserNotFound, "用户没有发现")
		trace.AddRequestTag(ctx, "delete_db_not_found", true)
		return errNotFound
	}
	return nil
}

func (c *UserConsumer) updateUserInDB(ctx context.Context, user *v1.User, expectedVersion *uint64) error {
	spanCtx, span := trace.StartSpan(ctx, "kafka-consumer", "update_user_db")
	trace.AddRequestTag(spanCtx, "username", user.Name)
	if expectedVersion != nil {
		trace.AddRequestTag(spanCtx, "expected_version", *expectedVersion)
	}
	ctx = spanCtx
	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	attempts := 0
	var execDuration time.Duration
	defer func() {
		details := map[string]any{
			"attempts": attempts,
		}
		if execDuration > 0 {
			details["last_exec_ms"] = execDuration.Milliseconds()
		}
		trace.EndSpan(span, status, statusCode, details)
	}()
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	if strings.TrimSpace(user.ExtendShadow) == "" {
		user.ExtendShadow = user.Extend.String()
	}

	db, err := c.ensureSQLX()
	if err != nil {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUnknown)
		trace.AddRequestTag(ctx, "update_db_prepare_error", err.Error())
		return fmt.Errorf("获取数据库连接失败: %w", err)
	}

	phoneValue := interface{}(nil)
	if trimmed := strings.TrimSpace(user.Phone); trimmed != "" {
		phoneValue = trimmed
	}
	loginedValue := interface{}(nil)
	if !user.LoginedAt.IsZero() {
		loginedValue = user.LoginedAt
	}

	newVersion := user.ObjectMeta.Version
	// 强制使用 (name, version) 复合索引以避免优化器选择次优索引导致高延迟
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("UPDATE `user` FORCE INDEX (idx_user_name_version) SET email = ?, password = ?, status = ?, isAdmin = ?, updatedAt = ?, extendShadow = ?, nickname = ?, phone = ?, loginedAt = ?, version = ? WHERE name = ?")
	args := []interface{}{
		user.Email,
		user.Password,
		user.Status,
		user.IsAdmin,
		user.UpdatedAt,
		user.ExtendShadow,
		user.Nickname,
		phoneValue,
		loginedValue,
		newVersion,
		user.Name,
	}
	if expectedVersion != nil {
		queryBuilder.WriteString(" AND version = ?")
		args = append(args, *expectedVersion)
	}

	var (
		res      sql.Result
		execErr  error
		duration time.Duration
	)
	for attempt := 1; attempt <= dbWriteMaxRetries; attempt++ {
		attempts = attempt
		start := time.Now()
		res, execErr = db.ExecContext(ctx, queryBuilder.String(), args...)
		duration = time.Since(start)
		execDuration = duration
		metrics.BusinessProcessingTime.WithLabelValues("consumer", "update_user_db").Observe(duration.Seconds())
		trace.AddRequestTag(ctx, "update_db_ms", duration.Milliseconds())
		if execErr == nil {
			break
		}
		metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "db_exec_error").Inc()
		if attempt == dbWriteMaxRetries || !isRetryableDBWriteError(execErr) {
			status = "error"
			if c := errors.GetCode(execErr); c != 0 {
				statusCode = strconv.Itoa(c)
			} else {
				statusCode = strconv.Itoa(code.ErrUnknown)
			}
			trace.AddRequestTag(ctx, "update_db_error", execErr.Error())
			return fmt.Errorf("数据库更新失败: %w", execErr)
		}
		log.Warnw("用户更新SQL检测到死锁/锁等待，准备重试", "username", user.Name, "attempt", attempt, "error", execErr)
		time.Sleep(nextDBBackoff(attempt))
	}

	if expectedVersion != nil {
		affected, affErr := res.RowsAffected()
		if affErr != nil {
			metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "rows_affected_error").Inc()
			status = "error"
			statusCode = strconv.Itoa(code.ErrUnknown)
			trace.AddRequestTag(ctx, "update_db_rows_error", affErr.Error())
			return fmt.Errorf("获取更新影响行数失败: %w", affErr)
		}
		if affected == 0 {
			metrics.BusinessFailures.WithLabelValues("consumer", "update_user_db", "version_conflict").Inc()
			status = "error"
			statusCode = strconv.Itoa(code.ErrResourceConflict)
			trace.AddRequestTag(ctx, "update_db_version_conflict", true)
			return errors.WithCode(code.ErrResourceConflict, "用户数据版本冲突")
		}
	}

	metrics.BusinessSuccess.WithLabelValues("consumer", "update_user_db", "db_exec").Inc()
	if duration > slowDBQueryThreshold {
		expected := uint64(0)
		if expectedVersion != nil {
			expected = *expectedVersion
		}
		log.Warnw("用户更新SQL耗时较长", "username", user.Name, "duration", duration, "expected_version", expected, "new_version", newVersion)
	}

	return nil
}

func (c *UserConsumer) loadUsersByConditions(ctx context.Context, whereClause string, args []interface{}) ([]v1.User, error) {
	db, err := c.ensureSQLX()
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %w", err)
	}

	query := fmt.Sprintf("SELECT id, instanceID, name, nickname, password, email, phone, status, isAdmin, extendShadow, createdAt, updatedAt, loginedAt, version FROM `user` WHERE %s", whereClause)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]v1.User, 0, 16)
	for rows.Next() {
		var record v1.User
		if _, err := dbscan.ScanUserFullInto(rows, &record); err != nil {
			return nil, err
		}
		results = append(results, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func buildUserConditionClause(conds v1.UserConditions) (string, []interface{}, error) {
	if len(conds) == 0 {
		return "", nil, fmt.Errorf("缺少批量更新条件")
	}
	clauses := make([]string, 0, len(conds))
	args := make([]interface{}, 0, len(conds))
	for field, raw := range conds {
		meta, ok := userConditionMeta[field]
		if !ok {
			return "", nil, fmt.Errorf("不支持的条件字段: %s", field)
		}
		fieldClauses, fieldArgs, err := parseCondition(meta, raw)
		if err != nil {
			return "", nil, fmt.Errorf("条件字段 %s 解析失败: %w", field, err)
		}
		clauses = append(clauses, fieldClauses...)
		args = append(args, fieldArgs...)
	}
	if len(clauses) == 0 {
		return "", nil, fmt.Errorf("条件解析后为空")
	}
	return strings.Join(clauses, " AND "), args, nil
}

func parseCondition(meta conditionFieldMeta, raw json.RawMessage) ([]string, []interface{}, error) {
	var payload interface{}
	if err := jsonCodec.Unmarshal(raw, &payload); err != nil {
		return nil, nil, err
	}
	switch typed := payload.(type) {
	case map[string]interface{}:
		return parseConditionObject(meta, typed)
	case []interface{}:
		return buildInClause(meta, typed)
	default:
		clause, arg, err := buildEqualityClause(meta, typed)
		if err != nil {
			return nil, nil, err
		}
		return []string{clause}, []interface{}{arg}, nil
	}
}

func parseConditionObject(meta conditionFieldMeta, obj map[string]interface{}) ([]string, []interface{}, error) {
	clauses := make([]string, 0, len(obj))
	args := make([]interface{}, 0, len(obj))
	for op, raw := range obj {
		switch strings.ToLower(op) {
		case "eq":
			clause, arg, err := buildEqualityClause(meta, raw)
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		case "neq":
			clause, arg, err := buildComparisonClause(meta, raw, "<>")
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		case "in":
			slice, ok := raw.([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("IN 条件必须是数组")
			}
			innerClauses, innerArgs, err := buildInClause(meta, slice)
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, innerClauses...)
			args = append(args, innerArgs...)
		case "like":
			if meta.Kind != conditionKindString {
				return nil, nil, fmt.Errorf("字段 %s 不支持 LIKE 条件", meta.Column)
			}
			str, err := convertToString(raw)
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, fmt.Sprintf("%s LIKE ?", meta.Column))
			args = append(args, str)
		case "gt":
			clause, arg, err := buildComparisonClause(meta, raw, ">")
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		case "gte":
			clause, arg, err := buildComparisonClause(meta, raw, ">=")
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		case "lt":
			clause, arg, err := buildComparisonClause(meta, raw, "<")
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		case "lte":
			clause, arg, err := buildComparisonClause(meta, raw, "<=")
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, clause)
			args = append(args, arg)
		default:
			return nil, nil, fmt.Errorf("不支持的条件操作符: %s", op)
		}
	}
	return clauses, args, nil
}

func buildEqualityClause(meta conditionFieldMeta, value interface{}) (string, interface{}, error) {
	switch meta.Kind {
	case conditionKindNumeric:
		num, err := convertToInt64(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s = ?", meta.Column), num, nil
	case conditionKindString:
		str, err := convertToString(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s = ?", meta.Column), str, nil
	case conditionKindTime:
		tm, err := convertToTime(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s = ?", meta.Column), tm, nil
	default:
		return "", nil, fmt.Errorf("未知的条件类型")
	}
}

func buildComparisonClause(meta conditionFieldMeta, value interface{}, operator string) (string, interface{}, error) {
	switch meta.Kind {
	case conditionKindNumeric:
		num, err := convertToInt64(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s ?", meta.Column, operator), num, nil
	case conditionKindTime:
		tm, err := convertToTime(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s ?", meta.Column, operator), tm, nil
	case conditionKindString:
		str, err := convertToString(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s ?", meta.Column, operator), str, nil
	default:
		return "", nil, fmt.Errorf("未知的条件类型")
	}
}

func buildInClause(meta conditionFieldMeta, values []interface{}) ([]string, []interface{}, error) {
	if len(values) == 0 {
		return nil, nil, fmt.Errorf("IN 条件值为空")
	}
	placeholders := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	for _, raw := range values {
		switch meta.Kind {
		case conditionKindNumeric:
			num, err := convertToInt64(raw)
			if err != nil {
				return nil, nil, err
			}
			args = append(args, num)
		case conditionKindString:
			str, err := convertToString(raw)
			if err != nil {
				return nil, nil, err
			}
			args = append(args, str)
		case conditionKindTime:
			tm, err := convertToTime(raw)
			if err != nil {
				return nil, nil, err
			}
			args = append(args, tm)
		}
		placeholders = append(placeholders, "?")
	}
	clause := fmt.Sprintf("%s IN (%s)", meta.Column, strings.Join(placeholders, ", "))
	return []string{clause}, args, nil
}

func convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return v.Int64()
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, fmt.Errorf("数值不能为空字符串")
		}
		parsed, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("无法转换为整数: %T", value)
	}
}

func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return "", fmt.Errorf("无法转换为字符串: %T", value)
	}
}

func convertToTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return time.Time{}, fmt.Errorf("时间不能为空字符串")
		}
		if tm, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return tm, nil
		}
		if tm, err := time.Parse(time.RFC3339, trimmed); err == nil {
			return tm, nil
		}
		return time.Time{}, fmt.Errorf("时间格式无效: %s", trimmed)
	case float64:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("无法转换为时间: %T", value)
	}
}

// 辅助函数
// processMessageWithRetry 带重试的消息处理
func (c *UserConsumer) processMessageWithRetry(ctx context.Context, msg kafka.Message, maxRetries int) error {
	retryCtx, retrySpan := trace.StartSpan(ctx, "kafka-consumer", "process_with_retry")
	trace.AddRequestTag(retryCtx, "max_retries", maxRetries)
	trace.AddRequestTag(retryCtx, "partition", msg.Partition)
	trace.AddRequestTag(retryCtx, "offset", msg.Offset)
	if len(msg.Key) > 0 {
		trace.AddRequestTag(retryCtx, "message_key", string(msg.Key))
	}

	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	finalDetails := map[string]any{
		"max_retries": maxRetries,
	}
	attemptsUsed := 0
	var lastErr error

	defer func() {
		finalDetails["attempts_used"] = attemptsUsed
		if lastErr != nil {
			finalDetails["error"] = lastErr.Error()
		}
		trace.EndSpan(retrySpan, status, statusCode, finalDetails)
	}()

	component := c.poolComponent
	if component == "" {
		component = c.groupID
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		attemptsUsed = attempt
		attemptCtx, attemptSpan := trace.StartSpan(retryCtx, "kafka-consumer", "process_attempt")
		trace.AddRequestTag(attemptCtx, "attempt", attempt)

		c.poolReporter.report(attemptCtx, component)

		err := c.processMessage(attemptCtx, msg)
		if err == nil {
			trace.EndSpan(attemptSpan, "success", strconv.Itoa(code.ErrSuccess), map[string]any{"attempt": attempt})
			trace.AddRequestTag(retryCtx, "retry_outcome", "success")
			trace.AddRequestTag(retryCtx, "retry_attempts", attempt)
			return nil
		}

		lastErr = err
		errorCode := code.ErrUnknown
		if c := errors.GetCode(err); c != 0 {
			errorCode = c
		}
		trace.EndSpan(attemptSpan, "error", strconv.Itoa(errorCode), map[string]any{
			"attempt": attempt,
			"error":   err.Error(),
		})

		if !shouldRetry(err) {
			trace.AddRequestTag(retryCtx, "retry_outcome", "non_retryable")
			finalDetails["non_retryable"] = true
			return nil
		}

		log.Warnf("消息处理失败，准备重试 (尝试 %d/%d): %v", attempt, maxRetries, err)

		if attempt < maxRetries {
			backoff := c.calculateBackoff(attempt)
			log.Warnf("等待 %v 后进行第%d次重试", backoff, attempt+1)
			select {
			case <-time.After(backoff):
			case <-attemptCtx.Done():
				status = "error"
				statusCode = strconv.Itoa(code.ErrUnknown)
				finalDetails["context_cancelled"] = true
				return fmt.Errorf("重试期间上下文取消: %v", attemptCtx.Err())
			}
		}
	}

	log.Errorf("消息处理重试次数用尽: %v", lastErr)
	trace.AddRequestTag(retryCtx, "retry_outcome", "exhausted")
	status = "degraded"
	if lastErr != nil {
		if c := errors.GetCode(lastErr); c != 0 {
			statusCode = strconv.Itoa(c)
		} else {
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	finalDetails["exhausted_retries"] = true
	retryErr := c.sendToRetry(retryCtx, msg, fmt.Sprintf("重试次数用尽: %v", lastErr))
	if retryErr != nil {
		status = "error"
		statusCode = strconv.Itoa(code.ErrUnknown)
		finalDetails["retry_publish_error"] = retryErr.Error()
		return fmt.Errorf("发送重试主题失败: %v (原错误: %v)", retryErr, lastErr)
	}

	finalDetails["forwarded_to_retry"] = true
	return nil
}

// calculateBackoff 计算指数退避延迟时间
func (c *UserConsumer) calculateBackoff(attempt int) time.Duration {
	maxBackoff := 30 * time.Second
	minBackoff := 1 * time.Second

	// 指数退避公式：base * 2^(attempt-1)
	backoff := minBackoff * time.Duration(1<<uint(attempt-1))

	// 限制最大延迟
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

// 记录消费信息
func (c *UserConsumer) recordConsumerMetrics(operation, messageKey string, processStart time.Time, processingErr error, workerID int) {
	processingDuration := time.Since(processStart).Seconds()

	// 添加详细的处理时间日志
	if processingErr != nil {
		log.Errorf("Worker %d 业务处理失败: topic=%s, key=%s, operation=%s, 处理耗时=%.3fs, 错误=%v",
			workerID, c.topic, messageKey, operation, processingDuration, processingErr)
	}

	// 记录消息接收（无论成功失败）
	if operation != "" {
		metrics.ConsumerMessagesReceived.WithLabelValues(c.topic, c.groupID, operation).Inc()
	}

	// 如果有错误，记录错误指标
	if processingErr != nil {
		if operation != "" {
			errorType := getErrorType(processingErr)
			metrics.ConsumerProcessingErrors.WithLabelValues(c.topic, c.groupID, operation, errorType).Inc()
			metrics.ConsumerProcessingTime.WithLabelValues(c.topic, c.groupID, operation, "error").Observe(processingDuration)
		}
		return
	}

	// 记录成功处理
	if operation != "" {
		metrics.ConsumerMessagesProcessed.WithLabelValues(c.topic, c.groupID, operation).Inc()
		metrics.ConsumerProcessingTime.WithLabelValues(c.topic, c.groupID, operation, "success").Observe(processingDuration)
	}
}

// 添加错误类型提取函数
func getErrorType(err error) string {
	if err == nil {
		return "none"
	}
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "UNMARSHAL_ERROR"):
		return "unmarshal_error"
	case strings.Contains(errStr, "数据库"):
		return "database_error"
	case strings.Contains(errStr, "缓存"):
		return "cache_error"
	case strings.Contains(errStr, "context deadline exceeded"):
		return "timeout"
	default:
		return "unknown_error"
	}
}

func (c *UserConsumer) getOperationFromHeaders(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == HeaderOperation {
			return string(header.Value)
		}
	}
	return OperationCreate
}

func (c *UserConsumer) getTraceIDFromHeaders(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == HeaderTraceID {
			return string(header.Value)
		}
	}
	return ""
}

func (c *UserConsumer) getHeaderValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c *UserConsumer) startAsyncTraceContext(parentCtx context.Context, msg kafka.Message, operation string, workerID int) (context.Context, *trace.Span) {
	traceID := c.getTraceIDFromHeaders(msg.Headers)
	if traceID == "" {
		traceID = trace.TraceIDFromContext(parentCtx)
	}
	if traceID == "" {
		traceID = fmt.Sprintf("generated-%d", time.Now().UnixNano())
	}
	opName := operation
	if strings.TrimSpace(opName) == "" {
		opName = "unknown"
	}
	_, asyncCtx := trace.NewDetached(trace.Options{
		TraceID:         traceID,
		Service:         "iam-apiserver",
		Component:       "user-consumer",
		Operation:       fmt.Sprintf("%s_async", opName),
		RequestID:       traceID,
		Path:            c.topic,
		Method:          "KAFKA",
		Now:             time.Now(),
		DisableLogging:  true,
		ForceLogOnError: true,
	})

	trace.AddRequestTag(asyncCtx, "topic", c.topic)
	trace.AddRequestTag(asyncCtx, "group", c.groupID)
	trace.AddRequestTag(asyncCtx, "partition", msg.Partition)
	trace.AddRequestTag(asyncCtx, "offset", msg.Offset)
	trace.AddRequestTag(asyncCtx, "worker_id", workerID)
	trace.AddRequestTag(asyncCtx, "operation", opName)
	if len(msg.Key) > 0 {
		trace.AddRequestTag(asyncCtx, "message_key", string(msg.Key))
	}
	if attemptHeader := c.getHeaderValue(msg.Headers, HeaderRetryCount); attemptHeader != "" {
		trace.AddRequestTag(asyncCtx, "retry_count", attemptHeader)
	}

	spanName := fmt.Sprintf("process_%s", opName)
	spanCtx, span := trace.StartSpan(asyncCtx, "kafka-consumer", spanName)
	return spanCtx, span
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// 第一层：明确不可重试的错误
	if isUnrecoverableError(errStr) {
		return false
	}

	// 第二层：明确可重试的错误
	if isRecoverableError(errStr) {
		return true
	}

	// 第三层：默认情况
	return false
}

// isUnrecoverableError 判断是否为不可恢复的错误
func isUnrecoverableError(errStr string) bool {
	unrecoverableErrors := []string{
		// 数据重复错误
		"Duplicate entry", "1062", "23000", "duplicate key value", "23505",
		"用户已存在", "UserAlreadyExist",

		// 消息格式错误
		"UNMARSHAL_ERROR", "invalid json", "unknown operation", "poison message",

		// 权限和DEFINER错误
		"definer", "DEFINER", "1449", "permission denied",

		// 数据不存在错误（幂等性）
		"does not exist", "not found", "record not found", "ErrRecordNotFound",

		// 数据库约束错误
		"constraint", "foreign key", "1451", "1452", "syntax error",

		// 字段超长错误
		"Data too long for column", "1406",

		// GORM 相关不可重试错误
		"ErrInvalidData", "ErrInvalidTransaction", "ErrNotImplemented", "ErrMissingWhereClause", "ErrPrimaryKeyRequired", "ErrModelValueRequired", "ErrUnsupportedRelation", "ErrRegistered", "ErrInvalidField", "ErrEmptySlice", "ErrDryRunModeUnsupported",

		// 业务逻辑错误
		"invalid format", "validation failed",
	}

	for _, unrecoverableErr := range unrecoverableErrors {
		if strings.Contains(errStr, unrecoverableErr) {
			return true
		}
	}
	return false
}

// isRecoverableError 判断是否为可恢复的错误
func isRecoverableError(errStr string) bool {
	recoverableErrors := []string{
		// 超时和网络错误
		"timeout", "deadline exceeded", "connection refused", "network error",
		"connection reset", "broken pipe", "no route to host",

		// 数据库临时错误
		"database is closed", "deadlock", "1213", "40001",
		"temporary", "busy", "lock", "try again",

		// 资源暂时不可用
		"resource temporarily unavailable", "too many connections",

		// GORM 可重试错误
		"ErrInvalidTransaction", "ErrDryRunModeUnsupported",
	}

	for _, recoverableErr := range recoverableErrors {
		if strings.Contains(errStr, recoverableErr) {
			return true
		}
	}
	return false
}

func (c *UserConsumer) setUserCache(ctx context.Context, user *v1.User, previous *v1.User) error {
	cacheCtx, span := trace.StartSpan(ctx, "kafka-consumer", "set_user_cache")
	trace.AddRequestTag(cacheCtx, "username", user.Name)
	ctx = cacheCtx
	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	pipelineCount := 0
	contactChanged := false
	var (
		writeStart      time.Time
		operationErr    error
		wroteCache      bool
		prepareDuration time.Duration
		writeDuration   time.Duration
	)
	totalStart := time.Now()
	defer func() {
		details := map[string]any{
			"username":        user.Name,
			"pipeline_items":  pipelineCount,
			"contact_changed": contactChanged,
			"wrote_cache":     wroteCache,
		}
		if operationErr != nil {
			details["error"] = operationErr.Error()
			status = "degraded"
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
		trace.EndSpan(span, status, statusCode, details)
	}()
	defer func() {
		if wroteCache {
			observed := writeDuration
			if observed <= 0 && !writeStart.IsZero() {
				observed = time.Since(writeStart)
			}
			metrics.RecordRedisOperation("set", observed.Seconds(), operationErr)
		}
	}()

	c.clearNegativeCache(ctx, user.Name)

	needCacheWrite := true
	if previous != nil && cacheEquivalent(previous, user) {
		needCacheWrite = false
	}

	var pipelineItems []storage.KeyValueTTL
	if needCacheWrite {
		cacheKey := usercache.UserKey(user.Name)
		if cacheKey != "" {
			data, err := usercache.Marshal(user)
			if err != nil {
				operationErr = err
				return err
			}
			pipelineItems = append(pipelineItems, storage.KeyValueTTL{Key: cacheKey, Value: string(data), TTL: 24 * time.Hour})
		}
	}

	prevEmail := ""
	prevPhone := ""
	if previous != nil {
		prevEmail = usercache.NormalizeEmail(previous.Email)
		prevPhone = usercache.NormalizePhone(previous.Phone)
	}
	newEmail := usercache.NormalizeEmail(user.Email)
	newPhone := usercache.NormalizePhone(user.Phone)
	contactChanged = previous == nil || prevEmail != newEmail || prevPhone != newPhone

	if contactChanged {
		if previous != nil {
			c.evictContactCaches(ctx, previous, user)
		}
		pipelineItems = append(pipelineItems, buildContactCacheItems(user)...)
	}

	prepareDuration = time.Since(totalStart)
	pipelineCount = len(pipelineItems)
	if len(pipelineItems) > 0 {
		writeStart = time.Now()
		wroteCache = true
		if len(pipelineItems) == 1 {
			item := pipelineItems[0]
			operationErr = c.redis.SetKey(ctx, item.Key, item.Value, item.TTL)
		} else {
			operationErr = c.redis.BatchSet(ctx, pipelineItems)
		}
		writeDuration = time.Since(writeStart)
		if operationErr != nil {
			metrics.BusinessFailures.WithLabelValues("consumer", "set_user_cache", "redis_write_error").Inc()
			return operationErr
		}
	}
	totalDuration := time.Since(totalStart)
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "set_user_cache_prepare").Observe(prepareDuration.Seconds())
	if wroteCache {
		metrics.BusinessProcessingTime.WithLabelValues("consumer", "set_user_cache_write").Observe(writeDuration.Seconds())
	}
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "set_user_cache_total").Observe(totalDuration.Seconds())
	trace.AddRequestTag(ctx, "cache_set_ms", totalDuration.Milliseconds())
	trace.AddRequestTag(ctx, "cache_prepare_ms", prepareDuration.Milliseconds())
	if wroteCache {
		trace.AddRequestTag(ctx, "cache_write_ms", writeDuration.Milliseconds())
	}
	trace.AddRequestTag(ctx, "cache_pipeline_items", len(pipelineItems))
	trace.AddRequestTag(ctx, "cache_contacts_changed", contactChanged)
	if needCacheWrite || contactChanged {
		log.Infow("用户缓存刷新完成", "username", user.Name, "duration", totalDuration, "prepare_duration", prepareDuration, "write_duration", writeDuration, "cache_write", needCacheWrite, "contacts_updated", contactChanged, "pipeline_items", len(pipelineItems))
	} else {
		log.Debugw("用户缓存刷新跳过写入", "username", user.Name, "duration", totalDuration)
	}
	metrics.BusinessSuccess.WithLabelValues("consumer", "set_user_cache", "complete").Inc()

	return operationErr
}

func (c *UserConsumer) deleteUserCache(ctx context.Context, username string) error {
	cacheKey := usercache.UserKey(username)
	if cacheKey == "" {
		return nil
	}
	if _, err := c.redis.DeleteKey(ctx, cacheKey); err != nil {
		return err
	}

	return nil
}

func cacheEquivalent(a, b *v1.User) bool {
	if a == nil || b == nil {
		return false
	}
	if a.ObjectMeta.ID != b.ObjectMeta.ID || a.ObjectMeta.InstanceID != b.ObjectMeta.InstanceID || a.ObjectMeta.Name != b.ObjectMeta.Name {
		return false
	}
	if !a.ObjectMeta.CreatedAt.Equal(b.ObjectMeta.CreatedAt) || !a.ObjectMeta.UpdatedAt.Equal(b.ObjectMeta.UpdatedAt) {
		return false
	}
	if a.Nickname != b.Nickname || a.Password != b.Password || a.Status != b.Status || a.IsAdmin != b.IsAdmin {
		return false
	}
	if usercache.NormalizeEmail(a.Email) != usercache.NormalizeEmail(b.Email) {
		return false
	}
	if usercache.NormalizePhone(a.Phone) != usercache.NormalizePhone(b.Phone) {
		return false
	}
	if strings.TrimSpace(a.ExtendShadow) != strings.TrimSpace(b.ExtendShadow) {
		return false
	}
	return true
}

func (c *UserConsumer) clearNegativeCache(ctx context.Context, username string) {
	if username == "" {
		return
	}
	cacheKey := usercache.UserKey(username)
	if cacheKey == "" {
		return
	}
	value, err := c.redis.GetKey(ctx, cacheKey)
	if err != nil {
		if err != redis.Nil {
			log.Warnf("负缓存校验失败: key=%s err=%v", cacheKey, err)
		}
		return
	}
	if value != cacheNullSentinel {
		return
	}
	if _, err := c.redis.DeleteKey(ctx, cacheKey); err != nil {
		log.Warnf("负缓存清理失败: key=%s err=%v", cacheKey, err)
		return
	}

}

func (c *UserConsumer) purgeUserState(ctx context.Context, username string, userID uint64, snapshot *v1.User) {
	spanCtx, span := trace.StartSpan(ctx, "kafka-consumer", "purge_user_state")
	trace.AddRequestTag(spanCtx, "username", username)
	if userID != 0 {
		trace.AddRequestTag(spanCtx, "user_id", userID)
	}
	ctx = spanCtx

	cacheError := false
	sessionError := false
	defer func() {
		details := map[string]any{
			"username":      username,
			"has_snapshot":  snapshot != nil,
			"user_id":       userID,
			"cache_error":   cacheError,
			"session_error": sessionError,
		}
		status := "success"
		statusCode := strconv.Itoa(code.ErrSuccess)
		if cacheError || sessionError {
			status = "degraded"
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
		trace.EndSpan(span, status, statusCode, details)
	}()

	if strings.TrimSpace(username) != "" {
		if _, err := c.clearPendingCreateMarker(ctx, username); err != nil {
			cacheError = true
			log.Warnw("删除流程清理pending标记失败", "username", username, "error", err)
		}
	}

	if err := c.deleteUserCache(ctx, username); err != nil {
		cacheError = true
		log.Errorw("缓存删除失败", "username", username, "error", err)
	}

	if snapshot != nil {
		c.evictContactCaches(ctx, snapshot, nil)
	}

	if userID == 0 {
		return
	}

	if err := cleanupUserSessions(ctx, c.redis, userID); err != nil {
		sessionError = true
		log.Errorw("刷新令牌清理失败", "username", username, "userID", userID, "error", err)
		return
	}

}
func buildPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	var sb strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
	return sb.String()
}

func (c *UserConsumer) evictContactCaches(ctx context.Context, previous *v1.User, current *v1.User) {
	if previous == nil {
		return
	}
	prevEmail := usercache.NormalizeEmail(previous.Email)
	curEmail := ""
	if current != nil {
		curEmail = usercache.NormalizeEmail(current.Email)
	}
	var keysToEvict []string
	if prevEmail != "" && prevEmail != curEmail {
		keysToEvict = append(keysToEvict, usercache.EmailKey(previous.Email))
	}

	prevPhone := usercache.NormalizePhone(previous.Phone)
	curPhone := ""
	if current != nil {
		curPhone = usercache.NormalizePhone(current.Phone)
	}
	if prevPhone != "" && prevPhone != curPhone {
		keysToEvict = append(keysToEvict, usercache.PhoneKey(previous.Phone))
	}

	switch len(keysToEvict) {
	case 0:
		return
	case 1:
		c.removeCacheKey(ctx, keysToEvict[0])
	default:
		if err := c.redis.BatchDelete(ctx, keysToEvict); err != nil {
			log.Warnw("批量删除联系缓存失败，回退逐个删除", "keys", keysToEvict, "error", err)
			for _, key := range keysToEvict {
				c.removeCacheKey(ctx, key)
			}
		}
	}
}

func (c *UserConsumer) writeContactCaches(ctx context.Context, user *v1.User) {
	if user == nil {
		return
	}
	items := buildContactCacheItems(user)
	if len(items) == 0 {
		return
	}
	if err := c.redis.BatchSet(ctx, items); err != nil {
		log.Warnf("批量写入联系缓存失败: username=%s err=%v", user.Name, err)
	}
}

func (c *UserConsumer) removeCacheKey(ctx context.Context, cacheKey string) {
	if cacheKey == "" {
		return
	}
	if _, err := c.redis.DeleteKey(ctx, cacheKey); err != nil {
		log.Warnf("缓存删除失败: key=%s err=%v", cacheKey, err)
	}
}

// 发送到重试主题
func (c *UserConsumer) sendToRetry(ctx context.Context, msg kafka.Message, errorInfo string) error {

	operation := c.getOperationFromHeaders(msg.Headers)

	errorType := getErrorType(fmt.Errorf("%s", errorInfo))
	// 记录重试指标
	metrics.ConsumerRetryMessages.WithLabelValues(c.topic, c.groupID, operation, errorType).Inc()

	if c.producer == nil {
		return fmt.Errorf("producer未初始化")
	}

	// ✅ 确保这里传递原始消息的Headers
	retryMsg := kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: msg.Headers, // 直接使用原始Headers
		Time:    time.Now(),
	}

	retryMsg.Headers = append(retryMsg.Headers, kafka.Header{
		Key:   HeaderRetryError,
		Value: []byte(errorInfo),
	})

	return c.producer.sendToRetryTopic(ctx, retryMsg, errorInfo)
}

func (c *UserConsumer) sendToDeadLetter(ctx context.Context, msg kafka.Message, reason string) error {
	operation := c.getOperationFromHeaders(msg.Headers)
	errorType := getErrorType(fmt.Errorf("%s", reason))
	// 记录死信指标
	metrics.ConsumerDeadLetterMessages.WithLabelValues(c.topic, c.groupID, operation, errorType).Inc()
	if c.producer == nil {
		return fmt.Errorf("producer未初始化")
	}
	return c.producer.SendToDeadLetterTopic(ctx, msg, reason)
}

// batchCreateToDB 使用 GORM 批量创建用户实体
func (c *UserConsumer) batchCreateToDB(ctx context.Context, msgs []kafka.Message) {

	if len(msgs) == 0 {
		return
	}
	batchCtx, span := trace.StartSpan(ctx, "kafka-consumer", "batch_create_db")
	trace.AddRequestTag(batchCtx, "batch_size", len(msgs))
	trace.AddRequestTag(batchCtx, "topic", c.topic)
	ctx = batchCtx

	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	successful := 0
	var opErr error
	defer func() {
		details := map[string]any{
			"batch_size":    len(msgs),
			"success_count": successful,
		}
		if opErr != nil {
			details["error"] = opErr.Error()
		}
		trace.EndSpan(span, status, statusCode, details)
	}()

	start := time.Now()
	metrics.BusinessOperationsTotal.WithLabelValues("consumer", "batch_create", "kafka").Inc()
	metrics.BusinessInProgress.WithLabelValues("consumer", "batch_create").Inc()
	defer metrics.BusinessInProgress.WithLabelValues("consumer", "batch_create").Dec()
	var (
		users     []v1.User
		validMsgs []kafka.Message
	)
	for _, m := range msgs {
		var u v1.User
		if err := decodeUserMessage(m.Value, &u); err != nil {
			log.Errorf("批量创建: 反序列化失败: %v", err)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, m, "BATCH_UNMARSHAL_ERROR: "+err.Error())
			}
			continue
		}
		if err := validation.ValidateUserFields(u.Name, u.Nickname, u.Password, u.Email, u.Phone); err != nil {
			log.Errorf("批量创建: %v", err)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, m, err.Error())
			}
			continue
		}
		u.Email = usercache.NormalizeEmail(u.Email)
		u.Phone = usercache.NormalizePhone(u.Phone)
		now := time.Now()
		u.CreatedAt = now
		u.UpdatedAt = now
		ensureUserInstanceID(&u)
		users = append(users, u)
		validMsgs = append(validMsgs, m)
	}

	if len(users) == 0 {
		return
	}
	for i := range users {

		created, err := c.createUserInDB(ctx, &users[i], false)
		if err != nil {
			opErr = err
			errorType := getErrorType(err)
			log.Errorf("[批量插入] 单条失败: username=%s err=%v", users[i].Name, err)
			metrics.BusinessFailures.WithLabelValues("consumer", "batch_create", errorType).Inc()
			if c.producer != nil {
				_ = c.producer.sendToRetryTopic(ctx, validMsgs[i], "BATCH_CREATE_DB_ERROR: "+err.Error())
			}
			continue
		}
		if created {
			successful++
			if err := c.setUserCache(ctx, &users[i], nil); err != nil {
				log.Warnf("批量创建后缓存设置失败: username=%s, error=%v", users[i].Name, err)
			}
		} else {
			log.Warnf("检测到批量创建中的重复用户，已忽略: username=%s", users[i].Name)
		}
	}
	if successful > 0 {
		metrics.BusinessSuccess.WithLabelValues("consumer", "batch_create", "success").Inc()

	}
	duration := time.Since(start).Seconds()
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "batch_create").Observe(duration)
	metrics.BusinessThroughputStats.WithLabelValues("consumer", "batch_create").Observe(duration)
	if opErr != nil {
		errorRate := 1.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_create").Set(errorRate)
	} else {
		errorRate := 0.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_create").Set(errorRate)
	}
	if opErr != nil {
		status = "degraded"
		if c := errors.GetCode(opErr); c != 0 {
			statusCode = strconv.Itoa(c)
		} else {
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
	}
}

// batchDeleteFromDB 批量删除用户（按 username）
func (c *UserConsumer) batchDeleteFromDB(ctx context.Context, msgs []kafka.Message) {

	if len(msgs) == 0 {
		return
	}
	batchCtx, span := trace.StartSpan(ctx, "kafka-consumer", "batch_delete_db")
	trace.AddRequestTag(batchCtx, "batch_size", len(msgs))
	trace.AddRequestTag(batchCtx, "topic", c.topic)
	ctx = batchCtx

	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	usernamesCount := 0
	cleanedCount := 0
	var opErr error
	defer func() {
		details := map[string]any{
			"batch_size":     len(msgs),
			"usernames":      usernamesCount,
			"cleaned_states": cleanedCount,
		}
		if opErr != nil {
			details["error"] = opErr.Error()
		}
		trace.EndSpan(span, status, statusCode, details)
	}()
	start := time.Now()
	metrics.BusinessOperationsTotal.WithLabelValues("consumer", "batch_delete", "kafka").Inc()
	metrics.BusinessInProgress.WithLabelValues("consumer", "batch_delete").Inc()
	defer metrics.BusinessInProgress.WithLabelValues("consumer", "batch_delete").Dec()
	var usernames []string
	cleanupTargets := make(map[string]uint64)
	snapshots := make(map[string]*v1.User)
	snapshotStorage := make([]v1.User, 0, len(usernames))
	for _, m := range msgs {
		deleteRequest := deleteMessagePool.Get().(*deleteMessage)
		if err := jsonCodec.Unmarshal(m.Value, deleteRequest); err != nil {
			log.Errorf("批量删除: 反序列化失败: %v", err)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, m, "BATCH_UNMARSHAL_ERROR: "+err.Error())
			}
			deleteRequest.Username = ""
			deleteRequest.DeletedAt = ""
			deleteMessagePool.Put(deleteRequest)
			continue
		}
		usernames = append(usernames, deleteRequest.Username)
		deleteRequest.Username = ""
		deleteRequest.DeletedAt = ""
		deleteMessagePool.Put(deleteRequest)
	}
	if len(usernames) == 0 {
		return
	}
	usernamesCount = len(usernames)
	db, err := c.ensureSQLX()
	if err != nil {
		log.Errorf("批量删除获取数据库连接失败: %v", err)
		metrics.BusinessFailures.WithLabelValues("consumer", "batch_delete", getErrorType(err)).Inc()
		return
	}

	if len(usernames) > 0 {
		placeholder := buildPlaceholders(len(usernames))
		args := make([]interface{}, len(usernames))
		for i := range usernames {
			args[i] = usernames[i]
		}

		query := fmt.Sprintf("SELECT id, name, email, phone FROM `user` WHERE name IN (%s)", placeholder)
		rows, queryErr := db.QueryContext(ctx, query, args...)
		if queryErr != nil {
			log.Warnf("批量删除前查询用户ID失败: %v", queryErr)
		} else {
			defer rows.Close()
			for rows.Next() {
				id, name, email, phone, scanErr := dbscan.ScanUserContact(rows)
				if scanErr != nil {
					log.Warnf("批量删除扫描行失败: %v", scanErr)
					continue
				}
				cleanupTargets[name] = id
				if email != "" || phone != "" {
					snapshotStorage = append(snapshotStorage, v1.User{})
					snapshot := &snapshotStorage[len(snapshotStorage)-1]
					snapshot.Email = email
					snapshot.Phone = phone
					snapshots[name] = snapshot
				}
			}
			if err := rows.Err(); err != nil {
				log.Warnf("批量删除Rows错误: %v", err)
			}
		}

		deleteSQL := fmt.Sprintf("DELETE FROM `user` WHERE name IN (%s)", placeholder)
		res, execErr := db.ExecContext(ctx, deleteSQL, args...)
		if execErr != nil {
			opErr = execErr
			log.Errorf("批量删除用户失败: %v", execErr)
			metrics.BusinessFailures.WithLabelValues("consumer", "batch_delete", getErrorType(execErr)).Inc()
			for _, m := range msgs {
				if c.producer != nil {
					_ = c.producer.sendToRetryTopic(ctx, m, "BATCH_DELETE_DB_ERROR: "+execErr.Error())
				}
			}
		} else {
			metrics.BusinessSuccess.WithLabelValues("consumer", "batch_delete", "success").Inc()
			affected, affErr := res.RowsAffected()
			if affErr != nil {
				log.Warnf("批量删除获取影响行数失败: %v", affErr)
			}
			if affected == 0 {
				log.Warnf("批量删除未影响任何行")
			}
			for _, username := range usernames {
				c.purgeUserState(ctx, username, cleanupTargets[username], snapshots[username])
				cleanedCount++
			}
		}
	}
	duration := time.Since(start).Seconds()
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "batch_delete").Observe(duration)
	metrics.BusinessThroughputStats.WithLabelValues("consumer", "batch_delete").Observe(duration)
	if opErr != nil {
		errorRate := 1.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_delete").Set(errorRate)
	} else {
		errorRate := 0.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_delete").Set(errorRate)
	}
	if opErr != nil {
		status = "degraded"
		if c := errors.GetCode(opErr); c != 0 {
			statusCode = strconv.Itoa(c)
		} else {
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
	}
}

// 唯一新增的方法
func (c *UserConsumer) SetInstanceID(id int) {
	c.instanceID = id
}

func (c *UserConsumer) SetPoolStatsProvider(provider func() []db.PoolStats) {
	c.poolReporter.provider = provider
}

func (c *UserConsumer) SetProducer(producer *UserProducer) {
	c.producer = producer
}

func (c *UserConsumer) Close() error {
	var firstErr error
	for _, reader := range c.readers {
		if reader == nil {
			continue
		}
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *UserConsumer) readerForIndex(idx int) *kafka.Reader {
	if idx < 0 || idx >= len(c.readers) {
		return nil
	}
	return c.readers[idx]
}

// batchUpdateToDB 批量更新用户（按 username）
func (c *UserConsumer) batchUpdateToDB(ctx context.Context, msgs []kafka.Message) {

	if len(msgs) == 0 {
		return
	}
	batchCtx, span := trace.StartSpan(ctx, "kafka-consumer", "batch_update_db")
	trace.AddRequestTag(batchCtx, "batch_size", len(msgs))
	trace.AddRequestTag(batchCtx, "topic", c.topic)
	ctx = batchCtx

	status := "success"
	statusCode := strconv.Itoa(code.ErrSuccess)
	processedIntents := 0
	updatedCount := 0
	var opErr error
	defer func() {
		details := map[string]any{
			"batch_size":    len(msgs),
			"processed":     processedIntents,
			"updated_count": updatedCount,
		}
		if opErr != nil {
			details["error"] = opErr.Error()
		}
		trace.EndSpan(span, status, statusCode, details)
	}()
	start := time.Now()
	metrics.BusinessOperationsTotal.WithLabelValues("consumer", "batch_update", "kafka").Inc()
	metrics.BusinessInProgress.WithLabelValues("consumer", "batch_update").Inc()
	defer metrics.BusinessInProgress.WithLabelValues("consumer", "batch_update").Dec()
	db, err := c.ensureSQLX()
	if err != nil {
		log.Errorf("批量更新获取数据库连接失败: %v", err)
		return
	}

	// 收集所有 username 并保持消息顺序
	type updateIntent struct {
		username string
		msg      kafka.Message
		user     *v1.User
	}
	intents := make([]updateIntent, 0, len(msgs))
	defer func() {
		for i := range intents {
			if intents[i].user != nil {
				*intents[i].user = v1.User{}
				userMessagePool.Put(intents[i].user)
				intents[i].user = nil
			}
		}
	}()
	uniqueNames := make(map[string]struct{}, len(msgs))
	usernames := make([]string, 0, len(msgs))
	for _, m := range msgs {
		u := userMessagePool.Get().(*v1.User)
		*u = v1.User{}
		if err := decodeUserMessage(m.Value, u); err != nil {
			log.Errorf("批量更新: 反序列化失败: %v", err)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, m, "BATCH_UNMARSHAL_ERROR: "+err.Error())
			}
			*u = v1.User{}
			userMessagePool.Put(u)
			continue
		}
		if err := validation.ValidateUserFields(u.Name, u.Nickname, u.Password, u.Email, u.Phone); err != nil {
			log.Errorf("批量更新: %v", err)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, m, err.Error())
			}
			*u = v1.User{}
			userMessagePool.Put(u)
			continue
		}
		intents = append(intents, updateIntent{username: u.Name, msg: m, user: u})
		if _, ok := uniqueNames[u.Name]; !ok {
			uniqueNames[u.Name] = struct{}{}
			usernames = append(usernames, u.Name)
		}
	}
	if len(intents) == 0 {
		return
	}

	// 批量查快照
	placeholder := buildPlaceholders(len(usernames))
	if placeholder == "" {
		log.Warn("批量更新快照: 未生成有效的占位符")
		return
	}
	args := make([]interface{}, len(usernames))
	for i := range usernames {
		args[i] = usernames[i]
	}
	query := fmt.Sprintf("SELECT id, instanceID, name, nickname, password, email, phone, status, isAdmin, createdAt, updatedAt, loginedAt, version FROM `user` WHERE name IN (%s)", placeholder)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Errorf("批量更新快照查询失败: %v", err)
		return
	}
	defer rows.Close()
	snapshotMap := make(map[string]*v1.User, len(usernames))
	snapshotStorage := make([]v1.User, 0, len(usernames))
	for rows.Next() {
		snapshotStorage = append(snapshotStorage, v1.User{})
		record := &snapshotStorage[len(snapshotStorage)-1]
		if _, scanErr := dbscan.ScanUserAuthInto(rows, record); scanErr != nil {
			snapshotStorage = snapshotStorage[:len(snapshotStorage)-1]
			log.Errorf("批量更新快照Scan失败: %v", scanErr)
			continue
		}
		snapshotMap[record.ObjectMeta.Name] = record
	}
	if err := rows.Err(); err != nil {
		log.Errorf("批量更新快照Rows错误: %v", err)
	}

	updateSQL := "UPDATE `user` SET email = ?, password = ?, status = ?, updatedAt = ?, extendShadow = ?, nickname = ?, phone = ? WHERE name = ?"
	for i := range intents {
		intent := &intents[i]
		processedIntents++
		u := intent.user
		existingSnapshot := snapshotMap[intent.username]
		if existingSnapshot == nil {
			log.Warnf("批量更新目标不存在: %s", intent.username)
			if c.producer != nil {
				_ = c.producer.SendToDeadLetterTopic(ctx, intent.msg, "BATCH_UPDATE_TARGET_NOT_FOUND: "+intent.username)
			}
			continue
		}
		u.Email = usercache.NormalizeEmail(u.Email)
		u.Phone = usercache.NormalizePhone(u.Phone)
		u.UpdatedAt = time.Now()
		if strings.TrimSpace(u.ExtendShadow) == "" {
			u.ExtendShadow = u.Extend.String()
		}
		var phoneValue interface{}
		if trimmed := strings.TrimSpace(u.Phone); trimmed != "" {
			phoneValue = trimmed
		}
		if _, execErr := db.ExecContext(ctx, updateSQL,
			u.Email,
			u.Password,
			u.Status,
			u.UpdatedAt,
			u.ExtendShadow,
			u.Nickname,
			phoneValue,
			u.Name,
		); execErr != nil {
			opErr = execErr
			log.Errorf("批量更新失败: %v, 用户: %s", execErr, u.Name)
			metrics.BusinessFailures.WithLabelValues("consumer", "batch_update", getErrorType(execErr)).Inc()
			if c.producer != nil {
				_ = c.producer.sendToRetryTopic(ctx, intent.msg, "BATCH_UPDATE_DB_ERROR: "+execErr.Error())
			}
			continue
		}
		updatedCount++
		metrics.BusinessSuccess.WithLabelValues("consumer", "batch_update", "success").Inc()
		_ = c.setUserCache(ctx, u, existingSnapshot)
	}
	duration := time.Since(start).Seconds()
	metrics.BusinessProcessingTime.WithLabelValues("consumer", "batch_update").Observe(duration)
	metrics.BusinessThroughputStats.WithLabelValues("consumer", "batch_update").Observe(duration)
	if opErr != nil {
		errorRate := 1.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_update").Set(errorRate)
	} else {
		errorRate := 0.0
		metrics.BusinessErrorRate.WithLabelValues("consumer", "batch_update").Set(errorRate)
	}
	if opErr != nil {
		status = "degraded"
		if c := errors.GetCode(opErr); c != 0 {
			statusCode = strconv.Itoa(c)
		} else {
			statusCode = strconv.Itoa(code.ErrUnknown)
		}
	}
}
