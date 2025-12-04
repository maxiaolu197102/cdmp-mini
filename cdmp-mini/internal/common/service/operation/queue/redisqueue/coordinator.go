package redisqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/idutil"
)

type Config struct {
	Redis               *storage.RedisCluster
	KeyPrefix           string
	TicketTTL           time.Duration
	PayloadTTL          time.Duration
	Clock               func() time.Time
	Resource            string
	MaxInflightDuration time.Duration
	ReclaimBatchSize    int
}

type Coordinator struct {
	redis             *storage.RedisCluster
	keyPrefix         string
	ticketTTL         time.Duration
	payloadTTL        time.Duration
	now               func() time.Time
	resource          string
	metricsMu         sync.Mutex
	metricsLast       time.Time
	metricsFail       int
	metricsHold       time.Time
	reclaimMu         sync.Mutex
	reclaimLast       time.Time
	reclaimMinGap     time.Duration
	reclaimWorkerOnce sync.Once
	reclaimWake       chan struct{}
	reclaimRequested  atomic.Bool
	maxInflightAge    time.Duration
	reclaimBatch      int
}

const (
	defaultTicketTTL      = 24 * time.Hour
	defaultPayloadTTL     = 48 * time.Hour
	defaultMaxInflightAge = 2 * time.Minute
	defaultReclaimBatch   = 32
	minMaxInflightAge     = 10 * time.Second
	minReclaimBatch       = 1
	maxReclaimBatch       = 32
)

const (
	queueMetricsMinInterval = 250 * time.Millisecond
	queueMetricsEvalTimeout = 750 * time.Millisecond
	queueMetricsBackoffBase = 2 * time.Second
	queueMetricsBackoffMax  = 30 * time.Second
	queueReclaimMinInterval = 2 * time.Second
	reclaimEvalTimeout      = 1 * time.Second
)

const (
	luaReleaseDue = `
local moved = 0
while true do
	local entry = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
	if entry == nil or #entry == 0 then
		return moved
	end
	local score = tonumber(entry[2])
	if score == nil or score > tonumber(ARGV[1]) then
		return moved
	end
	local id = entry[1]
	redis.call('ZREM', KEYS[1], id)
	if redis.call('SADD', KEYS[3], id) == 1 then
		redis.call('RPUSH', KEYS[2], id)
		moved = moved + 1
	end
end
`
	luaPopReady = `
while true do
	local id = redis.call('LPOP', KEYS[1])
	if not id then
		return nil
	end
	if redis.call('SREM', KEYS[4], id) == 1 then
		redis.call('SADD', KEYS[2], id)
		local score = tonumber(ARGV[1])
		if not score then
			score = 0
		end
		redis.call('ZADD', KEYS[3], score, id)
		return id
	end
end
`
	luaPushReady = `
if redis.call('SADD', KEYS[2], ARGV[1]) == 0 then
	return redis.call('LLEN', KEYS[1])
end
return redis.call('RPUSH', KEYS[1], ARGV[1])
`
	luaRequeueReady = `
local id = ARGV[1]
redis.call('SREM', KEYS[2], id)
redis.call('ZREM', KEYS[3], id)
redis.call('ZREM', KEYS[5], id)
redis.call('SREM', KEYS[4], id)
if redis.call('SADD', KEYS[4], id) == 0 then
	return redis.call('LLEN', KEYS[1])
end
return redis.call('RPUSH', KEYS[1], id)
`
	luaRequeueScheduled = `
local id = ARGV[1]
local score = tonumber(ARGV[2])
redis.call('SREM', KEYS[2], id)
redis.call('ZREM', KEYS[3], id)
redis.call('SREM', KEYS[4], id)
return redis.call('ZADD', KEYS[5], score, id)
`
	luaAckOperation = `
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('SREM', KEYS[5], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`
	luaCancelOperation = luaAckOperation
	luaListPosition    = `
local idx = redis.call('LPOS', KEYS[1], ARGV[1])
if not idx then
    return -1
end
return idx
`
	luaIsInflight     = `return redis.call('SISMEMBER', KEYS[1], ARGV[1])`
	luaScheduledScore = `return redis.call('ZSCORE', KEYS[1], ARGV[1])`
	luaQueueSnapshot  = `
local ready = redis.call('LLEN', KEYS[1])
local scheduled = redis.call('ZCARD', KEYS[2])
local inflight = redis.call('SCARD', KEYS[3])
return {ready, scheduled, inflight}
`
	luaReclaimStaleInflight = `
	local reclaimed = 0
	local now = tonumber(ARGV[1])
	local max_age = tonumber(ARGV[2])
	local limit = tonumber(ARGV[3])
	if not now or not max_age or max_age <= 0 or not limit or limit <= 0 then
		return 0
	end
	local cutoff = now - max_age
	local ids = {}
	local seen = {}
	local aged = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff, 'LIMIT', 0, limit)
	for _, id in ipairs(aged) do
		if not seen[id] then
			table.insert(ids, id)
			seen[id] = true
			if #ids >= limit then
				break
			end
		end
	end
	if #ids < limit then
		local needed = limit - #ids
		if needed > 0 then
			local samples = redis.call('SRANDMEMBER', KEYS[2], needed)
			if type(samples) == 'string' then
				samples = {samples}
			end
			if samples and #samples > 0 then
				for _, id in ipairs(samples) do
					if id and not seen[id] then
						local score = redis.call('ZSCORE', KEYS[1], id)
						if not score then
							table.insert(ids, id)
							seen[id] = true
							if #ids >= limit then
								break
							end
						end
					end
				end
			end
		end
	end
	for _, id in ipairs(ids) do
		redis.call('ZREM', KEYS[1], id)
		if redis.call('SREM', KEYS[2], id) > 0 then
			if redis.call('SADD', KEYS[4], id) == 1 then
				redis.call('RPUSH', KEYS[3], id)
			end
			reclaimed = reclaimed + 1
		end
	end
	return reclaimed
	`
)

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Redis == nil {
		return nil, fmt.Errorf("redis client is required")
	}

	prefix := strings.Trim(cfg.KeyPrefix, ":")
	if prefix == "" {
		prefix = "operation"
	}

	ticketTTL := cfg.TicketTTL
	if ticketTTL <= 0 {
		ticketTTL = defaultTicketTTL
	}

	payloadTTL := cfg.PayloadTTL
	if payloadTTL <= 0 {
		payloadTTL = defaultPayloadTTL
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	resource := strings.TrimSpace(cfg.Resource)
	if resource == "" {
		resource = prefix
	}

	maxInflightAge := cfg.MaxInflightDuration
	if maxInflightAge <= 0 {
		maxInflightAge = defaultMaxInflightAge
	}
	if maxInflightAge < minMaxInflightAge {
		maxInflightAge = minMaxInflightAge
	}

	reclaimBatch := cfg.ReclaimBatchSize
	if reclaimBatch < minReclaimBatch {
		reclaimBatch = defaultReclaimBatch
	}
	if reclaimBatch > maxReclaimBatch {
		reclaimBatch = maxReclaimBatch
	}

	return &Coordinator{
		redis:          cfg.Redis,
		keyPrefix:      prefix,
		ticketTTL:      ticketTTL,
		payloadTTL:     payloadTTL,
		now:            clock,
		resource:       resource,
		reclaimMinGap:  queueReclaimMinInterval,
		maxInflightAge: maxInflightAge,
		reclaimBatch:   reclaimBatch,
	}, nil
}

func (c *Coordinator) Enqueue(ctx context.Context, env *operation.OperationEnvelope) (*operation.QueueTicket, error) {
	if env == nil {
		return nil, fmt.Errorf("operation envelope is required")
	}
	if env.ID == "" {
		return nil, fmt.Errorf("operation id is required")
	}

	if err := c.persistEnvelope(ctx, env); err != nil {
		return nil, err
	}
	if err := c.setAttempts(ctx, env.ID, 0); err != nil {
		return nil, err
	}

	length, err := c.pushReady(ctx, env.ID)
	if err != nil {
		return nil, err
	}

	ticketID := idutil.GetUUID36("")
	if err := c.redis.SetKey(ctx, c.ticketKey(ticketID), env.ID, c.ticketTTL); err != nil {
		return nil, err
	}

	issuedAt := c.now()
	ticket := &operation.QueueTicket{
		TicketID:      ticketID,
		OperationID:   env.ID,
		QueuePosition: max64(length-1, 0),
		EstimatedWait: 0,
		IssuedAt:      issuedAt,
	}
	c.recordQueueMetrics()
	return ticket, nil
}

func (c *Coordinator) Poll(ctx context.Context, ticketID string) (*operation.QueueStatus, error) {
	if ticketID == "" {
		return nil, fmt.Errorf("ticket id is required")
	}

	operationID, err := c.redis.GetKey(ctx, c.ticketKey(ticketID))
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("ticket %s not found", ticketID)
		}
		return nil, err
	}

	position, err := c.listPosition(ctx, operationID)
	if err != nil {
		return nil, err
	}

	inflight, err := c.isInflight(ctx, operationID)
	if err != nil {
		return nil, err
	}

	remaining := time.Duration(0)
	state := operation.StateQueued

	if inflight {
		state = operation.StateExecuting
	} else if position < 0 {
		score, scoreFound, scoreErr := c.scheduledScore(ctx, operationID)
		if scoreErr != nil {
			return nil, scoreErr
		}
		if scoreFound {
			nowMillis := c.now().UnixMilli()
			if score > nowMillis {
				remaining = time.Duration(score-nowMillis) * time.Millisecond
			}
		} else {
			state = operation.StateCompleted
		}
	}

	return &operation.QueueStatus{
		OperationID: operationID,
		State:       state,
		Position:    position,
		Remaining:   remaining,
	}, nil
}

func (c *Coordinator) Cancel(ctx context.Context, ticketID string) error {
	if ticketID == "" {
		return fmt.Errorf("ticket id is required")
	}

	operationID, err := c.redis.GetKey(ctx, c.ticketKey(ticketID))
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return fmt.Errorf("ticket %s not found", ticketID)
		}
		return err
	}

	if _, err := c.redis.Eval(ctx, luaCancelOperation, []string{c.inflightKey(), c.readyKey(), c.scheduledKey(), c.inflightLeaseKey(), c.readyMembersKey()}, []interface{}{operationID}); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	_ = c.redis.BatchDelete(ctx, []string{
		c.ticketKey(ticketID),
		c.payloadKey(operationID),
		c.attemptKey(operationID),
	})
	c.recordQueueMetrics()
	return nil
}

func (c *Coordinator) Dequeue(ctx context.Context) (*operation.QueueItem, error) {
	if _, err := c.reclaimStaleInflight(ctx); err != nil {
		return nil, err
	}
	if err := c.releaseDue(ctx); err != nil {
		return nil, err
	}

	operationID, err := c.popReady(ctx)
	if err != nil {
		return nil, err
	}
	if operationID == "" {
		c.recordQueueMetrics()
		return nil, operation.ErrQueueEmpty
	}

	payload, err := c.redis.GetKey(ctx, c.payloadKey(operationID))
	if err != nil {
		return nil, err
	}

	var env operation.OperationEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		return nil, err
	}

	attempts, err := c.getAttempts(ctx, operationID)
	if err != nil {
		return nil, err
	}

	item := &operation.QueueItem{
		Envelope:    &env,
		Attempts:    attempts,
		AvailableAt: c.now(),
	}
	c.recordQueueMetrics()
	return item, nil
}

func (c *Coordinator) Ack(ctx context.Context, operationID string) error {
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}
	_, err := c.redis.Eval(ctx, luaAckOperation, []string{c.inflightKey(), c.readyKey(), c.scheduledKey(), c.inflightLeaseKey(), c.readyMembersKey()}, []interface{}{operationID})
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	c.recordQueueMetrics()
	return nil
}

func (c *Coordinator) Requeue(ctx context.Context, item *operation.QueueItem, delay time.Duration) error {
	if item == nil || item.Envelope == nil {
		return fmt.Errorf("queue item is nil")
	}
	operationID := item.Envelope.ID
	if operationID == "" {
		return fmt.Errorf("operation id is required")
	}

	if err := c.persistEnvelope(ctx, item.Envelope); err != nil {
		return err
	}
	if err := c.setAttempts(ctx, operationID, item.Attempts); err != nil {
		return err
	}

	targetTime := item.AvailableAt
	now := c.now()
	if delay > 0 {
		targetTime = now.Add(delay)
	} else if targetTime.IsZero() || targetTime.Before(now) {
		targetTime = now
	}

	if targetTime.After(now) {
		score := float64(targetTime.UnixMilli())
		if _, err := c.redis.Eval(ctx, luaRequeueScheduled, []string{c.readyKey(), c.inflightKey(), c.inflightLeaseKey(), c.readyMembersKey(), c.scheduledKey()}, []interface{}{operationID, score}); err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		c.recordQueueMetrics()
		return nil
	}

	if _, err := c.redis.Eval(ctx, luaRequeueReady, []string{c.readyKey(), c.inflightKey(), c.inflightLeaseKey(), c.readyMembersKey(), c.scheduledKey()}, []interface{}{operationID}); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	c.recordQueueMetrics()
	return nil
}

func (c *Coordinator) releaseDue(ctx context.Context) error {
	now := c.now().UnixMilli()
	_, err := c.redis.Eval(ctx, luaReleaseDue, []string{c.scheduledKey(), c.readyKey(), c.readyMembersKey()}, []interface{}{now})
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	c.recordQueueMetrics()
	return nil
}

func (c *Coordinator) reclaimStaleInflight(ctx context.Context) (int64, error) {
	if c.maxInflightAge <= 0 || c.reclaimBatch <= 0 {
		return 0, nil
	}

	now := c.now()
	if !c.reserveReclaimSlot(now) {
		return 0, nil
	}
	success := false
	defer func() {
		if !success {
			c.releaseReclaimSlot(now)
		}
	}()

	nowMillis := now.UnixMilli()
	ageMillis := int64(c.maxInflightAge / time.Millisecond)
	if ageMillis <= 0 {
		ageMillis = 1
	}

	result, err := c.redis.Eval(ctx, luaReclaimStaleInflight, []string{c.inflightLeaseKey(), c.inflightKey(), c.readyKey(), c.readyMembersKey()}, []interface{}{nowMillis, ageMillis, c.reclaimBatch})
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}

	reclaimed, convErr := toInt64(result)
	if convErr != nil && !errors.Is(convErr, redis.Nil) {
		return 0, convErr
	}

	if reclaimed > 0 {
		log.Infow("operation queue inflight reclaimed", "resource", c.Resource(), "count", reclaimed, "max_age_ms", ageMillis, "batch", c.reclaimBatch)
	}
	success = true

	return reclaimed, nil
}

func (c *Coordinator) reserveReclaimSlot(now time.Time) bool {
	if c == nil {
		return false
	}
	c.reclaimMu.Lock()
	defer c.reclaimMu.Unlock()
	interval := c.reclaimMinGap
	if interval <= 0 {
		c.reclaimLast = now
		return true
	}
	if !c.reclaimLast.IsZero() && now.Sub(c.reclaimLast) < interval {
		return false
	}
	c.reclaimLast = now
	return true
}

func (c *Coordinator) releaseReclaimSlot(start time.Time) {
	if c == nil {
		return
	}
	c.reclaimMu.Lock()
	if c.reclaimLast.Equal(start) {
		c.reclaimLast = time.Time{}
	}
	c.reclaimMu.Unlock()
}

func (c *Coordinator) shouldAttemptReclaim(now time.Time) bool {
	if c == nil {
		return false
	}
	c.reclaimMu.Lock()
	defer c.reclaimMu.Unlock()
	interval := c.reclaimMinGap
	if interval <= 0 {
		return true
	}
	if c.reclaimLast.IsZero() {
		return true
	}
	return now.Sub(c.reclaimLast) >= interval
}

func (c *Coordinator) noteMetricsFailure(errorTime time.Time) {
	c.metricsMu.Lock()
	if c.metricsFail < 16 {
		c.metricsFail++
	}
	backoff := queueMetricsBackoffBase
	for i := 1; i < c.metricsFail; i++ {
		backoff *= 2
		if backoff >= queueMetricsBackoffMax {
			backoff = queueMetricsBackoffMax
			break
		}
	}
	if backoff > queueMetricsBackoffMax {
		backoff = queueMetricsBackoffMax
	}
	c.metricsHold = errorTime.Add(backoff)
	c.metricsMu.Unlock()
}

func (c *Coordinator) recordQueueMetrics() {
	if c == nil {
		return
	}
	if metrics.OperationQueueReadyDepth == nil && metrics.OperationQueueScheduledDepth == nil && metrics.OperationQueueInflightGauge == nil {
		return
	}
	now := c.now()
	c.metricsMu.Lock()
	if !c.metricsHold.IsZero() && now.Before(c.metricsHold) {
		c.metricsMu.Unlock()
		return
	}
	if !c.metricsLast.IsZero() && now.Sub(c.metricsLast) < queueMetricsMinInterval {
		c.metricsMu.Unlock()
		return
	}
	c.metricsLast = now
	c.metricsMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), queueMetricsEvalTimeout)
	defer cancel()
	if c.shouldAttemptReclaim(now) {
		if _, err := c.reclaimStaleInflight(ctx); err != nil {
			c.noteMetricsFailure(c.now())
			return
		}
	}
	counts, err := c.redis.QueueSnapshotCounts(ctx, c.readyKey(), c.scheduledKey(), c.inflightKey())
	if err != nil {
		c.noteMetricsFailure(c.now())
		return
	}
	if len(counts) < 3 {
		c.noteMetricsFailure(c.now())
		return
	}
	c.metricsMu.Lock()
	c.metricsFail = 0
	c.metricsHold = time.Time{}
	c.metricsMu.Unlock()
	metrics.RecordOperationQueueDepth(c.resource, counts[0], counts[1], counts[2])
}

func (c *Coordinator) Resource() string {
	if c == nil {
		return "operation"
	}
	label := strings.TrimSpace(c.resource)
	if label == "" {
		return "operation"
	}
	return label
}

func (c *Coordinator) pushReady(ctx context.Context, operationID string) (int64, error) {
	result, err := c.redis.Eval(ctx, luaPushReady, []string{c.readyKey(), c.readyMembersKey()}, []interface{}{operationID})
	if err != nil {
		return 0, err
	}
	length, convErr := toInt64(result)
	if convErr != nil {
		return 0, convErr
	}
	return length, nil
}

func (c *Coordinator) popReady(ctx context.Context) (string, error) {
	nowMillis := c.now().UnixMilli()
	result, err := c.redis.Eval(ctx, luaPopReady, []string{c.readyKey(), c.inflightKey(), c.inflightLeaseKey(), c.readyMembersKey()}, []interface{}{nowMillis})
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	value, convErr := toString(result)
	if convErr != nil {
		if errors.Is(convErr, redis.Nil) {
			return "", nil
		}
		return "", convErr
	}
	return value, nil
}

func (c *Coordinator) listPosition(ctx context.Context, operationID string) (int64, error) {
	result, err := c.redis.Eval(ctx, luaListPosition, []string{c.readyKey()}, []interface{}{operationID})
	if err != nil {
		return 0, err
	}
	pos, convErr := toInt64(result)
	if convErr != nil {
		return 0, convErr
	}
	return pos, nil
}

func (c *Coordinator) isInflight(ctx context.Context, operationID string) (bool, error) {
	result, err := c.redis.Eval(ctx, luaIsInflight, []string{c.inflightKey()}, []interface{}{operationID})
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	asBool, convErr := toBool(result)
	if convErr != nil {
		return false, convErr
	}
	return asBool, nil
}

func (c *Coordinator) scheduledScore(ctx context.Context, operationID string) (int64, bool, error) {
	result, err := c.redis.Eval(ctx, luaScheduledScore, []string{c.scheduledKey()}, []interface{}{operationID})
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	score, convErr := toFloat64(result)
	if convErr != nil {
		if errors.Is(convErr, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, convErr
	}
	return int64(score), true, nil
}

func (c *Coordinator) persistEnvelope(ctx context.Context, env *operation.OperationEnvelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return c.redis.SetKey(ctx, c.payloadKey(env.ID), string(payload), c.payloadTTL)
}

func (c *Coordinator) setAttempts(ctx context.Context, operationID string, attempts int) error {
	return c.redis.SetKey(ctx, c.attemptKey(operationID), strconv.Itoa(attempts), c.payloadTTL)
}

func (c *Coordinator) getAttempts(ctx context.Context, operationID string) (int, error) {
	value, err := c.redis.GetKey(ctx, c.attemptKey(operationID))
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, storage.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	attempts, convErr := strconv.Atoi(value)
	if convErr != nil {
		return 0, convErr
	}
	return attempts, nil
}

func (c *Coordinator) ticketKey(ticketID string) string {
	return c.compose("ticket", ticketID)
}

func (c *Coordinator) payloadKey(operationID string) string {
	return c.compose("payload", operationID)
}

func (c *Coordinator) attemptKey(operationID string) string {
	return c.compose("attempt", operationID)
}

func (c *Coordinator) readyKey() string {
	return c.queueKey("ready")
}

func (c *Coordinator) readyMembersKey() string {
	return c.queueKey("ready:members")
}

func (c *Coordinator) inflightKey() string {
	return c.queueKey("inflight")
}

func (c *Coordinator) inflightLeaseKey() string {
	return c.queueKey("inflight:lease")
}

func (c *Coordinator) scheduledKey() string {
	return c.queueKey("scheduled")
}

func (c *Coordinator) queueKey(suffix string) string {
	trimmedSuffix := strings.TrimSpace(suffix)
	if trimmedSuffix == "" {
		trimmedSuffix = "default"
	}
	prefix := strings.TrimSpace(c.keyPrefix)
	tag := "queue"
	if prefix != "" {
		tag = fmt.Sprintf("%s:queue", prefix)
	}
	if prefix == "" {
		return fmt.Sprintf("{%s}:%s", tag, trimmedSuffix)
	}
	return fmt.Sprintf("%s:{%s}:%s", prefix, tag, trimmedSuffix)
}

func (c *Coordinator) compose(parts ...string) string {
	joined := strings.Join(parts, ":")
	if c.keyPrefix == "" {
		return joined
	}
	if joined == "" {
		return c.keyPrefix
	}
	return c.keyPrefix + ":" + joined
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		if len(v) == 0 {
			return 0, nil
		}
		return strconv.ParseInt(string(v), 10, 64)
	case nil:
		return 0, redis.Nil
	default:
		return 0, fmt.Errorf("unexpected integer type %T", v)
	}
}

func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, redis.Nil
		}
		return strconv.ParseFloat(v, 64)
	case []byte:
		if len(v) == 0 {
			return 0, redis.Nil
		}
		return strconv.ParseFloat(string(v), 64)
	case nil:
		return 0, redis.Nil
	default:
		return 0, fmt.Errorf("unexpected float type %T", v)
	}
}

func toString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case nil:
		return "", redis.Nil
	default:
		return fmt.Sprint(v), nil
	}
}

func toBool(val interface{}) (bool, error) {
	n, err := toInt64(val)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return n != 0, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

var _ operation.QueueCoordinator = (*Coordinator)(nil)
