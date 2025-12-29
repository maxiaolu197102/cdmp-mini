package usercache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

type PendingStateValue string

const (
	PendingStateUnknown  PendingStateValue = "unknown"
	PendingStateLease    PendingStateValue = "lease"
	PendingStateReleased PendingStateValue = "released"
	PendingStateExpired  PendingStateValue = "expired"
)

func startPendingSpan(ctx context.Context, operation string) (context.Context, *trace.Span) {
	spanCtx, span := trace.StartSpan(ctx, "pending-coordinator", operation)
	if spanCtx != nil {
		ctx = spanCtx
	}
	return ctx, span
}

func finishPendingSpan(span *trace.Span, status, code string, details map[string]interface{}) {
	if span == nil {
		return
	}
	trace.EndSpan(span, status, code, details)
}

type BackpressureLevel string

const (
	//无背压
	BackpressureNone BackpressureLevel = "none"
	//轻度背压
	BackpressureElevated BackpressureLevel = "elevated"
	//严重背压
	BackpressureSevere BackpressureLevel = "severe"
)

var backpressureGaugeValues = map[BackpressureLevel]float64{
	BackpressureNone:     0,
	BackpressureElevated: 1,
	BackpressureSevere:   2,
}

// BackpressureSample captures a point-in-time view of queue depths and levels.
// AggregateLevel merges global/user/instance to the highest severity.
type BackpressureSample struct {
	Username       string
	GlobalDepth    int64
	GlobalLevel    BackpressureLevel
	GlobalErr      error
	UserDepth      int64
	UserLevel      BackpressureLevel
	UserErr        error
	InstanceDepth  int64
	InstanceLevel  BackpressureLevel
	AggregateLevel BackpressureLevel
	SampledAt      time.Time
	Source         string
}

type backpressureSampleKey struct{}

func (s BackpressureSample) MaxDepth() int64 {
	maxDepth := s.GlobalDepth
	if s.UserDepth > maxDepth {
		maxDepth = s.UserDepth
	}
	if s.InstanceDepth > maxDepth {
		maxDepth = s.InstanceDepth
	}
	return maxDepth
}

func (s BackpressureSample) ShouldReject(threshold BackpressureLevel) bool {
	return backpressureRank(s.AggregateLevel) >= backpressureRank(threshold)
}

// ContextWithBackpressureSample attaches a sampled backpressure snapshot to the context.
func ContextWithBackpressureSample(ctx context.Context, sample BackpressureSample) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, backpressureSampleKey{}, sample)
}

// BackpressureSampleFromContext fetches a cached backpressure sample if present.
func BackpressureSampleFromContext(ctx context.Context) (BackpressureSample, bool) {
	if ctx == nil {
		return BackpressureSample{}, false
	}
	if sample, ok := ctx.Value(backpressureSampleKey{}).(BackpressureSample); ok {
		return sample, true
	}
	return BackpressureSample{}, false
}

const (
	queueDepthDecrementMaxRetries     = 3
	queueDepthDecrementRetryBase      = 40 * time.Millisecond
	queueDepthReconcileTimeout        = 3 * time.Second
	tokenBucketWaitTimeout            = 200 * time.Millisecond
	defaultUserBackpressureSoft       = 4
	defaultUserBackpressureHard       = 8
	defaultInstanceBackpressureSoft   = 120
	defaultInstanceBackpressureHard   = 240
	defaultTokenBucketBurstMultiplier = 2
	defaultDelayBucketCount           = 3
	defaultDepthMultiplier            = 8
	maxBackpressureDepthCap           = 1_000_000
	minDelayQuantum                   = time.Millisecond
	pendingCommandTimeoutDefault      = 600 * time.Millisecond
	scriptReloadMinSlack              = 100 * time.Millisecond
	scopedLimiterIdleTTL              = 10 * time.Minute
	scopedLimiterMaxEntries           = 2048
)

var (
	defaultFallbackDelay = 80 * time.Millisecond
)

const pendingDegradeReasonCreate = "create_degraded"

const pendingAcquireLua = `
local pendingKey = KEYS[1]
local globalKey = KEYS[2]
local userKey = KEYS[3]

local leaseTTL = tonumber(ARGV[1]) or 0
if leaseTTL <= 0 then
	leaseTTL = 60000
end
local globalExpire = tonumber(ARGV[2]) or 0
local userExpire = tonumber(ARGV[3]) or 0
local globalSoft = tonumber(ARGV[4]) or 0
local globalHard = tonumber(ARGV[5]) or 0
local userSoft = tonumber(ARGV[6]) or 0
local userHard = tonumber(ARGV[7]) or 0
local instanceDepth = tonumber(ARGV[8]) or 0
local instanceLevel = ARGV[9] or 'none'
if instanceLevel == '' then
	instanceLevel = 'none'
end
local updatedAt = ARGV[10] or ''
local baseSnapshotJson = ARGV[11]

if redis.call('EXISTS', pendingKey) == 1 then
	return {0}
end

local queueDepth = redis.call('INCR', globalKey)
if queueDepth == 1 and globalExpire > 0 then
	redis.call('EXPIRE', globalKey, globalExpire)
end

local userDepth = redis.call('INCR', userKey)
if userDepth == 1 and userExpire > 0 then
	redis.call('EXPIRE', userKey, userExpire)
end

local globalLevel = 'none'
if globalHard > 0 and queueDepth >= globalHard then
	globalLevel = 'severe'
elseif globalSoft > 0 and queueDepth >= globalSoft then
	globalLevel = 'elevated'
end

local userLevel = 'none'
if userHard > 0 and userDepth >= userHard then
	userLevel = 'severe'
elseif userSoft > 0 and userDepth >= userSoft then
	userLevel = 'elevated'
end

local function merge_level(a, b)
	if a == 'severe' or b == 'severe' then
		return 'severe'
	end
	if a == 'elevated' or b == 'elevated' then
		return 'elevated'
	end
	return 'none'
end

local aggregateLevel = merge_level(merge_level(globalLevel, userLevel), instanceLevel)

local snapshot = cjson.decode(baseSnapshotJson)
snapshot["queue_depth"] = queueDepth
snapshot["backpressure"] = aggregateLevel
snapshot["user_queue_depth"] = userDepth
snapshot["user_backpressure"] = userLevel
snapshot["instance_queue_depth"] = instanceDepth
snapshot["instance_backpressure"] = instanceLevel
snapshot["degraded"] = aggregateLevel ~= 'none'
if updatedAt ~= '' then
	snapshot["updated_at"] = updatedAt
end

local finalPayload = cjson.encode(snapshot)
redis.call('SET', pendingKey, finalPayload, 'PX', leaseTTL)

return {1, queueDepth, userDepth, globalLevel, userLevel, aggregateLevel, finalPayload}
`

var errAcquireLuaUnavailable = errors.New("pending lease acquire lua unavailable")

func ensurePendingMetricsKey(base string) string {
	trimmed := strings.TrimSpace(base)
	if trimmed == "" {
		trimmed = "user:pending:active"
	}
	if strings.Contains(trimmed, "{") {
		return trimmed
	}
	if strings.HasSuffix(trimmed, ":") {
		return trimmed + pendingHashTag
	}
	return trimmed + ":" + pendingHashTag
}

// BackpressureDelayBucket describes a queue depth threshold and the delay to apply when it is reached.
type BackpressureDelayBucket struct {
	Depth int
	Delay time.Duration
}

// BackpressureDelayProfile 是背压延迟配置结构体，核心作用是：为不同背压等级（Elevated 轻度 / Severe 严重）定义 “队列深度→延迟时间” 的映射规则，让生产端（产生任务）和消费端（处理任务）基于统一的延迟策略响应背压，避免两端策略不一致导致的系统震荡。
// 设计理念：
/*
BackpressureDelayProfile 是背压延迟策略的 “配置中心”：
按背压等级（轻度 / 严重）划分延迟规则，实现梯度化延迟；
统一生产 / 消费端的响应策略，避免系统震荡；
所有字段围绕 “队列深度→延迟时间” 的映射展开，兼顾灵活性（可配置）和规范性（有校验）；
核心目的：在系统出现背压时，通过 “可控的延迟” 平缓降低负载，而非直接拒绝任务，平衡系统稳定性和业务可用性。
1. 阶梯式延迟：通过定义多个深度区间和对应的延迟时间，实现随着队列深度增加而逐步加大的延迟效果，平滑系统负载。
2. 灵活配置：允许用户自定义延迟桶，支持不同的业务场景和性能需求。
3. 自动生成：提供默认的延迟桶生成逻辑，简化配置过程，确保在缺省配置下也能获得合理的背压响应。
4. 最大深度限制：通过最大深度参数，防止配置过度，确保延迟策略在合理范围内运行。
1.延迟桶的区间要连续且无重叠：
比如轻度背压的桶不能出现 “0-100” 和 “90-200” 重叠，否则匹配逻辑会混乱；正确的区间是 “0-100、100-200、200-300”。
2.BucketCount 要和桶列表长度一致：
比如 ElevatedBucketCount=3，则 Elevated 列表必须有 3 个桶，否则需在初始化时校验，避免配置错误：
3.BucketCount 要和桶列表长度一致：
比如 ElevatedBucketCount=3，则 Elevated 列表必须有 3 个桶，否则需在初始化时校验，避免配置错误
4.MaxDepth 要和桶的最大深度匹配：
比如 ElevatedMaxDepth=300，则轻度背压最后一个桶的 MaxDepth 必须是 300，否则会出现 “深度 250 属于轻度，但深度 350 未匹配到严重桶” 的漏洞。

BackpressureDelayProfile{
    // 轻度背压（Elevated）延迟桶：队列深度 1000~1500 分5个区间，延迟 20~45ms 线性递增
    Elevated: []BackpressureDelayBucket{
        {MinDepth: 1000, MaxDepth: 1100, Delay: 20 * time.Millisecond}, // 深度1000-1100 → 延迟20ms
        {MinDepth: 1100, MaxDepth: 1200, Delay: 26 * time.Millisecond}, // 深度1100-1200 → 延迟26ms
        {MinDepth: 1200, MaxDepth: 1300, Delay: 32 * time.Millisecond}, // 深度1200-1300 → 延迟32ms
        {MinDepth: 1300, MaxDepth: 1400, Delay: 38 * time.Millisecond}, // 深度1300-1400 → 延迟38ms
        {MinDepth: 1400, MaxDepth: 1500, Delay: 45 * time.Millisecond}, // 深度1400-1500 → 延迟45ms
    },
    // 严重背压（Severe）延迟桶：队列深度 1500~2000 分5个区间，延迟 80~150ms 线性递增
    Severe: []BackpressureDelayBucket{
        {MinDepth: 1500, MaxDepth: 1600, Delay: 80 * time.Millisecond},  // 深度1500-1600 → 延迟80ms
        {MinDepth: 1600, MaxDepth: 1700, Delay: 97 * time.Millisecond},  // 深度1600-1700 → 延迟97ms
        {MinDepth: 1700, MaxDepth: 1800, Delay: 114 * time.Millisecond}, // 深度1700-1800 → 延迟114ms
        {MinDepth: 1800, MaxDepth: 1900, Delay: 131 * time.Millisecond}, // 深度1800-1900 → 延迟131ms
        {MinDepth: 1900, MaxDepth: 2000, Delay: 150 * time.Millisecond}, // 深度1900-2000 → 延迟150ms
    },
    ElevatedBucketCount: 5,    // 轻度桶数量=5
    SevereBucketCount:   5,    // 严重桶数量=5
    ElevatedMaxDepth:    1500, // 轻度背压最大深度=1500（全局硬阈值）
    SevereMaxDepth:      2000, // 严重背压最大深度=2000（兜底值）
}
*/
type BackpressureDelayProfile struct {
	//轻度背压（Elevated）下的延迟桶列表（每个桶包含 “队列深度区间 + 对应延迟时间”）（比如深度 100→延迟 10ms，深度 200→延迟 20ms）
	Elevated []BackpressureDelayBucket
	//严重背压（Severe）下的延迟桶列表（每个桶包含 “队列深度区间 + 对应延迟时间”）（比如深度 100→延迟 50ms，深度 200→延迟 200ms）
	Severe []BackpressureDelayBucket
	//桶数量，用于自动生成延迟桶时的参考
	ElevatedBucketCount int
	//桶数量，用于自动生成延迟桶时的参考
	SevereBucketCount int
	//轻度背压最大队列深度，用于规范延迟桶的最大深度
	ElevatedMaxDepth int
	//严重背压最大队列深度，用于规范延迟桶的最大深度
	SevereMaxDepth int
}

// ensureDefaults 校验并填充延迟配置的默认值，确保配置的完整性和合理性。
func (p *BackpressureDelayProfile) ensureDefaults(soft, hard int, elevatedBase, elevatedMax, severeBase, severeMax time.Duration) {
	if soft <= 0 {
		soft = 1
	}
	if hard <= 0 {
		hard = soft
	}
	if elevatedBase <= 0 {
		elevatedBase = 20 * time.Millisecond
	}
	if elevatedMax <= 0 {
		elevatedMax = elevatedBase
	}
	if elevatedMax < elevatedBase {
		elevatedMax = elevatedBase
	}
	if severeBase <= 0 {
		severeBase = 80 * time.Millisecond
	}
	if severeMax <= 0 {
		severeMax = severeBase
	}
	if severeMax < severeBase {
		severeMax = severeBase
	}
	elevatedBuckets := p.ElevatedBucketCount
	if elevatedBuckets <= 0 {
		elevatedBuckets = defaultDelayBucketCount
	}
	severeBuckets := p.SevereBucketCount
	if severeBuckets <= 0 {
		severeBuckets = defaultDelayBucketCount
	}
	elevatedMaxDepth := clampDepth(p.ElevatedMaxDepth)
	if elevatedMaxDepth <= 0 {
		elevatedMaxDepth = clampDepth(soft * defaultDepthMultiplier)
	}
	if elevatedMaxDepth < soft {
		elevatedMaxDepth = soft
	}
	severeMaxDepth := clampDepth(p.SevereMaxDepth)
	if severeMaxDepth <= 0 {
		base := hard
		if base <= 0 {
			base = soft
		}
		severeMaxDepth = clampDepth(base * defaultDepthMultiplier)
	}
	if severeMaxDepth < hard {
		severeMaxDepth = hard
	}
	if len(p.Elevated) == 0 {
		p.Elevated = buildDelayBuckets(soft, elevatedBuckets, elevatedBase, elevatedMax, elevatedMaxDepth)
	} else {
		p.Elevated = normalizeDelayBuckets(p.Elevated, elevatedMaxDepth)
	}
	if err := validateDelayBuckets("elevated", p.Elevated, soft, p.ElevatedBucketCount, p.ElevatedMaxDepth); err != nil {
		panic(fmt.Sprintf("invalid elevated backpressure profile: %v", err))
	}
	if p.ElevatedBucketCount <= 0 {
		p.ElevatedBucketCount = len(p.Elevated)
	}
	if p.ElevatedMaxDepth <= 0 && len(p.Elevated) > 0 {
		p.ElevatedMaxDepth = p.Elevated[len(p.Elevated)-1].Depth
	}
	if len(p.Severe) == 0 {
		p.Severe = buildDelayBuckets(hard, severeBuckets, severeBase, severeMax, severeMaxDepth)
	} else {
		p.Severe = normalizeDelayBuckets(p.Severe, severeMaxDepth)
	}
	if err := validateDelayBuckets("severe", p.Severe, hard, p.SevereBucketCount, p.SevereMaxDepth); err != nil {
		panic(fmt.Sprintf("invalid severe backpressure profile: %v", err))
	}
	if p.SevereBucketCount <= 0 {
		p.SevereBucketCount = len(p.Severe)
	}
	if p.SevereMaxDepth <= 0 && len(p.Severe) > 0 {
		p.SevereMaxDepth = p.Severe[len(p.Severe)-1].Depth
	}
}

func (p BackpressureDelayProfile) delay(level BackpressureLevel, depth int64) time.Duration {
	switch level {
	case BackpressureElevated:
		return pickDelay(p.Elevated, depth)
	case BackpressureSevere:
		return pickDelay(p.Severe, depth)
	default:
		return 0
	}
}

func (p BackpressureDelayProfile) clone() BackpressureDelayProfile {
	cloneBuckets := func(src []BackpressureDelayBucket) []BackpressureDelayBucket {
		if len(src) == 0 {
			return nil
		}
		dst := make([]BackpressureDelayBucket, len(src))
		copy(dst, src)
		return dst
	}
	return BackpressureDelayProfile{
		Elevated:            cloneBuckets(p.Elevated),
		Severe:              cloneBuckets(p.Severe),
		ElevatedBucketCount: p.ElevatedBucketCount,
		SevereBucketCount:   p.SevereBucketCount,
		ElevatedMaxDepth:    p.ElevatedMaxDepth,
		SevereMaxDepth:      p.SevereMaxDepth,
	}
}

// 根据队列深度选择合适的延迟时间
// 本质是 “阶梯式延迟策略”：队列深度越大，延迟越久，从而通过 “延迟处理” 缓解系统负载压力。
//
//	数据样例:buckets = []BackpressureDelayBucket{
//	   {Depth: 100, Delay: 50 * time.Millisecond},  // 队列≥100，延迟50ms
//	   {Depth: 200, Delay: 200 * time.Millisecond}, // 队列≥200，延迟200ms
//	   {Depth: 500, Delay: 1 * time.Second},        // 队列≥500，延迟1s
//	}
func pickDelay(buckets []BackpressureDelayBucket, depth int64) time.Duration {
	var result time.Duration
	if depth < 0 {
		depth = 0
	}
	for _, bucket := range buckets {
		if bucket.Depth <= 0 || bucket.Delay <= 0 {
			continue
		}
		if depth >= int64(bucket.Depth) && bucket.Delay > result {
			result = bucket.Delay
		}
	}
	return result
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if b <= 0 {
		return a
	}
	if a <= 0 {
		return b
	}
	if a <= b {
		return a
	}
	return b
}

func clampDepth(depth int) int {
	if depth <= 0 {
		return depth
	}
	if depth > maxBackpressureDepthCap {
		return maxBackpressureDepthCap
	}
	return depth
}

func backpressureRank(level BackpressureLevel) int {
	switch level {
	case BackpressureSevere:
		return 2
	case BackpressureElevated:
		return 1
	default:
		return 0
	}
}

func ceilDuration(value, quantum time.Duration) time.Duration {
	if value <= 0 {
		return 0
	}
	if quantum <= 0 {
		return value
	}
	remainder := value % quantum
	if remainder == 0 {
		return value
	}
	return value + quantum - remainder
}

func normalizeDelayBuckets(buckets []BackpressureDelayBucket, maxDepth int) []BackpressureDelayBucket {
	if len(buckets) == 0 {
		return buckets
	}
	cappedMax := clampDepth(maxDepth)
	cleaned := make([]BackpressureDelayBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket.Depth <= 0 || bucket.Delay <= 0 {
			continue
		}
		depth := clampDepth(bucket.Depth)
		if cappedMax > 0 && depth > cappedMax {
			depth = cappedMax
		}
		if depth <= 0 {
			continue
		}
		delay := ceilDuration(bucket.Delay, minDelayQuantum)
		if delay <= 0 {
			delay = minDelayQuantum
		}
		cleaned = append(cleaned, BackpressureDelayBucket{Depth: depth, Delay: delay})
	}
	if len(cleaned) == 0 {
		return cleaned
	}
	sort.Slice(cleaned, func(i, j int) bool {
		if cleaned[i].Depth == cleaned[j].Depth {
			return cleaned[i].Delay < cleaned[j].Delay
		}
		return cleaned[i].Depth < cleaned[j].Depth
	})
	result := make([]BackpressureDelayBucket, 0, len(cleaned))
	for _, bucket := range cleaned {
		if len(result) == 0 {
			result = append(result, bucket)
			continue
		}
		last := &result[len(result)-1]
		if bucket.Depth == last.Depth {
			if bucket.Delay > last.Delay {
				last.Delay = bucket.Delay
			}
			continue
		}
		if bucket.Delay < last.Delay {
			bucket.Delay = last.Delay
		}
		result = append(result, bucket)
	}
	return result
}

func buildDelayBuckets(baseDepth, count int, baseDelay, maxDelay time.Duration, maxDepth int) []BackpressureDelayBucket {
	if count <= 0 {
		return nil
	}
	if baseDepth <= 0 {
		baseDepth = 1
	}
	cappedMax := clampDepth(maxDepth)
	if cappedMax > 0 && cappedMax < baseDepth {
		cappedMax = baseDepth
	}
	if baseDelay <= 0 {
		baseDelay = minDelayQuantum
	}
	if maxDelay <= 0 {
		maxDelay = baseDelay
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	buckets := make([]BackpressureDelayBucket, 0, count)
	depth := baseDepth
	for i := 0; i < count; i++ {
		if i == 0 {
			depth = baseDepth
		} else {
			next := depth * 2
			if next <= depth {
				next = depth + 1
			}
			remaining := count - i - 1
			if cappedMax > 0 {
				maxAllowed := cappedMax
				if remaining > 0 {
					maxAllowed -= remaining
				}
				if next > maxAllowed {
					next = maxAllowed
				}
			}
			if next > maxBackpressureDepthCap {
				next = maxBackpressureDepthCap
			}
			if next <= depth {
				next = depth + 1
			}
			depth = next
		}
		if cappedMax > 0 && i == count-1 {
			depth = cappedMax
		}
		var delay time.Duration
		if count == 1 {
			delay = maxDelay
		} else {
			fraction := float64(i) / float64(count-1)
			delay = baseDelay + time.Duration(fraction*float64(maxDelay-baseDelay))
		}
		delay = ceilDuration(delay, minDelayQuantum)
		if delay <= 0 {
			delay = minDelayQuantum
		}
		buckets = append(buckets, BackpressureDelayBucket{Depth: depth, Delay: delay})
	}
	return normalizeDelayBuckets(buckets, cappedMax)
}

func validateDelayBuckets(name string, buckets []BackpressureDelayBucket, baseDepth, expectedCount, configuredMax int) error {
	if len(buckets) == 0 {
		return fmt.Errorf("%s buckets must not be empty", name)
	}
	if expectedCount > 0 && len(buckets) != expectedCount {
		return fmt.Errorf("%s bucket count mismatch: expect %d, got %d", name, expectedCount, len(buckets))
	}
	boundary := baseDepth
	for idx, bucket := range buckets {
		if bucket.Delay <= 0 {
			return fmt.Errorf("%s bucket[%d] delay must be > 0", name, idx)
		}
		if idx == 0 {
			if baseDepth > 0 && bucket.Depth != baseDepth {
				return fmt.Errorf("%s bucket[%d] depth %d must equal base depth %d", name, idx, bucket.Depth, baseDepth)
			}
		} else if bucket.Depth <= boundary {
			return fmt.Errorf("%s bucket[%d] depth %d must be greater than previous boundary %d", name, idx, bucket.Depth, boundary)
		}
		if bucket.Depth <= 0 {
			return fmt.Errorf("%s bucket[%d] depth must be > 0", name, idx)
		}
		boundary = bucket.Depth
	}
	if configuredMax > 0 && boundary != configuredMax {
		return fmt.Errorf("%s max depth mismatch: expect %d, got %d", name, configuredMax, boundary)
	}
	return nil
}

type LeaseMetadata struct {
	Username        string //必填，标识所属用户
	RequestID       string //必填，唯一请求 ID
	Operator        string //可选，发起请求的操作者标识
	ClientIP        string //可选，发起请求的客户端 IP 地址
	LegacyRequestID string //可选，兼容旧请求 ID 字段
	Backend         string //可选，标识请求来源的后端服务
}

//背压配置

type PendingCoordinatorConfig struct {
	LeaseTTL              time.Duration              //租约有效期（控制单个操作的最大处理时间）
	ReleaseRetention      time.Duration              //正常释放租约状态保留时间
	ExpiredRetention      time.Duration              //过期租约状态保留时间
	ExpiredGracePeriod    time.Duration              //过期宽限期
	HeartbeatTimeout      time.Duration              //心跳超时时间（0 表示禁用，>0 时若超出即触发提前过期）
	ElevatedDelayBase     time.Duration              //基础延迟（背压升高时）
	ObserveTimeout        time.Duration              //观察超时时间（控制读取当前状态的最大等待时间）
	CommandTimeout        time.Duration              //单次Redis命令的最大执行时间
	DegradeActive         func(context.Context) bool //全局降级检测，返回 true 时直接走内存兜底
	BackpressureWindow    time.Duration              //窗口用于控制“活跃计数的保鲜期”（过期即清零）
	BackpressureSoftLimit int                        //软限制（达到该值开始应用背压）
	BackpressureHardLimit int                        //硬限制（达到该值触发严重背压429）
	MetricsKey            string                     //指标键
	Component             string                     //组件标识（用于区分不同服务）
	LogLeaseEvents        bool                       //是否记录租约事件日志

	ElevatedDelayMax              time.Duration            //最大延迟（背压升高时）
	SevereDelayBase               time.Duration            //基础延迟（严重背压时）
	SevereDelayMax                time.Duration            //最大延迟（严重背压时）
	BackpressureDelayProfile      BackpressureDelayProfile //延迟曲线配置
	TokenBucketRate               float64                  //令牌桶速率（req/s），0 表示关闭
	TokenBucketBurst              int                      //令牌桶突发容量
	UserTokenBucketRate           float64                  //用户级令牌桶速率（req/s），0 表示关闭
	UserTokenBucketBurst          int                      //用户级令牌桶突发容量
	InstanceTokenBucketRate       float64                  //实例级令牌桶速率（req/s），0 表示关闭
	InstanceTokenBucketBurst      int                      //实例级令牌桶突发容量
	UserMetricsPrefix             string                   //用户局部深度指标前缀
	UserBackpressureWindow        time.Duration            //用户级队列采样窗口,用于控制“活跃计数的保鲜期”（过期即清零）
	UserBackpressureSoftLimit     int                      //用户级软阈值
	UserBackpressureHardLimit     int                      //用户级硬阈值
	InstanceBackpressureSoftLimit int                      //实例级软阈值
	InstanceBackpressureHardLimit int                      //实例级硬阈值
	FallbackDelay                 time.Duration            //采样失败时的保守延迟
	CalibrationInterval           time.Duration            //全量校准执行间隔
	CalibrationTimeout            time.Duration            //单次全量校准超时
	BackpressureAntiShakeInterval time.Duration            //背压等级防抖时间窗口，>0 时等级切换需持续超过该时间才生效
	IsolateInstanceSevere         bool                     //仅实例级为 severe 且全局/用户非 severe 时，是否降级为 elevated（避免单实例拖累全局）
	IsolateUserSevere             bool                     //仅用户级为 severe 且全局/实例非 severe 时，是否降级为 elevated（避免单租户误伤全局）
}

// DefaultPendingCoordinatorConfig 提供按组件名称初始化的通用模板，便于不同服务复用并保持 key 前缀隔离。
// 调用方可在返回值上按需覆盖阈值、窗口等字段。
func DefaultPendingCoordinatorConfig(component string) PendingCoordinatorConfig {
	trimmed := strings.TrimSpace(component)
	if trimmed == "" {
		trimmed = "pending_coordinator"
	}

	return PendingCoordinatorConfig{
		Component:         trimmed,
		MetricsKey:        fmt.Sprintf("%s:pending:active", trimmed),
		UserMetricsPrefix: fmt.Sprintf("%s:pending:depth:", trimmed),
	}
}

/*
数据源抽象：PendingCoordinator 是背压数据的统一入口
指标来源：从 "pending lease metrics"（待处理租约指标）获取背压样本
业务上下文：在用户缓存场景中，"pending lease" 可能指：
等待处理的缓存更新操作
未完成的分布式锁获取
缓存击穿导致的积压请求
设计意义：
将数据采集与决策逻辑解耦
支持不同的背压信号源实现
便于测试和模拟
*/
type PendingCoordinator struct {
	redis                     *storage.RedisCluster                                                                         // Redis 集群客户端；必须指向可用连接，nil 表示强制回落至内存实现
	cfg                       PendingCoordinatorConfig                                                                      // 背压配置快照；构造时固定，热更新通过 Update* 接口写入其中的 atomic 字段
	component                 string                                                                                        // 组件名称标签，取值范围为任意非空字符串，例如 "iam"、"order"，用于区分指标来源
	mode                      string                                                                                        // 运行模式标识，典型值如 "redis"/"memory"；空字符串表示默认 Redis 路径
	queueDepthReconcileActive atomic.Bool                                                                                   // 对账协程运行标记，仅取 true/false，true 表示校准协程正在运行
	tokenLimiter              *rate.Limiter                                                                                 // 全局令牌桶限速器，nil 表示不启用；非 nil 时限制速率在 (0,+inf)
	instanceLimiter           *rate.Limiter                                                                                 // 实例级令牌桶限速器，nil 表示不启用
	userTokenBuckets          *scopedLimiterPool                                                                            // 用户级令牌桶缓存池，nil 表示不启用
	instanceActive            atomic.Int64                                                                                  // 当前实例活跃租约计数，范围 [0,+inf)，用于实例级背压及指标
	randomMu                  sync.Mutex                                                                                    // 互斥锁，仅在生成随机抖动时短暂加锁，保证 RNG 线程安全
	random                    *rand.Rand                                                                                    // 本地随机数发生器，可见度 scoped 到协调器；nil 时不会注入抖动
	fallbackDelay             time.Duration                                                                                 // 采样失败时采用的保守等待时间，>=0，常见取值 10~100ms
	fallbackOnce              sync.Once                                                                                     // 保证 fallback 初始化逻辑只执行一次，无额外取值语义
	calibrationOnce           sync.Once                                                                                     // 校准调度器单次初始化保护：只允许注册一次后台任务
	calibrationStop           chan struct{}                                                                                 // 通知后台校准协程退出的通道，非 nil 时表示可发送关闭信号
	calibrationStopOnce       sync.Once                                                                                     // 控制 calibrationStop 只关闭一次，避免 panic
	calibrationUpdateCh       chan struct{}                                                                                 // 触发即时校准的信号通道，通常为无缓冲；nil 表示未开启热校准
	calibrationIntervalNS     atomic.Int64                                                                                  // 校准周期缓存，单位纳秒，取值 >=0（0 表示禁用定时校准）
	calibrationTimeoutNS      atomic.Int64                                                                                  // 单次校准超时时间缓存，单位纳秒，取值 >0 才会生效
	backpressureProfile       atomic.Value                                                                                  // 存放 BackpressureDelayProfile 对象的原子容器，供 Acquire/Observe 读取
	fallback                  *memoryPendingCoordinator                                                                     // 内存兜底协调器实例，nil 表示未配置
	pendingAcquireScriptSHA   atomic.Value                                                                                  // 缓存 Lua 脚本 SHA1，类型 string；空值表示需要重新加载
	globalSampleCache         atomic.Value                                                                                  // 缓存最近一次全局采样 queueSampleCache，用于 sampleCacheTTL 内复用
	userSampleCache           sync.Map                                                                                      // 用户级采样缓存，key=用户名(string)，value=*queueSampleCache
	sampleCacheTTL            time.Duration                                                                                 // 采样缓存生存时间，>=0；0 表示禁用缓存
	scriptLoader              func(ctx context.Context, script string) (string, error)                                      // Lua 脚本加载回调，需返回 SHA；为 nil 时使用默认加载器
	luaExecutor               func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) // 执行 Redis Lua 的回调，nil 使用默认客户端执行
	scriptReloadGroup         singleflight.Group                                                                            // 控制脚本加载的单航班组，确保并发只加载一次

	decisionMu            sync.Mutex        //保护防抖状态
	lastAggregateLevel    BackpressureLevel //最近输出的聚合等级
	lastAggregateAt       time.Time         //最近输出的时间
	pendingAggregateLevel BackpressureLevel //待切换的聚合等级（防抖期间缓存）
	pendingAggregateSince time.Time         //待切换等级首次观测时间
}

// 用户令牌桶条目，包含令牌桶和最后使用时间
type scopedLimiterEntry struct {
	//用户令牌桶
	limiter *rate.Limiter
	//当前时间 - 令牌桶lastUsed时间 > 清理阈值（比如5分钟）判定为“长期未使用” → 从字典中删除这个令牌桶
	lastUsed time.Time
}

type scopedLimiterPool struct {
	mu sync.Mutex
	//用户令牌桶的存储字典（key = 用户名，value = 令牌桶 + 最后使用时间）
	limiters map[string]*scopedLimiterEntry
	//令牌桶速率（请求数/秒）
	rate float64
	//令牌桶突发容量
	burst int
}

func newScopedLimiterPool(rateValue float64, burst int) *scopedLimiterPool {
	resolvedBurst := resolveTokenBucketBurst(rateValue, burst)
	if resolvedBurst <= 0 {
		return nil
	}
	return &scopedLimiterPool{
		limiters: make(map[string]*scopedLimiterEntry),
		rate:     rateValue,
		burst:    resolvedBurst,
	}
}

// 获取指定 key 的令牌桶，若不存在则创建一个新的
func (p *scopedLimiterPool) getLimiter(key string) *rate.Limiter {
	if p == nil || key == "" {
		return nil
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limiters == nil {
		p.limiters = make(map[string]*scopedLimiterEntry)
	}
	if entry, ok := p.limiters[key]; ok {
		entry.lastUsed = now
		return entry.limiter
	}
	limiter := rate.NewLimiter(rate.Limit(p.rate), p.burst)
	p.limiters[key] = &scopedLimiterEntry{limiter: limiter, lastUsed: now}
	if len(p.limiters) > scopedLimiterMaxEntries {
		p.compactLocked(now)
	}
	return limiter
}

func (p *scopedLimiterPool) compactLocked(now time.Time) {
	cutoff := now.Add(-scopedLimiterIdleTTL)
	for key, entry := range p.limiters {
		if entry.lastUsed.Before(cutoff) {
			delete(p.limiters, key)
		}
		if len(p.limiters) <= scopedLimiterMaxEntries {
			return
		}
	}
	if len(p.limiters) <= scopedLimiterMaxEntries {
		return
	}
	excess := len(p.limiters) - scopedLimiterMaxEntries
	for key := range p.limiters {
		delete(p.limiters, key)
		excess--
		if excess <= 0 {
			break
		}
	}
}

// 根据速率和请求的突发量解析令牌桶的突发容量
func resolveTokenBucketBurst(rateValue float64, requested int) int {
	if rateValue <= 0 {
		return 0
	}
	resolved := requested
	if resolved <= 0 {
		base := int(rateValue)
		if base <= 0 {
			base = 1
		}
		resolved = base * defaultTokenBucketBurstMultiplier
	}
	if resolved < 1 {
		resolved = 1
	}
	return resolved
}

func newRateLimiter(rateValue float64, burst int) *rate.Limiter {
	resolvedBurst := resolveTokenBucketBurst(rateValue, burst)
	if resolvedBurst <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(rateValue), resolvedBurst)
}

type queueSampleCache struct {
	depth     int64
	level     BackpressureLevel
	expiresAt time.Time
}

type pendingAcquireOutcome struct {
	created        bool
	queueDepth     int64
	globalLevel    BackpressureLevel
	userQueueDepth int64
	userLevel      BackpressureLevel
	instanceDepth  int64
	instanceLevel  BackpressureLevel
	aggregateLevel BackpressureLevel
	snapshot       pendingLeaseSnapshot
	finalPayload   string
	userDepthErr   error
	setDuration    time.Duration
	method         string
}

type pendingLeaseSnapshot struct {
	Status               string `json:"status"`                          // 快照的业务状态：pending/completed/failed 等
	Degraded             bool   `json:"degraded,omitempty"`              // 是否因背压进入降级模式
	State                string `json:"state"`                           // 状态机中的租约状态：lease/released/expired
	OwnerID              string `json:"owner_id"`                        // 当前租约持有者 ID
	Version              int64  `json:"version"`                         // 快照版本号，使用时间戳保证单调
	LeaseExpiresAt       string `json:"lease_expires_at"`                // 租约到期时间（RFC3339Nano）
	AcquireAt            string `json:"acquire_at"`                      // 租约获取时间（RFC3339Nano）
	HeartbeatAt          string `json:"heartbeat_at,omitempty"`          // 最近一次心跳上报时间（RFC3339Nano）
	UpdatedAt            string `json:"updated_at"`                      // 快照最后更新时间（RFC3339Nano）
	QueueDepth           int64  `json:"queue_depth,omitempty"`           // 全局排队深度（所有实例活跃租约数）
	Backpressure         string `json:"backpressure,omitempty"`          // 全局背压等级：none/elevated/severe
	UserQueueDepth       int64  `json:"user_queue_depth,omitempty"`      // 同一用户名的排队深度
	UserBackpressure     string `json:"user_backpressure,omitempty"`     // 同一用户名的背压等级
	InstanceQueueDepth   int64  `json:"instance_queue_depth,omitempty"`  // 当前实例观测到的排队深度
	InstanceBackpressure string `json:"instance_backpressure,omitempty"` // 当前实例的背压等级
	Username             string `json:"username,omitempty"`              // 关联的用户名
	RequestID            string `json:"request_id,omitempty"`            // 请求链路 ID
	Operator             string `json:"operator,omitempty"`              // 操作人标识
	ClientIP             string `json:"client_ip,omitempty"`             // 客户端 IP
	LegacyRequestID      string `json:"legacy_request_id,omitempty"`     // 兼容旧系统的请求 ID
	ReleasedAt           string `json:"released_at,omitempty"`           // 租约释放时间（RFC3339Nano）
	ReleasedBy           string `json:"released_by,omitempty"`           // 执行释放的租约 owner
	ExpiredAt            string `json:"expired_at,omitempty"`            // 租约过期时间（RFC3339Nano）
	ExpiredReason        string `json:"expired_reason,omitempty"`        // 过期原因描述
}

type PendingLease struct {
	Username       string
	OwnerID        string
	Version        int64
	AcquireAt      time.Time
	LeaseExpiresAt time.Time
	HeartbeatAt    time.Time
	QueueDepth     int64
	Backpressure   BackpressureLevel
	Metadata       LeaseMetadata
}

type AcquireResult struct {
	Lease         *PendingLease
	SetNXDuration time.Duration
}

type PendingState struct {
	// Exists Redis键存在标记
	// 数据来源: Redis GET pending:{username}
	// 用途: 判定缓存是否命中以及是否继续解析状态
	Exists bool
	// State 待审批状态枚举值
	// 取值范围: PendingStateValue 各子状态
	// 用途: 指示租约当前所处流程节点（占用/释放/过期等）
	State PendingStateValue
	// LeaseOwner 当前租约持有者
	// 值格式: {hostname}/{pid}/{goroutine-id}
	// 用途: 判断租约归属以支撑重入、回收与监控
	LeaseOwner string
	// Version 状态版本号
	// 递增策略: 每次状态写入自增
	// 用途: 实现乐观锁与变更追踪
	Version int64
	// TTL Redis剩余存活时长
	// 数据来源: PTTL pending:{username}
	// 用途: 确认租约或记录的剩余有效期
	TTL time.Duration
	// LeaseExpiresAt 租约到期时间
	// 时区: 统一使用UTC
	// 用途: 与当前时间对比触发租约续租或清理
	LeaseExpiresAt time.Time
	// HeartbeatAt 最近一次心跳时间
	// 用途: 判断租约是否失联，从而提前触发过期
	HeartbeatAt time.Time
	// ReleasedAt 租约释放时间
	// 为空表示尚未释放
	// 用途: 辅助释放保护期与审计日志
	ReleasedAt time.Time
	// Username 租约关联用户名
	// 键格式: pending:{username}
	// 用途: 标记租约实体，便于日志关联
	Username string
	// ExpiredAt 状态过期时间戳
	// 仅在状态进入过期态时赋值
	// 用途: 指导延迟重试与脱敏清理
	ExpiredAt time.Time
	// ExpiredReason 过期原因描述
	// 取值示例: timeout、manual_release
	// 用途: 精细化回溯与监控维度
	ExpiredReason string
	// Backpressure 当前回压级别
	// 取值范围: BackpressureLevel 枚举 (None/Soft/Hard)
	// 用途: 控制并发与错误回传策略
	Backpressure BackpressureLevel
	// QueueDepth 当前排队深度
	// 数据来源: Lua 脚本统计
	// 用途: 用于回压判定与流量告警
	QueueDepth int64
	// Raw Redis原始字符串结果
	// 格式: JSON 序列化串或哨兵占位值
	// 用途: 调试与故障复盘时输出原文
	Raw string
	// RedisGetDuration Redis GET 时延
	// 单位: time.Duration
	// 用途: 监控读路径性能瓶颈
	RedisGetDuration time.Duration
	// RedisTTLDur Redis TTL 查询耗时
	// 单位: time.Duration
	// 用途: 用于性能打点与诊断慢查询
	RedisTTLDur time.Duration
	// UserQueueDepth 当前用户级排队深度
	// 数据来源: user:pending:depth:{username}
	// 用途: 捕获局部热点，控制单用户流量
	UserQueueDepth int64
	// InstanceQueueDepth 当前实例内的活跃租约数
	// 统计方式: 进程内原子计数
	// 用途: 防止单实例过载
	InstanceQueueDepth int64
	// UserBackpressure 用户级背压等级
	// 用途: 定位局部热点，调节租约策略
	UserBackpressure BackpressureLevel
	// InstanceBackpressure 实例级背压等级
	// 用途: 控制实例级限流或降速
	InstanceBackpressure BackpressureLevel
}

type AcquireFailureReason string

const (
	AcquireFailureConflict     AcquireFailureReason = "conflict"     //背压限流
	AcquireFailureBackpressure AcquireFailureReason = "backpressure" //并发冲突
)

// 租约抢占失败的专属错误结构体
type AcquireError struct {
	Reason     AcquireFailureReason
	State      *PendingState
	QueueDepth int64
}

var (
	errLeaseConflict = errors.New("pending lease conflict")
	errBackpressure  = errors.New("pending backpressure triggered")
)

var (
	ErrPendingLeaseConflict      = errLeaseConflict
	ErrPendingBackpressure       = errBackpressure
	ErrPendingLeaseOwnerMismatch = errors.New("pending lease owner mismatch")
)

func (e *AcquireError) Error() string {
	if e == nil {
		return "pending lease error"
	}
	switch e.Reason {
	case AcquireFailureBackpressure:
		return fmt.Sprintf("pending lease rejected by backpressure (depth=%d)", e.QueueDepth)
	case AcquireFailureConflict:
		return "pending lease already active"
	default:
		return "pending lease error"
	}
}

func (e *AcquireError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Reason {
	case AcquireFailureConflict:
		return errLeaseConflict
	case AcquireFailureBackpressure:
		return errBackpressure
	default:
		return nil
	}
}

func (e *AcquireError) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	if target == errLeaseConflict || target == ErrPendingLeaseConflict {
		return e.Reason == AcquireFailureConflict
	}
	if target == errBackpressure || target == ErrPendingBackpressure {
		return e.Reason == AcquireFailureBackpressure
	}
	return false
}

func NewPendingCoordinator(redis *storage.RedisCluster, cfg PendingCoordinatorConfig) *PendingCoordinator {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Minute
	}
	if cfg.ObserveTimeout <= 0 {
		cfg.ObserveTimeout = 1200 * time.Millisecond
	}
	if cfg.CommandTimeout <= 0 {
		cfg.CommandTimeout = pendingCommandTimeoutDefault
	}
	if cfg.BackpressureWindow <= 0 {
		cfg.BackpressureWindow = 5 * time.Second
	}
	if cfg.BackpressureSoftLimit <= 0 {
		cfg.BackpressureSoftLimit = 1000
	}
	if cfg.BackpressureHardLimit <= 0 {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit + 500
	}
	if cfg.BackpressureHardLimit < cfg.BackpressureSoftLimit {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit
	}
	if cfg.MetricsKey == "" {
		cfg.MetricsKey = "user:pending:active"
	}
	if cfg.ReleaseRetention <= 0 {
		cfg.ReleaseRetention = 3 * time.Second
	}
	if cfg.ExpiredRetention <= 0 {
		cfg.ExpiredRetention = 30 * time.Second
	}
	if cfg.ExpiredGracePeriod <= 0 {
		cfg.ExpiredGracePeriod = 2 * time.Second
	}
	if cfg.ElevatedDelayBase <= 0 {
		cfg.ElevatedDelayBase = 20 * time.Millisecond
	}
	if cfg.ElevatedDelayMax <= 0 {
		cfg.ElevatedDelayMax = 45 * time.Millisecond
	}
	if cfg.SevereDelayBase <= 0 {
		cfg.SevereDelayBase = 80 * time.Millisecond
	}
	if cfg.SevereDelayMax <= 0 {
		cfg.SevereDelayMax = 150 * time.Millisecond
	}
	// 防抖/隔离开关默认关闭，保持向后兼容；调用方显式配置后生效。
	if cfg.BackpressureAntiShakeInterval < 0 {
		cfg.BackpressureAntiShakeInterval = 0
	}

	applyCoordinatorEnvOverrides(&cfg)
	cfg.MetricsKey = ensurePendingMetricsKey(cfg.MetricsKey)
	if cfg.CommandTimeout <= 0 {
		cfg.CommandTimeout = pendingCommandTimeoutDefault
	}

	if cfg.ElevatedDelayBase <= 0 {
		cfg.ElevatedDelayBase = 20 * time.Millisecond
	}
	if cfg.ElevatedDelayMax <= 0 {
		cfg.ElevatedDelayMax = cfg.ElevatedDelayBase
	}
	if cfg.ElevatedDelayMax < cfg.ElevatedDelayBase {
		cfg.ElevatedDelayMax = cfg.ElevatedDelayBase
	}
	if cfg.SevereDelayBase <= 0 {
		cfg.SevereDelayBase = 80 * time.Millisecond
	}
	if cfg.SevereDelayMax <= 0 {
		cfg.SevereDelayMax = cfg.SevereDelayBase
	}
	if cfg.SevereDelayMax < cfg.SevereDelayBase {
		cfg.SevereDelayMax = cfg.SevereDelayBase
	}

	cfg.BackpressureDelayProfile.ensureDefaults(cfg.BackpressureSoftLimit, cfg.BackpressureHardLimit, cfg.ElevatedDelayBase, cfg.ElevatedDelayMax, cfg.SevereDelayBase, cfg.SevereDelayMax)
	cfg.BackpressureDelayProfile = cfg.BackpressureDelayProfile.clone()

	component := strings.TrimSpace(cfg.Component)
	if component == "" {
		component = "pending_coordinator"
	}
	cfg.Component = component

	mode := "redis"
	if redis == nil {
		mode = "memory"
	}

	if strings.TrimSpace(cfg.UserMetricsPrefix) == "" {
		cfg.UserMetricsPrefix = PendingUserDepthPrefix()
	}
	cfg.UserMetricsPrefix = normalizeKeyPrefix(cfg.UserMetricsPrefix)
	if cfg.UserBackpressureWindow <= 0 {
		cfg.UserBackpressureWindow = cfg.BackpressureWindow
		if cfg.UserBackpressureWindow <= 0 {
			cfg.UserBackpressureWindow = 5 * time.Second
		}
	}
	if cfg.UserBackpressureSoftLimit <= 0 {
		cfg.UserBackpressureSoftLimit = defaultUserBackpressureSoft
	}
	if cfg.UserBackpressureHardLimit <= 0 {
		cfg.UserBackpressureHardLimit = defaultUserBackpressureHard
	}
	if cfg.UserBackpressureHardLimit < cfg.UserBackpressureSoftLimit {
		cfg.UserBackpressureHardLimit = cfg.UserBackpressureSoftLimit
	}
	if cfg.InstanceBackpressureHardLimit <= 0 {
		cfg.InstanceBackpressureHardLimit = defaultInstanceBackpressureHard
	}
	if cfg.InstanceBackpressureHardLimit < cfg.InstanceBackpressureSoftLimit {
		cfg.InstanceBackpressureHardLimit = cfg.InstanceBackpressureSoftLimit
	}
	if cfg.FallbackDelay <= 0 {
		cfg.FallbackDelay = defaultFallbackDelay
	}
	if cfg.CalibrationInterval <= 0 {
		cfg.CalibrationInterval = 30 * time.Second
	}
	if cfg.CalibrationTimeout <= 0 || cfg.CalibrationTimeout > cfg.CalibrationInterval {
		timeout := minDuration(5*time.Second, cfg.CalibrationInterval/2)
		if timeout <= 0 {
			timeout = time.Second
		}
		cfg.CalibrationTimeout = timeout
	}

	cacheTTL := cfg.BackpressureWindow / 50
	if cacheTTL <= 0 {
		cacheTTL = 50 * time.Millisecond
	}
	if cacheTTL > 150*time.Millisecond {
		cacheTTL = 150 * time.Millisecond
	}

	tokenLimiter := newRateLimiter(cfg.TokenBucketRate, cfg.TokenBucketBurst)
	instanceLimiter := newRateLimiter(cfg.InstanceTokenBucketRate, cfg.InstanceTokenBucketBurst)
	userTokenBuckets := newScopedLimiterPool(cfg.UserTokenBucketRate, cfg.UserTokenBucketBurst)
	//确保随机数生成器线程安 随机数生成器是背压协调器实现「平滑削峰、避免系统抖动」的关键组件，核心作用是在预设的延迟区间内生成随机延迟时间全.
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	coordinator := &PendingCoordinator{
		redis:            redis,
		cfg:              cfg,
		component:        component,
		mode:             mode,
		tokenLimiter:     tokenLimiter,
		instanceLimiter:  instanceLimiter,
		userTokenBuckets: userTokenBuckets,
		random:           random,
		//fallbackDelay 是背压协调器的容错兜底延迟（也叫 “降级延迟”），核心作用是：当协调器无法正常获取 Redis 中的背压计数（比如 Redis 集群故障、网络抖动、超时）时，使用这个预设的延迟值对请求做 “保守限流”—— 既避免无延迟放行导致系统过载，又保证服务不直接报错，是「优雅降级」的关键配置。
		fallbackDelay: cfg.FallbackDelay,
		//租约计数校准协程的 “控制信号器”.calibrationUpdateCh 负责 “触发校准规则更新”
		calibrationStop:     make(chan struct{}),
		calibrationUpdateCh: make(chan struct{}, 1),
	}
	coordinator.ensureFallback()
	coordinator.sampleCacheTTL = cacheTTL
	coordinator.backpressureProfile.Store(cfg.BackpressureDelayProfile)
	coordinator.calibrationIntervalNS.Store(cfg.CalibrationInterval.Nanoseconds())
	coordinator.calibrationTimeoutNS.Store(cfg.CalibrationTimeout.Nanoseconds())
	if redis != nil {
		coordinator.scriptLoader = func(ctx context.Context, script string) (string, error) {
			return redis.ScriptLoad(ctx, script)
		}
		coordinator.luaExecutor = func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
			return redis.EvalSha(ctx, sha, keys, args)
		}
	}
	if redis == nil {
		return coordinator
	}
	coordinator.startCalibrationLoop()

	return coordinator
}

// Component returns the configured component label for metrics and logging.
func (c *PendingCoordinator) Component() string {
	if c == nil {
		return "pending_coordinator"
	}
	name := strings.TrimSpace(c.component)
	if name == "" {
		return "pending_coordinator"
	}
	return name
}

// ExpiredGracePeriod returns the configured grace interval applied after lease expiration.
func (c *PendingCoordinator) ExpiredGracePeriod() time.Duration {
	if c == nil {
		return 0
	}
	return c.cfg.ExpiredGracePeriod
}

func (c *PendingCoordinator) ensureFallback() *memoryPendingCoordinator {
	if c == nil {
		return nil
	}
	c.fallbackOnce.Do(func() {
		if c.fallback == nil {
			c.fallback = newMemoryPendingCoordinator(c.cfg, c.Component())
		}
	})
	return c.fallback
}

func (c *PendingCoordinator) degradeActive(ctx context.Context) bool {
	if c == nil || c.cfg.DegradeActive == nil {
		return false
	}
	return c.cfg.DegradeActive(ctx)
}

func (c *PendingCoordinator) recordDegradeFallback(operation, username string) {
	if c == nil {
		return
	}
	metrics.RecordPendingLeaseFallback(c.component, operation, pendingDegradeReasonCreate)
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, operation+"_degrade_bypass").Inc()
	}
	trimmed := strings.TrimSpace(username)
	fields := []interface{}{"operation", operation, "backend", c.Backend()}
	if trimmed != "" {
		fields = append(fields, "username", trimmed)
	}
	c.logLeaseEvent("info", "pending coordinator degrade fallback", fields...)
}

// Backend returns the storage backend used by the coordinator (redis or memory).
func (c *PendingCoordinator) Backend() string {
	if c == nil {
		return "none"
	}
	if mode := strings.TrimSpace(c.mode); mode != "" {
		return mode
	}
	if c.redis != nil {
		return "redis"
	}
	if c.fallback != nil {
		return "memory"
	}
	return "none"
}

// CheckHealth verifies the backend connectivity for the coordinator.
func (c *PendingCoordinator) CheckHealth(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("pending coordinator is nil")
	}
	if c.redis == nil {
		if c.fallback != nil {
			return nil
		}
		return fmt.Errorf("pending coordinator backend unavailable")
	}
	if err := c.redis.Up(); err != nil {
		return err
	}
	client := c.redis.GetClient()
	if client == nil {
		return storage.ErrRedisIsDown
	}
	healthCtx := ctx
	if healthCtx == nil {
		healthCtx = context.Background()
	}
	if _, hasDeadline := healthCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		healthCtx, cancel = context.WithTimeout(healthCtx, c.cfg.ObserveTimeout)
		defer cancel()
	}
	if err := client.Ping(healthCtx).Err(); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorEnvOverrides(cfg *PendingCoordinatorConfig) {
	if cfg == nil {
		return
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_LEASE_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.LeaseTTL = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_COMMAND_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.CommandTimeout = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_OBSERVE_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ObserveTimeout = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_BACKPRESSURE_WINDOW")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.BackpressureWindow = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_SOFT_LIMIT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			cfg.BackpressureSoftLimit = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_HARD_LIMIT")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			cfg.BackpressureHardLimit = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_RELEASE_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ReleaseRetention = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_EXPIRED_TTL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ExpiredRetention = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_EXPIRED_GRACE")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
			cfg.ExpiredGracePeriod = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_HEARTBEAT_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
			cfg.HeartbeatTimeout = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_ELEVATED")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ElevatedDelayBase = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_ELEVATED_MAX")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.ElevatedDelayMax = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_SEVERE")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.SevereDelayBase = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_DELAY_SEVERE_MAX")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.SevereDelayMax = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_CALIBRATION_INTERVAL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.CalibrationInterval = parsed
		}
	}
	if raw := strings.TrimSpace(os.Getenv("IAM_PENDING_CALIBRATION_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			cfg.CalibrationTimeout = parsed
		}
	}
	if cfg.BackpressureHardLimit < cfg.BackpressureSoftLimit {
		cfg.BackpressureHardLimit = cfg.BackpressureSoftLimit
	}
}

func (c *PendingCoordinator) ensurePendingAcquireScript(ctx context.Context) (string, error) {
	if c == nil || c.redis == nil {
		return "", errAcquireLuaUnavailable
	}
	if v := c.pendingAcquireScriptSHA.Load(); v != nil {
		if sha, ok := v.(string); ok && strings.TrimSpace(sha) != "" {
			return sha, nil
		}
	}
	return c.loadPendingAcquireScript(ctx)
}

func (c *PendingCoordinator) loadPendingAcquireScript(ctx context.Context) (string, error) {
	if c == nil {
		return "", errAcquireLuaUnavailable
	}
	callCtx := c.scriptReloadBaseContext(ctx)

	result, err, _ := c.scriptReloadGroup.Do("pending_acquire", func() (interface{}, error) {
		loader := c.scriptLoader
		if loader == nil {
			if c.redis == nil {
				return "", errAcquireLuaUnavailable
			}
			loader = func(ctx context.Context, script string) (string, error) {
				return c.redis.ScriptLoad(ctx, script)
			}
		}

		commandTimeout := c.cfg.CommandTimeout
		if commandTimeout <= 0 {
			commandTimeout = pendingCommandTimeoutDefault
		}
		loadCtx, cancel := context.WithTimeout(callCtx, commandTimeout)
		if cancel != nil {
			defer cancel()
		}

		sha, loadErr := loader(loadCtx, pendingAcquireLua)
		if loadErr != nil {
			return "", loadErr
		}
		c.pendingAcquireScriptSHA.Store(sha)
		return sha, nil
	})
	if err != nil {
		return "", err
	}
	if sha, ok := result.(string); ok && strings.TrimSpace(sha) != "" {
		return sha, nil
	}
	return "", errAcquireLuaUnavailable
}

func (c *PendingCoordinator) evalPendingAcquireScript(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
	if c == nil {
		return nil, errAcquireLuaUnavailable
	}
	executor := c.luaExecutor
	if executor == nil {
		if c.redis == nil {
			return nil, errAcquireLuaUnavailable
		}
		executor = func(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
			return c.redis.EvalSha(ctx, sha, keys, args)
		}
	}
	scriptCtx, cancel := c.newCommandContext(ctx)
	defer cancel()
	result, err := executor(scriptCtx, sha, keys, args)
	if err == nil {
		return result, nil
	}
	errMsg := err.Error()
	if strings.Contains(errMsg, "NOSCRIPT") {
		reloadCtx := c.scriptReloadBaseContext(ctx)
		newSha, loadErr := c.loadPendingAcquireScript(reloadCtx)
		if loadErr != nil {
			return nil, loadErr
		}
		retryCtx, retryCancel := c.newCommandContext(ctx)
		defer retryCancel()
		return executor(retryCtx, newSha, keys, args)
	}
	if strings.Contains(errMsg, "CROSSSLOT") || strings.Contains(errMsg, "wrong number of keys") {
		return nil, errAcquireLuaUnavailable
	}
	return nil, err
}

func luaToInt(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

func luaToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (c *PendingCoordinator) loadGlobalSample() (*queueSampleCache, bool) {
	if c == nil {
		return nil, false
	}
	if v := c.globalSampleCache.Load(); v != nil {
		if sample, ok := v.(*queueSampleCache); ok && sample != nil {
			return sample, true
		}
	}
	return nil, false
}

func (c *PendingCoordinator) storeGlobalSample(depth int64, level BackpressureLevel) {
	if c == nil {
		return
	}
	if c.sampleCacheTTL <= 0 {
		return
	}
	sample := &queueSampleCache{
		depth:     depth,
		level:     level,
		expiresAt: time.Now().Add(c.sampleCacheTTL),
	}
	c.globalSampleCache.Store(sample)
}

func (c *PendingCoordinator) loadUserSample(username string) (*queueSampleCache, bool) {
	if c == nil {
		return nil, false
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, false
	}
	if v, ok := c.userSampleCache.Load(trimmed); ok {
		if sample, okCast := v.(*queueSampleCache); okCast && sample != nil {
			return sample, true
		}
	}
	return nil, false
}

func (c *PendingCoordinator) storeUserSample(username string, depth int64, level BackpressureLevel) {
	if c == nil {
		return
	}
	if c.sampleCacheTTL <= 0 {
		return
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return
	}
	sample := &queueSampleCache{
		depth:     depth,
		level:     level,
		expiresAt: time.Now().Add(c.sampleCacheTTL),
	}
	c.userSampleCache.Store(trimmed, sample)
}

func (c *PendingCoordinator) observeLuaAttempt(outcome string) {
	if c == nil {
		return
	}
	if metrics.PendingLeaseLuaAttempts == nil {
		return
	}
	label := strings.TrimSpace(outcome)
	if label == "" {
		label = "unknown"
	}
	metrics.PendingLeaseLuaAttempts.WithLabelValues(c.component, label).Inc()
}

func (c *PendingCoordinator) tryPendingAcquire(ctx context.Context, username, key string, snapshot pendingLeaseSnapshot, basePayload string) (*pendingAcquireOutcome, error) {
	if outcome, err := c.pendingAcquireViaLua(ctx, username, key, snapshot); err == nil {
		return outcome, nil
	} else if !errors.Is(err, errAcquireLuaUnavailable) {
		return nil, err
	} else {
		c.logLeaseEvent("debug", "pending lease lua acquire fallback", "username", username, "error", err)
	}
	return c.pendingAcquireLegacy(ctx, username, key, snapshot, basePayload)
}

func (c *PendingCoordinator) pendingAcquireViaLua(ctx context.Context, username, key string, snapshot pendingLeaseSnapshot) (*pendingAcquireOutcome, error) {
	if c == nil || c.redis == nil {
		return nil, errAcquireLuaUnavailable
	}
	metricsKey := strings.TrimSpace(c.cfg.MetricsKey)
	if metricsKey == "" {
		return nil, errAcquireLuaUnavailable
	}
	userDepthKey := c.pendingUserDepthKey(username)
	if userDepthKey == "" {
		return nil, errAcquireLuaUnavailable
	}
	instanceDepth := c.incInstanceActive()
	instanceLevel := c.classifyInstanceBackpressure(instanceDepth)
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	snapshot.InstanceQueueDepth = instanceDepth
	snapshot.InstanceBackpressure = string(instanceLevel)
	snapshot.UpdatedAt = updatedAt
	basePayload, err := json.Marshal(&snapshot)
	if err != nil {
		c.decInstanceActive()
		return nil, err
	}
	ttlMs := c.cfg.LeaseTTL.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = int64((time.Minute).Milliseconds())
	}
	globalExpire := int64(c.cfg.BackpressureWindow.Seconds())
	userExpire := secondsCeil(c.cfg.UserBackpressureWindow)
	args := []interface{}{
		ttlMs,
		globalExpire,
		userExpire,
		c.cfg.BackpressureSoftLimit,
		c.cfg.BackpressureHardLimit,
		c.cfg.UserBackpressureSoftLimit,
		c.cfg.UserBackpressureHardLimit,
		instanceDepth,
		string(instanceLevel),
		updatedAt,
		string(basePayload),
	}
	keys := []string{key, metricsKey, userDepthKey}
	sha := ""
	if cached := c.pendingAcquireScriptSHA.Load(); cached != nil {
		if value, ok := cached.(string); ok {
			sha = strings.TrimSpace(value)
		}
	}
	if sha == "" {
		reloadCtx := c.scriptReloadBaseContext(ctx)
		var loadErr error
		sha, loadErr = c.loadPendingAcquireScript(reloadCtx)
		if loadErr != nil {
			c.decInstanceActive()
			return nil, loadErr
		}
	}
	start := time.Now()
	result, err := c.evalPendingAcquireScript(ctx, sha, keys, args)
	duration := time.Since(start)
	metrics.RecordRedisOperation("pending_lease_acquire_lua", duration.Seconds(), err)
	if err != nil {
		if errors.Is(err, errAcquireLuaUnavailable) {
			c.observeLuaAttempt("unavailable")
		} else {
			c.observeLuaAttempt("eval_error")
		}
		c.decInstanceActive()
		return nil, err
	}
	arr, ok := result.([]interface{})
	if !ok || len(arr) < 7 {
		c.observeLuaAttempt("bad_response")
		c.decInstanceActive()
		return nil, fmt.Errorf("unexpected lua result type %T", result)
	}
	if luaToInt(arr[0]) == 0 {
		c.observeLuaAttempt("exists")
		c.decInstanceActive()
		return &pendingAcquireOutcome{created: false, setDuration: duration, method: "lua", snapshot: snapshot}, nil
	}
	queueDepth := luaToInt(arr[1])
	userDepth := luaToInt(arr[2])
	globalLevel := BackpressureLevel(luaToString(arr[3]))
	userLevel := BackpressureLevel(luaToString(arr[4]))
	// Lua 返回的聚合等级基于旧规则，这里重新应用新决策以支持隔离/防抖
	aggregateLevel := c.decideAggregateLevel(globalLevel, userLevel, instanceLevel, time.Now())
	finalPayload := luaToString(arr[6])
	snapshot.QueueDepth = queueDepth
	snapshot.UserQueueDepth = userDepth
	snapshot.UserBackpressure = string(userLevel)
	snapshot.Backpressure = string(aggregateLevel)
	snapshot.Degraded = aggregateLevel != BackpressureNone
	if finalPayload == "" {
		if encoded, marshalErr := json.Marshal(&snapshot); marshalErr == nil {
			finalPayload = string(encoded)
		}
	}
	c.recordQueueDepthMetrics(queueDepth, globalLevel)
	c.storeGlobalSample(queueDepth, globalLevel)
	c.storeUserSample(username, userDepth, userLevel)
	outcome := &pendingAcquireOutcome{
		created:        true,
		queueDepth:     queueDepth,
		globalLevel:    globalLevel,
		userQueueDepth: userDepth,
		userLevel:      userLevel,
		instanceDepth:  instanceDepth,
		instanceLevel:  instanceLevel,
		aggregateLevel: aggregateLevel,
		snapshot:       snapshot,
		finalPayload:   finalPayload,
		userDepthErr:   nil,
		setDuration:    duration,
		method:         "lua",
	}
	c.observeLuaAttempt("acquired")
	return outcome, nil
}

func (c *PendingCoordinator) pendingAcquireLegacy(ctx context.Context, username, key string, snapshot pendingLeaseSnapshot, basePayload string) (*pendingAcquireOutcome, error) {
	if c == nil || c.redis == nil {
		return nil, errors.New("pending coordinator redis backend unavailable")
	}
	commandTimeout := c.cfg.CommandTimeout
	opCtx, cancel := c.newOpContext(ctx)
	setStart := time.Now()
	created, setErr := c.redis.SetNXWithCommandTimeout(opCtx, key, basePayload, c.cfg.LeaseTTL, commandTimeout)
	cancel()
	setDuration := time.Since(setStart)
	metrics.RecordRedisOperation("pending_lease_setnx", setDuration.Seconds(), setErr)
	if setErr != nil {
		return nil, setErr
	}
	if !created {
		return &pendingAcquireOutcome{created: false, setDuration: setDuration, method: "legacy", snapshot: snapshot}, nil
	}
	queueDepth := c.redis.IncrememntWithExpire(ctx, c.cfg.MetricsKey, int64(c.cfg.BackpressureWindow.Seconds()))
	level := c.classifyBackpressure(queueDepth)
	c.recordQueueDepthMetrics(queueDepth, level)
	c.storeGlobalSample(queueDepth, level)
	userQueueDepth, userDepthErr := c.incrementUserQueueDepth(ctx, username)
	userLevel := c.classifyUserBackpressure(userQueueDepth)
	instanceDepth := c.incInstanceActive()
	instanceLevel := c.classifyInstanceBackpressure(instanceDepth)
	aggregateLevel := c.decideAggregateLevel(level, userLevel, instanceLevel, time.Now())
	if userDepthErr != nil {
		aggregateLevel = mergeBackpressureLevels(aggregateLevel, BackpressureElevated)
		c.fallbackThrottle("user_counter")
	} else {
		c.storeUserSample(username, userQueueDepth, userLevel)
	}
	snapshot.QueueDepth = queueDepth
	snapshot.Backpressure = string(aggregateLevel)
	snapshot.UserQueueDepth = userQueueDepth
	snapshot.UserBackpressure = string(userLevel)
	snapshot.InstanceQueueDepth = instanceDepth
	snapshot.InstanceBackpressure = string(instanceLevel)
	snapshot.Degraded = aggregateLevel != BackpressureNone
	snapshot.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	finalPayload := basePayload
	if updatedPayload, marshalErr := json.Marshal(&snapshot); marshalErr == nil {
		updateCtx, updateCancel := c.newOpContext(ctx)
		if err := c.redis.SetKeyWithCommandTimeout(updateCtx, key, string(updatedPayload), c.cfg.LeaseTTL, commandTimeout); err != nil {
			log.Warnw("update pending snapshot failed", "username", username, "error", err)
		} else {
			finalPayload = string(updatedPayload)
		}
		updateCancel()
	} else {
		log.Warnw("marshal pending snapshot with queue depth failed", "username", username, "error", marshalErr)
	}
	outcome := &pendingAcquireOutcome{
		created:        true,
		queueDepth:     queueDepth,
		globalLevel:    level,
		userQueueDepth: userQueueDepth,
		userLevel:      userLevel,
		instanceDepth:  instanceDepth,
		instanceLevel:  instanceLevel,
		aggregateLevel: aggregateLevel,
		snapshot:       snapshot,
		finalPayload:   finalPayload,
		userDepthErr:   userDepthErr,
		setDuration:    setDuration,
		method:         "legacy",
	}
	return outcome, nil
}

const releasedStateRetryLimit = 3

// 大量请求 → 令牌桶过滤（剩80%）→ 柔性延迟过滤（剩60%）
// → 租约抢占过滤（剩10%）→ 执行核心业务
func (c *PendingCoordinator) Acquire(ctx context.Context, username string, meta LeaseMetadata) (res *AcquireResult, err error) {
	if c == nil {
		return nil, nil
	}
	ctx, span := startPendingSpan(ctx, "pending_acquire")
	status := "success"
	code := "0"
	var finalDetails map[string]interface{}
	defer func() {
		if err != nil {
			status = "error"
			code = err.Error()
		}
		finishPendingSpan(span, status, code, finalDetails)
	}()
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		err = errors.New("username required")
		return nil, err
	}
	if err := c.guardTokenBucket(ctx, trimmed); err != nil {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "token_bucket_block").Inc()
		}
		err = &AcquireError{Reason: AcquireFailureBackpressure, QueueDepth: 0}
		finalDetails = map[string]interface{}{
			"username":            trimmed,
			"acquire_result":      "token_bucket_block",
			"acquire_fail_reason": "token_bucket_block",
			"fallback_strategy":   "none",
		}
		trace.AddRequestTag(ctx, "acquire_fail_reason", "token_bucket_block")
		trace.AddRequestTag(ctx, "fallback_strategy", "none")
		return nil, err
	}
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("acquire", trimmed)
		if fallback := c.ensureFallback(); fallback != nil {
			trace.AddRequestTag(ctx, "acquire_fail_reason", "degrade_active")
			trace.AddRequestTag(ctx, "fallback_strategy", "delegate")
			return fallback.Acquire(ctx, trimmed, meta)
		}
		trace.AddRequestTag(ctx, "acquire_fail_reason", "degrade_active")
		trace.AddRequestTag(ctx, "fallback_strategy", "none")
		return nil, nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			trace.AddRequestTag(ctx, "acquire_fail_reason", "backend_unavailable")
			trace.AddRequestTag(ctx, "fallback_strategy", "delegate")
			return c.fallback.Acquire(ctx, trimmed, meta)
		}
		trace.AddRequestTag(ctx, "acquire_fail_reason", "backend_unavailable")
		trace.AddRequestTag(ctx, "fallback_strategy", "none")
		return nil, nil
	}
	// 格式:user:pending:{username}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return nil, errors.New("pending key empty")
	}
	// 重试机制：处理 Released 状态的竞态条件
	// 场景：多个并发请求同时尝试获取同一用户名的租约
	// 目标：确保 Released 状态被正确清理，避免冲突
	//比如前一个请求刚释放租约，Redis 里的快照还没过期（5 秒 TTL），此时新请求来了，可能误判为 “仍被占用”。重试几次能等快照过期 / 被清理，提高抢占成功率。
	var promoteExpired bool
	var leaseTTLMS int64
	if c.cfg.LeaseTTL > 0 {
		leaseTTLMS = c.cfg.LeaseTTL.Milliseconds()
	}
	var aggregateLevel BackpressureLevel
	var globalDepth, userDepth, instanceDepth int64
	var globalLevel, userLevel, instanceLevel BackpressureLevel
	var ownerID string
	for attempt := 0; attempt < releasedStateRetryLimit+1; attempt++ {
		ctxSample, hasCtxSample := BackpressureSampleFromContext(ctx)
		sample := ctxSample
		if !hasCtxSample || (sample.Username != "" && sample.Username != trimmed) {
			sample = c.SampleBackpressure(ctx, trimmed)
			ctx = ContextWithBackpressureSample(ctx, sample)
		} else if sample.Username == "" && trimmed != "" && sample.UserLevel == "" {
			// Reuse global sample from middleware but add user dimension when available.
			userDepth, userLevel, userErr := c.SampleUserQueueDepth(ctx, trimmed)
			sample.Username = trimmed
			sample.UserDepth = userDepth
			sample.UserLevel = userLevel
			sample.UserErr = userErr
			sample.AggregateLevel = c.decideAggregateLevel(sample.GlobalLevel, sample.UserLevel, sample.InstanceLevel, time.Now())
			sample.Source = "augmented"
			ctx = ContextWithBackpressureSample(ctx, sample)
		}

		globalDepth, userDepth, instanceDepth = sample.GlobalDepth, sample.UserDepth, sample.InstanceDepth
		globalLevel, userLevel, instanceLevel = sample.GlobalLevel, sample.UserLevel, sample.InstanceLevel
		aggregateLevel = sample.AggregateLevel
		fallbackSampling := false
		if sample.GlobalErr != nil || sample.UserErr != nil {
			aggregateLevel = mergeBackpressureLevels(aggregateLevel, BackpressureElevated)
			fallbackSampling = true
		}
		if sample.GlobalErr != nil {
			c.logLeaseEvent("warn", "pending lease global depth sample failed", "username", trimmed, "error", sample.GlobalErr)
		}
		if sample.UserErr != nil {
			c.logLeaseEvent("warn", "pending lease user depth sample failed", "username", trimmed, "error", sample.UserErr)
		}

		trace.AddRequestTag(ctx, "queue_depth_global", globalDepth)
		trace.AddRequestTag(ctx, "queue_depth_user", userDepth)
		trace.AddRequestTag(ctx, "queue_depth_instance", instanceDepth)
		trace.AddRequestTag(ctx, "backpressure_level", string(aggregateLevel))
		maxDepth := sample.MaxDepth()
		if aggregateLevel == BackpressureSevere {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "precheck_backpressure_reject").Inc()
			}
			c.logLeaseEvent("warn", "pending lease precheck rejected by severe backpressure", "username", trimmed, "queue_depth", maxDepth, "global_level", string(globalLevel), "user_level", string(userLevel), "instance_level", string(instanceLevel))
			finalDetails = map[string]interface{}{
				"username":             trimmed,
				"acquire_result":       "backpressure_reject",
				"queue_depth_global":   globalDepth,
				"queue_depth_user":     userDepth,
				"queue_depth_instance": instanceDepth,
				"backpressure_level":   string(aggregateLevel),
				"acquire_fail_reason":  "backpressure_reject",
				"fallback_strategy":    "precheck_block",
			}
			trace.AddRequestTag(ctx, "acquire_fail_reason", "backpressure_reject")
			trace.AddRequestTag(ctx, "fallback_strategy", "precheck_block")
			err = &AcquireError{Reason: AcquireFailureBackpressure, QueueDepth: maxDepth}
			return nil, err
		}
		if fallbackSampling {
			c.fallbackThrottle("sample")
		}
		if aggregateLevel == BackpressureElevated {
			c.applyBackpressureDelay(aggregateLevel, maxDepth)
		}

		now := time.Now().UTC()
		ownerID = uuid.New().String()
		expiresAt := now.Add(c.cfg.LeaseTTL)
		heartbeatStamp := now.Format(time.RFC3339Nano)
		snapshot := pendingLeaseSnapshot{
			Status:          "pending",
			State:           string(PendingStateLease),
			OwnerID:         ownerID,
			Version:         now.UnixNano(),
			LeaseExpiresAt:  expiresAt.Format(time.RFC3339Nano),
			AcquireAt:       heartbeatStamp,
			HeartbeatAt:     heartbeatStamp,
			UpdatedAt:       heartbeatStamp,
			Username:        trimmed,
			RequestID:       strings.TrimSpace(meta.RequestID),
			Operator:        strings.TrimSpace(meta.Operator),
			ClientIP:        strings.TrimSpace(meta.ClientIP),
			LegacyRequestID: strings.TrimSpace(meta.LegacyRequestID),
		}

		payload, err := json.Marshal(&snapshot)
		if err != nil {
			return nil, fmt.Errorf("marshal pending snapshot: %w", err)
		}

		basePayload := string(payload)
		outcome, attemptErr := c.tryPendingAcquire(ctx, trimmed, key, snapshot, basePayload)
		if attemptErr != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_error").Inc()
			}
			c.logLeaseEvent("error", "pending lease acquire execution failed", "username", trimmed, "error", attemptErr)
			trace.AddRequestTag(ctx, "acquire_fail_reason", "execution_error")
			trace.AddRequestTag(ctx, "fallback_strategy", "none")
			return nil, attemptErr
		}
		setNXDuration := outcome.setDuration
		snapshot = outcome.snapshot

		if !outcome.created {
			state, observeErr := c.Observe(ctx, trimmed)
			if observeErr != nil {
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(c.component, "observe_error").Inc()
				}
				c.logLeaseEvent("error", "pending lease observe failed", "username", trimmed, "error", observeErr)
				trace.AddRequestTag(ctx, "acquire_fail_reason", "observe_error")
				trace.AddRequestTag(ctx, "fallback_strategy", "none")
				return nil, observeErr
			}
			if c.shouldPromoteExpired(state) {
				promoted, promoteErr := c.promoteExpired(ctx, trimmed, state)
				if promoteErr != nil {
					err = promoteErr
					return nil, promoteErr
				}
				promoteExpired = promoteExpired || promoted
				updatedState, refreshedErr := c.Observe(ctx, trimmed)
				if refreshedErr == nil {
					state = updatedState
				} else {
					c.logLeaseEvent("debug", "pending lease observe after expire promotion failed", "username", trimmed, "error", refreshedErr)
				}
			}
			if state != nil && state.State == PendingStateReleased {
				if cleanupErr := c.cleanupReleasedState(ctx, trimmed); cleanupErr != nil {
					c.logLeaseEvent("warn", "failed to cleanup released pending lease", "username", trimmed, "error", cleanupErr)
					return nil, cleanupErr
				}
				c.logLeaseEvent("debug", "removed released pending lease before retry", "username", trimmed, "attempt", attempt+1)
				if attempt < releasedStateRetryLimit {
					continue
				}
			}
			if state != nil && state.State == PendingStateExpired {
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_conflict_expired").Inc()
				}
				expiredAt := ""
				if !state.ExpiredAt.IsZero() {
					expiredAt = state.ExpiredAt.Format(time.RFC3339Nano)
				}
				fields := []interface{}{"username", trimmed, "queue_depth", state.QueueDepth, "backpressure", string(state.Backpressure)}
				if expiredAt != "" {
					fields = append(fields, "expired_at", expiredAt)
				}
				if state.ExpiredReason != "" {
					fields = append(fields, "expired_reason", state.ExpiredReason)
				}
				c.logLeaseEvent("warn", "pending lease acquisition blocked by expired state", fields...)
				finalDetails = map[string]interface{}{
					"username":            trimmed,
					"acquire_result":      "expired_conflict",
					"queue_depth_global":  state.QueueDepth,
					"backpressure_level":  string(state.Backpressure),
					"acquire_fail_reason": "expired_conflict",
					"fallback_strategy":   "none",
				}
				trace.AddRequestTag(ctx, "acquire_fail_reason", "expired_conflict")
				trace.AddRequestTag(ctx, "fallback_strategy", "none")
				err = &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: state.QueueDepth}
				return nil, err
			}
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_conflict").Inc()
			}
			queueDepth := int64(0)
			level := BackpressureNone
			owner := ""
			if state != nil {
				queueDepth = state.QueueDepth
				owner = state.LeaseOwner
				if state.Backpressure != "" {
					level = state.Backpressure
				}
				c.recordQueueDepthMetrics(queueDepth, level)
			}
			c.logLeaseEvent("info", "pending lease acquire conflict", "username", trimmed, "owner", owner, "queue_depth", queueDepth, "backpressure", string(level))
			finalDetails = map[string]interface{}{
				"username":            trimmed,
				"acquire_result":      "conflict",
				"queue_depth_global":  queueDepth,
				"lease_owner":         owner,
				"backpressure_level":  string(level),
				"acquire_fail_reason": "conflict",
				"fallback_strategy":   "none",
			}
			trace.AddRequestTag(ctx, "acquire_fail_reason", "conflict")
			trace.AddRequestTag(ctx, "fallback_strategy", "none")
			err = &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: queueDepth}
			return nil, err
		}

		queueDepth := outcome.queueDepth
		level := outcome.globalLevel
		userQueueDepth := outcome.userQueueDepth
		userLevelCurrent := outcome.userLevel
		instanceDepthCurrent := outcome.instanceDepth
		instanceLevelCurrent := outcome.instanceLevel
		aggregateLevel = outcome.aggregateLevel
		finalPayload := outcome.finalPayload
		userDepthErr := outcome.userDepthErr
		if userDepthErr != nil {
			c.logLeaseEvent("warn", "pending lease increment user depth failed", "username", trimmed, "error", userDepthErr)
		}

		if aggregateLevel == BackpressureSevere {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "backpressure_reject").Inc()
			}
			c.logLeaseEvent("warn", "pending lease rejected by severe backpressure", "username", trimmed, "owner", ownerID, "queue_depth", queueDepth, "backpressure", string(aggregateLevel), "global_backpressure", string(level), "user_queue_depth", userQueueDepth, "user_backpressure", string(userLevelCurrent), "instance_queue_depth", instanceDepthCurrent, "instance_backpressure", string(instanceLevelCurrent))
			if _, releaseErr := c.Release(ctx, trimmed, ownerID); releaseErr != nil {
				log.Warnw("rollback pending lease after severe backpressure failed", "username", trimmed, "error", releaseErr)
			}
			state := &PendingState{
				Exists:               true,
				State:                PendingStateValue(snapshot.State),
				LeaseOwner:           ownerID,
				Version:              snapshot.Version,
				Username:             trimmed,
				Backpressure:         aggregateLevel,
				QueueDepth:           queueDepth,
				UserQueueDepth:       userQueueDepth,
				InstanceQueueDepth:   instanceDepthCurrent,
				UserBackpressure:     userLevelCurrent,
				InstanceBackpressure: instanceLevelCurrent,
				Raw:                  finalPayload,
			}
			finalDetails = map[string]interface{}{
				"username":             trimmed,
				"acquire_result":       "backpressure_reject_created",
				"queue_depth_global":   queueDepth,
				"queue_depth_user":     userQueueDepth,
				"queue_depth_instance": instanceDepthCurrent,
				"backpressure_level":   string(aggregateLevel),
				"lease_owner":          ownerID,
				"acquire_fail_reason":  "backpressure_reject",
				"fallback_strategy":    "release_and_abort",
			}
			trace.AddRequestTag(ctx, "acquire_fail_reason", "backpressure_reject")
			trace.AddRequestTag(ctx, "fallback_strategy", "release_and_abort")
			err = &AcquireError{Reason: AcquireFailureBackpressure, State: state, QueueDepth: queueDepth}
			return nil, err
		}

		fields := []interface{}{"username", trimmed, "owner", ownerID, "queue_depth", queueDepth, "backpressure", string(aggregateLevel), "global_backpressure", string(level), "lease_ttl_ms", c.cfg.LeaseTTL.Milliseconds(), "user_queue_depth", userQueueDepth, "user_backpressure", string(userLevelCurrent), "instance_queue_depth", instanceDepthCurrent, "instance_backpressure", string(instanceLevelCurrent)}
		if snapshot.RequestID != "" {
			fields = append(fields, "request_id", snapshot.RequestID)
		}
		if snapshot.Operator != "" {
			fields = append(fields, "operator", snapshot.Operator)
		}
		if snapshot.ClientIP != "" {
			fields = append(fields, "client_ip", snapshot.ClientIP)
		}
		if aggregateLevel != BackpressureNone {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "backpressure_elevated").Inc()
			}
			c.logLeaseEvent("warn", "pending lease acquired under backpressure", fields...)
		} else {
			c.logLeaseEvent("info", "pending lease acquired", fields...)
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "acquire_success").Inc()
		}
		if metrics.PendingLeaseActiveGauge != nil {
			metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Inc()
		}

		lease := &PendingLease{
			Username:       trimmed,
			OwnerID:        ownerID,
			Version:        snapshot.Version,
			AcquireAt:      now,
			LeaseExpiresAt: expiresAt,
			HeartbeatAt:    now,
			QueueDepth:     queueDepth,
			Backpressure:   aggregateLevel,
			Metadata:       meta,
		}
		finalDetails = map[string]interface{}{
			"username":             trimmed,
			"acquire_result":       "acquired",
			"queue_depth_global":   queueDepth,
			"queue_depth_user":     userQueueDepth,
			"queue_depth_instance": instanceDepthCurrent,
			"backpressure_level":   string(aggregateLevel),
			"lease_owner":          ownerID,
			"lease_ttl_ms":         leaseTTLMS,
			"promote_expired":      promoteExpired,
		}
		trace.AddRequestTag(ctx, "pending_lease_id", ownerID)
		if leaseTTLMS > 0 {
			trace.AddRequestTag(ctx, "pending_lease_ttl_ms", leaseTTLMS)
		}
		return &AcquireResult{Lease: lease, SetNXDuration: setNXDuration}, nil
	}

	return nil, errors.New("pending lease acquisition retries exhausted")
}

func (c *PendingCoordinator) Release(ctx context.Context, username string, ownerID string) (duration time.Duration, err error) {
	if c == nil {
		return 0, nil
	}
	ctx, span := startPendingSpan(ctx, "pending_release")
	status := "success"
	code := "0"
	var details map[string]interface{}
	releaseKey := PendingCreateKey(strings.TrimSpace(username))
	defer func() {
		if err != nil {
			status = "error"
			code = err.Error()
		}
		if details == nil {
			details = make(map[string]interface{})
		}
		if releaseKey != "" {
			details["lease_key"] = releaseKey
		}
		finishPendingSpan(span, status, code, details)
	}()
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("release", username)
		if fallback := c.ensureFallback(); fallback != nil {
			duration, err = fallback.Release(ctx, username, ownerID)
			return duration, err
		}
		return 0, nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			duration, err = c.fallback.Release(ctx, username, ownerID)
			return duration, err
		}
		return 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}
	if releaseKey == "" {
		return 0, nil
	}

	deleteCtx, cancel := c.newOpContext(ctx)
	deleteStart := time.Now()
	var deleted bool
	var snapshot *pendingLeaseSnapshot
	deleted, snapshot, err = c.deleteKeyWithOwner(deleteCtx, releaseKey, ownerID)
	cancel()
	duration = time.Since(deleteStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_delete", duration.Seconds(), metricErr)
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil {
		return duration, err
	}

	if details == nil {
		details = make(map[string]interface{})
	}
	details["release_result"] = "deleted"
	details["release_ms"] = duration.Milliseconds()
	details["lease_owner"] = ownerID
	if !deleted {
		details["release_result"] = "missing"
	}

	var holdDuration time.Duration
	if snapshot != nil && snapshot.AcquireAt != "" {
		if acquiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.AcquireAt); parseErr == nil {
			holdDuration = time.Since(acquiredAt)
			if holdDuration < 0 {
				holdDuration = 0
			}
		} else {
			c.logLeaseEvent("debug", "failed to parse pending lease acquire timestamp", "username", trimmed, "owner", ownerID, "error", parseErr)
		}
	}
	fallbackDepth := int64(0)
	fallbackUserDepth := int64(0)
	if snapshot != nil {
		fallbackDepth = snapshot.QueueDepth
		fallbackUserDepth = snapshot.UserQueueDepth
	}

	if !deleted {
		if snapshot != nil && snapshot.OwnerID != "" && ownerID != "" && snapshot.OwnerID != ownerID {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_owner_mismatch").Inc()
			}
			c.logLeaseEvent("warn", "pending lease release skipped due to owner mismatch", "username", trimmed, "owner", ownerID, "current_owner", snapshot.OwnerID)
			err = ErrPendingLeaseOwnerMismatch
			return duration, err
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_miss").Inc()
		}
		c.logLeaseEvent("debug", "pending lease release no-op", "username", trimmed, "owner", ownerID)
		details = map[string]interface{}{
			"username":           trimmed,
			"lease_owner":        ownerID,
			"acquire_result":     "release_miss",
			"queue_depth_global": fallbackDepth,
		}
		return duration, nil
	}

	if metrics.PendingLeaseActiveGauge != nil {
		metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Dec()
	}
	details = map[string]interface{}{
		"username":           trimmed,
		"lease_owner":        ownerID,
		"release_result":     "released",
		"queue_depth_global": fallbackDepth,
		"queue_depth_user":   fallbackUserDepth,
		"hold_duration_ms":   holdDuration.Milliseconds(),
	}
	remaining, decErr := c.decrementCounterWithRetry(ctx, c.cfg.MetricsKey, "release", fallbackDepth, "username", trimmed, "owner", ownerID)
	if decErr != nil {
		if metrics.PendingLeaseActiveGauge != nil {
			metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Inc()
		}
		err = decErr
		return duration, err
	}
	userRemaining, userDecErr := c.decrementUserQueueDepth(ctx, trimmed, fallbackUserDepth)
	if userDecErr != nil {
		c.logLeaseEvent("warn", "pending lease user depth decrement failed", "username", trimmed, "owner", ownerID, "error", userDecErr)
	}
	instanceRemaining := c.decInstanceActive()
	newInstanceLevel := c.classifyInstanceBackpressure(instanceRemaining)
	newLevel := c.classifyBackpressure(remaining)
	userLevelAfter := c.classifyUserBackpressure(userRemaining)
	if userDecErr != nil {
		c.userSampleCache.Delete(trimmed)
	} else {
		c.storeUserSample(trimmed, userRemaining, userLevelAfter)
	}
	c.storeGlobalSample(remaining, newLevel)
	aggregateAfterRelease := c.decideAggregateLevel(newLevel, userLevelAfter, newInstanceLevel, time.Now())
	c.recordQueueDepthMetrics(remaining, newLevel)
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_success").Inc()
	}
	if holdDuration > 0 && metrics.PendingLeaseHoldDuration != nil {
		metrics.PendingLeaseHoldDuration.WithLabelValues(c.component, "success").Observe(holdDuration.Seconds())
	}

	initialDepth := fallbackDepth
	initialUserDepth := fallbackUserDepth
	fields := []interface{}{"username", trimmed, "owner", ownerID, "initial_queue_depth", initialDepth, "remaining_queue_depth", remaining, "backpressure", string(newLevel), "aggregate_backpressure", string(aggregateAfterRelease)}
	if initialUserDepth > 0 || userRemaining > 0 {
		fields = append(fields, "initial_user_queue_depth", initialUserDepth, "remaining_user_queue_depth", userRemaining, "user_backpressure", string(userLevelAfter))
	}
	if instanceRemaining >= 0 {
		fields = append(fields, "instance_queue_depth", instanceRemaining, "instance_backpressure", string(newInstanceLevel))
	}
	if holdDuration > 0 {
		fields = append(fields, "hold_ms", holdDuration.Milliseconds())
	}
	c.logLeaseEvent("info", "pending lease released", fields...)
	if snapshot != nil {
		snapshot.UserQueueDepth = userRemaining
		snapshot.InstanceQueueDepth = instanceRemaining
		snapshot.UserBackpressure = string(userLevelAfter)
		snapshot.InstanceBackpressure = string(newInstanceLevel)
	}

	if err := c.writeReleaseSnapshot(ctx, trimmed, snapshot, ownerID, remaining); err != nil {
		c.logLeaseEvent("warn", "write release snapshot failed", "username", trimmed, "error", err)
	}
	details = map[string]interface{}{
		"username":                 trimmed,
		"lease_owner":              ownerID,
		"acquire_result":           "released",
		"queue_depth_initial":      fallbackDepth,
		"queue_depth_remaining":    remaining,
		"user_queue_depth_initial": fallbackUserDepth,
		"user_queue_depth":         userRemaining,
		"backpressure_level":       string(newLevel),
		"aggregate_backpressure":   string(aggregateAfterRelease),
	}
	trace.AddRequestTag(ctx, "pending_lease_id", ownerID)
	return duration, nil
}

func (c *PendingCoordinator) Heartbeat(ctx context.Context, username string, ownerID string) error {
	if c == nil {
		return nil
	}
	trimmed := strings.TrimSpace(username)
	trimmedOwner := strings.TrimSpace(ownerID)
	if trimmed == "" || trimmedOwner == "" {
		return nil
	}
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("heartbeat", trimmed)
		if fallback := c.ensureFallback(); fallback != nil {
			return fallback.Heartbeat(ctx, trimmed, trimmedOwner)
		}
		return nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			return c.fallback.Heartbeat(ctx, trimmed, trimmedOwner)
		}
		return nil
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return nil
	}
	getCtx, cancel := c.newOpContext(ctx)
	getStart := time.Now()
	raw, err := c.redis.GetKeyWithCommandTimeout(getCtx, key, c.cfg.CommandTimeout)
	cancel()
	getDuration := time.Since(getStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_heartbeat_get", getDuration.Seconds(), metricErr)
	if errors.Is(err, redis.Nil) {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "heartbeat_miss").Inc()
		}
		return ErrPendingLeaseConflict
	}
	if err != nil {
		return err
	}
	var snapshot pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr != nil {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "heartbeat_decode_error").Inc()
		}
		return decodeErr
	}
	if strings.TrimSpace(snapshot.State) != string(PendingStateLease) {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "heartbeat_invalid_state").Inc()
		}
		return ErrPendingLeaseConflict
	}
	existingOwner := strings.TrimSpace(snapshot.OwnerID)
	if existingOwner != "" && existingOwner != trimmedOwner {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "heartbeat_owner_mismatch").Inc()
		}
		return ErrPendingLeaseOwnerMismatch
	}
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	snapshot.OwnerID = trimmedOwner
	snapshot.HeartbeatAt = stamp
	snapshot.UpdatedAt = stamp
	if c.cfg.LeaseTTL > 0 {
		snapshot.LeaseExpiresAt = now.Add(c.cfg.LeaseTTL).Format(time.RFC3339Nano)
	}
	payload, marshalErr := json.Marshal(&snapshot)
	if marshalErr != nil {
		return marshalErr
	}
	setCtx, setCancel := c.newOpContext(ctx)
	setStart := time.Now()
	setErr := c.redis.SetKeyWithCommandTimeout(setCtx, key, string(payload), c.cfg.LeaseTTL, c.cfg.CommandTimeout)
	setCancel()
	metrics.RecordRedisOperation("pending_lease_heartbeat_set", time.Since(setStart).Seconds(), setErr)
	if setErr != nil {
		return setErr
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "heartbeat_success").Inc()
	}
	c.logLeaseEvent("debug", "pending lease heartbeat refreshed", "username", trimmed, "owner", trimmedOwner)
	return nil
}

// 观察指定用户的待处理租约状态
// 返回租约状态详情，包括存在性、状态、持有者、版本、队列深度等信息
// 如果租约不存在，返回状态为 Unknown
// 如果租约存在且可解析，填充详细字段
// 如果租约存在但不可解析，返回原始数据
// 记录相关的 Redis 操作指标和日志
func (c *PendingCoordinator) Observe(ctx context.Context, username string) (*PendingState, error) {
	result := &PendingState{State: PendingStateUnknown}
	if c == nil {
		return result, nil
	}
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("observe", username)
		if fallback := c.ensureFallback(); fallback != nil {
			return fallback.Observe(ctx, username)
		}
		return result, nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			return c.fallback.Observe(ctx, username)
		}
		return result, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return result, nil
	}
	result.Username = trimmed
	key := PendingCreateKey(trimmed)
	if key == "" {
		return result, nil
	}

	getCtx, cancel := c.newOpContext(ctx)
	getStart := time.Now()
	raw, err := c.redis.GetKeyWithCommandTimeout(getCtx, key, c.cfg.CommandTimeout)
	cancel()
	getDuration := time.Since(getStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_get", getDuration.Seconds(), metricErr)
	result.RedisGetDuration = getDuration
	if errors.Is(err, redis.Nil) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	result.Exists = true
	result.Raw = raw
	var snapshot pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr == nil {
		result.State = PendingStateValue(snapshot.State)
		result.LeaseOwner = snapshot.OwnerID
		result.Version = snapshot.Version
		result.Backpressure = BackpressureLevel(snapshot.Backpressure)
		result.QueueDepth = snapshot.QueueDepth
		result.UserQueueDepth = snapshot.UserQueueDepth
		result.InstanceQueueDepth = snapshot.InstanceQueueDepth
		if snapshot.UserBackpressure != "" {
			result.UserBackpressure = BackpressureLevel(snapshot.UserBackpressure)
		}
		if snapshot.InstanceBackpressure != "" {
			result.InstanceBackpressure = BackpressureLevel(snapshot.InstanceBackpressure)
		}
		if snapshot.Username != "" {
			result.Username = snapshot.Username
		}
		if snapshot.LeaseExpiresAt != "" {
			if expiresAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.LeaseExpiresAt); parseErr == nil {
				result.LeaseExpiresAt = expiresAt
			}
		}
		if snapshot.ReleasedAt != "" {
			if releasedAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.ReleasedAt); parseErr == nil {
				result.ReleasedAt = releasedAt
			}
		}
		if snapshot.HeartbeatAt != "" {
			if heartbeatAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.HeartbeatAt); parseErr == nil {
				result.HeartbeatAt = heartbeatAt
			}
		}
		if snapshot.ExpiredAt != "" {
			if expiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.ExpiredAt); parseErr == nil {
				result.ExpiredAt = expiredAt
			}
		}
		if snapshot.ExpiredReason != "" {
			result.ExpiredReason = snapshot.ExpiredReason
		}
		if snapshot.Degraded && result.Backpressure == BackpressureNone {
			result.Backpressure = BackpressureElevated
		}
		if result.UserBackpressure == "" {
			result.UserBackpressure = c.classifyUserBackpressure(result.UserQueueDepth)
		}
		if result.InstanceBackpressure == "" {
			result.InstanceBackpressure = c.classifyInstanceBackpressure(result.InstanceQueueDepth)
		}
	}
	if result.Exists {
		c.recordQueueDepthMetrics(result.QueueDepth, result.Backpressure)
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "observe").Inc()
		}
		c.logLeaseEvent("debug", "pending lease observed", "username", trimmed, "owner", result.LeaseOwner, "queue_depth", result.QueueDepth, "backpressure", string(result.Backpressure))
	}

	ttlCtx, ttlCancel := c.newOpContext(ctx)
	ttlStart := time.Now()
	// 获取租约的剩余 TTL
	ttlSeconds, ttlErr := c.redis.GetExp(ttlCtx, key)
	ttlCancel()
	ttlDuration := time.Since(ttlStart)
	ttlMetricErr := ttlErr
	if ttlErr == storage.ErrKeyNotFound {
		ttlMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_ttl", ttlDuration.Seconds(), ttlMetricErr)
	result.RedisTTLDur = ttlDuration
	if ttlErr == nil && ttlSeconds > 0 {
		result.TTL = time.Duration(ttlSeconds) * time.Second
	}
	if ttlErr != nil && ttlErr != storage.ErrKeyNotFound {
		return result, ttlErr
	}
	return result, nil
}

func (c *PendingCoordinator) cleanupReleasedState(ctx context.Context, username string) error {
	if c == nil || c.redis == nil {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	cleanupCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	_, err := c.redis.DeleteKeyWithCommandTimeout(cleanupCtx, key, c.cfg.CommandTimeout)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "cleanup_released").Inc()
	}
	return nil
}

func (c *PendingCoordinator) cleanupExpiredState(ctx context.Context, username string) error {
	if c == nil || c.redis == nil {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	cleanupCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	_, err := c.redis.DeleteKeyWithCommandTimeout(cleanupCtx, key, c.cfg.CommandTimeout)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "cleanup_expired").Inc()
	}
	return nil
}

func (c *PendingCoordinator) CleanupExpired(ctx context.Context, username string) error {
	return c.cleanupExpiredState(ctx, username)
}

func (c *PendingCoordinator) promoteExpired(ctx context.Context, username string, state *PendingState) (bool, error) {
	if c == nil || c.redis == nil || state == nil {
		return false, nil
	}
	var snapshot pendingLeaseSnapshot
	if err := json.Unmarshal([]byte(state.Raw), &snapshot); err != nil {
		c.logLeaseEvent("debug", "failed to decode pending lease snapshot before expire", "username", username, "error", err)
		return false, err
	}
	holdDuration := time.Duration(0)
	if snapshot.AcquireAt != "" {
		if acquiredAt, parseErr := time.Parse(time.RFC3339Nano, snapshot.AcquireAt); parseErr == nil {
			holdDuration = time.Since(acquiredAt)
			if holdDuration < 0 {
				holdDuration = 0
			}
		}
	}
	if metrics.PendingLeaseActiveGauge != nil {
		metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Dec()
	}
	remaining, decErr := c.decrementCounterWithRetry(ctx, c.cfg.MetricsKey, "expire", state.QueueDepth, "username", username)
	if decErr != nil {
		remaining = state.QueueDepth
	}
	if _, userDecErr := c.decrementUserQueueDepth(ctx, username, state.UserQueueDepth); userDecErr != nil {
		c.logLeaseEvent("warn", "pending lease user depth decrement failed while marking expired", "username", username, "error", userDecErr)
	}
	newLevel := c.classifyBackpressure(remaining)
	c.recordQueueDepthMetrics(remaining, newLevel)
	if holdDuration > 0 && metrics.PendingLeaseHoldDuration != nil {
		metrics.PendingLeaseHoldDuration.WithLabelValues(c.component, "expired").Observe(holdDuration.Seconds())
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "expire_promote").Inc()
	}
	reason := c.classifyExpiredReason(state, time.Now())
	if snapshot.ExpiredReason == "" && reason != "" {
		snapshot.ExpiredReason = reason
	}
	if snapshot.HeartbeatAt == "" && !state.HeartbeatAt.IsZero() {
		snapshot.HeartbeatAt = state.HeartbeatAt.Format(time.RFC3339Nano)
	}
	if err := c.writeExpiredSnapshot(ctx, username, &snapshot, remaining); err != nil {
		return false, err
	}
	fields := []interface{}{"username", username, "queue_depth", state.QueueDepth, "remaining_queue_depth", remaining}
	if holdDuration > 0 {
		fields = append(fields, "hold_ms", holdDuration.Milliseconds())
	}
	c.logLeaseEvent("warn", "pending lease expired without release", fields...)
	return true, nil
}

func (c *PendingCoordinator) heartbeatExpired(state *PendingState, now time.Time) bool {
	if c == nil || state == nil {
		return false
	}
	timeout := c.cfg.HeartbeatTimeout
	if timeout <= 0 {
		return false
	}
	last := state.HeartbeatAt
	if last.IsZero() && !state.LeaseExpiresAt.IsZero() && c.cfg.LeaseTTL > 0 {
		approx := state.LeaseExpiresAt.Add(-c.cfg.LeaseTTL)
		if approx.Before(state.LeaseExpiresAt) {
			last = approx
		}
	}
	if last.IsZero() {
		return false
	}
	return now.Sub(last) >= timeout
}

func (c *PendingCoordinator) classifyExpiredReason(state *PendingState, now time.Time) string {
	if c.heartbeatExpired(state, now) {
		return "heartbeat_timeout"
	}
	return "lease_timeout"
}

func (c *PendingCoordinator) shouldPromoteExpired(state *PendingState) bool {
	if c == nil || state == nil {
		return false
	}
	if state.State != PendingStateLease {
		return false
	}
	now := time.Now()
	if c.heartbeatExpired(state, now) {
		return true
	}
	if !state.LeaseExpiresAt.IsZero() {
		expiry := state.LeaseExpiresAt.Add(c.cfg.ExpiredGracePeriod)
		if now.After(expiry) {
			return true
		}
	}
	if state.TTL <= 0 {
		return true
	}
	return false
}

func (c *PendingCoordinator) writeReleaseSnapshot(ctx context.Context, username string, snapshot *pendingLeaseSnapshot, owner string, remaining int64) error {
	if c == nil || c.redis == nil {
		return nil
	}
	ttl := c.releaseRetentionTTL()
	if ttl <= 0 {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	now := time.Now().UTC()
	trimmedOwner := strings.TrimSpace(owner)
	resolvedOwner := trimmedOwner
	if resolvedOwner == "" && snapshot != nil {
		candidate := strings.TrimSpace(snapshot.OwnerID)
		if candidate != "" {
			resolvedOwner = candidate
			c.logLeaseEvent("debug", "pending lease release owner recovered", "username", username, "owner", resolvedOwner)
		}
	}
	released := pendingLeaseSnapshot{
		Status:             "completed",
		State:              string(PendingStateReleased),
		OwnerID:            resolvedOwner,
		Version:            now.UnixNano(),
		LeaseExpiresAt:     now.Add(ttl).Format(time.RFC3339Nano),
		AcquireAt:          "",
		UpdatedAt:          now.Format(time.RFC3339Nano),
		QueueDepth:         remaining,
		Backpressure:       "",
		UserQueueDepth:     0,
		InstanceQueueDepth: 0,
		Username:           username,
		ReleasedAt:         now.Format(time.RFC3339Nano),
		ReleasedBy:         resolvedOwner,
	}
	if snapshot != nil {
		if snapshot.RequestID != "" {
			released.RequestID = snapshot.RequestID
		}
		if snapshot.Operator != "" {
			released.Operator = snapshot.Operator
		}
		if snapshot.ClientIP != "" {
			released.ClientIP = snapshot.ClientIP
		}
		if snapshot.LegacyRequestID != "" {
			released.LegacyRequestID = snapshot.LegacyRequestID
		}
		if snapshot.HeartbeatAt != "" {
			released.HeartbeatAt = snapshot.HeartbeatAt
		}
		if snapshot.AcquireAt != "" {
			released.AcquireAt = snapshot.AcquireAt
		}
		if snapshot.Backpressure != "" {
			released.Backpressure = snapshot.Backpressure
		}
		if snapshot.UserQueueDepth > 0 {
			released.UserQueueDepth = snapshot.UserQueueDepth
		}
		if snapshot.InstanceQueueDepth > 0 {
			released.InstanceQueueDepth = snapshot.InstanceQueueDepth
		}
		if snapshot.UserBackpressure != "" {
			released.UserBackpressure = snapshot.UserBackpressure
		}
		if snapshot.InstanceBackpressure != "" {
			released.InstanceBackpressure = snapshot.InstanceBackpressure
		}
		if released.OwnerID == "" && strings.TrimSpace(snapshot.OwnerID) != "" {
			released.OwnerID = strings.TrimSpace(snapshot.OwnerID)
			released.ReleasedBy = strings.TrimSpace(snapshot.OwnerID)
		}
	}
	if released.OwnerID == "" {
		c.logLeaseEvent("warn", "pending lease release snapshot missing owner", "username", username)
	}

	payload, err := json.Marshal(&released)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKeyWithCommandTimeout(writeCtx, key, string(payload), ttl, c.cfg.CommandTimeout); err != nil {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "write_released").Inc()
	}
	c.logLeaseEvent("debug", "pending lease release snapshot written", "username", username, "ttl_ms", ttl.Milliseconds())
	return nil
}

func (c *PendingCoordinator) releaseRetentionTTL() time.Duration {
	if c == nil {
		return 0
	}
	ttl := c.cfg.ReleaseRetention
	if ttl <= 0 {
		return 0
	}
	if ttl > c.cfg.LeaseTTL {
		return c.cfg.LeaseTTL
	}
	return ttl
}

func (c *PendingCoordinator) writeExpiredSnapshot(ctx context.Context, username string, snapshot *pendingLeaseSnapshot, remaining int64) error {
	if c == nil || c.redis == nil {
		return nil
	}
	ttl := c.expiredRetentionTTL()
	if ttl <= 0 {
		return nil
	}
	key := PendingCreateKey(username)
	if key == "" {
		return nil
	}
	now := time.Now().UTC()
	expired := pendingLeaseSnapshot{
		Status:             "failed",
		State:              string(PendingStateExpired),
		OwnerID:            "",
		Version:            now.UnixNano(),
		LeaseExpiresAt:     now.Add(ttl).Format(time.RFC3339Nano),
		AcquireAt:          "",
		UpdatedAt:          now.Format(time.RFC3339Nano),
		QueueDepth:         remaining,
		Backpressure:       "",
		UserQueueDepth:     0,
		InstanceQueueDepth: 0,
		Username:           username,
		ExpiredAt:          now.Format(time.RFC3339Nano),
		ExpiredReason:      "lease_timeout",
	}
	if snapshot != nil {
		if snapshot.RequestID != "" {
			expired.RequestID = snapshot.RequestID
		}
		if snapshot.Operator != "" {
			expired.Operator = snapshot.Operator
		}
		if snapshot.ClientIP != "" {
			expired.ClientIP = snapshot.ClientIP
		}
		if snapshot.LegacyRequestID != "" {
			expired.LegacyRequestID = snapshot.LegacyRequestID
		}
		if snapshot.AcquireAt != "" {
			expired.AcquireAt = snapshot.AcquireAt
		}
		if snapshot.Backpressure != "" {
			expired.Backpressure = snapshot.Backpressure
		}
		if snapshot.QueueDepth > 0 && remaining == 0 {
			expired.QueueDepth = snapshot.QueueDepth
		}
		if snapshot.UserQueueDepth > 0 {
			expired.UserQueueDepth = snapshot.UserQueueDepth
		}
		if snapshot.InstanceQueueDepth > 0 {
			expired.InstanceQueueDepth = snapshot.InstanceQueueDepth
		}
		if snapshot.UserBackpressure != "" {
			expired.UserBackpressure = snapshot.UserBackpressure
		}
		if snapshot.InstanceBackpressure != "" {
			expired.InstanceBackpressure = snapshot.InstanceBackpressure
		}
		if strings.TrimSpace(snapshot.OwnerID) != "" {
			expired.OwnerID = strings.TrimSpace(snapshot.OwnerID)
		}
		if snapshot.ExpiredReason != "" {
			expired.ExpiredReason = snapshot.ExpiredReason
		}
		if snapshot.HeartbeatAt != "" {
			expired.HeartbeatAt = snapshot.HeartbeatAt
		}
	}

	payload, err := json.Marshal(&expired)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKeyWithCommandTimeout(writeCtx, key, string(payload), ttl, c.cfg.CommandTimeout); err != nil {
		return err
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "write_expired").Inc()
	}
	c.logLeaseEvent("warn", "pending lease marked as expired", "username", username, "ttl_ms", ttl.Milliseconds())
	return nil
}

func (c *PendingCoordinator) expiredRetentionTTL() time.Duration {
	if c == nil {
		return 0
	}
	ttl := c.cfg.ExpiredRetention
	if ttl <= 0 {
		return 0
	}
	maxRetention := 2 * c.cfg.LeaseTTL
	if maxRetention <= 0 {
		maxRetention = c.cfg.LeaseTTL
	}
	if maxRetention > 0 && ttl > maxRetention {
		return maxRetention
	}
	return ttl
}

func (c *PendingCoordinator) deleteKeyWithOwner(ctx context.Context, key, ownerID string) (bool, *pendingLeaseSnapshot, error) {
	getCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	raw, err := c.redis.GetKeyWithCommandTimeout(getCtx, key, c.cfg.CommandTimeout)
	if errors.Is(err, redis.Nil) {
		return false, nil, redis.Nil
	}
	if err != nil {
		return false, nil, err
	}
	var snapshot pendingLeaseSnapshot
	var snapshotPtr *pendingLeaseSnapshot
	if decodeErr := json.Unmarshal([]byte(raw), &snapshot); decodeErr == nil {
		snapshotPtr = &snapshot
		if ownerID != "" && snapshot.OwnerID != "" && snapshot.OwnerID != ownerID {
			return false, snapshotPtr, nil
		}
	} else {
		c.logLeaseEvent("debug", "failed to decode pending lease snapshot before delete", "key", key, "error", decodeErr)
	}
	deleteCtx, deleteCancel := c.newOpContext(ctx)
	defer deleteCancel()
	deleted, deleteErr := c.redis.DeleteKeyWithCommandTimeout(deleteCtx, key, c.cfg.CommandTimeout)
	return deleted, snapshotPtr, deleteErr
}

// classifyBackpressure 根据当前配置和队列深度分类全局背压等级
func (c *PendingCoordinator) classifyBackpressure(depth int64) BackpressureLevel {
	return classifyDepthWithProfile(depth, c.currentProfile(), c.cfg.BackpressureSoftLimit, c.cfg.BackpressureHardLimit)
}

const decrementActiveLua = `
local current = redis.call("GET", KEYS[1])
if not current then
    return 0
end
current = tonumber(current)
if not current then
    redis.call("DEL", KEYS[1])
    return 0
end
if current <= 1 then
    redis.call("DEL", KEYS[1])
    return 0
end
local newVal = redis.call("DECR", KEYS[1])
if not newVal then
    redis.call("DEL", KEYS[1])
    return 0
end
if newVal < 0 then
    redis.call("DEL", KEYS[1])
    return 0
end
return newVal
`

func (c *PendingCoordinator) safeDecrementCounter(ctx context.Context, key string) (int64, error) {
	if c == nil || c.redis == nil {
		return 0, errors.New("pending coordinator not initialized")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return 0, errors.New("counter key empty")
	}
	evalCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	start := time.Now()
	result, err := c.redis.Eval(evalCtx, decrementActiveLua, []string{key}, nil)
	metrics.RecordRedisOperation("pending_lease_decrement_eval", time.Since(start).Seconds(), err)
	if err != nil {
		return 0, err
	}
	switch v := result.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		parsed, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected decrement result type %T", result)
	}
}

// decrementCounterWithRetry 在减少计数失败时做有限重试，多次失败后触发异步修正。
func (c *PendingCoordinator) decrementCounterWithRetry(ctx context.Context, key, scope string, fallback int64, logFields ...interface{}) (int64, error) {
	if c == nil {
		return fallback, errors.New("pending coordinator is nil")
	}
	var lastErr error
	for attempt := 1; attempt <= queueDepthDecrementMaxRetries; attempt++ {
		remaining, err := c.safeDecrementCounter(ctx, key)
		if err == nil {
			if attempt > 1 && metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_retry_success").Inc()
			}
			return remaining, nil
		}
		lastErr = err
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_retry").Inc()
		}
		fields := append([]interface{}{"scope", scope, "attempt", attempt, "error", err}, logFields...)
		c.logLeaseEvent("warn", "pending lease decrement retry", fields...)
		select {
		case <-ctx.Done():
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_cancelled").Inc()
			}
			return fallback, ctx.Err()
		case <-time.After(queueDepthDecrementRetryBase * time.Duration(attempt)):
		}
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_error").Inc()
	}
	fields := append([]interface{}{"scope", scope, "error", lastErr}, logFields...)
	c.logLeaseEvent("error", "pending lease decrement failed after retries", fields...)
	c.scheduleQueueDepthReconcile(key, scope, logFields...)
	if lastErr == nil {
		lastErr = errors.New("pending lease decrement failed after retries")
	}
	return fallback, lastErr
}

// scheduleQueueDepthReconcile 异步尝试再次修正队列计数，避免长时间漂移。
func (c *PendingCoordinator) scheduleQueueDepthReconcile(key, scope string, logFields ...interface{}) {
	if c == nil || c.redis == nil || strings.TrimSpace(key) == "" {
		return
	}
	if !c.queueDepthReconcileActive.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.queueDepthReconcileActive.Store(false)
		time.Sleep(queueDepthDecrementRetryBase * time.Duration(queueDepthDecrementMaxRetries))
		reconcileCtx, cancel := context.WithTimeout(context.Background(), queueDepthReconcileTimeout)
		defer cancel()
		remaining, err := c.safeDecrementCounter(reconcileCtx, key)
		if err != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_reconcile_failed").Inc()
			}
			fields := append([]interface{}{"scope", scope, "error", err}, logFields...)
			c.logLeaseEvent("error", "pending lease queue depth reconcile failed", fields...)
			return
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_decrement_reconcile_success").Inc()
		}
		level := c.classifyBackpressure(remaining)
		if scope == "user" {
			level = c.classifyUserBackpressure(remaining)
		} else if scope == "instance" {
			level = c.classifyInstanceBackpressure(remaining)
		} else {
			c.recordQueueDepthMetrics(remaining, level)
		}
		fields := append([]interface{}{"scope", scope, "remaining_queue_depth", remaining, "backpressure", string(level)}, logFields...)
		c.logLeaseEvent("info", "pending lease queue depth reconciled", fields...)
	}()
}

func (c *PendingCoordinator) startCalibrationLoop() {
	if c == nil {
		return
	}
	if c.calibrationStop == nil {
		c.calibrationStop = make(chan struct{})
	}
	if c.calibrationUpdateCh == nil {
		c.calibrationUpdateCh = make(chan struct{}, 1)
	}
	if time.Duration(c.calibrationIntervalNS.Load()) <= 0 {
		return
	}
	c.calibrationOnce.Do(func() {
		go c.runCalibrationLoop()
	})
}

func (c *PendingCoordinator) runCalibrationLoop() {
	for {
		interval := time.Duration(c.calibrationIntervalNS.Load())
		if interval <= 0 {
			select {
			case <-c.calibrationStop:
				return
			case <-c.calibrationUpdateCh:
				continue
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			timeout := c.currentCalibrationTimeout(interval)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			c.calibrateCounters(ctx)
			cancel()
		case <-c.calibrationStop:
			timer.Stop()
			return
		case <-c.calibrationUpdateCh:
			timer.Stop()
			continue
		}
		timer.Stop()
	}
}

func (c *PendingCoordinator) currentCalibrationTimeout(interval time.Duration) time.Duration {
	timeout := time.Duration(c.calibrationTimeoutNS.Load())
	if timeout <= 0 || (interval > 0 && timeout > interval) {
		timeout = minDuration(5*time.Second, interval/2)
		if timeout <= 0 {
			timeout = time.Second
		}
	}
	return timeout
}

func (c *PendingCoordinator) signalCalibrationUpdate() {
	if c == nil || c.calibrationUpdateCh == nil {
		return
	}
	select {
	case c.calibrationUpdateCh <- struct{}{}:
	default:
	}
}

// UpdateCalibration 允许在运行时调整校准间隔与超时，动态生效。
func (c *PendingCoordinator) UpdateCalibration(interval, timeout time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		interval = c.cfg.CalibrationInterval
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	c.cfg.CalibrationInterval = interval
	c.calibrationIntervalNS.Store(interval.Nanoseconds())
	if timeout <= 0 || (interval > 0 && timeout > interval) {
		timeout = minDuration(5*time.Second, interval/2)
		if timeout <= 0 {
			timeout = time.Second
		}
	}
	c.cfg.CalibrationTimeout = timeout
	c.calibrationTimeoutNS.Store(timeout.Nanoseconds())
	c.signalCalibrationUpdate()
	c.startCalibrationLoop()
}

// UpdateBackpressureProfile 允许在运行时替换背压延迟曲线，便于按需调整桶数量与阈值。
func (c *PendingCoordinator) UpdateBackpressureProfile(profile BackpressureDelayProfile) {
	if c == nil {
		return
	}
	ctx := context.Background()
	ctx, span := startPendingSpan(ctx, "pending_update_backpressure_profile")
	status := "success"
	code := "0"
	defer func() {
		finishPendingSpan(span, status, code, map[string]interface{}{
			"elevated_buckets": len(profile.Elevated),
			"severe_buckets":   len(profile.Severe),
		})
	}()
	profile.ensureDefaults(c.cfg.BackpressureSoftLimit, c.cfg.BackpressureHardLimit, c.cfg.ElevatedDelayBase, c.cfg.ElevatedDelayMax, c.cfg.SevereDelayBase, c.cfg.SevereDelayMax)
	cloned := profile.clone()
	c.cfg.BackpressureDelayProfile = cloned
	c.backpressureProfile.Store(cloned)
	if fallback := c.ensureFallback(); fallback != nil {
		fallback.UpdateBackpressureProfile(cloned)
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "backpressure_profile_update").Inc()
	}
	c.logLeaseEvent("info", "pending lease backpressure profile updated", "elevated_buckets", len(cloned.Elevated), "severe_buckets", len(cloned.Severe))
}

// Stop 用于服务退出时优雅停止后台校准循环。
func (c *PendingCoordinator) Stop() {
	if c == nil {
		return
	}
	c.calibrationStopOnce.Do(func() {
		if c.calibrationStop != nil {
			close(c.calibrationStop)
		}
	})
}

func (c *PendingCoordinator) calibrateCounters(ctx context.Context) {
	if c == nil || c.redis == nil {
		return
	}
	start := time.Now()
	resultLabel := "success"
	defer func() {
		if metrics.PendingLeaseCalibrationDuration != nil {
			metrics.PendingLeaseCalibrationDuration.WithLabelValues(c.component, resultLabel).Observe(time.Since(start).Seconds())
		}
	}()
	keys := c.redis.GetKeys(ctx, PendingCreatePrefix())
	if err := ctx.Err(); err != nil {
		resultLabel = "cancelled"
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_cancelled").Inc()
		}
		c.logLeaseEvent("debug", "pending lease calibration cancelled", "error", err)
		return
	}
	totalActive := int64(0)
	userCounts := make(map[string]int64)
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		username := usernameFromKeyWithPrefix(key, PendingCreatePrefix())
		if username == "" {
			continue
		}
		state, err := c.Observe(ctx, username)
		if err != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_observe_error").Inc()
			}
			c.logLeaseEvent("warn", "pending lease calibration observe failed", "username", username, "error", err)
			continue
		}
		if state == nil || !state.Exists {
			continue
		}
		if state.State != PendingStateLease {
			continue
		}
		if c.shouldPromoteExpired(state) {
			continue
		}
		totalActive++
		userCounts[username]++
	}
	c.applyCalibratedCounts(ctx, totalActive, userCounts)
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_run").Inc()
	}
	c.logLeaseEvent("debug", "pending lease calibration completed", "active", totalActive, "elapsed_ms", time.Since(start).Milliseconds())
}

func (c *PendingCoordinator) applyCalibratedCounts(ctx context.Context, global int64, userCounts map[string]int64) {
	if c == nil || c.redis == nil {
		return
	}
	metricsKey := strings.TrimSpace(c.cfg.MetricsKey)
	if metricsKey != "" {
		writeCtx, cancel := c.newOpContext(ctx)
		start := time.Now()
		err := c.redis.SetKeyWithCommandTimeout(writeCtx, metricsKey, strconv.FormatInt(global, 10), c.cfg.BackpressureWindow, c.cfg.CommandTimeout)
		cancel()
		metrics.RecordRedisOperation("pending_lease_calibration_set_global", time.Since(start).Seconds(), err)
		if err != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_update_failed").Inc()
			}
			c.logLeaseEvent("warn", "pending lease calibration update failed", "scope", "global", "error", err)
		} else {
			level := c.classifyBackpressure(global)
			c.recordQueueDepthMetrics(global, level)
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_update_success").Inc()
			}
		}
	}
	if userCounts == nil {
		userCounts = map[string]int64{}
	}
	for username, depth := range userCounts {
		if ctx.Err() != nil {
			break
		}
		key := c.pendingUserDepthKey(username)
		if key == "" {
			continue
		}
		value := strconv.FormatInt(depth, 10)
		writeCtx, cancel := c.newOpContext(ctx)
		start := time.Now()
		err := c.redis.SetKeyWithCommandTimeout(writeCtx, key, value, c.cfg.UserBackpressureWindow, c.cfg.CommandTimeout)
		cancel()
		metrics.RecordRedisOperation("pending_lease_calibration_set_user", time.Since(start).Seconds(), err)
		if err != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_user_update_failed").Inc()
			}
			c.logLeaseEvent("warn", "pending lease calibration user update failed", "username", username, "error", err)
		}
	}
	c.cleanupStaleUserDepthKeys(ctx, userCounts)
}

func (c *PendingCoordinator) cleanupStaleUserDepthKeys(ctx context.Context, active map[string]int64) {
	if c == nil || c.redis == nil {
		return
	}
	prefix := strings.TrimSpace(c.cfg.UserMetricsPrefix)
	if prefix == "" {
		return
	}
	normalizedPrefix := normalizeKeyPrefix(prefix)
	keys := c.redis.GetKeys(ctx, normalizedPrefix)
	for _, key := range keys {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		username := usernameFromKeyWithPrefix(key, normalizedPrefix)
		if username == "" {
			continue
		}
		if active != nil {
			if _, ok := active[username]; ok {
				continue
			}
		}
		deleteCtx, cancel := c.newOpContext(ctx)
		start := time.Now()
		_, err := c.redis.DeleteKeyWithCommandTimeout(deleteCtx, key, c.cfg.CommandTimeout)
		cancel()
		metrics.RecordRedisOperation("pending_lease_calibration_user_delete", time.Since(start).Seconds(), err)
		if err != nil && err != storage.ErrKeyNotFound && !errors.Is(err, redis.Nil) {
			c.logLeaseEvent("debug", "pending lease calibration skip stale delete", "username", username, "error", err)
			continue
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "calibration_user_key_cleanup").Inc()
		}
	}
}

func (c *PendingCoordinator) newOpContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := c.cfg.ObserveTimeout
	if timeout <= 0 {
		timeout = 700 * time.Millisecond
	}
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (c *PendingCoordinator) newCommandContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := c.cfg.CommandTimeout
	if timeout <= 0 {
		timeout = pendingCommandTimeoutDefault
	}
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

func (c *PendingCoordinator) scriptReloadBaseContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	if err := ctx.Err(); err != nil {
		return context.Background()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < scriptReloadMinSlack {
			return context.Background()
		}
	}
	return ctx
}

func (c *PendingCoordinator) logLeaseEvent(severity, message string, fields ...interface{}) {
	if c == nil {
		return
	}
	args := []interface{}{"component", c.component}
	if len(fields) > 0 {
		args = append(args, fields...)
	}
	switch severity {
	case "debug":
		if c.cfg.LogLeaseEvents {
			log.Debugw(message, args...)
		}
	case "info":
		if c.cfg.LogLeaseEvents {
			log.Infow(message, args...)
		} else {
			log.Debugw(message, args...)
		}
	case "warn":
		log.Warnw(message, args...)
	case "error":
		log.Errorw(message, args...)
	default:
		if c.cfg.LogLeaseEvents {
			log.Infow(message, args...)
		} else {
			log.Debugw(message, args...)
		}
	}
}

func backpressureValue(level BackpressureLevel) float64 {
	if value, ok := backpressureGaugeValues[level]; ok {
		return value
	}
	return 0
}

func (c *PendingCoordinator) recordQueueDepthMetrics(queueDepth int64, level BackpressureLevel) {
	if metrics.PendingLeaseQueueDepth != nil {
		metrics.PendingLeaseQueueDepth.WithLabelValues(c.component).Set(float64(queueDepth))
	}
	if metrics.PendingLeaseQueueDepthSample != nil {
		metrics.PendingLeaseQueueDepthSample.WithLabelValues(c.component).Observe(float64(queueDepth))
	}
	if metrics.PendingLeaseBackpressureLevel != nil {
		metrics.PendingLeaseBackpressureLevel.WithLabelValues(c.component).Set(backpressureValue(level))
	}
}

// SampleBackpressure 采样当前实例的背压状态。
// username 可选，指定用户以采样用户级队列深度与	背压等级。
// 返回值包含各层队列深度与背压等级，以及综合背压等级。
/*
采样频率是按请求触发的，但有一个很短的缓存窗口：
• SampleBackpressure 每次请求都会走采样逻辑。
• 不过全局/用户深度有 sampleCacheTTL（默认约 BackpressureWindow/50，最小 50ms、最大 150ms），在 TTL 内如果缓存是非背压（none）则直接复用，不会再打 Redis；有背压或过期则立刻重新采样。
• 配置入口：BackpressureWindow 决定 sampleCacheTTL，不能直接配置更大/更小；如果要完全每次采样，可以把 BackpressureWindow 设置得更小让 TTL 逼近 50ms；如果想放大缓存，目前没有独立开关，需要改代码或放宽 BackpressureWindow（不推荐）。

*/
func (c *PendingCoordinator) SampleBackpressure(ctx context.Context, username string) BackpressureSample {
	trimmed := strings.TrimSpace(username)
	sample := BackpressureSample{Username: trimmed, SampledAt: time.Now(), Source: "live"}
	//获取各层队列深度与背压等级(全局)
	sample.GlobalDepth, sample.GlobalLevel, sample.GlobalErr = c.SampleQueueDepth(ctx)
	//获取各层队列深度与背压等级(用户)
	if trimmed != "" {
		sample.UserDepth, sample.UserLevel, sample.UserErr = c.SampleUserQueueDepth(ctx, trimmed)
	}
	//获取各层队列深度与背压等级(实例)
	sample.InstanceDepth, sample.InstanceLevel = c.sampleInstanceBackpressure()
	sample.AggregateLevel = c.decideAggregateLevel(sample.GlobalLevel, sample.UserLevel, sample.InstanceLevel, sample.SampledAt)
	if sample.GlobalErr != nil || sample.UserErr != nil {
		sample.AggregateLevel = mergeBackpressureLevels(sample.AggregateLevel, BackpressureElevated)
		sample.Source = "partial"
	}
	return sample
}

// SampleQueueDepth 采样当前全局队列深度与背压等级。
func (c *PendingCoordinator) SampleQueueDepth(ctx context.Context) (depth int64, level BackpressureLevel, err error) {
	if c == nil {
		return 0, BackpressureNone, nil
	}
	ctx, span := startPendingSpan(ctx, "pending_sample_queue_depth")
	status := "success"
	code := "0"
	defer func() {
		if err != nil {
			status = "error"
			code = err.Error()
		}
		finishPendingSpan(span, status, code, map[string]interface{}{
			"queue_depth":        depth,
			"backpressure_level": string(level),
		})
	}()
	//如果降级模式激活，则直接走降级逻辑
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("sample_queue_depth", "")
		if fallback := c.ensureFallback(); fallback != nil {
			depth, level, err = fallback.SampleQueueDepth(ctx)
			trace.AddRequestTag(ctx, "queue_depth_global", depth)
			trace.AddRequestTag(ctx, "backpressure_level", string(level))
			return depth, level, err
		}
		return 0, BackpressureNone, nil
	}
	//如果没有redis客户端，则直接走降级逻辑
	if c.redis == nil {
		if c.fallback != nil {
			depth, level, err = c.fallback.SampleQueueDepth(ctx)
			trace.AddRequestTag(ctx, "queue_depth_global", depth)
			trace.AddRequestTag(ctx, "backpressure_level", string(level))
			return depth, level, err
		}
		return 0, BackpressureNone, nil
	}
	//如果没有配置指标key，则认为没有背压
	if strings.TrimSpace(c.cfg.MetricsKey) == "" {
		return 0, BackpressureNone, nil
	}
	if c.sampleCacheTTL > 0 {
		if cached, ok := c.loadGlobalSample(); ok && cached != nil {
			now := time.Now()
			//如果缓存未过期且无背压，则直接返回缓存值
			if cached.level == BackpressureNone && cached.expiresAt.After(now) {
				return cached.depth, cached.level, nil
			}
		}
	}

	sampleCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	start := time.Now()
	raw, err := c.redis.GetKeyWithCommandTimeout(sampleCtx, c.cfg.MetricsKey, c.cfg.CommandTimeout)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	//记录redis操作指标,业务打点
	metrics.RecordRedisOperation("pending_lease_metrics_get", time.Since(start).Seconds(), metricErr)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, BackpressureNone, nil
		}
		return 0, BackpressureNone, err
	}
	//得到瞬间队列深度
	depth, parseErr := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if parseErr != nil {
		return 0, BackpressureNone, parseErr
	}
	if depth < 0 {
		depth = 0
	}
	level = c.classifyBackpressure(depth)
	c.storeGlobalSample(depth, level)
	c.recordQueueDepthMetrics(depth, level)
	return depth, level, nil
}

// BackpressureDelay returns the recommended sleep duration for the supplied level/depth pair.
func (c *PendingCoordinator) BackpressureDelay(level BackpressureLevel, depth int64) time.Duration {
	if c == nil {
		return 0
	}
	return c.currentProfile().delay(level, depth)
}

func (c *PendingCoordinator) currentProfile() BackpressureDelayProfile {
	if c == nil {
		return BackpressureDelayProfile{}
	}
	if v := c.backpressureProfile.Load(); v != nil {
		if profile, ok := v.(BackpressureDelayProfile); ok {
			return profile
		}
	}
	return c.cfg.BackpressureDelayProfile
}

// guardTokenBucket 依次执行用户级、实例级、全局三个令牌桶，
// 只要某一层拒绝（或等待失败）即直接返回 errBackpressure。
// username 用于区分用户级限流标签，可为空表示跳过用户维度。
func (c *PendingCoordinator) guardTokenBucket(ctx context.Context, username string) error {
	if c == nil {
		return nil
	}
	if err := c.waitOnLimiter(ctx, c.userLimiter(username), username, "user"); err != nil {
		return err
	}
	if err := c.waitOnLimiter(ctx, c.instanceLimiter, username, "instance"); err != nil {
		return err
	}
	return c.waitOnLimiter(ctx, c.tokenLimiter, username, "global")
}

// 获取令牌桶限流器
func (c *PendingCoordinator) userLimiter(username string) *rate.Limiter {
	if c == nil || c.userTokenBuckets == nil || username == "" {
		return nil
	}
	return c.userTokenBuckets.getLimiter(username)
}

func (c *PendingCoordinator) waitOnLimiter(ctx context.Context, limiter *rate.Limiter, username, scope string) error {
	if limiter == nil {
		return nil
	}
	if limiter.Allow() {
		return nil
	}
	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	var cancel context.CancelFunc
	if deadline, ok := waitCtx.Deadline(); !ok || time.Until(deadline) > tokenBucketWaitTimeout {
		waitCtx, cancel = context.WithTimeout(waitCtx, tokenBucketWaitTimeout)
		defer cancel()
	}
	if err := limiter.Wait(waitCtx); err == nil {
		c.recordTokenBucketMetric("wait", scope)
		return nil
	}
	c.recordTokenBucketMetric("reject", scope)
	c.logLeaseEvent("warn", "pending lease token bucket rejected", "scope", scope, "username", username)
	return errBackpressure
}

func (c *PendingCoordinator) recordTokenBucketMetric(action, scope string) {
	if c == nil || metrics.PendingLeaseEvents == nil {
		return
	}
	event := "token_bucket_" + action
	if scope != "" {
		event += "_" + scope
	}
	metrics.PendingLeaseEvents.WithLabelValues(c.component, event).Inc()
}

func (c *PendingCoordinator) pendingUserDepthKey(username string) string {
	if c == nil {
		return ""
	}
	prefix := strings.TrimSpace(c.cfg.UserMetricsPrefix)
	if prefix == "" {
		prefix = PendingUserDepthPrefix()
	}
	return userScopedKey(prefix, username)
}

func secondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	sec := d / time.Second
	if d%time.Second != 0 {
		sec++
	}
	if sec <= 0 {
		sec = 1
	}
	return int64(sec)
}

func (c *PendingCoordinator) incrementUserQueueDepth(ctx context.Context, username string) (int64, error) {
	if c == nil || c.redis == nil {
		return 0, errors.New("pending coordinator not initialized")
	}
	key := c.pendingUserDepthKey(username)
	if key == "" {
		return 0, errors.New("user depth key empty")
	}
	expireSeconds := secondsCeil(c.cfg.UserBackpressureWindow)
	start := time.Now()
	depth := c.redis.IncrememntWithExpire(ctx, key, expireSeconds)
	metrics.RecordRedisOperation("pending_lease_user_incr", time.Since(start).Seconds(), nil)
	return depth, nil
}

func (c *PendingCoordinator) decrementUserQueueDepth(ctx context.Context, username string, fallback int64) (int64, error) {
	key := c.pendingUserDepthKey(username)
	if key == "" {
		return fallback, nil
	}
	return c.decrementCounterWithRetry(ctx, key, "user", fallback, "username", username)
}

func (c *PendingCoordinator) classifyUserBackpressure(depth int64) BackpressureLevel {
	return classifyDepthWithProfile(depth, c.currentProfile(), c.cfg.UserBackpressureSoftLimit, c.cfg.UserBackpressureHardLimit)
}

func (c *PendingCoordinator) classifyInstanceBackpressure(depth int64) BackpressureLevel {
	return classifyDepthWithProfile(depth, c.currentProfile(), c.cfg.InstanceBackpressureSoftLimit, c.cfg.InstanceBackpressureHardLimit)
}

// mergeBackpressureLevels 合并多个背压等级，返回最高等级。
func mergeBackpressureLevels(levels ...BackpressureLevel) BackpressureLevel {
	result := BackpressureNone
	for _, lvl := range levels {
		switch lvl {
		case BackpressureSevere:
			return BackpressureSevere
		case BackpressureElevated:
			if result == BackpressureNone {
				result = BackpressureElevated
			}
		}
	}
	return result
}

// decideAggregateLevel applies optional isolation和防抖策略；未开启时行为等同 mergeBackpressureLevels。
func (c *PendingCoordinator) decideAggregateLevel(global, user, instance BackpressureLevel, now time.Time) BackpressureLevel {
	agg := mergeBackpressureLevels(global, user, instance)

	// 实例/用户隔离：仅当全局和其它维度非 severe 时，将局部 severe 降级为 elevated。
	if agg == BackpressureSevere {
		if c != nil && c.cfg.IsolateInstanceSevere && global != BackpressureSevere && user != BackpressureSevere {
			agg = BackpressureElevated
		}
		if c != nil && c.cfg.IsolateUserSevere && global != BackpressureSevere && instance != BackpressureSevere {
			agg = BackpressureElevated
		}
	}

	// 等级防抖：在窗口内切换则保留上一次等级；需观测到新等级持续超过窗口才切换。
	hold := time.Duration(0)
	if c != nil {
		hold = c.cfg.BackpressureAntiShakeInterval
	}
	if hold <= 0 {
		return agg
	}

	c.decisionMu.Lock()
	defer c.decisionMu.Unlock()
	if c.lastAggregateLevel == "" {
		c.lastAggregateLevel = agg
		c.lastAggregateAt = now
		c.pendingAggregateLevel = ""
		c.pendingAggregateSince = time.Time{}
		return agg
	}
	if agg == c.lastAggregateLevel {
		c.lastAggregateAt = now
		c.pendingAggregateLevel = ""
		c.pendingAggregateSince = time.Time{}
		return agg
	}
	if c.pendingAggregateLevel != agg {
		c.pendingAggregateLevel = agg
		c.pendingAggregateSince = now
		return c.lastAggregateLevel
	}
	if !c.pendingAggregateSince.IsZero() && now.Sub(c.pendingAggregateSince) >= hold {
		c.lastAggregateLevel = agg
		c.lastAggregateAt = now
		c.pendingAggregateLevel = ""
		c.pendingAggregateSince = time.Time{}
		return agg
	}
	return c.lastAggregateLevel
}

func (c *PendingCoordinator) instanceQueueDepth() int64 {
	if c == nil {
		return 0
	}
	return c.instanceActive.Load()
}

func (c *PendingCoordinator) incInstanceActive() int64 {
	if c == nil {
		return 0
	}
	return c.instanceActive.Add(1)
}

func (c *PendingCoordinator) decInstanceActive() int64 {
	if c == nil {
		return 0
	}
	for {
		current := c.instanceActive.Load()
		if current <= 0 {
			return 0
		}
		if c.instanceActive.CompareAndSwap(current, current-1) {
			return current - 1
		}
	}
}

func (c *PendingCoordinator) randJitterDuration(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	c.randomMu.Lock()
	defer c.randomMu.Unlock()
	if c.random == nil {
		c.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	jitterHalf := base / 2
	if jitterHalf <= 0 {
		return base
	}
	r := time.Duration(c.random.Int63n(int64(jitterHalf)))
	return jitterHalf + r
}

func (c *PendingCoordinator) applyBackpressureDelay(level BackpressureLevel, depth int64) {
	if level == BackpressureNone {
		return
	}
	delay := c.BackpressureDelay(level, depth)
	if delay <= 0 {
		return
	}
	actual := c.randJitterDuration(delay)
	time.Sleep(actual)
}

func (c *PendingCoordinator) fallbackThrottle(scope string) {
	delay := c.fallbackDelay
	if delay <= 0 {
		delay = defaultFallbackDelay
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, scope+"_fallback_delay").Inc()
	}
	time.Sleep(delay)
}

// SampleUserQueueDepth 返回指定用户的瞬间队列深度与背压等级。
func (c *PendingCoordinator) SampleUserQueueDepth(ctx context.Context, username string) (int64, BackpressureLevel, error) {
	if c == nil {
		return 0, BackpressureNone, nil
	}
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("sample_user_queue_depth", username)
		if fallback := c.ensureFallback(); fallback != nil {
			return fallback.SampleUserQueueDepth(ctx, username)
		}
		return 0, BackpressureNone, nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			return c.fallback.SampleUserQueueDepth(ctx, username)
		}
		return 0, BackpressureNone, nil
	}
	key := c.pendingUserDepthKey(username)
	if key == "" {
		return 0, BackpressureNone, nil
	}
	trimmed := strings.TrimSpace(username)
	if c.sampleCacheTTL > 0 {
		if cached, ok := c.loadUserSample(trimmed); ok && cached != nil {
			now := time.Now()
			if cached.level == BackpressureNone && cached.expiresAt.After(now) {
				return cached.depth, cached.level, nil
			}
			if cached.expiresAt.Before(now) {
				c.userSampleCache.Delete(trimmed)
			}
		}
	}
	sampleCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	start := time.Now()
	raw, err := c.redis.GetKeyWithCommandTimeout(sampleCtx, key, c.cfg.CommandTimeout)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_user_depth_get", time.Since(start).Seconds(), metricErr)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, BackpressureNone, nil
		}
		return 0, BackpressureNone, err
	}
	depth, parseErr := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if parseErr != nil {
		return 0, BackpressureNone, parseErr
	}
	if depth < 0 {
		depth = 0
	}
	level := c.classifyUserBackpressure(depth)
	c.storeUserSample(trimmed, depth, level)
	return depth, level, nil
}

func (c *PendingCoordinator) sampleInstanceBackpressure() (int64, BackpressureLevel) {
	depth := c.instanceQueueDepth()
	return depth, c.classifyInstanceBackpressure(depth)
}

func (c *PendingCoordinator) ComponentName() string {
	if c == nil {
		return ""
	}
	return c.component
}

func (c *PendingCoordinator) ListExpired(ctx context.Context, limit int) ([]*PendingState, error) {
	if c == nil {
		return nil, nil
	}
	if c.degradeActive(ctx) {
		c.recordDegradeFallback("list_expired", "")
		if fallback := c.ensureFallback(); fallback != nil {
			return fallback.ListExpired(ctx, limit)
		}
		return nil, nil
	}
	if c.redis == nil {
		if c.fallback != nil {
			return c.fallback.ListExpired(ctx, limit)
		}
		return nil, nil
	}
	if limit <= 0 {
		limit = 128
	}
	keys := c.redis.GetKeys(ctx, PendingCreatePrefix())
	if len(keys) == 0 {
		return nil, nil
	}
	result := make([]*PendingState, 0, limit)
	for _, key := range keys {
		if len(result) >= limit {
			break
		}
		if !strings.HasPrefix(key, PendingCreatePrefix()) {
			continue
		}
		snapshotKey := usernameFromKeyWithPrefix(key, PendingCreatePrefix())
		if snapshotKey == "" {
			continue
		}
		state, err := c.Observe(ctx, snapshotKey)
		if err != nil {
			c.logLeaseEvent("debug", "failed to observe pending lease during expired scan", "username", snapshotKey, "error", err)
			continue
		}
		if state != nil && state.Exists && state.State == PendingStateExpired {
			if strings.TrimSpace(state.Username) == "" {
				state.Username = snapshotKey
			}
			result = append(result, state)
		}
	}
	return result, nil
}
