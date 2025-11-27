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
	"golang.org/x/time/rate"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
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
)

var (
	defaultFallbackDelay = 80 * time.Millisecond
)

// BackpressureDelayBucket describes a queue depth threshold and the delay to apply when it is reached.
type BackpressureDelayBucket struct {
	Depth int
	Delay time.Duration
}

// BackpressureDelayProfile groups delay buckets for each backpressure level so that producers and consumers respond consistently.
type BackpressureDelayProfile struct {
	Elevated            []BackpressureDelayBucket
	Severe              []BackpressureDelayBucket
	ElevatedBucketCount int
	SevereBucketCount   int
	ElevatedMaxDepth    int
	SevereMaxDepth      int
}

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
	if len(p.Severe) == 0 {
		p.Severe = buildDelayBuckets(hard, severeBuckets, severeBase, severeMax, severeMaxDepth)
	} else {
		p.Severe = normalizeDelayBuckets(p.Severe, severeMaxDepth)
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
			depth = next
		}
		if cappedMax > 0 && depth > cappedMax {
			depth = cappedMax
		} else if depth > maxBackpressureDepthCap {
			depth = maxBackpressureDepthCap
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
		if (cappedMax > 0 && depth >= cappedMax) || depth >= maxBackpressureDepthCap {
			break
		}
	}
	return normalizeDelayBuckets(buckets, cappedMax)
}

type LeaseMetadata struct {
	Username        string
	RequestID       string
	Operator        string
	ClientIP        string
	LegacyRequestID string
}

//背压配置

type PendingCoordinatorConfig struct {
	LeaseTTL                      time.Duration            //租约有效期（控制单个操作的最大处理时间）
	ObserveTimeout                time.Duration            //观察超时时间（控制读取当前状态的最大等待时间）
	BackpressureWindow            time.Duration            //背压评估窗口
	BackpressureSoftLimit         int                      //软限制（达到该值开始应用背压）
	BackpressureHardLimit         int                      //硬限制（达到该值触发严重背压429）
	MetricsKey                    string                   //指标键
	Component                     string                   //组件标识（用于区分不同服务）
	LogLeaseEvents                bool                     //是否记录租约事件日志
	ReleaseRetention              time.Duration            //正常释放租约状态保留时间
	ExpiredRetention              time.Duration            //过期租约状态保留时间
	ExpiredGracePeriod            time.Duration            //过期宽限期
	ElevatedDelayBase             time.Duration            //基础延迟（背压升高时）
	ElevatedDelayMax              time.Duration            //最大延迟（背压升高时）
	SevereDelayBase               time.Duration            //基础延迟（严重背压时）
	SevereDelayMax                time.Duration            //最大延迟（严重背压时）
	BackpressureDelayProfile      BackpressureDelayProfile //延迟曲线配置
	TokenBucketRate               float64                  //令牌桶速率（req/s），0 表示关闭
	TokenBucketBurst              int                      //令牌桶突发容量
	UserMetricsPrefix             string                   //用户局部深度指标前缀
	UserBackpressureWindow        time.Duration            //用户级队列采样窗口
	UserBackpressureSoftLimit     int                      //用户级软阈值
	UserBackpressureHardLimit     int                      //用户级硬阈值
	InstanceBackpressureSoftLimit int                      //实例级软阈值
	InstanceBackpressureHardLimit int                      //实例级硬阈值
	FallbackDelay                 time.Duration            //采样失败时的保守延迟
	CalibrationInterval           time.Duration            //全量校准执行间隔
	CalibrationTimeout            time.Duration            //单次全量校准超时
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
	redis                     *storage.RedisCluster
	cfg                       PendingCoordinatorConfig //背压配置
	component                 string                   //组件标识（用于区分不同服务）
	queueDepthReconcileActive atomic.Bool
	tokenLimiter              *rate.Limiter
	instanceActive            atomic.Int64
	randomMu                  sync.Mutex
	random                    *rand.Rand
	fallbackDelay             time.Duration
	calibrationOnce           sync.Once
	calibrationStop           chan struct{}
	calibrationStopOnce       sync.Once
	calibrationUpdateCh       chan struct{}
	calibrationIntervalNS     atomic.Int64
	calibrationTimeoutNS      atomic.Int64
}

type pendingLeaseSnapshot struct {
	Status               string `json:"status"`
	Degraded             bool   `json:"degraded,omitempty"`
	State                string `json:"state"`
	OwnerID              string `json:"owner_id"`
	Version              int64  `json:"version"`
	LeaseExpiresAt       string `json:"lease_expires_at"`
	AcquireAt            string `json:"acquire_at"`
	UpdatedAt            string `json:"updated_at"`
	QueueDepth           int64  `json:"queue_depth,omitempty"`
	Backpressure         string `json:"backpressure,omitempty"`
	UserQueueDepth       int64  `json:"user_queue_depth,omitempty"`
	UserBackpressure     string `json:"user_backpressure,omitempty"`
	InstanceQueueDepth   int64  `json:"instance_queue_depth,omitempty"`
	InstanceBackpressure string `json:"instance_backpressure,omitempty"`
	Username             string `json:"username,omitempty"`
	RequestID            string `json:"request_id,omitempty"`
	Operator             string `json:"operator,omitempty"`
	ClientIP             string `json:"client_ip,omitempty"`
	LegacyRequestID      string `json:"legacy_request_id,omitempty"`
	ReleasedAt           string `json:"released_at,omitempty"`
	ReleasedBy           string `json:"released_by,omitempty"`
	ExpiredAt            string `json:"expired_at,omitempty"`
	ExpiredReason        string `json:"expired_reason,omitempty"`
}

type PendingLease struct {
	Username       string
	OwnerID        string
	Version        int64
	AcquireAt      time.Time
	LeaseExpiresAt time.Time
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
	if redis == nil {
		return nil
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Minute
	}
	if cfg.ObserveTimeout <= 0 {
		cfg.ObserveTimeout = 700 * time.Millisecond
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

	applyCoordinatorEnvOverrides(&cfg)

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

	component := strings.TrimSpace(cfg.Component)
	if component == "" {
		component = "pending_coordinator"
	}
	cfg.Component = component

	if strings.TrimSpace(cfg.UserMetricsPrefix) == "" {
		cfg.UserMetricsPrefix = PendingUserDepthPrefix()
	}
	cfg.UserMetricsPrefix = strings.TrimSpace(cfg.UserMetricsPrefix)
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
	if cfg.InstanceBackpressureSoftLimit <= 0 {
		cfg.InstanceBackpressureSoftLimit = defaultInstanceBackpressureSoft
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

	var tokenLimiter *rate.Limiter
	if cfg.TokenBucketRate > 0 {
		burst := cfg.TokenBucketBurst
		if burst <= 0 {
			burst = int(cfg.TokenBucketRate) * defaultTokenBucketBurstMultiplier
			if burst < 1 {
				burst = 1
			}
		}
		tokenLimiter = rate.NewLimiter(rate.Limit(cfg.TokenBucketRate), burst)
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	coordinator := &PendingCoordinator{
		redis:               redis,
		cfg:                 cfg,
		component:           component,
		tokenLimiter:        tokenLimiter,
		random:              random,
		fallbackDelay:       cfg.FallbackDelay,
		calibrationStop:     make(chan struct{}),
		calibrationUpdateCh: make(chan struct{}, 1),
	}
	coordinator.calibrationIntervalNS.Store(cfg.CalibrationInterval.Nanoseconds())
	coordinator.calibrationTimeoutNS.Store(cfg.CalibrationTimeout.Nanoseconds())
	coordinator.startCalibrationLoop()

	return coordinator
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

const releasedStateRetryLimit = 3

// 大量请求 → 令牌桶过滤（剩80%）→ 柔性延迟过滤（剩60%）
// → 租约抢占过滤（剩10%）→ 执行核心业务
func (c *PendingCoordinator) Acquire(ctx context.Context, username string, meta LeaseMetadata) (*AcquireResult, error) {
	if c == nil || c.redis == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, errors.New("username required")
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return nil, errors.New("pending key empty")
	}

	for attempt := 0; attempt < releasedStateRetryLimit+1; attempt++ {
		//令牌桶背压检查
		if err := c.guardTokenBucket(ctx, trimmed); err != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "token_bucket_block").Inc()
			}
			return nil, &AcquireError{Reason: AcquireFailureBackpressure, QueueDepth: 0}
		}
		//全局队列深度背压检查
		globalDepth, globalLevel, globalErr := c.SampleQueueDepth(ctx)
		if globalErr != nil {
			c.logLeaseEvent("warn", "pending lease global depth sample failed", "username", trimmed, "error", globalErr)
		}
		//用户队列深度背压检查
		userDepth, userLevel, userErr := c.SampleUserQueueDepth(ctx, trimmed)
		if userErr != nil {
			c.logLeaseEvent("warn", "pending lease user depth sample failed", "username", trimmed, "error", userErr)
		}
		//实例队列深度背压检查
		instanceDepth, instanceLevel := c.sampleInstanceBackpressure()
		aggregateLevel := mergeBackpressureLevels(globalLevel, userLevel, instanceLevel)
		fallbackSampling := false
		if globalErr != nil || userErr != nil {
			aggregateLevel = mergeBackpressureLevels(aggregateLevel, BackpressureElevated)
			fallbackSampling = true
		}
		maxDepth := globalDepth
		if userDepth > maxDepth {
			maxDepth = userDepth
		}
		if instanceDepth > maxDepth {
			maxDepth = instanceDepth
		}
		if aggregateLevel == BackpressureSevere {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "precheck_backpressure_reject").Inc()
			}
			c.logLeaseEvent("warn", "pending lease precheck rejected by severe backpressure", "username", trimmed, "queue_depth", maxDepth, "global_level", string(globalLevel), "user_level", string(userLevel), "instance_level", string(instanceLevel))
			return nil, &AcquireError{Reason: AcquireFailureBackpressure, QueueDepth: maxDepth}
		}
		if fallbackSampling {
			c.fallbackThrottle("sample")
		}
		if aggregateLevel == BackpressureElevated {
			c.applyBackpressureDelay(aggregateLevel, maxDepth)
		}

		now := time.Now().UTC()
		ownerID := uuid.New().String()
		expiresAt := now.Add(c.cfg.LeaseTTL)
		snapshot := pendingLeaseSnapshot{
			Status:          "pending",
			State:           string(PendingStateLease),
			OwnerID:         ownerID,
			Version:         now.UnixNano(),
			LeaseExpiresAt:  expiresAt.Format(time.RFC3339Nano),
			AcquireAt:       now.Format(time.RFC3339Nano),
			UpdatedAt:       now.Format(time.RFC3339Nano),
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

		opCtx, cancel := c.newOpContext(ctx)
		setNXStart := time.Now()
		created, setNXErr := c.redis.SetNX(opCtx, key, string(payload), c.cfg.LeaseTTL)
		cancel()
		setNXDuration := time.Since(setNXStart)
		metrics.RecordRedisOperation("pending_lease_setnx", setNXDuration.Seconds(), setNXErr)
		if setNXErr != nil {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "setnx_error").Inc()
			}
			c.logLeaseEvent("error", "pending lease setnx failed", "username", trimmed, "error", setNXErr)
			return nil, setNXErr
		}
		if !created {
			state, observeErr := c.Observe(ctx, trimmed)
			if observeErr != nil {
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(c.component, "observe_error").Inc()
				}
				c.logLeaseEvent("error", "pending lease observe failed", "username", trimmed, "error", observeErr)
				return nil, observeErr
			}
			if c.shouldPromoteExpired(state) {
				if _, promoteErr := c.promoteExpired(ctx, trimmed, state); promoteErr != nil {
					return nil, promoteErr
				}
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
				return nil, &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: state.QueueDepth}
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
			return nil, &AcquireError{Reason: AcquireFailureConflict, State: state, QueueDepth: queueDepth}
		}

		queueDepth := c.redis.IncrememntWithExpire(ctx, c.cfg.MetricsKey, int64(c.cfg.BackpressureWindow.Seconds()))
		level := c.classifyBackpressure(queueDepth)
		c.recordQueueDepthMetrics(queueDepth, level)

		userQueueDepth, userDepthErr := c.incrementUserQueueDepth(ctx, trimmed)
		if userDepthErr != nil {
			c.logLeaseEvent("warn", "pending lease increment user depth failed", "username", trimmed, "error", userDepthErr)
		}
		userLevelCurrent := c.classifyUserBackpressure(userQueueDepth)

		instanceDepthCurrent := c.incInstanceActive()
		instanceLevelCurrent := c.classifyInstanceBackpressure(instanceDepthCurrent)

		aggregateLevel = mergeBackpressureLevels(level, userLevelCurrent, instanceLevelCurrent)
		if userDepthErr != nil {
			aggregateLevel = mergeBackpressureLevels(aggregateLevel, BackpressureElevated)
			c.fallbackThrottle("user_counter")
		}

		snapshot.QueueDepth = queueDepth
		snapshot.Backpressure = string(aggregateLevel)
		snapshot.UserQueueDepth = userQueueDepth
		snapshot.UserBackpressure = string(userLevelCurrent)
		snapshot.InstanceQueueDepth = instanceDepthCurrent
		snapshot.InstanceBackpressure = string(instanceLevelCurrent)
		if aggregateLevel != BackpressureNone {
			snapshot.Degraded = true
		}
		snapshot.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

		if updatedPayload, marshalErr := json.Marshal(&snapshot); marshalErr == nil {
			updateCtx, updateCancel := c.newOpContext(ctx)
			if err := c.redis.SetKey(updateCtx, key, string(updatedPayload), c.cfg.LeaseTTL); err != nil {
				log.Warnw("update pending snapshot failed", "username", trimmed, "error", err)
			}
			updateCancel()
		} else {
			log.Warnw("marshal pending snapshot with queue depth failed", "username", trimmed, "error", marshalErr)
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
				Raw:                  string(payload),
			}
			return nil, &AcquireError{Reason: AcquireFailureBackpressure, State: state, QueueDepth: queueDepth}
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
			QueueDepth:     queueDepth,
			Backpressure:   aggregateLevel,
			Metadata:       meta,
		}

		return &AcquireResult{Lease: lease, SetNXDuration: setNXDuration}, nil
	}

	return nil, errors.New("pending lease acquisition retries exhausted")
}

func (c *PendingCoordinator) Release(ctx context.Context, username string, ownerID string) (time.Duration, error) {
	if c == nil || c.redis == nil {
		return 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return 0, nil
	}
	key := PendingCreateKey(trimmed)
	if key == "" {
		return 0, nil
	}

	deleteCtx, cancel := c.newOpContext(ctx)
	deleteStart := time.Now()
	deleted, snapshot, err := c.deleteKeyWithOwner(deleteCtx, key, ownerID)
	cancel()
	deleteDuration := time.Since(deleteStart)
	metricErr := err
	if errors.Is(err, redis.Nil) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_lease_delete", deleteDuration.Seconds(), metricErr)
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil {
		return deleteDuration, err
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

	if !deleted {
		if snapshot != nil && snapshot.OwnerID != "" && ownerID != "" && snapshot.OwnerID != ownerID {
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_owner_mismatch").Inc()
			}
			c.logLeaseEvent("warn", "pending lease release skipped due to owner mismatch", "username", trimmed, "owner", ownerID, "current_owner", snapshot.OwnerID)
			return deleteDuration, ErrPendingLeaseOwnerMismatch
		}
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "release_miss").Inc()
		}
		c.logLeaseEvent("debug", "pending lease release no-op", "username", trimmed, "owner", ownerID)
		return deleteDuration, nil
	}

	if metrics.PendingLeaseActiveGauge != nil {
		metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Dec()
	}
	fallbackDepth := int64(0)
	fallbackUserDepth := int64(0)
	if snapshot != nil {
		fallbackDepth = snapshot.QueueDepth
		fallbackUserDepth = snapshot.UserQueueDepth
	}
	remaining, decErr := c.decrementCounterWithRetry(ctx, c.cfg.MetricsKey, "release", fallbackDepth, "username", trimmed, "owner", ownerID)
	if decErr != nil {
		if metrics.PendingLeaseActiveGauge != nil {
			metrics.PendingLeaseActiveGauge.WithLabelValues(c.component).Inc()
		}
		return deleteDuration, decErr
	}
	userRemaining, userDecErr := c.decrementUserQueueDepth(ctx, trimmed, fallbackUserDepth)
	if userDecErr != nil {
		c.logLeaseEvent("warn", "pending lease user depth decrement failed", "username", trimmed, "owner", ownerID, "error", userDecErr)
	}
	instanceRemaining := c.decInstanceActive()
	newInstanceLevel := c.classifyInstanceBackpressure(instanceRemaining)
	newLevel := c.classifyBackpressure(remaining)
	userLevelAfter := c.classifyUserBackpressure(userRemaining)
	aggregateAfterRelease := mergeBackpressureLevels(newLevel, userLevelAfter, newInstanceLevel)
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

	return deleteDuration, nil
}

func (c *PendingCoordinator) Observe(ctx context.Context, username string) (*PendingState, error) {
	result := &PendingState{State: PendingStateUnknown}
	if c == nil || c.redis == nil {
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
	raw, err := c.redis.GetKey(getCtx, key)
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
	_, err := c.redis.DeleteKey(cleanupCtx, key)
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
	_, err := c.redis.DeleteKey(cleanupCtx, key)
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

func (c *PendingCoordinator) shouldPromoteExpired(state *PendingState) bool {
	if c == nil || state == nil {
		return false
	}
	if state.State != PendingStateLease {
		return false
	}
	now := time.Now()
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
	released := pendingLeaseSnapshot{
		Status:             "completed",
		State:              string(PendingStateReleased),
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
		ReleasedAt:         now.Format(time.RFC3339Nano),
		ReleasedBy:         owner,
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
	}

	payload, err := json.Marshal(&released)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKey(writeCtx, key, string(payload), ttl); err != nil {
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
	}

	payload, err := json.Marshal(&expired)
	if err != nil {
		return err
	}

	writeCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	if err := c.redis.SetKey(writeCtx, key, string(payload), ttl); err != nil {
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
	raw, err := c.redis.GetKey(ctx, key)
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
	deleted, deleteErr := c.redis.DeleteKey(ctx, key)
	return deleted, snapshotPtr, deleteErr
}

func (c *PendingCoordinator) classifyBackpressure(depth int64) BackpressureLevel {
	if depth >= int64(c.cfg.BackpressureHardLimit) {
		return BackpressureSevere
	}
	if depth >= int64(c.cfg.BackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
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
	result, err := c.redis.Eval(evalCtx, decrementActiveLua, []string{key}, nil)
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
	keys := c.redis.GetKeys(ctx, PendingCreatePrefix())
	if err := ctx.Err(); err != nil {
		c.logLeaseEvent("debug", "pending lease calibration cancelled", "error", err)
		return
	}
	totalActive := int64(0)
	userCounts := make(map[string]int64)
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		username := strings.TrimSpace(strings.TrimPrefix(key, PendingCreatePrefix()))
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
		err := c.redis.SetKey(writeCtx, metricsKey, strconv.FormatInt(global, 10), c.cfg.BackpressureWindow)
		cancel()
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
		err := c.redis.SetKey(writeCtx, key, value, c.cfg.UserBackpressureWindow)
		cancel()
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
	keys := c.redis.GetKeys(ctx, prefix)
	for _, key := range keys {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		username := strings.TrimSpace(strings.TrimPrefix(key, prefix))
		if username == "" {
			continue
		}
		if active != nil {
			if _, ok := active[username]; ok {
				continue
			}
		}
		deleteCtx, cancel := c.newOpContext(ctx)
		_, err := c.redis.DeleteKey(deleteCtx, key)
		cancel()
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

func (c *PendingCoordinator) SampleQueueDepth(ctx context.Context) (int64, BackpressureLevel, error) {
	if c == nil || c.redis == nil || strings.TrimSpace(c.cfg.MetricsKey) == "" {
		return 0, BackpressureNone, nil
	}
	sampleCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	raw, err := c.redis.GetKey(sampleCtx, c.cfg.MetricsKey)
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
	level := c.classifyBackpressure(depth)
	c.recordQueueDepthMetrics(depth, level)
	return depth, level, nil
}

// BackpressureDelay returns the recommended sleep duration for the supplied level/depth pair.
func (c *PendingCoordinator) BackpressureDelay(level BackpressureLevel, depth int64) time.Duration {
	if c == nil {
		return 0
	}
	return c.cfg.BackpressureDelayProfile.delay(level, depth)
}

func (c *PendingCoordinator) guardTokenBucket(ctx context.Context, username string) error {
	if c == nil || c.tokenLimiter == nil {
		return nil
	}
	if c.tokenLimiter.Allow() {
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
	if err := c.tokenLimiter.Wait(waitCtx); err == nil {
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(c.component, "token_bucket_wait").Inc()
		}
		return nil
	}
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues(c.component, "token_bucket_reject").Inc()
	}
	c.logLeaseEvent("warn", "pending lease token bucket rejected", "username", username)
	return errBackpressure
}

func (c *PendingCoordinator) pendingUserDepthKey(username string) string {
	if c == nil {
		return ""
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	prefix := strings.TrimSpace(c.cfg.UserMetricsPrefix)
	if prefix == "" {
		prefix = PendingUserDepthPrefix()
	}
	return prefix + trimmed
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
	depth := c.redis.IncrememntWithExpire(ctx, key, expireSeconds)
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
	if depth >= int64(c.cfg.UserBackpressureHardLimit) {
		return BackpressureSevere
	}
	if depth >= int64(c.cfg.UserBackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

func (c *PendingCoordinator) classifyInstanceBackpressure(depth int64) BackpressureLevel {
	if depth >= int64(c.cfg.InstanceBackpressureHardLimit) {
		return BackpressureSevere
	}
	if depth >= int64(c.cfg.InstanceBackpressureSoftLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

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

func (c *PendingCoordinator) SampleUserQueueDepth(ctx context.Context, username string) (int64, BackpressureLevel, error) {
	if c == nil || c.redis == nil {
		return 0, BackpressureNone, nil
	}
	key := c.pendingUserDepthKey(username)
	if key == "" {
		return 0, BackpressureNone, nil
	}
	sampleCtx, cancel := c.newOpContext(ctx)
	defer cancel()
	raw, err := c.redis.GetKey(sampleCtx, key)
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
	if c == nil || c.redis == nil {
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
		snapshotKey := strings.TrimSpace(strings.TrimPrefix(key, PendingCreatePrefix()))
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
