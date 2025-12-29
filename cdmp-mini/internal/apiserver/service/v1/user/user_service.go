package user

import (
	"context"
	stdErrors "errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"

	storectx "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	createpipeline "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/create"
	operation "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/unique"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/backpressure"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	serveropts "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/userctx"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/util"
	"github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/validator/jwtvalidator"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

const (
	// RATE_LIMIT_PREVENTION 负缓存哨兵标记
	// 完整键格式: user:{username} (值为哨兵)
	// 存储结构: String，特殊用户名占位符
	// 用途: 标记用户近期未找到结果，触发负缓存与限流保护
	// 存储位置: Redis
	RATE_LIMIT_PREVENTION = usercache.NegativeCacheSentinel
	// BLACKLIST_SENTINEL 黑名单哨兵标记
	// 完整键格式: user:{username} 或 user:blacklist:{username} (值为哨兵)
	// 存储结构: String，特殊用户名占位符
	// 用途: 表示用户被列入黑名单，阻断读写请求
	// 存储位置: Redis
	BLACKLIST_SENTINEL = usercache.BlacklistSentinel
	// createStepSlowThreshold 用户创建步骤慢日志阈值
	// 数值定义: 200ms
	// 数据类型: time.Duration 常量
	// 用途: 超过阈值时输出慢日志，监控创建链路性能
	// 生效范围: 应用内逻辑
	createStepSlowThreshold = 200 * time.Millisecond
	// contactPlaceholderTTL 联系方式预热占位过期时间
	// 数值定义: 30s
	// 数据类型: time.Duration 常量
	// 用途: 占位缓存防穿透，过期后需刷新真实数据
	// 生效范围: 联系方式缓存模块
	contactPlaceholderTTL = 30 * time.Second
	// contactWarmupTimeout 联系方式预热流程超时时间
	// 数值定义: 2m
	// 数据类型: time.Duration 常量
	// 用途: 限制预热任务单次执行时长，避免长时间占用资源
	// 生效范围: 联系方式预热任务
	contactWarmupTimeout = 2 * time.Minute
	// contactWarmupBatchSize 联系方式预热批量大小
	// 数值定义: 1000
	// 数据类型: 整型常量
	// 用途: 控制单批预热的拉取量，权衡速度与负载
	// 生效范围: 联系方式预热任务
	contactWarmupBatchSize = 1000
	// contactWarmupRetryDelay 联系方式预热失败后的重试间隔
	// 数值定义: 30s
	// 数据类型: time.Duration 常量
	// 用途: 控制预热失败后的退避节奏，避免持续冲击底层存储
	// 生效范围: 联系方式预热任务
	contactWarmupRetryDelay = 30 * time.Second
	// pendingWarmupRequestCount 待审批协调器预热请求数量
	// 数值定义: 3
	// 数据类型: 整型常量
	// 用途: 控制预热流程的请求数量，避免对 Redis 施加额外压力
	// 生效范围: pending 协调器预热流程
	pendingWarmupRequestCount = 3
	// pendingWarmupSyntheticPrefix 待审批协调器预热占位前缀
	// 字符串格式: __pending_warmup__
	// 用途: 区分预热合成用户，避免与真实用户名冲突
	// 生效范围: pending 协调器预热流程
	pendingWarmupSyntheticPrefix = "__pending_warmup__"
	// pendingWarmupOperationTimeout 待审批协调器预热操作超时
	// 数值定义: 300ms
	// 数据类型: time.Duration 常量
	// 用途: 限制预热 Acquire/Release 操作耗时，避免阻塞上线文
	// 生效范围: pending 协调器预热流程
	pendingWarmupOperationTimeout = 300 * time.Millisecond
	// pendingWarmupDelay 待审批协调器预热节流延迟
	// 数值定义: 150ms
	// 数据类型: time.Duration 常量
	// 用途: 拉长预热请求间隔，避免瞬时打爆 Redis
	// 生效范围: pending 协调器预热流程
	pendingWarmupDelay = 150 * time.Millisecond
	// pendingBackpressureMaxDelay pending 背压等待最大值
	// 数值定义: 80ms
	// 数据类型: time.Duration 常量
	// 用途: 限制 acquire 前的排队等待，避免尾部延迟过长
	// 生效范围: markUserPendingCreate 背压节流
	pendingBackpressureMaxDelay = 80 * time.Millisecond
	// contactCacheTTL 联系方式缓存有效期
	// 数值定义: 24h
	// 数据类型: time.Duration 常量
	// 用途: 决定邮箱/手机号缓存的刷新频率，保持数据新鲜
	// 生效范围: 联系方式缓存模块
	contactCacheTTL = 24 * time.Hour
	// strongConsistencyMaxRetries 强一致性重试上限
	// 数值定义: 3 次
	// 数据类型: 整型常量
	// 用途: 指定数据库读强一致性的最大重试次数
	// 生效范围: 强一致性读取流程
	strongConsistencyMaxRetries = 3
	// strongConsistencyBackoffBase 强一致性重试基础退避
	// 数值定义: 80ms
	// 数据类型: time.Duration 常量
	// 用途: 计算指数退避的基值，缓解数据库压力
	// 生效范围: 强一致性读取流程
	strongConsistencyBackoffBase = 80 * time.Millisecond
	// strongConsistencyBackoffCeiling 强一致性退避上限
	// 数值定义: 500ms
	// 数据类型: time.Duration 常量
	// 用途: 限制指数退避的最大等待时间，避免退避过长
	// 生效范围: 强一致性读取流程
	strongConsistencyBackoffCeiling = 500 * time.Millisecond
	// strongConsistencyInitialDelayBase 强一致性初始探测基准
	// 数值定义: 35ms
	// 数据类型: time.Duration 常量
	// 用途: 设置首次探测前的基础等待，用于等待副本同步
	// 生效范围: 强一致性读取流程
	strongConsistencyInitialDelayBase = 35 * time.Millisecond
	// strongConsistencyInitialDelayJitter 强一致性初始探测抖动
	// 数值定义: 45ms
	// 数据类型: time.Duration 常量
	// 用途: 为初始等待添加随机抖动，避免请求风暴
	// 生效范围: 强一致性读取流程
	strongConsistencyInitialDelayJitter = 45 * time.Millisecond
	// batchLookupCacheTTL 批量查找短期缓存TTL
	// 数值定义: 750ms
	// 数据类型: time.Duration 常量
	// 用途: 缓存批量查找结果，减少重复查询但保持短时一致性
	// 生效范围: 批量查找辅助缓存
	batchLookupCacheTTL = 750 * time.Millisecond
	// redisDegradeReasonCache Redis 降级常量标记
	redisDegradeReasonCache = "redis_cache_error"
	// redisDegradeReasonPlaceholder Redis 占位降级常量标记
	redisDegradeReasonPlaceholder = "redis_placeholder_error"
	// contactDegradeCacheDefaultMaxEntries 降级本地缓存默认最大容量
	contactDegradeCacheDefaultMaxEntries = 5000
	// pendingHeartbeatIntervalMin pending 心跳最小间隔，用于避免过度刷新
	// 数值定义: 2s
	pendingHeartbeatIntervalMin = 2 * time.Second
	// pendingHeartbeatIntervalMax pending 心跳最大间隔，用于保证长任务仍能看到及时刷新
	// 数值定义: 15s
	pendingHeartbeatIntervalMax = 15 * time.Second
	// pendingHeartbeatCallTimeout 单次心跳命令最大等待
	// 数值定义: 2s
	pendingHeartbeatCallTimeout = 2 * time.Second
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type UserService struct {
	// Store 底层存储工厂
	// 数据类型: interfaces.Factory 接口
	// 用途: 提供数据库与缓存的访问入口
	// 生效范围: UserService 内部所有读写流程
	Store interfaces.Factory
	// Redis Redis 集群客户端
	// 数据类型: *storage.RedisCluster 指针
	// 用途: 负责用户缓存、限流哨兵等数据操作
	// 生效范围: 缓存、预热及限流逻辑
	Redis *storage.RedisCluster
	// Options 服务配置项
	// 数据类型: *options.Options 指针
	// 用途: 承载 server run、限流等可调参数
	// 生效范围: 依赖配置的所有功能
	Options *options.Options
	// Producer 消息生产者
	// 数据类型: producer.MessageProducer[*v1.User, string] 泛型接口
	// 用途: 对外投递用户事件或异步任务
	// 生效范围: 用户创建、更新等事件通知
	Producer producer.MessageProducer[*v1.User, string]
	// Audit 审计管理器
	// 数据类型: *audit.Manager 指针
	// 用途: 记录用户操作日志与审计事件
	// 生效范围: 审计链路与安全日志
	Audit *audit.Manager
	// pendingCoordinator 待审批协调器
	// 数据类型: *usercache.PendingCoordinator 指针
	// 用途: 管理用户待处理状态与缓存协调
	// 生效范围: 待审批用户流程
	pendingCoordinator *usercache.PendingCoordinator
	// pendingHeartbeats 正在运行的 pending 心跳任务
	// 数据类型: sync.Map
	// 用途: 为长耗时操作维持 pending 心跳
	pendingHeartbeats sync.Map
	// group singleflight 组
	// 数据类型: singleflight.Group 结构体
	// 用途: 合并并发请求避免重复执行
	// 生效范围: 强一致性读写与缓存加载
	group singleflight.Group
	// createPipeline 创建流程管道
	// 数据类型: *createpipeline.Pipeline[*v1.User] 指针
	// 用途: 串联用户创建所需的校验与调用步骤
	// 生效范围: 用户创建服务调用链
	createPipeline *createpipeline.Pipeline[*v1.User]
	// operationPipelineOnce 确保异步操作管道只初始化一次
	operationPipelineOnce sync.Once
	// operationPipeline 用户操作异步管道
	operationPipeline *operation.Pipeline
	// operationQueue 操作队列协调器
	operationQueue operation.QueueCoordinator
	// operationStateStore 操作状态存储
	operationStateStore operation.RequestStateStore
	// operationExecutor 操作执行器注册表
	operationExecutor *userOperationExecutor
	// operationWorkersWG 操作管道工作协程等待组
	operationWorkersWG sync.WaitGroup
	// operationWorkersCancel 终止操作管道工作协程的取消函数
	operationWorkersCancel context.CancelFunc
	// operationModeInit 确保运行模式控制器初始化一次
	operationModeInit sync.Once
	// operationModeCtrl 运行模式控制器
	operationModeCtrl *operationModeController

	// contactWarmupMu 联系方式预热互斥锁
	// 数据类型: sync.Mutex 结构体
	// 用途: 串行化预热流程的并发进入
	// 生效范围: 联系方式预热生命周期
	contactWarmupMu sync.Mutex
	// contactWarming 联系方式预热标记
	// 数据类型: bool 布尔值
	// 用途: 表示预热任务是否正在运行
	// 生效范围: 预热任务调度
	contactWarming bool
	// contactWarmupWaitCh 当前预热线程完成通知通道
	// 数据类型: chan struct{}
	// 用途: 允许阻塞调用等待正在进行的预热任务
	// 生效范围: 启动预热与周期刷新
	contactWarmupWaitCh chan struct{}
	// contactCacheReady 联系方式缓存就绪标志
	// 数据类型: atomic.Bool 原子布尔
	// 用途: 指示缓存是否可供读取
	// 生效范围: 读流程与预热流程间的状态同步
	contactCacheReady atomic.Bool
	// preflightLimiter 预检限流器
	// 数据类型: *semaphore.Weighted 指针
	// 用途: 限制并发预检请求数量
	// 生效范围: 联系方式预检与唯一性校验
	preflightLimiter *semaphore.Weighted
	// poolReporter 连接池统计上报器
	// 数据类型: *poolStatsReporter 指针
	// 用途: 采集并上报数据库/缓存池使用指标
	// 生效范围: 运维监控模块
	poolReporter *poolStatsReporter
	// contactWarmupNextRetry 联系方式预热下次重试时间
	// 数据类型: atomic.Int64 原子整型
	// 用途: 记录下一次预热的重试时间戳
	// 生效范围: 预热调度与退避机制
	contactWarmupNextRetry atomic.Int64
	// contactWarmupLoopOnce 确保周期预热循环只启动一次
	contactWarmupLoopOnce sync.Once
	// contactWarmupLoopCancel 周期预热循环取消函数
	contactWarmupLoopCancel context.CancelFunc
	// contactWarmupLoopWG 周期预热循环等待组
	contactWarmupLoopWG sync.WaitGroup
	// contactRedisDegradeActive Redis 降级全局标记
	// 数据类型: atomic.Bool 原子布尔
	contactRedisDegradeActive atomic.Bool
	// contactRedisDegradeSince Redis 降级起始时间戳（Unix 秒）
	contactRedisDegradeSince atomic.Int64
	// contactRedisDegradeCache 降级模式下的本地唯一性缓存
	contactRedisDegradeCache sync.Map
	// contactRedisDegradeTTL 本地缓存有效期
	contactRedisDegradeTTL time.Duration
	// contactRedisHealthCheckInterval Redis 健康巡检间隔
	contactRedisHealthCheckInterval time.Duration
	// contactRedisMonitorOnce 确保健康巡检只启动一次
	contactRedisMonitorOnce sync.Once
	// contactRedisDegradeCacheSize 当前降级缓存条目数
	contactRedisDegradeCacheSize atomic.Int64
	// contactRedisDegradeCacheLimit 降级缓存容量上限
	contactRedisDegradeCacheLimit int64
}

type contactDegradeCacheEntry struct {
	owner   string
	expires int64
}

type contextKey string

const (
	forceCacheRefreshKey contextKey = "user.forceCacheRefresh"
	batchLookupCacheKey  contextKey = "user.batchLookupCache"
	verifyUserGoneKey    contextKey = "user.verifyUserGone"
)

func newPreflightLimiter(opts *options.Options) *semaphore.Weighted {
	if opts == nil || opts.ServerRunOptions == nil {
		return semaphore.NewWeighted(int64(serveropts.DefaultContactPreflightMaxConcurrency))
	}
	concurrency := opts.ServerRunOptions.ContactPreflightMaxConcurrency
	if concurrency <= 0 {
		concurrency = serveropts.DefaultContactPreflightMaxConcurrency
	}
	return semaphore.NewWeighted(int64(concurrency))
}

type pendingMarkerState struct {
	exists       bool
	ttl          time.Duration
	degraded     bool
	backpressure usercache.BackpressureLevel
	leaseOwner   string
	queueDepth   int64
}

// WithForceCacheRefresh 标记当前请求需要绕过负缓存/黑名单哨兵。
func WithForceCacheRefresh(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, forceCacheRefreshKey, true)
}

func forceCacheRefreshFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx.Value(forceCacheRefreshKey).(bool)
	if !ok || !v {
		return false
	}
	trace.AddRequestTag(ctx, "force_cache_refresh", true)
	return true
}

func isStrongConsistencyRequest(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v, ok := ctx.Value(forceCacheRefreshKey).(bool); ok && v {
		return true
	}
	return storectx.ForcePrimaryFromContext(ctx)
}

type batchLookupEntry struct {
	user     *v1.User
	notFound bool
	expires  time.Time
}

type batchLookupCache struct {
	mu      sync.RWMutex
	entries map[string]batchLookupEntry
}

func newBatchLookupCache() *batchLookupCache {
	return &batchLookupCache{
		entries: make(map[string]batchLookupEntry),
	}
}

func (c *batchLookupCache) get(username string) (batchLookupEntry, bool) {
	if c == nil {
		return batchLookupEntry{}, false
	}
	c.mu.RLock()
	entry, ok := c.entries[username]
	c.mu.RUnlock()
	if !ok {
		return batchLookupEntry{}, false
	}
	if time.Now().After(entry.expires) {
		c.mu.Lock()
		delete(c.entries, username)
		c.mu.Unlock()
		return batchLookupEntry{}, false
	}
	return entry, true
}

func (c *batchLookupCache) set(username string, user *v1.User, notFound bool) {
	if c == nil {
		return
	}
	entry := batchLookupEntry{
		user:     user,
		notFound: notFound,
		expires:  time.Now().Add(batchLookupCacheTTL),
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]batchLookupEntry)
	}
	c.entries[username] = entry
	c.mu.Unlock()
}

// WithBatchLookupCache ensures the context carries a per-request batch lookup cache for user existence checks.
func WithBatchLookupCache(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if existing := batchLookupCacheFromContext(ctx); existing != nil {
		return ctx
	}
	return context.WithValue(ctx, batchLookupCacheKey, newBatchLookupCache())
}

func batchLookupCacheFromContext(ctx context.Context) *batchLookupCache {
	if ctx == nil {
		return nil
	}
	if cache, ok := ctx.Value(batchLookupCacheKey).(*batchLookupCache); ok {
		return cache
	}
	return nil
}

// WithVerifyUserGone 标记当前请求用于验证用户是否已被删除。
func WithVerifyUserGone(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, verifyUserGoneKey, true)
}

func verifyUserGoneFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	marked, ok := ctx.Value(verifyUserGoneKey).(bool)
	return ok && marked
}

// NewUserService 创建用户服务实例
func NewUserService(store interfaces.Factory, redis *storage.RedisCluster, opts *options.Options, producer producer.MessageProducer[*v1.User, string], auditMgr *audit.Manager) *UserService {
	svc := &UserService{
		Store:            store,
		Redis:            redis,
		Options:          opts,
		Producer:         producer,
		Audit:            auditMgr,
		preflightLimiter: newPreflightLimiter(opts),
		poolReporter:     newPoolStatsReporterForFactory(store),
	}

	if opts != nil && opts.ServerRunOptions != nil {
		svc.contactRedisDegradeTTL = opts.ServerRunOptions.ContactDegradeCacheTTL
		svc.contactRedisHealthCheckInterval = opts.ServerRunOptions.ContactDegradeHealthCheckInterval
		if limit := opts.ServerRunOptions.ContactDegradeCacheMaxEntries; limit > 0 {
			svc.contactRedisDegradeCacheLimit = int64(limit)
		}
	}
	if svc.contactRedisDegradeTTL <= 0 {
		svc.contactRedisDegradeTTL = 20 * time.Second
	}
	if svc.contactRedisHealthCheckInterval <= 0 {
		svc.contactRedisHealthCheckInterval = 10 * time.Second
	}
	if svc.contactRedisDegradeCacheLimit <= 0 {
		svc.contactRedisDegradeCacheLimit = contactDegradeCacheDefaultMaxEntries
	}
	svc.startContactDegradeMonitor()
	if redis != nil {
		// 初始化待审批协调器
		cfg := usercache.PendingCoordinatorConfig{
			LeaseTTL:       svc.pendingCreateTTL(),
			Component:      "user_service",
			LogLeaseEvents: true,
		}
		// 通过 DegradeActive 将全局 Redis 降级视图注入 PendingCoordinator，
		// 降级期间由协调器内部自动切换到内存 fallback，而不是由上层直接跳过占位。
		cfg.DegradeActive = func(ctx context.Context) bool {
			return svc.isRedisDegradeActive()
		}
		if opts != nil && opts.KafkaOptions != nil {
			kopts := opts.KafkaOptions
			if kopts.PendingLeaseTTL > 0 {
				cfg.LeaseTTL = kopts.PendingLeaseTTL
			}
			if strings.TrimSpace(kopts.PendingMetricsKey) != "" {
				cfg.MetricsKey = strings.TrimSpace(kopts.PendingMetricsKey)
			}
			if kopts.PendingBackpressureWindow > 0 {
				cfg.BackpressureWindow = kopts.PendingBackpressureWindow
			}
			if kopts.PendingBackpressureSoft > 0 {
				cfg.BackpressureSoftLimit = kopts.PendingBackpressureSoft
			}
			if kopts.PendingBackpressureHard > 0 {
				cfg.BackpressureHardLimit = kopts.PendingBackpressureHard
			}
			if kopts.PendingReleaseRetention > 0 {
				cfg.ReleaseRetention = kopts.PendingReleaseRetention
			}
			if kopts.PendingExpiredRetention > 0 {
				cfg.ExpiredRetention = kopts.PendingExpiredRetention
			}
			if kopts.PendingExpiredGrace >= 0 {
				cfg.ExpiredGracePeriod = kopts.PendingExpiredGrace
			}
			if kopts.PendingDelayElevated > 0 {
				cfg.ElevatedDelayBase = kopts.PendingDelayElevated
			}
			if kopts.PendingDelayElevatedMax > 0 {
				cfg.ElevatedDelayMax = kopts.PendingDelayElevatedMax
			}
			if kopts.PendingDelaySevere > 0 {
				cfg.SevereDelayBase = kopts.PendingDelaySevere
			}
			if kopts.PendingDelaySevereMax > 0 {
				cfg.SevereDelayMax = kopts.PendingDelaySevereMax
			}
		}
		warnIfPendingDelayExceedsTimeout(cfg, opts)
		svc.pendingCoordinator = usercache.NewPendingCoordinator(redis, cfg)
	}
	svc.ensureOperationModeController()
	svc.initCreatePipeline()
	return svc
}

func warnIfPendingDelayExceedsTimeout(cfg usercache.PendingCoordinatorConfig, opts *options.Options) {
	if opts == nil || opts.ServerRunOptions == nil {
		return
	}
	budget := opts.ServerRunOptions.CtxTimeout
	if budget <= 0 {
		return
	}
	maxProfileDelay := maxDuration(cfg.ElevatedDelayMax, cfg.SevereDelayMax, cfg.ElevatedDelayBase, cfg.SevereDelayBase)
	if pendingBackpressureMaxDelay > 0 && pendingBackpressureMaxDelay > budget {
		log.Warnw("pending backpressure max delay exceeds request timeout budget", "pending_delay_ms", pendingBackpressureMaxDelay.Milliseconds(), "request_timeout_ms", budget.Milliseconds())
	}
	if maxProfileDelay > budget {
		log.Warnw("pending backpressure profile delay exceeds request timeout budget", "profile_delay_ms", maxProfileDelay.Milliseconds(), "request_timeout_ms", budget.Milliseconds())
	}
}

func maxDuration(values ...time.Duration) time.Duration {
	maxDelay := time.Duration(0)
	for _, v := range values {
		if v > maxDelay {
			maxDelay = v
		}
	}
	return maxDelay
}

// PendingCoordinator exposes the pending lease coordinator for downstream components (e.g. HTTP middleware).
func (u *UserService) PendingCoordinator() *usercache.PendingCoordinator {
	if u == nil {
		return nil
	}
	return u.pendingCoordinator
}

func (u *UserService) ensureOperationModeController() *operationModeController {
	if u == nil {
		return nil
	}
	u.operationModeInit.Do(func() {
		cfg := defaultOperationModeConfig()
		if u.Options != nil && u.Options.ServerRunOptions != nil {
			cfg = operationModeConfigFromOptions(u.Options.ServerRunOptions)
		}
		u.operationModeCtrl = newOperationModeController(cfg)
	})
	return u.operationModeCtrl
}

// decideOperationMode 决定指定用户在当前请求下的操作模式。
func (u *UserService) decideOperationMode(ctx context.Context, kind operation.OperationKind, subject string) OperationMode {
	ctrl := u.ensureOperationModeController()
	decision := OperationModeDecision{Mode: OperationModeQueue}
	snapshot := cloneOperationModeConfig(defaultOperationModeConfig())
	if ctrl != nil {
		decision = ctrl.DecideDetailed(ctx, kind, subject)
		snapshot = ctrl.Snapshot()
	}
	spanCtx, span := trace.StartSpan(ctx, "user-service", "decide_operation_mode")
	tagCtx := ctx
	if spanCtx != nil {
		tagCtx = spanCtx
	}
	// 按决策优先级写入 trace 标签，便于阅读
	trace.AddRequestTag(tagCtx, "queue_kinds_hit", decision.QueueKindsHit)
	if decision.SubjectBlocked {
		trace.AddRequestTag(tagCtx, "subject_block", true)
	}
	if decision.SubjectAllowed {
		trace.AddRequestTag(tagCtx, "subject_allow", true)
	}
	trace.AddRequestTag(tagCtx, "mode", decision.Mode.String())
	trace.AddRequestTag(tagCtx, "rollout_sample", decision.RolloutSample)
	if subject != "" {
		trace.AddRequestTag(tagCtx, "subject", subject)
	}
	if snapshot.RolloutPercent > 0 {
		trace.AddRequestTag(tagCtx, "rollout_percent", snapshot.RolloutPercent)
	}
	if snapshot.StickyHeader != "" {
		trace.AddRequestTag(tagCtx, "rollout_sticky_header", snapshot.StickyHeader)
	}
	if len(snapshot.AllowUsers) > 0 {
		trace.AddRequestTag(tagCtx, "allow_users", strings.Join(snapshot.AllowUsers, ","))
	}
	if len(snapshot.BlockUsers) > 0 {
		trace.AddRequestTag(tagCtx, "block_users", strings.Join(snapshot.BlockUsers, ","))
	}
	if span != nil {
		trace.EndSpan(span, "success", strconv.Itoa(code.ErrSuccess), map[string]interface{}{
			"mode":             decision.Mode.String(),
			"queue_kinds_hit":  decision.QueueKindsHit,
			"subject_block":    decision.SubjectBlocked,
			"subject_allow":    decision.SubjectAllowed,
			"rollout_sample":   decision.RolloutSample,
			"rollout_percent":  snapshot.RolloutPercent,
			"rollout_sticky":   snapshot.StickyHeader,
			"allow_users":      strings.Join(snapshot.AllowUsers, ","),
			"block_users":      strings.Join(snapshot.BlockUsers, ","),
			"operation_kind":   string(kind),
			"decision_subject": strings.TrimSpace(subject),
		})
	}

	return decision.Mode
}

func (u *UserService) UpdateOperationMode(cfg OperationModeConfig) OperationModeConfig {
	ctrl := u.ensureOperationModeController()
	if ctrl == nil {
		return cloneOperationModeConfig(defaultOperationModeConfig())
	}
	return ctrl.Update(cfg)
}

func (u *UserService) OperationModeSnapshot() OperationModeConfig {
	ctrl := u.ensureOperationModeController()
	if ctrl == nil {
		return cloneOperationModeConfig(defaultOperationModeConfig())
	}
	return ctrl.Snapshot()
}

// userStoreReadOnly 获取只读用户存储接口
func (u *UserService) userStoreReadOnly() interfaces.UserStore {
	if u == nil || u.Store == nil {
		return nil
	}
	store := u.Store.Users()
	if store == nil {
		return nil
	}
	if clusterAware, ok := store.(userStoreWithReadOnly); ok {
		if ro := clusterAware.ReadOnly(); ro != nil {
			return ro
		}
	}
	return store
}

// 记录createStepSlowThreshold 用户创建步骤慢日志阈值
func (u *UserService) recordUserCreateStep(ctx context.Context, step, field, username string, duration time.Duration, stepErr error) {
	metrics.RecordUserCreateStep(step, field, userctx.AccountType(ctx), duration, stepErr)

	if duration <= createStepSlowThreshold {
		return
	}
	fields := []interface{}{"step", step, "field", field, "duration", duration.String(), "username", username}
	if ctx != nil {
		if requestID := ctx.Value("requestID"); requestID != nil {
			fields = append(fields, "requestID", fmt.Sprint(requestID))
		}
	}
	if stepErr != nil {
		fields = append(fields, "error", stepErr.Error())
	}
	log.Warnw("用户创建链路耗时超过200ms", fields...)
}

type UserSrv interface {
	Create(ctx context.Context, user *v1.User, opts metav1.CreateOptions, opt *options.Options) error
	Update(ctx context.Context, user *v1.User, opts metav1.UpdateOptions, opt *options.Options) error
	BatchPatch(ctx context.Context, update *v1.User, opt *options.Options) error
	Delete(ctx context.Context, username string, force bool, opts metav1.DeleteOptions, opt *options.Options) error
	DeleteCollection(ctx context.Context, username []string, force bool, opts metav1.DeleteOptions, opt *options.Options) error
	Get(ctx context.Context, username string, opts metav1.GetOptions, opt *options.Options) (*v1.User, error)
	List(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error)
	ListWithBadPerformance(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error)
	ChangePassword(ctx context.Context, user *v1.User, claims *jwtvalidator.CustomClaims, opt *options.Options) error
}

type userStoreWithReadOnly interface {
	interfaces.UserStore
	ReadOnly() interfaces.UserStore
}

// getFromCache 从Redis获取缓存数据
func (u *UserService) getFromCache(ctx context.Context, cacheKey string) (*v1.User, bool, error) {
	startTime := time.Now()
	var operationErr error
	var cacheHit bool

	defer func() {
		metrics.RecordRedisOperation("get", time.Since(startTime).Seconds(), operationErr)
	}()

	data, err := u.Redis.GetKey(ctx, cacheKey)
	if err != nil {
		operationErr = err
		if errors.Is(err, redis.Nil) {
			log.Warnf("未进行缓存缓 key=%s", cacheKey)
			return nil, false, nil
		}
		log.Errorf("redis服务失败: key=%s, err=%v", cacheKey, err)
		return nil, false, err
	}

	var result *v1.User
	switch data {
	case RATE_LIMIT_PREVENTION:
		result = &v1.User{ObjectMeta: metav1.ObjectMeta{Name: RATE_LIMIT_PREVENTION}, Status: -1}
		cacheHit = true
	case BLACKLIST_SENTINEL:
		result = &v1.User{ObjectMeta: metav1.ObjectMeta{Name: BLACKLIST_SENTINEL}, Status: -2}
		cacheHit = true
	default:
		decoded, decodeErr := usercache.Unmarshal([]byte(data))
		if decodeErr != nil {
			operationErr = decodeErr
			return nil, false, errors.WithCode(code.ErrDecodingFailed, "数据解码失败")
		}
		if decoded == nil {
			return nil, true, errors.New("无效的用户数据")
		}
		result = decoded
		cacheHit = true
	}

	return result, cacheHit, nil
}

// getUserFromDBAndSetCache 带缓存的用户查询核心逻辑
func (u *UserService) getUserFromDBAndSetCache(ctx context.Context, username string) (*v1.User, error) {
	defer u.reportDBPoolStats(ctx, "apiserver_user_service")

	strongConsistency := isStrongConsistencyRequest(ctx)
	attempts := 0

	for {
		user, err := u.Store.Users().Get(ctx, username, metav1.GetOptions{}, u.Options)
		if err != nil {
			if errors.IsCode(err, code.ErrUserNotFound) {
				metrics.DBQueries.WithLabelValues("not_found").Inc()
				if strongConsistency {
					if state, lookupErr := u.lookupPendingCreateMarker(ctx, username); lookupErr != nil {
						trace.AddRequestTag(ctx, "pending_marker_lookup_error", lookupErr.Error())
						log.Debugw("强一致查询pending标记检测失败", "username", username, "error", lookupErr)
					} else if state.exists {
						trace.AddRequestTag(ctx, "pending_marker_active", true)
						if state.ttl > 0 {
							trace.AddRequestTag(ctx, "pending_marker_ttl_ms", state.ttl.Milliseconds())
						}
						if state.degraded {
							trace.AddRequestTag(ctx, "pending_marker_degraded", true)
						}
						message := "用户正在创建中，请稍后重试"
						if state.degraded {
							message = "用户创建正在排队，请稍后重试"
						}
						return nil, errors.WithCode(code.ErrUserNotFound, "%s", message)
					}
				}
				cacheApplied, blacklisted := u.handleProtectionForMiss(ctx, username)
				switch {
				case blacklisted:
					return &v1.User{ObjectMeta: metav1.ObjectMeta{Name: BLACKLIST_SENTINEL}}, nil
				case cacheApplied:
					return &v1.User{ObjectMeta: metav1.ObjectMeta{Name: RATE_LIMIT_PREVENTION}}, nil
				default:
					return nil, nil
				}
			}

			if strongConsistency {
				if retry, translatedErr := u.handleStrongConsistencyReadError(ctx, username, attempts, err); retry {
					attempts++
					continue
				} else if translatedErr != nil {
					return nil, translatedErr
				}
			}
			return nil, err
		}

		if user == nil {
			return nil, nil
		}

		// 写入缓存（带随机过期时间防雪崩）
		u.setUserCache(ctx, username, user)

		logger.Debugf("为用户%s设置缓存成功", username)
		return user, nil
	}
}

func (u *UserService) strongConsistencyRetryLimit() int {
	return strongConsistencyMaxRetries
}

func (u *UserService) strongConsistencyBackoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	factor := 1 << uint(attempt)
	delay := strongConsistencyBackoffBase * time.Duration(factor)
	if delay > strongConsistencyBackoffCeiling {
		return strongConsistencyBackoffCeiling
	}
	return delay
}

func (u *UserService) strongConsistencyProbeDelay() time.Duration {
	base := strongConsistencyInitialDelayBase
	if base <= 0 {
		return 0
	}
	jitter := strongConsistencyInitialDelayJitter
	if jitter <= 0 {
		return base
	}
	delta := time.Duration(rand.Int63n(int64(jitter)))
	return base + delta
}

func waitWithContext(ctx context.Context, delay time.Duration) (bool, time.Duration) {
	if delay <= 0 {
		return true, 0
	}
	start := time.Now()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, time.Since(start)
	case <-timer.C:
		return true, time.Since(start)
	}
}

func (u *UserService) handleStrongConsistencyReadError(ctx context.Context, username string, attempt int, queryErr error) (bool, error) {
	if queryErr == nil {
		return false, nil
	}
	if errors.GetCode(queryErr) != code.ErrDatabaseTimeout {
		return false, nil
	}

	state, lookupErr := u.lookupPendingCreateMarker(ctx, username)
	if lookupErr != nil {
		trace.AddRequestTag(ctx, "pending_marker_lookup_error", lookupErr.Error())
		log.Debugw("强一致查询pending标记检测失败", "username", username, "error", lookupErr)
		return false, nil
	}
	if !state.exists {
		return false, nil
	}

	trace.AddRequestTag(ctx, "strong_consistency_pending", true)
	if state.ttl > 0 {
		trace.AddRequestTag(ctx, "pending_marker_ttl_ms", state.ttl.Milliseconds())
	}
	if state.degraded {
		trace.AddRequestTag(ctx, "pending_marker_degraded", true)
	}

	maxAttempts := u.strongConsistencyRetryLimit()
	if attempt+1 < maxAttempts {
		delay := u.strongConsistencyBackoffDelay(attempt)
		trace.AddRequestTag(ctx, fmt.Sprintf("strong_consistency_retry_delay_ms_%d", attempt+1), delay.Milliseconds())
		if ok, _ := waitWithContext(ctx, delay); ok {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, queryErr
		}
	}

	message := "用户正在创建中，请稍后重试"
	if state.degraded {
		message = "用户创建正在排队，请稍后重试"
	}
	return false, errors.WithCode(code.ErrUserNotFound, "%s", message)
}

func (u *UserService) lookupPendingCreateMarker(ctx context.Context, username string) (pendingMarkerState, error) {
	state := pendingMarkerState{}
	if u == nil {
		return state, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return state, nil
	}
	if u.pendingCoordinator == nil {
		err := errors.WithCode(code.ErrServerBusy, "pending coordinator 未初始化")
		trace.AddRequestTag(ctx, "pending_coordinator_missing", true)
		return state, err
	}
	snapshot, err := u.pendingCoordinator.Observe(ctx, trimmed)
	if err != nil {
		return state, err
	}
	if snapshot == nil || !snapshot.Exists {
		return state, nil
	}
	state.exists = true
	state.ttl = snapshot.TTL
	state.backpressure = snapshot.Backpressure
	state.leaseOwner = snapshot.LeaseOwner
	state.queueDepth = snapshot.QueueDepth
	if snapshot.Backpressure != usercache.BackpressureNone {
		state.degraded = true
	}
	if !state.degraded {
		if degraded, decodeErr := usercache.PendingMarkerIsDegraded(snapshot.Raw); decodeErr != nil {
			trace.AddRequestTag(ctx, "pending_marker_decode_error", decodeErr.Error())
		} else if degraded {
			state.degraded = true
		}
	}
	return state, nil
}

// setUserCache 设置用户缓存
func (u *UserService) setUserCache(ctx context.Context, username string, user *v1.User) error {
	startTime := time.Now()
	var operationErr error

	defer func() {
		metrics.RecordRedisOperation("set", time.Since(startTime).Seconds(), operationErr)
	}()

	data, err := usercache.Marshal(user)
	if err != nil {
		operationErr = err
		log.L(ctx).Errorf("用户数据序列化失败", "error", err.Error())
		return errors.Wrap(err, "用户数据序列化失败")
	}

	// 基础过期时间 + 随机时间防雪崩
	baseExpire := 1 * time.Hour
	randomExpire := time.Duration(rand.Intn(300)) * time.Second
	expireTime := baseExpire + randomExpire
	cacheKey := u.generateUserCacheKey(username)
	operationErr = u.Redis.SetKey(ctx, cacheKey, string(data), expireTime)
	if operationErr != nil {
		log.L(ctx).Errorf("缓存写入失败", "error", operationErr.Error())
		return operationErr
	}
	return nil
}

// cacheNullValue 缓存空值（防穿透）
func (u *UserService) cacheNullValue(ctx context.Context, username string, ttl time.Duration) error {
	if u.Redis == nil || username == "" {
		return nil
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()

	cacheKey := u.generateUserCacheKey(username)
	expireTime := ttl
	if expireTime <= 0 {
		expireTime = 45 * time.Second
	}
	if jitter := time.Duration(rand.Intn(5)) * time.Second; jitter > 0 {
		expireTime += jitter
	}

	return u.Redis.SetKey(redisCtx, cacheKey, RATE_LIMIT_PREVENTION, expireTime)
}

// 这是为了防止“看到负缓存就人人去读库”造成雪崩： • 当命中了负缓存但    force=false，我们只想偶尔刷新一次来确认数据是否真的不存在。 • shouldRefreshNullCache 会尝试在 Redis 里加一把短 TTL 的互斥锁（或标志），只有拿到锁的那一个请求才允许回源刷新；没拿到锁的直接沿用旧的 negative 哨兵。 • 这样一来，多个请求同时命中负缓存时，不会都跑去查数据库；有锁的那一个负责刷新成功则更新缓存，其它请求复用新结果或继续读旧缓存即可。 • 如果 Redis 不可用，就返回 false，表示不要刷新，防止在 Redis 故障期间因每次都回源而把数据库打穿。  因此在“负缓存 + 非强制刷新”这条路径里，先调用 shouldRefreshNullCache 来控制是否需要真正回源刷新。shouldRefreshNullCache 检测是否需要刷新负缓存，如果需要则返回 true 和锁的 key，调用方负责在刷新完成后释放锁。
// “force=false” 并不是完全禁止查库，而是“不立刻回源”，具体逻辑是：
// 命中负缓存时先看 force。为 true 就直接回源刷新，这种强一致场景不讨论force=false 时，会调用 shouldRefreshNullCache 尝试拿一个短 TTL 的刷新锁。拿到锁：认为当前请求有资格“代表大家去确认一次”，于是回源查库并刷新缓存。没拿到锁：为了避免所有请求都去读库，就直接沿用现有负缓存结果（返回 nil,true），不再查库。所以“非强制”只是在绝大多数请求上不查库如果不需要刷新则返回 false，调用方直接沿用旧的负缓存结果。
func (u *UserService) shouldRefreshNullCache(ctx context.Context, username string) (bool, string) {
	if u.Redis == nil {
		return false, ""
	}
	lockKey := u.generateNullRefreshLockKey(username)
	lockTimeout := u.Options.RedisOptions.Timeout
	if lockTimeout <= 0 {
		lockTimeout = 500 * time.Millisecond
	}
	lockCtx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	success, err := u.Redis.SetNX(lockCtx, lockKey, "1", 2*time.Second)
	if err != nil {
		log.Warnf("获取负缓存刷新锁失败: username=%s err=%v", username, err)
		return false, ""
	}
	return success, lockKey
}

// releaseNullCacheRefreshLock 释放负缓存刷新锁
func (u *UserService) releaseNullCacheRefreshLock(lockKey string) {
	if lockKey == "" {
		return
	}
	releaseTimeout := u.Options.RedisOptions.Timeout
	if releaseTimeout <= 0 {
		releaseTimeout = 500 * time.Millisecond
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if _, err := u.Redis.DeleteKey(releaseCtx, lockKey); err != nil && err != redis.Nil {
		log.Warnf("释放负缓存刷新锁失败: key=%s err=%v", lockKey, err)
	}
}

// refreshUserCacheFromDB 从数据库刷新用户缓存
func (u *UserService) refreshUserCacheFromDB(ctx context.Context, username string) (*v1.User, error) {
	refreshKey := fmt.Sprintf("refresh:%s", username)
	result, err, _ := u.group.Do(refreshKey, func() (interface{}, error) {
		dbCtx, cancel := u.newDBContext(ctx, u.contactRefreshTimeout())
		dbCtx = storectx.WithForcePrimary(dbCtx)
		defer cancel()
		return u.getUserFromDBAndSetCache(dbCtx, username)
	})

	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	user := result.(*v1.User)
	if user == nil || user.Name == RATE_LIMIT_PREVENTION || user.Name == BLACKLIST_SENTINEL {
		return nil, nil
	}
	return user, nil
}

func (u *UserService) generateNullRefreshLockKey(username string) string {
	return fmt.Sprintf("%s:refresh-lock", u.generateUserCacheKey(username))
}

func (u *UserService) generateUserCacheKey(username string) string {
	return usercache.UserKey(username)
}

func (u *UserService) generateEmailCacheKey(email string) string {
	return usercache.EmailKey(email)
}

func (u *UserService) generatePhoneCacheKey(phone string) string {
	return usercache.PhoneKey(phone)
}

func (u *UserService) protectionConfig() serveropts.ProtectionConfig {
	defaults := serveropts.DefaultProtectionConfig()
	if u == nil || u.Options == nil || u.Options.AuditOptions == nil {
		return defaults
	}
	cfg := u.Options.AuditOptions.Protection
	if cfg.NegativeCacheThreshold <= 0 {
		cfg.NegativeCacheThreshold = defaults.NegativeCacheThreshold
	}
	if cfg.NegativeCacheWindow <= 0 {
		cfg.NegativeCacheWindow = defaults.NegativeCacheWindow
	}
	if cfg.NegativeCacheTTL <= 0 {
		cfg.NegativeCacheTTL = defaults.NegativeCacheTTL
	}
	if cfg.BlockThreshold <= 0 {
		cfg.BlockThreshold = defaults.BlockThreshold
	}
	if cfg.BlockWindow <= 0 {
		cfg.BlockWindow = defaults.BlockWindow
	}
	if cfg.BlockDuration <= 0 {
		cfg.BlockDuration = defaults.BlockDuration
	}
	return cfg
}

func durationToSecondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	seconds := d / time.Second
	if d%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		seconds = 1
	}
	return int64(seconds)
}

// tagIfLockWait 根据错误内容判断是否为锁等待相关错误，若是则在 trace 上打标签
// 记录“锁等待”是为了在观测层快速定位数据库层面的性能瓶颈。大多数 get/create/delete 的错误都只是“未找到”“参数错误”这类业务问题，而锁等待代表数据库行/表被其他事务长时间占用，通常预示着竞争或死锁隐患。 •    tagIfLockWait 会在 trace 上打    tag.lock_wait=<前缀> 之类的标记，方便在 Jaeger/Tempo 这类链路追踪系统里一眼筛出“因为锁阻塞而慢/失败”的请求；配合指标还能统计锁等待的发生频率、定位具体 SQL/用户。 • 没有这个标签的话，锁等待只是“数据库超时”或“未知错误”，需要翻日志才能确认；加上标签就能直接在可观测平台里看到“这条请求是由于锁等待失败的”，极大缩短排障时间。tag: 用于标识标签前缀
func tagIfLockWait(ctx context.Context, err error, tag string) {
	if ctx == nil || err == nil {
		return
	}
	if errors.IsCode(err, code.ErrDatabaseTimeout) {
		trace.AddRequestTag(ctx, tag+"_lock_wait", true)
		return
	}
	lowered := strings.ToLower(err.Error())
	if strings.Contains(lowered, "lock wait") || strings.Contains(lowered, "deadlock") {
		trace.AddRequestTag(ctx, tag+"_lock_wait", true)
	}
}

// pendingCreateTTL 获取用户创建占位标记的 TTL 配置
func (u *UserService) pendingCreateTTL() time.Duration {
	minTTL := serveropts.MinUserPendingCreateTTL
	if u == nil || u.Options == nil || u.Options.ServerRunOptions == nil {
		return minTTL
	}
	ttl := u.Options.ServerRunOptions.UserPendingCreateTTL
	if ttl < minTTL {
		return minTTL
	}
	return ttl
}

func (u *UserService) pendingLeaseMetadata(ctx context.Context, username string) usercache.LeaseMetadata {
	meta := usercache.LeaseMetadata{Username: username}
	if traceCtx := trace.FromContext(ctx); traceCtx != nil {
		meta.RequestID = traceCtx.RequestContext.RequestID
		meta.Operator = traceCtx.RequestContext.Operator
		meta.ClientIP = traceCtx.RequestContext.ClientIP
	}
	if legacyID := ctx.Value("requestID"); legacyID != nil {
		meta.LegacyRequestID = fmt.Sprint(legacyID)
	}
	if u != nil && u.pendingCoordinator != nil {
		meta.Backend = u.pendingCoordinator.Backend()
	}
	return meta
}

// markUserPendingCreate 为用户创建流程写入 Redis 占位标记
//
// 通过 SetNX 和 TTL 刷新机制标识某个用户名处于“创建中”状态，供消费侧和并发请求识别；同时记录相关耗时指标。
//
// 参数：
//
//	ctx: 调用上下文，需携带 trace 与取消控制
//	username: 需要设置占位标记的用户名
//
// 返回值：
//
//	bool: 是否首次创建占位
//	bool: 是否刷新了已有占位
//	time.Duration: 占位剩余 TTL
//	time.Duration: SetNX 操作耗时
//	time.Duration: TTL 刷新耗时
//	error: 写入或刷新过程中出现的错误，nil 表示占位成功
//
// 示例：
//
//	created, refreshed, ttl, setCost, refreshCost, err := u.markUserPendingCreate(ctx, "alice")
//	if err != nil {
//	    // 处理占位异常
//	}
//
// 注意事项：
//   - 当 Redis 未初始化时会直接返回错误
//   - 调用方需根据返回值判断是否需要额外处理并发情况
//
// 异常情况：
//   - Redis 操作失败会返回 ErrRedis 相关错误码
//   - 当上下文超时时会提前终止并返回错误
//
// 流程:参数校验 → 降级检查 → 背压采样+延迟 → 抢占租约 → 结果处理/异常兜底
func (u *UserService) markUserPendingCreate(ctx context.Context, username string) (bool, bool, time.Duration, time.Duration, time.Duration, string, string, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return false, false, 0, 0, 0, "", "", nil
	}
	if u.pendingCoordinator == nil {
		trace.AddRequestTag(ctx, "pending_coordinator_missing", true)
		log.Errorw("pending coordinator not initialized", "component", "user_service", "username", trimmed)
		return false, false, 0, 0, 0, "", "", errors.WithCode(code.ErrServerBusy, "pending coordinator 未初始化")
	}
	componentLabel := "user_service"
	// 根据采样排队深度与背压状态计算出延迟时间,并在必要时进行等待
	sample, hasSample := usercache.BackpressureSampleFromContext(ctx)
	if !hasSample || (sample.Username != "" && sample.Username != trimmed) {
		sample = u.pendingCoordinator.SampleBackpressure(ctx, trimmed)
		ctx = usercache.ContextWithBackpressureSample(ctx, sample)
	}
	if sample.GlobalErr != nil {
		trace.AddRequestTag(ctx, "pending_queue_sample_error", sample.GlobalErr.Error())
	}
	if sample.UserErr != nil {
		trace.AddRequestTag(ctx, "pending_user_sample_error", sample.UserErr.Error())
	}
	depth := sample.MaxDepth()
	level := sample.AggregateLevel
	if depth > 0 {
		trace.AddRequestTag(ctx, "pending_queue_depth", depth)
	}
	if level != usercache.BackpressureNone {
		trace.AddRequestTag(ctx, "pending_backpressure_level", string(level))
		if metrics.PendingLeaseEvents != nil {
			metrics.PendingLeaseEvents.WithLabelValues(componentLabel, "pre_acquire_backpressure").Inc()
		}
		if rawDelay := u.pendingCoordinator.BackpressureDelay(level, depth); rawDelay > 0 {
			if pendingBackpressureMaxDelay > 0 && rawDelay > pendingBackpressureMaxDelay {
				rawDelay = pendingBackpressureMaxDelay
			}
			requestElapsed := backpressure.LeadTime(ctx, time.Time{})
			if requestElapsed > 0 {
				trace.AddRequestTag(ctx, "pending_backpressure_elapsed_ms", requestElapsed.Milliseconds())
				metrics.RecordPendingBackpressureLeadTime(componentLabel, string(level), requestElapsed)
			}
			decision := backpressure.AlignDelayWithDeadline(ctx, rawDelay, 0)
			if decision.Action != "" {
				metrics.RecordPendingBackpressureDeadlineDecision(componentLabel, string(level), decision.Action)
				trace.AddRequestTag(ctx, "pending_backpressure_deadline_action", decision.Action)
				remaining := decision.Remaining
				if remaining < 0 {
					remaining = 0
				}
				trace.AddRequestTag(ctx, "pending_backpressure_deadline_remaining_ms", remaining.Milliseconds())
				log.Infow("pending lease delay adjusted by deadline", "component", componentLabel, "username", trimmed, "action", decision.Action, "requested_delay_ms", rawDelay.Milliseconds(), "remaining_budget_ms", decision.Remaining.Milliseconds())
			}
			delay := decision.Effective
			if delay <= 0 {
				trace.AddRequestTag(ctx, "pending_backpressure_delay_skipped", true)
			} else {
				metrics.RecordPendingBackpressureDelay(componentLabel, string(level), delay)
				trace.AddRequestTag(ctx, "pending_backpressure_delay_ms", delay.Milliseconds())
				log.Infow("pending lease pre-acquire delay", "component", componentLabel, "username", trimmed, "queue_depth", depth, "backpressure", string(level), "delay_ms", delay.Milliseconds())
				if ok, waited := waitWithContext(ctx, delay); !ok {
					metrics.RecordPendingBackpressureDelayCancellation(componentLabel, string(level))
					metrics.ObservePendingBackpressureCancellationDuration(componentLabel, string(level), waited)
					if waited > 0 {
						trace.AddRequestTag(ctx, "pending_backpressure_waited_before_cancel_ms", waited.Milliseconds())
					}
					if ctx != nil {
						return false, false, 0, 0, 0, "", "", ctx.Err()
					}
					return false, false, 0, 0, 0, "", "", context.Canceled
				}
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues(componentLabel, "pre_acquire_delay").Inc()
				}
			}
		}
	}
	meta := u.pendingLeaseMetadata(ctx, trimmed)
	// 等待delay事件后尝试抢占待创建租约
	result, err := u.pendingCoordinator.Acquire(ctx, trimmed, meta)
	if err != nil {
		var acquireErr *usercache.AcquireError
		if stdErrors.As(err, &acquireErr) {
			if acquireErr.State != nil {
				if acquireErr.State.QueueDepth > 0 {
					trace.AddRequestTag(ctx, "pending_queue_depth", acquireErr.State.QueueDepth)
				}
				if acquireErr.State.Backpressure != usercache.BackpressureNone {
					trace.AddRequestTag(ctx, "pending_backpressure_level", string(acquireErr.State.Backpressure))
				}
				if acquireErr.State.State == usercache.PendingStateExpired {
					trace.AddRequestTag(ctx, "pending_expired", true)
					if !acquireErr.State.ExpiredAt.IsZero() {
						trace.AddRequestTag(ctx, "pending_expired_at", acquireErr.State.ExpiredAt.Format(time.RFC3339Nano))
					}
					u.handleExpiredPendingConflict(ctx, trimmed, acquireErr.State)
					return false, false, 0, 0, 0, "", "", errors.WithCode(code.ErrServerBusy, "用户创建任务正在恢复，请稍后再试")
				}
			}
			switch acquireErr.Reason {
			case usercache.AcquireFailureBackpressure:
				var depth int64
				var level usercache.BackpressureLevel
				if acquireErr.State != nil {
					depth = acquireErr.State.QueueDepth
					level = acquireErr.State.Backpressure
				}
				log.Warnw("pending lease rejected by backpressure", "component", "user_service", "username", trimmed, "queue_depth", depth, "backpressure", string(level))
				return false, false, 0, 0, 0, "", "", errors.WithCode(code.ErrServerBusy, "用户创建排队中，请稍后重试")
			case usercache.AcquireFailureConflict:
				return false, false, 0, 0, 0, "", "", errors.WithCode(code.ErrServerBusy, "用户创建正在进行，请稍后再试")
			}
		}
		return false, false, 0, 0, 0, "", "", err
	}
	lease := result.Lease
	if lease == nil {
		trace.AddRequestTag(ctx, "pending_marker_setnx_ms", result.SetNXDuration.Milliseconds())
		backend := ""
		if u.pendingCoordinator != nil {
			backend = u.pendingCoordinator.Backend()
		}
		return false, false, 0, result.SetNXDuration, 0, "", backend, nil
	}
	pendingTTL := time.Until(lease.LeaseExpiresAt)
	if pendingTTL < 0 {
		pendingTTL = 0
	}
	trace.AddRequestTag(ctx, "pending_marker_new", true)
	trace.AddRequestTag(ctx, "pending_marker_setnx_ms", result.SetNXDuration.Milliseconds())
	if lease.QueueDepth > 0 {
		trace.AddRequestTag(ctx, "pending_queue_depth", lease.QueueDepth)
	}
	if lease.Backpressure != usercache.BackpressureNone {
		trace.AddRequestTag(ctx, "pending_backpressure_level", string(lease.Backpressure))
	}
	if pendingTTL > 0 {
		trace.AddRequestTag(ctx, "pending_marker_ttl_ms", pendingTTL.Milliseconds())
	}
	trace.AddRequestTag(ctx, "pending_lease_owner", lease.OwnerID)
	backend := strings.TrimSpace(lease.Metadata.Backend)
	if backend == "" && u.pendingCoordinator != nil {
		backend = u.pendingCoordinator.Backend()
	}
	return true, false, pendingTTL, result.SetNXDuration, 0, lease.OwnerID, backend, nil
}

func (u *UserService) handleExpiredPendingConflict(ctx context.Context, username string, state *usercache.PendingState) {
	if state == nil {
		return
	}
	fields := []interface{}{"component", "user_service", "username", username}
	if !state.ExpiredAt.IsZero() {
		fields = append(fields, "expired_at", state.ExpiredAt.Format(time.RFC3339Nano))
	}
	if state.QueueDepth > 0 {
		fields = append(fields, "queue_depth", state.QueueDepth)
	}
	if level := string(state.Backpressure); level != "" {
		fields = append(fields, "backpressure", level)
	}
	log.Warnw("pending lease expired conflict detected", fields...)
	if metrics.PendingLeaseEvents != nil {
		metrics.PendingLeaseEvents.WithLabelValues("user_service", "expired_conflict").Inc()
	}
	trace.AddRequestTag(ctx, "pending_expired_conflict", true)
}

func (u *UserService) pendingCoordinatorOrder(backendHint string) []*usercache.PendingCoordinator {
	if u == nil {
		return nil
	}
	coord := u.pendingCoordinator
	if coord == nil {
		return nil
	}
	return []*usercache.PendingCoordinator{coord}
}

func (u *UserService) redisOpTimeout() time.Duration {
	if u != nil && u.Options != nil && u.Options.RedisOptions != nil && u.Options.RedisOptions.Timeout > 0 {
		return u.Options.RedisOptions.Timeout
	}
	return 500 * time.Millisecond
}

func (u *UserService) redisOpContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := u.redisOpTimeout()
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (u *UserService) cacheBlacklistSentinel(ctx context.Context, username string, ttl time.Duration) error {
	if u.Redis == nil || username == "" {
		return nil
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	expire := ttl
	if expire <= 0 {
		expire = 30 * time.Minute
	}
	if jitter := time.Duration(rand.Intn(5)) * time.Second; jitter > 0 {
		expire += jitter
	}
	return u.Redis.SetKey(redisCtx, u.generateUserCacheKey(username), BLACKLIST_SENTINEL, expire)
}

type pendingHeartbeatSession struct {
	username    string
	ownerID     string
	component   string
	coordinator *usercache.PendingCoordinator
	interval    time.Duration
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func (s *pendingHeartbeatSession) start(ctx context.Context) {
	if s == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hbCtx, cancel := context.WithTimeout(ctx, pendingHeartbeatCallTimeout)
				if err := s.coordinator.Heartbeat(hbCtx, s.username, s.ownerID); err != nil {
					log.Debugw("pending lease heartbeat failed", "username", s.username, "owner", s.ownerID, "component", s.component, "error", err)
				}
				cancel()
			}
		}
	}()
}

func (s *pendingHeartbeatSession) stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (u *UserService) pendingHeartbeatInterval() time.Duration {
	if u == nil {
		return pendingHeartbeatIntervalMin
	}
	ttl := u.pendingCreateTTL()
	if ttl <= 0 {
		log.Warnw("pending heartbeat ttl invalid, fallback to default", "component", "user_service", "ttl", ttl)
		ttl = 30 * time.Second
	}
	grace := u.pendingExpiredGrace()
	if grace < 0 {
		log.Warnw("pending heartbeat grace invalid, clamped to zero", "component", "user_service", "grace", grace)
		grace = 0
	}
	base := ttl + grace
	if base <= 0 {
		log.Warnw("pending heartbeat base duration invalid", "component", "user_service", "ttl", ttl, "grace", grace)
		return pendingHeartbeatIntervalMin
	}
	interval := base / 2
	if interval < pendingHeartbeatIntervalMin {
		log.Warnw("pending heartbeat interval below minimum, clamped", "component", "user_service", "interval", interval, "min", pendingHeartbeatIntervalMin, "ttl", ttl, "grace", grace)
		interval = pendingHeartbeatIntervalMin
	}
	if interval > pendingHeartbeatIntervalMax {
		log.Warnw("pending heartbeat interval above maximum, clamped", "component", "user_service", "interval", interval, "max", pendingHeartbeatIntervalMax, "ttl", ttl, "grace", grace)
		interval = pendingHeartbeatIntervalMax
	}
	return interval
}

func (u *UserService) pendingExpiredGrace() time.Duration {
	const defaultGrace = 2 * time.Second
	if u == nil {
		return defaultGrace
	}
	if u.pendingCoordinator != nil {
		grace := u.pendingCoordinator.ExpiredGracePeriod()
		if grace >= 0 {
			return grace
		}
	}
	if u.Options != nil && u.Options.KafkaOptions != nil && u.Options.KafkaOptions.PendingExpiredGrace >= 0 {
		return u.Options.KafkaOptions.PendingExpiredGrace
	}
	log.Warnw("pending expired grace missing, fallback to default", "component", "user_service", "default_grace", defaultGrace)
	return defaultGrace
}

func (u *UserService) startPendingHeartbeatSession(operationID, username, ownerID string) {
	if u == nil || u.pendingCoordinator == nil {
		return
	}
	trimmedOwner := strings.TrimSpace(ownerID)
	trimmedUser := strings.TrimSpace(username)
	if trimmedOwner == "" || trimmedUser == "" {
		return
	}
	opID := strings.TrimSpace(operationID)
	if opID == "" {
		opID = trimmedUser
	}
	interval := u.pendingHeartbeatInterval()
	ctx, cancel := context.WithCancel(context.Background())
	session := &pendingHeartbeatSession{
		username:    trimmedUser,
		ownerID:     trimmedOwner,
		component:   "user_service",
		coordinator: u.pendingCoordinator,
		interval:    interval,
		cancel:      cancel,
	}
	session.start(ctx)
	if existing, loaded := u.pendingHeartbeats.LoadOrStore(opID, session); loaded {
		if oldSession, ok := existing.(*pendingHeartbeatSession); ok {
			oldSession.stop()
		}
		u.pendingHeartbeats.Store(opID, session)
	}
}

func (u *UserService) stopPendingHeartbeatSession(operationID string) {
	if u == nil {
		return
	}
	opID := strings.TrimSpace(operationID)
	if opID == "" {
		return
	}
	if existing, ok := u.pendingHeartbeats.LoadAndDelete(opID); ok {
		if session, ok := existing.(*pendingHeartbeatSession); ok {
			session.stop()
		}
	}
}

func (u *UserService) startHeartbeatFromEnvelope(env *operation.OperationEnvelope, username string) func() {
	if u == nil || env == nil || env.Headers == nil {
		return func() {}
	}
	owner := strings.TrimSpace(env.Headers[pendingOwnerHeader])
	if owner == "" {
		return func() {}
	}
	u.startPendingHeartbeatSession(env.ID, username, owner)
	return func() {
		u.stopPendingHeartbeatSession(env.ID)
	}
}

func (u *UserService) setBlacklist(ctx context.Context, username string, ttl time.Duration) error {
	if u.Redis == nil || username == "" {
		return nil
	}
	key := usercache.BlacklistKey(username)
	if key == "" {
		return nil
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	duration := ttl
	if duration <= 0 {
		duration = 30 * time.Minute
	}
	return u.Redis.SetKey(redisCtx, key, BLACKLIST_SENTINEL, duration)
}

func (u *UserService) clearProtectionCounters(ctx context.Context, username string) {
	if u.Redis == nil || username == "" {
		return
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	keys := []string{
		usercache.NegativeCounterKey(username),
		usercache.BlockCounterKey(username),
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, err := u.Redis.DeleteKey(redisCtx, key); err != nil && err != redis.Nil {
			log.Warnf("清理防护计数失败: key=%s err=%v", key, err)
		}
	}
}

func (u *UserService) emitProtectionAudit(ctx context.Context, username, reason string, metadata map[string]any) {
	if u == nil || u.Audit == nil {
		return
	}
	spanCtx, span := trace.StartSpan(ctx, "user-service", "audit_submit")
	if spanCtx != nil {
		ctx = spanCtx
	}
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	details := map[string]interface{}{
		"username": username,
		"reason":   reason,
	}
	eventMetadata := map[string]any{
		"username": username,
	}
	for k, v := range metadata {
		eventMetadata[k] = v
	}
	actor := ""
	requestID := ""
	clientIP := ""
	if traceCtx := trace.FromContext(ctx); traceCtx != nil {
		actor = traceCtx.RequestContext.Operator
		if actor == "" {
			actor = traceCtx.RequestContext.UserID
		}
		requestID = traceCtx.RequestContext.RequestID
		clientIP = traceCtx.RequestContext.ClientIP
		details["request_id"] = requestID
		details["client_ip"] = clientIP
		details["actor"] = actor
	}
	event := audit.Event{
		Actor:        actor,
		Action:       "user.protection." + reason,
		ResourceType: "user",
		ResourceID:   username,
		Target:       username,
		Outcome:      "warn",
		RequestID:    requestID,
		IP:           clientIP,
		Metadata:     eventMetadata,
	}
	u.Audit.Submit(ctx, event)
	trace.EndSpan(span, status, codeStr, details)
}

func (u *UserService) handleProtectionForMiss(ctx context.Context, username string) (bool, bool) {
	if u.Redis == nil || username == "" {
		return false, false
	}
	cfg := u.protectionConfig()
	cacheApplied := false
	blacklisted := false

	if cfg.NegativeCacheThreshold > 0 && cfg.NegativeCacheWindow > 0 {
		counterKey := usercache.NegativeCounterKey(username)
		if counterKey != "" {
			redisCtx, cancel := u.redisOpContext(ctx)
			count := u.Redis.IncrememntWithExpire(redisCtx, counterKey, durationToSecondsCeil(cfg.NegativeCacheWindow))
			cancel()
			if count > 0 {
				trace.AddRequestTag(ctx, "protection_negative_count", count)
				if int(count) >= cfg.NegativeCacheThreshold {
					details := map[string]any{
						"count":          count,
						"threshold":      cfg.NegativeCacheThreshold,
						"window_seconds": durationToSecondsCeil(cfg.NegativeCacheWindow),
						"ttl_seconds":    durationToSecondsCeil(cfg.NegativeCacheTTL),
					}
					if err := u.cacheNullValue(ctx, username, cfg.NegativeCacheTTL); err != nil {
						log.Warnf("写入负缓存失败: username=%s err=%v", username, err)
					} else {
						cacheApplied = true
						metrics.RecordUserProtectionEvent("negative_cache")
						trace.AddRequestTag(ctx, "protection_negative_applied", details)
						u.emitProtectionAudit(ctx, username, "negative-cache", details)
					}
				}
			}
		}
	}

	if cfg.BlockThreshold > 0 && cfg.BlockWindow > 0 {
		counterKey := usercache.BlockCounterKey(username)
		if counterKey != "" {
			redisCtx, cancel := u.redisOpContext(ctx)
			count := u.Redis.IncrememntWithExpire(redisCtx, counterKey, durationToSecondsCeil(cfg.BlockWindow))
			cancel()
			if count > 0 {
				trace.AddRequestTag(ctx, "protection_block_count", count)
				if int(count) >= cfg.BlockThreshold {
					details := map[string]any{
						"count":            count,
						"threshold":        cfg.BlockThreshold,
						"window_seconds":   durationToSecondsCeil(cfg.BlockWindow),
						"duration_seconds": durationToSecondsCeil(cfg.BlockDuration),
					}
					if err := u.setBlacklist(ctx, username, cfg.BlockDuration); err != nil {
						log.Warnf("写入黑名单失败: username=%s err=%v", username, err)
					} else {
						blacklisted = true
						metrics.RecordUserProtectionEvent("blacklist")
						if err := u.cacheBlacklistSentinel(ctx, username, cfg.BlockDuration); err != nil {
							log.Warnf("写入黑名单缓存失败: username=%s err=%v", username, err)
						} else {
							cacheApplied = true
						}
						trace.AddRequestTag(ctx, "protection_blacklist_applied", details)
						u.emitProtectionAudit(ctx, username, "blacklist", details)
						u.clearProtectionCounters(ctx, username)
					}
				}
			}
		}
	}

	return cacheApplied, blacklisted
}

func (u *UserService) isUserBlacklisted(ctx context.Context, username string) (bool, error) {
	if u.Redis == nil || username == "" {
		return false, nil
	}
	key := usercache.BlacklistKey(username)
	if key == "" {
		return false, nil
	}
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()

	value, err := u.Redis.GetKey(redisCtx, key)
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return value == BLACKLIST_SENTINEL, nil
}

func (u *UserService) normalizeUserContacts(user *v1.User) {
	if user == nil {
		return
	}
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
}

// ensureContactCacheReady 确保邮箱和手机号唯一性缓存处于预热状态
//
// 通过原子标记与定时重试机制判断是否需要异步触发 warmContactCache，避免高并发写入时命中冷缓存。
// 适用于用户创建等入口在首次访问时触发缓存预热，依赖 Redis 与用户存储已初始化。
// 参数：
//
//	ctx: 调用上下文，携带链路追踪信息以确保 span 父子关系正确
//
// 返回值：
//
//	无: 无返回值
//
// 示例：
//
//	u.ensureContactCacheReady(ctx)
//
// 注意事项：
//   - 预热过程在独立 goroutine 中执行，调用方无需等待结果
//   - 若依赖未就绪会直接返回，不会强制重试
//
// 异常情况：
//   - 预热失败会记录下一次重试时间并输出警告日志
//   - 上一次预热仍在运行时会跳过本次触发
func (u *UserService) ensureContactCacheReady(ctx context.Context) {
	if u == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	spanCtx, span := trace.StartSpan(ctx, "user-service", "ensure_contact_cache_ready")
	if spanCtx != nil {
		ctx = spanCtx
	}
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	degradeReason := ""
	createDegraded := false
	defer func() {
		ready := u.contactCacheReady.Load()
		finishDetails := map[string]interface{}{
			"ready":            ready,
			"warmup_enabled":   u.Options != nil && u.Options.ServerRunOptions != nil && u.Options.ServerRunOptions.EnableContactWarmup,
			"redis_available":  u.Redis != nil,
			"store_available":  u.Store != nil,
			"contact_degraded": u.isRedisDegradeActive(),
			"next_retry_unix":  u.contactWarmupNextRetry.Load(),
		}
		finishDetails["create_degraded"] = createDegraded
		if degradeReason != "" {
			finishDetails["degrade_reason"] = degradeReason
		}
		trace.EndSpan(span, status, codeStr, finishDetails)
	}()

	if u.contactCacheReady.Load() {
		trace.AddRequestTag(ctx, "contact_cache_ready", true)
		return
	}
	if u.Options == nil || u.Options.ServerRunOptions == nil || !u.Options.ServerRunOptions.EnableContactWarmup {
		u.contactCacheReady.Store(true)
		trace.AddRequestTag(ctx, "contact_cache_ready", true)
		trace.AddRequestTag(ctx, "create_degraded", false)
		return
	}
	if u.Store == nil || u.Redis == nil {
		trace.AddRequestTag(ctx, "contact_cache_ready", false)
		createDegraded = true
		degradeReason = "deps_not_ready"
		trace.AddRequestTag(ctx, "create_degraded", true)
		trace.AddRequestTag(ctx, "degrade_reason", degradeReason)
		status = "error"
		codeStr = strconv.Itoa(code.ErrServerBusy)
		return
	}
	next := u.contactWarmupNextRetry.Load()
	if next > 0 && time.Now().Unix() < next {
		trace.AddRequestTag(ctx, "contact_cache_ready", false)
		createDegraded = true
		degradeReason = "warmup_pending"
		trace.AddRequestTag(ctx, "create_degraded", true)
		trace.AddRequestTag(ctx, "degrade_reason", degradeReason)
		return
	}
	u.contactWarmupMu.Lock()
	if u.contactCacheReady.Load() || u.contactWarming {
		u.contactWarmupMu.Unlock()
		ready := u.contactCacheReady.Load()
		trace.AddRequestTag(ctx, "contact_cache_ready", ready)
		if !ready {
			createDegraded = true
			degradeReason = "warmup_inflight"
			trace.AddRequestTag(ctx, "create_degraded", true)
			trace.AddRequestTag(ctx, "degrade_reason", degradeReason)
		}
		return
	}
	u.contactWarming = true
	u.contactWarmupWaitCh = make(chan struct{})
	u.contactWarmupMu.Unlock()

	go func() {
		errorCtx, warmSpan := trace.StartSpan(ctx, "user-service", "contact_cache_warmup_async")
		warmStatus := "success"
		warmCode := strconv.Itoa(code.ErrSuccess)
		details := map[string]interface{}{}
		err := u.warmContactCache(errorCtx)
		if err != nil {
			warmStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				warmCode = strconv.Itoa(c)
			} else {
				warmCode = strconv.Itoa(code.ErrUnknown)
			}
			details["warmup_fail_reason"] = err.Error()
			nextRetry := u.contactWarmupNextRetry.Load()
			if nextRetry > 0 {
				delay := time.Until(time.Unix(nextRetry, 0))
				if delay < 0 {
					delay = 0
				}
				details["retry_delay_ms"] = delay.Milliseconds()
			}
		}
		trace.EndSpan(warmSpan, warmStatus, warmCode, details)
		u.completeContactWarmup(err)
	}()
}

// WarmupContactCacheBlocking 在启动阶段同步预热联系方式唯一性缓存。
//
// 当配置启用且依赖就绪后，会串行扫描用户存储并写入 Redis，直到成功或上下文取消。
// 若预热正在进行则阻塞等待结果，便于启动阶段与周期任务复用相同的互斥控制。
func (u *UserService) WarmupContactCacheBlocking(ctx context.Context) error {
	if u == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if u.contactCacheReady.Load() {
		return nil
	}
	if u.Options == nil || u.Options.ServerRunOptions == nil || !u.Options.ServerRunOptions.EnableContactWarmup {
		u.contactCacheReady.Store(true)
		return nil
	}
	if u.Store == nil || u.Redis == nil {
		return fmt.Errorf("contact warmup dependencies not ready")
	}

	for {
		u.contactWarmupMu.Lock()
		if u.contactCacheReady.Load() {
			u.contactWarmupMu.Unlock()
			return nil
		}
		if u.contactWarming {
			waitCh := u.contactWarmupWaitCh
			u.contactWarmupMu.Unlock()
			if waitCh == nil {
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case <-waitCh:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		u.contactWarming = true
		u.contactWarmupWaitCh = make(chan struct{})
		u.contactWarmupMu.Unlock()

		err := u.warmContactCache(ctx)
		u.completeContactWarmup(err)
		if err != nil {
			return err
		}
		return nil
	}
}

// StartContactWarmupLoop 启动周期性的联系方式缓存刷新任务。
//
// 若配置未启用或刷新间隔小于等于 0，将直接返回。
func (u *UserService) StartContactWarmupLoop() {
	if u == nil {
		return
	}
	if u.Options == nil || u.Options.ServerRunOptions == nil || !u.Options.ServerRunOptions.EnableContactWarmup {
		return
	}
	interval := u.Options.ServerRunOptions.ContactWarmupRefreshInterval
	if interval <= 0 {
		return
	}
	u.contactWarmupLoopOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		u.contactWarmupLoopCancel = cancel
		u.contactWarmupLoopWG.Add(1)
		go u.runContactWarmupLoop(ctx, interval)
	})
}

// StopContactWarmupLoop 停止周期性的联系方式缓存刷新任务。
func (u *UserService) StopContactWarmupLoop() {
	if u == nil {
		return
	}
	if cancel := u.contactWarmupLoopCancel; cancel != nil {
		cancel()
	}
	u.contactWarmupLoopWG.Wait()
	u.contactWarmupLoopCancel = nil
}

func (u *UserService) runContactWarmupLoop(ctx context.Context, interval time.Duration) {
	defer u.contactWarmupLoopWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			warmCtx, cancel := context.WithTimeout(ctx, contactWarmupTimeout)
			err := u.WarmupContactCacheBlocking(warmCtx)
			cancel()
			if err == nil {
				log.Debugw("周期联系人缓存预热完成", "component", "user_service", "interval", interval.String())
			}
		}
	}
}

func (u *UserService) completeContactWarmup(err error) {
	var waitCh chan struct{}
	u.contactWarmupMu.Lock()
	u.contactWarming = false
	waitCh = u.contactWarmupWaitCh
	u.contactWarmupWaitCh = nil
	if err == nil {
		u.contactCacheReady.Store(true)
		u.contactWarmupNextRetry.Store(0)
	} else if !stdErrors.Is(err, context.Canceled) {
		u.contactWarmupNextRetry.Store(time.Now().Add(contactWarmupRetryDelay).Unix())
	}
	u.contactWarmupMu.Unlock()

	if waitCh != nil {
		close(waitCh)
	}
	if err == nil || stdErrors.Is(err, context.Canceled) {
		return
	}
	log.Warnw("联系人缓存预热失败", "error", err, "retry_after", contactWarmupRetryDelay)
}

func (u *UserService) contactLookupTimeout() time.Duration {
	if u.Options != nil && u.Options.ServerRunOptions != nil && u.Options.ServerRunOptions.ContactLookupTimeout > 0 {
		return u.Options.ServerRunOptions.ContactLookupTimeout
	}
	return serveropts.DefaultContactLookupTimeout
}

func (u *UserService) contactRefreshTimeout() time.Duration {
	if u.Options != nil && u.Options.ServerRunOptions != nil && u.Options.ServerRunOptions.ContactRefreshTimeout > 0 {
		return u.Options.ServerRunOptions.ContactRefreshTimeout
	}
	return serveropts.DefaultContactRefreshTimeout
}

// shouldRunPreflight 判断当前请求是否需要执行数据库预检查
//
// 返回 true 表示需要跑预检；返回 false 则跳过预检直接依赖缓存或后续流程。
// 会根据上下文强一致标记、用户字段是否为空、缓存预热状态等条件综合决定是否访问数据库。
//
// 参数：
//
//	ctx: 当前请求上下文，可能携带强一致性标记
//	user: 待创建或校验的用户对象
//
// 返回值：
//
//	bool: true 代表执行预检，false 代表不执行
//
// 示例：
//
//	if u.shouldRunPreflight(ctx, user) {
//	    // 调用 store.PreflightConflicts 做数据库预检
//	}
//
// 注意事项：
//   - 当缓存尚未预热或 Redis 客户端不可用时，会主动要求执行预检
//   - 进入 Redis 降级模式后会跳过预检，避免在缓存异常时进一步放大数据库负载
//   - 强一致性请求（如删除、强制刷新）会始终执行预检
//
// forceCacheRefreshFromContext(ctx)强制刷新标记时会执行预检
// isStrongConsistencyRequest(ctx)强一致性请求时会执行预检
//
// 异常情况：
//   - 入参 user 为空时直接返回 false
func (u *UserService) shouldRunPreflight(ctx context.Context, user *v1.User) bool {
	if user == nil {
		return false
	}
	// Redis 全局降级模式时跳过预检
	/*
		Redis 进入降级态（isRedisDegradeActive=true）意味着我们已经判定缓存链路不可靠：占位写失败、操作超时或健康检查连续告警。此时如果仍按原逻辑对每个请求执行数据库预检（PreflightConflicts），“唯一性校验 + 限流”将在高并发下把读压全部落到主库，会立刻把数据库拖垮，形成“缓存雪崩 → 读风暴 → 主库故障”的连锁反应。 在降级模式里，系统会切换到以下策略来维持可用性与数据一致性： • 联系人数占位：调用 ensureContactPlaceholder 把当前请求写成本地降级缓存，减少重复写库。 • 本地降级缓存：contactRedisDegradeCache 维护最近的联系方式命中，避免每次都查数据库。 • 必要时的兜底查库：ensureContactUniqueDegraded 提供严格一致性路径，只在确认需要的时候才访问数据库。 • 限流保护：   preflightLimiter 仍然生效，防止数据库被历史积压请求冲垮。  因此，降级状态下跳过预检并不是因为“数据库也不可用”，而是为了避免在缓存异常期间再额外制造数据库高峰，转而依赖降级校验逻辑（本地缓存 + 限流 + 必要的兜底查库）来保证唯一性。待 Redis 恢复、降级标记解除后，预检路径会自动恢复正常工作。

	*/
	if u.isRedisDegradeActive() {
		return false
	}
	// 强制刷新或强一致性请求时执行预检
	if forceCacheRefreshFromContext(ctx) || isStrongConsistencyRequest(ctx) {
		return true
	}
	// 用户信息完全为空时跳过预检
	if strings.TrimSpace(user.Name) == "" && user.Email == "" && user.Phone == "" {
		return false
	}
	// 缓存未预热时强制执行预检
	if u.Redis == nil || !u.contactCacheReady.Load() {
		return true
	}
	return false
}

func (u *UserService) newDBContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	base := parent
	if base == nil {
		base = context.Background()
	}
	if parent != nil {
		if reqID := parent.Value("requestID"); reqID != nil {
			base = context.WithValue(base, "requestID", reqID)
		}
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	if deadline, ok := base.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(base, timeout)
}

func shouldDegradeForError(err error) bool {
	if err == nil {
		return false
	}
	// 1. 可重试/临时性错误（包含 Redis 网络抖动、超时等）一律视为可降级，
	//    通过 isRetryableError 统一判定，避免因为短暂抖动直接打回整条创建链路。
	if isRetryableError(err) {
		return true
	}
	// 2. 兜底：显式标记为数据库超时的错误也视为可降级（兼容历史行为，
	//    防止某些错误码未被 isRetryableError 捕获）。
	if errors.IsCode(err, code.ErrDatabaseTimeout) {
		return true
	}
	return false
}

func (u *UserService) isRedisDegradeActive() bool {
	return u != nil && u.contactRedisDegradeActive.Load()
}

func (u *UserService) enableRedisDegrade(reason string, kv ...interface{}) {
	if u == nil {
		return
	}
	if u.contactRedisDegradeActive.CompareAndSwap(false, true) {
		u.contactRedisDegradeSince.Store(time.Now().Unix())
		fields := []interface{}{"reason", reason}
		if len(kv) > 0 {
			fields = append(fields, kv...)
		}
		log.Warnw("联系人唯一性进入Redis降级模式", fields...)
	}
}

// disableRedisDegrade 关闭联系人唯一性 Redis 降级模式
//
// 当 Redis 健康检查恢复或手动干预时调用，重置降级标记并清理本地缓存。
//
// 参数：
//
//	reason: 关闭降级的原因描述
//
// 返回值：
//
//	无: 无返回值
func (u *UserService) disableRedisDegrade(reason string) {
	if u == nil {
		return
	}
	// 原本未处于降级状态则直接返回
	//确保全局只有一次机会将 contactRedisDegradeActive 从 true 改为 false，后续其他协程执行到这里时，因为值已经是 false，会直接 return，避免重复操作。
	if !u.contactRedisDegradeActive.CompareAndSwap(true, false) {
		return
	}
	// 重置降级开始时间
	u.contactRedisDegradeSince.Store(0)
	// 清理本地降级缓存
	u.clearContactDegradeCache()
	log.Infow("联系人唯一性Redis降级结束", "reason", reason)
}

func (u *UserService) contactDegradeCacheGet(key string) (string, bool) {
	if u == nil || strings.TrimSpace(key) == "" {
		return "", false
	}
	value, ok := u.contactRedisDegradeCache.Load(key)
	if !ok {
		return "", false
	}
	entry, castOK := value.(contactDegradeCacheEntry)
	if !castOK {
		u.contactDegradeCacheDelete(key)
		return "", false
	}
	if entry.expires > 0 && time.Now().UnixNano() > entry.expires {
		u.contactDegradeCacheDelete(key)
		return "", false
	}
	return entry.owner, true
}

func (u *UserService) contactDegradeCacheSet(key, owner string) {
	if u == nil || strings.TrimSpace(key) == "" {
		return
	}
	ttl := u.contactRedisDegradeTTL
	if ttl <= 0 {
		return
	}
	now := time.Now()
	nowUnix := now.UnixNano()
	value, existed := u.contactRedisDegradeCache.Load(key)
	if existed {
		if entry, ok := value.(contactDegradeCacheEntry); ok {
			if entry.expires > 0 && nowUnix > entry.expires {
				u.contactDegradeCacheDelete(key)
				existed = false
			}
		} else {
			u.contactDegradeCacheDelete(key)
			existed = false
		}
	}
	if !existed {
		if !u.ensureContactDegradeCacheCapacity() {
			return
		}
	}
	u.contactRedisDegradeCache.Store(key, contactDegradeCacheEntry{
		owner:   owner,
		expires: now.Add(ttl).UnixNano(),
	})
	if !existed {
		u.contactRedisDegradeCacheSize.Add(1)
	}
}

func (u *UserService) cleanupContactDegradeCache() {
	if u == nil {
		return
	}
	now := time.Now().UnixNano()
	u.contactRedisDegradeCache.Range(func(key, value interface{}) bool {
		entry, ok := value.(contactDegradeCacheEntry)
		if !ok || (entry.expires > 0 && now > entry.expires) {
			u.contactDegradeCacheDelete(key)
		}
		return true
	})
}

func (u *UserService) clearContactDegradeCache() {
	if u == nil {
		return
	}
	u.contactRedisDegradeCache.Range(func(key, _ interface{}) bool {
		u.contactDegradeCacheDelete(key)
		return true
	})
	u.contactRedisDegradeCacheSize.Store(0)
}

// contactDegradeCacheDelete 从本地降级缓存中删除指定键
func (u *UserService) contactDegradeCacheDelete(key interface{}) bool {
	if u == nil || key == nil {
		return false
	}
	if _, ok := u.contactRedisDegradeCache.LoadAndDelete(key); ok {
		u.decrementContactDegradeCacheSize()
		return true
	}
	return false
}

func (u *UserService) decrementContactDegradeCacheSize() {
	if u == nil {
		return
	}
	for {
		current := u.contactRedisDegradeCacheSize.Load()
		if current <= 0 {
			return
		}
		if u.contactRedisDegradeCacheSize.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (u *UserService) ensureContactDegradeCacheCapacity() bool {
	if u == nil {
		return false
	}
	limit := u.contactRedisDegradeCacheLimit
	if limit <= 0 {
		return true
	}
	if u.contactRedisDegradeCacheSize.Load() < limit {
		return true
	}
	u.cleanupContactDegradeCache()
	if u.contactRedisDegradeCacheSize.Load() < limit {
		return true
	}
	evicted := false
	u.contactRedisDegradeCache.Range(func(key, _ interface{}) bool {
		if u.contactDegradeCacheDelete(key) {
			evicted = true
			return false
		}
		return true
	})
	if u.contactRedisDegradeCacheSize.Load() < limit {
		return true
	}
	size := u.contactRedisDegradeCacheSize.Load()
	if !evicted {
		log.Warnw("联系人唯一性降级缓存达到容量上限，忽略新条目", "limit", limit, "size", size)
		return false
	}
	log.Warnw("联系人唯一性降级缓存逐出后仍超出容量，忽略新条目", "limit", limit, "size", size)
	return false
}

func (u *UserService) checkRedisHealthy() bool {
	if u == nil || u.Redis == nil {
		return false
	}
	timeout := 3 * time.Second
	if u.Options != nil && u.Options.RedisOptions != nil && u.Options.RedisOptions.Timeout > 0 {
		timeout = u.Options.RedisOptions.Timeout
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := u.Redis.GetKey(ctx, "__contact_health_ping__")
	return err == nil || stdErrors.Is(err, redis.Nil)
}

// startContactDegradeMonitor 启动周期性的 Redis 健康检查任务。
//
// 通过定时探测 Redis 可用性，动态管理联系人唯一性缓存的降级状态。
// 当 Redis 恢复后会自动清理降级缓存并解除降级标记。
func (u *UserService) startContactDegradeMonitor() {
	if u == nil {
		return
	}
	u.contactRedisMonitorOnce.Do(func() {
		interval := u.contactRedisHealthCheckInterval
		if interval <= 0 {
			interval = 10 * time.Second
		}
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				if u.contactRedisDegradeActive.Load() {
					if u.checkRedisHealthy() {
						u.disableRedisDegrade("redis_recovered")
					} else {
						u.cleanupContactDegradeCache()
					}
				} else {
					u.cleanupContactDegradeCache()
				}
			}
		}()
	})
}

// markCreateDegraded 记录用户创建降级状态并输出日志
func (u *UserService) markCreateDegraded(ctx context.Context, reason string, kv ...interface{}) {
	metrics.RecordUserCreateDegrade(reason, userctx.AccountType(ctx))

	//MarkCreateDegraded 切换降级标记；当从“非降级”转为“降级”时返回 true
	if userctx.MarkCreateDegraded(ctx) {
		//首次进入降级模式：记录追踪标记和警告日志
		trace.AddRequestTag(ctx, "create_degraded", true)
		if reason != "" {
			trace.AddRequestTag(ctx, "create_degraded_reason", reason)
		}
		fields := []interface{}{"reason", reason}
		if len(kv) > 0 {
			fields = append(fields, kv...)
		}
		log.Warnw("用户创建进入降级模式", fields...)
	}
	// 非首次进入降级模式：只更新降级原因（如果有新的原因）
	if reason != "" {
		trace.AddRequestTag(ctx, "create_degraded_reason", reason)
	}
	//如果是redis错误把全局降级标志拉起，让后续请求改走本地缓存兜底，直到监控线程发现 Redis 恢复
	if reason == redisDegradeReasonCache || reason == redisDegradeReasonPlaceholder {
		u.enableRedisDegrade(reason, kv...)
	}
}

func contactFieldFromCacheKey(cacheKey string) string {
	if strings.Contains(cacheKey, ":email:") {
		return "email"
	}
	if strings.Contains(cacheKey, ":phone:") {
		return "phone"
	}
	return "username"
}

// ensureContactPlaceholder 确保联系方式唯一性占位符存在于 Redis
// 通过 SetNX 操作尝试写入占位符，若已存在则读取现有值并根据需要刷新过期时间。
// 适用于用户创建等入口在降级模式下写入联系方式占位符，防止重复创建。//
// 参数：
//
//	ctx: 调用上下文，携带 trace、deadline 等信息
//	cacheKey: 联系方式对应的 Redis 键
//	owner: 占位符所有者标识，通常为用户名
//
// 返回值：
//
//	无: 此函数无返回值
func (u *UserService) ensureContactPlaceholder(ctx context.Context, cacheKey, owner string) {
	if u.Redis == nil || cacheKey == "" {
		return
	}
	fieldKey := contactFieldFromCacheKey(cacheKey)
	//placeholder 是我们即将写入 Redis 的 占位值，
	//用来表示“当前这个字段暂时由谁占着坑”。正常情况下传入的
	// owner 就是发起请求的用户（也就是    allowedOwner），
	// 于是占位写的是用户名。
	//如果 owner 为空串（例如调用方没带用户名、降级兜底时想写哨兵值），
	// 我们 fallback 到 RATE_LIMIT_PREVENTION，避免把空字符串写进 Redis。
	//allowedOwner 是业务侧允许占用该字段的用户，用来判断缓存命中时能不能直接放行； 
	// cachedOwner（或 cacheOwner）则是从 Redis 读到的实际值。
	//换句话说：   placeholder/   owner 表示“实际写入占位的值”，
	//    allowedOwner 表示“允许占这个坑的用户”， 
	// cachedOwner 是“缓存里目前记录的占用者”。多数场景下三者一样，
	// 但在降级或哨兵占位时，
	//   placeholder 会是 sentinel，而    allowedOwner 仍然保留真实用户名。
	placeholder := owner
	if strings.TrimSpace(placeholder) == "" {
		placeholder = RATE_LIMIT_PREVENTION
	}
	setCtx, setCancel := u.redisOpContext(ctx)
	setStart := time.Now()
	//30秒TTL就是用来做“短期幂等保护”，防止并发写穿
	ok, err := u.Redis.SetNX(setCtx, cacheKey, placeholder, contactPlaceholderTTL)
	setDuration := time.Since(setStart)
	setCancel()
	metrics.ObserveUserContactPlaceholderSet("redis_placeholder_setnx", fieldKey, setDuration, err)
	resultLabel := "set"
	u.recordUserCreateStep(ctx, "redis_placeholder_setnx", fieldKey, owner, setDuration, err)
	if err != nil {
		log.Warnw("唯一性灰度占位失败", "key", cacheKey, "error", err)
		resultLabel = "error"
		metrics.RecordUserContactPlaceholderEvent("redis_placeholder_setnx", fieldKey, resultLabel)
		return
	}
	if ok {
		metrics.RecordUserContactPlaceholderEvent("redis_placeholder_setnx", fieldKey, resultLabel)
		return
	}
	//键已存在，读取旧值
	getCtx, getCancel := u.redisOpContext(ctx)
	getStart := time.Now()
	existing, err := u.Redis.GetKey(getCtx, cacheKey)
	getDuration := time.Since(getStart)
	getCancel()
	getErr := err
	if errors.Is(err, redis.Nil) {
		getErr = nil
	}
	u.recordUserCreateStep(ctx, "redis_placeholder_get", fieldKey, owner, getDuration, getErr)
	if err != nil {
		if err != redis.Nil {
			log.Warnw("唯一性灰度占位读取失败", "key", cacheKey, "error", err)
		}
		metrics.RecordUserContactPlaceholderEvent("redis_placeholder_get", fieldKey, "error")
		return
	}
	//ok == false：表示键早就存在（可能是同一用户的幂等请求，
	// 或者已经有其它占位/哨兵值）。这时才会执行后面的 GetKey，
	// 看旧值是谁；如果旧值和我们当前的 placeholder（或特殊哨兵）一致，
	// 就调用 SetKey 延长 TTL。若旧值不同，则保持原状，不去覆盖。
	if strings.EqualFold(existing, placeholder) || existing == "" || existing == RATE_LIMIT_PREVENTION {
		refreshCtx, refreshCancel := u.redisOpContext(ctx)
		refreshStart := time.Now()
		setErr := u.Redis.SetKey(refreshCtx, cacheKey, placeholder, contactPlaceholderTTL)
		refreshDuration := time.Since(refreshStart)
		u.recordUserCreateStep(ctx, "redis_placeholder_refresh", fieldKey, owner, refreshDuration, setErr)
		if setErr != nil {
			log.Warnw("唯一性灰度占位ttl刷新失败", "key", cacheKey, "error", setErr)
		}
		refreshCancel()
	}
	metrics.RecordUserContactPlaceholderEvent("redis_placeholder_get", fieldKey, "hit")
}

//ensureDegradedContactPlaceholders 确保用户的邮箱和手机号占位符存在于 Redis

func (u *UserService) ensureDegradedContactPlaceholders(ctx context.Context, username, email, phone string) {
	if email != "" {
		emailKey := u.generateEmailCacheKey(email)
		u.ensureContactPlaceholder(ctx, emailKey, username)
		metrics.RecordUserContactPlaceholderEvent("degraded_placeholder", "email", "degraded")
	}
	if phone != "" {
		phoneKey := u.generatePhoneCacheKey(phone)
		u.ensureContactPlaceholder(ctx, phoneKey, username)
		metrics.RecordUserContactPlaceholderEvent("degraded_placeholder", "phone", "degraded")
	}
}

// ensureContactUniqueness 校验用户的邮箱与手机号在全局范围内唯一
//
// 结合数据库预检、Redis 占位和本地降级标记，确保在创建或更新场景下不会写入重复的联系方式，并返回预检命中的冲突用户。
// 适用于用户创建、资料修改等需要严格联系方式唯一性的流程。
//
// 参数：
//
//	ctx: 调用上下文，携带 trace、deadline 等信息
//	user: 待检查的用户实体，需提前执行 Normalize 以确保键一致
//
// 返回值：
//
//	map[string]*v1.User: 预检冲突列表，键为 "email"/"phone"/"username" 等，值为冲突用户
//	bool: 是否已在预检阶段确认用户名占用
//	error: 校验过程中出现的错误，nil 表示唯一性通过
//
// 示例：
//
//	conflicts, preflighted, err := u.ensureContactUniqueness(ctx, user)
//	if err != nil {
//	    // 处理唯一性冲突或外部错误
//	}
//
// 注意事项：
//   - 当 Redis 或数据库超时时会尝试降级，必要时写入占位以降低后续风险
//   - 调用方应根据返回的冲突列表决定是否继续后续流程
//
// 异常情况：
//   - 数据库不可用时可能返回 ErrDatabase、ErrDatabaseTimeout 等错误码
//   - 数据不一致时会返回 ErrValidation 指示具体占用字段
func (u *UserService) ensureContactUniqueness(ctx context.Context, user *v1.User) (map[string]*v1.User, bool, error) {
	spanCtx, span := trace.StartSpan(ctx, "user-service", "ensure_contact_uniqueness")
	if spanCtx != nil {
		ctx = spanCtx
	}
	status := "success"
	codeStr := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]interface{}{}
	defer func() {
		trace.EndSpan(span, status, codeStr, spanDetails)
	}()
	//对数据库预检加限流保护，防止高并发时打垮数据库
	limiter := u.preflightLimiter
	if limiter != nil {
		limiterCtx, limiterSpan := trace.StartSpan(ctx, "user-service", "preflight_limiter_wait")
		if limiterCtx != nil {
			ctx = limiterCtx
		}
		limiterStatus := "success"
		limiterCode := strconv.Itoa(code.ErrSuccess)
		limiterDetails := map[string]interface{}{"permit_requested": 1}
		waitStart := time.Now()
		err := limiter.Acquire(ctx, 1)
		waitDuration := time.Since(waitStart)
		limiterDetails["wait_duration_ms"] = waitDuration.Milliseconds()
		limiterDetails["permit_acquired"] = err == nil
		trace.AddRequestTag(ctx, "preflight_limiter_wait_ms", waitDuration.Milliseconds())
		u.recordUserCreateStep(ctx, "preflight_limiter_wait", "limiter", user.Name, waitDuration, err)
		if err != nil {
			limiterStatus = "error"
			limiterCode = err.Error()
			trace.EndSpan(limiterSpan, limiterStatus, limiterCode, limiterDetails)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				status = "error"
				codeStr = strconv.Itoa(code.ErrDatabaseTimeout)
				return nil, false, errors.WithCode(code.ErrDatabaseTimeout, "预检-数据库限流查询等待超时")
			}
			status = "error"
			codeStr = strconv.Itoa(code.ErrDatabase)
			return nil, false, errors.WithCode(code.ErrDatabase, "预检查询等待失败: %v", err)
		}
		defer func() {
			releaseStart := time.Now()
			limiter.Release(1)
			releaseDuration := time.Since(releaseStart)
			limiterDetails["release_duration_ms"] = releaseDuration.Milliseconds()
			u.recordUserCreateStep(ctx, "preflight_limiter_release", "limiter", user.Name, releaseDuration, nil)
			trace.EndSpan(limiterSpan, limiterStatus, limiterCode, limiterDetails)
		}()
	}
	//确保联系方式缓存已预热
	u.ensureContactCacheReady(ctx)
	spanDetails["contact_cache_ready"] = u.contactCacheReady.Load()
	//规范化用户联系方式
	u.normalizeUserContacts(user)

	email := user.Email
	phone := user.Phone
	//
	store := u.userStoreReadOnly()
	if store == nil {
		status = "error"
		codeStr = strconv.Itoa(code.ErrDatabase)
		return nil, false, errors.WithCode(code.ErrDatabase, "用户存储未就绪")
	}

	var (
		preflight       map[string]*v1.User                 //预检冲突结果
		preflightErr    error                               //		预检错误
		retryAttempts   = u.Options.RedisOptions.MaxRetries //重试次数
		usernameChecked bool                                // 标记用户名是否经过有效检查
		ranPreflight    bool                                // 标记是否执行过预检
	)

	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	//判断当前请求是否需要执行数据库预检查
	//需要预检的条件：强一致性请求、user,redis为nil、缓存未预热等
	//不需要预检的条件：降级模式、用户字段全为空、缓存已预热等
	//bool: true 代表执行预检，false 代表不执行
	runPreflight := u.shouldRunPreflight(ctx, user)
	//执行预检逻辑
	spanDetails["run_preflight"] = runPreflight
	if runPreflight && (strings.TrimSpace(user.Name) != "" || email != "" || phone != "") {
		pfCtx, pfSpan := trace.StartSpan(ctx, "user-service", "preflight_conflicts")
		if pfCtx != nil {
			ctx = pfCtx
		}
		pfStatus := "success"
		pfCode := strconv.Itoa(code.ErrSuccess)
		pfDetails := map[string]interface{}{"preflight_executed": true}
		attemptCount := 0
		var lastDBDuration time.Duration
		result, err := util.RetryWithBackoff(retryAttempts, isRetryableError, func() (interface{}, error) {
			attemptCount++
			dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
			defer cancel()
			ranPreflight = true
			dbStart := time.Now()
			// 执行数据库预检
			dbSpanCtx, dbSpan := trace.StartSpan(dbCtx, "mysql", "preflight_db_query")
			if dbSpanCtx != nil {
				dbCtx = dbSpanCtx
			}
			conflicts, confErr := store.PreflightConflicts(dbCtx, user.Name, email, phone, u.Options)
			lastDBDuration = time.Since(dbStart)
			u.recordUserCreateStep(ctx, "preflight_query", "database", user.Name, lastDBDuration, confErr)
			if dbSpan != nil {
				status := "success"
				codeStr := strconv.Itoa(code.ErrSuccess)
				if confErr != nil {
					status = "error"
					if c := errors.GetCode(confErr); c != 0 {
						codeStr = strconv.Itoa(c)
					} else {
						codeStr = confErr.Error()
					}
				}
				dbDetails := map[string]interface{}{
					"username": user.Name,
					"email":    email,
					"phone":    phone,
					"attempt":  attemptCount,
					"db_ms":    lastDBDuration.Milliseconds(),
				}
				if confErr != nil {
					dbDetails["error"] = confErr.Error()
				}
				trace.EndSpan(dbSpan, status, codeStr, dbDetails)
			}
			return conflicts, confErr
		})
		//处理预检结果
		if err != nil {
			preflightErr = err
			pfStatus = "error"
			pfCode = err.Error()
		} else if result != nil {
			//类型转换：将结果转为冲突用户的map（key为scope：username/email/phone）
			if typed, ok := result.(map[string]*v1.User); ok {
				preflight = typed
			}
		}
		if pfSpan != nil {
			pfDetails["attempts"] = attemptCount
			pfDetails["database_query_ms"] = lastDBDuration.Milliseconds()
			pfDetails["conflict_found"] = len(preflight) > 0
			if preflightErr != nil {
				pfDetails["preflight_error"] = preflightErr.Error()
			}
			trace.EndSpan(pfSpan, pfStatus, pfCode, pfDetails)
		}
	} else if !runPreflight {
		// 记录跳过预检的情况--实际执行时，由于传入 0 耗时，并不会产生 “慢操作日志”，只是走了一个统一的流程入口。
		u.recordUserCreateStep(ctx, "preflight_query_skip", "database", user.Name, 0, nil)
	}
	// 标记用户名是否经过有效检查
	if ranPreflight && strings.TrimSpace(user.Name) != "" && preflightErr == nil {
		usernameChecked = true
	}
	// 处理预检错误：根据配置决定是否降级
	/*
				如果数据库预检（preflight）失败，并且错误属于“可降级”类型（如超时、Redis 故障等），就会：
				标记本次创建为降级（markCreateDegraded），用于后续链路和监控。
				调用 ensureDegradedContactPlaceholders，给 username/email/phone 写入“降级占位符”到 Redis。
				将 preflightErr 置为 nil，usernameChecked 置为 false，流程继续往下走（即“放行”）。
				你的疑问。
				“此时写入的是非哨兵模式的占位符，本身预检失败了，写入占位符有什么意义？”
				“下次请求不是拿不到真实的值？”
				解答：

				写入降级占位符的意义：

				目的是“兜底保护并发唯一性”：即使预检失败（比如数据库超时），也要在 Redis 里占个位，防止同一邮箱/手机号/用户名被多个请求并发写入，造成唯一性冲突。
				这种占位符不是“哨兵”模式（即不是严格的唯一性确认），而是“临时锁位”，让后续请求看到有占位符就不再并发写入，等 Redis 恢复或预检恢复后再做真正的唯一性校验。
				这样做的好处是：即使后端存储不可靠，仍然能用 Redis 作为“并发写保护”，避免雪崩式写入。
		下次请求会发生什么？

		下次请求如果命中降级占位符，流程会优先判断是否处于降级状态（isRedisDegradeActive），如果是，依然会走降级兜底分支（比如直接查库或本地缓存），不会直接信任 Redis 的唯一性。
		等 Redis/数据库恢复后，后台会清理降级占位符，恢复正常的唯一性校验流程。
		期间如果有并发写入，依然有 Redis 占位符兜底，防止同一用户被多次创建。
		总结：

		这一步的本质是“牺牲强一致性，优先保证高可用和幂等性”，即使后端不可靠，也要用 Redis 做兜底保护，防止并发写穿。
		占位符不是最终的唯一性判据，只是临时锁位，等后端恢复后再做最终一致性校验。

	*/
	if runPreflight && preflightErr != nil {

		if shouldDegradeForError(preflightErr) {
			u.markCreateDegraded(ctx, "preflight_timeout", "username", user.Name)
			u.ensureDegradedContactPlaceholders(ctx, user.Name, email, phone)
			metrics.RecordUserContactPlaceholderEvent("preflight", "all", "degraded")
			preflightErr = nil
			usernameChecked = false
		} else {
			status = "error"
			if c := errors.GetCode(preflightErr); c != 0 {
				codeStr = strconv.Itoa(c)
			}
			return nil, false, preflightErr
		}
	}
	if preflight == nil {
		preflight = make(map[string]*v1.User)
	}
	trace.AddRequestTag(ctx, "preflight_executed", ranPreflight)

	type contactCheck struct {
		cacheKey string
		label    string
		value    string
		fieldKey string
		lookup   func(context.Context, interfaces.UserStore, string) (*v1.User, error)
	}

	checks := make([]contactCheck, 0, 2)

	if email != "" {
		emailCopy := email
		checks = append(checks, contactCheck{
			cacheKey: u.generateEmailCacheKey(emailCopy),
			label:    "邮箱",
			value:    emailCopy,
			fieldKey: "email",
			//这里的逻辑其实是“如果预检已经覆盖了邮箱，就不再打库；只有预检没跑或没覆盖到时才查库”。
			lookup: func(lookupCtx context.Context, fieldStore interfaces.UserStore, value string) (*v1.User, error) {
				if err := lookupCtx.Err(); err != nil {
					return nil, err
				}
				if fieldStore == nil {
					fieldStore = store
				}
				//找预检结果里有没有邮箱冲突
				if existing := preflight["email"]; existing != nil {
					return existing, nil
				}
				//ErrUserNotFound 只是用来告诉通用检查器“这条路已经验证过，没有冲突”，不会阻止后续逻辑。
				if runPreflight {
					return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
				}
				//预检没命中邮箱，才查库
				return fieldStore.GetByEmail(lookupCtx, value, u.Options)
			},
		})
	}

	if phone != "" {
		phoneCopy := phone
		checks = append(checks, contactCheck{
			cacheKey: u.generatePhoneCacheKey(phoneCopy),
			label:    "手机号",
			value:    phoneCopy,
			fieldKey: "phone",
			lookup: func(lookupCtx context.Context, fieldStore interfaces.UserStore, value string) (*v1.User, error) {
				if err := lookupCtx.Err(); err != nil {
					return nil, err
				}
				if fieldStore == nil {
					fieldStore = store
				}
				if existing := preflight["phone"]; existing != nil {
					return existing, nil
				}
				if runPreflight {
					return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
				}
				return fieldStore.GetByPhone(lookupCtx, value, u.Options)
			},
		})
	}

	runCheck := func(runCtx context.Context, spec contactCheck) error {
		return u.ensureContactUnique(
			runCtx,
			store,
			spec.cacheKey,
			user.Name,
			spec.label,
			spec.value,
			spec.fieldKey,
			spec.lookup,
		)
	}

	switch len(checks) {
	case 0:
		spanDetails["preflight_executed"] = ranPreflight
		spanDetails["username_checked"] = usernameChecked
		return preflight, usernameChecked, nil
	case 1:
		if err := runCheck(ctx, checks[0]); err != nil {
			status = "error"
			if c := errors.GetCode(err); c != 0 {
				codeStr = strconv.Itoa(c)
			}
			return nil, false, err
		}
	default:
		group, groupCtx := errgroup.WithContext(ctx)
		for _, spec := range checks {
			spec := spec
			group.Go(func() error {
				return runCheck(groupCtx, spec)
			})
		}
		if err := group.Wait(); err != nil {
			status = "error"
			if c := errors.GetCode(err); c != 0 {
				codeStr = strconv.Itoa(c)
			}
			return nil, false, err
		}
	}

	return preflight, usernameChecked, nil
}

// ensureContactUnique 利用泛型唯一性检查器验证单个联系方式的占用情况。
// 该函数将用户服务的缓存、降级与错误码策略注入通用检查器，避免重复实现表字段的唯一性校验。
//
// param ctx: 请求上下文，建议包含 trace 与超时控制，不可为nil。
// param store: 只读用户存储接口，需支持邮箱或手机号等字段查重。
// param cacheKey: 联系方式对应的缓存键，不能为空。
// param allowedOwner: 当前允许占用该字段的用户名，可为空字符串。
// param fieldLabel: 字段中文名，用于提示信息。
// param fieldValue: 待校验的字段值，需提前归一化。
// param fieldKey: 字段标识，用于链路指标记录。
// param lookup: 查重函数，接收上下文、存储与字段值，返回命中用户或 ErrUserNotFound。
//
// returns: 唯一性通过时返回nil；冲突时返回携带 ErrValidation 的错误；当底层依赖不可用且无法降级时返回对应错误码。
//
// note: 函数在降级模式下会自动写入占位符，并依赖数据库唯一索引兜底。
func (u *UserService) ensureContactUnique(
	ctx context.Context,
	store interfaces.UserStore,
	cacheKey string,
	allowedOwner string,
	fieldLabel string,
	fieldValue string,
	fieldKey string,
	lookup func(context.Context, interfaces.UserStore, string) (*v1.User, error),
) error {
	if strings.TrimSpace(cacheKey) == "" {
		return nil
	}
	if store == nil {
		return errors.WithCode(code.ErrDatabase, "用户存储未就绪")
	}
	//redis不可用(降级模式下)使用本地缓存避免频繁访问 Redis
	if u.isRedisDegradeActive() {
		return u.ensureContactUniqueDegraded(ctx, store, cacheKey, allowedOwner, fieldLabel, fieldValue, fieldKey, lookup)
	}

	maxRetries := 1
	if u.Options != nil && u.Options.RedisOptions != nil && u.Options.RedisOptions.MaxRetries > 0 {
		maxRetries = u.Options.RedisOptions.MaxRetries
	}

	checker := unique.NewChecker[interfaces.UserStore, *v1.User](unique.CheckerConfig[interfaces.UserStore, *v1.User]{
		Store:               store,                 //执行查重的存储实现，通常为具体的 DAO 或仓储实例，必填。
		Cache:               u.Redis,               //用于占位与命中加速；若为空则退化为纯数据库校验。
		PlaceholderTTL:      contactPlaceholderTTL, //	占位符在缓存中的存活时间。
		CacheTTL:            contactCacheTTL,       //	缓存命中时的存活时间。
		PlaceholderFallback: RATE_LIMIT_PREVENTION, //	当 AllowedOwner 与 PlaceholderValue 为空时使用的默认占位符。
		CacheReady: func() bool {
			return u.contactCacheReady.Load()
		}, //返回缓存是否已预热完成，控制是否跳过数据库兜底。
		DegradeActive: userctx.IsCreateDegraded, //检查当前请求是否处于降级模式。
		//写入占位符
		EnsurePlaceholder: func(innerCtx context.Context, key string, owner string) {
			u.ensureContactPlaceholder(innerCtx, key, owner)
		}, //当需要写入占位符时的回调，通常用于降级兜底。
		MarkDegraded: func(innerCtx context.Context, reason string, kv ...interface{}) {
			u.markCreateDegraded(innerCtx, reason, kv...)
		}, //标记当前请求进入降级模式，需幂等处理。
		Retry:          util.RetryWithBackoff, //通用重试函数。
		RetryPredicate: isRetryableError,      //判断是否需要重试的函数。
		MaxRetries:     maxRetries,            //最大重试次数。
		ShouldDegrade:  shouldDegradeForError, //判断是否需要降级的函数。
		ShouldReleasePlaceholder: func(checkErr error) bool {
			return !errors.IsCode(checkErr, code.ErrValidation)
		}, //	判断在出现错误时是否应释放占位符。
		NewLookupContext: u.newDBContext,           //创建用于查重的数据库上下文的函数。
		LookupTimeout:    u.contactLookupTimeout(), // 单次查库的超时时间，小于等于0表示不限。
		IsNotFound: func(err error) bool {
			return errors.IsCode(err, code.ErrUserNotFound)
		}, // 判断错误是否为“未找到”，用于终止重试。
		IsCacheMiss: func(err error) bool {
			return errors.Is(err, redis.Nil)
		}, // 判断缓存访问是否未命中，未提供时默认所有错误均视为异常。(键不存在的时候为命中)
		RecordStep: u.recordUserCreateStep,
		Logger: unique.LoggerHooks{
			Warn: func(msg string, kv ...interface{}) {
				log.Warnw(msg, kv...)
			}, //记录性能指标的回调，用于链路观测。
			Error: func(msg string, kv ...interface{}) {
				log.Errorw(msg, kv...)
			}, // 日志钩子，用于输出缓存或占位异常。
		},
	})

	fieldCfg := unique.FieldConfig[interfaces.UserStore, *v1.User]{
		FieldLabel:       fieldLabel,   //错误提示中显示的字段中文名（如 “手机号已被占用”）
		FieldKey:         fieldKey,     //指标记录中使用的字段标识（如 "phone"）
		FieldValue:       fieldValue,   //待校验的字段值，需提前归一化。
		AllowedOwner:     allowedOwner, //当前允许占用该字段的用户名，可为空字符串。
		CacheKey:         cacheKey,     //对应的缓存键。
		PlaceholderValue: allowedOwner, //占位符值，通常为当前用户名或特殊标记。
		Lookup: func(lookupCtx context.Context, fieldStore interfaces.UserStore, value string) (*v1.User, error) {
			return lookup(lookupCtx, fieldStore, value)
		}, //查库函数，需返回持有该字段的实体，支持对 Store 的多态实现。
		ExtractOwner: func(entity *v1.User) string {
			if entity == nil {
				return ""
			}
			return entity.Name
		}, //从实体中提取唯一标识的回调，返回空字符串视为无冲突。
		ConflictError: func(label, value string) error {
			return errors.WithCode(code.ErrValidation, "%s已被占用: %s", label, value)
		},
		IsAllowedOwner: func(existingOwner, owner string) bool {
			return strings.EqualFold(existingOwner, owner)
		}, //构造冲突错误的回调，由业务方决定错误码与提示。
		SkipPlaceholderLookup: func(owner string) bool {
			return owner != "" && !strings.EqualFold(owner, RATE_LIMIT_PREVENTION)
		}, //当占位成功且缓存已预热时，决定是否跳过数据库查重。
		DegradeReason: "contact_lookup_timeout",                                //触发降级时上报的原因标签，可为空。
		DegradeKV:     []interface{}{"field", fieldKey, "owner", allowedOwner}, //触发降级时额外记录的键值对，需偶数字段。
		StepName:      "ensure_contact_unique",                                 //性能采集使用的步骤名，默认 ensure_field_unique。
	}

	return checker.EnsureFieldUnique(ctx, fieldCfg)
}

func (u *UserService) ensureContactUniqueDegraded(
	ctx context.Context,
	store interfaces.UserStore,
	cacheKey string,
	allowedOwner string,
	fieldLabel string,
	fieldValue string,
	fieldKey string,
	lookup func(context.Context, interfaces.UserStore, string) (*v1.User, error),
) error {
	owner, ok := u.contactDegradeCacheGet(cacheKey)
	if ok {
		var cacheErr error
		if owner != "" && !strings.EqualFold(owner, allowedOwner) {
			cacheErr = errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
		}
		u.recordUserCreateStep(ctx, "ensure_contact_unique_degraded_cache", fieldKey, allowedOwner, 0, cacheErr)
		if cacheErr != nil {
			return cacheErr
		}
		return nil
	}

	dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
	defer cancel()
	start := time.Now()
	entity, err := lookup(dbCtx, store, fieldValue)
	duration := time.Since(start)
	recordErr := err
	if errors.IsCode(err, code.ErrUserNotFound) {
		recordErr = nil
	}
	u.recordUserCreateStep(ctx, "ensure_contact_unique_degraded_lookup", fieldKey, allowedOwner, duration, recordErr)

	if err != nil {
		if errors.IsCode(err, code.ErrUserNotFound) {
			u.contactDegradeCacheSet(cacheKey, allowedOwner)
			return nil
		}
		return err
	}

	if entity == nil {
		u.contactDegradeCacheSet(cacheKey, allowedOwner)
		return nil
	}

	existingOwner := strings.TrimSpace(entity.Name)
	if existingOwner == "" {
		u.contactDegradeCacheSet(cacheKey, allowedOwner)
		return nil
	}

	u.contactDegradeCacheSet(cacheKey, existingOwner)
	if strings.EqualFold(existingOwner, allowedOwner) {
		return nil
	}
	return errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
}

// warmContactCache 预热联系方式唯一性缓存
//
// 批量扫描用户存储中的所有用户记录，并将其邮箱与手机号写入 Redis 缓存，提升后续唯一性检查的命中率。
// 适用于系统启动后或大规模数据变更后执行，以加速后续的用户创建与更新操作。
//
// 返回值：
//
//	error: 预热过程中发生的错误，nil 表示预热成功
func (u *UserService) warmContactCache(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, contactWarmupTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	if cancel != nil {
		defer cancel()
	}

	if u.Store == nil || u.Redis == nil {
		return fmt.Errorf("warmContactCache dependencies not ready")
	}

	var (
		offset int64
		total  int64
	)

	batchSize := int64(contactWarmupBatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		off := offset
		limit := batchSize
		opts := metav1.ListOptions{
			Offset: &off,
			Limit:  &limit,
		}

		attempt := 0
		result, err := util.RetryWithBackoff(3, isRetryableError, func() (interface{}, error) {
			attempt++
			trace.AddRequestTag(ctx, "warmup_retry_attempt", attempt)
			listCtx, listSpan := trace.StartSpan(ctx, "mysql", "user_store_list_users_db")
			if listCtx != nil {
				ctx = listCtx
			}
			listStart := time.Now()
			users, listErr := u.Store.Users().List(ctx, opts, u.Options)
			if listSpan != nil {
				status := "success"
				codeStr := strconv.Itoa(code.ErrSuccess)
				if listErr != nil {
					status = "error"
					if c := errors.GetCode(listErr); c != 0 {
						codeStr = strconv.Itoa(c)
					} else {
						codeStr = strconv.Itoa(code.ErrUnknown)
					}
				}
				trace.EndSpan(listSpan, status, codeStr, map[string]interface{}{
					"offset":  off,
					"limit":   limit,
					"attempt": attempt,
					"db_ms":   time.Since(listStart).Milliseconds(),
				})
			}
			return users, listErr
		})
		if err != nil {
			return err
		}
		var list *v1.UserList
		if result != nil {
			if typed, ok := result.(*v1.UserList); ok {
				list = typed
			}
		}
		if list == nil || len(list.Items) == 0 {
			break
		}
		//以分页为单位的串行预热—— 取回一页、处理一页（写入 Redis）、再取下一页，直到遍历完所有数据或触发终止条件（超时 / 无数据 / 查询失败）
		for _, entry := range list.Items {
			if entry == nil {
				continue
			}
			email := usercache.NormalizeEmail(entry.Email)
			if email != "" {
				emailKey := u.generateEmailCacheKey(email)
				if emailKey != "" {
					if err := u.Redis.SetKey(ctx, emailKey, entry.Name, contactCacheTTL); err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						log.Warnf("预热邮箱唯一性缓存失败", "key", emailKey, "error", err)
					}
				}
			}

			phone := usercache.NormalizePhone(entry.Phone)
			if phone != "" {
				phoneKey := u.generatePhoneCacheKey(phone)
				if phoneKey != "" {
					if err := u.Redis.SetKey(ctx, phoneKey, entry.Name, contactCacheTTL); err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						log.Warnf("预热手机号唯一性缓存失败", "key", phoneKey, "error", err)
					}
				}
			}
		}

		count := int64(len(list.Items))
		total += count
		if count < batchSize {
			break
		}
		offset += count
	}

	return nil
}

// 从缓存和数据库查询用户是否存在
// 通用重试工具

// checkUserExist 根据用户名判断用户是否存在
//
// 优先查询批量预读缓存，其次命中 Redis 或数据库；支持强制跳过缓存并记录耗时与错误信息，用于用户创建、删除等场景的存在性确认。
//
// 参数：
//
//	ctx: 请求上下文，需携带 trace、取消信号等
//	username: 需要检查的用户名，大小写不敏感
//	forceRefresh: 是否强制绕过缓存直接走数据库查询
//
// 返回值：
//
//	*v1.User: 当用户存在时返回用户实体，否则返回 nil
//	error: 查询过程中发生的错误，nil 表示查询成功（即使用户不存在）
//
// 示例：
//
//	user, err := u.checkUserExist(ctx, "alice", false)
//	if err != nil {
//	    // 处理查询异常
//	}
//
// 注意事项：
//   - 当 forceRefresh 为 true 时会短路缓存，增加数据库压力
//   - 调用方需根据返回的 user 是否为空判断存在性
//
// 异常情况：
//   - Redis/数据库超时将返回相应错误码
//   - 当批量缓存不可用时会自动降级到单条查询
//   - 用户不存在时不会返回错误，而是返回 nil 用户实体
//     强刷（   forceRefresh 或    isStrongConsistencyRequest) 不会无条件“命中也查库”，而是根据命中的类型决定： ◦ 普通正缓存命中：先返回缓存结果，但同时会在后台发起 refreshUserCacheFromDB，用最新数据覆盖缓存（create/update 场景需要即时确认时很有用）。 ◦ 负缓存 / 黑名单命中：视为可疑，强刷请求会直接绕过 sentinel，立即回源主库确认；若查库成功就更新缓存，否则仍返回负缓存结论。 ◦ 缓存 miss：一定会查库，而且在强一致场景下会先 sleep 一小段（strongConsistencyProbeDelay) 再读主库，提升“刚写就读”的成功率。    所以“强刷”并不等同于“永远忽略缓存”，而是“命中正缓存→可用就直接用，但后台刷新；命中哨兵→绕过去查库；没命中→查库并带保护延迟”。- 调用方可通过上下文携带的标记控制缓存行为（如强一致性请求）.
//
// 当前实现里真正“触发写负缓存”的入口只有两个： 1. 查询 miss 触发保护阈值：checkUserExist/Get 走到 DB，返回    code.ErrUserNotFound 后会调用 handleProtectionForMiss。它给用户名的 NegativeCounterKey 加 1；当计数在窗口内到达 NegativeCacheThreshold（默认 5 次/60s，可配）时，才会调用 cacheNullValue 把    user:<username> 写成    RATE_LIMIT_PREVENTION，TTL 约 45s 带抖动。未达到阈值前不会写负缓存。 2. 删除流程主动兜底：cleanupUserStateForDelete 在清理正缓存后，总是调用 cacheNullValue(ctx, username, 0)，直接写入负缓存，确保刚删完的用户在 TTL 内命中 “不存在”。   •  其他提到的“普通 miss 就写”“SetNX 占位失败就写”等在当前代码没有实现；缓存 miss 只会统计    CacheHits.WithLabelValues("no_record") 并回源，除非进入上面两个分支。  •  这些入口都满足你说的“先删正缓存→短 TTL→只在确定不存在时写”，同时也避免了“查询出错/超时”误写：只有拿到明确的 ErrUserNotFound 或删除完成后才会写负缓存。主要是为了“区分偶发 miss 和真实不存在”。普通 miss 很可能只是缓存刚失效、DB 延迟、复制未完成等暂态，如果一 miss 就写负缓存，会把真实存在的数据标记成不存在，影响业务。 • 阈值机制把需要负缓存的对象限定在“短时间内被重复查库却始终不存在”的热点，比如机器人穷举用户名、压测脚本反复查未建用户。这类流量才对 DB 造成压力，适合用负缓存挡住。 • 由于你的系统会做自动重试、singleflight、强一致检查等，某些请求会在短时间内触发 2~3 次 miss（比如 DB 回源失败/重试），如果没有阈值，这些正常重试也会被误认为“真的不存在”而写入负缓存。阈值（默认 5 次/60 秒）就相当于一道“判别器”，让真正的恶性 miss 才触发负缓存，而把一次性的 miss/重试过滤掉。
func (u *UserService) checkUserExist(ctx context.Context, username string, forceRefresh bool) (*v1.User, error) {
	//按需启动批量缓. 先查批量预读缓存
	batchCache := batchLookupCacheFromContext(ctx)
	recordBatchResult := func(user *v1.User, notFound bool) {
		if batchCache != nil {
			batchCache.set(username, user, notFound)
		}
	}
	if entry, ok := batchCache.get(username); ok {
		metrics.CacheHits.WithLabelValues("batch_hit").Inc()
		if entry.notFound {
			return nil, nil
		}
		return entry.user, nil
	}
	//未命中批量缓存
	if batchCache != nil {
		metrics.CacheHits.WithLabelValues("batch_miss").Inc()
	}

	cacheSpanCtx, cacheSpan := trace.StartSpan(ctx, "user-service", "check_user_cache")
	if cacheSpanCtx != nil {
		ctx = cacheSpanCtx
	}
	cacheStatus := "success"
	cacheCode := strconv.Itoa(code.ErrSuccess)
	cacheDetails := map[string]any{
		"username":      username,
		"force_refresh": forceRefresh,
	}
	//验证用户已删除意图
	//verifyIntent  是通过    WithVerifyUserGone(ctx) 在上下文里打的一个标记，表示“当前这一趟 checkUserExist 是为了确认用户是否已经被删除/不存在”，主要服务于删除流程和幂等校验。它的作用有几方面： • Trace 可观测性：当    verifyIntent 为 true 时，缓存阶段会打 verify_user_gone_* 标签（命中、miss、负缓存等），数据库阶段也会记录 verify_user_gone_db_result，让我们在链路追踪里区分“普通存在性查询”与“删除后确认查询”，排查“删完还读到”的问题更容易。 • 语义区分：这个标记不会直接改变查询逻辑，但能让 checkUserExist 在处理结果时明确这是“验证是否消失”。例如命中负缓存、黑名单或 nil 时会把结果视为“已不存在”，并打相应 tag，供删除流程决定是否继续。 • 上游流程控制：删除和异步操作流水线在调用 checkUserExist 前都会调用    WithVerifyUserGone，这样就能统一收集“删除验证”这类调用的命中率、延迟，帮助我们判断删除后缓存/DB 刷新是否及时。  所以    verifyIntent 本质是一个“查询意图”标记，用于观测和决策，而不是决定是否查库。
	verifyIntent := verifyUserGoneFromContext(ctx)
	if verifyIntent {
		cacheDetails["verify_user_gone"] = true
		trace.AddRequestTag(ctx, "verify_user_gone_intent", "cache")
	}
	endCacheSpan := func() {
		if cacheSpan != nil {
			trace.EndSpan(cacheSpan, cacheStatus, cacheCode, cacheDetails)
			cacheSpan = nil
		}
	}
	defer endCacheSpan()

	baseCtx := ctx
	if forceRefresh {
		baseCtx = WithForceCacheRefresh(ctx)
		cacheDetails["forced_refresh_ctx"] = true
	}
	//尝试从缓存获取用户
	user, found, err := u.tryGetFromCache(baseCtx, username)
	cacheDetails["cache_found"] = found
	if err != nil {
		log.Errorf("缓存查询异常，继续流程", "error", err.Error(), "username", username)
		metrics.CacheErrors.WithLabelValues("query_failed", "get").Inc()
		cacheStatus = "error"
		cacheDetails["cache_error"] = err.Error()
		if c := errors.GetCode(err); c != 0 {
			cacheCode = strconv.Itoa(c)
		} else {
			cacheCode = strconv.Itoa(code.ErrUnknown)
		}
	}
	//处理缓存命中结果
	if err == nil && found {
		cacheDetails["cache_return_candidate"] = true
	}
	if err == nil && found && user != nil {
		if verifyIntent {
			trace.AddRequestTag(ctx, "verify_user_gone_cache_hit", true)
		}
		switch user.Name {
		case RATE_LIMIT_PREVENTION:
			cacheDetails["cache_result"] = "negative_hit"
			if verifyIntent {
				trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "negative")
			}
			if !forceRefresh {
				recordBatchResult(user, false)
				return user, nil
			}
			cacheDetails["cache_result"] = "negative_bypass"
			if verifyIntent {
				trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "negative_bypass")
			}
		case BLACKLIST_SENTINEL:
			cacheDetails["cache_result"] = "blacklist_hit"
			if verifyIntent {
				trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "blacklist")
			}
			if !forceRefresh {
				recordBatchResult(user, false)
				return user, nil
			}
			cacheDetails["cache_result"] = "blacklist_bypass"
			if verifyIntent {
				trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "blacklist_bypass")
			}
		default:
			cacheDetails["cache_result"] = "hit"
			if verifyIntent {
				trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "positive")
			}
			recordBatchResult(user, false)
			return user, nil
		}
	}

	if err == nil && !found {
		cacheDetails["cache_result"] = "miss"
		if verifyIntent {
			trace.AddRequestTag(ctx, "verify_user_gone_cache_result", "miss")
		}
	}

	if cacheSpan != nil {
		cacheDetails["fallback_db"] = true
	}
	endCacheSpan()
	//缓存未命中或强制刷新，继续查库.这里的    shouldDelay 只是“在打 DB 之前先等一小段时间”，并不是决定查不查库。   forceRefresh（或者    ctx 标记了    isStrongConsistencyRequest）通常意味着：刚刚有人针对这个用户做了写操作，随后立刻来验证状态。此时 cache miss 立刻去读 DB，有可能正好撞上复制/提交的窗口期，得到一个“旧状态”，又会触发下一轮刷新。 • strongConsistencyProbeDelay() 返回的就是这个“保护性等待”；等上一点点（几十毫秒级）可以让前面的写完成提交、主从同步或 CDC 触发，把“确认读”变得更可靠，同时也避免一窝蜂地打 DB。 • 等完之后仍然会执行后面的    util.RetryWithBackoff(...) 查库逻辑，因此不会影响最终一定会回源 DB 的事实，只是让“强一致/force refresh”场景更稳、更少脏读。
	// 当然，这个保护性等待也不是绝对的，仍然有可能遇到极端情况（比如写入延迟特别高、DB 瞬时压力大等）导致读到旧数据，但整体概率会大幅降低。
	//create/delete 场景下强制刷新的读请求，通常希望尽量避免读到旧数据
	shouldDelay := forceRefresh || isStrongConsistencyRequest(ctx)
	if shouldDelay {
		delay := u.strongConsistencyProbeDelay()
		if delay > 0 {
			trace.AddRequestTag(ctx, "strong_consistency_probe_delay_ms", delay.Milliseconds())
			if ok, _ := waitWithContext(ctx, delay); !ok {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
			}
		}
	}

	dbSpanCtx, dbSpan := trace.StartSpan(ctx, "user-service", "check_user_primary_lookup")
	if dbSpanCtx != nil {
		ctx = dbSpanCtx
	}
	dbStatus := "success"
	dbCode := strconv.Itoa(code.ErrSuccess)
	dbDetails := map[string]any{
		"username":      username,
		"force_refresh": forceRefresh,
	}
	start := time.Now()
	attemptCount := 0
	sharedHit := false
	defer func() {
		if dbSpan != nil {
			dbDetails["duration_ms"] = time.Since(start).Milliseconds()
			dbDetails["attempts"] = attemptCount
			dbDetails["singleflight_shared"] = sharedHit
			trace.EndSpan(dbSpan, dbStatus, dbCode, dbDetails)
		}
	}()

	result, err := util.RetryWithBackoff(u.Options.RedisOptions.MaxRetries, isRetryableError, func() (interface{}, error) {
		attemptCount++
		dbCtx, cancel := u.newDBContext(ctx, u.contactRefreshTimeout())
		if forceRefresh {
			dbCtx = storectx.WithForcePrimary(dbCtx)
		}
		defer cancel()
		//利用 singleflight 避免并发请求打穿数据库
		//对于同一用户名的多并发请求，只会有一个实际查询数据库，其他请求等待结果返回
		//减少数据库压力和重复查询
		//注意：singleflight 作用域为 UserService 实例级别，不同实例间无法共享
		//适用于高并发场景下的用户存在性检查
		r, err, shared := u.group.Do(username, func() (interface{}, error) {
			return u.getUserFromDBAndSetCache(dbCtx, username)
		})
		if shared {
			metrics.RequestsMerged.WithLabelValues("get").Inc()
			sharedHit = true
		}
		return r, err
	})
	if err != nil {
		dbStatus = "error"
		dbDetails["error"] = err.Error()
		if c := errors.GetCode(err); c != 0 {
			dbCode = strconv.Itoa(c)
		} else {
			dbCode = strconv.Itoa(code.ErrUnknown)
		}
		tagIfLockWait(ctx, err, "check_user_exist_db")
		if verifyIntent {
			trace.AddRequestTag(ctx, "verify_user_gone_db_result", "error")
		}
		return nil, err
	}
	if result == nil {
		dbDetails["db_result"] = "not_found"
		recordBatchResult(nil, true)
		if verifyIntent {
			trace.AddRequestTag(ctx, "verify_user_gone_db_result", "not_found")
		}
		return nil, nil
	}
	dbDetails["db_result"] = "hit"
	if userObj, ok := result.(*v1.User); ok {
		recordBatchResult(userObj, false)
		if verifyIntent {
			trace.AddRequestTag(ctx, "verify_user_gone_db_result", "hit")
		}
		return userObj, nil
	}
	if verifyIntent {
		trace.AddRequestTag(ctx, "verify_user_gone_db_result", "unexpected_type")
	}
	return nil, fmt.Errorf("unexpected user lookup result type %T", result)
}

// 判断是否为可重试错误（如超时、临时网络错误、数据库临时错误等）
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// 1. context 超时/取消
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	// 2. 标准库 Temporary 接口
	if e, ok := err.(interface{ Temporary() bool }); ok && e.Temporary() {
		return true
	}
	// 3. 错误字符串分析（参考 shouldRetry/isRecoverableError）
	errStr := err.Error()
	recoverableErrors := []string{
		// 超时和网络错误
		"timeout", "deadline exceeded", "connection refused", "network error",
		"connection reset", "broken pipe", "no route to host",
		// 数据库临时错误
		"database is closed", "deadlock", "1213", "40001", "invalid connection",
		"temporary", "busy", "lock", "try again",
		// 资源暂时不可用
		"resource temporarily unavailable", "too many connections",
	}
	for _, substr := range recoverableErrors {
		if strings.Contains(errStr, substr) {
			return true
		}
	}
	return false
}
