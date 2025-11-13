package user

import (
	"context"
	stdjson "encoding/json"
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

	"github.com/google/uuid"

	storectx "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
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
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

const (
	RATE_LIMIT_PREVENTION               = usercache.NegativeCacheSentinel
	BLACKLIST_SENTINEL                  = usercache.BlacklistSentinel
	createStepSlowThreshold             = 200 * time.Millisecond
	contactPlaceholderTTL               = 30 * time.Second
	contactWarmupTimeout                = 2 * time.Minute
	contactWarmupBatchSize              = 1000
	contactCacheTTL                     = 24 * time.Hour
	strongConsistencyMaxRetries         = 3
	strongConsistencyBackoffBase        = 80 * time.Millisecond
	strongConsistencyBackoffCeiling     = 500 * time.Millisecond
	strongConsistencyInitialDelayBase   = 35 * time.Millisecond
	strongConsistencyInitialDelayJitter = 45 * time.Millisecond
	batchLookupCacheTTL                 = 750 * time.Millisecond
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type UserService struct {
	Store              interfaces.Factory
	Redis              *storage.RedisCluster
	Options            *options.Options
	Producer           producer.MessageProducer
	Audit              *audit.Manager
	pendingCoordinator *usercache.PendingCoordinator
	group              singleflight.Group

	contactWarmupMu        sync.Mutex
	contactWarming         bool
	contactCacheReady      atomic.Bool
	preflightLimiter       *semaphore.Weighted
	poolReporter           *poolStatsReporter
	contactWarmupNextRetry atomic.Int64
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

// pendingMarkerPayload uses a concrete struct so JSON encoding avoids map-based reflection overhead.
type pendingMarkerPayload struct {
	Status          string `json:"status"`
	Degraded        bool   `json:"degraded,omitempty"`
	Username        string `json:"username"`
	Timestamp       string `json:"timestamp"`
	RequestID       string `json:"request_id,omitempty"`
	Operator        string `json:"operator,omitempty"`
	ClientIP        string `json:"client_ip,omitempty"`
	LegacyRequestID string `json:"legacy_request_id,omitempty"`
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
func NewUserService(store interfaces.Factory, redis *storage.RedisCluster, opts *options.Options, producer producer.MessageProducer, auditMgr *audit.Manager) *UserService {
	svc := &UserService{
		Store:            store,
		Redis:            redis,
		Options:          opts,
		Producer:         producer,
		Audit:            auditMgr,
		preflightLimiter: newPreflightLimiter(opts),
		poolReporter:     newPoolStatsReporterForFactory(store),
	}
	if redis != nil {
		cfg := usercache.PendingCoordinatorConfig{
			LeaseTTL:       svc.pendingCreateTTL(),
			Component:      "user_service",
			LogLeaseEvents: true,
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
		svc.pendingCoordinator = usercache.NewPendingCoordinator(redis, cfg)
	}
	return svc
}

// PendingCoordinator exposes the pending lease coordinator for downstream components (e.g. HTTP middleware).
func (u *UserService) PendingCoordinator() *usercache.PendingCoordinator {
	if u == nil {
		return nil
	}
	return u.pendingCoordinator
}

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

func (u *UserService) recordUserCreateStep(ctx context.Context, step, field, username string, duration time.Duration, stepErr error) {
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

func waitWithContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
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
		if waitWithContext(ctx, delay) {
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
		return u.legacyLookupPendingCreateMarker(ctx, trimmed)
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

func (u *UserService) legacyLookupPendingCreateMarker(ctx context.Context, username string) (pendingMarkerState, error) {
	state := pendingMarkerState{}
	if u == nil || u.Redis == nil {
		return state, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return state, nil
	}
	key := usercache.PendingCreateKey(trimmed)
	if key == "" {
		return state, nil
	}

	redisCtx, cancel := u.redisOpContext(ctx)
	start := time.Now()
	value, err := u.Redis.GetKey(redisCtx, key)
	cancel()
	duration := time.Since(start)
	metricErr := err
	if errors.Is(err, redis.Nil) || errors.Is(err, storage.ErrKeyNotFound) {
		metricErr = nil
	}
	metrics.RecordRedisOperation("pending_marker_get", duration.Seconds(), metricErr)
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, storage.ErrKeyNotFound) {
			return state, nil
		}
		return state, err
	}

	state.exists = true
	if degraded, decodeErr := usercache.PendingMarkerIsDegraded(value); decodeErr != nil {
		trace.AddRequestTag(ctx, "pending_marker_decode_error", decodeErr.Error())
	} else if degraded {
		state.degraded = true
	}

	ttlCtx, ttlCancel := u.redisOpContext(ctx)
	ttlStart := time.Now()
	ttlSeconds, ttlErr := u.Redis.GetExp(ttlCtx, key)
	ttlCancel()
	ttlDuration := time.Since(ttlStart)
	ttlMetricErr := ttlErr
	if errors.Is(ttlErr, storage.ErrKeyNotFound) {
		ttlMetricErr = nil
	}
	metrics.RecordRedisOperation("pending_marker_ttl", ttlDuration.Seconds(), ttlMetricErr)
	if ttlErr == nil && ttlSeconds > 0 {
		state.ttl = time.Duration(ttlSeconds) * time.Second
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
	return meta
}

func (u *UserService) pendingCreatePayload(ctx context.Context, username string) string {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	degraded := userctx.IsCreateDegraded(ctx)
	status := "pending"
	if degraded {
		status = "degraded"
		trace.AddRequestTag(ctx, "create_pending_degraded", true)
	}
	payload := pendingMarkerPayload{
		Status:    status,
		Degraded:  degraded,
		Username:  username,
		Timestamp: timestamp,
	}
	if traceCtx := trace.FromContext(ctx); traceCtx != nil {
		if requestID := traceCtx.RequestContext.RequestID; requestID != "" {
			payload.RequestID = requestID
		}
		if operator := traceCtx.RequestContext.Operator; operator != "" {
			payload.Operator = operator
		}
		if clientIP := traceCtx.RequestContext.ClientIP; clientIP != "" {
			payload.ClientIP = clientIP
		}
	}
	if legacyID := ctx.Value("requestID"); legacyID != nil {
		payload.LegacyRequestID = fmt.Sprint(legacyID)
	}
	data, err := json.Marshal(&payload)
	if err != nil {
		log.Warnw("构造用户创建幂等标记payload失败，降级为时间戳", "username", username, "error", err)
		return timestamp
	}
	return string(data)
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
func (u *UserService) markUserPendingCreate(ctx context.Context, username string) (bool, bool, time.Duration, time.Duration, time.Duration, error) {
	if u == nil {
		return false, false, 0, 0, 0, nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return false, false, 0, 0, 0, nil
	}
	if u.pendingCoordinator == nil {
		return u.legacyMarkUserPendingCreate(ctx, trimmed)
	}
	if depth, level, sampleErr := u.pendingCoordinator.SampleQueueDepth(ctx); sampleErr != nil {
		trace.AddRequestTag(ctx, "pending_queue_sample_error", sampleErr.Error())
	} else {
		if depth > 0 {
			trace.AddRequestTag(ctx, "pending_queue_depth", depth)
		}
		if level != usercache.BackpressureNone {
			trace.AddRequestTag(ctx, "pending_backpressure_level", string(level))
			if metrics.PendingLeaseEvents != nil {
				metrics.PendingLeaseEvents.WithLabelValues("user_service", "pre_acquire_backpressure").Inc()
			}
			if delay := u.pendingCoordinator.BackpressureDelay(level, depth); delay > 0 {
				trace.AddRequestTag(ctx, "pending_backpressure_delay_ms", delay.Milliseconds())
				log.Infow("pending lease pre-acquire delay", "component", "user_service", "username", trimmed, "queue_depth", depth, "backpressure", string(level), "delay_ms", delay.Milliseconds())
				if !waitWithContext(ctx, delay) {
					if ctx != nil {
						return false, false, 0, 0, 0, ctx.Err()
					}
					return false, false, 0, 0, 0, context.Canceled
				}
				if metrics.PendingLeaseEvents != nil {
					metrics.PendingLeaseEvents.WithLabelValues("user_service", "pre_acquire_delay").Inc()
				}
			}
		}
	}
	meta := u.pendingLeaseMetadata(ctx, trimmed)
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
					return false, false, 0, 0, 0, errors.WithCode(code.ErrServerBusy, "用户创建任务正在恢复，请稍后再试")
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
				return false, false, 0, 0, 0, errors.WithCode(code.ErrServerBusy, "用户创建排队中，请稍后重试")
			case usercache.AcquireFailureConflict:
				return false, false, 0, 0, 0, errors.WithCode(code.ErrServerBusy, "用户创建正在进行，请稍后再试")
			}
		}
		return false, false, 0, 0, 0, err
	}
	lease := result.Lease
	if lease == nil {
		trace.AddRequestTag(ctx, "pending_marker_setnx_ms", result.SetNXDuration.Milliseconds())
		return false, false, 0, result.SetNXDuration, 0, nil
	}
	pendingTTL := lease.LeaseExpiresAt.Sub(time.Now())
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
	return true, false, pendingTTL, result.SetNXDuration, 0, nil
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

func (u *UserService) legacyMarkUserPendingCreate(ctx context.Context, username string) (bool, bool, time.Duration, time.Duration, time.Duration, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" || u == nil || u.Redis == nil {
		return false, false, 0, 0, 0, nil
	}
	key := usercache.PendingCreateKey(trimmed)
	if key == "" {
		return false, false, 0, 0, 0, nil
	}
	ttl := u.pendingCreateTTL()
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	if jitter := time.Duration(rand.Intn(5)) * time.Second; jitter > 0 {
		ttl += jitter
	}
	meta := u.pendingLeaseMetadata(ctx, trimmed)
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)
	ownerID := uuid.New().String()
	degraded := userctx.IsCreateDegraded(ctx)
	status := "pending"
	if degraded {
		status = "degraded"
	}
	snapshot := struct {
		Status          string `json:"status"`
		Degraded        bool   `json:"degraded,omitempty"`
		State           string `json:"state"`
		OwnerID         string `json:"owner_id"`
		Version         int64  `json:"version"`
		LeaseExpiresAt  string `json:"lease_expires_at"`
		AcquireAt       string `json:"acquire_at"`
		UpdatedAt       string `json:"updated_at"`
		Username        string `json:"username,omitempty"`
		RequestID       string `json:"request_id,omitempty"`
		Operator        string `json:"operator,omitempty"`
		ClientIP        string `json:"client_ip,omitempty"`
		LegacyRequestID string `json:"legacy_request_id,omitempty"`
	}{
		Status:          status,
		Degraded:        degraded,
		State:           string(usercache.PendingStateLease),
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
	payloadBytes, marshalErr := stdjson.Marshal(&snapshot)
	leaseOwnerForTrace := ""
	if marshalErr != nil {
		log.Warnw("构造 pending 租约快照失败，回退旧格式", "username", trimmed, "error", marshalErr)
		payloadBytes = []byte(u.pendingCreatePayload(ctx, trimmed))
	} else {
		leaseOwnerForTrace = ownerID
	}
	payload := string(payloadBytes)
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	setNXStart := time.Now()
	created, err := u.Redis.SetNX(redisCtx, key, payload, ttl)
	setNXDuration := time.Since(setNXStart)
	metrics.RecordRedisOperation("pending_marker_setnx", setNXDuration.Seconds(), err)
	trace.AddRequestTag(ctx, "pending_marker_setnx_ms", setNXDuration.Milliseconds())
	if leaseOwnerForTrace != "" {
		trace.AddRequestTag(ctx, "pending_lease_owner", leaseOwnerForTrace)
	}
	if err != nil {
		log.Errorw("设置用户创建幂等标记失败", "username", trimmed, "error", err)
		trace.AddRequestTag(ctx, "pending_marker_setnx_error", err.Error())
		return false, false, ttl, setNXDuration, 0, errors.WithCode(code.ErrRedisFailed, "设置用户创建幂等标记失败")
	}
	if created {
		return true, false, ttl, setNXDuration, 0, nil
	}
	refreshCtx, refreshCancel := u.redisOpContext(ctx)
	refreshStart := time.Now()
	refreshErr := u.Redis.SetKey(refreshCtx, key, payload, ttl)
	refreshCancel()
	refreshDuration := time.Since(refreshStart)
	metrics.RecordRedisOperation("pending_marker_refresh", refreshDuration.Seconds(), refreshErr)
	trace.AddRequestTag(ctx, "pending_marker_refresh_ms", refreshDuration.Milliseconds())
	if refreshErr != nil {
		log.Errorw("刷新用户创建幂等标记失败", "username", trimmed, "error", refreshErr)
		trace.AddRequestTag(ctx, "pending_marker_refresh_error", refreshErr.Error())
		return false, false, ttl, setNXDuration, refreshDuration, errors.WithCode(code.ErrRedisFailed, "刷新用户创建幂等标记失败")
	}
	return false, true, ttl, setNXDuration, refreshDuration, nil
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
//	无: 此函数无显式入参
//
// 返回值：
//
//	无: 无返回值
//
// 示例：
//
//	u.ensureContactCacheReady()
//
// 注意事项：
//   - 预热过程在独立 goroutine 中执行，调用方无需等待结果
//   - 若依赖未就绪会直接返回，不会强制重试
//
// 异常情况：
//   - 预热失败会记录下一次重试时间并输出警告日志
//   - 上一次预热仍在运行时会跳过本次触发
func (u *UserService) ensureContactCacheReady() {
	if u.contactCacheReady.Load() {
		return
	}
	if u.Options == nil || u.Options.ServerRunOptions == nil || !u.Options.ServerRunOptions.EnableContactWarmup {
		u.contactCacheReady.Store(true)
		return
	}
	if u.Store == nil || u.Redis == nil {
		return
	}
	next := u.contactWarmupNextRetry.Load()
	if next > 0 && time.Now().Unix() < next {
		return
	}
	u.contactWarmupMu.Lock()
	if u.contactCacheReady.Load() || u.contactWarming {
		u.contactWarmupMu.Unlock()
		return
	}
	u.contactWarming = true
	u.contactWarmupMu.Unlock()

	go func() {
		retryDelay := 30 * time.Second
		if err := u.warmContactCache(); err != nil {
			u.contactWarmupNextRetry.Store(time.Now().Add(retryDelay).Unix())
			log.Warnw("联系人缓存预热失败", "error", err, "retry_after", retryDelay)
			u.contactWarmupMu.Lock()
			u.contactWarming = false
			u.contactWarmupMu.Unlock()
			return
		}
		u.contactCacheReady.Store(true)
		u.contactWarmupNextRetry.Store(0)
		u.contactWarmupMu.Lock()
		u.contactWarming = false
		u.contactWarmupMu.Unlock()
	}()
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
//   - 当缓存尚未预热或 Redis 不可用时，会主动要求执行预检
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
	if forceCacheRefreshFromContext(ctx) || isStrongConsistencyRequest(ctx) {
		return true
	}
	if strings.TrimSpace(user.Name) == "" && user.Email == "" && user.Phone == "" {
		return false
	}
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
	//数据库超时错误
	if errors.IsCode(err, code.ErrDatabaseTimeout) {
		return true
	}
	cause := errors.Cause(err)
	if cause == nil {
		cause = err
	}
	//上下文取消或超时错误 上下文被取消
	if cause == context.DeadlineExceeded || cause == context.Canceled {
		return true
	}
	//检查是否为超时类型错误
	if te, ok := cause.(interface{ Timeout() bool }); ok && te.Timeout() {
		return true
	}
	msg := strings.ToLower(cause.Error())
	//错误信息中包含超时关键词
	return strings.Contains(msg, "deadline exceeded") || strings.Contains(msg, "timeout")
}

func (u *UserService) markCreateDegraded(ctx context.Context, reason string, kv ...interface{}) {
	if userctx.MarkCreateDegraded(ctx) {
		trace.AddRequestTag(ctx, "create_degraded", true)
		if reason != "" {
			trace.AddRequestTag(ctx, "create_degraded_reason", reason)
		}
		fields := []interface{}{"reason", reason}
		if len(kv) > 0 {
			fields = append(fields, kv...)
		}
		log.Warnw("用户创建进入降级模式", fields...)
		return
	}
	if reason != "" {
		trace.AddRequestTag(ctx, "create_degraded_reason", reason)
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

func (u *UserService) ensureContactPlaceholder(ctx context.Context, cacheKey, owner string) {
	if u.Redis == nil || cacheKey == "" {
		return
	}
	fieldKey := contactFieldFromCacheKey(cacheKey)
	placeholder := owner
	if strings.TrimSpace(placeholder) == "" {
		placeholder = RATE_LIMIT_PREVENTION
	}
	setCtx, setCancel := u.redisOpContext(ctx)
	setStart := time.Now()
	ok, err := u.Redis.SetNX(setCtx, cacheKey, placeholder, contactPlaceholderTTL)
	setDuration := time.Since(setStart)
	setCancel()
	u.recordUserCreateStep(ctx, "redis_placeholder_setnx", fieldKey, owner, setDuration, err)
	if err != nil {
		log.Warnw("唯一性灰度占位失败", "key", cacheKey, "error", err)
		return
	}
	if ok {
		return
	}
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
		return
	}
	/// 判断是否需要刷新占位符：若现有值与当前占位符匹配（或为特殊标记/空），则延长过期时间
	if strings.EqualFold(existing, placeholder) || existing == "" || existing == RATE_LIMIT_PREVENTION {
		refreshCtx, refreshCancel := u.redisOpContext(ctx)
		refreshStart := time.Now()
		setErr := u.Redis.SetKey(refreshCtx, cacheKey, placeholder, contactPlaceholderTTL)
		refreshDuration := time.Since(refreshStart)
		u.recordUserCreateStep(ctx, "redis_placeholder_refresh", fieldKey, owner, refreshDuration, setErr)
		if setErr != nil {
			log.Warnw("唯一性灰度占位刷新失败", "key", cacheKey, "error", setErr)
		}
		refreshCancel()
	}
}

func (u *UserService) ensureDegradedContactPlaceholders(ctx context.Context, username, email, phone string) {
	if email != "" {
		emailKey := u.generateEmailCacheKey(email)
		u.ensureContactPlaceholder(ctx, emailKey, username)
	}
	if phone != "" {
		phoneKey := u.generatePhoneCacheKey(phone)
		u.ensureContactPlaceholder(ctx, phoneKey, username)
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
	limiter := u.preflightLimiter
	if limiter != nil {
		waitStart := time.Now()
		err := limiter.Acquire(ctx, 1)
		u.recordUserCreateStep(ctx, "preflight_limiter_wait", "limiter", user.Name, time.Since(waitStart), err)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, false, errors.WithCode(code.ErrDatabaseTimeout, "预检查询等待超时")
			}
			return nil, false, errors.WithCode(code.ErrDatabase, "预检查询等待失败: %v", err)
		}
		defer func() {
			releaseStart := time.Now()
			limiter.Release(1)
			u.recordUserCreateStep(ctx, "preflight_limiter_release", "limiter", user.Name, time.Since(releaseStart), nil)
		}()
	}

	u.ensureContactCacheReady()
	u.normalizeUserContacts(user)

	email := user.Email
	phone := user.Phone

	store := u.userStoreReadOnly()
	if store == nil {
		return nil, false, errors.WithCode(code.ErrDatabase, "用户存储未就绪")
	}

	var (
		preflight       map[string]*v1.User
		preflightErr    error
		retryAttempts   = u.Options.RedisOptions.MaxRetries
		usernameChecked bool
		ranPreflight    bool
	)

	if retryAttempts <= 0 {
		retryAttempts = 1
	}

	runPreflight := u.shouldRunPreflight(ctx, user)
	if runPreflight && (strings.TrimSpace(user.Name) != "" || email != "" || phone != "") {
		result, err := util.RetryWithBackoff(retryAttempts, isRetryableError, func() (interface{}, error) {
			dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
			defer cancel()
			ranPreflight = true
			dbStart := time.Now()
			// 执行数据库预检
			conflicts, confErr := store.PreflightConflicts(dbCtx, user.Name, email, phone, u.Options)
			u.recordUserCreateStep(ctx, "preflight_query", "database", user.Name, time.Since(dbStart), confErr)
			return conflicts, confErr
		})
		//处理预检结果
		if err != nil {
			preflightErr = err
		} else if result != nil {
			//类型转换：将结果转为冲突用户的map（key为scope：username/email/phone）
			if typed, ok := result.(map[string]*v1.User); ok {
				preflight = typed
			}
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
	if runPreflight && preflightErr != nil {
		//通过 Redis 写入临时占位符避免并发冲突
		if shouldDegradeForError(preflightErr) {
			u.markCreateDegraded(ctx, "preflight_timeout", "username", user.Name)
			u.ensureDegradedContactPlaceholders(ctx, user.Name, email, phone)
			preflightErr = nil
			usernameChecked = false
		} else {
			return nil, false, preflightErr
		}
	}
	if preflight == nil {
		preflight = make(map[string]*v1.User)
	}

	if email != "" {
		emailCopy := email
		if err := u.ensureContactUnique(ctx,
			u.generateEmailCacheKey(emailCopy),
			user.Name,
			"邮箱",
			emailCopy,
			"email",
			func(lookupCtx context.Context) (*v1.User, error) {
				if err := lookupCtx.Err(); err != nil {
					return nil, err
				}
				if existing := preflight["email"]; existing != nil {
					return existing, nil
				}
				if runPreflight {
					return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
				}
				return store.GetByEmail(lookupCtx, emailCopy, u.Options)
			},
		); err != nil {
			return nil, false, err
		}
	}

	if phone != "" {
		phoneCopy := phone
		if err := u.ensureContactUnique(ctx,
			u.generatePhoneCacheKey(phoneCopy),
			user.Name,
			"手机号",
			phoneCopy,
			"phone",
			func(lookupCtx context.Context) (*v1.User, error) {
				if err := lookupCtx.Err(); err != nil {
					return nil, err
				}
				if existing := preflight["phone"]; existing != nil {
					return existing, nil
				}
				if runPreflight {
					return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
				}
				return store.GetByPhone(lookupCtx, phoneCopy, u.Options)
			},
		); err != nil {
			return nil, false, err
		}
	}

	return preflight, usernameChecked, nil
}

func (u *UserService) ensureContactUnique(
	ctx context.Context,
	cacheKey string,
	allowedOwner string,
	fieldLabel string,
	fieldValue string,
	fieldKey string,
	lookup func(context.Context) (*v1.User, error),
) (err error) {
	if cacheKey == "" {
		return nil
	}

	if userctx.IsCreateDegraded(ctx) {
		if u.Redis != nil {
			u.ensureContactPlaceholder(ctx, cacheKey, allowedOwner)
		}
		return nil
	}

	if u.Redis == nil {
		dbStart := time.Now()
		existing, lookupErr := lookup(ctx)
		u.recordUserCreateStep(ctx, "ensure_contact_lookup", fieldKey, allowedOwner, time.Since(dbStart), lookupErr)
		if lookupErr != nil {
			if errors.IsCode(lookupErr, code.ErrUserNotFound) {
				return nil
			}
			err = lookupErr
			return err
		}
		if existing == nil || strings.EqualFold(existing.Name, allowedOwner) {
			return nil
		}
		err = errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
		return err
	}

	start := time.Now()
	placeholderAcquired := false
	defer func() {
		u.recordUserCreateStep(ctx, "ensure_contact_unique", fieldKey, allowedOwner, time.Since(start), err)
		if placeholderAcquired && err != nil && !errors.IsCode(err, code.ErrValidation) {
			if _, delErr := u.Redis.DeleteKey(ctx, cacheKey); delErr != nil {
				log.Warnw("释放唯一性占位失败", "key", cacheKey, "field", fieldKey, "error", delErr)
			}
		}
	}()

	cachedOwner, cacheErr := u.Redis.GetKey(ctx, cacheKey)
	if cacheErr != nil {
		if !errors.Is(cacheErr, redis.Nil) {
			log.Warnf("%s唯一性缓存读取失败: key=%s err=%v", fieldLabel, cacheKey, cacheErr)
		}
	} else if cachedOwner != "" {
		if strings.EqualFold(cachedOwner, allowedOwner) {
			return nil
		}
		return errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
	}

	// 缓存未命中或键不存在时，尝试基于 SETNX 占位，降低并发探库次数
	if errors.Is(cacheErr, redis.Nil) || cachedOwner == "" {
		placeholderValue := allowedOwner
		if placeholderValue == "" {
			placeholderValue = RATE_LIMIT_PREVENTION
		}
		ok, setErr := u.Redis.SetNX(ctx, cacheKey, placeholderValue, contactPlaceholderTTL)
		if setErr != nil {
			log.Warnf("%s唯一性占位失败: key=%s err=%v", fieldLabel, cacheKey, setErr)
		} else if ok {
			placeholderAcquired = true
			cachedOwner = placeholderValue
			if u.contactCacheReady.Load() && allowedOwner != "" && !strings.EqualFold(allowedOwner, RATE_LIMIT_PREVENTION) {
				return nil
			}
		} else {
			if refreshed, err := u.Redis.GetKey(ctx, cacheKey); err == nil {
				cachedOwner = refreshed
			}
		}
		if cachedOwner != "" {
			if strings.EqualFold(cachedOwner, allowedOwner) && !placeholderAcquired {
				return nil
			}
			if !strings.EqualFold(cachedOwner, allowedOwner) {
				return errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
			}
		}
	}

	result, retryErr := util.RetryWithBackoff(u.Options.RedisOptions.MaxRetries, isRetryableError, func() (interface{}, error) {
		dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
		defer cancel()

		dbStart := time.Now()
		existing, lookupErr := lookup(dbCtx)
		u.recordUserCreateStep(ctx, "ensure_contact_lookup", fieldKey, allowedOwner, time.Since(dbStart), lookupErr)
		if lookupErr != nil {
			if errors.IsCode(lookupErr, code.ErrUserNotFound) {
				return nil, nil
			}
			return nil, lookupErr
		}
		return existing, nil
	})
	if retryErr != nil {
		if shouldDegradeForError(retryErr) {
			u.markCreateDegraded(ctx, "contact_lookup_timeout", "field", fieldKey, "owner", allowedOwner)
			u.ensureContactPlaceholder(ctx, cacheKey, allowedOwner)
			err = nil
			return nil
		}
		err = retryErr
		return err
	}
	if result == nil {
		return nil
	}
	existing := result.(*v1.User)
	if strings.EqualFold(existing.Name, allowedOwner) {
		return nil
	}

	if setErr := u.Redis.SetKey(ctx, cacheKey, existing.Name, contactCacheTTL); setErr != nil {
		log.Warnf("%s唯一性缓存写入失败: key=%s err=%v", fieldLabel, cacheKey, setErr)
	}
	return errors.WithCode(code.ErrValidation, "%s已被占用: %s", fieldLabel, fieldValue)
}

func (u *UserService) warmContactCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), contactWarmupTimeout)
	defer cancel()

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

		result, err := util.RetryWithBackoff(3, isRetryableError, func() (interface{}, error) {
			return u.Store.Users().List(ctx, opts, u.Options)
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
func (u *UserService) checkUserExist(ctx context.Context, username string, forceRefresh bool) (*v1.User, error) {
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

	shouldDelay := forceRefresh || isStrongConsistencyRequest(ctx)
	if shouldDelay {
		delay := u.strongConsistencyProbeDelay()
		if delay > 0 {
			trace.AddRequestTag(ctx, "strong_consistency_probe_delay_ms", delay.Milliseconds())
			if !waitWithContext(ctx, delay) {
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
