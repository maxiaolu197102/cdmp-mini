/*
聚焦分析 checkUserExist 新参数在各业务流程里的用法，我先拆成两步：1) 解释 checkUserExist 的内部机制与 forceRefresh 的作用；2) 对照 create/update/delete/list/changePassword 等调用点说明为何选择不同的布尔值。

	}

缓存优先 + 可选回源：入口在 user_service.go 的 checkUserExist(ctx, username, forceRefresh)。它先通过 tryGetFromCache 查询 Redis（见 get_service.go），命中真实用户就直接返回；命中的是我们自己写入的负缓存 RATE_LIMIT_PREVENTION 时，会根据 forceRefresh 判断是否继续。
forceRefresh=false：直接把负缓存返回，认为“这个用户不存在”。
forceRefresh=true：即便命中了负缓存，也会日志提示“命中负缓存, 强制回源校验”，然后继续往数据库查一遍。
防止击穿的负缓存：getUserFromDBAndSetCache 查库找不到用户时，会调用 cacheNullValue，通过新改动后的 Redis.SetNX 写入带统一前缀的空值，短期内阻挡重复查询。
数据库兜底：缓存未命中或强制回源时，使用 util.RetryWithBackoff + singleflight 查询数据库，再把结果写回缓存。这段逻辑保证并发下只打一次 DB，同时对临时错误做重试。
不同业务场景如何传 forceRefresh
场景	代码位置	参数	理由
创建	create_service.go 第 15 行附近 checkUserExist(ctx, user.Name, false)	false	创建时我们只需要知道“目前是否有人占用这个用户名”。如果缓存里已经有负缓存，说明之前查库确认过不存在，可以放心创建；命中真实用户则阻止重复创建。
更新	update_service.go 第 14 行 checkUserExist(ctx, user.Name, true)	true	更新必须保证用户真实存在。即便缓存说不存在（可能是老的负缓存），也要强制回源 DB，避免误报“用户不存在”导致 404。
删除 / 批量删除	delete_service.go 第 17、31 行	true	删除尤其是强制删时，需要跨缓存验证，防止负缓存阻挡真实存在的用户被删除。
改密码	change_password_servcice.go 第 15 行	true	密码修改只针对现有用户，必须强制回源确认，不让负缓存阻塞。
查询（List/Get 前置过滤）	list_service.go 第 33 行等	true	列表在进入存储层前也要确认用户存在；否则负缓存会让查询直接中断，实际库里的用户就查不到了。Get 本身通过缓存->singleflight 也是强制最终一致。
总结来说，写操作里区分“是否需要强一致性”：

创造性动作（Create）允许负缓存阻挡，避免多余回源；
读取/变更/删除类操作为了强一致，统一传 true，强制绕过负缓存；
配合 checkUserExist 新增的 forceRefresh 逻辑，就能既保留负缓存的抗击穿效果，又避免它影响后续真实存在的用户操作。

GPT-5-Codex (Preview) • 1x
*/
package user

import (
	"context"
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
	RATE_LIMIT_PREVENTION           = usercache.NegativeCacheSentinel
	BLACKLIST_SENTINEL              = usercache.BlacklistSentinel
	createStepSlowThreshold         = 200 * time.Millisecond
	contactPlaceholderTTL           = 30 * time.Second
	contactWarmupTimeout            = 2 * time.Minute
	contactWarmupBatchSize          = 1000
	contactCacheTTL                 = 24 * time.Hour
	strongConsistencyMaxRetries     = 3
	strongConsistencyBackoffBase    = 80 * time.Millisecond
	strongConsistencyBackoffCeiling = 500 * time.Millisecond
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type UserService struct {
	Store    interfaces.Factory
	Redis    *storage.RedisCluster
	Options  *options.Options
	Producer producer.MessageProducer
	Audit    *audit.Manager
	group    singleflight.Group

	contactWarmupMu        sync.Mutex
	contactWarming         bool
	contactCacheReady      atomic.Bool
	preflightLimiter       *semaphore.Weighted
	poolReporter           *poolStatsReporter
	contactWarmupNextRetry atomic.Int64
}

type contextKey string

const forceCacheRefreshKey contextKey = "user.forceCacheRefresh"

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
	exists   bool
	ttl      time.Duration
	degraded bool
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

// NewUserService 创建用户服务实例
func NewUserService(store interfaces.Factory, redis *storage.RedisCluster, opts *options.Options, producer producer.MessageProducer, auditMgr *audit.Manager) *UserService {
	return &UserService{
		Store:            store,
		Redis:            redis,
		Options:          opts,
		Producer:         producer,
		Audit:            auditMgr,
		preflightLimiter: newPreflightLimiter(opts),
		poolReporter:     newPoolStatsReporterForFactory(store),
	}
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

// markUserPendingCreate 将用户名的 pending 标记写入 Redis，确保 Kafka 侧能够做最终一致性校验。
func (u *UserService) markUserPendingCreate(ctx context.Context, username string) (bool, bool, time.Duration, time.Duration, time.Duration, error) {
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
	payload := u.pendingCreatePayload(ctx, trimmed)
	redisCtx, cancel := u.redisOpContext(ctx)
	defer cancel()
	setNXStart := time.Now()
	created, err := u.Redis.SetNX(redisCtx, key, payload, ttl)
	setNXDuration := time.Since(setNXStart)
	metrics.RecordRedisOperation("pending_marker_setnx", setNXDuration.Seconds(), err)
	trace.AddRequestTag(ctx, "pending_marker_setnx_ms", setNXDuration.Milliseconds())
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
	if errors.IsCode(err, code.ErrDatabaseTimeout) {
		return true
	}
	cause := errors.Cause(err)
	if cause == nil {
		cause = err
	}
	if cause == context.DeadlineExceeded || cause == context.Canceled {
		return true
	}
	if te, ok := cause.(interface{ Timeout() bool }); ok && te.Timeout() {
		return true
	}
	msg := strings.ToLower(cause.Error())
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

	if strings.TrimSpace(user.Name) != "" || email != "" || phone != "" {
		result, err := util.RetryWithBackoff(retryAttempts, isRetryableError, func() (interface{}, error) {
			dbCtx, cancel := u.newDBContext(ctx, u.contactLookupTimeout())
			defer cancel()
			ranPreflight = true
			dbStart := time.Now()
			conflicts, confErr := store.PreflightConflicts(dbCtx, user.Name, email, phone, u.Options)
			u.recordUserCreateStep(ctx, "preflight_query", "database", user.Name, time.Since(dbStart), confErr)
			return conflicts, confErr
		})
		if err != nil {
			preflightErr = err
		} else if result != nil {
			if typed, ok := result.(map[string]*v1.User); ok {
				preflight = typed
			}
		}
	}

	if ranPreflight && strings.TrimSpace(user.Name) != "" && preflightErr == nil {
		usernameChecked = true
	}

	if preflightErr != nil {
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
				return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
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
				return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在")
			},
		); err != nil {
			return nil, false, err
		}
	}

	return preflight, usernameChecked, nil
}

func (u *UserService) ensureEmailUnique(ctx context.Context, email, owner string) (err error) {
	start := time.Now()
	defer func() {
		u.recordUserCreateStep(ctx, "ensure_email_unique", "email", owner, time.Since(start), err)
	}()
	normalized := usercache.NormalizeEmail(email)
	if normalized == "" {
		err = errors.WithCode(code.ErrInvalidParameter, "邮箱不能为空")
		return err
	}
	store := u.userStoreReadOnly()
	if store == nil {
		err = errors.WithCode(code.ErrDatabase, "用户存储未就绪")
		return err
	}
	cacheKey := u.generateEmailCacheKey(normalized)
	return u.ensureContactUnique(ctx, cacheKey, owner, "邮箱", normalized, "email", func(dbCtx context.Context) (*v1.User, error) {
		return store.GetByEmail(dbCtx, normalized, u.Options)
	})
}

func (u *UserService) ensurePhoneUnique(ctx context.Context, phone, owner string) (err error) {
	start := time.Now()
	defer func() {
		u.recordUserCreateStep(ctx, "ensure_phone_unique", "phone", owner, time.Since(start), err)
	}()
	normalized := usercache.NormalizePhone(phone)
	if normalized == "" {
		return nil
	}
	store := u.userStoreReadOnly()
	if store == nil {
		err = errors.WithCode(code.ErrDatabase, "用户存储未就绪")
		return err
	}
	cacheKey := u.generatePhoneCacheKey(normalized)
	return u.ensureContactUnique(ctx, cacheKey, owner, "手机号", normalized, "phone", func(dbCtx context.Context) (*v1.User, error) {
		return store.GetByPhone(dbCtx, normalized, u.Options)
	})
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

func (u *UserService) checkUserExist(ctx context.Context, username string, forceRefresh bool) (*v1.User, error) {
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
		switch user.Name {
		case RATE_LIMIT_PREVENTION:
			cacheDetails["cache_result"] = "negative_hit"
			if !forceRefresh {
				return user, nil
			}
			cacheDetails["cache_result"] = "negative_bypass"
		case BLACKLIST_SENTINEL:
			cacheDetails["cache_result"] = "blacklist_hit"
			if !forceRefresh {
				return user, nil
			}
			cacheDetails["cache_result"] = "blacklist_bypass"
		default:
			cacheDetails["cache_result"] = "hit"
			return user, nil
		}
	}

	if err == nil && !found {
		cacheDetails["cache_result"] = "miss"
	}

	if cacheSpan != nil {
		cacheDetails["fallback_db"] = true
	}
	endCacheSpan()

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
		return nil, err
	}
	if result == nil {
		dbDetails["db_result"] = "not_found"
		return nil, nil
	}
	dbDetails["db_result"] = "hit"
	return result.(*v1.User), nil
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
