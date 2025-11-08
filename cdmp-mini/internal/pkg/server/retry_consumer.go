// internal/pkg/server/retry_consumer.go
package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/dbscan"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/db"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type RetryConsumer struct {
	reader          *kafka.Reader
	db              *gorm.DB
	sqlxDB          *sqlx.DB
	redis           *storage.RedisCluster
	producer        *UserProducer
	maxRetries      int
	kafkaOptions    *options.KafkaOptions
	poolReporter    poolStatsReporter
	poolComponent   string
	groupInstanceID string
	instanceIndex   int
}

func NewRetryConsumer(db *gorm.DB, redis *storage.RedisCluster, producer *UserProducer, kafkaOptions *options.KafkaOptions, topic, groupid string, instanceIndex int) *RetryConsumer {
	groupInstanceID := buildGroupInstanceID(kafkaOptions.InstanceID, groupid, instanceIndex)
	rc := &RetryConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:         kafkaOptions.Brokers,
			Topic:           topic,
			GroupID:         groupid,
			GroupInstanceID: groupInstanceID,
			MinBytes:        10e3,
			MaxBytes:        10e6,
			CommitInterval:  2 * time.Second,
			StartOffset:     kafka.FirstOffset,
		}),
		db:              db,
		redis:           redis,
		producer:        producer,
		maxRetries:      kafkaOptions.MaxRetries,
		kafkaOptions:    kafkaOptions,
		poolComponent:   groupid,
		groupInstanceID: groupInstanceID,
		instanceIndex:   instanceIndex,
	}
	if core, err := db.DB(); err != nil {
		log.Errorf("retry consumer sqlx init failed: %v", err)
	} else {
		rc.sqlxDB = sqlx.NewDb(core, "mysql").Unsafe()
	}
	return rc
}

func (rc *RetryConsumer) ensureSQLX() (*sqlx.DB, error) {
	if rc.sqlxDB != nil {
		return rc.sqlxDB, nil
	}
	if rc.db == nil {
		return nil, fmt.Errorf("gorm db not initialized")
	}
	core, err := rc.db.DB()
	if err != nil {
		return nil, fmt.Errorf("acquire sql db failed: %w", err)
	}
	rc.sqlxDB = sqlx.NewDb(core, "mysql").Unsafe()
	return rc.sqlxDB, nil
}

func (rc *RetryConsumer) loadUserSnapshot(ctx context.Context, username string) (*v1.User, error) {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil, nil
	}
	db, err := rc.ensureSQLX()
	if err != nil {
		return nil, err
	}
	const query = "SELECT id, instanceID, name, nickname, password, email, phone, status, isAdmin, createdAt, updatedAt, loginedAt, version FROM `user` WHERE name = ? LIMIT 1"
	rows, err := db.QueryContext(ctx, query, trimmed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var record v1.User
	if _, err := dbscan.ScanUserAuthInto(rows, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &record, nil
}

func (rc *RetryConsumer) Close() error {
	if rc.reader != nil {
		return rc.reader.Close()
	}
	return nil
}

func (rc *RetryConsumer) SetPoolStatsProvider(provider func() []db.PoolStats) {
	rc.poolReporter.provider = provider
}

func (rc *RetryConsumer) StartConsuming(ctx context.Context, workerCount int, ready *sync.WaitGroup) {
	log.Debugf("StartConsuming 开始，worker数量: %d", workerCount)
	log.Debugf("上下文状态: %v", ctx.Err())
	log.Debugf("Reader已初始化: %v", rc.reader != nil)
	var wg sync.WaitGroup
	readyOnce := sync.Once{}
	signalReady := func() {
		if ready != nil {
			readyOnce.Do(func() {
				ready.Done()
			})
		}
	}
	defer signalReady()

	if rc.reader == nil {
		log.Warn("Reader未初始化，等待...")
		if rc.reader == nil {
			log.Warn("Reader仍然未初始化，退出")
			return
		}
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rc.retryWorker(ctx, workerID)
		}(i)
	}

	signalReady()
	log.Debug("等待所有worker完成...")
	wg.Wait()
	log.Debug("所有worker已完成，StartConsuming返回")
}

func (rc *RetryConsumer) retryWorker(ctx context.Context, workerID int) {
	log.Debugf("启动重试消费者Worker %d (groupInstanceID=%s)", workerID, rc.groupInstanceID)

	for {
		select {
		case <-ctx.Done():
			log.Debugf("重试Worker %d (groupInstanceID=%s): 停止消费", workerID, rc.groupInstanceID)
			return
		default:
			log.Debugf("Worker %d (groupInstanceID=%s): 准备获取消息...", workerID, rc.groupInstanceID)
			msg, err := rc.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					log.Debugf("Worker %d (groupInstanceID=%s): 上下文已取消，退出", workerID, rc.groupInstanceID)
					return
				}
				log.Warnf("重试Worker %d (groupInstanceID=%s): 获取消息失败: %v，稍后重试", workerID, rc.groupInstanceID, err)
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return
				}
				continue
			}

			if err := rc.processRetryMessage(ctx, msg); err != nil {
				log.Errorf("重试Worker %d (groupInstanceID=%s): 处理消息失败: %v", workerID, rc.groupInstanceID, err)
				continue
			}

			if err := rc.commitWithRetry(ctx, msg, workerID); err != nil {
				log.Errorf("重试Worker %d (groupInstanceID=%s): 提交偏移量失败: %v", workerID, rc.groupInstanceID, err)
			}
		}
	}
}

// commitWithRetry 对 RetryConsumer 的 CommitMessages 进行短次数重试
func (rc *RetryConsumer) commitWithRetry(ctx context.Context, msg kafka.Message, workerID int) error {
	maxAttempts := 3
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := rc.reader.CommitMessages(ctx, msg); err != nil {
			lastErr = err
			metrics.ConsumerProcessingErrors.WithLabelValues(rc.reader.Config().Topic, rc.reader.Config().GroupID, "commit", "commit_error").Inc()
			log.Warnf("RetryWorker %d: 提交偏移量失败 (尝试 %d/%d): %v", workerID, i+1, maxAttempts, err)
			wait := time.Duration(100*(1<<uint(i))) * time.Millisecond
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			log.Debugf("RetryWorker %d: 偏移量提交成功", workerID)
			return nil
		}
	}
	log.Errorf("RetryWorker %d: 提交偏移量最终失败: %v", workerID, lastErr)
	return lastErr
}

func (rc *RetryConsumer) processRetryMessage(ctx context.Context, msg kafka.Message) error {
	component := rc.poolComponent
	if component == "" && rc.reader != nil {
		component = rc.reader.Config().GroupID
	}
	rc.poolReporter.report(ctx, component)
	operation := rc.getOperationFromHeaders(msg.Headers)

	switch operation {
	case OperationCreate:
		return rc.processRetryCreate(ctx, msg)
	case OperationUpdate:
		return rc.processRetryUpdate(ctx, msg)
	case OperationDelete:
		return rc.processRetryDelete(ctx, msg)
	default:
		log.Errorf("重试消息未知操作类型: %s", operation)
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "UNKNOWN_OPERATION_IN_RETRY: "+operation)
	}
}

func (rc *RetryConsumer) getOperationFromHeaders(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == HeaderOperation {
			return string(header.Value)
		}
	}
	return OperationCreate
}

func (rc *RetryConsumer) parseRetryHeaders(headers []kafka.Header) (int, time.Time, string) {
	var (
		currentRetryCount int = 0
		nextRetryTime     time.Time
		lastError         string = "未知错误"
	)

	for _, header := range headers {
		switch header.Key {
		case HeaderRetryCount:
			if count, err := strconv.Atoi(string(header.Value)); err == nil {
				currentRetryCount = count
			}
		case HeaderNextRetryTS:
			if t, err := time.Parse(time.RFC3339, string(header.Value)); err == nil {
				nextRetryTime = t
			}
		case HeaderRetryError:
			lastError = string(header.Value)
		}
	}

	// 如果没有找到下次重试时间，设置一个默认值
	if nextRetryTime.IsZero() {
		nextRetryTime = time.Now().Add(10 * time.Second)
		log.Debugf("未找到下次重试时间，使用默认值: %v", nextRetryTime)
	}

	return currentRetryCount, nextRetryTime, lastError
}

func (rc *RetryConsumer) processRetryCreate(ctx context.Context, msg kafka.Message) error {
	log.Debugf("开始处理重试创建消息: key=%s, headers=%+v", string(msg.Key), msg.Headers)
	user := userMessagePool.Get().(*v1.User)
	defer func() {
		*user = v1.User{}
		userMessagePool.Put(user)
	}()

	if err := decodeUserMessage(msg.Value, user); err != nil {
		log.Errorf("重试消息解析失败: %v, 原始消息: %s", err, string(msg.Value))
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "POISON_MESSAGE_IN_RETRY: "+err.Error())
	}
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)

	currentRetryCount, nextRetryTime, lastError := rc.parseRetryHeaders(msg.Headers)

	if currentRetryCount >= rc.maxRetries {
		log.Warnf("创建消息达到最大重试次数(%d), 发送到死信队列: username=%s",
			rc.maxRetries, user.Name)
		return rc.producer.SendToDeadLetterTopic(ctx, msg,
			fmt.Sprintf("MAX_RETRIES_EXCEEDED(%d): %s", rc.maxRetries, lastError))
	}

	existingSnapshot, snapshotErr := rc.loadUserSnapshot(ctx, user.Name)
	if snapshotErr != nil {
		return rc.handleProcessingError(ctx, msg, currentRetryCount, "查询用户失败: "+snapshotErr.Error())
	}
	if existingSnapshot != nil {
		log.Infof("重试创建检测到用户已存在，跳过插入: username=%s", user.Name)
		// 同步缓存，确保与数据库状态一致
		if cacheErr := rc.setUserCache(ctx, existingSnapshot, nil); cacheErr != nil {
			log.Warnf("重试创建同步缓存失败: username=%s err=%v", user.Name, cacheErr)
		}
		return nil
	}

	if time.Now().Before(nextRetryTime) {
		remaining := time.Until(nextRetryTime)
		log.Debugf("等待重试时间到达: username=%s, 剩余=%v", user.Name, remaining)
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := rc.createUserInDB(ctx, user); err != nil {
		return rc.handleProcessingError(ctx, msg, currentRetryCount, "检查用户存在性失败: "+err.Error())
	}

	rc.setUserCache(ctx, user, nil)

	return nil
}

func (rc *RetryConsumer) processRetryUpdate(ctx context.Context, msg kafka.Message) error {
	user := userMessagePool.Get().(*v1.User)
	defer func() {
		*user = v1.User{}
		userMessagePool.Put(user)
	}()

	if err := decodeUserMessage(msg.Value, user); err != nil {
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "POISON_MESSAGE_IN_RETRY: "+err.Error())
	}
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
	command := user.Command
	if strings.TrimSpace(string(command)) == "" {
		command = v1.UserUpdateCommandFull
	}
	if command == v1.UserUpdateCommandBatch {
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "BATCH_UPDATE_UNSUPPORTED_IN_RETRY_QUEUE")
	}

	currentRetryCount, nextRetryTime, lastError := rc.parseRetryHeaders(msg.Headers)

	if currentRetryCount >= rc.maxRetries {
		return rc.producer.SendToDeadLetterTopic(ctx, msg,
			fmt.Sprintf("MAX_RETRIES_EXCEEDED(%d): %s", rc.maxRetries, lastError))
	}

	if time.Now().Before(nextRetryTime) {
		time.Sleep(time.Until(nextRetryTime))
	}

	existingSnapshot, err := rc.loadUserSnapshot(ctx, user.Name)
	if err != nil {
		return rc.handleProcessingError(ctx, msg, currentRetryCount, "查询用户失败: "+err.Error())
	}
	if existingSnapshot == nil {
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "RETRY_UPDATE_TARGET_NOT_FOUND: "+user.Name)
	}
	existingCopy := *existingSnapshot
	existingSnapshot = &existingCopy

	if len(user.Extend) == 0 && len(existingSnapshot.Extend) > 0 {
		cloned := make(metav1.Extend, len(existingSnapshot.Extend))
		for k, v := range existingSnapshot.Extend {
			cloned[k] = v
		}
		user.Extend = cloned
	}

	updated := *existingSnapshot
	if strings.TrimSpace(user.InstanceID) != "" {
		updated.InstanceID = user.InstanceID
	}
	updated.ID = existingSnapshot.ID
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
			return rc.producer.SendToDeadLetterTopic(ctx, msg, "APPLY_PATCH_FAILED: "+err.Error())
		}
		// 保持未显式修改的联系信息与原快照一致，避免被零值覆盖
		if user.Patch.Email == nil {
			updated.Email = existingSnapshot.Email
		}
		if user.Patch.Phone == nil {
			updated.Phone = existingSnapshot.Phone
		}
	}
	if err := v1.EnsureExtendShadow(&updated.ObjectMeta); err != nil {
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "SERIALIZE_EXTEND_FAILED: "+err.Error())
	}

	updated.UpdatedAt = time.Now().UTC()
	existingVersion := existingSnapshot.ObjectMeta.Version
	var (
		expectedVersion *uint64
		expectedValue   uint64
	)
	if user.ExpectedVersion != nil {
		expectedValue = *user.ExpectedVersion
		expectedVersion = &expectedValue
		if existingVersion != expectedValue {
			return rc.producer.SendToDeadLetterTopic(ctx, msg,
				fmt.Sprintf("VERSION_CONFLICT: expected=%d actual=%d", expectedValue, existingVersion))
		}
	}
	if expectedVersion == nil {
		if command == v1.UserUpdateCommandPatch {
			expectedValue = existingVersion
			expectedVersion = &expectedValue
			log.Warnw("Retry PATCH 缺少版本号，使用当前版本兜底", "username", user.Name, "command", command, "current_version", existingVersion)
		} else {
			log.Warnf("Retry 用户更新缺少版本号，丢弃消息防止覆盖: username=%s command=%s current_version=%d", user.Name, command, existingVersion)
			return rc.producer.SendToDeadLetterTopic(ctx, msg,
				fmt.Sprintf("MISSING_EXPECTED_VERSION: current=%d", existingVersion))
		}
	}
	updated.ObjectMeta.Version = existingVersion + 1

	if err := rc.updateUserInDB(ctx, &updated, expectedVersion); err != nil {
		return rc.handleProcessingError(ctx, msg, currentRetryCount, "错误信息: "+err.Error())
	}

	rc.setUserCache(ctx, &updated, existingSnapshot)

	return nil
}

// internal/pkg/server/retry_consumer.go
// 修改 processRetryDelete 方法
func (rc *RetryConsumer) processRetryDelete(ctx context.Context, msg kafka.Message) error {
	deleteRequest := deleteMessagePool.Get().(*deleteMessage)
	defer func() {
		deleteRequest.Username = ""
		deleteRequest.DeletedAt = ""
		deleteMessagePool.Put(deleteRequest)
	}()

	if err := jsonCodec.Unmarshal(msg.Value, deleteRequest); err != nil {
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "POISON_MESSAGE_IN_RETRY: "+err.Error())
	}

	currentRetryCount, _, lastError := rc.parseRetryHeaders(msg.Headers)

	if currentRetryCount >= rc.maxRetries {
		return rc.producer.SendToDeadLetterTopic(ctx, msg,
			fmt.Sprintf("MAX_RETRIES_EXCEEDED(%d): %s", rc.maxRetries, lastError))
	}

	var (
		userID           uint64
		existingSnapshot *v1.User
	)
	if deleteRequest.Username != "" {
		existing, err := rc.loadUserSnapshot(ctx, deleteRequest.Username)
		if err != nil {
			return rc.handleProcessingError(ctx, msg, currentRetryCount, "查询用户失败: "+err.Error())
		}
		if existing != nil {
			userID = existing.ID
			existingSnapshot = existing
		}
	}

	if err := rc.deleteUserFromDB(ctx, deleteRequest.Username); err != nil {
		// 检查是否为可忽略的错误
		if rc.isIgnorableDeleteError(err) {
			lowerErr := strings.ToLower(err.Error())
			if strings.Contains(lowerErr, "not found") || strings.Contains(lowerErr, "does not exist") || strings.Contains(lowerErr, "record not found") {
				trace.AddRequestTag(ctx, "retry_not_found_delete", true)
				log.Debugf("删除目标尚未创建，准备重试: username=%s, retryCount=%d", deleteRequest.Username, currentRetryCount)
				return rc.prepareNextRetry(ctx, msg, currentRetryCount, "DELETE_TARGET_NOT_READY: "+err.Error())
			}
			log.Warnf("删除操作遇到可忽略错误，直接提交: username=%s, error=%v",
				deleteRequest.Username, err)
			rc.purgeUserState(ctx, deleteRequest.Username, userID, existingSnapshot)
			return nil
		}
		return rc.handleProcessingError(ctx, msg, currentRetryCount, "错误信息: "+err.Error())
	}

	rc.purgeUserState(ctx, deleteRequest.Username, userID, existingSnapshot)
	return nil
}

// 新增：判断是否为可忽略的删除错误
func (rc *RetryConsumer) isIgnorableDeleteError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	ignorableErrors := []string{
		"definer",          // DEFINER权限错误
		"does not exist",   // 数据不存在
		"not found",        // 数据不存在
		"record not found", // 数据不存在
		"duplicate entry",  // 重复数据
		"1062",             // MySQL重复键
	}

	lowerError := strings.ToLower(errStr)
	for _, ignorableError := range ignorableErrors {
		if strings.Contains(lowerError, ignorableError) {
			return true
		}
	}
	return false
}

// handleProcessingError 处理错误，决定是重试还是进入死信队列
func (rc *RetryConsumer) handleProcessingError(ctx context.Context, msg kafka.Message, currentRetryCount int, errorInfo string) error {
	// 1. 可以直接忽略的错误 - 直接提交
	if shouldIgnoreError(errorInfo) {
		log.Debugf("忽略可幂等错误，直接提交消息: %s", errorInfo)
		return nil // 直接返回nil，外层会提交偏移量
	}

	// 2. 需要人工干预的错误 - 发送到死信队列
	if shouldGoToDeadLetterImmediately(errorInfo) {
		log.Warnf("致命错误，发送到死信队列: %s", errorInfo)
		return rc.producer.SendToDeadLetterTopic(ctx, msg, "FATAL_ERROR: "+errorInfo)
	}

	// 3. 可以重试的错误 - 准备下次重试
	return rc.prepareNextRetry(ctx, msg, currentRetryCount, errorInfo)
}

// shouldIgnoreError 判断是否可以直接忽略的错误
func shouldIgnoreError(errorInfo string) bool {
	ignorableErrors := []string{
		"definer",            // DEFINER权限错误
		"does not exist",     // 数据不存在
		"not found",          // 数据不存在
		"record not found",   // 数据不存在
		"duplicate entry",    // 重复数据（幂等性）
		"user already exist", // 用户已存在
		"1062",               // MySQL重复键错误
	}

	lowerError := strings.ToLower(errorInfo)
	for _, ignorableError := range ignorableErrors {
		if strings.Contains(lowerError, ignorableError) {
			return true
		}
	}
	return false
}

// 更新 shouldGoToDeadLetterImmediately，移除已归类到忽略的错误
func shouldGoToDeadLetterImmediately(errorInfo string) bool {
	fatalErrors := []string{
		"invalid json",      // 消息格式错误
		"unmarshal error",   // 反序列化错误
		"unknown operation", // 未知操作类型
		"poison message",    // 毒药消息
		"permission denied", // 真正的权限错误（不是DEFINER）
	}

	lowerError := strings.ToLower(errorInfo)
	for _, fatalError := range fatalErrors {
		if strings.Contains(lowerError, fatalError) {
			return true
		}
	}
	return false
}

func (rc *RetryConsumer) prepareNextRetry(ctx context.Context, msg kafka.Message, currentRetryCount int, errorInfo string) error {
	// 确保 currentRetryCount 不会导致负数位移
	if currentRetryCount < 0 {
		currentRetryCount = 0
	}

	newRetryCount := currentRetryCount + 1

	// 检查是否达到最大重试次数
	if newRetryCount > rc.maxRetries {
		log.Warnf("达到最大重试次数(%d)，发送到死信队列: error=%s", rc.maxRetries, errorInfo)
		return rc.producer.SendToDeadLetterTopic(ctx, msg,
			fmt.Sprintf("MAX_RETRIES_EXCEEDED(%d): %s", rc.maxRetries, errorInfo))
	}

	// 根据错误类型决定重试策略
	var nextRetryDelay time.Duration
	var retryStrategy string

	switch {
	case isDBConnectionError(errorInfo):
		// 安全的指数退避计算
		baseDelay := rc.kafkaOptions.BaseRetryDelay
		if currentRetryCount > 0 {
			baseDelay = rc.kafkaOptions.BaseRetryDelay * time.Duration(1<<(currentRetryCount-1))
		}
		jitter := time.Duration(rand.Int63n(int64(baseDelay / 2)))
		nextRetryDelay = baseDelay + jitter
		retryStrategy = "指数退避(连接问题)"

	case isDBDeadlockError(errorInfo):
		nextRetryDelay = 100 * time.Millisecond
		retryStrategy = "快速重试(死锁)"

	case isDuplicateKeyError(errorInfo):
		nextRetryDelay = 2 * time.Second
		retryStrategy = "中等延迟(重复键)"

	case isDeleteTargetNotReadyError(errorInfo):
		nextRetryDelay = 250 * time.Millisecond
		retryStrategy = "快速重试(Delete目标尚未就绪)"

	case isTemporaryError(errorInfo):
		// 线性增长：n * baseDelay
		nextRetryDelay = time.Duration(currentRetryCount+1) * rc.kafkaOptions.BaseRetryDelay
		retryStrategy = "线性增长(临时错误)"

	default:
		// 默认指数退避 - 确保不会负数位移
		baseDelay := rc.kafkaOptions.BaseRetryDelay
		if currentRetryCount > 0 {
			baseDelay = rc.kafkaOptions.BaseRetryDelay * time.Duration(1<<(currentRetryCount-1))
		}
		nextRetryDelay = baseDelay
		retryStrategy = "指数退避(默认)"
	}

	// 应用最大延迟限制
	if nextRetryDelay > rc.kafkaOptions.MaxRetryDelay {
		nextRetryDelay = rc.kafkaOptions.MaxRetryDelay
	}

	nextRetryTime := time.Now().Add(nextRetryDelay)

	// 创建新的headers数组
	newHeaders := make([]kafka.Header, len(msg.Headers))
	copy(newHeaders, msg.Headers)

	// 更新重试相关的headers
	newHeaders = rc.updateOrAddHeader(newHeaders, HeaderRetryCount, strconv.Itoa(newRetryCount))
	newHeaders = rc.updateOrAddHeader(newHeaders, HeaderNextRetryTS, nextRetryTime.Format(time.RFC3339))
	newHeaders = rc.updateOrAddHeader(newHeaders, HeaderRetryError, errorInfo)
	newHeaders = rc.updateOrAddHeader(newHeaders, "retry-strategy", retryStrategy)

	// 创建重试消息
	retryMsg := kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: newHeaders,
		Time:    time.Now(),
	}

	log.Warnf("准备第%d次重试[%s]: delay=%v, error=%s",
		newRetryCount, retryStrategy, nextRetryDelay, errorInfo)

	return rc.producer.sendToRetryTopic(ctx, retryMsg, errorInfo)
}

// 错误类型判断函数
func isDBConnectionError(errorInfo string) bool {
	return strings.Contains(errorInfo, "connection") ||
		strings.Contains(errorInfo, "timeout") ||
		strings.Contains(errorInfo, "network") ||
		strings.Contains(errorInfo, "refused")
}

func isDBDeadlockError(errorInfo string) bool {
	return strings.Contains(errorInfo, "deadlock") ||
		strings.Contains(errorInfo, "Deadlock") ||
		strings.Contains(errorInfo, "1213") || // MySQL死锁错误码
		strings.Contains(errorInfo, "40001") // SQLServer死锁错误码
}

func isDuplicateKeyError(errorInfo string) bool {
	return strings.Contains(errorInfo, "1062") || // MySQL重复键
		strings.Contains(errorInfo, "Duplicate") ||
		strings.Contains(errorInfo, "unique constraint")
}

func isTemporaryError(errorInfo string) bool {
	return strings.Contains(errorInfo, "temporary") ||
		strings.Contains(errorInfo, "busy") ||
		strings.Contains(errorInfo, "lock")
}

func isDeleteTargetNotReadyError(errorInfo string) bool {
	lower := strings.ToLower(errorInfo)
	return strings.Contains(lower, "delete_target_not_ready") ||
		strings.Contains(lower, "delete target not ready")
}

func (rc *RetryConsumer) createUserInDB(ctx context.Context, user *v1.User) error {
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
	now := time.Now()
	user.CreatedAt = now
	user.UpdatedAt = now
	db, err := rc.ensureSQLX()
	if err != nil {
		return err
	}
	var phoneValue interface{}
	if trimmed := strings.TrimSpace(user.Phone); trimmed != "" {
		phoneValue = trimmed
	}
	version := user.ObjectMeta.Version
	if version == 0 {
		version = 1
	}
	_, execErr := db.ExecContext(ctx,
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
	if execErr != nil {
		if isDuplicateKeyError(execErr.Error()) {
			log.Warnf("重试创建检测到数据库已存在记录，跳过插入: username=%s", user.Name)
			user.ObjectMeta.Version = version
			return nil
		}
		return execErr
	}
	user.ObjectMeta.Version = version
	return nil
}

func (rc *RetryConsumer) updateUserInDB(ctx context.Context, user *v1.User, expectedVersion *uint64) error {
	user.Email = usercache.NormalizeEmail(user.Email)
	user.Phone = usercache.NormalizePhone(user.Phone)
	if strings.TrimSpace(user.ExtendShadow) == "" {
		if err := v1.EnsureExtendShadow(&user.ObjectMeta); err != nil {
			return err
		}
	}

	db, err := rc.ensureSQLX()
	if err != nil {
		return err
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
	// 强制使用 (name, version) 复合索引保证重试路径与主消费路径性能一致
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

	res, execErr := db.ExecContext(ctx, queryBuilder.String(), args...)
	if execErr != nil {
		if isDuplicateKeyError(execErr.Error()) {
			log.Warnf("重试更新命中唯一约束冲突，忽略并提交偏移: username=%s err=%v", user.Name, execErr)
			return nil
		}
		return execErr
	}
	if expectedVersion != nil {
		affected, affErr := res.RowsAffected()
		if affErr != nil {
			return affErr
		}
		if affected == 0 {
			return errors.New("用户数据版本冲突")
		}
	}
	return nil
}

func (rc *RetryConsumer) deleteUserFromDB(ctx context.Context, username string) error {
	db, err := rc.ensureSQLX()
	if err != nil {
		return err
	}
	_, execErr := db.ExecContext(ctx, "DELETE FROM `user` WHERE name = ?", username)
	return execErr
}

func (rc *RetryConsumer) setUserCache(ctx context.Context, user *v1.User, previous *v1.User) error {
	var (
		startTime    time.Time
		operationErr error
		wroteCache   bool
	)
	defer func() {
		if wroteCache {
			metrics.RecordRedisOperation("set", time.Since(startTime).Seconds(), operationErr)
		}
	}()

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
				log.L(ctx).Errorw("用户数据序列化失败", "username", user.Name, "error", err)
				return operationErr
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
	contactChanged := previous == nil || prevEmail != newEmail || prevPhone != newPhone

	if contactChanged {
		if previous != nil {
			rc.evictContactCaches(ctx, previous, user)
		}
		pipelineItems = append(pipelineItems, buildContactCacheItems(user)...)
	}

	if len(pipelineItems) > 0 {
		startTime = time.Now()
		wroteCache = true
		if len(pipelineItems) == 1 {
			item := pipelineItems[0]
			operationErr = rc.redis.SetKey(ctx, item.Key, item.Value, item.TTL)
		} else {
			operationErr = rc.redis.BatchSet(ctx, pipelineItems)
		}
		if operationErr != nil {
			log.L(ctx).Errorw("缓存写入失败", "username", user.Name, "error", operationErr)
			return operationErr
		}
	}
	return operationErr
}

func (rc *RetryConsumer) deleteUserCache(ctx context.Context, username string) error {
	cacheKey := usercache.UserKey(username)
	if cacheKey == "" {
		return nil
	}
	_, err := rc.redis.DeleteKey(ctx, cacheKey)
	return err
}

func (rc *RetryConsumer) clearPendingCreateMarker(ctx context.Context, username string) error {
	if rc.redis == nil {
		return nil
	}
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return nil
	}
	key := usercache.PendingCreateKey(trimmed)
	if key == "" {
		return nil
	}
	_, err := rc.redis.DeleteKey(ctx, key)
	if err == redis.Nil {
		return nil
	}
	return err
}

func (rc *RetryConsumer) purgeUserState(ctx context.Context, username string, userID uint64, snapshot *v1.User) {
	if err := rc.clearPendingCreateMarker(ctx, username); err != nil {
		log.Warnw("重试流程清理pending标记失败", "username", username, "error", err)
	}
	if err := rc.deleteUserCache(ctx, username); err != nil {
		log.Errorw("重试缓存删除失败", "username", username, "error", err)
	} else {
		log.Debugf("重试缓存删除成功: username=%s", username)
	}

	if snapshot != nil {
		rc.evictContactCaches(ctx, snapshot, nil)
	}

	if userID == 0 {
		return
	}

	if err := cleanupUserSessions(ctx, rc.redis, userID); err != nil {
		log.Errorw("重试刷新令牌清理失败", "username", username, "userID", userID, "error", err)
		return
	}
	log.Debugf("重试刷新令牌清理成功: username=%s userID=%d", username, userID)
}

func (rc *RetryConsumer) evictContactCaches(ctx context.Context, previous *v1.User, current *v1.User) {
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
		rc.removeCacheKey(ctx, keysToEvict[0])
	default:
		if err := rc.redis.BatchDelete(ctx, keysToEvict); err != nil {
			log.Warnw("重试消费者批量删除联系缓存失败，回退逐个删除", "keys", keysToEvict, "error", err)
			for _, key := range keysToEvict {
				rc.removeCacheKey(ctx, key)
			}
		}
	}
}

func (rc *RetryConsumer) writeContactCaches(ctx context.Context, user *v1.User) {
	if user == nil {
		return
	}
	items := buildContactCacheItems(user)
	if len(items) == 0 {
		return
	}
	if err := rc.redis.BatchSet(ctx, items); err != nil {
		log.Warnf("重试消费者批量写入联系缓存失败: username=%s err=%v", user.Name, err)
	}
}

func (rc *RetryConsumer) removeCacheKey(ctx context.Context, cacheKey string) {
	if cacheKey == "" {
		return
	}
	if _, err := rc.redis.DeleteKey(ctx, cacheKey); err != nil {
		log.Warnf("重试消费者缓存删除失败: key=%s err=%v", cacheKey, err)
	}
}

// 若key为空字符串，直接panic（空Key无业务意义，会导致头逻辑混乱）
func (rc *RetryConsumer) updateOrAddHeader(headers []kafka.Header, key, value string) []kafka.Header {
	// 1. 基础校验：阻断空Key输入（避免无效头）
	if key == "" {
		panic("kafka header key cannot be empty string")
	}

	// 2. 统一目标Key为小写（用于忽略大小写匹配，不改变原始Key的展示）
	targetKeyLower := strings.ToLower(key)

	// 3. 初始化新切片+标记：newHeaders存最终结果，found标记是否已保留一个目标头
	var newHeaders []kafka.Header
	foundTargetHeader := false

	// 4. 遍历原始切片：逐个处理每个头，筛选重复目标头
	for _, header := range headers {
		// 4.1 判断当前头是否为目标头（忽略大小写）
		currentHeaderKeyLower := strings.ToLower(header.Key)
		if currentHeaderKeyLower == targetKeyLower {
			// 4.2 若未保留过目标头：更新其Value，加入新切片，标记已保留
			if !foundTargetHeader {
				// 保留原始Key的大小写（仅更新Value），避免修改用户输入的Key格式
				updatedHeader := kafka.Header{
					Key:   header.Key,    // 如原Key是"Retry-Error"，仍保留该格式
					Value: []byte(value), // 写入最新Value
				}
				newHeaders = append(newHeaders, updatedHeader)
				foundTargetHeader = true // 标记已保留，后续重复头不再处理
			}
			// 4.3 若已保留过目标头：直接跳过，不加入新切片（删除重复）
			continue
		}

		// 4.4 非目标头：直接加入新切片（保持原有逻辑不变）
		newHeaders = append(newHeaders, header)
	}

	// 5. 若遍历完未找到任何目标头：新增一个（保留用户输入的原始Key大小写）
	if !foundTargetHeader {
		newHeaders = append(newHeaders, kafka.Header{
			Key:   key, // 如用户传"Retry-Error"，新增时就用该Key，不强制小写
			Value: []byte(value),
		})
	}

	return newHeaders
}
