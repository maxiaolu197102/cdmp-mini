package server

import "time"

// 登录失败限制配置默认值
const (
	DefaultMaxLoginFails = 6
)

var (
	DefaultLoginFailReset = 15 * time.Minute
)

// 系统常量
const (
	// APIServerAudience defines the value of jwt audience field.
	APIServerAudience = "https://github.com/maxiaolu1981/cretem"

	// Issuer - 标识令牌的"签发系统"（系统视角）
	APIServerIssuer = "https://github.com/maxiaolu1981/cretem"
	// Realm - 标识受保护的"资源领域"（用户视角）
	APIServerRealm = "github.com/maxiaolu1981/cretem"
)

// 常量定义：重试和死信Topic的命名后缀
const (
	RetryTopicSuffix      = ".retry"
	DeadLetterTopicSuffix = ".deadletter"
)

const (
	ConsumerGroupPrefix = "user-service-prod"
	// Topic 定义
	UserCreateTopic     = "user.create.v1"
	UserUpdateTopic     = "user.update.v1"
	UserDeleteTopic     = "user.delete.v1"
	UserRetryTopic      = "user.retry.v1"
	UserDeadLetterTopic = "user.deadletter.v1"

	// Header Keys
	HeaderOperation         = "operation"
	HeaderOriginalTimestamp = "original-timestamp"
	HeaderRetryCount        = "retry-count"
	HeaderRetryError        = "retry-error"
	HeaderNextRetryTS       = "next-retry-ts"
	HeaderDeadLetterReason  = "deadletter-reason"
	HeaderDeadLetterTS      = "deadletter-timestamp"
	HeaderTraceID           = "trace-id"

	// Operation Types
	OperationCreate = "create"
	OperationUpdate = "update"
	OperationDelete = "delete"

	// 重试配置
	// MaxRetryCount  = 5
	// BaseRetryDelay = 10 * time.Second
	// MaxRetryDelay  = 5 * time.Minute

	// Consumer 配置
	MainConsumerWorkers  = 3
	RetryConsumerWorkers = 3
)
