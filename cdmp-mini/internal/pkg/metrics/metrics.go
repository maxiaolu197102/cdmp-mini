/*
这份 Go 代码基于 prometheus/client_golang 库，定义了Kafka（生产者 / 消费者）、数据库、HTTP、缓存, redis五大核心场景的监控指标（Counter/Gauge/Histogram），并提供了辅助函数用于简化业务代码中指标的上报操作。
所有指标通过手动初始化后注册到 Prometheus 默认注册表（需在 init 函数中调用 prometheus.MustRegister）；函数的核心作用是封装指标的标签赋值、计数 / 观测逻辑，让业务代码无需关注 Prometheus 指标的底层操作，只需传入业务参数即可完成监控上报。
*/
package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/errors/category"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// -------------------------- 1. 先声明所有指标变量（仅声明，不初始化）--------------------------
const defaultSlowQueryThreshold = 200 * time.Millisecond

var (
	// 生产者指标
	ProducerAttempts      *prometheus.CounterVec
	ProducerSuccess       *prometheus.CounterVec
	ProducerFailures      *prometheus.CounterVec
	ProducerRetries       *prometheus.CounterVec
	DeadLetterMessages    *prometheus.CounterVec
	MessageProcessingTime *prometheus.HistogramVec
	ProducerWALBacklog    prometheus.Gauge
	ProducerWALMaxAge     prometheus.Gauge

	// 生产者当前未完成发送数（in-flight）
	ProducerInFlightCurrent prometheus.Gauge
	// 写限流触发计数
	WriteLimiterTotal *prometheus.CounterVec
	// initialization moved to init()
	// ProducerDeliveryLatency 记录生产者消息从入队到Broker确认的耗时
	ProducerDeliveryLatency *prometheus.HistogramVec
	// ProducerEnqueueWaitLatency 记录生产者消息在入队前的排队等待时间
	ProducerEnqueueWaitLatency *prometheus.HistogramVec
	// ProducerBrokerAckLatency 记录Broker确认耗时
	ProducerBrokerAckLatency *prometheus.HistogramVec

	// 业务处理指标
	BusinessProcessingTime *prometheus.HistogramVec
	BusinessSuccess        *prometheus.CounterVec
	BusinessFailures       *prometheus.CounterVec

	// 新增：业务吞吐量指标
	BusinessOperationsTotal *prometheus.CounterVec // 业务操作总数
	BusinessOperationsRate  *prometheus.GaugeVec   // 业务操作速率（QPS）
	BusinessInProgress      *prometheus.GaugeVec   // 当前处理中的业务数
	BusinessThroughputStats *prometheus.SummaryVec // 业务吞吐量统计
	BusinessErrorRate       *prometheus.GaugeVec   // 业务错误率

	TraceSpanDuration      *prometheus.HistogramVec
	TraceOperationDuration *prometheus.HistogramVec

	UserCreateStepDuration            *prometheus.HistogramVec
	UserCreateSlowStepsTotal          *prometheus.CounterVec
	UserCreateStepTotal               *prometheus.CounterVec
	UserCreateRequestsTotal           *prometheus.CounterVec
	UserCreateDegradeTotal            *prometheus.CounterVec
	UserCreateMessageTotal            *prometheus.CounterVec
	UserContactPlaceholderSetDuration *prometheus.HistogramVec

	AuditEventsTotal   *prometheus.CounterVec // 审计事件计数
	AuditEventFailures *prometheus.CounterVec // 审计失败计数
)

var (
	// Kafka消费者指标
	ConsumerMessagesReceived   *prometheus.CounterVec
	ConsumerMessagesProcessed  *prometheus.CounterVec
	ConsumerProcessingErrors   *prometheus.CounterVec
	ConsumerProcessingTime     *prometheus.HistogramVec
	ConsumerRetryMessages      *prometheus.CounterVec
	ConsumerDeadLetterMessages *prometheus.CounterVec
	ConsumerLag                *prometheus.GaugeVec
	// Commit success / failure metrics (include partition)
	ConsumerCommitSuccess  *prometheus.CounterVec
	ConsumerCommitFailures *prometheus.CounterVec

	// 新增：topic 分区数量、组实例数量与无主分区的启发式计数
	ConsumerTopicPartitions   *prometheus.GaugeVec
	ConsumerGroupInstances    *prometheus.GaugeVec
	ConsumerPartitionsNoOwner *prometheus.GaugeVec
	ConsumerMessageAgeSeconds *prometheus.HistogramVec

	KafkaBrokerHealth      *prometheus.GaugeVec
	KafkaBrokerLatency     *prometheus.HistogramVec
	KafkaClusterHealth     *prometheus.GaugeVec
	KafkaClusterBrokers    *prometheus.GaugeVec
	KafkaTopicStatus       *prometheus.GaugeVec
	KafkaHeartbeatFailures *prometheus.CounterVec

	// 数据库操作指标
	DatabaseQueryDuration *prometheus.HistogramVec
	DatabaseQueryErrors   *prometheus.CounterVec
	DatabaseSlowQueries   *prometheus.CounterVec

	DatabasePoolOpenConnections     *prometheus.GaugeVec
	DatabasePoolInUse               *prometheus.GaugeVec
	DatabasePoolIdle                *prometheus.GaugeVec
	DatabasePoolWaitCount           *prometheus.GaugeVec
	DatabasePoolWaitDurationSeconds *prometheus.GaugeVec
	DatabasePoolMaxOpenConnections  *prometheus.GaugeVec
	DatabaseHeartbeatStatus         *prometheus.GaugeVec
	DatabaseHeartbeatLatency        *prometheus.HistogramVec
	DatabaseReplicaStatus           *prometheus.GaugeVec
)

var (
	// Pending lease / coordinator 指标
	PendingLeaseActiveGauge         *prometheus.GaugeVec
	PendingLeaseQueueDepth          *prometheus.GaugeVec
	PendingLeaseQueueDepthSample    *prometheus.HistogramVec
	PendingLeaseBackpressureLevel   *prometheus.GaugeVec
	PendingLeaseEvents              *prometheus.CounterVec
	PendingLeaseFallbackTotal       *prometheus.CounterVec
	PendingLeaseHoldDuration        *prometheus.HistogramVec
	PendingLeaseCalibrationDuration *prometheus.HistogramVec
	PendingCoordinatorHealth        *prometheus.GaugeVec
	PendingLeaseLuaAttempts         *prometheus.CounterVec

	// Pending consumer 观测指标
	PendingConsumerQueueDepth                *prometheus.GaugeVec
	PendingConsumerRedisLatency              *prometheus.HistogramVec
	PendingConsumerDequeueDuration           *prometheus.HistogramVec
	PendingBackpressureDelaySeconds          *prometheus.HistogramVec
	PendingBackpressureDelayTriggerRate      *prometheus.CounterVec
	PendingBackpressureDelayCancelRate       *prometheus.CounterVec
	PendingBackpressureLeadTimeSeconds       *prometheus.HistogramVec
	PendingBackpressureDelayCancelledSeconds *prometheus.HistogramVec
	PendingBackpressureDeadlineDecisions     *prometheus.CounterVec
)

var (
	OperationQueueReadyDepth         *prometheus.GaugeVec
	OperationQueueScheduledDepth     *prometheus.GaugeVec
	OperationQueueInflightGauge      *prometheus.GaugeVec
	OperationQueueFallbackGauge      *prometheus.GaugeVec
	OperationWorkerIterations        *prometheus.CounterVec
	OperationWorkerIterationDuration *prometheus.HistogramVec
	OperationCompensationTotal       *prometheus.CounterVec
	OperationCompensationDuration    *prometheus.HistogramVec

	OperationModeDecisions      *prometheus.CounterVec
	OperationModeCurrent        *prometheus.GaugeVec
	OperationModeRolloutPercent *prometheus.GaugeVec
	OperationModeAllowlistSize  *prometheus.GaugeVec
	OperationModeBlocklistSize  *prometheus.GaugeVec
)

// Redis操作指标
var (
	RedisOperations              *prometheus.CounterVec
	RedisOperationDuration       *prometheus.HistogramVec
	RedisErrors                  *prometheus.CounterVec
	RedisCacheSize               *prometheus.GaugeVec
	StorageSetNXDuration         *prometheus.HistogramVec
	RedisCommandStageDuration    *prometheus.HistogramVec
	RedisPoolTotalConnections    *prometheus.GaugeVec
	RedisPoolInUseConnections    *prometheus.GaugeVec
	RedisPoolIdleConnections     *prometheus.GaugeVec
	RedisPoolWaitDurationSeconds *prometheus.GaugeVec
	RedisPoolWaitCountTotal      *prometheus.GaugeVec
	RedisPoolHitsTotal           *prometheus.GaugeVec
	RedisPoolMissesTotal         *prometheus.GaugeVec
	RedisPoolTimeoutsTotal       *prometheus.GaugeVec
)

// Redis集群监控指标
var (
	// Redis集群节点状态指标
	RedisClusterNodesTotal prometheus.Gauge     // 集群节点总数
	RedisClusterNodesUp    prometheus.Gauge     // 正常节点数量
	RedisClusterNodesDown  prometheus.Gauge     // 异常节点数量
	RedisClusterState      *prometheus.GaugeVec // 集群状态（0=异常, 1=正常）

	// Redis集群槽位分配指标
	RedisClusterSlotsAssigned prometheus.Gauge // 已分配的槽位数量
	RedisClusterSlotsOk       prometheus.Gauge // 正常的槽位数量
	RedisClusterSlotsPFail    prometheus.Gauge // 可能失败的槽位数量
	RedisClusterSlotsFail     prometheus.Gauge // 失败的槽位数量

	// Redis集群节点详细指标
	RedisClusterNodeInfo        *prometheus.GaugeVec // 节点基本信息
	RedisClusterNodeMemory      *prometheus.GaugeVec // 节点内存使用
	RedisClusterNodeCPU         *prometheus.GaugeVec // 节点CPU使用
	RedisClusterNodeConnections *prometheus.GaugeVec // 节点连接数
	RedisClusterNodeKeys        *prometheus.GaugeVec // 节点键数量
	RedisClusterNodeOpsPerSec   *prometheus.GaugeVec // 节点每秒操作数

	// Redis集群性能指标
	RedisClusterHitRate            prometheus.Gauge       // 集群整体命中率
	RedisClusterMemoryUsage        prometheus.Gauge       // 集群总内存使用
	RedisClusterMemoryUsagePercent prometheus.Gauge       // 集群内存使用百分比
	RedisClusterTotalCommands      *prometheus.CounterVec // 集群命令统计
	RedisClusterKeyspaceHits       prometheus.Counter     // 集群键空间命中
	RedisClusterKeyspaceMisses     prometheus.Counter     // 集群键空间未命中

	// Redis集群网络指标
	RedisClusterNetworkIO      *prometheus.GaugeVec // 集群网络IO
	RedisClusterReplicationLag *prometheus.GaugeVec // 主从复制延迟

	// Redis集群故障相关指标
	RedisClusterFailoverCount  prometheus.Counter   // 故障转移次数
	RedisClusterMigrationCount prometheus.Counter   // 槽位迁移次数
	RedisClusterHealthCheck    *prometheus.GaugeVec // 健康检查状态
)

var (
	//可能专门记录业务逻辑处理时间（区别于网络传输）
	HTTPResponseTime *prometheus.HistogramVec
	//并发请求数
	HTTPRequestsInFlight prometheus.Gauge // 修正：去掉指针
	//监控请求体数据量
	HTTPRequestSize *prometheus.HistogramVec
	// 响应体大小分布
	HTTPResponseSize *prometheus.HistogramVec
	//统计接收到的总请求数量
	HTTPRequestsTotal *prometheus.CounterVec
	//记录请求从接收到响应的完整时间
	HTTPRequestDuration *prometheus.HistogramVec
	//按不同维度监控进行中的请求
	HTTPRequestsInProgress *prometheus.GaugeVec
	CacheRequests          *prometheus.CounterVec
	// 统计超过阈值的慢请求
	SlowHTTPRequests *prometheus.CounterVec
	HTTPErrors       *prometheus.CounterVec
)

var (
	// 缓存命中指标
	CacheHits *prometheus.CounterVec
	// 数据库查询指标
	DBQueries *prometheus.CounterVec
	// RequestsMerged 记录被singleflight合并的请求数量
	RequestsMerged *prometheus.CounterVec
	// CacheErrors 记录缓存相关错误
	CacheErrors *prometheus.CounterVec
	// 空值缓存数量（使用Gauge）
	CacheNullValuesCount prometheus.Gauge // 修正：去掉指针
	// 空值缓存操作统计
	CacheNullValueOperations *prometheus.CounterVec
	// UserProtectionEvents 统计用户防护动作触发次数，例如负缓存、黑名单
	UserProtectionEvents *prometheus.CounterVec
	// 用户缓存联系人写入批处理指标
	UserCacheContactBatchDuration *prometheus.HistogramVec
	UserCacheContactBatchSize     *prometheus.HistogramVec
	UserCacheContactBatchRetries  *prometheus.CounterVec
	UserCacheRefreshPhaseDuration *prometheus.HistogramVec
	UserCacheRefreshItems         *prometheus.HistogramVec
	UserContactPlaceholderEvents  *prometheus.CounterVec
)

// -------------------------- 2. 在 init 函数中初始化指标 + 手动注册 --------------------------
func init() {
	// -------------------------- 初始化：生产者指标 --------------------------
	ProducerAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_attempts_total",
			Help: "生产者发送消息的总尝试次数（包括首次发送和重试）",
		},
		[]string{"topic", "operation"},
	)

	ProducerSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_success_total",
			Help: "Total number of successfully sent Kafka messages",
		},
		[]string{"topic", "operation"},
	)

	ProducerFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_failures_total",
			Help: "Total number of failed Kafka message sending attempts",
		},
		[]string{"topic", "operation", "error_type"},
	)

	ProducerRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_retries_total",
			Help: "Total number of message retries",
		},
		[]string{"topic", "operation"},
	)

	DeadLetterMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_dead_letter_messages_total",
			Help: "Total number of messages sent to dead letter queue",
		},
		[]string{"topic", "operation"},
	)

	MessageProcessingTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_message_processing_seconds",
			Help:    "Time taken to process messages",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"topic", "operation", "status"},
	)

	ProducerWALBacklog = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_producer_wal_backlog",
			Help: "Current number of messages waiting in the producer WAL queue",
		},
	)

	ProducerWALMaxAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_producer_wal_oldest_age_seconds",
			Help: "Age in seconds of the oldest message within the producer WAL queue",
		},
	)

	// -------------------------- 初始化：业务处理指标 --------------------------
	BusinessProcessingTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "business_processing_seconds",
			Help:    "Time taken for business logic processing",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"service", "operation"}, // 统一标签
	)

	BusinessSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "business_operations_success_total",
			Help: "Total number of successful business operations",
		},
		[]string{"service", "operation", "type"}, // 增加service标签
	)

	BusinessFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "business_operations_failures_total",
			Help: "Total number of failed business operations",
		},
		[]string{"service", "operation", "error_type"}, // 增加service标签
	)

	BusinessOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "business_operations_total",
			Help: "Total number of business operations",
		},
		[]string{"service", "operation", "source"}, // ✅ 正确
	)

	BusinessOperationsRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "business_operations_rate_per_second",
			Help: "Business operations processing rate per second",
		},
		[]string{"service", "operation"}, // ✅ 正确
	)

	BusinessInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "business_operations_in_progress",
			Help: "Number of business operations currently in progress",
		},
		[]string{"service", "operation"}, // ✅ 正确
	)

	BusinessThroughputStats = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "business_throughput_stats_seconds",
			Help:       "Business throughput statistics in seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"service", "operation"}, // ✅ 正确
	)

	BusinessErrorRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "business_error_rate",
			Help: "Business operation error rate percentage",
		},
		[]string{"service", "operation"}, // ✅ 正确
	)

	TraceSpanDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "trace_span_duration_seconds",
			Help:    "Duration of individual trace spans",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"component", "operation", "status"},
	)

	TraceOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "trace_operation_duration_seconds",
			Help:    "Total duration for traced operations",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"operation", "phase", "status"},
	)

	UserCreateStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_create_step_duration_seconds",
			Help:    "Duration of individual steps in user creation flow",
			Buckets: []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2},
		},
		[]string{"step", "field", "account_type"},
	)

	UserCreateSlowStepsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_slow_steps_total",
			Help: "Total number of user create sub-steps exceeding the slow threshold",
		},
		[]string{"step", "field", "account_type"},
	)

	UserCreateStepTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_step_total",
			Help: "Total number of user create steps grouped by outcome",
		},
		[]string{"step", "field", "account_type", "outcome"},
	)

	UserCreateRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_requests_total",
			Help: "Total number of user create requests grouped by execution mode and outcome",
		},
		[]string{"mode", "account_type", "outcome"},
	)

	UserCreateDegradeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_degrade_total",
			Help: "Total number of user create degrade events grouped by reason",
		},
		[]string{"reason", "account_type"},
	)

	UserCreateMessageTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_message_total",
			Help: "Total number of user create message dispatch attempts grouped by account type and result",
		},
		[]string{"account_type", "result"},
	)

	UserContactPlaceholderSetDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_contact_placeholder_set_duration_seconds",
			Help:    "Duration distribution for contact placeholder SetNX operations",
			Buckets: []float64{0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1.0},
		},
		[]string{"step", "field", "status"},
	)

	UserContactPlaceholderEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_contact_placeholder_events_total",
			Help: "Total number of contact placeholder operations grouped by step and result",
		},
		[]string{"step", "field", "result"},
	)

	AuditEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "audit_events_total",
			Help: "Total number of audit events emitted",
		},
		[]string{"action", "resource_type", "outcome"},
	)

	AuditEventFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "audit_event_failures_total",
			Help: "Total number of audit events with non-success outcome",
		},
		[]string{"action", "resource_type"},
	)

	// 初始化新增指标
	ProducerInFlightCurrent = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_producer_inflight_current",
			Help: "Current number of in-flight synchronous producer sends",
		},
	)

	WriteLimiterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "write_rate_limiter_total",
			Help: "Total number of requests blocked by write rate limiter",
		},
		[]string{"path", "reason"},
	)

	ProducerDeliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_producer_delivery_latency_seconds",
			Help:    "Time from enqueue to broker acknowledgment for Kafka producer messages",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"topic", "operation", "result"},
	)

	ProducerEnqueueWaitLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_producer_enqueue_wait_seconds",
			Help:    "Time a Kafka producer message waits before entering the async channel",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"topic", "operation", "result"},
	)

	ProducerBrokerAckLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_producer_broker_ack_seconds",
			Help:    "Time from async enqueue to broker acknowledgment for Kafka producer messages",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"topic", "operation", "result"},
	)

	// -------------------------- 初始化：Kafka消费者指标 --------------------------
	ConsumerMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_messages_received_total",
			Help: "Total number of messages received by consumer",
		},
		[]string{"topic", "group", "operation"},
	)

	ConsumerMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_messages_processed_total",
			Help: "Total number of messages successfully processed",
		},
		[]string{"topic", "group", "operation"},
	)

	ConsumerProcessingErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_processing_errors_total",
			Help: "Total number of message processing errors",
		},
		[]string{"topic", "group", "operation", "error_type"},
	)

	ConsumerProcessingTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_consumer_processing_seconds",
			Help:    "Time taken to process messages by consumer",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
		},
		[]string{"topic", "group", "operation", "status"},
	)

	ConsumerRetryMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_retry_messages_total",
			Help: "Total number of messages sent to retry topic",
		},
		[]string{"topic", "group", "operation", "error_type"},
	)

	ConsumerDeadLetterMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_dead_letter_messages_total",
			Help: "Total number of messages sent to dead letter queue by consumer",
		},
		[]string{"topic", "group", "operation", "error_type"},
	)

	ConsumerCommitSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_commit_success_total",
			Help: "Total number of successful consumer commit operations",
		},
		[]string{"topic", "group", "partition"},
	)

	ConsumerCommitFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_commit_failures_total",
			Help: "Total number of failed consumer commit operations",
		},
		[]string{"topic", "group", "partition"},
	)

	ConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current consumer lag (estimated)",
		},
		[]string{"topic", "group"},
	)

	// 新增：topic 分区与消费组实例相关指标
	ConsumerTopicPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_partitions",
			Help: "Number of partitions for a Kafka topic",
		},
		[]string{"topic"},
	)

	ConsumerGroupInstances = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_group_instances",
			Help: "Number of active consumer instances in the consumer group",
		},
		[]string{"group"},
	)

	ConsumerPartitionsNoOwner = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_partitions_without_consumer",
			Help: "Heuristic count of partitions without active consumers for a topic/group",
		},
		[]string{"topic", "group"},
	)

	ConsumerMessageAgeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_consumer_message_age_seconds",
			Help:    "Age of Kafka messages when picked up by consumer workers",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"component", "topic", "group"},
	)

	KafkaBrokerHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_broker_health_status",
			Help: "Kafka broker heartbeat status (1=healthy, 0=unhealthy)",
		},
		[]string{"cluster", "broker"},
	)

	KafkaBrokerLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_broker_heartbeat_latency_seconds",
			Help:    "Latency of Kafka broker heartbeat probes",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"cluster", "broker"},
	)

	KafkaClusterHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_cluster_health_status",
			Help: "Kafka cluster metadata heartbeat status (1=healthy, 0=unhealthy)",
		},
		[]string{"cluster"},
	)

	KafkaClusterBrokers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_cluster_broker_count",
			Help: "Number of brokers discovered via Kafka metadata",
		},
		[]string{"cluster"},
	)

	KafkaTopicStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_health_status",
			Help: "Kafka topic availability observed during heartbeat checks",
		},
		[]string{"cluster", "topic"},
	)

	KafkaHeartbeatFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_heartbeat_failures_total",
			Help: "Total number of Kafka heartbeat probe failures grouped by reason",
		},
		[]string{"cluster", "reason"},
	)

	PendingLeaseActiveGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_lease_active_total",
			Help: "Number of active pending leases tracked by the coordinator",
		},
		[]string{"component"},
	)

	PendingLeaseQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_lease_queue_depth",
			Help: "Current queue depth observed by the pending lease coordinator",
		},
		[]string{"component"},
	)

	PendingLeaseQueueDepthSample = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_lease_queue_depth_sample",
			Help:    "Sampled queue depth distribution captured by the pending lease coordinator",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000},
		},
		[]string{"component"},
	)

	PendingLeaseBackpressureLevel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_lease_backpressure_level",
			Help: "Current backpressure level derived from pending lease queue depth (0=none,1=elevated,2=severe)",
		},
		[]string{"component"},
	)

	PendingLeaseEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_lease_events_total",
			Help: "Total number of pending lease lifecycle events",
		},
		[]string{"component", "event"},
	)

	PendingLeaseFallbackTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_lease_fallback_total",
			Help: "Total number of pending lease operations served via degraded fallback",
		},
		[]string{"component", "operation", "reason"},
	)

	PendingLeaseHoldDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_lease_hold_duration_seconds",
			Help:    "Time between lease acquisition and release",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"component", "result"},
	)

	PendingLeaseCalibrationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_lease_calibration_duration_seconds",
			Help:    "Runtime of full pending lease calibration rounds grouped by component and result",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 40, 60, 120},
		},
		[]string{"component", "result"},
	)

	PendingCoordinatorHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_coordinator_health",
			Help: "Health of the pending coordinator backend (1=healthy,0=unhealthy)",
		},
		[]string{"component", "backend"},
	)

	PendingLeaseLuaAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_lease_lua_attempts_total",
			Help: "Total pending lease Lua acquire attempts grouped by outcome",
		},
		[]string{"component", "outcome"},
	)

	PendingConsumerQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_consumer_queue_depth",
			Help: "Depth samples reported by pending queue consumers after coordinator probes",
		},
		[]string{"component"},
	)

	PendingConsumerRedisLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_consumer_redis_latency_seconds",
			Help:    "Redis round-trip latency observed by pending queue consumers",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		},
		[]string{"component", "operation", "status"},
	)

	PendingConsumerDequeueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_consumer_dequeue_duration_seconds",
			Help:    "End-to-end dequeue duration for pending queue consumers",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"component", "outcome"},
	)

	PendingBackpressureDelaySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_backpressure_delay_seconds",
			Help:    "Scheduled backpressure delays applied before pending operations proceed",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"component", "level"},
	)

	PendingBackpressureDelayTriggerRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_backpressure_delay_trigger_rate",
			Help: "Count of times a pending backpressure delay is injected (derive rate() in Prometheus)",
		},
		[]string{"component", "level"},
	)

	PendingBackpressureDelayCancelRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_backpressure_delay_cancel_rate",
			Help: "Count of pending backpressure delay waits canceled before completion",
		},
		[]string{"component", "level"},
	)

	PendingBackpressureLeadTimeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_backpressure_lead_time_seconds",
			Help:    "Elapsed request time before a pending backpressure delay is applied",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"component", "level"},
	)

	PendingBackpressureDelayCancelledSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pending_backpressure_delay_cancelled_seconds",
			Help:    "Actual time spent waiting before a pending backpressure delay was canceled",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"component", "level"},
	)

	PendingBackpressureDeadlineDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pending_backpressure_deadline_decisions_total",
			Help: "Count of times pending backpressure delays were truncated or skipped due to context deadlines",
		},
		[]string{"component", "level", "action"},
	)

	OperationQueueReadyDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_queue_ready_depth",
			Help: "Current length of the ready queue for async operations",
		},
		[]string{"resource"},
	)

	OperationQueueScheduledDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_queue_scheduled_depth",
			Help: "Number of operations scheduled for future retry",
		},
		[]string{"resource"},
	)

	OperationQueueInflightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_queue_inflight_total",
			Help: "Number of operations currently marked in-flight",
		},
		[]string{"resource"},
	)

	OperationQueueFallbackGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_queue_fallback_active",
			Help: "Indicator flag showing if an in-memory fallback queue is active",
		},
		[]string{"resource"},
	)

	OperationWorkerIterations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operation_worker_iterations_total",
			Help: "Count of worker loop iterations grouped by outcome",
		},
		[]string{"resource", "outcome"},
	)

	OperationWorkerIterationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operation_worker_iteration_seconds",
			Help:    "Duration spent inside a single worker iteration",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"resource", "outcome"},
	)

	OperationCompensationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operation_compensation_total",
			Help: "Total number of compensation attempts grouped by outcome",
		},
		[]string{"resource", "outcome"},
	)

	OperationCompensationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operation_compensation_duration_seconds",
			Help:    "Duration of compensation handlers grouped by outcome",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"resource", "outcome"},
	)

	OperationModeDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operation_mode_decisions_total",
			Help: "Total number of operation mode decisions grouped by resource kind and outcome",
		},
		[]string{"component", "kind", "mode"},
	)

	OperationModeCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_mode_current",
			Help: "Current operation mode flag per component and mode",
		},
		[]string{"component", "mode"},
	)

	OperationModeRolloutPercent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_mode_rollout_percent",
			Help: "Rollout percentage for operation mode per component",
		},
		[]string{"component"},
	)

	OperationModeAllowlistSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_mode_allowlist_size",
			Help: "Number of allowlisted subjects for queue mode",
		},
		[]string{"component"},
	)

	OperationModeBlocklistSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_mode_blocklist_size",
			Help: "Number of blocklisted subjects forcing sync mode",
		},
		[]string{"component"},
	)

	// -------------------------- 初始化：数据库操作指标 --------------------------
	DatabaseQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_query_duration_seconds",
			Help:    "Time taken for database queries grouped by component and operation",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"component", "operation", "status"},
	)

	DatabaseQueryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_query_errors_total",
			Help: "Total number of database query errors grouped by component and operation",
		},
		[]string{"component", "operation", "error_type"},
	)

	DatabaseSlowQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_slow_queries_total",
			Help: "Number of database queries exceeding the configured slow threshold",
		},
		[]string{"component", "operation"},
	)

	DatabasePoolOpenConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_open_connections",
			Help: "Number of open connections held by the database connection pool",
		},
		[]string{"component", "role", "index"},
	)

	DatabasePoolInUse = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_in_use_connections",
			Help: "Number of active (in use) connections in the database connection pool",
		},
		[]string{"component", "role", "index"},
	)

	DatabasePoolIdle = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_idle_connections",
			Help: "Number of idle connections in the database connection pool",
		},
		[]string{"component", "role", "index"},
	)

	DatabasePoolWaitCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_wait_count",
			Help: "Total number of waits for a database connection",
		},
		[]string{"component", "role", "index"},
	)

	DatabasePoolWaitDurationSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_wait_duration_seconds",
			Help: "Total time blocked waiting for a database connection (seconds)",
		},
		[]string{"component", "role", "index"},
	)

	DatabasePoolMaxOpenConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_max_open_connections",
			Help: "Maximum number of open connections allowed in the pool",
		},
		[]string{"component", "role", "index"},
	)

	DatabaseHeartbeatStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_heartbeat_status",
			Help: "Database heartbeat status (1=healthy, 0=unhealthy)",
		},
		[]string{"component", "role"},
	)

	DatabaseHeartbeatLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_heartbeat_latency_seconds",
			Help:    "Latency of database heartbeat probes",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"component", "role"},
	)

	DatabaseReplicaStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_replica_status",
			Help: "Database replica statistics (state=replica_total|replica_healthy)",
		},
		[]string{"component", "state"},
	)

	// -------------------------- 初始化：Redis操作指标 --------------------------
	RedisOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_operations_total",
			Help: "Redis操作总数",
		},
		[]string{"operation", "status"}, // operation: get, set, del; status: success, error
	)

	RedisOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_operation_duration_seconds",
			Help:    "Redis操作延迟",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"operation"},
	)

	RedisErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_errors_total",
			Help: "Redis错误总数",
		},
		[]string{"operation", "error_type"}, // error_type: connection, timeout, serialization等
	)

	RedisCacheSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cache_size_bytes",
			Help: "Redis缓存大小",
		},
		[]string{"key_pattern"},
	)

	StorageSetNXDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_storage_setnx_duration_seconds",
			Help:    "Duration of Redis SetNX executions invoked by the storage layer",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1.0},
		},
		[]string{"status"},
	)

	RedisCommandStageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_command_stage_duration_seconds",
			Help:    "Breakdown of Redis command latency by stage (total, queue, service)",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2},
		},
		[]string{"component", "node", "command", "stage", "status"},
	)

	RedisPoolTotalConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_total_connections",
			Help: "Total connections currently allocated by the Redis client pool",
		},
		[]string{"component", "node"},
	)

	RedisPoolInUseConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_in_use_connections",
			Help: "Connections from the Redis pool that are currently in use",
		},
		[]string{"component", "node"},
	)

	RedisPoolIdleConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_idle_connections",
			Help: "Idle connections currently available in the Redis client pool",
		},
		[]string{"component", "node"},
	)

	RedisPoolWaitDurationSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_wait_duration_seconds",
			Help: "Cumulative time spent waiting for Redis pool connections",
		},
		[]string{"component", "node"},
	)

	RedisPoolWaitCountTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_wait_events_total",
			Help: "Cumulative number of waits encountered when acquiring Redis pool connections",
		},
		[]string{"component", "node"},
	)

	RedisPoolHitsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_hits_total",
			Help: "Cumulative number of times a free Redis connection was available",
		},
		[]string{"component", "node"},
	)

	RedisPoolMissesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_misses_total",
			Help: "Cumulative number of times Redis pool had to create a new connection",
		},
		[]string{"component", "node"},
	)

	RedisPoolTimeoutsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_timeouts_total",
			Help: "Cumulative number of timeouts when waiting for Redis pool connections",
		},
		[]string{"component", "node"},
	)

	// -------------------------- 初始化：Redis集群监控指标 --------------------------
	RedisClusterNodesTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_nodes_total",
			Help: "Redis集群节点总数",
		},
	)

	RedisClusterNodesUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_nodes_up",
			Help: "Redis集群正常节点数量",
		},
	)

	RedisClusterNodesDown = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_nodes_down",
			Help: "Redis集群异常节点数量",
		},
	)

	RedisClusterState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_state",
			Help: "Redis集群状态（0=异常, 1=正常）",
		},
		[]string{"cluster_name"},
	)

	RedisClusterSlotsAssigned = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_slots_assigned",
			Help: "Redis集群已分配的槽位数量",
		},
	)

	RedisClusterSlotsOk = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_slots_ok",
			Help: "Redis集群正常的槽位数量",
		},
	)

	RedisClusterSlotsPFail = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_slots_pfail",
			Help: "Redis集群可能失败的槽位数量",
		},
	)

	RedisClusterSlotsFail = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_slots_fail",
			Help: "Redis集群失败的槽位数量",
		},
	)

	RedisClusterNodeInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_info",
			Help: "Redis集群节点基本信息",
		},
		[]string{"node_id", "node_role", "node_address", "cluster_name"},
	)

	RedisClusterNodeMemory = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_memory_bytes",
			Help: "Redis集群节点内存使用量",
		},
		[]string{"node_id", "node_address"},
	)

	RedisClusterNodeCPU = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_cpu_usage_percent",
			Help: "Redis集群节点CPU使用率",
		},
		[]string{"node_id", "node_address"},
	)

	RedisClusterNodeConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_connections",
			Help: "Redis集群节点连接数",
		},
		[]string{"node_id", "node_address", "connection_type"}, // client, replica等
	)

	RedisClusterNodeKeys = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_keys_total",
			Help: "Redis集群节点键数量",
		},
		[]string{"node_id", "node_address"},
	)

	RedisClusterNodeOpsPerSec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_node_operations_per_second",
			Help: "Redis集群节点每秒操作数",
		},
		[]string{"node_id", "node_address", "operation_type"}, // cmd, net_input, net_output
	)

	RedisClusterHitRate = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_hit_rate",
			Help: "Redis集群整体命中率",
		},
	)

	RedisClusterMemoryUsage = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_memory_usage_bytes",
			Help: "Redis集群总内存使用量",
		},
	)

	RedisClusterMemoryUsagePercent = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_cluster_memory_usage_percent",
			Help: "Redis集群内存使用百分比",
		},
	)

	RedisClusterTotalCommands = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_cluster_commands_total",
			Help: "Redis集群命令执行总数",
		},
		[]string{"node_id", "node_address", "command"},
	)

	RedisClusterKeyspaceHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cluster_keyspace_hits_total",
			Help: "Redis集群键空间命中总数",
		},
	)

	RedisClusterKeyspaceMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cluster_keyspace_misses_total",
			Help: "Redis集群键空间未命中总数",
		},
	)

	RedisClusterNetworkIO = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_network_io_bytes",
			Help: "Redis集群网络IO流量",
		},
		[]string{"node_id", "node_address", "direction"}, // input, output
	)

	RedisClusterReplicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_replication_lag_seconds",
			Help: "Redis集群主从复制延迟",
		},
		[]string{"master_id", "slave_id", "master_address", "slave_address"},
	)

	RedisClusterFailoverCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cluster_failover_events_total",
			Help: "Redis集群故障转移事件总数",
		},
	)

	RedisClusterMigrationCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cluster_slot_migrations_total",
			Help: "Redis集群槽位迁移总数",
		},
	)

	RedisClusterHealthCheck = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_health_check",
			Help: "Redis集群健康检查状态（0=异常, 1=正常）",
		},
		[]string{"check_type", "cluster_name"}, // node_connect, slot_integrity, replication
	)

	// -------------------------- 初始化：HTTP指标 --------------------------
	HTTPResponseTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_time_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"path", "method", "status"},
	)

	HTTPRequestsInFlight = prometheus.NewGauge( // 修正：直接赋值，不使用指针
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of ongoing HTTP requests",
		},
	)

	HTTPRequestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_size_bytes",
			Help:    "HTTP request size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 6), // 100B to 100MB
		},
		[]string{"path", "method"},
	)

	HTTPResponseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "HTTP response size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7), // 100B to 1GB
		},
		[]string{"path", "method", "status"},
	)

	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP请求总数",
		},
		[]string{"path", "method", "status", "error_type", "user_id", "tenant_id", "client_ip", "user_agent", "host"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP请求延迟分布",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
		},
		[]string{"path", "method", "status", "error_type"},
	)

	HTTPRequestsInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_requests_in_progress",
			Help: "当前正在处理的HTTP请求数",
		},
		[]string{"path", "method"},
	)

	CacheRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_cache_requests_total",
			Help: "HTTP请求缓存命中统计",
		},
		[]string{"path", "cache_status", "user_id", "tenant_id"},
	)

	SlowHTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_slow_requests_total",
			Help: "慢HTTP请求统计（>1s）",
		},
		[]string{"path", "method", "status", "error_type"},
	)

	HTTPErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_errors_total",
			Help: "Total number of HTTP errors by type",
		},
		[]string{"method", "path", "status", "error_type", "error_code", "tenant_id", "user_id"},
	)

	// -------------------------- 初始化：缓存相关指标 --------------------------
	CacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_cache_hits_total",
			Help: "Total user cache hits",
		},
		[]string{"type"}, // hit, miss, null_hit
	)

	DBQueries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_db_queries_total",
			Help: "Total user database queries",
		},
		[]string{"result"}, // found, not_found
	)

	RequestsMerged = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_service_requests_merged_total",
			Help: "被singleflight合并的请求数量",
		},
		[]string{"operation"}, // 操作类型：get, create, update等
	)

	CacheErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_service_cache_errors_total",
			Help: "缓存操作错误数量",
		},
		[]string{"type", "operation"}, // 错误类型，操作类型
	)

	CacheNullValuesCount = prometheus.NewGauge( // 修正：直接赋值，不使用指针
		prometheus.GaugeOpts{
			Name: "cache_null_values_count",
			Help: "Current number of null value caches in Redis",
		},
	)

	CacheNullValueOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_null_value_operations_total",
			Help: "Total null value cache operations",
		},
		[]string{"operation"}, // operation: set, hit, expire
	)

	UserProtectionEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_protection_events_total",
			Help: "Total number of user protection events triggered",
		},
		[]string{"type"}, // type: negative_cache, blacklist
	)

	UserCacheContactBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_cache_contact_batch_duration_seconds",
			Help:    "Redis pipeline duration when writing contact cache batches",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 20, 30, 45, 60, 90, 120},
		},
		[]string{"component", "status"},
	)

	UserCacheContactBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_cache_contact_batch_size",
			Help:    "Number of contact cache keys written per Redis batch",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 8, 10, 15, 20},
		},
		[]string{"component", "status"},
	)

	UserCacheContactBatchRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_cache_contact_batch_retries_total",
			Help: "Total number of retry attempts when writing contact cache batches",
		},
		[]string{"component", "status"},
	)

	UserCacheRefreshPhaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_cache_refresh_phase_duration_seconds",
			Help:    "Duration distribution for individual phases during user cache refresh",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 20, 30, 45, 60, 90, 120},
		},
		[]string{"component", "phase"},
	)

	UserCacheRefreshItems = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "user_cache_refresh_item_count",
			Help:    "Number of cache items processed per category during refresh",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 32},
		},
		[]string{"component", "item_type"},
	)

	// -------------------------- 手动注册所有指标到 Prometheus 默认注册表 --------------------------
	// MustRegister：注册失败会panic（适合初始化阶段，提前暴露配置错误）
	prometheus.MustRegister(
		// 生产者指标
		ProducerAttempts,
		ProducerSuccess,
		ProducerFailures,
		ProducerRetries,
		DeadLetterMessages,
		MessageProcessingTime,
		ProducerWALBacklog,
		ProducerWALMaxAge,

		// 业务处理指标
		BusinessProcessingTime,
		BusinessSuccess,
		BusinessFailures,

		// 新增业务吞吐量指标
		BusinessOperationsTotal,
		BusinessOperationsRate,
		BusinessInProgress,
		BusinessThroughputStats,
		BusinessErrorRate,
		TraceSpanDuration,
		TraceOperationDuration,
		UserCreateStepDuration,
		UserCreateSlowStepsTotal,
		UserCreateStepTotal,
		UserCreateRequestsTotal,
		UserCreateDegradeTotal,
		UserCreateMessageTotal,
		UserContactPlaceholderSetDuration,
		UserContactPlaceholderEvents,
		AuditEventsTotal,
		AuditEventFailures,

		// 消费者指标
		ConsumerMessagesReceived,
		ConsumerMessagesProcessed,
		ConsumerProcessingErrors,
		ConsumerProcessingTime,
		ConsumerRetryMessages,
		ConsumerDeadLetterMessages,
		// commit success/failure metrics should be registered so they're exposed
		ConsumerCommitSuccess,
		ConsumerCommitFailures,
		ConsumerLag,
		ConsumerTopicPartitions,
		ConsumerGroupInstances,
		ConsumerPartitionsNoOwner,
		ConsumerMessageAgeSeconds,
		KafkaBrokerHealth,
		KafkaBrokerLatency,
		KafkaClusterHealth,
		KafkaClusterBrokers,
		KafkaTopicStatus,
		KafkaHeartbeatFailures,

		PendingLeaseActiveGauge,
		PendingLeaseQueueDepth,
		PendingLeaseQueueDepthSample,
		PendingLeaseBackpressureLevel,
		PendingLeaseEvents,
		PendingLeaseFallbackTotal,
		PendingLeaseHoldDuration,
		PendingLeaseCalibrationDuration,
		PendingCoordinatorHealth,
		PendingLeaseLuaAttempts,
		PendingConsumerQueueDepth,
		PendingConsumerRedisLatency,
		PendingConsumerDequeueDuration,
		PendingBackpressureDelaySeconds,
		PendingBackpressureDelayTriggerRate,
		PendingBackpressureDelayCancelRate,
		PendingBackpressureLeadTimeSeconds,
		PendingBackpressureDelayCancelledSeconds,
		PendingBackpressureDeadlineDecisions,
		OperationQueueReadyDepth,
		OperationQueueScheduledDepth,
		OperationQueueInflightGauge,
		OperationQueueFallbackGauge,
		OperationWorkerIterations,
		OperationWorkerIterationDuration,
		OperationCompensationTotal,
		OperationCompensationDuration,
		OperationModeDecisions,
		OperationModeCurrent,
		OperationModeRolloutPercent,
		OperationModeAllowlistSize,
		OperationModeBlocklistSize,

		// 数据库指标
		DatabaseQueryDuration,
		DatabaseQueryErrors,
		DatabaseSlowQueries,
		DatabasePoolOpenConnections,
		DatabasePoolInUse,
		DatabasePoolIdle,
		DatabasePoolWaitCount,
		DatabasePoolWaitDurationSeconds,
		DatabasePoolMaxOpenConnections,
		DatabaseHeartbeatStatus,
		DatabaseHeartbeatLatency,
		DatabaseReplicaStatus,

		// Redis指标
		RedisOperations,
		RedisOperationDuration,
		RedisErrors,
		RedisCacheSize,
		StorageSetNXDuration,
		RedisCommandStageDuration,
		RedisPoolTotalConnections,
		RedisPoolInUseConnections,
		RedisPoolIdleConnections,
		RedisPoolWaitDurationSeconds,
		RedisPoolWaitCountTotal,
		RedisPoolHitsTotal,
		RedisPoolMissesTotal,
		RedisPoolTimeoutsTotal,

		// 新增指标
		ProducerInFlightCurrent,
		WriteLimiterTotal,
		ProducerDeliveryLatency,
		ProducerEnqueueWaitLatency,
		ProducerBrokerAckLatency,

		// Redis集群指标
		RedisClusterNodesTotal,
		RedisClusterNodesUp,
		RedisClusterNodesDown,
		RedisClusterState,
		RedisClusterSlotsAssigned,
		RedisClusterSlotsOk,
		RedisClusterSlotsPFail,
		RedisClusterSlotsFail,
		RedisClusterNodeInfo,
		RedisClusterNodeMemory,
		RedisClusterNodeCPU,
		RedisClusterNodeConnections,
		RedisClusterNodeKeys,
		RedisClusterNodeOpsPerSec,
		RedisClusterHitRate,
		RedisClusterMemoryUsage,
		RedisClusterMemoryUsagePercent,
		RedisClusterTotalCommands,
		RedisClusterKeyspaceHits,
		RedisClusterKeyspaceMisses,
		RedisClusterNetworkIO,
		RedisClusterReplicationLag,
		RedisClusterFailoverCount,
		RedisClusterMigrationCount,
		RedisClusterHealthCheck,

		// HTTP指标
		HTTPResponseTime,
		HTTPRequestsInFlight, // 修正：现在可以正确注册
		HTTPRequestSize,
		HTTPResponseSize,
		HTTPRequestsTotal,
		HTTPRequestDuration,
		HTTPRequestsInProgress,
		CacheRequests,
		SlowHTTPRequests,
		HTTPErrors,

		// 缓存相关指标
		CacheHits,
		DBQueries,
		RequestsMerged,
		CacheErrors,
		CacheNullValuesCount, // 修正：现在可以正确注册
		CacheNullValueOperations,
		UserProtectionEvents,
		UserCacheContactBatchDuration,
		UserCacheContactBatchSize,
		UserCacheContactBatchRetries,
		UserCacheRefreshPhaseDuration,
		UserCacheRefreshItems,
	)
}

// -------------------------- 以下辅助函数保持不变 --------------------------
// // 统一处理数据库查询的监控上报，包括查询错误计数和查询耗时统计
// func RecordDatabaseQuery(operation, table string, duration float64, err error) {
// 	//DatabaseQueryDuration.WithLabelValues(operation, table).Observe(duration)
// 	if err != nil {
// 	//	errorType := GetDatabaseErrorType(err)
// 	//	DatabaseQueryErrors.WithLabelValues(operation, table, errorType).Inc()
// 	}
// }

// // GetDatabaseErrorType 数据库错误分类，仅用于监控指标
// func GetDatabaseErrorType(err error) string {
// 	if err == nil {
// 		return "success"
// 	}

// 	// 1. 先检查是否是业务框架已知错误
// 	coder := errors.ParseCoderByErr(err)
// 	if coder != nil {
// 		// 对于已知业务错误，返回通用分类
// 		return "business_error"
// 	}

// 	// 2. 检查上下文相关错误
// 	switch {
// 	case errors.Is(err, context.DeadlineExceeded):
// 		return "timeout"
// 	case errors.Is(err, context.Canceled):
// 		return "cancelled"
// 	}

// 	// 3. 检查GORM特定错误
// 	switch {
// 	case errors.Is(err, gorm.ErrRecordNotFound):
// 		return "not_found"
// 	case errors.Is(err, gorm.ErrDuplicatedKey):
// 		return "duplicate_key"
// 	case errors.Is(err, gorm.ErrForeignKeyViolated):
// 		return "foreign_key_violation"
// 	}

// 	// 4. 检查MySQL驱动错误
// 	var mysqlErr *mysql.MySQLError
// 	if errors.As(err, &mysqlErr) {
// 		switch mysqlErr.Number {
// 		case 1213: // ER_LOCK_DEADLOCK
// 			return "deadlock"
// 		case 1205: // ER_LOCK_WAIT_TIMEOUT
// 			return "lock_timeout"
// 		case 1062: // ER_DUP_ENTRY
// 			return "duplicate_entry"
// 		case 1452: // ER_NO_REFERENCED_ROW
// 			return "foreign_key_violation"
// 		case 1045: // ER_ACCESS_DENIED_ERROR
// 			return "access_denied"
// 		case 2002, 2003: // 连接相关错误
// 			return "connection_error"
// 		default:
// 			return "mysql_error"
// 		}
// 	}

//		// 5. 基于错误消息的模式匹配
//		errMsg := strings.ToLower(err.Error())
//		switch {
//		case strings.Contains(errMsg, "timeout"):
//			return "timeout"
//		case strings.Contains(errMsg, "connection"):
//			return "connection_error"
//		case strings.Contains(errMsg, "deadlock"):
//			return "deadlock"
//		case strings.Contains(errMsg, "duplicate"):
//			return "duplicate"
//		case strings.Contains(errMsg, "constraint"):
//			return "constraint_violation"
//		case strings.Contains(errMsg, "full"):
//			return "disk_full"
//		default:
//			return "unknown"
//		}
//	}
//
// 并发请求数
func HTTPMiddlewareStart() {
	HTTPRequestsInFlight.Inc() // 现在可以正确调用方法
}

// 标记请求结束
func HTTPMiddlewareEnd() {
	HTTPRequestsInFlight.Dec() // 现在可以正确调用方法
}

// 监控数据库连接池的实时活跃连接数
func SetDatabaseConnectionsInUse(poolName string, count int) {
	//DatabaseConnectionsInUse.WithLabelValues(poolName).Set(float64(count))
}

// HTTP 请求最核心的监控上报函数，一次性上报请求计数、耗时、大小、慢请求等多维度指标，关联 8 个 HTTP 相关指标（覆盖请求生命周期），支持多租户、用户级别的精细化监控。
func RecordHTTPRequest(path, method, status string, duration float64, requestSize, responseSize int64,
	clientIP, userAgent, host, errorCode, errorType, userID, tenantID string) {

	// 使用现有的HTTP指标
	HTTPResponseTime.WithLabelValues(path, method, status).Observe(duration)

	if requestSize > 0 {
		HTTPRequestSize.WithLabelValues(path, method).Observe(float64(requestSize))
	}
	if responseSize > 0 {
		HTTPResponseSize.WithLabelValues(path, method, status).Observe(float64(responseSize))
	}

	// 记录新增的增强版指标
	HTTPRequestsTotal.WithLabelValues(
		path, method, status, errorType, userID, tenantID, clientIP, userAgent, host,
	).Inc()

	// 记录延迟分布
	HTTPRequestDuration.WithLabelValues(
		path, method, status, errorType,
	).Observe(duration)

	// 记录慢请求
	if duration > 1.0 {
		SlowHTTPRequests.WithLabelValues(path, method, status, errorType).Inc()
	}
}

// GetRedisErrorType Redis错误分类
func GetRedisErrorType(err error) string {
	if err == nil {
		return "success"
	}

	errMsg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errMsg, "timeout"):
		return "timeout"
	case strings.Contains(errMsg, "connection"):
		return "connection_error"
	case strings.Contains(errMsg, "network"):
		return "network_error"
	case strings.Contains(errMsg, "max memory"):
		return "memory_limit"
	case strings.Contains(errMsg, "serialize"), strings.Contains(errMsg, "marshal"):
		return "serialization_error"
	case strings.Contains(errMsg, "deserialize"), strings.Contains(errMsg, "unmarshal"):
		return "deserialization_error"
	case strings.Contains(errMsg, "nil"): // redis.Nil错误
		return "key_not_found"
	default:
		return "unknown_error"
	}
}

// RecordRedisOperation 记录Redis操作指标
func normalizeLabelValue(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

// RecordDatabaseQuery 记录数据库查询耗时与结果。
func RecordDatabaseQuery(component, operation string, duration time.Duration, err error) {
	componentLabel := normalizeLabelValue(component, "unknown")
	operationLabel := normalizeLabelValue(operation, "unknown")
	status := "success"
	if err != nil {
		status = "error"
	}

	if DatabaseQueryDuration != nil {
		DatabaseQueryDuration.WithLabelValues(componentLabel, operationLabel, status).Observe(duration.Seconds())
	}

	if err != nil && DatabaseQueryErrors != nil {
		errorType := getDBErrorType(err)
		DatabaseQueryErrors.WithLabelValues(componentLabel, operationLabel, errorType).Inc()
	}

	if duration >= defaultSlowQueryThreshold && DatabaseSlowQueries != nil {
		DatabaseSlowQueries.WithLabelValues(componentLabel, operationLabel).Inc()
	}
}

func getDBErrorType(err error) string {
	if err == nil {
		return "success"
	}

	if errCode := errors.GetCode(err); errCode != 0 {
		switch errCode {
		case code.ErrDatabaseTimeout:
			return "timeout"
		case code.ErrDatabaseDeadlock:
			return "deadlock"
		case code.ErrDatabase:
			return "database_error"
		case code.ErrUserNotFound:
			return "not_found"
		}
	}

	lowered := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lowered, "timeout"), strings.Contains(lowered, "deadline"):
		return "timeout"
	case strings.Contains(lowered, "deadlock"):
		return "deadlock"
	case strings.Contains(lowered, "duplicate"), strings.Contains(lowered, "unique"):
		return "duplicate"
	case strings.Contains(lowered, "not found"), strings.Contains(lowered, "no rows"):
		return "not_found"
	case strings.Contains(lowered, "cancel"):
		return "cancelled"
	case strings.Contains(lowered, "connection"), strings.Contains(lowered, "broken pipe"), strings.Contains(lowered, "reset"):
		return "connection"
	default:
		return "unknown"
	}
}

func RecordRedisOperation(operation string, duration float64, err error) {
	// 记录操作延迟
	RedisOperationDuration.WithLabelValues(operation).Observe(duration)

	// 记录操作计数
	status := "success"
	if err != nil {
		status = "error"
		errorType := GetRedisErrorType(err)
		RedisErrors.WithLabelValues(operation, errorType).Inc()
	}

	RedisOperations.WithLabelValues(operation, status).Inc()
}

// RedisCommandStage enumerates different latency segments we export.
type RedisCommandStage string

const (
	RedisStageTotal   RedisCommandStage = "total"
	RedisStageQueue   RedisCommandStage = "queue"
	RedisStageService RedisCommandStage = "service"
)

// ObserveRedisCommandDurations records detailed latency breakdown for a Redis command.
func ObserveRedisCommandDurations(component, node, command string, total, queue, service time.Duration, err error) {
	if RedisCommandStageDuration == nil {
		return
	}

	componentLabel := normalizeLabelValue(component, "default")
	nodeLabel := normalizeLabelValue(node, "unknown")
	commandLabel := normalizeLabelValue(strings.ToLower(command), "unknown")
	status := "success"
	if err != nil {
		status = "error"
	}

	observeStage := func(stage RedisCommandStage, value time.Duration) {
		if value < 0 {
			value = 0
		}
		RedisCommandStageDuration.WithLabelValues(componentLabel, nodeLabel, commandLabel, string(stage), status).Observe(value.Seconds())
	}

	observeStage(RedisStageTotal, total)
	observeStage(RedisStageQueue, queue)
	observeStage(RedisStageService, service)
}

// RedisPoolSnapshot captures a point-in-time view of client pool utilization counters.
type RedisPoolSnapshot struct {
	TotalConns          float64
	IdleConns           float64
	InUseConns          float64
	WaitDurationSeconds float64
	WaitCount           float64
	Hits                float64
	Misses              float64
	Timeouts            float64
}

// ObserveRedisPoolStats updates Prometheus gauges with the provided pool snapshot.
func ObserveRedisPoolStats(component, node string, snapshot RedisPoolSnapshot) {
	componentLabel := normalizeLabelValue(component, "default")
	nodeLabel := normalizeLabelValue(node, "unknown")

	if RedisPoolTotalConnections != nil {
		RedisPoolTotalConnections.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.TotalConns)
	}
	if RedisPoolInUseConnections != nil {
		RedisPoolInUseConnections.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.InUseConns)
	}
	if RedisPoolIdleConnections != nil {
		RedisPoolIdleConnections.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.IdleConns)
	}
	if RedisPoolWaitDurationSeconds != nil {
		RedisPoolWaitDurationSeconds.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.WaitDurationSeconds)
	}
	if RedisPoolWaitCountTotal != nil {
		RedisPoolWaitCountTotal.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.WaitCount)
	}
	if RedisPoolHitsTotal != nil {
		RedisPoolHitsTotal.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.Hits)
	}
	if RedisPoolMissesTotal != nil {
		RedisPoolMissesTotal.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.Misses)
	}
	if RedisPoolTimeoutsTotal != nil {
		RedisPoolTimeoutsTotal.WithLabelValues(componentLabel, nodeLabel).Set(snapshot.Timeouts)
	}
}

// ObserveStorageSetNX 记录底层存储层调用 SetNX 的耗时分布。
func ObserveStorageSetNX(duration time.Duration, err error) {
	if StorageSetNXDuration == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	StorageSetNXDuration.WithLabelValues(status).Observe(duration.Seconds())
}

// RecordOperationQueueDepth 更新异步操作队列的关键深度指标。
func RecordOperationQueueDepth(resource string, ready, scheduled, inflight int64) {
	label := normalizeLabelValue(resource, "unknown")
	if ready < 0 {
		ready = 0
	}
	if scheduled < 0 {
		scheduled = 0
	}
	if inflight < 0 {
		inflight = 0
	}
	if OperationQueueReadyDepth != nil {
		OperationQueueReadyDepth.WithLabelValues(label).Set(float64(ready))
	}
	if OperationQueueScheduledDepth != nil {
		OperationQueueScheduledDepth.WithLabelValues(label).Set(float64(scheduled))
	}
	if OperationQueueInflightGauge != nil {
		OperationQueueInflightGauge.WithLabelValues(label).Set(float64(inflight))
	}
}

// SetOperationQueueFallback 标记是否启用内存回退队列。
func SetOperationQueueFallback(resource string, active bool) {
	if OperationQueueFallbackGauge == nil {
		return
	}
	value := 0.0
	if active {
		value = 1.0
	}
	label := normalizeLabelValue(resource, "unknown")
	OperationQueueFallbackGauge.WithLabelValues(label).Set(value)
}

// ObservePendingConsumerRedisLatency 记录消费者访问 Redis 的往返耗时。
func ObservePendingConsumerRedisLatency(component, operation string, duration time.Duration, err error) {
	if PendingConsumerRedisLatency == nil {
		return
	}
	comp := normalizeLabelValue(component, "user_consumer")
	op := normalizeLabelValue(operation, "unknown")
	status := "success"
	if err != nil {
		status = "error"
	}
	PendingConsumerRedisLatency.WithLabelValues(comp, op, status).Observe(duration.Seconds())
}

// ObserveUserCacheContactBatch 记录联系人批量写入的耗时、批量大小与重试次数。
func ObserveUserCacheContactBatch(component string, batchSize int, duration time.Duration, attempts int, err error) {
	comp := normalizeLabelValue(component, "user_consumer")
	status := "success"
	if err != nil {
		status = "error"
	}
	if UserCacheContactBatchDuration != nil && duration > 0 {
		UserCacheContactBatchDuration.WithLabelValues(comp, status).Observe(duration.Seconds())
	}
	if batchSize < 0 {
		batchSize = 0
	}
	if UserCacheContactBatchSize != nil {
		UserCacheContactBatchSize.WithLabelValues(comp, status).Observe(float64(batchSize))
	}
	if attempts > 1 && UserCacheContactBatchRetries != nil {
		UserCacheContactBatchRetries.WithLabelValues(comp, status).Add(float64(attempts - 1))
	}
}

// ObserveUserCacheRefreshPhase 记录缓存刷新各阶段的耗时分布。
func ObserveUserCacheRefreshPhase(component, phase string, duration time.Duration) {
	if duration <= 0 || UserCacheRefreshPhaseDuration == nil {
		return
	}
	comp := normalizeLabelValue(component, "user_consumer")
	ph := normalizeLabelValue(phase, "unknown")
	UserCacheRefreshPhaseDuration.WithLabelValues(comp, ph).Observe(duration.Seconds())
}

// ObserveUserCacheRefreshItems 记录缓存刷新过程中的各类元素数量（如管道条目、联系人条目等）。
func ObserveUserCacheRefreshItems(component, itemType string, count int) {
	if UserCacheRefreshItems == nil {
		return
	}
	if count < 0 {
		count = 0
	}
	comp := normalizeLabelValue(component, "user_consumer")
	it := normalizeLabelValue(itemType, "unknown")
	UserCacheRefreshItems.WithLabelValues(comp, it).Observe(float64(count))
}

// ObservePendingConsumerDequeue 记录消费者从消息开始清理到完成的耗时。
func ObservePendingConsumerDequeue(component string, duration time.Duration, err error) {
	if PendingConsumerDequeueDuration == nil {
		return
	}
	comp := normalizeLabelValue(component, "user_consumer")
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	PendingConsumerDequeueDuration.WithLabelValues(comp, outcome).Observe(duration.Seconds())
}

// SetPendingConsumerQueueDepth 更新消费者侧采样到的队列深度。
func SetPendingConsumerQueueDepth(component string, depth int64) {
	if PendingConsumerQueueDepth == nil {
		return
	}
	if depth < 0 {
		depth = 0
	}
	comp := normalizeLabelValue(component, "user_consumer")
	PendingConsumerQueueDepth.WithLabelValues(comp).Set(float64(depth))
}

// RecordPendingBackpressureDelay 记录一次背压延迟的注入与分布。
func RecordPendingBackpressureDelay(component, level string, delay time.Duration) {
	if delay <= 0 {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	lvl := normalizeLabelValue(level, "none")
	if PendingBackpressureDelaySeconds != nil {
		PendingBackpressureDelaySeconds.WithLabelValues(comp, lvl).Observe(delay.Seconds())
	}
	if PendingBackpressureDelayTriggerRate != nil {
		PendingBackpressureDelayTriggerRate.WithLabelValues(comp, lvl).Inc()
	}
}

// RecordPendingBackpressureDelayCancellation 统计背压延迟被提前取消的次数。
func RecordPendingBackpressureDelayCancellation(component, level string) {
	if PendingBackpressureDelayCancelRate == nil {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	lvl := normalizeLabelValue(level, "none")
	PendingBackpressureDelayCancelRate.WithLabelValues(comp, lvl).Inc()
}

// RecordPendingBackpressureLeadTime 观测从请求开始到延迟注入前的耗时。
func RecordPendingBackpressureLeadTime(component, level string, elapsed time.Duration) {
	if elapsed <= 0 || PendingBackpressureLeadTimeSeconds == nil {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	lvl := normalizeLabelValue(level, "none")
	PendingBackpressureLeadTimeSeconds.WithLabelValues(comp, lvl).Observe(elapsed.Seconds())
}

// ObservePendingBackpressureCancellationDuration 记录延迟等待被取消前已等待的时长。
func ObservePendingBackpressureCancellationDuration(component, level string, waited time.Duration) {
	if waited <= 0 || PendingBackpressureDelayCancelledSeconds == nil {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	lvl := normalizeLabelValue(level, "none")
	PendingBackpressureDelayCancelledSeconds.WithLabelValues(comp, lvl).Observe(waited.Seconds())
}

// RecordPendingBackpressureDeadlineDecision 统计因 deadline 触发的延迟调整动作。
func RecordPendingBackpressureDeadlineDecision(component, level, action string) {
	if PendingBackpressureDeadlineDecisions == nil {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	lvl := normalizeLabelValue(level, "none")
	act := normalizeLabelValue(action, "unknown")
	PendingBackpressureDeadlineDecisions.WithLabelValues(comp, lvl, act).Inc()
}

// RecordOperationWorkerIteration 统计一次工作线程迭代，并记录耗时。
func RecordOperationWorkerIteration(resource, outcome string, duration time.Duration) {
	resourceLabel := normalizeLabelValue(resource, "unknown")
	outcomeLabel := normalizeLabelValue(outcome, "unknown")
	if OperationWorkerIterations != nil {
		OperationWorkerIterations.WithLabelValues(resourceLabel, outcomeLabel).Inc()
	}
	if OperationWorkerIterationDuration != nil {
		OperationWorkerIterationDuration.WithLabelValues(resourceLabel, outcomeLabel).Observe(duration.Seconds())
	}
}

// ObserveOperationCompensation 记录补偿处理的结果与耗时。
func ObserveOperationCompensation(resource, outcome string, duration time.Duration) {
	resourceLabel := normalizeLabelValue(resource, "unknown")
	outcomeLabel := normalizeLabelValue(outcome, "unknown")
	if OperationCompensationTotal != nil {
		OperationCompensationTotal.WithLabelValues(resourceLabel, outcomeLabel).Inc()
	}
	if OperationCompensationDuration != nil {
		OperationCompensationDuration.WithLabelValues(resourceLabel, outcomeLabel).Observe(duration.Seconds())
	}
}

// RecordKafkaProducerOperation 记录Kafka生产者操作指标
func RecordKafkaProducerOperation(topic, operation string, duration float64, err error, isRetry bool) {
	// 记录尝试次数
	ProducerAttempts.WithLabelValues(topic, operation).Inc()

	if err != nil {
		errorType := GetKafkaErrorType(err)
		ProducerFailures.WithLabelValues(topic, operation, errorType).Inc()

		// 如果是重试，记录重试指标
		if isRetry {
			ProducerRetries.WithLabelValues(topic, operation).Inc()
		}
	} else {
		ProducerSuccess.WithLabelValues(topic, operation).Inc()
	}

	// 记录处理时间（如果有duration信息）
	if duration > 0 {
		status := "success"
		if err != nil {
			status = "error"
		}
		MessageProcessingTime.WithLabelValues(topic, operation, status).Observe(duration)
	}
}

// RecordKafkaProducerDelivery 记录Kafka生产者从入队到Broker确认的耗时，以及最终结果。
func RecordKafkaProducerDelivery(topic, operation string, duration time.Duration, err error) {
	if ProducerDeliveryLatency == nil {
		return
	}
	if topic == "" {
		topic = "unknown"
	}
	if operation == "" {
		operation = "unknown"
	}
	result := "success"
	if err != nil {
		result = GetKafkaErrorType(err)
	}
	ProducerDeliveryLatency.WithLabelValues(topic, operation, result).Observe(duration.Seconds())
}

// RecordKafkaProducerEnqueueWait 记录生产者入队前的排队等待时间。
func RecordKafkaProducerEnqueueWait(topic, operation string, duration time.Duration, err error) {
	if ProducerEnqueueWaitLatency == nil {
		return
	}
	if topic == "" {
		topic = "unknown"
	}
	if operation == "" {
		operation = "unknown"
	}
	result := "success"
	if err != nil {
		result = GetKafkaErrorType(err)
	}
	ProducerEnqueueWaitLatency.WithLabelValues(topic, operation, result).Observe(duration.Seconds())
}

// RecordKafkaProducerBrokerAck 记录生产者等待 Broker ACK 的耗时。
func RecordKafkaProducerBrokerAck(topic, operation string, duration time.Duration, err error) {
	if ProducerBrokerAckLatency == nil {
		return
	}
	if topic == "" {
		topic = "unknown"
	}
	if operation == "" {
		operation = "unknown"
	}
	result := "success"
	if err != nil {
		result = GetKafkaErrorType(err)
	}
	ProducerBrokerAckLatency.WithLabelValues(topic, operation, result).Observe(duration.Seconds())
}

// RecordProducerWALStats 记录生产者WAL队列积压指标
func RecordProducerWALStats(backlog int, maxAge time.Duration) {
	if backlog < 0 {
		backlog = 0
	}
	ProducerWALBacklog.Set(float64(backlog))
	seconds := maxAge.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	ProducerWALMaxAge.Set(seconds)
}

// RecordDeadLetterMessage 记录死信消息
func RecordDeadLetterMessage(topic, operation string) {
	DeadLetterMessages.WithLabelValues(topic, operation).Inc()
}

// RecordKafkaConsumerOperation 记录Kafka消费者操作指标
func RecordKafkaConsumerOperation(topic, group, operation string, duration float64, err error) {
	// 记录接收消息
	ConsumerMessagesReceived.WithLabelValues(topic, group, operation).Inc()

	if err != nil {
		errorType := GetKafkaErrorType(err)
		ConsumerProcessingErrors.WithLabelValues(topic, group, operation, errorType).Inc()
	} else {
		ConsumerMessagesProcessed.WithLabelValues(topic, group, operation).Inc()
	}

	// 记录处理时间
	status := "success"
	if err != nil {
		status = "error"
	}
	ConsumerProcessingTime.WithLabelValues(topic, group, operation, status).Observe(duration)
}

// RecordConsumerRetry 记录消费者重试
func RecordConsumerRetry(topic, group, operation, errorType string) {
	ConsumerRetryMessages.WithLabelValues(topic, group, operation, errorType).Inc()
}

// RecordConsumerDeadLetter 记录消费者死信
func RecordConsumerDeadLetter(topic, group, operation, errorType string) {
	ConsumerDeadLetterMessages.WithLabelValues(topic, group, operation, errorType).Inc()
}

// SetConsumerLag 设置消费者延迟
func SetConsumerLag(topic, group string, lag int64) {
	ConsumerLag.WithLabelValues(topic, group).Set(float64(lag))
}

// GetKafkaErrorType Kafka错误分类
func GetKafkaErrorType(err error) string {
	if err == nil {
		return "success"
	}

	errMsg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errMsg, "timeout"):
		return "timeout"
	case strings.Contains(errMsg, "connection"):
		return "connection_error"
	case strings.Contains(errMsg, "network"):
		return "network_error"
	case strings.Contains(errMsg, "leader not available"):
		return "leader_unavailable"
	case strings.Contains(errMsg, "not leader for partition"):
		return "not_leader"
	case strings.Contains(errMsg, "message size too large"):
		return "message_too_large"
	case strings.Contains(errMsg, "unknown topic or partition"):
		return "unknown_topic"
	case strings.Contains(errMsg, "offset out of range"):
		return "offset_out_of_range"
	case strings.Contains(errMsg, "serialize"), strings.Contains(errMsg, "marshal"):
		return "serialization_error"
	case strings.Contains(errMsg, "deserialize"), strings.Contains(errMsg, "unmarshal"):
		return "deserialization_error"
	case strings.Contains(errMsg, "authentication"):
		return "authentication_error"
	case strings.Contains(errMsg, "authorization"):
		return "authorization_error"
	default:
		return "unknown_error"
	}
}

// GetBusinessErrorType 业务错误分类
func GetBusinessErrorType(err error) string {
	return category.CategoryFromError(err)
}

// -------------------------- 新增业务监控辅助函数 --------------------------

// MonitorBusinessOperation 函数也需要检查
func MonitorBusinessOperation(service, operation, source string, fn func() error) error {
	start := time.Now()

	// 增加处理中计数
	BusinessInProgress.WithLabelValues(service, operation).Inc()
	defer BusinessInProgress.WithLabelValues(service, operation).Dec()

	// 记录操作开始
	BusinessOperationsTotal.WithLabelValues(service, operation, source).Inc()

	// 执行业务逻辑
	err := fn()
	duration := time.Since(start).Seconds()

	// 记录处理时长（使用统一的标签）
	BusinessProcessingTime.WithLabelValues(service, operation).Observe(duration)
	BusinessThroughputStats.WithLabelValues(service, operation).Observe(duration)

	// 记录成功/失败
	if err != nil {
		errorType := category.CategoryFromError(err)
		BusinessFailures.WithLabelValues(service, operation, errorType).Inc()
	} else {
		BusinessSuccess.WithLabelValues(service, operation, "success").Inc()
	}

	return err
}

const userCreateSlowThresholdSeconds = 0.2

// RecordUserCreateStep 记录用户创建链路中某个步骤的耗时，并在超过阈值时累计慢步骤计数。
func RecordUserCreateStep(step, field, accountType string, duration time.Duration, err error) {
	stepLabel := normalizeLabelValue(step, "unknown")
	fieldLabel := normalizeLabelValue(field, "unknown")
	accountLabel := normalizeLabelValue(accountType, "unknown")

	UserCreateStepDuration.WithLabelValues(stepLabel, fieldLabel, accountLabel).Observe(duration.Seconds())

	outcome := "success"
	if err != nil {
		outcome = GetBusinessErrorType(err)
		if outcome == "" {
			outcome = "error"
		}
	}
	UserCreateStepTotal.WithLabelValues(stepLabel, fieldLabel, accountLabel, outcome).Inc()

	if duration.Seconds() > userCreateSlowThresholdSeconds {
		UserCreateSlowStepsTotal.WithLabelValues(stepLabel, fieldLabel, accountLabel).Inc()
	}
}

// RecordUserCreateRequest 记录创建请求整体的执行模式与结果。
func RecordUserCreateRequest(mode, outcome, accountType string) {
	modeLabel := normalizeLabelValue(mode, "unknown")
	outcomeLabel := normalizeLabelValue(outcome, "unknown")
	accountLabel := normalizeLabelValue(accountType, "unknown")
	UserCreateRequestsTotal.WithLabelValues(modeLabel, accountLabel, outcomeLabel).Inc()
}

// RecordUserCreateDegrade 记录降级触发的原因统计。
func RecordUserCreateDegrade(reason, accountType string) {
	reasonLabel := normalizeLabelValue(reason, "unknown")
	accountLabel := normalizeLabelValue(accountType, "unknown")
	UserCreateDegradeTotal.WithLabelValues(reasonLabel, accountLabel).Inc()
}

// RecordUserCreateMessage 记录 Kafka 发送阶段的业务视角结果。
func RecordUserCreateMessage(accountType, result string) {
	accountLabel := normalizeLabelValue(accountType, "unknown")
	resultLabel := normalizeLabelValue(result, "unknown")
	UserCreateMessageTotal.WithLabelValues(accountLabel, resultLabel).Inc()
}

// ObserveUserContactPlaceholderSet 记录联系人占位命令的耗时，用于定位 SetNX 阶段的尾延迟。
const userContactPlaceholderSlowThreshold = 20 * time.Millisecond

func ObserveUserContactPlaceholderSet(step, field string, duration time.Duration, err error) {
	if UserContactPlaceholderSetDuration == nil {
		return
	}
	switch step {
	case "ensure_contact_unique_placeholder_setnx", "redis_placeholder_setnx":
	default:
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	} else if duration > userContactPlaceholderSlowThreshold {
		status = "slow"
	}
	UserContactPlaceholderSetDuration.WithLabelValues(
		normalizeLabelValue(step, "unknown"),
		normalizeLabelValue(field, "unknown"),
		status,
	).Observe(duration.Seconds())
}

// RecordUserContactPlaceholderEvent 记录联系人唯一性路径的命令结果与降级分支，便于定位 Redis 退化场景。
func RecordUserContactPlaceholderEvent(step, field, result string) {
	if UserContactPlaceholderEvents == nil {
		return
	}
	UserContactPlaceholderEvents.WithLabelValues(
		normalizeLabelValue(step, "unknown"),
		normalizeLabelValue(field, "unknown"),
		normalizeLabelValue(result, "unknown"),
	).Inc()
}

// RecordBusinessQPS 记录业务QPS（供外部调用）
func RecordBusinessQPS(service, operation string, qps float64) {
	BusinessOperationsRate.WithLabelValues(service, operation).Set(qps)
}

// RecordBusinessErrorRate 记录业务错误率
func RecordBusinessErrorRate(service, operation string, errorRate float64) {
	BusinessErrorRate.WithLabelValues(service, operation).Set(errorRate)
}

// RecordPendingLeaseFallback 记录 PendingCoordinator 在降级兜底路径上的命中次数，便于观测 Redis 降级对并发的影响范围。
func RecordPendingLeaseFallback(component, operation, reason string) {
	if PendingLeaseFallbackTotal == nil {
		return
	}
	comp := normalizeLabelValue(component, "unknown")
	op := normalizeLabelValue(operation, "unknown")
	rs := normalizeLabelValue(reason, "unknown")
	PendingLeaseFallbackTotal.WithLabelValues(comp, op, rs).Inc()
}

// RecordTraceSpanDuration records duration for individual trace spans.
func RecordTraceSpanDuration(component, operation, status string, duration time.Duration) {
	if TraceSpanDuration == nil {
		return
	}
	TraceSpanDuration.WithLabelValues(component, operation, status).Observe(duration.Seconds())
}

// RecordTraceOperationDuration records duration for a full traced operation phase.
func RecordTraceOperationDuration(operation, phase, status string, duration time.Duration) {
	if TraceOperationDuration == nil {
		return
	}
	TraceOperationDuration.WithLabelValues(operation, phase, status).Observe(duration.Seconds())
}

// RecordAuditEvent 记录审计事件发生次数。
func RecordAuditEvent(action, resourceType, outcome string) {
	if action == "" {
		action = "unknown"
	}
	if resourceType == "" {
		resourceType = "unknown"
	}
	if outcome == "" {
		outcome = "unknown"
	}
	AuditEventsTotal.WithLabelValues(action, resourceType, outcome).Inc()
}

// RecordAuditFailure 记录审计事件失败次数。
func RecordAuditFailure(action, resourceType string) {
	if action == "" {
		action = "unknown"
	}
	if resourceType == "" {
		resourceType = "unknown"
	}
	AuditEventFailures.WithLabelValues(action, resourceType).Inc()
}

var operationModeGaugeModes = []string{"sync", "queue", "rollout"}

// RecordOperationModeDecision increments counters for mode decisions per component and operation kind.
func RecordOperationModeDecision(component, kind, mode string) {
	if component == "" {
		component = "unknown"
	}
	if kind == "" {
		kind = "unknown"
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "unknown"
	}
	OperationModeDecisions.WithLabelValues(component, kind, mode).Inc()
}

// PublishOperationModeSnapshot updates gauges reflecting the active mode and rollout metadata.
func PublishOperationModeSnapshot(component, mode string, rolloutPercent, allowCount, blockCount int) {
	if component == "" {
		component = "unknown"
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "unknown"
	}
	for _, candidate := range operationModeGaugeModes {
		value := 0.0
		if candidate == mode {
			value = 1.0
		}
		OperationModeCurrent.WithLabelValues(component, candidate).Set(value)
	}
	OperationModeCurrent.WithLabelValues(component, mode).Set(1.0)
	OperationModeRolloutPercent.WithLabelValues(component).Set(float64(rolloutPercent))
	OperationModeAllowlistSize.WithLabelValues(component).Set(float64(allowCount))
	OperationModeBlocklistSize.WithLabelValues(component).Set(float64(blockCount))
}

// RecordUserProtectionEvent 记录用户防护动作的触发次数。
func RecordUserProtectionEvent(eventType string) {
	if eventType == "" {
		eventType = "unknown"
	}
	UserProtectionEvents.WithLabelValues(eventType).Inc()
}

// GetBusinessOperationLabels 获取业务操作的标准标签
func GetBusinessOperationLabels(service, operation string) prometheus.Labels {
	return prometheus.Labels{
		"service":   service,
		"operation": operation,
	}
}

// BusinessOperationTimer 业务操作计时器
type BusinessOperationTimer struct {
	start     time.Time
	service   string
	operation string
	source    string
}

// StartBusinessOperation 开始业务操作监控
func StartBusinessOperation(service, operation, source string) *BusinessOperationTimer {
	BusinessInProgress.WithLabelValues(service, operation).Inc()
	BusinessOperationsTotal.WithLabelValues(service, operation, source).Inc()

	return &BusinessOperationTimer{
		start:     time.Now(),
		service:   service,
		operation: operation,
		source:    source,
	}
}

// EndBusinessOperation 方法需要统一标签调用
func (t *BusinessOperationTimer) EndBusinessOperation(err error) {
	defer BusinessInProgress.WithLabelValues(t.service, t.operation).Dec()

	duration := time.Since(t.start).Seconds()

	// 统一使用 [service, operation] 标签
	BusinessProcessingTime.WithLabelValues(t.service, t.operation).Observe(duration)
	BusinessThroughputStats.WithLabelValues(t.service, t.operation).Observe(duration)

	// 记录成功/失败（现在标签一致了）
	if err != nil {
		errorType := GetBusinessErrorType(err)
		BusinessFailures.WithLabelValues(t.service, t.operation, errorType).Inc()
	} else {
		BusinessSuccess.WithLabelValues(t.service, t.operation, "success").Inc()
	}
}

// -------------------------- 特定业务场景的便捷函数 --------------------------

// MonitorUserCreation 监控用户创建业务
func MonitorUserCreation(source string, fn func() error) error {
	return MonitorBusinessOperation("user_service", "create_user", source, fn)
}

// MonitorUserQuery 监控用户查询业务
func MonitorUserQuery(source string, fn func() error) error {
	return MonitorBusinessOperation("user_service", "query_user", source, fn)
}

// MonitorKafkaMessageProcessing 监控Kafka消息处理业务
func MonitorKafkaMessageProcessing(operation string, fn func() error) error {
	return MonitorBusinessOperation("kafka_consumer", operation, "kafka", fn)
}

// MonitorHTTPRequestBusiness 监控HTTP请求业务逻辑
func MonitorHTTPRequestBusiness(operation string, fn func() error) error {
	return MonitorBusinessOperation("http_service", operation, "http", fn)
}

// -------------------------- Redis集群监控辅助函数 --------------------------

// RedisClusterMetrics 集群指标数据结构
type RedisClusterMetrics struct {
	ClusterState          string
	SlotsAssigned         int64
	SlotsOk               int64
	SlotsPFail            int64
	SlotsFail             int64
	KnownNodes            int64
	ClusterSize           int64
	CurrentEpoch          int64
	MyEpoch               int64
	StatsMessagesSent     int64
	StatsMessagesReceived int64
}

// RecordRedisClusterMetrics 记录Redis集群指标
func RecordRedisClusterMetrics(clusterName string, metrics *RedisClusterMetrics) {
	// 记录集群状态
	stateValue := 0.0
	if metrics.ClusterState == "ok" {
		stateValue = 1.0
	}
	RedisClusterState.WithLabelValues(clusterName).Set(stateValue)

	// 记录槽位信息
	RedisClusterSlotsAssigned.Set(float64(metrics.SlotsAssigned))
	RedisClusterSlotsOk.Set(float64(metrics.SlotsOk))
	RedisClusterSlotsPFail.Set(float64(metrics.SlotsPFail))
	RedisClusterSlotsFail.Set(float64(metrics.SlotsFail))

	// 记录节点数量（假设所有节点都是正常的，实际应该从节点信息计算）
	RedisClusterNodesTotal.Set(float64(metrics.KnownNodes))
	RedisClusterNodesUp.Set(float64(metrics.KnownNodes)) // 简化处理
	RedisClusterNodesDown.Set(0)
}

// RecordRedisNodeMetrics 记录Redis节点指标
func RecordRedisNodeMetrics(nodeID, address, role string, info map[string]string) {
	// 记录节点基本信息
	RedisClusterNodeInfo.WithLabelValues(nodeID, role, address, "default_cluster").Set(1)

	// 记录内存使用
	if usedMemory, ok := info["used_memory"]; ok {
		if memBytes, err := strconv.ParseFloat(usedMemory, 64); err == nil {
			RedisClusterNodeMemory.WithLabelValues(nodeID, address).Set(memBytes)
		}
	}

	// 记录CPU使用
	if usedCpuSys, ok := info["used_cpu_sys"]; ok {
		if cpuUsage, err := strconv.ParseFloat(usedCpuSys, 64); err == nil {
			RedisClusterNodeCPU.WithLabelValues(nodeID, address).Set(cpuUsage)
		}
	}

	// 记录连接数
	if connectedClients, ok := info["connected_clients"]; ok {
		if clients, err := strconv.ParseFloat(connectedClients, 64); err == nil {
			RedisClusterNodeConnections.WithLabelValues(nodeID, address, "clients").Set(clients)
		}
	}

	// 记录键数量 - 这里直接从info参数获取，不通过指标值
	if keyspaceHits, ok := info["keyspace_hits"]; ok {
		if hits, err := strconv.ParseFloat(keyspaceHits, 64); err == nil {
			// 直接使用Add方法增加计数
			RedisClusterKeyspaceHits.Add(hits)
		}
	}

	if keyspaceMisses, ok := info["keyspace_misses"]; ok {
		if misses, err := strconv.ParseFloat(keyspaceMisses, 64); err == nil {
			RedisClusterKeyspaceMisses.Add(misses)
		}
	}
}

// RecordRedisClusterCommand 记录Redis集群命令执行
func RecordRedisClusterCommand(nodeID, address, command string) {
	RedisClusterTotalCommands.WithLabelValues(nodeID, address, command).Inc()
}

// RecordRedisClusterNetworkIO 记录Redis集群网络IO
func RecordRedisClusterNetworkIO(nodeID, address string, inputBytes, outputBytes int64) {
	RedisClusterNetworkIO.WithLabelValues(nodeID, address, "input").Set(float64(inputBytes))
	RedisClusterNetworkIO.WithLabelValues(nodeID, address, "output").Set(float64(outputBytes))
}

// RecordRedisClusterReplicationLag 记录主从复制延迟
func RecordRedisClusterReplicationLag(masterID, slaveID, masterAddr, slaveAddr string, lagSeconds int64) {
	RedisClusterReplicationLag.WithLabelValues(masterID, slaveID, masterAddr, slaveAddr).Set(float64(lagSeconds))
}

// RecordRedisClusterFailover 记录故障转移事件
func RecordRedisClusterFailover() {
	RedisClusterFailoverCount.Inc()
}

// RecordRedisClusterMigration 记录槽位迁移事件
func RecordRedisClusterMigration() {
	RedisClusterMigrationCount.Inc()
}

// UpdateRedisClusterHealthCheck 更新集群健康检查状态
func UpdateRedisClusterHealthCheck(checkType, clusterName string, healthy bool) {
	healthValue := 0.0
	if healthy {
		healthValue = 1.0
	}
	RedisClusterHealthCheck.WithLabelValues(checkType, clusterName).Set(healthValue)
}

// CalculateRedisClusterHitRate 计算集群命中率 - 修正版本
func CalculateRedisClusterHitRate(hits, misses float64) float64 {
	total := hits + misses
	if total == 0 {
		return 0
	}
	return hits / total
}

// UpdateRedisClusterHitRate 更新集群命中率指标 - 修正版本
func UpdateRedisClusterHitRate(hits, misses float64) {
	hitRate := CalculateRedisClusterHitRate(hits, misses)
	RedisClusterHitRate.Set(hitRate)
}

// GetRedisClusterHitRateFromInfo 从Redis INFO命令结果计算命中率
func GetRedisClusterHitRateFromInfo(info map[string]string) float64 {
	var hits, misses float64

	if hitsStr, ok := info["keyspace_hits"]; ok {
		hits, _ = strconv.ParseFloat(hitsStr, 64)
	}

	if missesStr, ok := info["keyspace_misses"]; ok {
		misses, _ = strconv.ParseFloat(missesStr, 64)
	}

	return CalculateRedisClusterHitRate(hits, misses)
}
