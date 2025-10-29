/*
这份 Go 代码基于 prometheus/client_golang 库，定义了Kafka（生产者 / 消费者）、数据库、HTTP、缓存, redis五大核心场景的监控指标（Counter/Gauge/Histogram），并提供了辅助函数用于简化业务代码中指标的上报操作。
所有指标通过手动初始化后注册到 Prometheus 默认注册表（需在 init 函数中调用 prometheus.MustRegister）；函数的核心作用是封装指标的标签赋值、计数 / 观测逻辑，让业务代码无需关注 Prometheus 指标的底层操作，只需传入业务参数即可完成监控上报。
*/
package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// -------------------------- 1. 先声明所有指标变量（仅声明，不初始化）--------------------------
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

	UserCreateStepDuration   *prometheus.HistogramVec
	UserCreateSlowStepsTotal *prometheus.CounterVec

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

	// 数据库操作指标
	// DatabaseQueryDuration    *prometheus.HistogramVec
	// DatabaseQueryErrors      *prometheus.CounterVec
	// DatabaseConnectionsInUse *prometheus.GaugeVec
	// DatabaseConnectionsWait  *prometheus.CounterVec

	DatabasePoolOpenConnections     *prometheus.GaugeVec
	DatabasePoolInUse               *prometheus.GaugeVec
	DatabasePoolIdle                *prometheus.GaugeVec
	DatabasePoolWaitCount           *prometheus.GaugeVec
	DatabasePoolWaitDurationSeconds *prometheus.GaugeVec
	DatabasePoolMaxOpenConnections  *prometheus.GaugeVec
)

// Redis操作指标
var (
	RedisOperations        *prometheus.CounterVec
	RedisOperationDuration *prometheus.HistogramVec
	RedisErrors            *prometheus.CounterVec
	RedisCacheSize         *prometheus.GaugeVec
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
	// HTTP指标 - 手动初始化（原配置不变）
	HTTPResponseTime       *prometheus.HistogramVec
	HTTPRequestsInFlight   prometheus.Gauge // 修正：去掉指针
	HTTPRequestSize        *prometheus.HistogramVec
	HTTPResponseSize       *prometheus.HistogramVec
	HTTPRequestsTotal      *prometheus.CounterVec
	HTTPRequestDuration    *prometheus.HistogramVec
	HTTPRequestsInProgress *prometheus.GaugeVec
	CacheRequests          *prometheus.CounterVec
	SlowHTTPRequests       *prometheus.CounterVec
	HTTPErrors             *prometheus.CounterVec
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
		[]string{"step", "field"},
	)

	UserCreateSlowStepsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "user_create_slow_steps_total",
			Help: "Total number of user create sub-steps exceeding the slow threshold",
		},
		[]string{"step", "field"},
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

	// -------------------------- 初始化：数据库操作指标 --------------------------
	// DatabaseQueryDuration = prometheus.NewHistogramVec(
	// 	prometheus.HistogramOpts{
	// 		Name:    "database_query_duration_seconds",
	// 		Help:    "Time taken for database queries",
	// 		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2},
	// 	},
	// 	[]string{"operation", "table"},
	// )

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

	// DatabaseQueryErrors = prometheus.NewCounterVec(
	// 	prometheus.CounterOpts{
	// 		Name: "database_query_errors_total",
	// 		Help: "Total number of database query errors",
	// 	},
	// 	[]string{"operation", "table", "error_type"},
	// )

	// DatabaseConnectionsInUse = prometheus.NewGaugeVec(
	// 	prometheus.GaugeOpts{
	// 		Name: "database_connections_in_use",
	// 		Help: "Number of database connections currently in use",
	// 	},
	// 	[]string{"pool"},
	// )

	// DatabaseConnectionsWait = prometheus.NewCounterVec(
	// 	prometheus.CounterOpts{
	// 		Name: "database_connections_wait_total",
	// 		Help: "Total number of database connection waits",
	// 	},
	// 	[]string{"pool"},
	// )

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

		// 数据库指标
		// DatabaseQueryDuration,
		// DatabaseQueryErrors,
		// DatabaseConnectionsInUse,
		// DatabaseConnectionsWait,
		DatabasePoolOpenConnections,
		DatabasePoolInUse,
		DatabasePoolIdle,
		DatabasePoolWaitCount,
		DatabasePoolWaitDurationSeconds,
		DatabasePoolMaxOpenConnections,

		// Redis指标
		RedisOperations,
		RedisOperationDuration,
		RedisErrors,
		RedisCacheSize,

		// 新增指标
		ProducerInFlightCurrent,
		WriteLimiterTotal,
		ProducerDeliveryLatency,

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

// 	// 5. 基于错误消息的模式匹配
// 	errMsg := strings.ToLower(err.Error())
// 	switch {
// 	case strings.Contains(errMsg, "timeout"):
// 		return "timeout"
// 	case strings.Contains(errMsg, "connection"):
// 		return "connection_error"
// 	case strings.Contains(errMsg, "deadlock"):
// 		return "deadlock"
// 	case strings.Contains(errMsg, "duplicate"):
// 		return "duplicate"
// 	case strings.Contains(errMsg, "constraint"):
// 		return "constraint_violation"
// 	case strings.Contains(errMsg, "full"):
// 		return "disk_full"
// 	default:
// 		return "unknown"
// 	}
// }

// 标记请求开始
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
	if err == nil {
		return "success"
	}

	// 使用现有的错误分类逻辑
	coder := errors.ParseCoderByErr(err)
	if coder != nil {
		return "business_error"
	}

	errMsg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errMsg, "validation"), strings.Contains(errMsg, "validate"):
		return "validation_error"
	case strings.Contains(errMsg, "timeout"):
		return "timeout"
	case strings.Contains(errMsg, "database"):
		return "database_error"
	case strings.Contains(errMsg, "network"):
		return "network_error"
	case strings.Contains(errMsg, "permission"), strings.Contains(errMsg, "unauthorized"):
		return "permission_error"
	case strings.Contains(errMsg, "marshal"), strings.Contains(errMsg, "unmarshal"):
		return "serialization_error"
	case strings.Contains(errMsg, "not found"), strings.Contains(errMsg, "不存在"):
		return "not_found"
	case strings.Contains(errMsg, "already exists"), strings.Contains(errMsg, "已存在"):
		return "duplicate"
	default:
		return "unknown_error"
	}
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
		errorType := GetBusinessErrorType(err)
		BusinessFailures.WithLabelValues(service, operation, errorType).Inc()
	} else {
		BusinessSuccess.WithLabelValues(service, operation, "success").Inc()
	}

	return err
}

const userCreateSlowThresholdSeconds = 0.2

// RecordUserCreateStep 记录用户创建链路中某个步骤的耗时，并在超过阈值时累计慢步骤计数。
func RecordUserCreateStep(step, field string, duration time.Duration) {
	UserCreateStepDuration.WithLabelValues(step, field).Observe(duration.Seconds())
	if duration.Seconds() > userCreateSlowThresholdSeconds {
		UserCreateSlowStepsTotal.WithLabelValues(step, field).Inc()
	}
}

// RecordBusinessQPS 记录业务QPS（供外部调用）
func RecordBusinessQPS(service, operation string, qps float64) {
	BusinessOperationsRate.WithLabelValues(service, operation).Set(qps)
}

// RecordBusinessErrorRate 记录业务错误率
func RecordBusinessErrorRate(service, operation string, errorRate float64) {
	BusinessErrorRate.WithLabelValues(service, operation).Set(errorRate)
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
