package options

import (
	"os"
	"strconv"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
	"github.com/spf13/pflag"
)

// KafkaOptions 定义Kafka配置选项
type KafkaOptions struct {
	Brokers []string `json:"brokers" mapstructure:"brokers" validate:"min=1"` // Kafka broker 地址列表，至少 1 个 host:port，建议配置多个提升容错。
	Topic   string   `json:"topic" mapstructure:"topic" validate:"nonzero"`   // Topic 名称，必填，长度建议 <255 字符。

	ConsumerGroup string `json:"consumerGroup" mapstructure:"consumerGroup" validate:"nonzero"`    // 消费者组 ID，用于隔离消费进度，必填。
	RequiredAcks  int    `json:"requiredAcks" mapstructure:"requiredAcks" validate:"min=-1,max=1"` // 生产确认等级：-1=全副本，0=无需确认，1=leader，生产环境推荐 1 或 -1。

	BatchSize    int           `json:"batchSize" mapstructure:"batchSize" validate:"min=1"`         // 单批最大全量条数，>=1，常见 50~500。
	BatchTimeout time.Duration `json:"batchTimeout" mapstructure:"batchTimeout" validate:"min=1ms"` // 批处理聚合最大等待时长，>=1ms，通常 10~200ms。
	MaxRetries   int           `json:"maxRetries" mapstructure:"maxRetries" validate:"min=0"`       // 同步发送失败的最大重试次数，>=0，0 表示不重试。
	MinBytes     int           `json:"minBytes" mapstructure:"minBytes" validate:"min=1"`           // 消费者单次 fetch 的最小字节数，>=1，建议 1KB~1MB。
	MaxBytes     int           `json:"maxBytes" mapstructure:"maxBytes" validate:"min=1024"`        // 消费者单次 fetch 的最大字节数，>=1KB，建议不超过 50MB。
	WorkerCount  int           `json:"workerCount" mapstructure:"workerCount" validate:"min=1"`     // 消费 worker 数量，>=1，通常与分区数持平。
	FetcherCount int           `json:"fetcherCount" mapstructure:"fetcherCount" validate:"min=1"`   // 每实例 Kafka Reader 并发数，>=1 且不应大于 WorkerCount。

	RetryWorkerCount int `json:"retryWorkerCount" mapstructure:"retryWorkerCount" validate:"min=1"` // 重试队列 worker 数量，>=1，用于后台补偿任务。

	EnableMetricsRefresh   bool          `json:"enableMetricsRefresh" mapstructure:"enableMetricsRefresh"`     // 是否启用重试/Topic 指标定期刷新，默认 true。
	MetricsRefreshInterval time.Duration `json:"metricsRefreshInterval" mapstructure:"metricsRefreshInterval"` // 指标刷新周期，>0 时生效，典型 10s~5m。

	EnableSSL   bool   `json:"enableSSL" mapstructure:"enableSSL"`     // 是否启用 TLS 连接 Kafka。
	SSLCertFile string `json:"sslCertFile" mapstructure:"sslCertFile"` // TLS 证书文件路径，启用 SSL 时需要配置。

	BaseRetryDelay       time.Duration `json:"baseretrydelay" mapstructure:"baseretrydelay"`                        // 生产端重试的起始延迟，常用 1s~10s。
	MaxRetryDelay        time.Duration `json:"maxretrydelay" mapstructure:"maxretrydelay"`                          // 重试退避的最大延迟上限，通常 30s~5m。
	AutoCreateTopic      bool          `json:"autoCreateTopic" mapstructure:"autoCreateTopic"`                      // Topic 不存在时是否自动创建，需具备对应 ACL。
	DesiredPartitions    int           `json:"desiredPartitions" mapstructure:"desiredPartitions" validate:"min=1"` // 期望分区数，>=1，常与 WorkerCount 对齐。
	AutoExpandPartitions bool          `json:"autoExpandPartitions" mapstructure:"autoExpandPartitions"`            // 是否允许在滞后时自动扩容分区。

	LagScaleThreshold int64         `json:"lagScaleThreshold" mapstructure:"lagScaleThreshold"` // 滞后触发扩容/保护的阈值（条），>=0。
	LagCheckInterval  time.Duration `json:"lagCheckInterval" mapstructure:"lagCheckInterval"`   // 滞后检查周期，>0，典型 5s~60s。

	MaxDBBatchSize         int `json:"maxDBBatchSize" mapstructure:"maxDBBatchSize" validate:"min=1"`                 // 单批写 DB 的最大条目，>=1，受数据库事务限制。
	BatchChannelCapacity   int `json:"batchChannelCapacity" mapstructure:"batchChannelCapacity" validate:"min=1"`     // 消费者批处理缓冲队列容量，>=1，用于控制内存。
	MinDBBatchSize         int `json:"minDBBatchSize" mapstructure:"minDBBatchSize" validate:"min=1"`                 // 自适应批量写入的下限，>=1，需 <= MaxDBBatchSize。
	BatchCreateParallelism int `json:"batchCreateParallelism" mapstructure:"batchCreateParallelism" validate:"min=1"` // 批量创建阶段的 DB 并发度，>=1，建议 <= WorkerCount。

	SkipCacheWrite     bool          `json:"skipCacheWrite" mapstructure:"skipCacheWrite"`         // 是否跳过用户缓存写入，仅用于诊断。
	CacheWriteTimeout  time.Duration `json:"cacheWriteTimeout" mapstructure:"cacheWriteTimeout"`   // Redis 写缓存超时时长，0 表示不限制，建议 0.5s~2s。
	CacheWriteFallback bool          `json:"cacheWriteFallback" mapstructure:"cacheWriteFallback"` // 缓存写失败时是否降级为单键写入用户主体。

	MinBatchTimeout time.Duration `json:"minBatchTimeout" mapstructure:"minBatchTimeout" validate:"min=1ms"` // 批量聚合的最小超时，>=1ms，压力大时用于快速 flush。
	MaxBatchTimeout time.Duration `json:"maxBatchTimeout" mapstructure:"maxBatchTimeout" validate:"min=1ms"` // 批量聚合的最大超时，>=1ms 且应 >= MinBatchTimeout。

	PendingLeaseTTL           time.Duration `json:"pendingLeaseTTL" mapstructure:"pendingLeaseTTL"`                     // Pending 租约 TTL，必须 >= MinUserPendingCreateTTL，保障创建幂等。
	PendingMetricsKey         string        `json:"pendingMetricsKey" mapstructure:"pendingMetricsKey"`                 // Pending 活跃度指标使用的 Redis Key，不能为空。
	PendingBackpressureWindow time.Duration `json:"pendingBackpressureWindow" mapstructure:"pendingBackpressureWindow"` // Pending 深度采样窗口，>0，典型 1s~10s。
	PendingBackpressureSoft   int           `json:"pendingBackpressureSoft" mapstructure:"pendingBackpressureSoft"`     // Pending 软阈值（条），超过进入 Elevated。
	PendingBackpressureHard   int           `json:"pendingBackpressureHard" mapstructure:"pendingBackpressureHard"`     // Pending 硬阈值（条），>= Soft，触发 Severe。
	PendingReleaseRetention   time.Duration `json:"pendingReleaseRetention" mapstructure:"pendingReleaseRetention"`     // Pending 释放快照保留时长，>=0，典型 1s~5s。
	PendingExpiredRetention   time.Duration `json:"pendingExpiredRetention" mapstructure:"pendingExpiredRetention"`     // Pending 过期快照保留时长，>=0，建议 10s~60s。
	PendingExpiredGrace       time.Duration `json:"pendingExpiredGrace" mapstructure:"pendingExpiredGrace"`             // Pending 过期后的宽限期，>=0，用于补偿慢任务。
	PendingDelayElevated      time.Duration `json:"pendingDelayElevated" mapstructure:"pendingDelayElevated"`           // Elevated 背压建议的最小延迟阈值。
	PendingDelayElevatedMax   time.Duration `json:"pendingDelayElevatedMax" mapstructure:"pendingDelayElevatedMax"`     // Elevated 背压最大延迟，需 >= PendingDelayElevated。
	PendingDelaySevere        time.Duration `json:"pendingDelaySevere" mapstructure:"pendingDelaySevere"`               // Severe 背压建议的最小延迟阈值。
	PendingDelaySevereMax     time.Duration `json:"pendingDelaySevereMax" mapstructure:"pendingDelaySevereMax"`         // Severe 背压最大延迟，需 >= PendingDelaySevere。

	ProducerMaxInFlight int    `json:"producerMaxInFlight" mapstructure:"producerMaxInFlight" validate:"min=1"` // 允许同时在途的同步请求数，>=1，过大可能挤压内存。
	LagProtected        bool   `json:"lagProtected" mapstructure:"lagProtected"`                                // 是否处于滞后保护态（true 表示滞后超过阈值）。
	InstanceID          string `json:"instanceID" mapstructure:"instanceID"`                                    // 实例唯一 ID，建议 hostname/pod/UUID，便于排障。

	StartingRate int           `json:"startingRate" mapstructure:"startingRate"` // 自适应限流的初始速率（条/秒），应 >= MinRate。
	MinRate      int           `json:"minRate" mapstructure:"minRate"`           // 自适应限流最小速率，>=1，防止降至 0。
	MaxRate      int           `json:"maxRate" mapstructure:"maxRate"`           // 自适应限流最大速率，需 >= MinRate。
	AdjustPeriod time.Duration `json:"adjustPeriod" mapstructure:"adjustPeriod"` // 限流调节周期，>0，典型 500ms~5s。

	FlushFrequency      time.Duration `json:"flushFrequency" mapstructure:"flushFrequency"`           // Sarama Flush 定时器，0 表示禁用，常用 5~50ms。
	FlushMaxMessages    int           `json:"flushMaxMessages" mapstructure:"flushMaxMessages"`       // 单次 Flush 的最大消息数，>=1。
	ProducerCompression string        `json:"producerCompression" mapstructure:"producerCompression"` // 生产者压缩算法：none/snappy/gzip/lz4/zstd。

	ProducerReturnSuccesses bool `json:"producerReturnSuccesses" mapstructure:"producerReturnSuccesses"` // 异步生产者是否返回 success 事件，开启可做监控但增加通道压力。
	ProducerReturnErrors    bool `json:"producerReturnErrors" mapstructure:"producerReturnErrors"`       // 异步生产者是否返回 error 事件，用于监控报警。

	ChannelBufferSize      int           `json:"channelBufferSize" mapstructure:"channelBufferSize"`           // Sarama 异步通道缓冲大小，>=0，0 表示采用默认。
	ProducerEnqueueTimeout time.Duration `json:"producerEnqueueTimeout" mapstructure:"producerEnqueueTimeout"` // 异步生产者入队等待上限，>0，超时触发降级。

	FallbackRetryEnabled     bool          `json:"fallbackRetryEnabled" mapstructure:"fallbackRetryEnabled"`         // 是否启用降级文件的后台补偿任务。
	FallbackRetryInterval    time.Duration `json:"fallbackRetryInterval" mapstructure:"fallbackRetryInterval"`       // 补偿任务调度间隔，>0，常见 10s~5m。
	FallbackRetryMaxAttempts int           `json:"fallbackRetryMaxAttempts" mapstructure:"fallbackRetryMaxAttempts"` // 单条消息最大补偿次数，>=0，0 表示无限重试。
	FallbackRetryBatchSize   int           `json:"fallbackRetryBatchSize" mapstructure:"fallbackRetryBatchSize"`     // 单次补偿处理的最大消息数，>=0，0 表示不限制。
}

// NewKafkaOptions 创建带有默认值的Kafka配置
func NewKafkaOptions() *KafkaOptions {
	return &KafkaOptions{
		Brokers:                   []string{"192.168.10.8:9092", "192.168.10.8:9093", "192.168.10.8:9094"},
		Topic:                     "user.create.v1",
		ConsumerGroup:             "",
		RequiredAcks:              1, // leader确认
		BatchSize:                 80,
		BatchTimeout:              60 * time.Millisecond,
		MaxRetries:                4,
		MinBytes:                  60 * 1024,        // 10KB
		MaxBytes:                  10 * 1024 * 1024, // 10MB
		WorkerCount:               64,
		FetcherCount:              4,
		RetryWorkerCount:          3,
		EnableMetricsRefresh:      true,
		MetricsRefreshInterval:    30 * time.Second,
		EnableSSL:                 false,
		SSLCertFile:               "",
		BaseRetryDelay:            5 * time.Second,
		MaxRetryDelay:             2 * time.Minute,
		AutoCreateTopic:           true,
		DesiredPartitions:         64, // 默认按照现有 64 个分区并发消费
		AutoExpandPartitions:      true,
		ProducerMaxInFlight:       40000,
		LagScaleThreshold:         10000,            // 默认滞后阈值
		LagCheckInterval:          30 * time.Second, // 默认滞后检查间隔
		MaxDBBatchSize:            230,              // 默认批量写DB大小
		BatchChannelCapacity:      1024,
		MinDBBatchSize:            120,
		MinBatchTimeout:           40 * time.Millisecond,
		MaxBatchTimeout:           200 * time.Millisecond,
		BatchCreateParallelism:    16,
		SkipCacheWrite:            false,
		CacheWriteTimeout:         1500 * time.Millisecond,
		CacheWriteFallback:        true,
		PendingLeaseTTL:           MinUserPendingCreateTTL,
		PendingMetricsKey:         "user:pending:active",
		PendingBackpressureWindow: 5 * time.Second,
		PendingBackpressureSoft:   1000,
		PendingBackpressureHard:   1500,
		PendingReleaseRetention:   3 * time.Second,
		PendingExpiredRetention:   30 * time.Second,
		PendingExpiredGrace:       2 * time.Second,
		PendingDelayElevated:      20 * time.Millisecond,
		PendingDelayElevatedMax:   45 * time.Millisecond,
		PendingDelaySevere:        80 * time.Millisecond,
		PendingDelaySevereMax:     150 * time.Millisecond,
		InstanceID:                "", // 新增字段默认值为空，建议启动时赋值
		StartingRate:              10000,
		MinRate:                   10000,
		MaxRate:                   20000,
		AdjustPeriod:              2 * time.Second,
		// 默认关闭基于时间的 flush，仅依靠批量阈值触发，避免 200ms 定时器带来的额外 ACK 延迟
		FlushFrequency:           8 * time.Millisecond,
		FlushMaxMessages:         256,
		ProducerCompression:      "snappy",
		ProducerReturnSuccesses:  true,
		ProducerReturnErrors:     true,
		ChannelBufferSize:        1024,
		ProducerEnqueueTimeout:   10 * time.Second,
		FallbackRetryEnabled:     true,
		FallbackRetryInterval:    30 * time.Second,
		FallbackRetryMaxAttempts: 5,
		FallbackRetryBatchSize:   5000,
	}
}

// Complete 完成配置的最终处理
func (k *KafkaOptions) Complete() {
	// 从环境变量获取配置（如果存在）
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		k.Brokers = k.parseBrokersFromEnv(envBrokers)
	}
	if envTopic := os.Getenv("KAFKA_TOPIC"); envTopic != "" {
		k.Topic = envTopic
	}
	if envGroup := os.Getenv("KAFKA_CONSUMER_GROUP"); envGroup != "" {
		k.ConsumerGroup = envGroup
	}
	if envFetcher := os.Getenv("KAFKA_FETCHER_COUNT"); envFetcher != "" {
		if parsed, err := strconv.Atoi(envFetcher); err == nil && parsed > 0 {
			k.FetcherCount = parsed
		}
	}

	// 设置合理的默认值
	if len(k.Brokers) == 0 {
		k.Brokers = []string{"localhost:9092"}
	}
	if k.BatchSize <= 0 {
		k.BatchSize = 100
	}
	if k.BatchTimeout <= 0 {
		k.BatchTimeout = 100 * time.Millisecond
	}
	if k.WorkerCount <= 0 {
		k.WorkerCount = 5
	}
	if k.FetcherCount <= 0 {
		k.FetcherCount = 1
	}
	if k.MinBytes <= 0 {
		k.MinBytes = 10 * 1024
	}
	if k.MaxBytes <= 0 {
		k.MaxBytes = 10 * 1024 * 1024
	}

	// 设置合理的默认值
	if len(k.Brokers) == 0 {
		k.Brokers = []string{"localhost:9092"}
	}
	if k.BatchSize <= 0 {
		k.BatchSize = 100
	}
	if k.BatchTimeout <= 0 {
		k.BatchTimeout = 100 * time.Millisecond
	}
	if k.WorkerCount <= 0 {
		k.WorkerCount = k.DesiredPartitions
	}
	if k.MinBytes <= 0 {
		k.MinBytes = 10 * 1024
	}
	if k.MaxBytes <= 0 {
		k.MaxBytes = 10 * 1024 * 1024
	}

	// 新增：设置合理的分区数默认值
	if k.DesiredPartitions <= 0 {
		k.DesiredPartitions = 64
	}
	if k.WorkerCount <= 0 {
		k.WorkerCount = k.DesiredPartitions
	}
	if k.MetricsRefreshInterval <= 0 {
		k.MetricsRefreshInterval = 30 * time.Second
	}
	// 默认启用周期性指标刷新
	// 如果未显式配置，则保持默认 true
	// 确保worker数量不超过分区数
	if k.WorkerCount > k.DesiredPartitions {
		log.Warnf("Worker数量(%d)超过分区数(%d)，部分worker可能空闲",
			k.WorkerCount, k.DesiredPartitions)
	}

	if k.FetcherCount > k.WorkerCount {
		log.Warnf("Fetcher数量(%d)超过worker数量(%d)，将自动收敛到worker数量", k.FetcherCount, k.WorkerCount)
		k.FetcherCount = k.WorkerCount
	}
	if k.FetcherCount > k.DesiredPartitions {
		log.Warnf("Fetcher数量(%d)超过分区数(%d)，将自动收敛到分区数", k.FetcherCount, k.DesiredPartitions)
		k.FetcherCount = k.DesiredPartitions
	}

	if k.BatchCreateParallelism <= 0 {
		k.BatchCreateParallelism = 16
	}
	if k.WorkerCount > 0 && k.BatchCreateParallelism > k.WorkerCount {
		k.BatchCreateParallelism = k.WorkerCount
	}
	if k.CacheWriteTimeout < 0 {
		k.CacheWriteTimeout = 0
	}

	if k.ChannelBufferSize < 0 {
		k.ChannelBufferSize = 0
	}

	if k.ProducerEnqueueTimeout <= 0 {
		k.ProducerEnqueueTimeout = 2 * time.Second
	}

	if k.ProducerCompression == "" {
		k.ProducerCompression = "snappy"
	}

	if k.FallbackRetryInterval <= 0 {
		k.FallbackRetryInterval = time.Minute
	}

	if k.FallbackRetryBatchSize < 0 {
		k.FallbackRetryBatchSize = 0
	}

	if k.FallbackRetryMaxAttempts < 0 {
		k.FallbackRetryMaxAttempts = 0
	}

	if k.BatchChannelCapacity <= 0 {
		k.BatchChannelCapacity = 1024
	}

	if k.MaxDBBatchSize <= 0 {
		k.MaxDBBatchSize = 230
	}

	if k.MinDBBatchSize <= 0 || k.MinDBBatchSize > k.MaxDBBatchSize {
		k.MinDBBatchSize = k.MaxDBBatchSize / 2
		if k.MinDBBatchSize <= 0 {
			k.MinDBBatchSize = 1
		}
	}

	if k.MinBatchTimeout <= 0 {
		k.MinBatchTimeout = 40 * time.Millisecond
	}
	if k.MaxBatchTimeout <= 0 {
		k.MaxBatchTimeout = 200 * time.Millisecond
	}
	if k.MaxBatchTimeout < k.MinBatchTimeout {
		k.MaxBatchTimeout = k.MinBatchTimeout
	}
	if k.BatchTimeout < k.MinBatchTimeout {
		k.BatchTimeout = k.MinBatchTimeout
	} else if k.BatchTimeout > k.MaxBatchTimeout {
		k.BatchTimeout = k.MaxBatchTimeout
	}
	if k.PendingLeaseTTL <= 0 {
		k.PendingLeaseTTL = MinUserPendingCreateTTL
	}
	if k.PendingLeaseTTL < MinUserPendingCreateTTL {
		k.PendingLeaseTTL = MinUserPendingCreateTTL
	}
	if k.PendingMetricsKey == "" {
		k.PendingMetricsKey = "user:pending:active"
	}
	if k.PendingBackpressureWindow <= 0 {
		k.PendingBackpressureWindow = 5 * time.Second
	}
	if k.PendingBackpressureSoft <= 0 {
		k.PendingBackpressureSoft = 1000
	}
	if k.PendingBackpressureHard <= 0 {
		k.PendingBackpressureHard = k.PendingBackpressureSoft + 500
	}
	if k.PendingBackpressureHard < k.PendingBackpressureSoft {
		k.PendingBackpressureHard = k.PendingBackpressureSoft
	}
	if k.PendingReleaseRetention <= 0 {
		k.PendingReleaseRetention = 3 * time.Second
	}
	if k.PendingExpiredRetention <= 0 {
		k.PendingExpiredRetention = 30 * time.Second
	}
	if k.PendingExpiredGrace < 0 {
		k.PendingExpiredGrace = 0
	}
	if k.PendingDelayElevated <= 0 {
		k.PendingDelayElevated = 20 * time.Millisecond
	}
	if k.PendingDelayElevatedMax <= 0 {
		k.PendingDelayElevatedMax = k.PendingDelayElevated
	}
	if k.PendingDelayElevatedMax < k.PendingDelayElevated {
		k.PendingDelayElevatedMax = k.PendingDelayElevated
	}
	if k.PendingDelaySevere <= 0 {
		k.PendingDelaySevere = 80 * time.Millisecond
	}
	if k.PendingDelaySevereMax <= 0 {
		k.PendingDelaySevereMax = k.PendingDelaySevere
	}
	if k.PendingDelaySevereMax < k.PendingDelaySevere {
		k.PendingDelaySevereMax = k.PendingDelaySevere
	}
}

// Validate 验证配置的有效性
func (k *KafkaOptions) Validate() []error {
	var errs []error

	// 验证brokers
	if len(k.Brokers) == 0 {
		errs = append(errs, field.Required(field.NewPath("kafka", "brokers"), "必须指定至少一个Kafka broker地址"))
	}

	for i, broker := range k.Brokers {
		if broker == "" {
			errs = append(errs, field.Required(field.NewPath("kafka", "brokers").Index(i), "broker地址不能为空"))
		}
	}

	// 验证topic
	if k.Topic == "" {
		errs = append(errs, field.Required(field.NewPath("kafka", "topic"), "必须指定Kafka topic名称"))
	} else if len(k.Topic) > 255 {
		errs = append(errs, field.TooLong(field.NewPath("kafka", "topic"), k.Topic, 255))
	}

	// 验证consumer group
	if k.ConsumerGroup == "" {
		errs = append(errs, field.Required(field.NewPath("kafka", "consumerGroup"), "必须指定消费者组ID"))
	}

	// 验证required acks
	if k.RequiredAcks < -1 || k.RequiredAcks > 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "requiredAcks"), k.RequiredAcks, "必须为-1, 0或1"))
	}

	// 验证batch大小
	if k.BatchSize < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "batchSize"), k.BatchSize, "必须大于0"))
	}

	// 验证超时时间
	if k.BatchTimeout < time.Millisecond {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "batchTimeout"), k.BatchTimeout, "必须大于1ms"))
	}

	// 验证worker数量
	if k.WorkerCount < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "workerCount"), k.WorkerCount, "必须大于0"))
	}
	if k.FetcherCount < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "fetcherCount"), k.FetcherCount, "必须大于0"))
	}

	// 验证producer compression codec
	switch k.ProducerCompression {
	case "", "none", "snappy", "gzip", "lz4", "zstd":
	default:
		errs = append(errs, field.Invalid(field.NewPath("kafka", "producerCompression"), k.ProducerCompression, "必须为 none,snappy,gzip,lz4 或 zstd"))
	}

	if k.ChannelBufferSize < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "channelBufferSize"), k.ChannelBufferSize, "必须大于等于0"))
	}

	if k.ProducerEnqueueTimeout <= 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "producerEnqueueTimeout"), k.ProducerEnqueueTimeout, "必须大于0"))
	}

	if k.FallbackRetryInterval <= 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "fallbackRetryInterval"), k.FallbackRetryInterval, "必须大于0"))
	}

	if k.FallbackRetryBatchSize < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "fallbackRetryBatchSize"), k.FallbackRetryBatchSize, "必须大于等于0"))
	}

	if k.FallbackRetryMaxAttempts < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "fallbackRetryMaxAttempts"), k.FallbackRetryMaxAttempts, "必须大于等于0"))
	}

	if k.BatchChannelCapacity < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "batchChannelCapacity"), k.BatchChannelCapacity, "必须大于0"))
	}

	if k.BatchCreateParallelism < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "batchCreateParallelism"), k.BatchCreateParallelism, "必须大于0"))
	}

	if k.MinDBBatchSize < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "minDBBatchSize"), k.MinDBBatchSize, "必须大于0"))
	}
	if k.MinDBBatchSize > k.MaxDBBatchSize {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "minDBBatchSize"), k.MinDBBatchSize, "不能大于maxDBBatchSize"))
	}

	if k.MinBatchTimeout < time.Millisecond {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "minBatchTimeout"), k.MinBatchTimeout, "必须大于1ms"))
	}
	if k.MaxBatchTimeout < k.MinBatchTimeout {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "maxBatchTimeout"), k.MaxBatchTimeout, "必须不小于minBatchTimeout"))
	}
	if k.PendingLeaseTTL < MinUserPendingCreateTTL {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingLeaseTTL"), k.PendingLeaseTTL, "不能小于最小租约TTL"))
	}
	if k.PendingMetricsKey == "" {
		errs = append(errs, field.Required(field.NewPath("kafka", "pendingMetricsKey"), "必须指定pending指标Key"))
	}
	if k.PendingBackpressureWindow <= 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingBackpressureWindow"), k.PendingBackpressureWindow, "必须大于0"))
	}
	if k.PendingBackpressureSoft < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingBackpressureSoft"), k.PendingBackpressureSoft, "必须大于0"))
	}
	if k.PendingBackpressureHard < k.PendingBackpressureSoft {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingBackpressureHard"), k.PendingBackpressureHard, "必须不小于soft limit"))
	}
	if k.PendingReleaseRetention < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingReleaseRetention"), k.PendingReleaseRetention, "不能小于0"))
	}
	if k.PendingExpiredRetention < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingExpiredRetention"), k.PendingExpiredRetention, "不能小于0"))
	}
	if k.PendingExpiredGrace < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingExpiredGrace"), k.PendingExpiredGrace, "不能小于0"))
	}
	if k.PendingDelayElevated <= 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingDelayElevated"), k.PendingDelayElevated, "必须大于0"))
	}
	if k.PendingDelayElevatedMax < k.PendingDelayElevated {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingDelayElevatedMax"), k.PendingDelayElevatedMax, "不能小于pendingDelayElevated"))
	}
	if k.PendingDelaySevere <= 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingDelaySevere"), k.PendingDelaySevere, "必须大于0"))
	}
	if k.PendingDelaySevereMax < k.PendingDelaySevere {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "pendingDelaySevereMax"), k.PendingDelaySevereMax, "不能小于pendingDelaySevere"))
	}
	if k.CacheWriteTimeout < 0 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "cacheWriteTimeout"), k.CacheWriteTimeout, "不能小于0"))
	}

	// 如果启用SSL，验证证书文件
	if k.EnableSSL && k.SSLCertFile != "" {
		if _, err := os.Stat(k.SSLCertFile); os.IsNotExist(err) {
			errs = append(errs, field.Invalid(field.NewPath("kafka", "sslCertFile"), k.SSLCertFile, "SSL证书文件不存在"))
		}
	}

	// 验证分区数
	if k.DesiredPartitions < 1 {
		errs = append(errs, field.Invalid(field.NewPath("kafka", "partitions"),
			k.DesiredPartitions, "分区数必须大于0"))
	}

	// 验证worker数量与分区的合理性（警告级别，不阻断启动）
	if k.WorkerCount > k.DesiredPartitions {
		log.Warnf("配置警告: worker数量(%d)超过分区数(%d)，建议调整配置",
			k.WorkerCount, k.DesiredPartitions)
	}

	return errs
}

// AddFlags 添加命令行标志
func (k *KafkaOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&k.Brokers, "kafka.brokers", k.Brokers,
		"Kafka broker地址列表 (例如: localhost:9092,broker2:9092)。也可以通过环境变量 KAFKA_BROKERS 设置")

	fs.StringVar(&k.Topic, "kafka.topic", k.Topic,
		"Kafka topic名称。也可以通过环境变量 KAFKA_TOPIC 设置")

	fs.StringVar(&k.ConsumerGroup, "kafka.consumer-group", k.ConsumerGroup,
		"Kafka消费者组ID。也可以通过环境变量 KAFKA_CONSUMER_GROUP 设置")

	fs.IntVar(&k.RequiredAcks, "kafka.required-acks", k.RequiredAcks,
		"消息确认机制: -1=所有副本确认, 0=无需确认, 1=leader确认")

	fs.IntVar(&k.BatchSize, "kafka.batch-size", k.BatchSize,
		"生产者批处理大小")

	fs.DurationVar(&k.BatchTimeout, "kafka.batch-timeout", k.BatchTimeout,
		"生产者批处理超时时间")

	fs.IntVar(&k.MaxRetries, "kafka.max-retries", k.MaxRetries,
		"最大重试次数")

	fs.IntVar(&k.MinBytes, "kafka.min-bytes", k.MinBytes,
		"消费者读取最小字节数")

	fs.IntVar(&k.MaxBytes, "kafka.max-bytes", k.MaxBytes,
		"消费者读取最大字节数")

	fs.IntVar(&k.WorkerCount, "kafka.worker-count", k.WorkerCount,
		"消费者worker数量")

	fs.IntVar(&k.FetcherCount, "kafka.fetcher-count", k.FetcherCount,
		"每个消费者实例使用的Kafka reader数量，用于并发拉取消息")

	fs.IntVar(&k.BatchChannelCapacity, "kafka.batch-channel-capacity", k.BatchChannelCapacity,
		"Kafka消费者批处理聚合通道容量（减小可降低内存占用，增大可缓冲突发流量）")

	fs.IntVar(&k.MinDBBatchSize, "kafka.min-db-batch-size", k.MinDBBatchSize,
		"Kafka消费者在高压场景下的最小数据库批量写入条数，用于自适应限流")

	fs.IntVar(&k.BatchCreateParallelism, "kafka.batch-create-parallelism", k.BatchCreateParallelism,
		"批量创建场景中单个批次在数据库写入阶段使用的最大并发度")

	fs.BoolVar(&k.SkipCacheWrite, "kafka.skip-cache-write", k.SkipCacheWrite,
		"是否跳过用户缓存写入（诊断用途）")
	fs.DurationVar(&k.CacheWriteTimeout, "kafka.cache-write-timeout", k.CacheWriteTimeout,
		"用户缓存写入的最大等待时间，0 表示不做超时限制")
	fs.BoolVar(&k.CacheWriteFallback, "kafka.cache-write-fallback", k.CacheWriteFallback,
		"Redis 写入失败时是否降级为单键写入用户主体缓存")

	fs.DurationVar(&k.MinBatchTimeout, "kafka.min-batch-timeout", k.MinBatchTimeout,
		"Kafka消费者批量聚合的最小超时时间，用于快速清空堆积")

	fs.DurationVar(&k.MaxBatchTimeout, "kafka.max-batch-timeout", k.MaxBatchTimeout,
		"Kafka消费者批量聚合的最大超时时间，用于在空闲期降低写入频次")

	fs.DurationVar(&k.PendingLeaseTTL, "kafka.pending-lease-ttl", k.PendingLeaseTTL,
		"Pending租约TTL，用于确保创建链路在高延迟时仍可恢复")
	fs.StringVar(&k.PendingMetricsKey, "kafka.pending-metrics-key", k.PendingMetricsKey,
		"Pending租约活跃指标计数使用的Redis Key")
	fs.DurationVar(&k.PendingBackpressureWindow, "kafka.pending-backpressure-window", k.PendingBackpressureWindow,
		"Pending租约采样窗口，用于计算活跃队列深度")
	fs.IntVar(&k.PendingBackpressureSoft, "kafka.pending-backpressure-soft", k.PendingBackpressureSoft,
		"Pending租约软阈值，超过后进入Elevated背压")
	fs.IntVar(&k.PendingBackpressureHard, "kafka.pending-backpressure-hard", k.PendingBackpressureHard,
		"Pending租约硬阈值，超过后进入Severe背压")
	fs.DurationVar(&k.PendingReleaseRetention, "kafka.pending-release-retention", k.PendingReleaseRetention,
		"Pending租约释放快照保留时长")
	fs.DurationVar(&k.PendingExpiredRetention, "kafka.pending-expired-retention", k.PendingExpiredRetention,
		"Pending租约过期快照保留时长")
	fs.DurationVar(&k.PendingExpiredGrace, "kafka.pending-expired-grace", k.PendingExpiredGrace,
		"Pending租约过期后的宽限期")
	fs.DurationVar(&k.PendingDelayElevated, "kafka.pending-delay-elevated", k.PendingDelayElevated,
		"Elevated背压等级建议的最小延迟")
	fs.DurationVar(&k.PendingDelayElevatedMax, "kafka.pending-delay-elevated-max", k.PendingDelayElevatedMax,
		"Elevated背压等级建议的最大延迟")
	fs.DurationVar(&k.PendingDelaySevere, "kafka.pending-delay-severe", k.PendingDelaySevere,
		"Severe背压等级建议的最小延迟")
	fs.DurationVar(&k.PendingDelaySevereMax, "kafka.pending-delay-severe-max", k.PendingDelaySevereMax,
		"Severe背压等级建议的最大延迟")

	fs.BoolVar(&k.EnableSSL, "kafka.enable-ssl", k.EnableSSL,
		"是否启用SSL连接")

	fs.StringVar(&k.SSLCertFile, "kafka.ssl-cert-file", k.SSLCertFile,
		"SSL证书文件路径")

	// 新增分区管理标志
	fs.BoolVar(&k.AutoCreateTopic, "kafka.auto-create-topic", k.AutoCreateTopic,
		"是否自动创建不存在的topic")

	fs.IntVar(&k.DesiredPartitions, "kafka.partitions", k.DesiredPartitions,
		"期望的分区数量")

	fs.BoolVar(&k.AutoExpandPartitions, "kafka.auto-expand-partitions", k.AutoExpandPartitions,
		"是否自动扩展分区")

	// 新增：实例ID参数
	fs.StringVar(&k.InstanceID, "kafka.instance-id", k.InstanceID, "Kafka消费者实例唯一ID（建议用hostname、pod name、uuid等保证全局唯一）。也可通过环境变量 KAFKA_INSTANCE_ID 设置")
	// 新增：从环境变量获取实例ID
	if envInstanceID := os.Getenv("KAFKA_INSTANCE_ID"); envInstanceID != "" {
		k.InstanceID = envInstanceID
	}
	// 若仍为空，自动用主机名兜底
	if k.InstanceID == "" {
		host, err := os.Hostname()
		if err == nil {
			k.InstanceID = host
		}
	}

	fs.DurationVar(&k.FlushFrequency, "kafka.flush-frequency", k.FlushFrequency, "Sarama producer flush frequency.")
	fs.IntVar(&k.FlushMaxMessages, "kafka.flush-max-messages", k.FlushMaxMessages, "Sarama producer flush max messages.")
	fs.StringVar(&k.ProducerCompression, "kafka.producer-compression", k.ProducerCompression, "Sarama producer compression codec (none,snappy,gzip,lz4,zstd).")
	fs.BoolVar(&k.ProducerReturnSuccesses, "kafka.producer-return-successes", k.ProducerReturnSuccesses, "Whether Sarama async producer should return successes.")
	fs.BoolVar(&k.ProducerReturnErrors, "kafka.producer-return-errors", k.ProducerReturnErrors, "Whether Sarama async producer should return errors.")
	fs.IntVar(&k.ChannelBufferSize, "kafka.channel-buffer-size", k.ChannelBufferSize, "Sarama async producer channel buffer size. 0表示使用默认值。")
	fs.DurationVar(&k.ProducerEnqueueTimeout, "kafka.producer-enqueue-timeout", k.ProducerEnqueueTimeout, "异步生产者入队等待的最大时长，超过则触发降级。")
	fs.BoolVar(&k.FallbackRetryEnabled, "kafka.fallback-retry-enabled", k.FallbackRetryEnabled, "是否启用本地降级消息的后台补偿任务。")
	fs.DurationVar(&k.FallbackRetryInterval, "kafka.fallback-retry-interval", k.FallbackRetryInterval, "后台补偿任务的执行间隔。")
	fs.IntVar(&k.FallbackRetryMaxAttempts, "kafka.fallback-retry-max-attempts", k.FallbackRetryMaxAttempts, "每条降级消息的最大重试次数，0 表示无限重试。")
	fs.IntVar(&k.FallbackRetryBatchSize, "kafka.fallback-retry-batch-size", k.FallbackRetryBatchSize, "单次补偿任务处理的最大消息数，0 表示不限制。")
}

// parseBrokersFromEnv 从环境变量字符串解析broker列表
func (k *KafkaOptions) parseBrokersFromEnv(envBrokers string) []string {
	// 这里可以添加解析逻辑，比如逗号分隔的字符串转数组
	// 但因为我们使用StringSliceVar，pflag会自动处理
	// 如果需要自定义解析逻辑可以在这里实现
	return []string{envBrokers} // 简单实现，实际可能需要分割字符串
}

// IsValid 检查配置是否有效
func (k *KafkaOptions) IsValid() bool {
	return len(k.Validate()) == 0
}

// GetRequiredAcks 获取kafka.RequiredAcks类型
func (k *KafkaOptions) GetRequiredAcks() int {
	return k.RequiredAcks
}

// GetBrokers 获取broker列表
func (k *KafkaOptions) GetBrokers() []string {
	return k.Brokers
}
