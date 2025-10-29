package options

import (
	"os"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
	"github.com/spf13/pflag"
)

// KafkaOptions 定义Kafka配置选项
type KafkaOptions struct {
	// Broker地址列表
	Brokers []string `json:"brokers" mapstructure:"brokers" validate:"min=1"`

	// Topic名称
	Topic string `json:"topic" mapstructure:"topic" validate:"nonzero"`

	// 消费者组ID
	ConsumerGroup string `json:"consumerGroup" mapstructure:"consumerGroup" validate:"nonzero"`

	// 消息确认机制 (0:无需确认, 1:leader确认, -1:所有副本确认)
	RequiredAcks int `json:"requiredAcks" mapstructure:"requiredAcks" validate:"min=-1,max=1"`

	// 批处理大小
	BatchSize int `json:"batchSize" mapstructure:"batchSize" validate:"min=1"`

	// 批处理超时时间
	BatchTimeout time.Duration `json:"batchTimeout" mapstructure:"batchTimeout" validate:"min=1ms"`

	// 最大重试次数
	MaxRetries int `json:"maxRetries" mapstructure:"maxRetries" validate:"min=0"`

	// 读取消息最小字节数
	MinBytes int `json:"minBytes" mapstructure:"minBytes" validate:"min=1"`

	// 读取消息最大字节数
	MaxBytes int `json:"maxBytes" mapstructure:"maxBytes" validate:"min=1024"`

	// 消费者worker数量
	WorkerCount int `json:"workerCount" mapstructure:"workerCount" validate:"min=1"`

	RetryWorkerCount int `json:"retryWorkerCount" mapstructure:"retryWorkerCount" validate:"min=1"`

	// Metrics refresh configuration for retry/topic metrics
	EnableMetricsRefresh   bool          `json:"enableMetricsRefresh" mapstructure:"enableMetricsRefresh"`
	MetricsRefreshInterval time.Duration `json:"metricsRefreshInterval" mapstructure:"metricsRefreshInterval"`

	// 是否启用SSL
	EnableSSL bool `json:"enableSSL" mapstructure:"enableSSL"`

	// SSL证书路径
	SSLCertFile          string        `json:"sslCertFile" mapstructure:"sslCertFile"`
	BaseRetryDelay       time.Duration `json:"baseretrydelay" mapstructure:"baseretrydelay"`
	MaxRetryDelay        time.Duration `json:"maxretrydelay" mapstructure:"maxretrydelay"`
	AutoCreateTopic      bool          `json:"autoCreateTopic" mapstructure:"autoCreateTopic"`
	DesiredPartitions    int           `json:"desiredPartitions" mapstructure:"desiredPartitions" validate:"min=1"`
	AutoExpandPartitions bool          `json:"autoExpandPartitions" mapstructure:"autoExpandPartitions"`
	// 当消费者滞后超过该阈值时触发保护/扩容（单位：消息数）
	LagScaleThreshold int64 `json:"lagScaleThreshold" mapstructure:"lagScaleThreshold"`
	// 检查滞后间隔
	LagCheckInterval time.Duration `json:"lagCheckInterval" mapstructure:"lagCheckInterval"`
	// 批量写入数据库时每个批次的L最大条数
	MaxDBBatchSize int `json:"maxDBBatchSize" mapstructure:"maxDBBatchSize" validate:"min=1"`
	// 批处理聚合通道容量（防止无界增长导致内存放大）
	BatchChannelCapacity int `json:"batchChannelCapacity" mapstructure:"batchChannelCapacity" validate:"min=1"`
	// 自适应批量写入的最小条数（压力大时收敛到该值）
	MinDBBatchSize int `json:"minDBBatchSize" mapstructure:"minDBBatchSize" validate:"min=1"`
	// 批量聚合的最小/最大超时时间边界
	MinBatchTimeout time.Duration `json:"minBatchTimeout" mapstructure:"minBatchTimeout" validate:"min=1ms"`
	MaxBatchTimeout time.Duration `json:"maxBatchTimeout" mapstructure:"maxBatchTimeout" validate:"min=1ms"`
	// Producer in-flight limit: maximum concurrent synchronous sends allowed
	ProducerMaxInFlight int `json:"producerMaxInFlight" mapstructure:"producerMaxInFlight" validate:"min=1"`
	// 当前是否处于滞后保护状态（true 表示滞后超过阈值）
	LagProtected bool `json:"lagProtected" mapstructure:"lagProtected"`
	// 实例唯一ID（建议用 hostname、pod name、uuid 等保证全局唯一）
	InstanceID string `json:"instanceID" mapstructure:"instanceID"`
	//初始速率
	StartingRate int `json:"startingRate" mapstructure:"startingRate"`
	//最小速率
	MinRate int `json:"minRate" mapstructure:"minRate"`
	//最大速率
	MaxRate int `json:"maxRate" mapstructure:"maxRate"`
	//轮询时间
	AdjustPeriod time.Duration `json:"adjustPeriod" mapstructure:"adjustPeriod"`

	// Sarama producer flush frequency
	FlushFrequency time.Duration `json:"flushFrequency" mapstructure:"flushFrequency"`
	// Sarama producer flush max messages
	FlushMaxMessages int `json:"flushMaxMessages" mapstructure:"flushMaxMessages"`
	// Sarama producer compression codec (none,snappy,gzip,lz4,zstd)
	ProducerCompression string `json:"producerCompression" mapstructure:"producerCompression"`
	// Whether to return successes on the async channel
	ProducerReturnSuccesses bool `json:"producerReturnSuccesses" mapstructure:"producerReturnSuccesses"`
	// Whether to return errors on the async channel
	ProducerReturnErrors bool `json:"producerReturnErrors" mapstructure:"producerReturnErrors"`
	// Sarama channel buffer size for async producer
	ChannelBufferSize int `json:"channelBufferSize" mapstructure:"channelBufferSize"`
	// Maximum duration to wait while enqueuing a message into the async producer
	ProducerEnqueueTimeout time.Duration `json:"producerEnqueueTimeout" mapstructure:"producerEnqueueTimeout"`
	// Enable background fallback compensation job
	FallbackRetryEnabled bool `json:"fallbackRetryEnabled" mapstructure:"fallbackRetryEnabled"`
	// Interval between compensation attempts
	FallbackRetryInterval time.Duration `json:"fallbackRetryInterval" mapstructure:"fallbackRetryInterval"`
	// Maximum attempts before giving up (0 means unlimited)
	FallbackRetryMaxAttempts int `json:"fallbackRetryMaxAttempts" mapstructure:"fallbackRetryMaxAttempts"`
	// Maximum messages to process in a single compensation cycle (0 means no limit)
	FallbackRetryBatchSize int `json:"fallbackRetryBatchSize" mapstructure:"fallbackRetryBatchSize"`
}

// NewKafkaOptions 创建带有默认值的Kafka配置
func NewKafkaOptions() *KafkaOptions {
	return &KafkaOptions{
		Brokers:                  []string{"192.168.10.8:9092", "192.168.10.8:9093", "192.168.10.8:9094"},
		Topic:                    "default-topic",
		ConsumerGroup:            "default-consumer-group",
		RequiredAcks:             1, // leader确认
		BatchSize:                80,
		BatchTimeout:             60 * time.Millisecond,
		MaxRetries:               4,
		MinBytes:                 60 * 1024,        // 10KB
		MaxBytes:                 10 * 1024 * 1024, // 10MB
		WorkerCount:              64,
		RetryWorkerCount:         3,
		EnableMetricsRefresh:     true,
		MetricsRefreshInterval:   30 * time.Second,
		EnableSSL:                false,
		SSLCertFile:              "",
		BaseRetryDelay:           5 * time.Second,
		MaxRetryDelay:            2 * time.Minute,
		AutoCreateTopic:          true,
		DesiredPartitions:        96, //CPU 核数的 2~4 倍设置（如 32、48、64）
		AutoExpandPartitions:     true,
		ProducerMaxInFlight:      40000,
		LagScaleThreshold:        10000,            // 默认滞后阈值
		LagCheckInterval:         30 * time.Second, // 默认滞后检查间隔
		MaxDBBatchSize:           230,              // 默认批量写DB大小
		BatchChannelCapacity:     1024,
		MinDBBatchSize:           120,
		MinBatchTimeout:          40 * time.Millisecond,
		MaxBatchTimeout:          200 * time.Millisecond,
		InstanceID:               "", // 新增字段默认值为空，建议启动时赋值
		StartingRate:             10000,
		MinRate:                  10000,
		MaxRate:                  20000,
		AdjustPeriod:             2 * time.Second,
		FlushFrequency:           200 * time.Millisecond,
		FlushMaxMessages:         1024,
		ProducerCompression:      "snappy",
		ProducerReturnSuccesses:  true,
		ProducerReturnErrors:     true,
		ChannelBufferSize:        1024,
		ProducerEnqueueTimeout:   4000 * time.Millisecond,
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
		k.WorkerCount = 16 // 默认调整为16个worker
	}
	if k.MinBytes <= 0 {
		k.MinBytes = 10 * 1024
	}
	if k.MaxBytes <= 0 {
		k.MaxBytes = 10 * 1024 * 1024
	}

	// 新增：设置合理的分区数默认值
	if k.DesiredPartitions <= 0 {
		k.DesiredPartitions = 48 // 默认16个分区
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

	if k.ChannelBufferSize < 0 {
		k.ChannelBufferSize = 0
	}

	if k.ProducerEnqueueTimeout <= 0 {
		k.ProducerEnqueueTimeout = 500 * time.Millisecond
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

	fs.IntVar(&k.BatchChannelCapacity, "kafka.batch-channel-capacity", k.BatchChannelCapacity,
		"Kafka消费者批处理聚合通道容量（减小可降低内存占用，增大可缓冲突发流量）")

	fs.IntVar(&k.MinDBBatchSize, "kafka.min-db-batch-size", k.MinDBBatchSize,
		"Kafka消费者在高压场景下的最小数据库批量写入条数，用于自适应限流")

	fs.DurationVar(&k.MinBatchTimeout, "kafka.min-batch-timeout", k.MinBatchTimeout,
		"Kafka消费者批量聚合的最小超时时间，用于快速清空堆积")

	fs.DurationVar(&k.MaxBatchTimeout, "kafka.max-batch-timeout", k.MaxBatchTimeout,
		"Kafka消费者批量聚合的最大超时时间，用于在空闲期降低写入频次")

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
