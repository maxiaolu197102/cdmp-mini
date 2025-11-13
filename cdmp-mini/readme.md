## Kafka 参数说明

### 基础连接
- `kafka.brokers`：Kafka Broker 列表，逗号分隔。
- `kafka.topic`：主 Topic 名称。
- `kafka.consumer-group`：消费者组 ID。
- `kafka.fetcher-count`：每个实例并发拉取的 reader 数量。
- `kafka.worker-count`：消费者处理 worker 数量。
- `kafka.instance-id`：消费者实例标识（建议使用 hostname/pod-name）。

### 消费行为
- `kafka.batch-size`：reader 拉取消息的批次大小。
- `kafka.batch-timeout`：reader 拉取批次的超时阈值。
- `kafka.min-bytes` / `kafka.max-bytes`：Fetch 请求允许的最小/最大字节数。
- `kafka.max-retries`：Fetch 或处理失败时的最大重试次数。
- `kafka.batch-channel-capacity`：聚合批处理通道的容量。
- `kafka.min-db-batch-size` / `kafka.max-db-batch-size`：数据库写入批次的最小/最大条数。
- `kafka.min-batch-timeout` / `kafka.max-batch-timeout`：批量刷写的最小/最大延迟。
- `kafka.lag-check-interval` / `kafka.lag-scale-threshold`：滞后检测间隔与触发阈值。

### 消费补偿
- `kafka.retry-worker-count`：重试消费者的 worker 数量。
- `kafka.fallback-retry-enabled`：是否启动降级消息补偿任务。
- `kafka.fallback-retry-interval` / `kafka.fallback-retry-max-attempts`：补偿任务执行间隔及单条消息最大重试次数。
- `kafka.fallback-retry-batch-size`：单次补偿任务处理的消息上限。

### 生产者
- `kafka.required-acks`：写入确认级别（-1/0/1）。
- `kafka.producer-max-inflight`：同步发送并发上限。
- `kafka.flush-frequency` / `kafka.flush-max-messages`：Producer flush 的时间和条目阈值。
- `kafka.producer-compression`：消息压缩算法（none/snappy/gzip/lz4/zstd）。
- `kafka.channel-buffer-size`：异步生产者缓冲区大小。
- `kafka.producer-enqueue-timeout`：异步发送入队最大等待时间。
- `kafka.producer-return-successes` / `kafka.producer-return-errors`：是否开启异步 producer 的成功/错误回调。

### Pending 租约协同参数
- `kafka.pending-lease-ttl`：Pending 租约 TTL，决定 API 与消费者持有幂等标记的最长时间。
- `kafka.pending-metrics-key`：Redis 中用于累计队列深度的指标 Key。
- `kafka.pending-backpressure-window`：累计活跃租约时的滑动窗口。
- `kafka.pending-backpressure-soft` / `kafka.pending-backpressure-hard`：进入 Elevated/Severe 背压的队列深度阈值。
- `kafka.pending-release-retention`：释放租约后保留快照的时间，用于观察回放。
- `kafka.pending-expired-retention`：过期租约快照的保留时间。
- `kafka.pending-expired-grace`：租约过期后的宽限时间，超过后会被标记为 expired。
- `kafka.pending-delay-elevated` / `kafka.pending-delay-elevated-max`：Elevated 背压下建议的最小/最大延迟。
- `kafka.pending-delay-severe` / `kafka.pending-delay-severe-max`：Severe 背压下建议的最小/最大延迟。

所有参数可通过命令行标志或配置文件覆盖，部分参数支持环境变量覆盖（具体参见 `internal/pkg/options/kafka_options.go`）。

## Create API 链路流程
1. **请求接入**：API 收到用户创建请求，解析身份、Trace 信息并生成租约元数据。
2. **队列采样与预延迟**：调用 `PendingCoordinator.SampleQueueDepth` 获取当前队列深度与背压等级；若达到阈值则根据共享延迟配置主动等待，并记录 Trace 标签与指标。
3. **租约申请**：执行 `PendingCoordinator.Acquire`，在 Redis 写入 pending 快照（包含租约所有者、TTL、队列深度、Trace 元数据）。若冲突或过期则返回 `ErrServerBusy`，背压冲突会附带队列深度与等级标签。
4. **业务入库与消息投递**：持有租约的请求继续执行业务校验、数据库写入，并通过 Kafka Producer 推送创建事件。
5. **消费者处理**：`user_consumer` 从 Kafka 拉取消息，同样使用 `PendingCoordinator.SampleQueueDepth` 与 `BackpressureDelay` 控制拉取速度，在持久化成功后更新用户状态并记录 Trace（包含 Fetch Lag 与 Record Age）。
6. **标记清理**：消费者完成处理后，通过 `PendingCoordinator.Release` 删除 pending 快照并写入释放快照；若租约长时间未释放将被 `promoteExpired` 标记为 expired，供 API 和重试消费者感知。
7. **重试与巡检**：`retry_consumer` 使用相同协调器配置扫描过期租约，记录告警并按需触发补偿或人工干预。

整个链路的关键指标（活跃租约、队列深度、背压等级、持有时长等）均通过 Prometheus 暴露，可在运营看板统一观测。API 端与消费者端共享同一套背压阈值与延迟配置，确保入口与后端对负载的感知一致。

### Create API 监管指标
| 指标名称 | 类型 | 关键标签 | 含义 |
| --- | --- | --- | --- |
| `pending_lease_active_total` | Gauge | `component` | 当前活跃的 pending 租约数量，反映 API 与消费者手中的幂等标记总量。|
| `pending_lease_queue_depth` | Gauge | `component` | 近期采样的租约队列深度，用于判断 backlog 长度和触发背压。|
| `pending_lease_backpressure_level` | Gauge | `component` | 依据队列深度计算出的背压等级（0=none，1=elevated，2=severe），指导入口限流与消费者减速。|
| `pending_lease_events_total` | Counter | `component`、`event` | 租约生命周期事件计数（如 `acquire_success`、`backpressure_reject`、`expire_promote`）。|
| `pending_lease_hold_duration_seconds` | Histogram | `component`、`result` | 从租约获取到释放/过期的持有时长，评估处理耗时与堆积风险。|
| `kafka_consumer_message_age_seconds` | Histogram | `component`、`topic`、`group` | 消费者提取消息时的消息年龄，衡量下游处理延迟。|
| `business_operations_total` | Counter | `service`、`operation`、`source` | 业务层操作总量，create API 对应 `service="user_service"`、`operation="create"`。|
| `business_operations_success_total` | Counter | `service`、`operation`、`type` | 成功的业务操作次数，可区分主流程/补偿等 `type`。|
| `business_operations_failures_total` | Counter | `service`、`operation`、`error_type` | 失败的业务操作次数，用于识别错误类型分布。|
| `user_create_step_duration_seconds` | Histogram | `step`、`field` | 用户创建链路各子步骤耗时，例如写库、下发消息。|
| `user_create_slow_steps_total` | Counter | `step`、`field` | 超过慢阈值的步骤计数，用于定位瓶颈字段。|
| `http_requests_total` | Counter | `path`、`method`、`status` 等 | HTTP 调用次数，create API 可通过 `path="/users"`、`method="POST"` 过滤。|
| `http_request_duration_seconds` | Histogram | `path`、`method`、`status`、`error_type` | HTTP 延迟分布，可计算 P95/P99 以衡量入口性能。|
| `http_requests_in_progress` | Gauge | `path`、`method` | 正在处理的 HTTP 请求数量，用于察觉瞬时堆积。|
| `audit_events_total` | Counter | `action`、`resource_type`、`outcome` | 审计事件发出次数，确认 create API 已写审计。|
| `audit_event_failures_total` | Counter | `action`、`resource_type` | 审计写入失败次数，必要时触发告警。|
| `write_rate_limiter_total` | Counter | `path`、`reason` | 入口写限流触发计数，监控是否因背压导致 API 被限流。|
| `redis_operation_duration_seconds` | Histogram | `command`、`component`、`status`（通过 `metrics.RecordRedisOperation` ） | Redis 操作耗时，可用 `component="user_service"` 聚焦租约读写性能。|
