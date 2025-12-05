package server

import (
	"context"
	stdErrors "errors"
	"os"
	"sync"
	"sync/atomic"

	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/user"

	mysql "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/store/interfaces"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/middleware"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/ratelimiter"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/consumer"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/server/producer"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/db"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
	"gorm.io/gorm"
)

const (
	userProducerKey          = "apiserver.users"
	userOperationConsumerKey = "apiserver.users.operations"
	userRetryConsumerKey     = "apiserver.users.retry"

	mysqlHeartbeatDefaultInterval = 30 * time.Second
	mysqlHeartbeatTimeout         = 5 * time.Second
	kafkaHeartbeatDefaultInterval = 30 * time.Second
	kafkaBrokerDialTimeout        = 3 * time.Second
	kafkaMetadataTimeout          = 5 * time.Second
	fastDebugHeartbeatInterval    = 5 * time.Second
)

type workerCountSetter interface {
	SetWorkerCount(int)
}

type GenericAPIServer struct {
	insecureServer    *http.Server          // 非TLS HTTP服务实例，默认 nil
	*gin.Engine                             // Gin 引擎，默认通过 gin.New() 初始化
	options           *options.Options      // 启动配置，默认由构造函数注入
	redis             *storage.RedisCluster // Redis 集群客户端，默认 nil
	redisCancel       context.CancelFunc    // Redis 连接取消函数，默认 nil
	mysqlDB           *gorm.DB              // MySQL 主库连接，默认 nil
	mysqlCancel       context.CancelFunc    // MySQL 心跳取消函数，默认 nil
	initOnce          sync.Once             // 初始化幂等控制器，默认零值
	producers         *producer.Registry    // Kafka 生产者注册表，默认 nil
	consumers         *consumer.Registry    // Kafka 消费者注册表，默认 nil
	consumerCtx       context.Context       // 消费者运行上下文，默认 nil
	consumerCancel    context.CancelFunc    // 消费者取消函数，默认 nil
	kafkaCancel       context.CancelFunc    // Kafka 心跳取消函数，默认 nil
	audit             *audit.Manager        // 审计管理器，默认 nil
	shutdownOnce      sync.Once             // 关闭流程幂等控制器，默认零值
	loginLimit        atomic.Int64          // 登录限流计数器，默认 0
	userService       *user.UserService     // 用户服务实例，默认 nil
	loginUpdates      chan *v1.User         // 登录状态更新通道，默认 nil
	loginUpdateCtx    context.Context       // 登录更新上下文，默认 nil
	loginUpdateCancel context.CancelFunc    // 登录更新取消函数，默认 nil
	loginUpdateWG     sync.WaitGroup        // 登录更新工作队列，默认零值
	credentialCache   *credentialCache      // 登录凭证缓存，默认 nil
	loginInFlight     atomic.Int64          // 并发登录计数器，默认 0
	datastore         *mysql.Datastore      // MySQL 数据源封装，默认 nil
}

func (g *GenericAPIServer) registerUserProducer(p producer.MessageProducer[*v1.User, string]) {
	if g == nil || p == nil {
		return
	}
	if g.producers == nil {
		g.producers = producer.NewRegistry()
	}
	if err := producer.RegisterProducer(g.producers, userProducerKey, p); err != nil {
		log.Warnf("failed to register user producer: %v", err)
	}
}

func (g *GenericAPIServer) userProducer() producer.MessageProducer[*v1.User, string] {
	if g == nil || g.producers == nil {
		return nil
	}
	prod, ok := producer.GetProducer[*v1.User, string](g.producers, userProducerKey)
	if !ok {
		return nil
	}
	return prod
}

func (g *GenericAPIServer) ensureUserProducer() producer.MessageProducer[*v1.User, string] {
	if prod := g.userProducer(); prod != nil {
		return prod
	}
	noop := &noopProducer{}
	g.registerUserProducer(noop)
	return noop
}

func (g *GenericAPIServer) isDebugMode() bool {
	return strings.EqualFold(g.options.ServerRunOptions.Mode, gin.DebugMode)
}

func (g *GenericAPIServer) fastDebugStartupEnabled() bool {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return false
	}
	return g.isDebugMode() && g.options.ServerRunOptions.FastDebugStartup
}

func (g *GenericAPIServer) consumerGroupBase() string {
	return ConsumerGroupPrefix
}

func (g *GenericAPIServer) consumerGroupID(suffix string) string {
	base := g.consumerGroupBase()
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return base
	}
	if strings.HasSuffix(base, "-") {
		return base + suffix
	}
	return base + "-" + suffix
}

func (g *GenericAPIServer) shutdownAudit() {
	if g.audit == nil {
		return
	}
	timeout := g.options.AuditOptions.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := g.audit.Shutdown(ctx); err != nil {
		log.Warnf("审计管理器关闭超时: %v", err)
	}
}

func (g *GenericAPIServer) submitAuditEvent(ctx context.Context, event audit.Event) {
	if g.audit == nil {
		return
	}
	g.audit.Submit(ctx, event)
}

func (g *GenericAPIServer) auditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		audit.InjectToGinContext(c, g.audit)
		c.Next()
	}
}

func (g *GenericAPIServer) auditServiceEvent(service, stage, outcome string, err error) {
	if g == nil || g.audit == nil {
		return
	}
	event := audit.Event{
		Action:       fmt.Sprintf("%s.%s", service, stage),
		ResourceType: "service",
		ResourceID:   service,
		Target:       service,
		Outcome:      outcome,
		Actor:        "system",
		OccurredAt:   time.Now(),
		Metadata: map[string]any{
			"stage": stage,
		},
	}
	if err != nil {
		event.ErrorMessage = err.Error()
	}
	g.audit.Submit(context.Background(), event)
}

func (g *GenericAPIServer) closeWithAudit(ctx context.Context, service string, fn func(context.Context) error) {
	g.auditServiceEvent(service, "shutdown", "start", nil)
	if fn == nil {
		g.auditServiceEvent(service, "shutdown", "success", nil)
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := fn(ctx); err != nil {
		g.auditServiceEvent(service, "shutdown", "fail", err)
	} else {
		g.auditServiceEvent(service, "shutdown", "success", nil)
	}
}

func (g *GenericAPIServer) performShutdown(ctx context.Context) {
	g.shutdownOnce.Do(func() {
		shutdownCtx := ctx
		if shutdownCtx == nil {
			shutdownCtx = context.Background()
		}
		g.stopLoginUpdater()
		g.closeWithAudit(shutdownCtx, "kafka", g.shutdownKafka)
		g.closeWithAudit(shutdownCtx, "redis", g.shutdownRedis)
		g.closeWithAudit(shutdownCtx, "mysql", g.shutdownMySQL)
		// 审计管理器最后关闭，避免丢失前面的关闭事件
		g.shutdownAudit()
	})
}

func (g *GenericAPIServer) shutdownKafka(ctx context.Context) error {
	var combined error
	if g.kafkaCancel != nil {
		g.kafkaCancel()
		g.kafkaCancel = nil
	}
	if g.consumerCancel != nil {
		g.consumerCancel()
	}
	if g.consumers != nil {
		if err := g.consumers.CloseAll(); err != nil {
			combined = stdErrors.Join(combined, err)
		}
	}
	if g.producers != nil {
		if err := g.producers.CloseAll(); err != nil {
			combined = stdErrors.Join(combined, err)
		}
	}
	return combined
}

func (g *GenericAPIServer) shutdownRedis(ctx context.Context) error {
	if g.redisCancel != nil {
		g.redisCancel()
	}
	err := storage.CloseRedisClients()
	g.redis = nil
	return err
}

func (g *GenericAPIServer) shutdownMySQL(ctx context.Context) error {
	if g.mysqlCancel != nil {
		g.mysqlCancel()
		g.mysqlCancel = nil
	}
	factory := interfaces.Client()
	if factory == nil {
		return nil
	}
	return factory.Close()
}

func NewGenericAPIServer(opts *options.Options) (*GenericAPIServer, error) {
	// 初始化日志
	log.Infof("正在初始化GenericAPIServer服务器，环境: %s", opts.ServerRunOptions.Mode)

	//创建服务器实例
	g := &GenericAPIServer{
		Engine:    gin.New(),
		options:   opts,
		initOnce:  sync.Once{},
		producers: producer.NewRegistry(),
		consumers: consumer.NewRegistry(),
	}
	g.loginLimit.Store(int64(opts.ServerRunOptions.LoginRateLimit))

	auditMgr, err := audit.NewManager(audit.Config{
		Enabled:         opts.AuditOptions.Enabled,
		BufferSize:      opts.AuditOptions.BufferSize,
		ShutdownTimeout: opts.AuditOptions.ShutdownTimeout,
		LogFile:         opts.AuditOptions.LogFile,
		EnableMetrics:   opts.AuditOptions.EnableMetrics,
		RecentBuffer:    opts.AuditOptions.RecentBuffer,
	})
	if err != nil {
		log.Errorf("初始化审计管理器失败: %v", err)
	} else {
		g.audit = auditMgr
		// 记录审计服务自身启动事件
		g.auditServiceEvent("audit", "startup", "success", nil)
	}

	g.Use(g.auditMiddleware())

	//设置gin运行模式
	if err := g.configureGin(); err != nil {
		return nil, err
	}
	// 初始化mysql
	g.auditServiceEvent("mysql", "startup", "start", nil)
	storeIns, dbIns, err := mysql.GetMySQLFactoryOr(opts.MysqlOptions)
	if err != nil {
		log.Error("mysql服务器启动失败")
		g.auditServiceEvent("mysql", "startup", "fail", err)
		return nil, err
	}
	interfaces.SetClient(storeIns)
	g.mysqlDB = dbIns
	log.Infof("mysql服务器初始化成功")
	g.auditServiceEvent("mysql", "startup", "success", nil)

	// ========== 新增：增强版集群状态检查和初始化 ==========
	if datastore, ok := storeIns.(*mysql.Datastore); ok {
		g.datastore = datastore
		if datastore.IsClusterMode() {
			log.Infof("🚀 检测到Galera集群模式，正在初始化集群连接...")

			// 执行集群健康检查
			if err := initializeGaleraCluster(datastore); err != nil {
				log.Errorf("Galera集群初始化警告: %v", err)
				// 不阻止启动，但记录警告
			}

			// 定期监控集群状态（可选）
			go monitorClusterHealth(datastore, opts.MysqlOptions.HealthCheckInterval)
		} else {
			log.Info("✅ 使用单节点MySQL模式")
		}
	}

	mysqlWait := 30 * time.Second
	if g.fastDebugStartupEnabled() {
		mysqlWait = 5 * time.Second
	}
	if err := waitForMySQLReady(dbIns, mysqlWait); err != nil {
		if !g.fastDebugStartupEnabled() {
			log.Error("mysql服务器未就绪")
			g.auditServiceEvent("mysql", "startup", "fail", err)
			return nil, err
		}
		log.Warnf("调试快速启动: MySQL 未在 %v 内就绪，将降级继续启动（err=%v）", mysqlWait, err)
		g.auditServiceEvent("mysql", "startup", "degraded", err)
		go func() {
			if followErr := waitForMySQLReady(dbIns, 30*time.Second); followErr != nil {
				log.Warnf("调试快速启动: 后台等待 MySQL 仍失败: %v", followErr)
			} else {
				log.Infof("调试快速启动: MySQL 已在后台就绪")
			}
		}()
	}
	g.startMySQLMonitor(dbIns)

	//初始化redis
	g.auditServiceEvent("redis", "startup", "start", nil)
	if err := g.initRedisStore(); err != nil {
		log.Error("redis服务器启动失败")
		g.auditServiceEvent("redis", "startup", "fail", err)
		return nil, err
	}
	log.Info("redis服务器启动成功")
	g.auditServiceEvent("redis", "startup", "success", nil)
	// 生成唯一的 KAFKA_INSTANCE_ID
	instanceID := os.Getenv("KAFKA_INSTANCE_ID")
	if instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknownhost"
		}
		timestamp := time.Now().UnixNano()
		instanceID = fmt.Sprintf("%s-%d", host, timestamp)
	}
	if opts.KafkaOptions != nil {
		opts.KafkaOptions.InstanceID = instanceID
		log.Infof("[Kafka] 自动生成唯一 InstanceID = %s", instanceID)
	}
	g.auditServiceEvent("kafka", "startup", "start", nil)
	if err := g.initKafkaComponents(dbIns); err != nil {
		log.Error("kafka服务启动失败")
		if !g.fastDebugStartupEnabled() {
			g.auditServiceEvent("kafka", "startup", "fail", err)
			return nil, err
		}
		log.Warnf("调试快速启动: Kafka 初始化失败，将使用空生产者继续运行（err=%v）", err)
		g.auditServiceEvent("kafka", "startup", "degraded", err)
		g.registerUserProducer(newNoopProducer())
		g.consumers = consumer.NewRegistry()
	} else {
		log.Info("kafka服务器启动成功")
		g.auditServiceEvent("kafka", "startup", "success", nil)
	}
	g.startKafkaMonitor()

	g.initUserService(storeIns)
	g.initCredentialCache()
	g.initLoginUpdater()

	// 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	g.consumerCtx = ctx
	g.consumerCancel = cancel

	var consumerReady sync.WaitGroup

	operationConsumers := g.consumers.List(userOperationConsumerKey)
	if len(operationConsumers) > 0 {
		workerCount := g.options.KafkaOptions.WorkerCount
		if workerCount < 1 {
			workerCount = 1
		}
		for _, mc := range operationConsumers {
			if setter, ok := mc.(workerCountSetter); ok {
				setter.SetWorkerCount(workerCount)
			}
			consumerReady.Add(1)
			go mc.Start(ctx, &consumerReady)
		}
		log.Infof("已启动 %d 个操作通道消费者实例", len(operationConsumers))
	}

	retryConsumers := g.consumers.List(userRetryConsumerKey)
	if len(retryConsumers) > 0 {
		partitionCount := 0
		brokers := g.options.KafkaOptions.Brokers
		if len(brokers) > 0 {
			retryCtx, retryCancel := context.WithTimeout(ctx, 5*time.Second)
			p, err := getTopicPartitionCount(retryCtx, brokers, UserOperationRetryTopic)
			retryCancel()
			if err == nil {
				partitionCount = p
			} else {
				if stdErrors.Is(err, context.DeadlineExceeded) {
					log.Warnf("获取 topic %s 分区信息超时，将稍后重试: %v", UserOperationRetryTopic, err)
				} else {
					log.Warnf("获取 topic %s 分区信息失败: %v", UserOperationRetryTopic, err)
				}
			}
		}

		retryGroupId := g.consumerGroupID("retry")
		metrics.ConsumerTopicPartitions.WithLabelValues(UserOperationRetryTopic).Set(float64(partitionCount))
		metrics.ConsumerGroupInstances.WithLabelValues(retryGroupId).Set(float64(len(retryConsumers)))
		if len(retryConsumers) == 0 {
			metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserOperationRetryTopic, retryGroupId).Set(float64(partitionCount))
		} else {
			metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserOperationRetryTopic, retryGroupId).Set(0)
		}

		workersPerInstance := 1
		if partitionCount > 0 {
			workersPerInstance = (partitionCount + len(retryConsumers) - 1) / len(retryConsumers)
			if workersPerInstance > RetryConsumerWorkers {
				workersPerInstance = RetryConsumerWorkers
			}
			if workersPerInstance < 1 {
				workersPerInstance = 1
			}
		}

		for _, mc := range retryConsumers {
			if setter, ok := mc.(workerCountSetter); ok {
				setter.SetWorkerCount(workersPerInstance)
			}
			consumerReady.Add(1)
			go mc.Start(ctx, &consumerReady)
		}

		if g.options.KafkaOptions.EnableMetricsRefresh {
			go func() {
				ticker := time.NewTicker(g.options.KafkaOptions.MetricsRefreshInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if len(brokers) == 0 {
							continue
						}

						isDebug := g.options.ServerRunOptions.Mode == "debug"

						if p, err := getTopicPartitionCount(ctx, brokers, UserOperationRetryTopic); err == nil {
							metrics.ConsumerTopicPartitions.WithLabelValues(UserOperationRetryTopic).Set(float64(p))
							retryCount := len(g.consumers.List(userRetryConsumerKey))
							metrics.ConsumerGroupInstances.WithLabelValues(retryGroupId).Set(float64(retryCount))
							if retryCount == 0 {
								metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserOperationRetryTopic, retryGroupId).Set(float64(p))
								if isDebug {
								}
							} else {
								if noOwner, err := getPartitionsWithoutOwner(ctx, brokers, retryGroupId, UserOperationRetryTopic); err == nil {
									metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserOperationRetryTopic, retryGroupId).Set(float64(noOwner))
									if isDebug {

									}
								} else {
									metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserOperationRetryTopic, retryGroupId).Set(0)
									log.Warnf("周期更新: 无法计算无主分区，使用回退值 0: %v", err)
								}
							}
						} else {
							if g.options.ServerRunOptions.Mode == "debug" {

							}
						}
					}
				}
			}()
		}
	}

	consumerReady.Wait()
	// 如果我们未创建按实例存储（回退模式），启动单个全局重试消费者

	log.Infof("所有Kafka消费者已启动")
	g.printKafkaConfigInfo()

	//安装中间件
	if err := middleware.InstallMiddlewares(g.Engine, opts); err != nil {
		log.Error("中间件安装失败")
		return nil, err
	}
	log.Info("中间件安装成功")

	//. 安装路由
	g.installRoutes()

	return g, nil
}

// ========== 新增：集群健康监控 ==========
func monitorClusterHealth(datastore *mysql.Datastore, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second // 默认30秒
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastStatus db.ClusterStatus
	unhealthyCount := 0

	for range ticker.C {
		currentStatus := datastore.ClusterStatus()

		// 只在状态变化时记录
		if currentStatus.PrimaryHealthy != lastStatus.PrimaryHealthy ||
			currentStatus.HealthyReplicas != lastStatus.HealthyReplicas {

			if currentStatus.PrimaryHealthy && currentStatus.HealthyReplicas > 0 {
				log.Warnf("📊 集群状态: 主节点健康，%d/%d 副本可用",
					currentStatus.HealthyReplicas, currentStatus.ReplicaCount)
				unhealthyCount = 0
			} else if !currentStatus.PrimaryHealthy {
				unhealthyCount++
				log.Errorf("🚨 集群告警: 主节点不可用 (连续%d次)", unhealthyCount)
			} else if currentStatus.HealthyReplicas == 0 {
				log.Warn("⚠️  集群警告: 无可用副本节点")
			}
		}

		lastStatus = currentStatus

		// 如果连续多次检测到主节点不可用，可能需要告警
		if unhealthyCount >= 3 {
			log.Error("🚨 严重: 集群主节点持续不可用，请立即检查!")
		}
	}
}

func (g *GenericAPIServer) configureGin() error {
	// 设置运行模式
	gin.SetMode(g.options.ServerRunOptions.Mode)

	// 开发环境配置
	if g.options.ServerRunOptions.Mode == gin.DebugMode {
		gin.DebugPrintRouteFunc = func(httpMethod, absolutePath, handlerName string, nuHandlers int) {
			log.Infof("📍 %-6s %-50s → %s (%d middleware)",
				httpMethod, absolutePath, filepath.Base(handlerName), nuHandlers)
		}
	} else {
		// 生产环境禁用调试输出
		gin.DebugPrintRouteFunc = func(httpMethod, absolutePath, handlerName string, nuHandlers int) {}
	}

	return nil
}

func (g *GenericAPIServer) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	g.auditServiceEvent("api-server", "startup", "start", nil)

	address := net.JoinHostPort(g.options.InsecureServingOptions.BindAddress, strconv.Itoa(g.options.InsecureServingOptions.BindPort))

	g.insecureServer = &http.Server{
		Addr:              address,
		Handler:           g,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 20,
		ConnState: func(conn net.Conn, state http.ConnState) {
			// 预留连接状态监控
		},
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		wrapped := fmt.Errorf("创建监听器失败: %w", err)
		g.auditServiceEvent("api-server", "startup", "fail", wrapped)
		g.performShutdown(context.Background())
		return wrapped
	}

	serverErr := make(chan error, 1)
	serverStarted := make(chan struct{})

	go func() {
		close(serverStarted)

		if serveErr := g.insecureServer.Serve(listener); serveErr != nil {
			serverErr <- serveErr
			return
		}
		serverErr <- nil
	}()

	select {
	case <-serverStarted:
		g.auditServiceEvent("api-server", "startup", "success", nil)
		log.Infof("GenericAPIServer服务器已开始监听，准备进行健康检查...")
	case <-ctx.Done():
		listener.Close()
		reason := fmt.Errorf("启动被取消: %w", ctx.Err())
		g.auditServiceEvent("api-server", "startup", "fail", reason)
		g.performShutdown(context.Background())
		return ctx.Err()
	case <-time.After(10 * time.Second):
		err := fmt.Errorf("GenericAPIServer服务器启动超时，无法在10秒内开始监听")
		g.auditServiceEvent("api-server", "startup", "fail", err)
		_ = g.insecureServer.Close()
		g.performShutdown(context.Background())
		return err
	}

	if g.options.ServerRunOptions.Healthz {
		healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		if err := g.waitForPortReady(healthCtx, address, 10*time.Second); err != nil {
			err := fmt.Errorf("端口就绪检测失败: %w", err)
			g.auditServiceEvent("api-server", "startup", "fail", err)
			_ = g.insecureServer.Close()
			g.performShutdown(context.Background())
			return err
		}
		if err := g.ping(healthCtx, address); err != nil {
			err := fmt.Errorf("健康检查失败: %w", err)
			g.auditServiceEvent("api-server", "startup", "fail", err)
			_ = g.insecureServer.Close()
			g.performShutdown(context.Background())
			return err
		}
	}

	for {
		select {
		case err := <-serverErr:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				wrapped := fmt.Errorf("GenericAPIServer服务器运行失败: %w", err)
				g.auditServiceEvent("api-server", "runtime", "fail", wrapped)
				g.performShutdown(ctx)
				return wrapped
			}
			g.auditServiceEvent("api-server", "shutdown", "success", nil)
			g.performShutdown(ctx)
			if err == nil || errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			g.auditServiceEvent("api-server", "shutdown", "start", ctx.Err())
			shutdownTimeout := g.options.AuditOptions.ShutdownTimeout
			if shutdownTimeout <= 0 {
				shutdownTimeout = 10 * time.Second
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			errShutdown := g.insecureServer.Shutdown(shutdownCtx)
			cancel()
			if errShutdown != nil {
				g.auditServiceEvent("api-server", "shutdown", "fail", errShutdown)
			} else {
				g.auditServiceEvent("api-server", "shutdown", "success", nil)
			}
			g.performShutdown(ctx)
			if serveErr := <-serverErr; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				return serveErr
			}
			if errShutdown != nil {
				return errShutdown
			}
			return nil
		}
	}
}

// waitForPortReady 等待端口就绪
func (g *GenericAPIServer) waitForPortReady(ctx context.Context, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	log.Warnf("等待端口 %s 就绪，超时时间: %v", address, timeout)

	for attempt := 1; ; attempt++ {
		// 检查是否超时
		if time.Now().After(deadline) {
			return fmt.Errorf("端口就绪检测超时")
		}

		// 尝试连接端口
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			log.Infof("端口 %s 就绪检测成功，尝试次数: %d", address, attempt)
			return nil
		}

		// 记录重试信息（每5次尝试记录一次）
		if attempt%5 == 0 {
			log.Infof("端口就绪检测尝试 %d: %v", attempt, err)
		}

		// 等待重试或上下文取消
		select {
		case <-ctx.Done():
			return fmt.Errorf("端口就绪检测被取消: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
			// 继续重试
		}
	}
}

// 初始化Kafka组件
// internal/apiserver/server/server.go

// 初始化Kafka组件 - 使用options中的完整配置
func (g *GenericAPIServer) initKafkaComponents(db *gorm.DB) error {
	kafkaOpts := g.options.KafkaOptions

	// 1. 初始化生产端动态限速器
	// 统计函数：返回总请求数和失败数
	collectCounterTotal := func(collector prometheus.Collector) float64 {
		ch := make(chan prometheus.Metric)
		go func() {
			collector.Collect(ch)
			close(ch)
		}()
		total := 0.0
		for m := range ch {
			var pb dto.Metric
			if err := m.Write(&pb); err != nil {
				continue
			}
			if pb.Counter != nil {
				total += pb.Counter.GetValue()
			}
		}
		return total
	}

	getProducerStats := func() (int, int) {
		success := collectCounterTotal(metrics.ProducerSuccess)
		fail := collectCounterTotal(metrics.ProducerFailures)
		return int(success + fail), int(fail)
	}

	var rateLimiter *ratelimiter.RateLimiterController
	if g.options.ServerRunOptions.EnableRateLimiter {
		log.Info("初始化生产端动态限速器...")
		rateLimiter = ratelimiter.NewRateLimiterController(
			float64(kafkaOpts.StartingRate), // 初始速率
			float64(kafkaOpts.MinRate),      // 最小速率
			float64(kafkaOpts.MaxRate),      // 最大速率
			kafkaOpts.AdjustPeriod,          // 调整周期
			getProducerStats,
		)
	} else {
		log.Infof("[Producer] 未启用限速器（EnableRateLimiter=false）")
	}

	log.Info("初始化Kafka生产者...")
	userProducer, err := NewUserProducer(kafkaOpts, rateLimiter, g.options.ServerRunOptions.ProducerFallbackDir)
	if err != nil {
		return fmt.Errorf("failed to create user producer: %w", err)
	}

	// 为用户操作主通道与重试通道创建消费者实例
	consumerCount := kafkaOpts.WorkerCount
	retryconsumerCount := kafkaOpts.RetryWorkerCount

	log.Infof("为用户操作通道创建 %d 个消费者实例，消费组前缀: %s", consumerCount, g.consumerGroupBase())

	operationConsumers := make([]*UserConsumer, consumerCount)
	retryConsumers := make([]*RetryConsumer, retryconsumerCount)

	operationsGroupID := g.consumerGroupID("operations")

	for i := 0; i < consumerCount; i++ {
		operationConsumers[i] = NewUserConsumer(kafkaOpts, UserOperationTopic,
			operationsGroupID, i, db, g.redis)
		operationConsumers[i].SetProducer(userProducer)
		operationConsumers[i].SetInstanceID(i)
		if g.datastore != nil {
			operationConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
		if g.options.ServerRunOptions.EnableRateLimiter {
			//	go operationConsumers[i].startLagMonitor(context.Background())
		}
	}

	log.Info("初始化重试消费者...")
	retryGroupId := g.consumerGroupID("retry")
	for i := 0; i < kafkaOpts.RetryWorkerCount; i++ {
		retryConsumers[i] = NewRetryConsumer(db, g.redis, userProducer, kafkaOpts, UserOperationRetryTopic, retryGroupId, i)
		if g.datastore != nil {
			retryConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
	}
	// 3. 注册用户消息生产者，便于后续扩展其他业务对象
	g.registerUserProducer(userProducer)

	// 注册消费者实例，供统一生命周期管理
	for _, c := range operationConsumers {
		if c == nil {
			continue
		}
		if err := g.consumers.Register(userOperationConsumerKey, c); err != nil {
			return fmt.Errorf("register operation consumer: %w", err)
		}
	}
	for _, c := range retryConsumers {
		if c == nil {
			continue
		}
		if err := g.consumers.Register(userRetryConsumerKey, c); err != nil {
			return fmt.Errorf("register retry consumer: %w", err)
		}
	}

	return nil
}

func (g *GenericAPIServer) initUserService(factory interfaces.Factory) {
	if g == nil || factory == nil {
		return
	}
	userProducer := g.ensureUserProducer()
	g.userService = user.NewUserService(factory, g.redis, g.options, userProducer, g.audit)
}

func (g *GenericAPIServer) initCredentialCache() {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return
	}
	cache := newCredentialCache(
		g.options.ServerRunOptions.LoginCredentialCacheTTL,
		g.options.ServerRunOptions.LoginCredentialCacheSize,
	)
	g.credentialCache = cache
}

func (g *GenericAPIServer) initLoginUpdater() {
	if g == nil || g.options == nil || g.options.ServerRunOptions == nil {
		return
	}
	if g.loginUpdates != nil {
		return
	}
	buffer := g.options.ServerRunOptions.LoginUpdateBuffer
	if buffer <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.loginUpdateCtx = ctx
	g.loginUpdateCancel = cancel
	g.loginUpdates = make(chan *v1.User, buffer)
	batchSize := g.options.ServerRunOptions.LoginUpdateBatchSize
	if batchSize <= 0 {
		batchSize = 64
	}
	flushInterval := g.options.ServerRunOptions.LoginUpdateFlushInterval
	if flushInterval <= 0 {
		flushInterval = 200 * time.Millisecond
	}
	g.loginUpdateWG.Add(1)
	go g.loginUpdateWorker(ctx, batchSize, flushInterval)
}

func (g *GenericAPIServer) stopLoginUpdater() {
	if g == nil || g.loginUpdateCancel == nil {
		return
	}
	g.loginUpdateCancel()
	g.loginUpdateWG.Wait()
}

func (g *GenericAPIServer) loginUpdateWorker(ctx context.Context, batchSize int, flushInterval time.Duration) {
	defer g.loginUpdateWG.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	batch := make([]*v1.User, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		g.flushLoginUpdates(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case user := <-g.loginUpdates:
			if user == nil {
				continue
			}
			batch = append(batch, user)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (g *GenericAPIServer) flushLoginUpdates(batch []*v1.User) {
	if g == nil || len(batch) == 0 {
		return
	}
	updates := make(map[string]*v1.User, len(batch))
	for _, u := range batch {
		if u == nil || u.Name == "" {
			continue
		}
		if existing, ok := updates[u.Name]; ok {
			if u.LoginedAt.After(existing.LoginedAt) {
				copy := *u
				updates[u.Name] = &copy
			}
			continue
		}
		copy := *u
		updates[u.Name] = &copy
	}
	if len(updates) == 0 {
		return
	}
	timeout := g.options.ServerRunOptions.LoginUpdateTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, u := range updates {
		if err := interfaces.Client().Users().Update(ctx, u, metav1.UpdateOptions{}, g.options); err != nil {
			log.Warnf("batch update loginedAt failed: username=%s, err=%v", u.Name, err)
		}
	}
}

func (g *GenericAPIServer) monitorRedisConnection(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Warnf("Redis集群监控退出")
			return
		case <-ticker.C:
			client := g.redis.GetClient()
			if client == nil {
				log.Error("Redis集群客户端丢失")
				continue
			}

			// 减少日志输出，只在出错时记录
			if err := g.pingRedis(ctx, client); err != nil {
				log.Errorf("Redis集群健康检查失败: %v", err)
			}
			// 成功时不输出日志，或者改为Debug级别

		}
	}
}

func (g *GenericAPIServer) startMySQLMonitor(db *gorm.DB) {
	if g == nil || db == nil {
		return
	}
	if g.mysqlCancel != nil {
		g.mysqlCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.mysqlCancel = cancel
	go g.monitorMySQLConnection(ctx, db)
}

func (g *GenericAPIServer) mysqlHeartbeatInterval() time.Duration {
	if g == nil || g.options == nil || g.options.MysqlOptions == nil {
		return mysqlHeartbeatDefaultInterval
	}
	interval := g.options.MysqlOptions.MonitorInterval
	if interval <= 0 {
		interval = g.options.MysqlOptions.HealthCheckInterval
	}
	if interval <= 0 {
		interval = mysqlHeartbeatDefaultInterval
	}
	if g.fastDebugStartupEnabled() && interval > fastDebugHeartbeatInterval {
		return fastDebugHeartbeatInterval
	}
	return interval
}

func (g *GenericAPIServer) mysqlReadDB() *gorm.DB {
	if g == nil {
		return nil
	}
	if g.datastore != nil {
		if read := g.datastore.ReadDB(); read != nil {
			return read
		}
	}
	return g.mysqlDB
}

func (g *GenericAPIServer) monitorMySQLConnection(ctx context.Context, primary *gorm.DB) {
	interval := g.mysqlHeartbeatInterval()
	if interval <= 0 {
		interval = mysqlHeartbeatDefaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastPrimaryHealthy := false
	lastReadHealthy := false
	firstRun := true

	for {
		select {
		case <-ctx.Done():
			log.Warn("MySQL心跳监控退出")
			return
		case <-ticker.C:
			primaryLatency, primaryErr := g.mysqlHeartbeatProbe(ctx, primary)
			primaryHealthy := primaryErr == nil
			metrics.DatabaseHeartbeatLatency.WithLabelValues("mysql", "primary").Observe(primaryLatency.Seconds())
			metrics.DatabaseHeartbeatStatus.WithLabelValues("mysql", "primary").Set(boolToFloat(primaryHealthy))
			if firstRun || primaryHealthy != lastPrimaryHealthy {
				if primaryHealthy {
					log.Infof("✅ MySQL主库心跳正常，耗时 %.3f 秒", primaryLatency.Seconds())
				} else {
					log.Errorf("🚨 MySQL主库心跳失败: %v", primaryErr)
				}
			}
			lastPrimaryHealthy = primaryHealthy

			readDB := g.mysqlReadDB()
			var readLatency time.Duration
			var readErr error
			readHealthy := false
			if readDB != nil {
				readLatency, readErr = g.mysqlHeartbeatProbe(ctx, readDB)
				readHealthy = readErr == nil
				metrics.DatabaseHeartbeatLatency.WithLabelValues("mysql", "read").Observe(readLatency.Seconds())
				metrics.DatabaseHeartbeatStatus.WithLabelValues("mysql", "read").Set(boolToFloat(readHealthy))
				if firstRun || readHealthy != lastReadHealthy {
					if readHealthy {
						log.Infof("✅ MySQL读库心跳正常，耗时 %.3f 秒", readLatency.Seconds())
					} else {
						log.Warnf("⚠️ MySQL读库心跳失败: %v", readErr)
					}
				}
			} else {
				metrics.DatabaseHeartbeatStatus.WithLabelValues("mysql", "read").Set(0)
				if firstRun || lastReadHealthy {
					log.Warn("⚠️ MySQL读库心跳跳过：未找到可用的读连接")
				}
			}
			lastReadHealthy = readHealthy

			if g.datastore != nil {
				status := g.datastore.ClusterStatus()
				metrics.DatabaseReplicaStatus.WithLabelValues("mysql", "replica_total").Set(float64(status.ReplicaCount))
				metrics.DatabaseReplicaStatus.WithLabelValues("mysql", "replica_healthy").Set(float64(status.HealthyReplicas))
				if firstRun {
					log.Infof("MySQL集群状态：主库健康=%v，副本总数=%d，健康副本=%d", status.PrimaryHealthy, status.ReplicaCount, status.HealthyReplicas)
				}
			}

			firstRun = false
		}
	}
}

func (g *GenericAPIServer) mysqlHeartbeatProbe(ctx context.Context, db *gorm.DB) (time.Duration, error) {
	if db == nil {
		return 0, fmt.Errorf("nil database handle")
	}
	pingCtx, cancel := context.WithTimeout(ctx, mysqlHeartbeatTimeout)
	defer cancel()
	start := time.Now()
	err := db.WithContext(pingCtx).Exec("SELECT 1").Error
	return time.Since(start), err
}

func (g *GenericAPIServer) startKafkaMonitor() {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return
	}
	if len(g.options.KafkaOptions.Brokers) == 0 {
		return
	}
	if g.kafkaCancel != nil {
		g.kafkaCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.kafkaCancel = cancel
	go g.monitorKafkaCluster(ctx)
}

func (g *GenericAPIServer) kafkaHeartbeatInterval() time.Duration {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return kafkaHeartbeatDefaultInterval
	}
	interval := g.options.KafkaOptions.MetricsRefreshInterval
	if interval <= 0 {
		interval = g.options.KafkaOptions.LagCheckInterval
	}
	if interval <= 0 {
		interval = kafkaHeartbeatDefaultInterval
	}
	if g.fastDebugStartupEnabled() && interval > fastDebugHeartbeatInterval {
		return fastDebugHeartbeatInterval
	}
	return interval
}

func (g *GenericAPIServer) kafkaClusterName() string {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return "kafka"
	}
	if group := strings.TrimSpace(g.options.KafkaOptions.ConsumerGroup); group != "" {
		return group
	}
	if len(g.options.KafkaOptions.Brokers) > 0 {
		return g.options.KafkaOptions.Brokers[0]
	}
	return "kafka"
}

func (g *GenericAPIServer) kafkaHeartbeatTopics() []string {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return nil
	}
	topics := make(map[string]struct{})
	if mainTopic := strings.TrimSpace(g.options.KafkaOptions.Topic); mainTopic != "" {
		topics[mainTopic] = struct{}{}
	}
	for _, t := range []string{UserOperationTopic, UserOperationRetryTopic, UserOperationCompTopic, UserDeadLetterTopic} {
		if strings.TrimSpace(t) != "" {
			topics[t] = struct{}{}
		}
	}
	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}
	return result
}

func (g *GenericAPIServer) monitorKafkaCluster(ctx context.Context) {
	interval := g.kafkaHeartbeatInterval()
	if interval <= 0 {
		interval = kafkaHeartbeatDefaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	cluster := g.kafkaClusterName()
	brokerState := make(map[string]bool)
	lastClusterHealthy := false
	firstRun := true

	for {
		select {
		case <-ctx.Done():
			log.Warn("Kafka心跳监控退出")
			return
		case <-ticker.C:
			g.probeKafkaBrokers(ctx, cluster, brokerState, firstRun)
			clusterHealthy := g.probeKafkaMetadata(ctx, cluster)
			if firstRun || clusterHealthy != lastClusterHealthy {
				if clusterHealthy {
					log.Infof("✅ Kafka集群[%s]元数据检查通过", cluster)
				} else {
					log.Errorf("🚨 Kafka集群[%s]元数据检查失败", cluster)
				}
			}
			lastClusterHealthy = clusterHealthy
			firstRun = false
		}
	}
}

func (g *GenericAPIServer) probeKafkaBrokers(ctx context.Context, cluster string, state map[string]bool, firstRun bool) {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return
	}
	for _, broker := range g.options.KafkaOptions.Brokers {
		dialCtx, cancel := context.WithTimeout(ctx, kafkaBrokerDialTimeout)
		start := time.Now()
		conn, err := (&net.Dialer{Timeout: kafkaBrokerDialTimeout}).DialContext(dialCtx, "tcp", broker)
		cancel()
		latency := time.Since(start)
		metrics.KafkaBrokerLatency.WithLabelValues(cluster, broker).Observe(latency.Seconds())
		healthy := err == nil
		metrics.KafkaBrokerHealth.WithLabelValues(cluster, broker).Set(boolToFloat(healthy))
		if !healthy {
			metrics.KafkaHeartbeatFailures.WithLabelValues(cluster, "broker_dial").Inc()
		}
		if prev, ok := state[broker]; !ok || prev != healthy || firstRun {
			if healthy {
				log.Infof("✅ Kafka Broker[%s] 心跳正常，耗时 %.3f 秒", broker, latency.Seconds())
			} else {
				log.Errorf("🚨 Kafka Broker[%s] 心跳失败: %v", broker, err)
			}
		}
		state[broker] = healthy
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func (g *GenericAPIServer) probeKafkaMetadata(ctx context.Context, cluster string) bool {
	if g == nil || g.options == nil || g.options.KafkaOptions == nil {
		return false
	}
	if len(g.options.KafkaOptions.Brokers) == 0 {
		return false
	}
	topicList := g.kafkaHeartbeatTopics()
	topicLookup := make(map[string]struct{}, len(topicList))
	for _, t := range topicList {
		topicLookup[t] = struct{}{}
	}
	metaCtx, cancel := context.WithTimeout(ctx, kafkaMetadataTimeout)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(g.options.KafkaOptions.Brokers...)}
	request := &kafka.MetadataRequest{}
	if len(topicList) > 0 {
		request.Topics = topicList
	}
	metadata, err := client.Metadata(metaCtx, request)
	if err != nil {
		metrics.KafkaClusterHealth.WithLabelValues(cluster).Set(0)
		metrics.KafkaClusterBrokers.WithLabelValues(cluster).Set(0)
		metrics.KafkaHeartbeatFailures.WithLabelValues(cluster, "metadata").Inc()
		for _, topic := range topicList {
			metrics.KafkaTopicStatus.WithLabelValues(cluster, topic).Set(0)
			metrics.ConsumerTopicPartitions.WithLabelValues(topic).Set(0)
		}
		return false
	}

	metrics.KafkaClusterHealth.WithLabelValues(cluster).Set(1)
	metrics.KafkaClusterBrokers.WithLabelValues(cluster).Set(float64(len(metadata.Brokers)))

	topicFound := make(map[string]bool, len(topicLookup))
	for _, topic := range metadata.Topics {
		if len(topicLookup) > 0 {
			if _, ok := topicLookup[topic.Name]; !ok {
				continue
			}
		}
		topicFound[topic.Name] = topic.Error == nil
		if topic.Error != nil {
			metrics.KafkaTopicStatus.WithLabelValues(cluster, topic.Name).Set(0)
			metrics.ConsumerTopicPartitions.WithLabelValues(topic.Name).Set(0)
			metrics.KafkaHeartbeatFailures.WithLabelValues(cluster, "topic_error").Inc()
			log.Warnf("⚠️ Kafka主题 %s 元数据错误: %v", topic.Name, topic.Error)
			continue
		}
		metrics.KafkaTopicStatus.WithLabelValues(cluster, topic.Name).Set(1)
		metrics.ConsumerTopicPartitions.WithLabelValues(topic.Name).Set(float64(len(topic.Partitions)))
	}

	for topic := range topicLookup {
		if ok := topicFound[topic]; !ok {
			metrics.KafkaTopicStatus.WithLabelValues(cluster, topic).Set(0)
			metrics.ConsumerTopicPartitions.WithLabelValues(topic).Set(0)
			metrics.KafkaHeartbeatFailures.WithLabelValues(cluster, "topic_missing").Inc()
			log.Warnf("⚠️ Kafka主题 %s 未在元数据中找到", topic)
		}
	}

	return true
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (g *GenericAPIServer) ping(ctx context.Context, address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("无效的地址格式: %w", err)
	}

	if host == "0.0.0.0" {
		host = "127.0.0.1"
	}

	url := fmt.Sprintf("http://%s/healthz", net.JoinHostPort(host, port))
	log.Infof("开始健康检查，目标URL: %s", url)

	attempt := 0

	for {
		attempt++
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("健康检查超时: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("创建请求失败: %w", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if attempt%3 == 0 { // 每3次失败记录一次日志，避免日志过多
				log.Infof("健康检查尝试 %d 失败: %v", attempt, err)
			}
		} else {
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				log.Info("健康检查成功")
				return nil
			}

			log.Infof("健康检查尝试 %d: 状态码 %d", attempt, resp.StatusCode)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("健康检查超时: %w", ctx.Err())
		case <-time.After(1 * time.Second):
			// 继续重试
		}
	}
}

func (g *GenericAPIServer) initRedisStore() error {
	ctx, cancel := context.WithCancel(context.Background())
	g.redisCancel = cancel

	// 🔥 必须先初始化 g.redis！
	g.redis = &storage.RedisCluster{
		KeyPrefix: "genericapiserver:",
		HashKeys:  false,
		IsCache:   false,
	}

	// 启动异步连接任务
	go func() {
		log.Info("启动Redis集群异步连接任务")
		storage.ConnectToRedis(ctx, g.options.RedisOptions)
		log.Warn("Redis集群异步连接任务退出（可能上下文已取消）")
	}()

	// 同步等待Redis完全启动
	log.Info("等待Redis集群完全启动...")

	debugMode := g.fastDebugStartupEnabled()
	basicTimeout := 60 * time.Second
	healthyTimeout := 90 * time.Second
	if debugMode {
		basicTimeout = 5 * time.Second
		healthyTimeout = 10 * time.Second
		log.Infof("调试模式启用快速启动策略: basicTimeout=%v healthyTimeout=%v", basicTimeout, healthyTimeout)
	}

	basicErr := g.waitForBasicConnection(basicTimeout)
	if basicErr != nil {
		if !debugMode {
			return basicErr
		}
		log.Warnf("调试模式: Redis基础连接未就绪，将继续启动（err=%v）", basicErr)
	}

	var healthyErr error
	if basicErr == nil {
		healthyErr = g.waitForHealthyCluster(ctx, healthyTimeout)
		if healthyErr != nil {
			if !debugMode {
				return healthyErr
			}
			log.Warnf("调试模式: Redis健康检查未通过，将在后台持续重试（err=%v）", healthyErr)
		}
	}

	if basicErr == nil && healthyErr == nil {
		log.Info("✅ Redis集群完全启动并验证成功")
	} else if debugMode {
		log.Warn("⚠️ 调试模式降级: Redis尚未完全就绪，相关功能可能受限，后台重连成功后会自动恢复")
	}

	// 启动监控
	go g.monitorRedisConnection(ctx)
	g.setupRedisClusterMonitoring()

	return nil
}

// 等待集群健康状态 - 添加 nil 检查
func (g *GenericAPIServer) waitForHealthyCluster(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for attempt := 1; time.Now().Before(deadline); attempt++ {
		// 🔥 添加 nil 检查
		if g.redis == nil {
			log.Warnf("RedisCluster实例为空（尝试 %d 次）", attempt)
			time.Sleep(2 * time.Second)
			continue
		}

		redisClient := g.redis.GetClient()
		if redisClient != nil {
			if err := g.pingRedis(ctx, redisClient); err == nil {
				log.Infof("Redis集群健康检查通过（尝试 %d 次）", attempt)
				return nil
			}
		}

		if attempt%2 == 0 {
			log.Infof("等待Redis集群健康检查...（尝试 %d 次）", attempt)
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("Redis集群健康检查超时（%v）", timeout)
}

// 等待基础连接建立 - 添加 nil 检查
func (g *GenericAPIServer) waitForBasicConnection(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for attempt := 1; time.Now().Before(deadline); attempt++ {
		// 🔥 添加 nil 检查
		if g.redis == nil {
			log.Warnf("RedisCluster实例为空（尝试 %d 次）", attempt)
			time.Sleep(1 * time.Second)
			continue
		}

		if storage.Connected() && g.redis.GetClient() != nil {
			log.Infof("✅ Redis基础连接建立（尝试 %d 次）", attempt)
			return nil
		}

		if attempt%3 == 0 {
			log.Infof("等待Redis基础连接...（尝试 %d 次）", attempt)
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("Redis基础连接建立超时（%v）", timeout)
}

// setupRedisClusterMonitoring 设置Redis集群监控
func (g *GenericAPIServer) setupRedisClusterMonitoring() {
	// 从Redis配置中获取集群节点地址
	nodes := g.options.RedisOptions.Addrs
	if len(nodes) == 0 {
		// 如果没有配置集群地址，使用默认的单节点地址
		nodes = []string{fmt.Sprintf("%s:%d", g.options.RedisOptions.Host, g.options.RedisOptions.Port)}
	}

	log.Infof("启动Redis集群监控，节点: %v", nodes)

	// 创建集群监控器
	monitor := metrics.NewRedisClusterMonitor(
		"generic_api_server_cluster", // 集群名称
		nodes,                        // 集群节点地址
		30*time.Second,               // 每30秒采集一次
	)

	// 启动监控
	go monitor.Start(context.Background())

	log.Info("✅ Redis集群监控已启动")
}

// pingRedis 支持redis.UniversalClient类型
func (g *GenericAPIServer) pingRedis(ctx context.Context, client redis.UniversalClient) error {
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 检查集群状态
	actualNodeCount := 0

	if clusterClient, ok := client.(*redis.ClusterClient); ok {
		// 集群模式：检查集群信息
		clusterInfo, err := clusterClient.ClusterInfo(pingCtx).Result()
		if err != nil {
			return fmt.Errorf("集群状态检查失败: %v", err)
		}

		// 执行 CLUSTER NODES 命令获取完整集群信息
		clusterNodes, err := clusterClient.ClusterNodes(pingCtx).Result()
		if err != nil {
			log.Warnf("执行CLUSTER NODES失败: %v", err)
		} else {
			//log.Infof("=== Redis集群节点详情 ===")
			//log.Infof("配置的节点数量: %d", len(g.options.RedisOptions.Addrs))
			//log.Infof("配置的节点列表: %v", g.options.RedisOptions.Addrs)

			lines := strings.Split(clusterNodes, "\n")
			for _, line := range lines {
				if strings.TrimSpace(line) != "" {
					actualNodeCount++
					//		log.Infof("节点 %d: %s", actualNodeCount, strings.TrimSpace(line))
				}
			}
			//		log.Infof("实际发现的节点数量: %d", actualNodeCount)
		}

		// 解析集群信息
		//log.Infof("=== Redis集群状态 ===")
		infoLines := strings.Split(clusterInfo, "\n")
		for _, line := range infoLines {
			if strings.Contains(line, ":") {
				//			log.Infof("  %s", strings.TrimSpace(line))
			}
		}

		// 🔥 修改：同时检查主节点和从节点
		var lastError error
		masterCount := 0
		slaveCount := 0
		successCount := 0
		failedNodes := make([]string, 0)

		// 检查所有主节点
		clusterClient.ForEachMaster(pingCtx, func(ctx context.Context, nodeClient *redis.Client) error {
			masterCount++
			if err := nodeClient.Ping(ctx).Err(); err != nil {
				addr := nodeClient.Options().Addr
				if addr == "" {
					addr = fmt.Sprintf("master-%d", masterCount)
				}
				log.Warnf("主节点 %d PING 失败: addr=%s err=%v", masterCount, addr, err)
				failedNodes = append(failedNodes, fmt.Sprintf("master@%s:%v", addr, err))
				lastError = err
			} else {
				//			log.Infof("✅ 主节点 %d PING 成功", masterCount)
				successCount++
			}
			return nil
		})

		// 检查所有从节点
		err = clusterClient.ForEachSlave(pingCtx, func(ctx context.Context, nodeClient *redis.Client) error {
			slaveCount++
			if err := nodeClient.Ping(ctx).Err(); err != nil {
				addr := nodeClient.Options().Addr
				if addr == "" {
					addr = fmt.Sprintf("slave-%d", slaveCount)
				}
				log.Warnf("从节点 %d PING 失败: addr=%s err=%v", slaveCount, addr, err)
				failedNodes = append(failedNodes, fmt.Sprintf("slave@%s:%v", addr, err))
				lastError = err
			} else {
				//		log.Infof("✅ 从节点 %d PING 成功", slaveCount)
				successCount++
			}
			return nil
		})

		totalNodes := masterCount + slaveCount
		if len(failedNodes) > 0 {
			log.Warnf("Redis节点健康检查失败: %s", strings.Join(failedNodes, "; "))
		}

		// log.Infof("=== Redis集群健康检查总结 ===")
		// log.Infof("主节点数: %d, 从节点数: %d", masterCount, slaveCount)
		// log.Infof("总节点数: %d, 成功节点: %d, 失败节点: %d", totalNodes, successCount, totalNodes-successCount)

		if successCount == 0 {
			return fmt.Errorf("所有集群节点PING检查失败")
		}

		// 🔥 修改：检查是否所有配置的节点都被发现
		seen := make(map[string]struct{})
		for _, addr := range g.options.RedisOptions.Addrs {
			trimmed := strings.TrimSpace(addr)
			if trimmed == "" {
				continue
			}
			seen[trimmed] = struct{}{}
		}
		expectedNodes := len(seen)
		if expectedNodes == 0 {
			expectedNodes = totalNodes
		}
		observedNodes := totalNodes
		if actualNodeCount > 0 {
			observedNodes = actualNodeCount
		}
		if observedNodes != expectedNodes {
			log.Warnf("⚠️  节点数量不匹配: 配置%d个, 集群中发现%d个", expectedNodes, observedNodes)
		} else {
			//		log.Infof("✅ 节点数量匹配: %d个", totalNodes)
		}

		if successCount < totalNodes {
			log.Warnf("部分节点连接异常 (%d/%d 成功)", successCount, totalNodes)
		} else {
			//		log.Infof("✅ 所有节点连接正常")
		}

		// 如果至少有一个节点正常，认为集群可用
		if err != nil && lastError != nil {
			return fmt.Errorf("集群节点检查异常: %v", lastError)
		}

		return nil
	}

	// 单机模式或普通客户端
	return client.Ping(pingCtx).Err()
}

// 打印Kafka配置信息
func (g *GenericAPIServer) printKafkaConfigInfo() {
	kafkaOpts := g.options.KafkaOptions
	operationCount := 0
	retryCount := 0
	if g.consumers != nil {
		operationCount = len(g.consumers.List(userOperationConsumerKey))
		retryCount = len(g.consumers.List(userRetryConsumerKey))
	}
	if operationCount == 0 {
		operationCount = 1
	}

	log.Debugf("📊 Kafka配置信息:")
	log.Debugf("  运行模式: %s", g.options.ServerRunOptions.Mode)
	log.Debugf("  Brokers: %v", kafkaOpts.Brokers)
	log.Debugf("  主题配置:")
	log.Debugf("    - 操作主通道: %s (%d个消费者实例)", UserOperationTopic, operationCount)
	log.Debugf("    - 重试通道: %s (%d个消费者实例)", UserOperationRetryTopic, retryCount)
	log.Debugf("    - 补偿通道: %s", UserOperationCompTopic)
	log.Debugf("  配置参数:")
	log.Debugf("    - 最大重试: %d", kafkaOpts.MaxRetries)
	log.Debugf("    - 操作消费者实例数量: %d", operationCount)
	log.Debugf("    - 批量大小: %d", kafkaOpts.BatchSize)
	log.Debugf("    - 批量超时: %v", kafkaOpts.BatchTimeout)
	log.Debugf("    - Flush Frequency: %v", kafkaOpts.FlushFrequency)
	log.Debugf("    - Flush Max Messages: %d", kafkaOpts.FlushMaxMessages)
}

func waitForMySQLReady(db *gorm.DB, timeout time.Duration) error {
	if db == nil {
		return fmt.Errorf("mysql数据库连接为空")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	attempt := 0

	for {
		err := db.WithContext(ctx).Exec("SELECT 1").Error
		attempt++
		if err == nil {
			log.Debugf("MySQL就绪（尝试 %d 次）", attempt)
			return nil
		}

		if ctx.Err() != nil {
			return fmt.Errorf("MySQL就绪检查超时: %w", ctx.Err())
		}

		if attempt%3 == 0 {
			log.Debugf("等待MySQL就绪...（尝试 %d 次, 错误: %v）", attempt, err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("MySQL就绪检查被取消: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ========== 新增：集群初始化函数 ==========
func initializeGaleraCluster(datastore *mysql.Datastore) error {
	maxRetries := 20                 // 最大重试次数
	retryInterval := 2 * time.Second // 重试间隔

	for attempt := 1; attempt <= maxRetries; attempt++ {
		status := datastore.ClusterStatus()

		log.Debugf("🔍 集群健康检查 [%d/%d]: 主节点=%v, 副本=%d/%d 健康",
			attempt, maxRetries, status.PrimaryHealthy, status.HealthyReplicas, status.ReplicaCount)

		// 检查集群健康条件
		if status.PrimaryHealthy {
			if status.HealthyReplicas >= 1 {
				// 理想状态：主节点健康且至少1个副本健康
				log.Debugf("✅ Galera集群状态良好: 主节点健康，%d个副本节点可用", status.HealthyReplicas)
				return nil
			} else if status.HealthyReplicas == 0 {
				// 只有主节点健康（可能是单节点集群或副本节点故障）
				log.Warn("⚠️  Galera集群主节点健康，但副本节点不可用")
				return nil // 仍然继续启动
			}
		}

		if attempt < maxRetries {
			log.Debugf("⏳ 集群未就绪，%v后重试...", retryInterval)
			time.Sleep(retryInterval)
		}
	}

	// 最终检查
	finalStatus := datastore.ClusterStatus()
	if !finalStatus.PrimaryHealthy {
		return fmt.Errorf("Galera集群主节点不可用，请检查集群状态")
	}

	log.Warn("⚠️  Galera集群部分节点不可用，但服务将继续启动")
	return nil
}

// getTopicPartitionCount returns the number of partitions for the given topic using kafka.Client.Metadata
func getTopicPartitionCount(ctx context.Context, brokers []string, topic string) (int, error) {
	if len(brokers) == 0 {
		return 0, fmt.Errorf("no brokers provided")
	}

	admin := &kafka.Client{Addr: kafka.TCP(brokers...)}
	metadata, err := admin.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, err
	}

	for _, t := range metadata.Topics {
		if t.Name == topic {
			return len(t.Partitions), nil
		}
	}
	return 0, fmt.Errorf("topic %s not found in metadata", topic)
}

// getPartitionsWithoutOwner queries the consumer group and topic metadata to compute the number
// of partitions of 'topic' that are not currently assigned to any member of the consumer group.
// It uses kafka.Client to fetch Metadata and DescribeGroups.
func getPartitionsWithoutOwner(ctx context.Context, brokers []string, groupID, topic string) (int, error) {
	if len(brokers) == 0 {
		return 0, fmt.Errorf("no brokers provided")
	}

	admin := &kafka.Client{Addr: kafka.TCP(brokers...)}

	// 1) 获取 topic partitions
	metadata, err := admin.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, fmt.Errorf("metadata error: %w", err)
	}
	var topicMeta *kafka.Topic
	for _, t := range metadata.Topics {
		if t.Name == topic {
			topicMeta = &t
			break
		}
	}
	if topicMeta == nil {
		return 0, fmt.Errorf("topic %s not found", topic)
	}
	totalPartitions := len(topicMeta.Partitions)

	// 2) Describe group to get member assignments
	describeResp, err := admin.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{groupID}})
	if err != nil {
		return 0, fmt.Errorf("describe groups error: %w", err)
	}
	if len(describeResp.Groups) == 0 {
		// 没有成员，所有分区都没有 owner
		return totalPartitions, nil
	}

	// Collect partitions that are owned by members (for the topic)
	owned := make(map[int]struct{})
	for _, g := range describeResp.Groups {
		for _, member := range g.Members {
			// Use MemberAssignments (Topics/Partitions)
			for _, t := range member.MemberAssignments.Topics {
				if t.Topic != topic {
					continue
				}
				for _, p := range t.Partitions {
					owned[p] = struct{}{}
				}
			}
			// Also include OwnedPartitions from MemberMetadata for cooperative assignor
			for _, op := range member.MemberMetadata.OwnedPartitions {
				if op.Topic != topic {
					continue
				}
				for _, p := range op.Partitions {
					owned[p] = struct{}{}
				}
			}
		}
	}

	// If we couldn't find owned partitions via DescribeGroups parsing, fallback to 0 ownership (conservative)
	if len(owned) == 0 {
		// Fallback: use ConsumerOffsets (deprecated helper) to see committed offsets for group/topic
		if offs, err := admin.ConsumerOffsets(ctx, kafka.TopicAndGroup{Topic: topic, GroupId: groupID}); err == nil {
			for pid := range offs {
				owned[pid] = struct{}{}
			}
		}
	}

	// Count partitions without owner
	noOwner := 0
	for _, p := range topicMeta.Partitions {
		if _, ok := owned[p.ID]; !ok {
			noOwner++
		}
	}

	return noOwner, nil
}
