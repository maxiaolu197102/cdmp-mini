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

type GenericAPIServer struct {
	insecureServer *http.Server
	*gin.Engine
	options           *options.Options
	redis             *storage.RedisCluster
	redisCancel       context.CancelFunc
	initOnce          sync.Once
	producer          producer.MessageProducer
	consumerCtx       context.Context
	consumerCancel    context.CancelFunc
	audit             *audit.Manager
	shutdownOnce      sync.Once
	loginLimit        atomic.Int64
	userService       *user.UserService
	loginUpdates      chan *v1.User
	loginUpdateCtx    context.Context
	loginUpdateCancel context.CancelFunc
	loginUpdateWG     sync.WaitGroup
	credentialCache   *credentialCache
	loginInFlight     atomic.Int64
	datastore         *mysql.Datastore
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
	if g.consumerCancel != nil {
		g.consumerCancel()
	}
	instances := g.getConsumerInstances()
	if instances != nil {
		for _, consumer := range instances.createConsumers {
			if consumer != nil {
				if err := consumer.Close(); err != nil {
					combined = stdErrors.Join(combined, err)
				}
			}
		}
		for _, consumer := range instances.updateConsumers {
			if consumer != nil {
				if err := consumer.Close(); err != nil {
					combined = stdErrors.Join(combined, err)
				}
			}
		}
		for _, consumer := range instances.deleteConsumers {
			if consumer != nil {
				if err := consumer.Close(); err != nil {
					combined = stdErrors.Join(combined, err)
				}
			}
		}
		for _, consumer := range instances.retryConsumers {
			if consumer != nil {
				if err := consumer.Close(); err != nil {
					combined = stdErrors.Join(combined, err)
				}
			}
		}
	}
	if g.producer != nil {
		if err := g.producer.Close(); err != nil {
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
	factory := interfaces.Client()
	if factory == nil {
		return nil
	}
	return factory.Close()
}

func NewGenericAPIServer(opts *options.Options) (*GenericAPIServer, error) {
	// 初始化日志
	log.Infof("正在初始化GenericAPIServer服务器，环境: %s", opts.ServerRunOptions.Mode)
	// 打印 Kafka 实例ID
	if opts.KafkaOptions != nil {
		log.Infof("[Kafka] 当前实例 InstanceID = %s", opts.KafkaOptions.InstanceID)
	}

	//创建服务器实例
	g := &GenericAPIServer{
		Engine:   gin.New(),
		options:  opts,
		initOnce: sync.Once{},
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
		g.producer = newNoopProducer()
		g.setConsumerInstances(nil, nil, nil, nil)
	} else {
		log.Info("kafka服务器启动成功")
		g.auditServiceEvent("kafka", "startup", "success", nil)
	}

	g.initUserService(storeIns)
	g.initCredentialCache()
	g.initLoginUpdater()

	// 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	g.consumerCtx = ctx
	g.consumerCancel = cancel

	// 获取所有消费者实例
	instances := g.getConsumerInstances()
	var consumerReady sync.WaitGroup
	if instances != nil {
		workerCount := g.options.KafkaOptions.WorkerCount
		if workerCount < 1 {
			workerCount = 1
		}

		// 启动所有消费者实例（每个实例1个worker或者配置中的数量）
		for i := 0; i < len(instances.createConsumers); i++ {
			if instances.createConsumers[i] != nil {
				consumerReady.Add(1)
				go instances.createConsumers[i].StartConsuming(ctx, workerCount, &consumerReady)
			}
			if instances.updateConsumers[i] != nil {
				consumerReady.Add(1)
				go instances.updateConsumers[i].StartConsuming(ctx, 1, &consumerReady)
			}
			if instances.deleteConsumers[i] != nil {
				consumerReady.Add(1)
				go instances.deleteConsumers[i].StartConsuming(ctx, 1, &consumerReady)
			}
		}

		// 单独启动重试消费者的所有实例，保证重试主题能在消费者组中均衡分配分区
		if len(instances.retryConsumers) > 0 {
			// 查询 topic 分区数用于指标和并发计算
			partitionCount := 0
			brokers := g.options.KafkaOptions.Brokers
			if len(brokers) > 0 {
				retryCtx, retryCancel := context.WithTimeout(ctx, 5*time.Second)
				p, err := getTopicPartitionCount(retryCtx, brokers, UserRetryTopic)
				retryCancel()
				if err == nil {
					partitionCount = p
				} else {
					if stdErrors.Is(err, context.DeadlineExceeded) {
						log.Warnf("获取 topic %s 分区信息超时，将稍后重试: %v", UserRetryTopic, err)
					} else {
						log.Warnf("获取 topic %s 分区信息失败: %v", UserRetryTopic, err)
					}
				}
			}

			// 更新 prometheus 指标
			retryGroupId := g.consumerGroupID("retry")
			metrics.ConsumerTopicPartitions.WithLabelValues(UserRetryTopic).Set(float64(partitionCount))
			metrics.ConsumerGroupInstances.WithLabelValues(retryGroupId).Set(float64(len(instances.retryConsumers)))
			if len(instances.retryConsumers) == 0 {
				metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserRetryTopic, retryGroupId).Set(float64(partitionCount))
			} else {
				// 简单启发式：当有实例存在时，认为无主分区为0（更精确的检测需要 Kafka admin/group 查询）
				metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserRetryTopic, retryGroupId).Set(0)
			}

			// 根据分区数与实例数计算每个实例需要的 worker 数（上限为 RetryConsumerWorkers）
			workersPerInstance := 1
			if partitionCount > 0 && len(instances.retryConsumers) > 0 {
				workersPerInstance = (partitionCount + len(instances.retryConsumers) - 1) / len(instances.retryConsumers)
				if workersPerInstance > RetryConsumerWorkers {
					workersPerInstance = RetryConsumerWorkers
				}
				if workersPerInstance < 1 {
					workersPerInstance = 1
				}
			}

			for i := 0; i < len(instances.retryConsumers); i++ {
				if instances.retryConsumers[i] != nil {
					consumerReady.Add(1)
					go instances.retryConsumers[i].StartConsuming(ctx, workersPerInstance, &consumerReady)
				}
			}

			// 定期更新 topic/实例/无主分区指标（可配置）
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

							// 更丰富的日志在 Debug 模式下打印
							isDebug := g.options.ServerRunOptions.Mode == "debug"

							if p, err := getTopicPartitionCount(ctx, brokers, UserRetryTopic); err == nil {
								metrics.ConsumerTopicPartitions.WithLabelValues(UserRetryTopic).Set(float64(p))
								metrics.ConsumerGroupInstances.WithLabelValues(retryGroupId).Set(float64(len(instances.retryConsumers)))
								if len(instances.retryConsumers) == 0 {
									metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserRetryTopic, retryGroupId).Set(float64(p))
									if isDebug {
									}
								} else {
									if noOwner, err := getPartitionsWithoutOwner(ctx, brokers, retryGroupId, UserRetryTopic); err == nil {
										metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserRetryTopic, retryGroupId).Set(float64(noOwner))
										if isDebug {

										}
									} else {
										// 回退到启发式
										metrics.ConsumerPartitionsNoOwner.WithLabelValues(UserRetryTopic, retryGroupId).Set(0)
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
		log.Infof("已启动 %d 个消费者实例", len(instances.createConsumers))
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

	// 为每个主题创建多个消费者实例
	consumerCount := kafkaOpts.WorkerCount
	retryconsumerCount := kafkaOpts.RetryWorkerCount

	log.Infof("为每个主题创建 %d 个消费者实例，消费组前缀: %s", consumerCount, g.consumerGroupBase())

	// 创建消费者实例切片
	createConsumers := make([]*UserConsumer, consumerCount)
	updateConsumers := make([]*UserConsumer, consumerCount)
	deleteConsumers := make([]*UserConsumer, consumerCount)
	retryConsumers := make([]*RetryConsumer, retryconsumerCount)

	createGroupID := g.consumerGroupID("create")
	updateGroupID := g.consumerGroupID("update")
	deleteGroupID := g.consumerGroupID("delete")

	for i := 0; i < consumerCount; i++ {
		// 创建消费者实例 - 使用相同的消费组ID
		createConsumers[i] = NewUserConsumer(kafkaOpts, UserCreateTopic,
			createGroupID, db, g.redis)
		createConsumers[i].SetProducer(userProducer)
		createConsumers[i].SetInstanceID(i)
		if g.datastore != nil {
			createConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
		if g.options.ServerRunOptions.EnableRateLimiter {
			//	go createConsumers[i].startLagMonitor(context.Background())
		}

		updateConsumers[i] = NewUserConsumer(kafkaOpts, UserUpdateTopic,
			updateGroupID, db, g.redis)
		updateConsumers[i].SetProducer(userProducer)
		updateConsumers[i].SetInstanceID(i)
		if g.datastore != nil {
			updateConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
		if g.options.ServerRunOptions.EnableRateLimiter {
			//	go updateConsumers[i].startLagMonitor(context.Background())
		}

		deleteConsumers[i] = NewUserConsumer(kafkaOpts, UserDeleteTopic,
			deleteGroupID, db, g.redis)
		deleteConsumers[i].SetProducer(userProducer)
		deleteConsumers[i].SetInstanceID(i)
		if g.datastore != nil {
			deleteConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
		if g.options.ServerRunOptions.EnableRateLimiter {
			//	go deleteConsumers[i].startLagMonitor(context.Background())
		}
	}

	log.Info("初始化重试消费者...")
	retryGroupId := g.consumerGroupID("retry")
	for i := 0; i < kafkaOpts.RetryWorkerCount; i++ {
		retryConsumers[i] = NewRetryConsumer(db, g.redis, userProducer, kafkaOpts, UserRetryTopic, retryGroupId)
		if g.datastore != nil {
			retryConsumers[i].SetPoolStatsProvider(g.datastore.PoolStats)
		}
	}
	// 3. 赋值到服务器实例
	g.producer = userProducer

	// 5. 存储所有消费者实例（新增字段）
	g.setConsumerInstances(createConsumers, updateConsumers, deleteConsumers, retryConsumers)

	return nil
}

func (g *GenericAPIServer) initUserService(factory interfaces.Factory) {
	if g == nil || factory == nil {
		return
	}
	g.userService = user.NewUserService(factory, g.redis, g.options, g.producer, g.audit)
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
	instances := g.getConsumerInstances()
	instanceCount := 1
	if instances != nil {
		instanceCount = len(instances.createConsumers)
	}

	log.Debugf("📊 Kafka配置信息:")
	log.Debugf("  运行模式: %s", g.options.ServerRunOptions.Mode)
	log.Debugf("  Brokers: %v", kafkaOpts.Brokers)
	log.Debugf("  主题配置:")
	log.Debugf("    - 创建: %s (%d个消费者实例)", UserCreateTopic, instanceCount)
	log.Debugf("    - 更新: %s (%d个消费者实例)", UserUpdateTopic, instanceCount)
	log.Debugf("    - 删除: %s (%d个消费者实例)", UserDeleteTopic, instanceCount)
	log.Debugf("    - 重试: %s", UserRetryTopic)
	log.Debugf("  配置参数:")
	log.Debugf("    - 最大重试: %d", kafkaOpts.MaxRetries)
	log.Debugf("    - 消费者实例数量: %d", instanceCount)
	log.Debugf("    - 批量大小: %d", kafkaOpts.BatchSize)
	log.Debugf("    - 批量超时: %v", kafkaOpts.BatchTimeout)
}

// 新增：存储所有消费者实例
type consumerInstances struct {
	createConsumers []*UserConsumer
	updateConsumers []*UserConsumer
	deleteConsumers []*UserConsumer
	retryConsumers  []*RetryConsumer
}

var consumerInstancesStore = &consumerInstances{}

func (g *GenericAPIServer) setConsumerInstances(create, update, delete []*UserConsumer, retry []*RetryConsumer) {
	consumerInstancesStore.createConsumers = create
	consumerInstancesStore.updateConsumers = update
	consumerInstancesStore.deleteConsumers = delete
	consumerInstancesStore.retryConsumers = retry

}

func (g *GenericAPIServer) getConsumerInstances() *consumerInstances {
	return consumerInstancesStore
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
