package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// 扩展 Options 结构体以支持集群
type Options struct {
	Host               string           // 数据库主机地址（如 "127.0.0.1:3306"）
	Username           string           // 数据库访问用户名
	Password           string           // 数据库访问密码
	Database           string           // 要连接的数据库名
	MaxIdleConnections int              // 连接池中的最大空闲连接数
	MaxOpenConnections int              // 与数据库的最大打开连接数
	LogLevel           int              // 日志级别（GORM 日志详细程度）
	Logger             logger.Interface // GORM 日志器实例（用于自定义日志输出）
	SlowQueryThreshold time.Duration    // 自定义慢查询告警阈值
	TablePrefix        string           // 表前缀
	Timeout            time.Duration    // 连接超时

	// ========== 新增：集群配置 ==========
	PrimaryHost         string        `json:"primary-host,omitempty"`          // 主节点主机
	PrimaryPort         int           `json:"primary-port,omitempty"`          // 主节点端口
	ReplicaHosts        []string      `json:"replica-hosts,omitempty"`         // 副本节点主机列表
	ReplicaPorts        []int         `json:"replica-ports,omitempty"`         // 副本节点端口列表
	LoadBalance         bool          `json:"load-balance,omitempty"`          // 是否启用负载均衡
	FailoverEnabled     bool          `json:"failover-enabled,omitempty"`      // 是否启用故障转移
	HealthCheckInterval time.Duration `json:"health-check-interval,omitempty"` // 健康检查间隔
	MaxRetryAttempts    int           `json:"max-retry-attempts,omitempty"`    // 最大重试次数
	RetryInterval       time.Duration `json:"retry-interval,omitempty"`        // 重试间隔
	ReadTimeout         time.Duration `json:"read-timeout,omitempty"`          // 读超时
	WriteTimeout        time.Duration `json:"write-timeout,omitempty"`         // 写超时
	DialTimeout         time.Duration `json:"dial-timeout,omitempty"`          // 连接建立超时
	ConnMaxLifetime     time.Duration `json:"conn-max-lifetime,omitempty"`     // 连接最大生命周期
	ConnMaxIdleTime     time.Duration `json:"conn-max-idle-time,omitempty"`    // 空闲连接最大存活时间
}

// 数据库连接管理器
type DBManager struct {
	primaryDB  *gorm.DB
	replicaDBs []*gorm.DB
	opts       *Options
	mu         sync.RWMutex
	currentIdx int
	healthy    bool
}

// 创建数据库管理器（支持集群）
func NewDBManager(opts *Options) (*DBManager, error) {
	mgr := &DBManager{
		opts:       opts,
		healthy:    false,
		currentIdx: 0,
	}

	// 设置默认选项
	setDefaultOptions(opts)

	// 连接主数据库（写操作）
	primaryDSN := buildDSN(opts.PrimaryHost, opts.PrimaryPort, opts)
	db, err := connectWithRetry(primaryDSN, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to primary database: %v", err)
	}
	mgr.primaryDB = db

	// 连接副本数据库（读操作）
	if opts.LoadBalance && len(opts.ReplicaHosts) > 0 {
		for i, host := range opts.ReplicaHosts {
			if i < len(opts.ReplicaPorts) {
				replicaDSN := buildDSN(host, opts.ReplicaPorts[i], opts)
				replicaDB, err := connectWithRetry(replicaDSN, opts)
				if err != nil {
					log.Warnf("Failed to connect to replica %s:%d: %v", host, opts.ReplicaPorts[i], err)
					continue
				}
				mgr.replicaDBs = append(mgr.replicaDBs, replicaDB)
			}
		}
	}

	// 如果没有可用的副本，使用主数据库作为副本
	if len(mgr.replicaDBs) == 0 {
		mgr.replicaDBs = []*gorm.DB{mgr.primaryDB}
	}

	mgr.healthy = true

	// 启动健康检查
	if opts.HealthCheckInterval > 0 && opts.FailoverEnabled {
		go mgr.healthCheck()
	}

	log.Debugf("Database cluster initialized: primary=1, replicas=%d, load_balance=%v",
		len(mgr.replicaDBs), opts.LoadBalance)

	return mgr, nil
}

// 获取读连接（负载均衡）
func (m *DBManager) GetReadDB() *gorm.DB {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.healthy || len(m.replicaDBs) == 0 {
		return m.primaryDB
	}

	// 简单的轮询负载均衡
	m.currentIdx = (m.currentIdx + 1) % len(m.replicaDBs)
	return m.replicaDBs[m.currentIdx]
}

// 获取写连接（总是主节点）
func (m *DBManager) GetWriteDB() *gorm.DB {
	return m.primaryDB
}

// 健康检查
func (m *DBManager) healthCheck() {
	ticker := time.NewTicker(m.opts.HealthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		m.checkConnections()
	}
}

func (m *DBManager) checkConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查主节点
	primaryHealthy := m.pingDB(m.primaryDB)
	if !primaryHealthy {
		log.Errorf("Primary database connection lost")
	}

	// 检查副本节点并收集健康的副本
	healthyReplicas := make([]*gorm.DB, 0)
	for i, db := range m.replicaDBs {
		if m.pingDB(db) {
			healthyReplicas = append(healthyReplicas, db)
		} else {
			log.Warnf("Replica database %d connection lost", i)
		}
	}

	// 更新副本列表
	if len(healthyReplicas) > 0 {
		m.replicaDBs = healthyReplicas
	} else {
		// 如果没有健康的副本，使用主数据库
		m.replicaDBs = []*gorm.DB{m.primaryDB}
	}

	m.healthy = primaryHealthy || len(healthyReplicas) > 0
}

// PoolStats 返回当前主库与副本库的连接池统计数据，帮助调用方在压测时观测连接池利用率。
type PoolStats struct {
	Role  string
	Index int
	Stats sql.DBStats
}

// GetPoolStats 快照当前各个数据库连接池（主库+副本）的统计信息。
func (m *DBManager) GetPoolStats() []PoolStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]PoolStats, 0, 1+len(m.replicaDBs))

	if m.primaryDB != nil {
		if sqlDB, err := m.primaryDB.DB(); err == nil {
			stats = append(stats, PoolStats{
				Role:  "primary",
				Index: -1,
				Stats: sqlDB.Stats(),
			})
		}
	}

	for i, replica := range m.replicaDBs {
		if replica == nil {
			continue
		}
		if sqlDB, err := replica.DB(); err == nil {
			stats = append(stats, PoolStats{
				Role:  "replica",
				Index: i,
				Stats: sqlDB.Stats(),
			})
		}
	}

	return stats
}

// 检查数据库连接是否健康
func (m *DBManager) pingDB(db *gorm.DB) bool {
	sqlDB, err := db.DB()
	if err != nil {
		return false
	}
	return sqlDB.Ping() == nil
}

// 关闭所有数据库连接
func (m *DBManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	// 关闭主数据库
	if m.primaryDB != nil {
		if sqlDB, err := m.primaryDB.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close primary DB failed: %v", err))
			}
		}
	}

	// 关闭副本数据库
	for i, db := range m.replicaDBs {
		if db != m.primaryDB { // 避免重复关闭
			if sqlDB, err := db.DB(); err == nil {
				if err := sqlDB.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close replica DB %d failed: %v", i, err))
				}
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing database connections: %v", errs)
	}

	m.healthy = false
	return nil
}

// 构建DSN
func buildDSN(host string, port int, opts *Options) string {
	return fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=%t&loc=%s&timeout=%s&readTimeout=%s&writeTimeout=%s&tx_isolation='READ-COMMITTED'&interpolateParams=true&clientFoundRows=true`,
		opts.Username,
		opts.Password,
		host,
		port,
		opts.Database,
		true,
		"Local",
		opts.DialTimeout,
		opts.ReadTimeout,
		opts.WriteTimeout)
}

// 带重试的连接
func connectWithRetry(dsn string, opts *Options) (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	for attempt := 0; attempt < opts.MaxRetryAttempts; attempt++ {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger:                                   opts.Logger,
			PrepareStmt:                              true,
			SkipDefaultTransaction:                   true,
			DisableForeignKeyConstraintWhenMigrating: true,
			NamingStrategy: schema.NamingStrategy{
				TablePrefix:   opts.TablePrefix,
				SingularTable: true,
			},
		})

		if err == nil {
			if execErr := db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; execErr != nil {
				log.Warnf("failed to set session isolation level (attempt %d/%d): %v", attempt+1, opts.MaxRetryAttempts, execErr)
			}
			// 配置连接池
			sqlDB, err := db.DB()
			if err != nil {
				return nil, err
			}
			sqlDB.SetMaxOpenConns(opts.MaxOpenConnections)
			sqlDB.SetConnMaxLifetime(opts.ConnMaxLifetime)
			sqlDB.SetMaxIdleConns(opts.MaxIdleConnections)
			sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)

			// 健康检查
			if err := sqlDB.Ping(); err == nil {
				return db, nil
			}
		}

		if attempt < opts.MaxRetryAttempts-1 {
			time.Sleep(opts.RetryInterval)
			log.Debugf("Retrying database connection (attempt %d/%d)", attempt+1, opts.MaxRetryAttempts)
		}
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %v", opts.MaxRetryAttempts, err)
}

// 原有的单节点连接函数（保持向后兼容）
func New(opts *Options) (*gorm.DB, error) {
	// 设置默认参数
	setDefaultOptions(opts)

	// 构建DSN
	dsn := fmt.Sprintf(`%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s&timeout=%s&readTimeout=%s&writeTimeout=%s&tx_isolation=READ-COMMITTED&interpolateParams=true&clientFoundRows=true`,
		opts.Username,
		opts.Password,
		opts.Host,
		opts.Database,
		true,
		"Local",
		opts.DialTimeout,
		opts.ReadTimeout,
		opts.WriteTimeout)

	// 创建数据库连接
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:                                   opts.Logger,
		PrepareStmt:                              true,
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   opts.TablePrefix,
			SingularTable: true,
		},
	})

	if err != nil {
		log.Errorf("failed to open database: %v", err)
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	if execErr := db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; execErr != nil {
		log.Warnf("failed to set session isolation level: %v", execErr)
	}

	// 获取底层sql.DB并配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		log.Errorf("failed to get sql.DB: %v", err)
		return nil, fmt.Errorf("failed to get sql.DB: %v", err)
	}

	sqlDB.SetMaxOpenConns(opts.MaxOpenConnections)
	sqlDB.SetConnMaxLifetime(opts.ConnMaxLifetime)
	sqlDB.SetMaxIdleConns(opts.MaxIdleConnections)
	sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)

	// 健康检查
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("database ping failed: %v", err)
	}

	// 添加查询超时回调
	addQueryTimeoutCallbacks(db, opts.Timeout)

	log.Debugf("Database connection initialized: Host=%s, MaxOpenConns=%d, MaxIdleConns=%d",
		opts.Host, opts.MaxOpenConnections, opts.MaxIdleConnections)

	return db, nil
}

// 设置默认选项
func setDefaultOptions(opts *Options) {
	// 原有默认值设置
	if opts.MaxOpenConnections <= 0 {
		opts.MaxOpenConnections = 100
	}
	if opts.MaxIdleConnections <= 0 {
		opts.MaxIdleConnections = 20
	}
	if opts.ConnMaxLifetime <= 0 {
		opts.ConnMaxLifetime = 1 * time.Hour
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}

	// 新增集群配置默认值
	if opts.PrimaryHost == "" {
		opts.PrimaryHost = "192.168.10.8"
	}
	if opts.PrimaryPort == 0 {
		opts.PrimaryPort = 3306
	}
	if len(opts.ReplicaHosts) == 0 {
		opts.ReplicaHosts = []string{opts.PrimaryHost}
		opts.ReplicaPorts = []int{opts.PrimaryPort}
	}
	if opts.HealthCheckInterval <= 0 {
		opts.HealthCheckInterval = 30 * time.Second
	}
	if opts.MaxRetryAttempts <= 0 {
		opts.MaxRetryAttempts = 3
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = 1 * time.Second
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = 30 * time.Second
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 30 * time.Second
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.ConnMaxLifetime <= 0 {
		opts.ConnMaxLifetime = 1 * time.Hour
	}
	if opts.ConnMaxIdleTime <= 0 {
		opts.ConnMaxIdleTime = 10 * time.Minute
	}

	if opts.SlowQueryThreshold <= 0 {
		opts.SlowQueryThreshold = 200 * time.Millisecond
	}
	opts.Logger = newGormLogger(opts)
}

// 原有的超时回调函数保持不变
func addQueryTimeoutCallbacks(db *gorm.DB, timeout time.Duration) {
	// 为Create操作添加超时
	db.Callback().Create().Before("gorm:create").Register("query_timeout:create", func(db *gorm.DB) {
		setQueryTimeout(db, timeout)
	})
	db.Callback().Create().After("gorm:create").Register("query_timeout:create_cleanup", cleanupTimeout)

	// 为Query操作添加超时
	db.Callback().Query().Before("gorm:query").Register("query_timeout:query", func(db *gorm.DB) {
		setQueryTimeout(db, timeout)
	})
	db.Callback().Query().After("gorm:query").Register("query_timeout:query_cleanup", cleanupTimeout)

	// 为Update操作添加超时
	db.Callback().Update().Before("gorm:update").Register("query_timeout:update", func(db *gorm.DB) {
		setQueryTimeout(db, timeout)
	})
	db.Callback().Update().After("gorm:update").Register("query_timeout:update_cleanup", cleanupTimeout)

	// 为Delete操作添加超时
	db.Callback().Delete().Before("gorm:delete").Register("query_timeout:delete", func(db *gorm.DB) {
		setQueryTimeout(db, timeout)
	})
	db.Callback().Delete().After("gorm:delete").Register("query_timeout:delete_cleanup", cleanupTimeout)
}

func setQueryTimeout(db *gorm.DB, timeout time.Duration) {
	if db.Statement.Context == nil || db.Statement.Context.Done() == nil {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		db.Statement.Context = ctx
		db.InstanceSet("query_timeout_cancel", cancel)
	}
}

func cleanupTimeout(db *gorm.DB) {
	if cancel, ok := db.InstanceGet("query_timeout_cancel"); ok {
		if c, ok := cancel.(context.CancelFunc); ok {
			c()
		}
		db.InstanceSet("query_timeout_cancel", nil)
	}
}

// 集群状态信息
type ClusterStatus struct {
	PrimaryHealthy  bool `json:"primary_healthy"`
	ReplicaCount    int  `json:"replica_count"`
	HealthyReplicas int  `json:"healthy_replicas"`
	LoadBalance     bool `json:"load_balance"`
	FailoverEnabled bool `json:"failover_enabled"`
}

// 获取集群状态
func (m *DBManager) GetClusterStatus() ClusterStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthyReplicas := 0
	for _, db := range m.replicaDBs {
		if m.pingDB(db) {
			healthyReplicas++
		}
	}

	return ClusterStatus{
		PrimaryHealthy:  m.pingDB(m.primaryDB),
		ReplicaCount:    len(m.replicaDBs),
		HealthyReplicas: healthyReplicas,
		LoadBalance:     m.opts.LoadBalance,
		FailoverEnabled: m.opts.FailoverEnabled,
	}
}
