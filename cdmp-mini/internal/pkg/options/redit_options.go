package options

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

type RedisOptions struct {
	Host                  string        `json:"host"                     mapstructure:"host"                     description:"Redis service host address"`
	Port                  int           `json:"port"                     mapstructure:"port"`
	Addrs                 []string      `json:"addrs"                    mapstructure:"addrs"`
	Username              string        `json:"username"                 mapstructure:"username"`
	Password              string        `json:"password"                 mapstructure:"password"`
	Database              int           `json:"database"                 mapstructure:"database"`
	MasterName            string        `json:"master-name"              mapstructure:"master-name"`
	MaxIdle               int           `json:"optimisation-max-idle"    mapstructure:"optimisation-max-idle"    description:"Maximum number of idle connections"`
	MaxActive             int           `json:"optimisation-max-active"  mapstructure:"optimisation-max-active"  description:"Maximum number of active connections"`
	Timeout               time.Duration `json:"timeout"                  mapstructure:"timeout"                  description:"Connection timeout in seconds"`
	EnableCluster         bool          `json:"enable-cluster"           mapstructure:"enable-cluster"`
	UseSSL                bool          `json:"use-ssl"                  mapstructure:"use-ssl"`
	SSLInsecureSkipVerify bool          `json:"ssl-insecure-skip-verify" mapstructure:"ssl-insecure-skip-verify"`
	IdleTimeout           time.Duration `json:"idle-timeout"             mapstructure:"idle-timeout"             description:"Idle connection timeout in seconds"`
	MaxConnLifetime       time.Duration `json:"max-conn-lifetime"        mapstructure:"max-conn-lifetime"        description:"Maximum connection lifetime in seconds"`
	Wait                  bool          `json:"wait"                     mapstructure:"wait"                     description:"Wait for available connection when pool is exhausted"`
	PoolSize              int           `json:"pool-size"                mapstructure:"pool-size"                description:"Connection pool size per node (cluster mode)"`
	MaxRetries            int           `json:"max-retries"        mapstructure:"max-retries"`
	MaxRetryDelay         time.Duration `json:"max-retry-delay"   mapstructure:"max-retry-delay"`
}

func NewRedisOptions() *RedisOptions {
	return &RedisOptions{
		Addrs: []string{
			"192.168.10.14:6379",
			"192.168.10.14:6380",
			"192.168.10.14:6381",
		},
		Username:              "",
		Password:              "",
		Database:              0,
		MasterName:            "",
		MaxIdle:               25,              // 空闲连接数          // 最大活跃连接数
		Timeout:               5 * time.Second, // 连接超时
		EnableCluster:         true,            // 集群模式
		UseSSL:                false,
		SSLInsecureSkipVerify: false,
		IdleTimeout:           120 * time.Second,  // 空闲超时2分钟
		MaxConnLifetime:       1800 * time.Second, // 连接生命周期30分钟
		Wait:                  true,               // 池耗尽时等待
		PoolSize:              90,                 // 🔥 与MaxActive一致
		MaxRetries:            3,
		MaxRetryDelay:         30 * time.Second,
	}
}

// Complete 补全Redis配置选项，处理默认值和依赖关系
func (r *RedisOptions) Complete() {
	// 地址处理：优先使用Addrs，否则从Host+Port生成
	if len(r.Addrs) == 0 {
		host := r.Host
		if host == "" {
			host = "localhost" // 默认主机地址
		}

		port := r.Port
		if port == 0 {
			port = 6379 // Redis默认端口
		}

		r.Addrs = []string{fmt.Sprintf("%s:%d", host, port)}
	}

	// 连接池配置默认值
	if r.MaxIdle <= 0 {
		r.MaxIdle = 10 // 默认最大空闲连接数
	}
	if r.MaxActive <= 0 {
		r.MaxActive = 100 // 默认最大活跃连接数
	}

	// 超时相关默认值
	if r.Timeout <= 0 {
		r.Timeout = 30 // 默认连接超时时间30秒
	}
	if r.IdleTimeout <= 0 {
		r.IdleTimeout = 300 // 默认空闲连接超时5分钟
	}
	if r.MaxConnLifetime <= 0 {
		r.MaxConnLifetime = 3600 // 默认连接最大生命周期1小时
	}

	// 连接池大小默认值（区分集群和单机模式）
	if r.PoolSize <= 0 {
		if r.EnableCluster {
			r.PoolSize = 50 // 集群模式：每个节点50个连接
		} else {
			r.PoolSize = 10 // 单机模式：10个连接
		}
	}

	// 等待策略默认值（高并发推荐设置为true）
	// Wait字段是bool类型，默认值为false，这里不强制设置

	// SSL安全相关默认值
	if !r.UseSSL {
		r.SSLInsecureSkipVerify = false // 非SSL模式不跳过验证
	}

	// 哨兵模式默认主节点名称
	if r.EnableCluster && r.MasterName == "" {
		r.MasterName = "mymaster" // 默认主节点名称
	}

	// 数据库索引有效性检查
	if r.Database < 0 {
		r.Database = 0 // 确保数据库索引不小于0
	}

	if r.MaxRetries < 0 {
		r.MaxRetries = 3 // 默认重试次数
	}

	if r.MaxRetryDelay <= 0 {
		r.MaxRetryDelay = 30 * time.Second // 默认最大重试延迟
	}
}

// Validate 验证Redis配置选项的有效性，返回所有验证错误
func (r *RedisOptions) Validate() []error {
	var errors []error

	// 验证地址配置 - 必须提供至少一个地址
	if len(r.Addrs) == 0 && (r.Host == "" && r.Port == 0) {
		errors = append(errors, fmt.Errorf("redis配置警告：未提供有效地址，需配置Addrs或Host/Port"))
	}

	// 如果提供了Host但没有Port，或者提供了Port但没有Host
	if (r.Host != "" && r.Port == 0) || (r.Host == "" && r.Port != 0) {
		errors = append(errors, fmt.Errorf("redis配置警告：Host和Port需同时配置或同时不配置"))
	}

	// 验证数据库索引有效性
	if r.Database < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：数据库索引不能为负数"))
	}

	// 验证连接池配置有效性
	if r.MaxIdle < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：最大空闲连接数不能为负数"))
	}
	if r.MaxActive < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：最大活跃连接数不能为负数"))
	}

	// 验证超时配置有效性
	if r.Timeout < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：超时时间不能为负数"))
	}

	// 集群模式验证
	if r.EnableCluster && len(r.Addrs) == 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：启用集群模式时必须配置Addrs"))
	}

	// 主从模式验证
	if r.MasterName != "" && len(r.Addrs) == 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：配置主节点名称时必须配置Addrs"))
	}

	// SSL配置验证
	if r.SSLInsecureSkipVerify && !r.UseSSL {
		errors = append(errors, fmt.Errorf("redis配置警告：仅当UseSSL为true时才能设置SSLInsecureSkipVerify"))
	}

	if r.MaxRetries < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：最大重试次数不能为负数"))
	}

	if r.MaxRetryDelay < 0 {
		errors = append(errors, fmt.Errorf("redis配置警告：最大重试延迟不能为负数"))
	}

	return errors
}

// AddFlags 将Redis配置选项添加为命令行标志
func (r *RedisOptions) AddFlags(fs *pflag.FlagSet) {
	// 基础连接配置
	fs.StringVar(&r.Host, "redis.host", r.Host, "Redis service host address")
	fs.IntVar(&r.Port, "redis.port", r.Port, "Redis service port number")
	fs.StringSliceVar(&r.Addrs, "redis.addrs", r.Addrs, "List of Redis server addresses, format: host:port (used for cluster mode)")
	fs.StringVar(&r.Username, "redis.username", r.Username, "Username for Redis authentication (if required)")
	fs.StringVar(&r.Password, "redis.password", r.Password, "Password for Redis authentication")
	fs.IntVar(&r.Database, "redis.database", r.Database, "Redis database index to use (0-15)")

	// 主从集群配置
	fs.StringVar(&r.MasterName, "redis.master-name", r.MasterName, "Name of the master node in Redis sentinel mode")
	fs.BoolVar(&r.EnableCluster, "redis.enable-cluster", r.EnableCluster, "Enable Redis cluster mode (requires multiple addresses in redis.addrs)")

	// 连接池基础优化配置
	fs.IntVar(&r.MaxIdle, "redis.optimisation-max-idle", r.MaxIdle, "Maximum number of idle connections in the pool (recommended: 10-2000)")
	fs.IntVar(&r.MaxActive, "redis.optimisation-max-active", r.MaxActive, "Maximum number of active connections in the pool (recommended: 100-20000)")
	fs.DurationVar(&r.Timeout, "redis.timeout", r.Timeout,
		"Connection timeout in seconds (0 for no timeout)")
	// 新增的连接池高级配置（针对高并发优化）
	fs.DurationVar(&r.IdleTimeout, "redis.idle-timeout", r.IdleTimeout, "Idle connection timeout in seconds (connections idle longer than this will be closed)")
	fs.DurationVar(&r.MaxConnLifetime, "redis.max-conn-lifetime", r.MaxConnLifetime, "Maximum connection lifetime in seconds (connections older than this will be recycled)")
	fs.BoolVar(&r.Wait, "redis.wait", r.Wait, "Wait for available connection when pool is exhausted (recommended for high concurrency scenarios)")
	fs.IntVar(&r.PoolSize, "redis.pool-size", r.PoolSize, "Connection pool size per node (especially important for cluster mode, recommended: 50-100)")

	// SSL/TLS安全配置
	fs.BoolVar(&r.UseSSL, "redis.use-ssl", r.UseSSL, "Enable SSL/TLS for Redis connections")
	fs.BoolVar(&r.SSLInsecureSkipVerify, "redis.ssl-insecure-skip-verify", r.SSLInsecureSkipVerify, "Skip SSL certificate verification (insecure, not recommended for production)")

	fs.IntVar(&r.MaxRetries, "redis.max-retries", r.MaxRetries, "Maximum number of retries before giving up (default 3)")
	fs.DurationVar(&r.MaxRetryDelay, "redis.max-retry-delay", r.MaxRetryDelay, "Maximum delay between retry attempts (default 30s)")
}
