package options

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/auth"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/util/sets"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation/field"
	"github.com/spf13/pflag"
	"golang.org/x/crypto/bcrypt"
)

const (
	DefaultContactLookupTimeout           = 2 * time.Second
	DefaultContactRefreshTimeout          = 3 * time.Second
	DefaultContactPreflightMaxConcurrency = 64
	//MinUserPendingCreateTTL guarantees pending markers survive slow consumer restarts and large Kafka backlogs.
	MinUserPendingCreateTTL = 10 * time.Minute
)

type ServerRunOptions struct {
	Mode                           string        `json:"mode"        mapstructure:"mode"`
	Healthz                        bool          `json:"healthz"     mapstructure:"healthz"`
	Middlewares                    []string      `json:"middlewares" mapstructure:"middlewares"`
	EnableProfiling                bool          `json:"enableProfiling" mapstructure:"enableProfiling"`
	EnableMetrics                  bool          `json:"enableMetrics" mapstructure:"enableMetrics"`
	FastDebugStartup               bool          `json:"fastDebugStartup" mapstructure:"fastDebugStartup"`
	EnableContactWarmup            bool          `json:"enableContactWarmup" mapstructure:"enableContactWarmup"`
	EnableUserTraceLogging         bool          `json:"enableUserTraceLogging" mapstructure:"enableUserTraceLogging"`
	UserTraceLogSampleRate         float64       `json:"userTraceLogSampleRate" mapstructure:"userTraceLogSampleRate"`
	UserTraceForceLogErrors        bool          `json:"userTraceForceLogErrors" mapstructure:"userTraceForceLogErrors"`
	UserTraceDisableLogging        bool          `json:"userTraceDisableLogging" mapstructure:"userTraceDisableLogging"`
	ContactLookupTimeout           time.Duration `json:"contactLookupTimeout" mapstructure:"contactLookupTimeout"`
	ContactRefreshTimeout          time.Duration `json:"contactRefreshTimeout" mapstructure:"contactRefreshTimeout"`
	ContactPreflightMaxConcurrency int           `json:"contactPreflightMaxConcurrency" mapstructure:"contactPreflightMaxConcurrency"`
	// 新增：Cookie相关配置
	CookieDomain             string        `json:"cookieDomain"    mapstructure:"cookieDomain"`
	CookieSecure             bool          `json:"cookieSecure"    mapstructure:"cookieSecure"`
	CtxTimeout               time.Duration `json:"ctxtimeout"    mapstructure:"ctxtimeout"`
	Env                      string        `json:"env"    mapstructure:"env"`
	LoginRateLimit           int           `json:"loginlimit"   mapstructure:"loginlimit"`
	LoginWindow              time.Duration `json:"loginwindow"   mapstructure:"loginwindow"`
	MaxLoginFailures         int           `json:"maxLoginFailures" mapstructure:"maxLoginFailures"`
	LoginFailReset           time.Duration `json:"loginFailReset"   mapstructure:"loginFailReset"`
	LoginFastFailThreshold   int           `json:"loginFastFailThreshold" mapstructure:"loginFastFailThreshold"`
	LoginFastFailMessage     string        `json:"loginFastFailMessage" mapstructure:"loginFastFailMessage"`
	LoginUpdateBuffer        int           `json:"loginUpdateBuffer" mapstructure:"loginUpdateBuffer"`
	LoginUpdateBatchSize     int           `json:"loginUpdateBatchSize" mapstructure:"loginUpdateBatchSize"`
	LoginUpdateFlushInterval time.Duration `json:"loginUpdateFlushInterval" mapstructure:"loginUpdateFlushInterval"`
	LoginUpdateTimeout       time.Duration `json:"loginUpdateTimeout" mapstructure:"loginUpdateTimeout"`
	LoginCredentialCacheTTL  time.Duration `json:"loginCredentialCacheTTL" mapstructure:"loginCredentialCacheTTL"`
	LoginCredentialCacheSize int           `json:"loginCredentialCacheSize" mapstructure:"loginCredentialCacheSize"`
	// WriteRateLimit: 默认的写操作限流阈值（当 Redis 未配置 override 时使用）
	WriteRateLimit int `json:"writeRateLimit"   mapstructure:"writeRateLimit"`
	// AdminToken: 简单的管理API访问令牌（如果为空，只允许本地或 debug 访问）
	AdminToken string `json:"adminToken" mapstructure:"adminToken"`
	// 新增：生产端限流器开关
	EnableRateLimiter bool `json:"enableRateLimiter" mapstructure:"enableRateLimiter"`
	// 并发处理配置
	MaxGoroutines    int           `json:"max-goroutines" mapstructure:"max-goroutines"`
	MaxQueueSize     int           `json:"max-queue-size" mapstructure:"max-queue-size"`
	TimeoutThreshold time.Duration `json:"timeout-threshold" mapstructure:"timeout-threshold"`
	// 新增：Kafka 生产者失败消息的降级目录
	ProducerFallbackDir   string        `json:"producer-fallback-dir" mapstructure:"producer-fallback-dir"`
	PasswordHashCost      int           `json:"password-hash-cost" mapstructure:"password-hash-cost"`
	PasswordHashAlgorithm string        `json:"password-hash-algorithm" mapstructure:"password-hash-algorithm"`
	Argon2Time            uint32        `json:"argon2-time" mapstructure:"argon2-time"`
	Argon2MemoryKB        uint32        `json:"argon2-memory-kb" mapstructure:"argon2-memory-kb"`
	Argon2Parallelism     uint32        `json:"argon2-parallelism" mapstructure:"argon2-parallelism"`
	Argon2KeyLength       uint32        `json:"argon2-key-length" mapstructure:"argon2-key-length"`
	Argon2SaltLength      uint32        `json:"argon2-salt-length" mapstructure:"argon2-salt-length"`
	UserPendingCreateTTL  time.Duration `json:"userPendingCreateTTL" mapstructure:"userPendingCreateTTL"`
}

// NewServerRunOptions 初始化并返回服务器运行的默认配置选项
func NewServerRunOptions() *ServerRunOptions {
	return &ServerRunOptions{
		Mode:                           gin.ReleaseMode,         // Gin框架运行模式：默认生产模式（ReleaseMode），关闭调试日志
		Healthz:                        true,                    // 是否启用健康检查接口（如/healthz），默认开启
		Middlewares:                    []string{},              // 全局启用的中间件列表，默认为空（可动态添加）
		EnableProfiling:                true,                    // 是否启用性能分析（如pprof），默认开启
		EnableMetrics:                  true,                    // 是否启用监控指标采集（如Prometheus），默认开启
		FastDebugStartup:               false,                   // 是否启用快速调试启动模式（跳过部分初始化步骤），默认关闭
		CookieDomain:                   "",                      // Cookie绑定的域名，默认为空（适用于当前域名）
		CookieSecure:                   false,                   // Cookie是否仅通过HTTPS传输，默认关闭（开发环境常用）
		CtxTimeout:                     60 * time.Second,        // 上下文超时时间：请求处理的最大超时，默认60秒
		TimeoutThreshold:               100 * time.Second,       // 单个请求超时阈值：超过该时间强制终止处理
		Env:                            "development",           // 运行环境标识：默认开发环境（development）
		LoginRateLimit:                 500000,                  // 登录接口限流阈值：50万次/分钟（结合LoginWindow生效）
		LoginWindow:                    2 * time.Minute,         // 登录限流时间窗口：2分钟（与LoginRateLimit配合限制单位时间请求数）
		WriteRateLimit:                 500000,                  // 写操作默认限流阈值：每时间窗口内的最大请求数
		MaxLoginFailures:               5,                       // 最大登录失败次数：超过该次数可能触发临时锁定
		LoginFailReset:                 15 * time.Minute,        // 登录失败计数重置时间：15分钟后失败次数清零
		LoginFastFailThreshold:         0,                       // 登录快速失败阈值：超过该值直接返回失败（0表示不启用）
		LoginFastFailMessage:           "系统繁忙，请稍后再试",            // 登录快速失败时返回的提示信息
		LoginUpdateBuffer:              1024,                    // 登录状态更新缓冲区大小：批量处理登录记录的缓冲区容量
		LoginUpdateBatchSize:           64,                      // 登录状态批量更新大小：每次批量写入的最大记录数
		LoginUpdateFlushInterval:       200 * time.Millisecond,  // 登录状态刷新间隔：缓冲区数据定时写入的间隔（200毫秒）
		LoginUpdateTimeout:             2 * time.Second,         // 登录状态更新超时时间：批量写入操作的最大超时
		LoginCredentialCacheTTL:        30 * time.Second,        // 登录凭证缓存有效期：30秒（减少重复校验开销）
		LoginCredentialCacheSize:       1024,                    // 登录凭证缓存最大条目数：最多缓存1024条凭证
		AdminToken:                     "",                      // 管理员令牌：用于绕过部分权限校验的特殊令牌（默认空）
		EnableRateLimiter:              false,                   // 是否启用生产端限流器：默认关闭（按需开启）
		MaxGoroutines:                  100,                     // 默认最大并发处理协程数：控制同时处理请求的协程上限
		MaxQueueSize:                   100,                     // 默认任务队列大小：请求排队等待的最大数量
		EnableUserTraceLogging:         true,                    // 是否启用用户操作跟踪日志：默认开启
		UserTraceLogSampleRate:         1,                       // 用户跟踪日志采样率：10%的请求会记录详细日志
		UserTraceForceLogErrors:        true,                    // 错误时是否强制记录跟踪日志：默认开启（即使未命中采样）
		UserTraceDisableLogging:        false,                   // 是否禁用用户跟踪日志：默认关闭（与EnableUserTraceLogging配合）
		EnableContactWarmup:            true,                    // 是否启用联系人预热：提前加载联系人数据（默认关闭）
		ContactLookupTimeout:           2 * time.Second,         // 联系人查询超时时间：使用默认值
		ContactRefreshTimeout:          3 * time.Second,         // 联系人刷新超时时间：使用默认值
		ContactPreflightMaxConcurrency: 64,                      // 联系人预检查最大并发数：使用默认值
		ProducerFallbackDir:            "/var/log/iam/producer", // 生产者降级日志目录：无法正常发送时的日志存储路径
		PasswordHashCost:               6,                       // 密码哈希计算成本：数值越高越安全但耗时越长（适用于Argon2等算法）
		PasswordHashAlgorithm:          auth.AlgorithmArgon2id,  // 密码哈希算法：默认使用Argon2id（高安全性算法）
		Argon2Time:                     1,                       // Argon2算法的时间成本参数：迭代次数
		Argon2MemoryKB:                 8 * 1024,                // Argon2算法的内存成本参数：8MB（8*1024 KB）
		Argon2Parallelism:              1,                       // Argon2算法的并行度参数：单线程降低 GC/CPU 压力
		Argon2KeyLength:                32,                      // Argon2算法生成的哈希值长度：32字节
		Argon2SaltLength:               16,                      // Argon2算法使用的盐值长度：16字节
		UserPendingCreateTTL:           MinUserPendingCreateTTL, // 待创建用户的有效期：使用最小默认值
	}
}

func (s *ServerRunOptions) Complete() {
	s.applyEnvOverrides()

	// EnableRateLimiter: 如果为零值，设置默认值 true
	// 注意：bool类型零值为false，只有未配置时才设为true
	// 若希望默认关闭，改为 false
	// 这里默认 true
	// 不做处理即可，除非有特殊需求
	// 如果字段为零值，设置默认值；否则保持配置的值

	// Mode: 如果为空，设置默认值
	if s.Mode == "" {
		s.Mode = gin.ReleaseMode
	} else {
		// 验证Mode是否有效，如果无效则使用默认值
		validModes := []string{gin.DebugMode, gin.ReleaseMode, gin.TestMode}
		isValid := false
		for _, mode := range validModes {
			if s.Mode == mode {
				isValid = true
				break
			}
		}
		if !isValid {
			s.Mode = gin.ReleaseMode
		}
	}

	// Healthz: 如果为零值，设置默认值
	if !s.Healthz {
		s.Healthz = true
	}

	if s.UserTraceLogSampleRate < 0 {
		s.UserTraceLogSampleRate = 0
	} else if s.UserTraceLogSampleRate > 1 {
		s.UserTraceLogSampleRate = 1
	}

	// Middlewares: 如果为nil或空，设置默认空切片
	if s.Middlewares == nil {
		s.Middlewares = []string{}
	}

	// EnableProfiling 默认为 true，但允许显式关闭
	// EnableMetrics 默认为 true，但允许显式关闭

	// CookieDomain: 如果为空，设置默认值
	if s.CookieDomain == "" {
		s.CookieDomain = ""
	}

	// CookieSecure: 设置默认值（如果需要）
	// 注意：bool类型的零值是false，所以这里根据业务需求决定
	// 如果希望默认是false，可以不做处理

	// CtxTimeout: 如果为零值，设置默认值
	if s.CtxTimeout <= 0 {
		s.CtxTimeout = 5 * time.Second
	}
	if s.Env == "" {
		s.Env = "Env"
	}

	if s.LoginRateLimit == 0 {
		s.LoginRateLimit = 1000
	}

	if s.WriteRateLimit == 0 {
		s.WriteRateLimit = 1000
	}

	if s.LoginWindow == 0 {
		s.LoginWindow = time.Minute
	}

	if s.MaxLoginFailures <= 0 {
		s.MaxLoginFailures = 5
	}

	if s.LoginFailReset <= 0 {
		s.LoginFailReset = 15 * time.Minute
	}
	if s.LoginFastFailThreshold < 0 {
		s.LoginFastFailThreshold = 0
	}
	if s.LoginFastFailMessage == "" {
		s.LoginFastFailMessage = "系统繁忙，请稍后再试"
	}
	if s.LoginUpdateBuffer <= 0 {
		s.LoginUpdateBuffer = 1024
	}
	if s.LoginUpdateBatchSize <= 0 {
		s.LoginUpdateBatchSize = 64
	}
	if s.LoginUpdateFlushInterval <= 0 {
		s.LoginUpdateFlushInterval = 200 * time.Millisecond
	}
	if s.LoginUpdateTimeout <= 0 {
		s.LoginUpdateTimeout = 2 * time.Second
	}
	if s.LoginCredentialCacheTTL <= 0 {
		s.LoginCredentialCacheTTL = 30 * time.Second
	}
	if s.LoginCredentialCacheSize <= 0 {
		s.LoginCredentialCacheSize = 1024
	}

	if s.ContactLookupTimeout <= 0 {
		s.ContactLookupTimeout = DefaultContactLookupTimeout
	}

	if s.ContactRefreshTimeout <= 0 {
		s.ContactRefreshTimeout = DefaultContactRefreshTimeout
	}

	if s.ContactPreflightMaxConcurrency <= 0 {
		s.ContactPreflightMaxConcurrency = DefaultContactPreflightMaxConcurrency
	}

	if strings.TrimSpace(s.PasswordHashAlgorithm) == "" {
		s.PasswordHashAlgorithm = auth.AlgorithmBcrypt
	} else {
		s.PasswordHashAlgorithm = strings.ToLower(strings.TrimSpace(s.PasswordHashAlgorithm))
	}

	if s.PasswordHashAlgorithm == auth.AlgorithmBcrypt {
		if s.PasswordHashCost <= 0 {
			s.PasswordHashCost = bcrypt.DefaultCost
		}
		if s.PasswordHashCost < bcrypt.MinCost {
			s.PasswordHashCost = bcrypt.MinCost
		}
		if s.PasswordHashCost > bcrypt.MaxCost {
			s.PasswordHashCost = bcrypt.MaxCost
		}
	}

	if s.Argon2Time == 0 {
		s.Argon2Time = 1
	}
	if s.Argon2MemoryKB == 0 {
		s.Argon2MemoryKB = 8 * 1024
	}
	if s.Argon2Parallelism == 0 {
		s.Argon2Parallelism = 1
	}
	if s.Argon2KeyLength == 0 {
		s.Argon2KeyLength = 32
	}
	if s.Argon2SaltLength == 0 {
		s.Argon2SaltLength = 16
	}

	if s.UserPendingCreateTTL <= 0 {
		s.UserPendingCreateTTL = MinUserPendingCreateTTL
	}
	if s.UserPendingCreateTTL < MinUserPendingCreateTTL {
		s.UserPendingCreateTTL = MinUserPendingCreateTTL
	}
}

func (s *ServerRunOptions) applyEnvOverrides() {
	if v := strings.TrimSpace(os.Getenv("TRACE_SAMPLE_RATE")); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			s.UserTraceLogSampleRate = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv("TRACE_FORCE_LOG_ERRORS")); v != "" {
		if parsed, err := strconv.ParseBool(v); err == nil {
			s.UserTraceForceLogErrors = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv("TRACE_DISABLE_LOGGING")); v != "" {
		if parsed, err := strconv.ParseBool(v); err == nil {
			s.UserTraceDisableLogging = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv("TRACE_ENABLE_LOGGING")); v != "" {
		if parsed, err := strconv.ParseBool(v); err == nil {
			s.EnableUserTraceLogging = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv("TRACE_EXPORT")); v != "" {
		switch strings.ToLower(v) {
		case "stdout":
			s.EnableUserTraceLogging = true
			s.UserTraceDisableLogging = false
			s.UserTraceLogSampleRate = 1
		case "off":
			s.EnableUserTraceLogging = false
			s.UserTraceDisableLogging = true
		}
	}
}

func (s *ServerRunOptions) Validate() []error {
	var errs = field.ErrorList{}
	var path = field.NewPath("server")

	if s.Mode != "" {
		set := sets.NewString(gin.DebugMode, gin.ReleaseMode, gin.TestMode)
		if !set.Has(s.Mode) {
			errs = append(errs, field.Invalid(path.Child("mode"), s.Mode, "无效的mode模式"))
		}
	}
	if s.Env != "" {
		set := sets.NewString("development", "release", "test")
		if !set.Has(s.Env) {
			errs = append(errs, field.Invalid(path.Child("env"), s.Env, "无效的env模式"))
		}
	}

	// 2. 验证CookieDomain
	if s.CookieDomain != "" {
		domainToValidate := s.CookieDomain
		// 处理通配符域名（如 ".example.com"）
		if strings.HasPrefix(domainToValidate, ".") {
			domainToValidate = strings.TrimPrefix(domainToValidate, ".")
			if domainToValidate == "" {
				errs = append(errs, field.Invalid(
					path.Child("cookieDomain"),
					s.CookieDomain,
					"Cookie域名不能仅为点号",
				))
			}
		}

		// 使用标准的DNS验证
		if validationErrs := validation.IsDNS1123Subdomain(domainToValidate); len(validationErrs) > 0 {
			for _, err := range validationErrs {
				errs = append(errs, field.Invalid(
					path.Child("cookieDomain"),
					s.CookieDomain,
					"Cookie域名格式无效: "+err,
				))
			}
		}
	}
	// 3. 验证CookieSecure的合理性
	if s.CookieSecure && s.Mode == gin.DebugMode {
		errs = append(errs, field.Invalid(
			path.Child("cookieSecure"),
			s.CookieSecure,
			"调试模式下不应启用Secure Cookie（建议设置为false）",
		))
	}

	if s.LoginRateLimit < 0 {
		errs = append(errs, field.Invalid(
			path.Child("loginRateLimit"),
			s.LoginRateLimit,
			"限流数不能小于0",
		))
	}

	if s.LoginWindow < 1 {
		errs = append(errs, field.Invalid(
			path.Child("LoginWindow"),
			s.LoginWindow,
			"限流时间不能小于1",
		))
	}

	if s.MaxLoginFailures <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("maxLoginFailures"),
			s.MaxLoginFailures,
			"最大登录失败次数必须大于0",
		))
	}

	if s.LoginFailReset <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("loginFailReset"),
			s.LoginFailReset,
			"登录失败计数失效时间必须大于0",
		))
	}

	if s.ContactLookupTimeout <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("contactLookupTimeout"),
			s.ContactLookupTimeout,
			"联系人唯一性查库超时时间必须大于0",
		))
	}

	if s.ContactRefreshTimeout <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("contactRefreshTimeout"),
			s.ContactRefreshTimeout,
			"联系人唯一性负缓存刷新超时时间必须大于0",
		))
	}

	if s.ContactPreflightMaxConcurrency <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("contactPreflightMaxConcurrency"),
			s.ContactPreflightMaxConcurrency,
			"联系人预检最大并发数必须大于0",
		))
	}

	if s.PasswordHashAlgorithm != "" {
		algo := strings.ToLower(strings.TrimSpace(s.PasswordHashAlgorithm))
		if algo != auth.AlgorithmBcrypt && algo != auth.AlgorithmArgon2id {
			errs = append(errs, field.Invalid(
				path.Child("passwordHashAlgorithm"),
				s.PasswordHashAlgorithm,
				"仅支持 bcrypt 或 argon2id",
			))
		}
	}

	if s.PasswordHashAlgorithm == "" || strings.ToLower(strings.TrimSpace(s.PasswordHashAlgorithm)) == auth.AlgorithmBcrypt {
		if s.PasswordHashCost < bcrypt.MinCost || s.PasswordHashCost > bcrypt.MaxCost {
			errs = append(errs, field.Invalid(
				path.Child("passwordHashCost"),
				s.PasswordHashCost,
				"bcrypt成本因子超出允许范围",
			))
		}
	}

	if s.UserPendingCreateTTL <= 0 {
		errs = append(errs, field.Invalid(
			path.Child("userPendingCreateTTL"),
			s.UserPendingCreateTTL,
			"pending create 标记的 TTL 必须大于 0",
		))
	}

	if s.UserTraceLogSampleRate < 0 || s.UserTraceLogSampleRate > 1 {
		errs = append(errs, field.Invalid(
			path.Child("userTraceLogSampleRate"),
			s.UserTraceLogSampleRate,
			"用户链路日志采样率必须位于[0,1]区间",
		))
	}

	agg := errs.ToAggregate()
	if agg == nil {
		return nil // 无错误时返回空切片，而非nil
	}
	return agg.Errors()
}

// HashConfig converts server options into a password hashing configuration.
func (s *ServerRunOptions) HashConfig() auth.HashConfig {
	if s == nil {
		return auth.HashConfig{}
	}
	cfg := auth.HashConfig{
		Algorithm:        s.PasswordHashAlgorithm,
		BcryptCost:       s.PasswordHashCost,
		Argon2Time:       s.Argon2Time,
		Argon2MemoryKB:   s.Argon2MemoryKB,
		Argon2KeyLength:  s.Argon2KeyLength,
		Argon2SaltLength: s.Argon2SaltLength,
	}
	if s.Argon2Parallelism > 0 {
		if s.Argon2Parallelism > uint32(^uint8(0)) {
			cfg.Argon2Parallelism = ^uint8(0)
		} else {
			cfg.Argon2Parallelism = uint8(s.Argon2Parallelism)
		}
	}
	return cfg
}

func (s *ServerRunOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&s.EnableRateLimiter, "server.enable-rate-limiter", s.EnableRateLimiter, "是否启用生产端限流器（默认启用）")
	fs.BoolVar(&s.EnableContactWarmup, "server.enable-contact-warmup", s.EnableContactWarmup, "是否在启动后预热邮箱/手机号唯一性缓存（默认关闭）")
	fs.BoolVar(&s.EnableMetrics, "server.enable-metrics", s.EnableMetrics, "是否注册 Prometheus 指标路由")
	fs.BoolVar(&s.EnableProfiling, "server.enable-profiling", s.EnableProfiling, "是否暴露 pprof 调试端点（仅 debug 模式有效）")
	fs.BoolVar(&s.EnableUserTraceLogging, "server.enable-user-trace-logging", s.EnableUserTraceLogging, "是否启用用户API链路追踪日志输出")
	fs.Float64Var(&s.UserTraceLogSampleRate, "server.user-trace-log-sample-rate", s.UserTraceLogSampleRate, "用户API链路日志的采样率（0-1 之间，默认0.1）")
	fs.BoolVar(&s.UserTraceForceLogErrors, "server.user-trace-force-log-errors", s.UserTraceForceLogErrors, "是否在出现错误时强制输出用户链路日志")
	fs.BoolVar(&s.UserTraceDisableLogging, "server.user-trace-disable-logging", s.UserTraceDisableLogging, "是否关闭用户链路日志（仍会在force模式下输出错误日志）")
	fs.StringVarP(&s.Mode, "server.mode", "M", s.Mode, ""+
		"指定服务器运行模式。支持的服务器模式：debug(调试)、test(测试)、release(发布)。")

	fs.BoolVarP(&s.Healthz, "server.healthz", "z", s.Healthz, ""+
		"启用健康检查并安装 /healthz 路由。")

	fs.BoolVar(&s.CookieSecure, "server.cookieSecure", s.CookieSecure, ""+
		"启用cookie安全设置(建议在生成环境下开启。")

	fs.StringVar(&s.CookieDomain, "server.cookieDomain", s.CookieDomain, ""+
		"指定cookie对域的限制.空字符串表示任何域都可以绑定cookie")
	fs.StringSliceVarP(&s.Middlewares, "server.middlewares", "w", s.Middlewares, ""+
		"服务器允许的中间件列表，逗号分隔。如果列表为空，将使用默认中间件。")
	fs.StringVar(&s.Env, "server.env", s.Env, ""+
		"环境模式包括:development,release,test")

	fs.IntVar(&s.LoginRateLimit, "server.Loginlimit", s.LoginRateLimit, ""+
		"指定限流次数")
	fs.DurationVar(&s.LoginWindow, "server.loginwindow", s.LoginWindow, ""+
		"指定限流时间")
	fs.IntVar(&s.MaxLoginFailures, "server.login-max-attempts", s.MaxLoginFailures, ""+
		"同一用户在计数窗口内允许的最大登录失败次数")
	fs.DurationVar(&s.LoginFailReset, "server.login-fail-reset", s.LoginFailReset, ""+
		"登录失败计数的自动重置时间窗口")
	fs.IntVar(&s.LoginFastFailThreshold, "server.login-fastfail-threshold", s.LoginFastFailThreshold, ""+
		"当并发登录请求超过该值时快速返回（0 表示禁用）")
	fs.StringVar(&s.LoginFastFailMessage, "server.login-fastfail-message", s.LoginFastFailMessage, ""+
		"快速降级时返回给客户端的提示信息")
	fs.IntVar(&s.LoginUpdateBuffer, "server.login-update-buffer", s.LoginUpdateBuffer, ""+
		"登录时间异步更新队列缓存大小")
	fs.IntVar(&s.LoginUpdateBatchSize, "server.login-update-batch", s.LoginUpdateBatchSize, ""+
		"登录时间异步更新单次批量写入的最大条数")
	fs.DurationVar(&s.LoginUpdateFlushInterval, "server.login-update-flush-interval", s.LoginUpdateFlushInterval, ""+
		"登录时间异Async更新强制刷新间隔")
	fs.DurationVar(&s.LoginUpdateTimeout, "server.login-update-timeout", s.LoginUpdateTimeout, ""+
		"登录时间批量更新的数据库超时时间")
	fs.DurationVar(&s.LoginCredentialCacheTTL, "server.login-credential-cache-ttl", s.LoginCredentialCacheTTL, ""+
		"登录凭证比较结果在本地缓存的有效期")
	fs.IntVar(&s.LoginCredentialCacheSize, "server.login-credential-cache-size", s.LoginCredentialCacheSize, ""+
		"登录凭证比较结果本地缓存的最大条目数")
	fs.DurationVar(&s.ContactLookupTimeout, "server.contact-lookup-timeout", s.ContactLookupTimeout, "联系人唯一性查库超时阈值")
	fs.DurationVar(&s.ContactRefreshTimeout, "server.contact-refresh-timeout", s.ContactRefreshTimeout, "联系人唯一性负缓存刷新查库超时阈值")
	fs.IntVar(&s.ContactPreflightMaxConcurrency, "server.contact-preflight-max-concurrency", s.ContactPreflightMaxConcurrency, "预检查询允许的最大并发数，用于保护数据库连接数")
	fs.StringVar(&s.AdminToken, "server.admin-token", s.AdminToken,
		"管理API的简单访问令牌（默认为空，仅允许本地访问）")
	fs.BoolVar(&s.FastDebugStartup, "server.fast-debug-startup", s.FastDebugStartup, "调试模式下是否跳过耗时的依赖等待，加速本地调试启动")
	fs.StringVar(&s.ProducerFallbackDir, "server.producer-fallback-dir", s.ProducerFallbackDir, "Directory to store failed Kafka producer messages as a fallback.")
	fs.IntVar(&s.PasswordHashCost, "server.password-hash-cost", s.PasswordHashCost, "设置bcrypt密码哈希成本（范围 4-31，默认10，压测可适当降低）")
	fs.StringVar(&s.PasswordHashAlgorithm, "server.password-hash-algorithm", s.PasswordHashAlgorithm, "密码哈希算法（bcrypt 或 argon2id）")
	fs.Uint32Var(&s.Argon2Time, "server.argon2-time", s.Argon2Time, "argon2id 的迭代次数 (t)，建议>=1")
	fs.Uint32Var(&s.Argon2MemoryKB, "server.argon2-memory-kb", s.Argon2MemoryKB, "argon2id 使用的内存（单位KB）")
	fs.Uint32Var(&s.Argon2Parallelism, "server.argon2-parallelism", s.Argon2Parallelism, "argon2id 并行度 (p)")
	fs.Uint32Var(&s.Argon2KeyLength, "server.argon2-key-length", s.Argon2KeyLength, "argon2id 输出哈希长度 (字节)")
	fs.Uint32Var(&s.Argon2SaltLength, "server.argon2-salt-length", s.Argon2SaltLength, "argon2id 盐长度 (字节)")
	fs.DurationVar(&s.UserPendingCreateTTL, "server.user-pending-create-ttl", s.UserPendingCreateTTL, "Redis 用户创建幂等标记的过期时间")
}
