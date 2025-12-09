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
	operationModeSync       = "sync"
	operationModeQueue      = "queue"
	operationModeRollout    = "rollout"
)

var defaultOperationQueueKinds = []string{
	"create",
	"update",
	"delete",
	"batch",
}

type ServerRunOptions struct {
	Mode                              string        `json:"mode"        mapstructure:"mode"`
	Healthz                           bool          `json:"healthz"     mapstructure:"healthz"`
	Middlewares                       []string      `json:"middlewares" mapstructure:"middlewares"`
	EnableProfiling                   bool          `json:"enableProfiling" mapstructure:"enableProfiling"`
	EnableMetrics                     bool          `json:"enableMetrics" mapstructure:"enableMetrics"`
	FastDebugStartup                  bool          `json:"fastDebugStartup" mapstructure:"fastDebugStartup"`
	EnableContactWarmup               bool          `json:"enableContactWarmup" mapstructure:"enableContactWarmup"`
	EnableUserTraceLogging            bool          `json:"enableUserTraceLogging" mapstructure:"enableUserTraceLogging"`
	UserTraceLogSampleRate            float64       `json:"userTraceLogSampleRate" mapstructure:"userTraceLogSampleRate"`
	UserTraceForceLogErrors           bool          `json:"userTraceForceLogErrors" mapstructure:"userTraceForceLogErrors"`
	UserTraceDisableLogging           bool          `json:"userTraceDisableLogging" mapstructure:"userTraceDisableLogging"`
	ContactLookupTimeout              time.Duration `json:"contactLookupTimeout" mapstructure:"contactLookupTimeout"`
	ContactRefreshTimeout             time.Duration `json:"contactRefreshTimeout" mapstructure:"contactRefreshTimeout"`
	ContactPreflightMaxConcurrency    int           `json:"contactPreflightMaxConcurrency" mapstructure:"contactPreflightMaxConcurrency"`
	ContactDegradeCacheTTL            time.Duration `json:"contactDegradeCacheTTL" mapstructure:"contactDegradeCacheTTL"`
	ContactDegradeHealthCheckInterval time.Duration `json:"contactDegradeHealthCheckInterval" mapstructure:"contactDegradeHealthCheckInterval"`
	ContactDegradeCacheMaxEntries     int           `json:"contactDegradeCacheMaxEntries" mapstructure:"contactDegradeCacheMaxEntries"`
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
	ProducerFallbackDir                string        `json:"producer-fallback-dir" mapstructure:"producer-fallback-dir"`
	PasswordHashCost                   int           `json:"password-hash-cost" mapstructure:"password-hash-cost"`
	PasswordHashAlgorithm              string        `json:"password-hash-algorithm" mapstructure:"password-hash-algorithm"`
	Argon2Time                         uint32        `json:"argon2-time" mapstructure:"argon2-time"`
	Argon2MemoryKB                     uint32        `json:"argon2-memory-kb" mapstructure:"argon2-memory-kb"`
	Argon2Parallelism                  uint32        `json:"argon2-parallelism" mapstructure:"argon2-parallelism"`
	Argon2KeyLength                    uint32        `json:"argon2-key-length" mapstructure:"argon2-key-length"`
	Argon2SaltLength                   uint32        `json:"argon2-salt-length" mapstructure:"argon2-salt-length"`
	UserPendingCreateTTL               time.Duration `json:"userPendingCreateTTL" mapstructure:"userPendingCreateTTL"`
	OperationMode                      string        `json:"operationMode" mapstructure:"operationMode"`
	OperationRolloutPercent            int           `json:"operationRolloutPercent" mapstructure:"operationRolloutPercent"`
	OperationRolloutStickyHeader       string        `json:"operationRolloutStickyHeader" mapstructure:"operationRolloutStickyHeader"`
	OperationQueueKinds                []string      `json:"operationQueueKinds" mapstructure:"operationQueueKinds"`
	OperationQueueUserAllowlist        []string      `json:"operationQueueUserAllowlist" mapstructure:"operationQueueUserAllowlist"`
	OperationQueueUserBlocklist        []string      `json:"operationQueueUserBlocklist" mapstructure:"operationQueueUserBlocklist"`
	OperationRolloutPreferSubjectKinds []string      `json:"operationRolloutPreferSubjectKinds" mapstructure:"operationRolloutPreferSubjectKinds"`
	OperationRolloutPreferSubjectUsers []string      `json:"operationRolloutPreferSubjectUsers" mapstructure:"operationRolloutPreferSubjectUsers"`
}

// NewServerRunOptions 初始化并返回服务器运行的默认配置选项
func NewServerRunOptions() *ServerRunOptions {
	return &ServerRunOptions{
		// Mode 控制 Gin 运行模式，可选值 gin.DebugMode/gin.ReleaseMode/gin.TestMode。
		// 被 internal/pkg/server/genericapiserver.go 中的 configureGin() 读取，影响路由日志输出与 pprof 注册策略。
		Mode: gin.ReleaseMode,
		// Healthz 决定是否暴露 /healthz 健康检查入口，installSystemRoutes() 会按该开关注册路由。
		Healthz: true,
		// Middlewares 为可选中间件键名列表，middleware.InstallMiddlewares() -> common.GetMiddlewareStack() 会据此构建顺序。
		Middlewares: []string{},
		// EnableProfiling 控制是否在 debug 模式下注册 pprof，installSystemRoutes() 联动 Mode 开关。
		EnableProfiling: true,
		// EnableMetrics 控制是否安装 Prometheus handler，installSystemRoutes() 新建 ginprometheus 实例时使用。
		EnableMetrics: true,
		// FastDebugStartup 仅在 debug 模式生效，genericapiserver.go 的 fastDebugStartupEnabled() 会依据它跳过 Kafka/MySQL 等耗时检查。
		FastDebugStartup: false,
		// EnableContactWarmup 决定是否执行联系人唯一性缓存预热，user_service.ensureContactCacheReady() 按此开关拉起后台任务。
		EnableContactWarmup: true,
		// EnableUserTraceLogging 控制是否安装用户链路日志中间件，router.installSystemRoutes() 创建 UserTraceLoggingMiddleware 时读取。
		EnableUserTraceLogging: true,
		// UserTraceLogSampleRate 为用户链路日志采样率，合法范围 [0,1]，传入 UserTraceLoggingMiddleware 配置。
		UserTraceLogSampleRate: 1,
		// UserTraceForceLogErrors 指定链路日志在错误场景下是否强制落盘，common.UserTraceLoggingMiddleware 使用。
		UserTraceForceLogErrors: true,
		// UserTraceDisableLogging 完全关闭链路日志输出（仍可配合 ForceLog），同样由 UserTraceLoggingMiddleware 读取。
		UserTraceDisableLogging: false,
		// ContactLookupTimeout 为联系人唯一性校验的外部存储查询超时，user_service.contactLookupTimeout() 消费。
		ContactLookupTimeout: 2 * time.Second,
		// ContactRefreshTimeout 控制联系人负缓存刷新时的存储请求超时时间，user_service.contactRefreshTimeout() 调用。
		ContactRefreshTimeout: 3 * time.Second,
		// ContactPreflightMaxConcurrency 限制预检并发度，user_service.newContactPreflightLimiter() 使用，需为正整数。
		ContactPreflightMaxConcurrency: 64,
		// ContactDegradeCacheTTL 控制联系人唯一性降级时的本地缓存存活时间。
		ContactDegradeCacheTTL: 20 * time.Second,
		// ContactDegradeHealthCheckInterval 定义降级期间 Redis 健康巡检间隔。
		ContactDegradeHealthCheckInterval: 10 * time.Second,
		// ContactDegradeCacheMaxEntries 为降级模式下本地缓存的最大条目数，用于避免占用过多内存。
		ContactDegradeCacheMaxEntries: 5000,
		// CookieDomain 设置登录 Cookie 绑定域，server/auth.go 在写入 token 时读取，可为空或形如 .example.com。
		CookieDomain: "",
		// CookieSecure 决定 Cookie 是否仅在 HTTPS 传输，server/auth.go 设置 Set-Cookie 时使用。
		CookieSecure: false,
		// CtxTimeout 为用户控制器内派生上下文的超时时间（见 create_control.go 等），建议 >=30s 防止长链路崩溃。
		CtxTimeout: 60 * time.Second,
		// Env 标识当前环境，用于选择中间件组合（middleware/common/common.go）及日志输出标签。
		Env: "development",
		// LoginRateLimit 为登录接口速率阈值，LoginRateLimiterWithProvider() 读取，单位请求/窗口。
		LoginRateLimit: 500000,
		// LoginWindow 对应登录限流时间窗口，常与 LoginRateLimit 搭配，需 >0。
		LoginWindow: 2 * time.Minute,
		// MaxLoginFailures 定义失败次数阈值，server/auth.go 的防爆破逻辑会基于该值锁定账号。
		MaxLoginFailures: 5,
		// LoginFailReset 控制失败计数重置时间，auth 登录保护读取，需 >0。
		LoginFailReset: 15 * time.Minute,
		// LoginFastFailThreshold 达到后快速降级登录请求，auth.loginQuickFail() 调用，0 表示关闭。
		LoginFastFailThreshold: 0,
		// LoginFastFailMessage 为触发快速降级时返回的提示语，auth.loginQuickFail() 使用。
		LoginFastFailMessage: "系统繁忙，请稍后再试",
		// LoginUpdateBuffer 控制后台写入登录时间的缓冲区大小，genericapiserver.go 的 loginUpdater 构建时使用。
		LoginUpdateBuffer: 1024,
		// LoginUpdateBatchSize 为登录时间批量刷新的单次批量上限，同 loginUpdater。
		LoginUpdateBatchSize: 64,
		// LoginUpdateFlushInterval 表示登录时间刷新间隔，loginUpdater 用于定时落库。
		LoginUpdateFlushInterval: 200 * time.Millisecond,
		// LoginUpdateTimeout 为批量更新数据库的最大等待时长，loginUpdater -> server/auth.go 使用。
		LoginUpdateTimeout: 2 * time.Second,
		// LoginCredentialCacheTTL 设定登录凭证本地缓存过期时间，genericapiserver.go 安装 credentialCache 时读取，需 >0。
		LoginCredentialCacheTTL: 30 * time.Second,
		// LoginCredentialCacheSize 为凭证缓存最大条目数，同上构造器使用。
		LoginCredentialCacheSize: 1024,
		// WriteRateLimit 是写接口限流阈值，router.installApiRoutes() 创建 WriteRateLimiter 时使用。
		WriteRateLimit: 500000,
		// AdminToken 为管理端 API 的共享令牌，server/audit_admin.go 及 server/ratelimit_admin.go 在校验 Header 时读取。
		AdminToken: "",
		// EnableRateLimiter 控制 Kafka 生产端限流器是否启用，genericapiserver.go 中的 producer 初始化会按该值配置。
		EnableRateLimiter: false,
		// MaxGoroutines 用于限制后台任务协程数，目前尚未在运行期引用，预留给负载调度扩展。
		MaxGoroutines: 100,
		// MaxQueueSize 控制任务排队长度，当前未消费，保留给后续通用工作池实现。
		MaxQueueSize: 100,
		// TimeoutThreshold 计划作为整体请求超时熔断阈值，尚未接入具体逻辑，建议保持大于 CtxTimeout。
		TimeoutThreshold: 100 * time.Second,
		// ProducerFallbackDir 为 Kafka 生产失败时的降级落盘目录，genericapiserver.go 初始化 NewUserProducer() 时使用。
		ProducerFallbackDir: "/var/log/iam/producer",
		// PasswordHashCost 为 bcrypt 成本因子，仅在选择 bcrypt 算法时生效，合法范围 [bcrypt.MinCost, bcrypt.MaxCost]，HashConfig() 转发。
		PasswordHashCost: 6,
		// PasswordHashAlgorithm 选择密码哈希算法（bcrypt 或 argon2id），HashConfig() 以及用户接口的密码校验逻辑使用。
		PasswordHashAlgorithm: auth.AlgorithmArgon2id,
		// Argon2Time 为 Argon2 迭代次数，对应 HashConfig()，常取 >=1。
		Argon2Time: 1,
		// Argon2MemoryKB 指定 Argon2 内存参数，HashConfig() 使用，单位 KB，需 >=1024。
		Argon2MemoryKB: 8 * 1024,
		// Argon2Parallelism 控制 Argon2 并行度，HashConfig() 中压缩为 uint8，建议 >=1。
		Argon2Parallelism: 1,
		// Argon2KeyLength 为 Argon2 输出长度（字节），HashConfig() 消费，通常 32。
		Argon2KeyLength: 32,
		// Argon2SaltLength 为 Argon2 盐长度（字节），HashConfig() 使用，须 >=16 以保证随机度。
		Argon2SaltLength: 16,
		// UserPendingCreateTTL 控制 Redis 用户创建幂等标记的TTL，user_service.markPendingCreate() 读取，必须 >=MinUserPendingCreateTTL。
		UserPendingCreateTTL:               MinUserPendingCreateTTL,
		OperationMode:                      operationModeSync, //	 默认同步模式
		OperationRolloutPercent:            0,                 // 默认全部走同步模式
		OperationQueueKinds:                append([]string{}, defaultOperationQueueKinds...),
		OperationQueueUserAllowlist:        nil,
		OperationQueueUserBlocklist:        nil,
		OperationRolloutPreferSubjectKinds: nil,
		OperationRolloutPreferSubjectUsers: nil,
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

	s.OperationMode = strings.TrimSpace(strings.ToLower(s.OperationMode))
	if s.OperationMode == "" {
		s.OperationMode = operationModeSync
	}
	if s.OperationMode != operationModeSync && s.OperationMode != operationModeQueue && s.OperationMode != operationModeRollout {
		s.OperationMode = operationModeQueue
	}
	if s.OperationRolloutPercent < 0 {
		s.OperationRolloutPercent = 0
	} else if s.OperationRolloutPercent > 100 {
		s.OperationRolloutPercent = 100
	}
	s.OperationRolloutStickyHeader = strings.ToLower(strings.TrimSpace(s.OperationRolloutStickyHeader))
	if len(s.OperationQueueKinds) == 0 {
		s.OperationQueueKinds = append([]string{}, defaultOperationQueueKinds...)
	} else {
		s.OperationQueueKinds = normalizeStringSlice(s.OperationQueueKinds)
		if len(s.OperationQueueKinds) == 0 {
			s.OperationQueueKinds = append([]string{}, defaultOperationQueueKinds...)
		}
	}
	s.OperationQueueUserAllowlist = normalizeStringSlice(s.OperationQueueUserAllowlist)
	s.OperationQueueUserBlocklist = normalizeStringSlice(s.OperationQueueUserBlocklist)
	s.OperationRolloutPreferSubjectKinds = normalizeStringSlice(s.OperationRolloutPreferSubjectKinds)
	s.OperationRolloutPreferSubjectUsers = normalizeStringSlice(s.OperationRolloutPreferSubjectUsers)

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
	if s.ContactDegradeCacheTTL <= 0 {
		s.ContactDegradeCacheTTL = 20 * time.Second
	}
	if s.ContactDegradeHealthCheckInterval <= 0 {
		s.ContactDegradeHealthCheckInterval = 10 * time.Second
	}
	if s.ContactDegradeCacheMaxEntries <= 0 {
		s.ContactDegradeCacheMaxEntries = 5000
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
		Algorithm:        s.PasswordHashAlgorithm, // 算法类型：bcrypt、argon2等
		BcryptCost:       s.PasswordHashCost,      // bcrypt成本因子
		Argon2Time:       s.Argon2Time,            //	 Argon2迭代次数
		Argon2MemoryKB:   s.Argon2MemoryKB,        //	 Argon2内存使用（KB）
		Argon2KeyLength:  s.Argon2KeyLength,       //	 Argon2输出长度（字节）
		Argon2SaltLength: s.Argon2SaltLength,      //	Argon2盐长度（字节）
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
	fs.DurationVar(&s.ContactDegradeCacheTTL, "server.contact-degrade-cache-ttl", s.ContactDegradeCacheTTL, "联系人唯一性降级模式下本地缓存的有效期")
	fs.DurationVar(&s.ContactDegradeHealthCheckInterval, "server.contact-degrade-health-check-interval", s.ContactDegradeHealthCheckInterval, "联系人唯一性降级期间 Redis 健康检查的轮询间隔")
	fs.IntVar(&s.ContactDegradeCacheMaxEntries, "server.contact-degrade-cache-max-entries", s.ContactDegradeCacheMaxEntries, "联系人唯一性降级模式下本地缓存的最大条目数")
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
	fs.StringVar(&s.OperationMode, "server.operation-mode", s.OperationMode, "用户异步操作模式：sync（完全同步）、queue（全部进入异步队列）、rollout（按百分比分流）")
	fs.IntVar(&s.OperationRolloutPercent, "server.operation-rollout-percent", s.OperationRolloutPercent, "当 operation-mode=rollout 时，进入异步队列的百分比 (0-100)")
	fs.StringVar(&s.OperationRolloutStickyHeader, "server.operation-rollout-header", s.OperationRolloutStickyHeader, "用于粘性灰度的请求头名，留空则以用户名进行哈希")
	fs.StringSliceVar(&s.OperationQueueKinds, "server.operation-queue-kinds", s.OperationQueueKinds, "进入异步队列的操作类型列表（默认: create,update,delete,batch）")
	fs.StringSliceVar(&s.OperationQueueUserAllowlist, "server.operation-queue-user-allowlist", s.OperationQueueUserAllowlist, "强制走异步队列的用户名白名单（小写匹配）")
	fs.StringSliceVar(&s.OperationQueueUserBlocklist, "server.operation-queue-user-blocklist", s.OperationQueueUserBlocklist, "强制走同步模式的用户名黑名单（小写匹配）")
	fs.StringSliceVar(&s.OperationRolloutPreferSubjectKinds, "server.operation-rollout-prefer-subject-kinds", s.OperationRolloutPreferSubjectKinds, "在灰度模式下优先使用 subject 作为粘性 key 的操作类型列表（小写匹配）")
	fs.StringSliceVar(&s.OperationRolloutPreferSubjectUsers, "server.operation-rollout-prefer-subject-users", s.OperationRolloutPreferSubjectUsers, "在灰度模式下优先使用 subject 作为粘性 key 的用户列表（小写匹配）")
}

func normalizeStringSlice(items []string) []string {
	if len(items) == 0 {
		return items
	}
	set := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, raw := range items {
		trimmed := strings.ToLower(strings.TrimSpace(raw))
		if trimmed == "" {
			continue
		}
		if _, exists := set[trimmed]; exists {
			continue
		}
		set[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
