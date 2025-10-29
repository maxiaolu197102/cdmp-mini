package options

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// ProtectionConfig 定义用户防护相关的阈值参数。
type ProtectionConfig struct {
	NegativeCacheThreshold int           `json:"negativeCacheThreshold" mapstructure:"negativeCacheThreshold"`
	NegativeCacheWindow    time.Duration `json:"negativeCacheWindow" mapstructure:"negativeCacheWindow"`
	NegativeCacheTTL       time.Duration `json:"negativeCacheTTL" mapstructure:"negativeCacheTTL"`
	BlockThreshold         int           `json:"blockThreshold" mapstructure:"blockThreshold"`
	BlockWindow            time.Duration `json:"blockWindow" mapstructure:"blockWindow"`
	BlockDuration          time.Duration `json:"blockDuration" mapstructure:"blockDuration"`
}

func defaultProtectionConfig() ProtectionConfig {
	return ProtectionConfig{
		NegativeCacheThreshold: 3,
		NegativeCacheWindow:    time.Minute,
		NegativeCacheTTL:       15 * time.Second,
		BlockThreshold:         10,
		BlockWindow:            5 * time.Minute,
		BlockDuration:          30 * time.Minute,
	}
}

// DefaultProtectionConfig 返回系统级默认防护配置。
func DefaultProtectionConfig() ProtectionConfig {
	return defaultProtectionConfig()
}

func (p *ProtectionConfig) applyDefaults() {
	defaults := defaultProtectionConfig()
	if p.NegativeCacheThreshold <= 0 {
		p.NegativeCacheThreshold = defaults.NegativeCacheThreshold
	}
	if p.NegativeCacheWindow <= 0 {
		p.NegativeCacheWindow = defaults.NegativeCacheWindow
	}
	if p.NegativeCacheTTL <= 0 {
		p.NegativeCacheTTL = defaults.NegativeCacheTTL
	}
	if p.BlockThreshold <= 0 {
		p.BlockThreshold = defaults.BlockThreshold
	}
	if p.BlockWindow <= 0 {
		p.BlockWindow = defaults.BlockWindow
	}
	if p.BlockDuration <= 0 {
		p.BlockDuration = defaults.BlockDuration
	}
}

func (p ProtectionConfig) validate() []error {
	var errs []error
	if p.NegativeCacheThreshold < 0 {
		errs = append(errs, fmt.Errorf("negativeCacheThreshold must be non-negative"))
	}
	if p.NegativeCacheWindow < 0 {
		errs = append(errs, fmt.Errorf("negativeCacheWindow must be non-negative"))
	}
	if p.NegativeCacheTTL < 0 {
		errs = append(errs, fmt.Errorf("negativeCacheTTL must be non-negative"))
	}
	if p.BlockThreshold < 0 {
		errs = append(errs, fmt.Errorf("blockThreshold must be non-negative"))
	}
	if p.BlockWindow < 0 {
		errs = append(errs, fmt.Errorf("blockWindow must be non-negative"))
	}
	if p.BlockDuration < 0 {
		errs = append(errs, fmt.Errorf("blockDuration must be non-negative"))
	}
	return errs
}

// AuditOptions 控制审计功能的开关与落地方式。
type AuditOptions struct {
	Enabled         bool             `json:"enabled" mapstructure:"enabled"`
	BufferSize      int              `json:"bufferSize" mapstructure:"bufferSize"`
	ShutdownTimeout time.Duration    `json:"shutdownTimeout" mapstructure:"shutdownTimeout"`
	LogFile         string           `json:"logFile" mapstructure:"logFile"`
	EnableMetrics   bool             `json:"enableMetrics" mapstructure:"enableMetrics"`
	RecentBuffer    int              `json:"recentBuffer" mapstructure:"recentBuffer"`
	Protection      ProtectionConfig `json:"protection" mapstructure:"protection"`
}

func NewAuditOptions() *AuditOptions {
	return &AuditOptions{
		Enabled:         true,
		BufferSize:      512,
		ShutdownTimeout: 5 * time.Second,
		LogFile:         "/var/log/iam/audit.log",
		EnableMetrics:   true,
		RecentBuffer:    256,
		Protection:      defaultProtectionConfig(),
	}
}

func (o *AuditOptions) Complete() {
	if o.BufferSize <= 0 {
		o.BufferSize = 512
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 5 * time.Second
	}
	if o.LogFile == "" {
		o.LogFile = "log/audit.log"
	}
	if o.RecentBuffer <= 0 {
		o.RecentBuffer = 256
	}
	o.Protection.applyDefaults()
}

func (o *AuditOptions) Validate() []error {
	return o.Protection.validate()
}

func (o *AuditOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Enabled, "audit.enabled", o.Enabled, "是否开启审计功能")
	fs.IntVar(&o.BufferSize, "audit.buffer-size", o.BufferSize, "审计事件缓冲队列大小")
	fs.DurationVar(&o.ShutdownTimeout, "audit.shutdown-timeout", o.ShutdownTimeout, "服务退出时等待审计队列耗尽的超时时间")
	fs.StringVar(&o.LogFile, "audit.log-file", o.LogFile, "审计文件落地路径(JSON Lines)")
	fs.BoolVar(&o.EnableMetrics, "audit.enable-metrics", o.EnableMetrics, "是否将审计事件写入指标监控")
	fs.IntVar(&o.RecentBuffer, "audit.recent-buffer", o.RecentBuffer, "Recent audit events cache size for debugging/diagnostics")
	fs.IntVar(&o.Protection.NegativeCacheThreshold, "audit.protection-negative-threshold", o.Protection.NegativeCacheThreshold, "负缓存写入阈值(命中次数)")
	fs.DurationVar(&o.Protection.NegativeCacheWindow, "audit.protection-negative-window", o.Protection.NegativeCacheWindow, "负缓存计数窗口时长")
	fs.DurationVar(&o.Protection.NegativeCacheTTL, "audit.protection-negative-ttl", o.Protection.NegativeCacheTTL, "负缓存TTL时长")
	fs.IntVar(&o.Protection.BlockThreshold, "audit.protection-block-threshold", o.Protection.BlockThreshold, "黑名单封禁触发次数阈值")
	fs.DurationVar(&o.Protection.BlockWindow, "audit.protection-block-window", o.Protection.BlockWindow, "黑名单计数窗口时长")
	fs.DurationVar(&o.Protection.BlockDuration, "audit.protection-block-duration", o.Protection.BlockDuration, "黑名单封禁时长")
}
