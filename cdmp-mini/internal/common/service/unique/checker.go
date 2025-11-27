package unique

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	degradeReasonCache       = "redis_cache_error"
	degradeReasonPlaceholder = "redis_placeholder_error"
)

// CacheClient 定义唯一性检查所需的缓存客户端能力。
//
// param ctx: 请求上下文，用于控制超时与取消，允许为nil但实现需自处理。
// note: 实现必须是并发安全的，并且需要自行处理键名前缀等细节。
type CacheClient interface {
	GetKey(ctx context.Context, key string) (string, error)
	SetKey(ctx context.Context, key string, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error)
	DeleteKey(ctx context.Context, key string) (bool, error)
}

// LoggerHooks 封装唯一性检查过程中的日志回调。
//
// note: Warn 与 Error 均为可选，未提供时将静默忽略对应日志。
type LoggerHooks struct {
	Warn  func(msg string, kv ...interface{})
	Error func(msg string, kv ...interface{})
}

// CheckerConfig 描述构建唯一性检查器所需的外部依赖。
//
// param Store: 执行查重的存储实现，通常为具体的 DAO 或仓储实例，必填。
// param Cache: 可选的缓存客户端，用于占位与命中加速；若为空则退化为纯数据库校验。
// param PlaceholderTTL: 占位键的存活时间，需结合业务热点评估。
// param CacheTTL: 冲突命中后写入缓存的过期时间，建议设置为分钟级以上。
// param PlaceholderFallback:
/*
当 AllowedOwner 与 PlaceholderValue 为空时使用的默认占位符。
只会在「拥有者」(AllowedOwner) 和实际占位值 (PlaceholderValue) 都是空字符串时才派上用场。
在正常的联系人校验里我们会把 AllowedOwner 设成当前用户的用户名，PlaceholderValue 也默认沿用它，所以大多数场景不会触发。出现两者都空的情况主要有两类：
上层调用本身不知道“谁”应该持有这个字段，比如只想检查一个邮箱是否被任何人占用，而没有传入候选用户名；
业务方想手动覆盖占位符且设置成空（PlaceholderValue 仍留空），同时没有提供 AllowedOwner。
为了避免把“空字符串”写进 Redis，占位逻辑会退回到 PlaceholderFallback（默认是像 RATE_LIMIT_PREVENTION 这样的哨兵值），保证缓存里始终有一个明确的占位标记。
*/
// param CacheReady: 返回缓存是否已预热完成，控制是否跳过数据库兜底。
// param DegradeActive: 判断当前请求是否已经处于降级模式。
// param EnsurePlaceholder: 当需要写入占位符时的回调，通常用于降级兜底。
// param MarkDegraded: 标记当前请求进入降级模式，需幂等处理。
// param Retry: 控制查库重试策略，通常传入 util.RetryWithBackoff。
// param RetryPredicate: 判断错误是否可重试的函数，可为空。
// param MaxRetries: 查库重试次数，小于等于0时视为1。
// param ShouldDegrade: 判断错误是否触发降级逻辑。
// param ShouldReleasePlaceholder: 判断返回错误时是否需要释放占位符。
// param NewLookupContext: 构造带超时控制的上下文，为空时回退到 context.WithTimeout。
// param LookupTimeout: 单次查库的超时时间，小于等于0表示不限。
// param IsNotFound: 判断错误是否为“未找到”，用于终止重试。
// param IsCacheMiss: 判断缓存访问是否未命中，未提供时默认所有错误均视为异常。
// param RecordStep: 记录性能指标的回调，用于链路观测。
// param Logger: 日志钩子，用于输出缓存或占位异常。
type CheckerConfig[S any, E any] struct {
	Store                    S
	Cache                    CacheClient
	PlaceholderTTL           time.Duration
	CacheTTL                 time.Duration
	PlaceholderFallback      string
	CacheReady               func() bool
	DegradeActive            func(context.Context) bool
	EnsurePlaceholder        func(context.Context, string, string)
	MarkDegraded             func(context.Context, string, ...interface{})
	Retry                    func(int, func(error) bool, func() (interface{}, error)) (interface{}, error)
	RetryPredicate           func(error) bool
	MaxRetries               int
	ShouldDegrade            func(error) bool
	ShouldReleasePlaceholder func(error) bool
	NewLookupContext         func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	LookupTimeout            time.Duration
	IsNotFound               func(error) bool
	IsCacheMiss              func(error) bool
	RecordStep               func(ctx context.Context, step string, field string, owner string, duration time.Duration, err error)
	Logger                   LoggerHooks
}

// FieldConfig 定义单个唯一性字段的校验细节。
//
// param FieldLabel: 用于错误提示的字段中文名，必填。
// param FieldKey: 性能采集和日志的字段标识，建议使用英文蛇形命名。
// param FieldValue: 待校验的字段值，建议在调用前先归一化。
// param AllowedOwner: 允许占用当前字段的实体标识（例如用户名）。
// param CacheKey: 缓存键，若为空则跳过缓存流程直接返回。
// param PlaceholderValue: 自定义占位符，为空时依次回退到 AllowedOwner 与 PlaceholderFallback。
// param Lookup: 查库函数，需返回持有该字段的实体，支持对 Store 的多态实现。
// param ExtractOwner: 从实体中提取唯一标识的回调，返回空字符串视为无冲突。
// param ConflictError: 构造冲突错误的回调，由业务方决定错误码与提示。
// param IsAllowedOwner: 判断缓存命中是否可放行，未提供时默认大小写不敏感比对。
// param SkipPlaceholderLookup: 当占位成功且缓存已预热时，决定是否跳过数据库查重。
// param DegradeReason: 触发降级时上报的原因标签，可为空。
// param DegradeKV: 触发降级时额外记录的键值对，需偶数字段。
// param StepName: 性能采集使用的步骤名，默认 ensure_field_unique。
type FieldConfig[S any, E any] struct {
	FieldLabel            string
	FieldKey              string
	FieldValue            string
	AllowedOwner          string
	CacheKey              string
	PlaceholderValue      string
	Lookup                func(context.Context, S, string) (E, error)
	ExtractOwner          func(E) string
	ConflictError         func(fieldLabel, fieldValue string) error
	IsAllowedOwner        func(existingOwner, allowedOwner string) bool
	SkipPlaceholderLookup func(string) bool
	DegradeReason         string
	DegradeKV             []interface{}
	StepName              string
}

// Checker 提供可复用的字段唯一性校验逻辑。
//
// note: Checker 本身无状态，可在多 goroutine 中并发调用 EnsureFieldUnique。
type Checker[S any, E any] struct {
	cfg CheckerConfig[S, E]
}

// NewChecker 构建一个通用唯一性检查器实例，聚合缓存、占位、查库与降级策略。
//
// param cfg: 外部依赖配置，需至少提供 Store，其余回调按需注入：
//   - Cache: 可选缓存客户端，未提供时退化为纯数据库校验。
//   - PlaceholderTTL/CacheTTL: 缓存占位与命中写入的有效期。
//   - CacheReady/DegradeActive: 控制降级模式与预热状态。
//   - EnsurePlaceholder/MarkDegraded: 发生降级时写占位并记录原因。
//   - Retry/RetryPredicate/MaxRetries: 查库重试策略。
//   - ShouldDegrade/ShouldReleasePlaceholder: 失败后的治理逻辑。
//   - NewLookupContext/LookupTimeout: 自定义查库上下文与超时。
//   - IsNotFound/IsCacheMiss: 定义缓存/查库的未命中判断。
//   - RecordStep/Logger: 性能指标与日志钩子。
//
// returns: 初始化后的 Checker 指针；若 cfg.MaxRetries<=0 会自动回退为 1 次重试。
func NewChecker[S any, E any](cfg CheckerConfig[S, E]) *Checker[S, E] {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 1
	}
	if cfg.ShouldReleasePlaceholder == nil {
		cfg.ShouldReleasePlaceholder = func(error) bool { return true }
	}
	return &Checker[S, E]{cfg: cfg}
}

// EnsureFieldUnique 按字段级别执行唯一性校验逻辑。
// 该方法组合缓存占位、数据库回查与降级兜底，从而在多模型之间复用唯一性控制流程。
//
// param ctx: 请求上下文，需携带取消与超时控制，可为nil但不建议。
// param fieldCfg: 字段配置项，描述缓存键、查库函数等依赖，必填。
//
// returns: 唯一性通过时返回nil；冲突时返回 ConflictError 构造的错误；底层依赖失败时返回原始错误或降级后的nil。
//
// note: Checker 调用前需保证 Store、ConflictError 与 ExtractOwner 已就绪，否则行为未定义。
func (c *Checker[S, E]) EnsureFieldUnique(ctx context.Context, fieldCfg FieldConfig[S, E]) (err error) {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(fieldCfg.CacheKey) == "" {
		return nil
	}

	cache := c.cfg.Cache
	/*
		可能放行的情况 缓存占位符中记录的 “字段所有者”
		= 允许占用该字段的实体标识（例如用户名）
		1.update 操作（同一用户修改自身字段）
		2.create 操作（同一用户重复提交 / 幂等请求）
		3.delete 后重新 create（同一用户回收字段后复用）

	*/
	isAllowed := fieldCfg.IsAllowedOwner //判断缓存命中是否可放行，未提供时默认大小写不敏感比对。
	if isAllowed == nil {
		isAllowed = func(existingOwner, allowedOwner string) bool {
			return strings.EqualFold(existingOwner, allowedOwner)
		}
	}
	//「真实用户占位成功 + 缓存已预热 → 跳过查(true)；降级兜底占位 / 占位异常 → 强制查库」(返回false)
	skipPlaceholderLookup := fieldCfg.SkipPlaceholderLookup
	if skipPlaceholderLookup == nil {
		skipPlaceholderLookup = func(string) bool { return false }
	}

	placeholderValue := fieldCfg.PlaceholderValue
	if strings.TrimSpace(placeholderValue) == "" {
		placeholderValue = fieldCfg.AllowedOwner
	}
	if strings.TrimSpace(placeholderValue) == "" {
		placeholderValue = c.cfg.PlaceholderFallback
	}

	var placeholderAcquired bool
	start := time.Now()
	defer func() {
		if c.cfg.RecordStep != nil {
			stepName := fieldCfg.StepName
			if strings.TrimSpace(stepName) == "" {
				stepName = "ensure_field_unique"
			}
			c.cfg.RecordStep(ctx, stepName, fieldCfg.FieldKey, fieldCfg.AllowedOwner, time.Since(start), err)
		}
		if placeholderAcquired && err != nil && cache != nil && c.cfg.ShouldReleasePlaceholder(err) {
			if _, releaseErr := cache.DeleteKey(ctx, fieldCfg.CacheKey); releaseErr != nil && c.cfg.Logger.Warn != nil {
				c.cfg.Logger.Warn("唯一性占位释放失败", "cacheKey", fieldCfg.CacheKey, "field", fieldCfg.FieldKey, "error", releaseErr)
			}
		}
	}()

	/*
		create 链路中降级开关的 “被动触发” 特性
			现有逻辑中，降级开关（DegradeActive）确实不会主动开启，而是依赖下游异常 “被动激活”：
			初始状态：createState 挂载到 ctx 时，degraded 标记默认为 false，所有校验走正常流程（查缓存→占位→查库）。
			触发条件：仅当关键步骤（如预检、查库、Redis 操作）出现可降级错误（超时、上下文取消、依赖不可用等），才会调用 markCreateDegraded 将 degraded 置为 true。
			后续影响：一旦标记生效，unique.Checker 会跳过数据库兜底校验，仅靠缓存占位快速放行，减少对故障依赖的访问，属于 “故障后的自我保护”
	*/
	// 判断是否已处于降级模式
	if c.cfg.DegradeActive != nil && c.cfg.DegradeActive(ctx) {
		//若缓存客户端和占位符写入回调存在 → 执行占位符写入/兜底
		if cache != nil && c.cfg.EnsurePlaceholder != nil {
			c.cfg.EnsurePlaceholder(ctx, fieldCfg.CacheKey, placeholderValue)
		}
		return nil
	}

	if cache == nil {
		entity, lookupErr := c.runLookup(ctx, fieldCfg)
		if lookupErr != nil {
			return lookupErr
		}
		owner := ""
		if fieldCfg.ExtractOwner != nil {
			owner = fieldCfg.ExtractOwner(entity)
		}
		if owner == "" || isAllowed(owner, fieldCfg.AllowedOwner) {
			return nil
		}
		if fieldCfg.ConflictError != nil {
			return fieldCfg.ConflictError(fieldCfg.FieldLabel, fieldCfg.FieldValue)
		}
		return fmt.Errorf("field %s is already taken", fieldCfg.FieldKey)
	}

	cachedOwner, cacheErr := cache.GetKey(ctx, fieldCfg.CacheKey)
	if cacheErr != nil {
		if c.cfg.IsCacheMiss == nil || !c.cfg.IsCacheMiss(cacheErr) {
			if c.cfg.MarkDegraded != nil {
				c.cfg.MarkDegraded(ctx, degradeReasonCache, "field", fieldCfg.FieldKey, "error", cacheErr.Error())
			}
			if c.cfg.Logger.Warn != nil {
				c.cfg.Logger.Warn("唯一性缓存读取失败", "cacheKey", fieldCfg.CacheKey, "field", fieldCfg.FieldKey, "error", cacheErr)
			}
		}
	} else if strings.TrimSpace(cachedOwner) != "" {
		if isAllowed(cachedOwner, fieldCfg.AllowedOwner) {
			return nil
		}
		if fieldCfg.ConflictError != nil {
			return fieldCfg.ConflictError(fieldCfg.FieldLabel, fieldCfg.FieldValue)
		}
		return fmt.Errorf("field %s is already taken", fieldCfg.FieldKey)
	}

	if cacheErr == nil && strings.TrimSpace(cachedOwner) != "" {
		return nil
	}

	if cacheErr != nil && c.cfg.IsCacheMiss != nil && !c.cfg.IsCacheMiss(cacheErr) {
		cachedOwner = ""
	}

	if strings.TrimSpace(cachedOwner) == "" {
		ok, setErr := cache.SetNX(ctx, fieldCfg.CacheKey, placeholderValue, c.cfg.PlaceholderTTL)
		if setErr != nil {
			if c.cfg.MarkDegraded != nil {
				c.cfg.MarkDegraded(ctx, degradeReasonPlaceholder, "field", fieldCfg.FieldKey, "error", setErr.Error())
			}
			if c.cfg.Logger.Warn != nil {
				c.cfg.Logger.Warn("唯一性占位失败", "cacheKey", fieldCfg.CacheKey, "field", fieldCfg.FieldKey, "error", setErr)
			}
		} else if ok {
			placeholderAcquired = true
			cachedOwner = placeholderValue
			if c.cfg.CacheReady != nil && c.cfg.CacheReady() && skipPlaceholderLookup(placeholderValue) {
				return nil
			}
		} else {
			refreshed, refreshErr := cache.GetKey(ctx, fieldCfg.CacheKey)
			if refreshErr == nil {
				cachedOwner = refreshed
			} else if c.cfg.IsCacheMiss == nil || !c.cfg.IsCacheMiss(refreshErr) {
				if c.cfg.Logger.Warn != nil {
					c.cfg.Logger.Warn("唯一性占位刷新失败", "cacheKey", fieldCfg.CacheKey, "field", fieldCfg.FieldKey, "error", refreshErr)
				}
			}
		}
		if strings.TrimSpace(cachedOwner) != "" {
			if !isAllowed(cachedOwner, fieldCfg.AllowedOwner) {
				if fieldCfg.ConflictError != nil {
					return fieldCfg.ConflictError(fieldCfg.FieldLabel, fieldCfg.FieldValue)
				}
				return fmt.Errorf("field %s is already taken", fieldCfg.FieldKey)
			}
			if !placeholderAcquired {
				return nil
			}
		}
	}

	entity, lookupErr := c.runLookup(ctx, fieldCfg)
	if lookupErr != nil {
		if c.cfg.ShouldDegrade != nil && c.cfg.ShouldDegrade(lookupErr) {
			if c.cfg.MarkDegraded != nil {
				c.cfg.MarkDegraded(ctx, fieldCfg.DegradeReason, fieldCfg.DegradeKV...)
			}
			if c.cfg.EnsurePlaceholder != nil {
				c.cfg.EnsurePlaceholder(ctx, fieldCfg.CacheKey, placeholderValue)
			}
			return nil
		}
		return lookupErr
	}

	owner := ""
	if fieldCfg.ExtractOwner != nil {
		owner = fieldCfg.ExtractOwner(entity)
	}
	if owner == "" || isAllowed(owner, fieldCfg.AllowedOwner) {
		return nil
	}
	if err := cache.SetKey(ctx, fieldCfg.CacheKey, owner, c.cfg.CacheTTL); err != nil {
		if c.cfg.MarkDegraded != nil {
			c.cfg.MarkDegraded(ctx, degradeReasonCache, "field", fieldCfg.FieldKey, "error", err.Error())
		}
		if c.cfg.Logger.Warn != nil {
			c.cfg.Logger.Warn("唯一性缓存写入失败", "cacheKey", fieldCfg.CacheKey, "field", fieldCfg.FieldKey, "error", err)
		}
	}
	if fieldCfg.ConflictError != nil {
		return fieldCfg.ConflictError(fieldCfg.FieldLabel, fieldCfg.FieldValue)
	}
	return fmt.Errorf("field %s is already taken", fieldCfg.FieldKey)
}

func (c *Checker[S, E]) runLookup(ctx context.Context, fieldCfg FieldConfig[S, E]) (E, error) {
	var zero E
	if fieldCfg.Lookup == nil {
		return zero, nil
	}

	execute := func() (interface{}, error) {
		lookupCtx := ctx
		var cancel context.CancelFunc
		if c.cfg.NewLookupContext != nil {
			lookupCtx, cancel = c.cfg.NewLookupContext(ctx, c.cfg.LookupTimeout)
		} else if c.cfg.LookupTimeout > 0 {
			lookupCtx, cancel = context.WithTimeout(ctx, c.cfg.LookupTimeout)
		}
		if cancel != nil {
			defer cancel()
		}
		start := time.Now()
		entity, err := fieldCfg.Lookup(lookupCtx, c.cfg.Store, fieldCfg.FieldValue)
		if c.cfg.RecordStep != nil {
			c.cfg.RecordStep(ctx, "ensure_field_lookup", fieldCfg.FieldKey, fieldCfg.AllowedOwner, time.Since(start), err)
		}
		if err != nil {
			if c.cfg.IsNotFound != nil && c.cfg.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		return entity, nil
	}

	var (
		result interface{}
		err    error
	)
	if c.cfg.Retry != nil {
		result, err = c.cfg.Retry(c.cfg.MaxRetries, c.cfg.RetryPredicate, execute)
	} else {
		result, err = execute()
	}
	if err != nil {
		return zero, err
	}
	if result == nil {
		return zero, nil
	}
	entity, ok := result.(E)
	if !ok {
		return zero, fmt.Errorf("unique: lookup result类型不匹配")
	}
	return entity, nil
}
