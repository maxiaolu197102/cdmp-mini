package user

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"

	operationpkg "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	serveropts "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
)

// OperationMode 表示用户 CRUD 流程使用的执行模式。
type OperationMode string

const (
	OperationModeSync    OperationMode = "sync"
	OperationModeQueue   OperationMode = "queue"
	OperationModeRollout OperationMode = "rollout"
)

var defaultOperationQueueKindNames = []string{
	string(operationpkg.OperationCreate),
	string(operationpkg.OperationUpdate),
	string(operationpkg.OperationDelete),
	string(operationpkg.OperationBatch),
}

// OperationModeConfig 对外暴露的运行时配置快照。
//
// 该配置通常由管理接口或配置中心下发，用于控制用户 CRUD
// 在「同步直写 / 全量排队 / 灰度排队」三种模式之间如何选择。
type OperationModeConfig struct {
	// Mode 表示当前的全局操作模式。
	//  - "sync"   ：所有命中 QueueKinds 且未命中黑白名单/灰度的请求都走同步直写管道；
	//  - "queue"  ：所有命中 QueueKinds 且未命中黑白名单的请求都走队列（异步）；
	//  - "rollout"：开启灰度模式，由 RolloutPercent + StickyHeader/subject 决定
	//               每个请求最终走同步还是队列。
	//
	// 取值范围："sync" / "queue" / "rollout"；大小写不敏感，非法值会回退为 "queue"。
	Mode OperationMode `json:"mode"`
	// RolloutPercent 在灰度模式 (Mode="rollout") 下生效，控制「路由到队列的百分比」。
	//  - 0   ：全部走同步（即使 Mode=rollout，本质等价于 sync）；
	//  - 1-99：约有 RolloutPercent% 的请求走队列，其余走同步；
	//  - 100：全部走队列（等价于 queue）。
	//
	// 取值范围：0-100；小于 0 会被归一化为 0，大于 100 会被归一化为 100。
	RolloutPercent int `json:"rolloutPercent"`

	// StickyHeader 用作灰度采样的首选粘性 Key 的 HTTP 头名称。
	//  - 为空：不使用 Header 做粘性采样，优先 subject，其次内部自增序列；
	//  - 非空：在灰度模式下优先尝试从该 Header 提取值作为采样 key，
	//          若 PreferSubject* 命中且 subject 可用，则 subject 的优先级高于 Header。
	//
	// 取值形式：HTTP 头名称的小写字符串（如 "x-rollout-key"），空串表示关闭。
	StickyHeader string `json:"stickyHeader,omitempty"`

	// QueueKinds 定义允许进入异步队列的「操作类型」白名单。
	//  - 为空：会自动回填为 [create, update, delete, batch] 四种操作；
	//  - 非空：仅列表中的操作种类会参与同步/队列/灰度决策，其余操作强制走同步。
	//
	// 典型取值：
	//  - ["create", "update"]：仅创建/更新走异步或灰度，删除等操作始终同步；
	//  - ["create", "update", "delete", "batch"]：所有写操作都纳入队列决策。
	QueueKinds []string `json:"queueKinds,omitempty"`

	// AllowUsers 为「用户白名单」，按用户名粒度强制队列，优先级仅次于 BlockUsers。
	//  - 命中白名单的 subject：无论全局 Mode/RolloutPercent 为何，都直接走队列；
	//  - 未命中：继续后续 Sync/Queue/Rollout 决策。
	//
	// 取值形式：用户名的小写列表，支持任意业务自定义的 userName；为空表示不启用白名单。
	AllowUsers []string `json:"allowUsers,omitempty"`

	// BlockUsers 为「用户黑名单」，按用户名粒度强制同步，优先级最高。
	//  - 命中黑名单的 subject：直接走同步模式，不参与队列/灰度采样；
	//  - 未命中：继续判断 AllowUsers、Mode 等配置。
	//
	// 取值形式：用户名的小写列表，通常用于风险账号/压测账号等；为空表示不启用黑名单。
	BlockUsers []string `json:"blockUsers,omitempty"`

	// PreferSubjectKinds 指定哪些操作类型在灰度模式下「优先使用 subject 作为采样 Key」。
	//  - 命中操作类型且 subject 非空：subject 将覆盖 StickyHeader 作为采样 key；
	//  - 未命中：按 StickyHeader → subject → 自增序列 的顺序选择采样 key。
	//
	// 取值形式：操作种类字符串列表（如 ["create", "update"]），为空则不做种类优先。
	PreferSubjectKinds []string `json:"preferSubjectKinds,omitempty"`

	// PreferSubjectUsers 指定哪些用户在灰度模式下「强制使用 subject 作为采样 Key」。
	//  - 命中用户名：即使 StickyHeader 存在，也优先用 subject 做哈希采样；
	//  - 未命中：遵循 StickyHeader → subject → 自增序列 的默认顺序。
	//
	// 取值形式：用户名的小写列表；可与 PreferSubjectKinds 组合使用实现更细粒度的灰度粘性控制。
	PreferSubjectUsers []string `json:"preferSubjectUsers,omitempty"`
}

// operationModeState 内部使用的运行时状态快照。
type operationModeState struct {
	config               OperationModeConfig
	mode                 OperationMode
	rolloutPercent       int
	stickyHeader         string
	queueKindSet         map[operationpkg.OperationKind]struct{}
	allowUserSet         map[string]struct{}
	blockUserSet         map[string]struct{}
	preferSubjectKindSet map[operationpkg.OperationKind]struct{}
	preferSubjectUserSet map[string]struct{}
}

// OperationModeDecision 表示单次决策的结果及关键命中信息，主要用于埋点和观测。
type OperationModeDecision struct {
	// Mode 本次请求最终选中的执行模式（sync/queue/rollout 展开后的实际模式）。
	//  - OperationModeSync  ：本次按同步直写执行；
	//  - OperationModeQueue ：本次进入异步队列；
	//  - OperationModeRollout 不会直接出现在这里，rollout 会被展开为 sync 或 queue。
	Mode OperationMode
	// QueueKindsHit 标记是否命中了 QueueKinds 白名单。
	//  - true  ：kind 在 QueueKinds 中，本次请求允许走队列/灰度；
	//  - false ：kind 不在 QueueKinds 中，本次被强制走同步（后续不再考虑队列/灰度）。
	QueueKindsHit bool
	// SubjectBlocked 标记是否命中了 BlockUsers 黑名单。
	//  - true  ：subject 在 BlockUsers 中，本次被直接切到同步模式；
	//  - false ：未命中黑名单。
	SubjectBlocked bool
	// SubjectAllowed 标记是否命中了 AllowUsers 白名单。
	//  - true  ：subject 在 AllowUsers 中，本次被直接切到队列模式；
	//  - false ：未命中白名单，由全局 Mode/RolloutPercent 决定。
	SubjectAllowed bool
	// RolloutSample 标记在灰度模式下是否命中「进入队列」的采样。
	//  - true  ：命中采样，本次请求最终走队列；
	//  - false ：未命中采样，本次请求最终走同步；
	// 仅当 Mode=OperationModeRollout 时有意义，其它模式恒为 false。
	RolloutSample bool
}

type operationModeController struct {
	mu    sync.RWMutex
	seq   atomic.Uint64
	state operationModeState
}

func newOperationModeController(cfg OperationModeConfig) *operationModeController {
	state := sanitizeOperationModeConfig(cfg)
	return &operationModeController{state: state}
}

// Decide 根据当前配置和请求信息决定使用的操作模式。
func (c *operationModeController) Decide(ctx context.Context, kind operationpkg.OperationKind, subject string) OperationMode {
	decision := c.DecideDetailed(ctx, kind, subject)
	return decision.Mode
}

// DecideDetailed returns the mode and decision flags for tracing/metrics.
func (c *operationModeController) DecideDetailed(ctx context.Context, kind operationpkg.OperationKind, subject string) OperationModeDecision {
	decision := OperationModeDecision{Mode: OperationModeQueue}
	if c == nil {
		return decision
	}

	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()
	//白名单查询,如果未命中则走同步
	if len(state.queueKindSet) > 0 {
		//未命中种类则走同步
		if _, ok := state.queueKindSet[kind]; !ok {
			decision.Mode = OperationModeSync
			return decision
		}
		decision.QueueKindsHit = true
	}

	normalizedSubject := strings.ToLower(strings.TrimSpace(subject))
	if normalizedSubject != "" {
		//用户黑名单优先走同步
		if _, blocked := state.blockUserSet[normalizedSubject]; blocked {
			decision.Mode = OperationModeSync
			decision.SubjectBlocked = true
			return decision
		}
		//用户白名单优先走队列
		if _, allowed := state.allowUserSet[normalizedSubject]; allowed {
			decision.SubjectAllowed = true
			decision.Mode = OperationModeQueue
			return decision
		}
	}
	//判断当前模式
	switch state.mode {
	case OperationModeSync:
		decision.Mode = OperationModeSync
		return decision
	case OperationModeQueue:
		decision.Mode = OperationModeQueue
		return decision
	case OperationModeRollout:
		percent := state.rolloutPercent
		if percent <= 0 {
			decision.Mode = OperationModeSync
			return decision
		}
		if percent >= 100 {
			decision.Mode = OperationModeQueue
			decision.RolloutSample = true
			return decision
		}

		preferSubject := false
		/*
			只对“创建/更新 + 重点客户”用 subject：
			PreferSubjectKinds: ["create", "update"]
			PreferSubjectUsers: ["vip_user_001", "enterprise_acme"]
			全量写操作一律按 subject 灰度（方便从“按用户”视角看效果）：
			PreferSubjectKinds: ["create", "update", "delete", "batch"]
			PreferSubjectUsers: []（留空）
		*/
		if _, ok := state.preferSubjectKindSet[kind]; ok {
			preferSubject = true
		}
		if normalizedSubject != "" {
			if _, ok := state.preferSubjectUserSet[normalizedSubject]; ok {
				preferSubject = true
			}
		}

		key := normalizedSubject
		if state.stickyHeader != "" {
			if headerValue := rolloutHeaderFromContext(ctx, state.stickyHeader); headerValue != "" {
				if !(preferSubject && key != "") {
					key = headerValue
				}
			}
		}
		if key == "" {
			seq := c.seq.Add(1)
			key = fmt.Sprintf("rollout:%d", seq)
		}
		decision.RolloutSample = withinRolloutSample(key, percent)
		if decision.RolloutSample {
			decision.Mode = OperationModeQueue
			return decision
		}
		decision.Mode = OperationModeSync
		return decision
		//兜底走队列,防止配置错误
	default:
		decision.Mode = OperationModeQueue
		return decision
	}
}

func (c *operationModeController) Update(cfg OperationModeConfig) OperationModeConfig {
	state := sanitizeOperationModeConfig(cfg)
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
	return cloneOperationModeConfig(state.config)
}

func (c *operationModeController) Snapshot() OperationModeConfig {
	if c == nil {
		return cloneOperationModeConfig(defaultOperationModeConfig())
	}
	c.mu.RLock()
	snapshot := cloneOperationModeConfig(c.state.config)
	c.mu.RUnlock()
	return snapshot
}

func (m OperationMode) String() string {
	switch m {
	case OperationModeSync, OperationModeQueue, OperationModeRollout:
		return string(m)
	default:
		return string(OperationModeQueue)
	}
}

// sanitizeOperationModeConfig 清理并规范化传入的配置。
func sanitizeOperationModeConfig(cfg OperationModeConfig) operationModeState {
	normalized := OperationModeConfig{
		Mode:               OperationMode(strings.ToLower(strings.TrimSpace(string(cfg.Mode)))),
		RolloutPercent:     cfg.RolloutPercent,
		StickyHeader:       strings.ToLower(strings.TrimSpace(cfg.StickyHeader)),
		PreferSubjectKinds: append([]string{}, cfg.PreferSubjectKinds...),
		PreferSubjectUsers: append([]string{}, cfg.PreferSubjectUsers...),
	}

	if normalized.Mode == "" {
		normalized.Mode = OperationModeQueue
	}
	if normalized.Mode != OperationModeSync && normalized.Mode != OperationModeQueue && normalized.Mode != OperationModeRollout {
		normalized.Mode = OperationModeQueue
	}
	if normalized.RolloutPercent < 0 {
		normalized.RolloutPercent = 0
	}
	if normalized.RolloutPercent > 100 {
		normalized.RolloutPercent = 100
	}

	queueKinds, queueSet := dedupeToLower(cfg.QueueKinds)
	if len(queueKinds) == 0 {
		queueKinds = append(queueKinds, defaultOperationQueueKindNames...)
		queueSet = make(map[string]struct{}, len(defaultOperationQueueKindNames))
		for _, name := range defaultOperationQueueKindNames {
			queueSet[name] = struct{}{}
		}
	}
	normalized.QueueKinds = queueKinds

	allowList, allowSet := dedupeToLower(cfg.AllowUsers)
	blockList, blockSet := dedupeToLower(cfg.BlockUsers)
	preferSubjectKinds, preferKindSet := dedupeToLower(cfg.PreferSubjectKinds)
	preferSubjectUsers, preferUserSet := dedupeToLower(cfg.PreferSubjectUsers)
	normalized.AllowUsers = allowList
	normalized.BlockUsers = blockList
	normalized.PreferSubjectKinds = preferSubjectKinds
	normalized.PreferSubjectUsers = preferSubjectUsers

	state := operationModeState{
		config:               normalized,
		mode:                 normalized.Mode,
		rolloutPercent:       normalized.RolloutPercent,
		stickyHeader:         normalized.StickyHeader,
		queueKindSet:         make(map[operationpkg.OperationKind]struct{}, len(queueSet)),
		allowUserSet:         allowSet,
		blockUserSet:         blockSet,
		preferSubjectKindSet: make(map[operationpkg.OperationKind]struct{}, len(preferKindSet)),
		preferSubjectUserSet: preferUserSet,
	}

	for kind := range queueSet {
		state.queueKindSet[operationpkg.OperationKind(kind)] = struct{}{}
	}
	for kind := range preferKindSet {
		state.preferSubjectKindSet[operationpkg.OperationKind(kind)] = struct{}{}
	}

	return state
}

func withinRolloutSample(key string, percent int) bool {
	if percent >= 100 {
		return true
	}
	if percent <= 0 {
		return false
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32()%100) < percent
}

// 返回去重且小写化的字符串列表及其集合形式
func dedupeToLower(items []string) ([]string, map[string]struct{}) {
	if len(items) == 0 {
		return nil, make(map[string]struct{})
	}
	set := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, raw := range items {
		trimmed := strings.ToLower(strings.TrimSpace(raw))
		if trimmed == "" {
			continue
		}
		if _, exists := set[trimmed]; exists {
			continue
		}
		set[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out, set
}

func cloneOperationModeConfig(cfg OperationModeConfig) OperationModeConfig {
	clone := cfg
	clone.QueueKinds = append([]string(nil), cfg.QueueKinds...)
	clone.AllowUsers = append([]string(nil), cfg.AllowUsers...)
	clone.BlockUsers = append([]string(nil), cfg.BlockUsers...)
	clone.PreferSubjectKinds = append([]string(nil), cfg.PreferSubjectKinds...)
	clone.PreferSubjectUsers = append([]string(nil), cfg.PreferSubjectUsers...)
	return clone
}

func defaultOperationModeConfig() OperationModeConfig {
	return OperationModeConfig{
		Mode:           OperationModeQueue,
		RolloutPercent: 100, //全部走队列模式
		QueueKinds:     append([]string{}, defaultOperationQueueKindNames...),
	}
}

func operationModeConfigFromOptions(opts *serveropts.ServerRunOptions) OperationModeConfig {
	if opts == nil {
		return defaultOperationModeConfig()
	}
	cfg := OperationModeConfig{
		Mode:               OperationMode(opts.OperationMode),
		RolloutPercent:     opts.OperationRolloutPercent,
		StickyHeader:       opts.OperationRolloutStickyHeader,
		QueueKinds:         append([]string{}, opts.OperationQueueKinds...),
		AllowUsers:         append([]string{}, opts.OperationQueueUserAllowlist...),
		BlockUsers:         append([]string{}, opts.OperationQueueUserBlocklist...),
		PreferSubjectKinds: append([]string{}, opts.OperationRolloutPreferSubjectKinds...),
		PreferSubjectUsers: append([]string{}, opts.OperationRolloutPreferSubjectUsers...),
	}
	return cfg
}

// rolloutHeaderFromContext 从上下文中提取指定的标头值。
func rolloutHeaderFromContext(ctx context.Context, header string) string {
	if ctx == nil || header == "" {
		return ""
	}
	if value, ok := ctx.Value(contextHeaderKey(header)).(string); ok {
		return strings.ToLower(strings.TrimSpace(value))
	}
	return ""
}

type contextHeaderKey string

func (k contextHeaderKey) String() string {
	return string(k)
}
