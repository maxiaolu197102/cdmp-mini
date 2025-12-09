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
type OperationModeConfig struct {
	Mode               OperationMode `json:"mode"`
	RolloutPercent     int           `json:"rolloutPercent"`
	StickyHeader       string        `json:"stickyHeader,omitempty"`
	QueueKinds         []string      `json:"queueKinds,omitempty"`
	AllowUsers         []string      `json:"allowUsers,omitempty"`
	BlockUsers         []string      `json:"blockUsers,omitempty"`
	PreferSubjectKinds []string      `json:"preferSubjectKinds,omitempty"`
	PreferSubjectUsers []string      `json:"preferSubjectUsers,omitempty"`
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
	if c == nil {
		return OperationModeQueue
	}

	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()
	//白名单查询,如果未命中则走同步
	if len(state.queueKindSet) > 0 {
		//未命中种类则走同步
		if _, ok := state.queueKindSet[kind]; !ok {
			return OperationModeSync
		}
	}

	normalizedSubject := strings.ToLower(strings.TrimSpace(subject))
	if normalizedSubject != "" {
		//用户黑名单优先走同步
		if _, blocked := state.blockUserSet[normalizedSubject]; blocked {
			return OperationModeSync
		}
		//用户白名单优先走队列
		if _, allowed := state.allowUserSet[normalizedSubject]; allowed {
			return OperationModeQueue
		}
	}
	//判断当前模式
	switch state.mode {
	case OperationModeSync:
		return OperationModeSync
	case OperationModeQueue:
		return OperationModeQueue
	case OperationModeRollout:
		percent := state.rolloutPercent
		if percent <= 0 {
			return OperationModeSync
		}
		if percent >= 100 {
			return OperationModeQueue
		}

		preferSubject := false
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
		if withinRolloutSample(key, percent) {
			return OperationModeQueue
		}
		return OperationModeSync
	default:
		return OperationModeQueue
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
