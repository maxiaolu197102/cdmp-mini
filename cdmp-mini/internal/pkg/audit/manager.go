package audit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Event 表示一次受审计的行为。所有字段都是可选的，但建议至少提供 Actor、Action 和 Outcome。
// Metadata 用于扩展自定义业务字段，例如请求体摘要、资源变更前后的对比信息等。
type Event struct {
	Actor        string         // 触发行为的主体（用户名、服务名等）
	ActorID      string         // 主体唯一标识，可选
	Action       string         // 执行的动作，如 auth.login、user.create
	ResourceType string         // 资源类型，例如 user、policy
	ResourceID   string         // 资源唯一标识
	Target       string         // 行为影响的目标，可与 ResourceID 不同，例如 IP、角色名称
	Outcome      string         // 结果：success、fail、deny 等
	ErrorMessage string         // 若失败，可记录错误信息（注意脱敏）
	RequestID    string         // 关联一次请求的唯一 ID，便于串联日志
	IP           string         // 操作者来源 IP
	UserAgent    string         // 客户端 UA
	Metadata     map[string]any // 额外的业务数据
	OccurredAt   time.Time      // 行为发生时间，若为空将在 Submit 时自动补齐
}

// Clone 返回当前审计事件的浅拷贝，确保后台异步处理不会受到调用方后续修改的影响。
func (e Event) Clone() Event {
	if e.Metadata == nil {
		return e
	}
	clonedMeta := make(map[string]any, len(e.Metadata))
	for k, v := range e.Metadata {
		clonedMeta[k] = v
	}
	e.Metadata = clonedMeta
	return e
}

// Sink 定义审计事件的落地方式，例如写入日志、推送消息队列、持久化数据库等。
type Sink interface {
	Name() string
	Write(context.Context, Event) error
}

// MetricsSink 将审计事件转换为 Prometheus 指标，可用于实时观测系统行为。
type MetricsSink struct{}

func (MetricsSink) Name() string { return "metrics" }

func (MetricsSink) Write(_ context.Context, event Event) error {
	metrics.RecordAuditEvent(event.Action, event.ResourceType, event.Outcome)
	if event.Outcome != "success" {
		metrics.RecordAuditFailure(event.Action, event.ResourceType)
	}
	return nil
}

// SinkFunc 允许使用函数快速实现 Sink 接口，简化调用方自定义。
type SinkFunc struct {
	SinkName string
	Fn       func(context.Context, Event) error
}

func (s SinkFunc) Name() string { return s.SinkName }

func (s SinkFunc) Write(ctx context.Context, event Event) error {
	if s.Fn == nil {
		return nil
	}
	return s.Fn(ctx, event)
}

// LogSink 是默认的落地实现，会使用项目统一的日志系统输出结构化审计信息。
type LogSink struct{}

func (LogSink) Name() string { return "log" }

func (LogSink) Write(_ context.Context, event Event) error {
	keysAndValues := []any{
		"actor", event.Actor,
		"actor_id", event.ActorID,
		"action", event.Action,
		"resource_type", event.ResourceType,
		"resource_id", event.ResourceID,
		"target", event.Target,
		"outcome", event.Outcome,
		"error", event.ErrorMessage,
		"request_id", event.RequestID,
		"ip", event.IP,
		"user_agent", event.UserAgent,
		"occurred_at", event.OccurredAt.Format(time.RFC3339Nano),
	}
	if len(event.Metadata) > 0 {
		keysAndValues = append(keysAndValues, "metadata", event.Metadata)
	}
	log.Infow("audit event", keysAndValues...)
	return nil
}

// FileSink 将审计事件以JSON Lines格式写入指定文件。
type FileSink struct {
	path string
	mu   sync.Mutex
}

func NewFileSink(path string) (*FileSink, error) {
	if path == "" {
		return nil, errors.New("file sink path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create audit log dir: %w", err)
	}
	return &FileSink{path: path}, nil
}

func (f *FileSink) Name() string { return "file" }

func (f *FileSink) Write(_ context.Context, event Event) error {
	data := map[string]any{
		"actor":         event.Actor,
		"actor_id":      event.ActorID,
		"action":        event.Action,
		"resource_type": event.ResourceType,
		"resource_id":   event.ResourceID,
		"target":        event.Target,
		"outcome":       event.Outcome,
		"error":         event.ErrorMessage,
		"request_id":    event.RequestID,
		"ip":            event.IP,
		"user_agent":    event.UserAgent,
		"occurred_at":   event.OccurredAt.Format(time.RFC3339Nano),
	}
	if len(event.Metadata) > 0 {
		data["metadata"] = event.Metadata
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal audit event: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open audit log file: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write audit log: %w", err)
	}
	return nil
}

// Config 用于构建审计管理器。
type Config struct {
	Enabled         bool
	BufferSize      int
	ShutdownTimeout time.Duration
	LogFile         string
	Sinks           []Sink
	EnableMetrics   bool
	RecentBuffer    int
}

// Manager 负责异步收集并分发审计事件。
type Manager struct {
	cfg    Config
	sinks  []Sink
	events chan Event

	ctx    context.Context
	cancel context.CancelFunc

	once sync.Once
	wg   sync.WaitGroup

	recentMu sync.RWMutex
	recent   []Event
}

func NewManager(cfg Config) (*Manager, error) {
	if !cfg.Enabled {
		return &Manager{cfg: cfg}, nil
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 256
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.RecentBuffer <= 0 {
		cfg.RecentBuffer = 256
	}

	sinks := dedupeSinks(cfg.Sinks)
	if len(sinks) == 0 {
		sinks = append(sinks, LogSink{})
	}
	if cfg.EnableMetrics {
		sinks = appendIfMissing(sinks, MetricsSink{})
	}
	if cfg.LogFile != "" {
		fileSink, err := NewFileSink(cfg.LogFile)
		if err != nil {
			return nil, err
		}
		sinks = appendIfMissing(sinks, fileSink)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:    cfg,
		sinks:  sinks,
		events: make(chan Event, cfg.BufferSize),
		ctx:    ctx,
		cancel: cancel,
		recent: make([]Event, 0, cfg.RecentBuffer),
	}

	m.wg.Add(1)
	go m.loop()
	return m, nil
}

func appendIfMissing(sinks []Sink, candidate Sink) []Sink {
	for _, existing := range sinks {
		if existing.Name() == candidate.Name() {
			return sinks
		}
	}
	return append(sinks, candidate)
}

func dedupeSinks(sinks []Sink) []Sink {
	if len(sinks) <= 1 {
		return sinks
	}
	seen := make(map[string]struct{}, len(sinks))
	result := make([]Sink, 0, len(sinks))
	for _, s := range sinks {
		if s == nil {
			continue
		}
		name := s.Name()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, s)
	}
	return result
}

// Submit 发送审计事件到后台处理。
func (m *Manager) Submit(ctx context.Context, event Event) {
	if m == nil || !m.cfg.Enabled {
		return
	}

	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now()
	}
	cloned := event.Clone()
	m.appendRecent(cloned)

	select {
	case m.events <- cloned:
	default:
		log.Warnw("audit buffer full, dropping event", map[string]any{
			"action": event.Action,
			"actor":  event.Actor,
		})
	}

	if ctx != nil && ctx.Err() != nil {
		return
	}
}

// Shutdown 在给定超时时间内等待所有事件处理完成。
func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil || !m.cfg.Enabled {
		return nil
	}
	var err error
	m.once.Do(func() {
		m.cancel()
		close(m.events)
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-done:
		}
	})
	return err
}

func (m *Manager) loop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			for event := range m.events {
				m.dispatch(event)
			}
			return
		case event, ok := <-m.events:
			if !ok {
				return
			}
			m.dispatch(event)
		}
	}
}

func (m *Manager) dispatch(event Event) {
	for _, sink := range m.sinks {
		if err := sink.Write(m.ctx, event); err != nil {
			log.Warnw("audit sink write failed", map[string]any{
				"sink":   sink.Name(),
				"error":  err,
				"action": event.Action,
			})
		}
	}
}

func (m *Manager) appendRecent(event Event) {
	if m == nil || !m.cfg.Enabled {
		return
	}
	m.recentMu.Lock()
	defer m.recentMu.Unlock()
	if len(m.recent) >= m.cfg.RecentBuffer {
		// drop oldest
		copy(m.recent, m.recent[1:])
		m.recent[len(m.recent)-1] = event
		return
	}
	m.recent = append(m.recent, event)
}

// Recent 返回最近 limit 条审计事件，最新事件在前。
func (m *Manager) Recent(limit int) []Event {
	if m == nil || !m.cfg.Enabled || limit <= 0 {
		return nil
	}
	m.recentMu.RLock()
	defer m.recentMu.RUnlock()
	if len(m.recent) == 0 {
		return nil
	}
	if limit > len(m.recent) {
		limit = len(m.recent)
	}
	result := make([]Event, 0, limit)
	for i := 0; i < limit; i++ {
		idx := len(m.recent) - 1 - i
		if idx < 0 {
			break
		}
		result = append(result, m.recent[idx])
	}
	return result
}

// Enabled 返回审计是否开启。
func (m *Manager) Enabled() bool {
	return m != nil && m.cfg.Enabled
}

// BuildEventFromRequest 根据 HTTP 请求快速构建事件骨架。
func BuildEventFromRequest(req *http.Request) Event {
	if req == nil {
		return Event{}
	}
	event := Event{
		RequestID: req.Header.Get("X-Request-Id"),
		UserAgent: req.UserAgent(),
		Metadata:  map[string]any{"method": req.Method, "path": req.URL.Path},
	}
	if host, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		event.IP = host
	} else {
		event.IP = req.RemoteAddr
	}
	if actor := req.Header.Get("X-Forwarded-User"); actor != "" {
		event.Actor = actor
	}
	if actorID := req.Header.Get("X-Forwarded-User-Id"); actorID != "" {
		event.ActorID = actorID
	}
	return event
}
