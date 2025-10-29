/*
当前已接入审计的事件
GenericAPIServer 服务生命周期
api-server.startup：启动成功/失败/被取消
api-server.runtime：运行期异常
api-server.shutdown：优雅关停开始/成功/失败
关键基础组件启动/关停
mysql.startup / mysql.shutdown
redis.startup / redis.shutdown
kafka.startup / kafka.shutdown
审计管理器自身：audit.startup（成功）
用户相关控制器
user.change_password：输入校验失败、密码比对失败、修改成功
user.delete（强制删除接口）：参数错误、服务层错误、删除成功
user.delete_collection：参数缺失、批量删除失败、成功
user.get：参数非法、查询失败、用户不存在、查询成功
user.create / user.update / user.list / user.login / user.logout 等完整 CRUD + 登录流程
这些事件都会记录操作者（system 或登陆用户）、目标资源、结果以及错误信息，并通过审计管理器写到日志/文件/指标。

建议未来补充审计的事件
结合现有功能和常见安全要求，可以按板块补齐：

用户 & 鉴权
用户角色、权限绑定 / 解绑
密码重置、锁定/解锁、账号禁用/启用
Token、Session、API Key 相关操作
资源管理（按业务模块拆分）
角色、策略、租户、项目等 CRUD
资源授权、撤销、批量导入导出
敏感配置变更（如策略模板、密钥、证书）
运维与系统操作
管理后台配置变更（开关、限流、配额、审计策略）
系统任务执行（批量同步、数据迁移、脚本操作）
后台手动触发的补偿/重放/重试任务
服务自身行为
子组件异常恢复、重启、重连事件
关键健康检查失败、重试、恢复
配置热更新、版本升级、回滚
安全事件
认证失败、鉴权拒绝
违规访问/频繁操作（配合风控/告警）
审计日志本身的读取、导出
审计功能扩展
新增外部 sink（Kafka、ES、Webhook 等）启停与失败
审计配置调整（启停、文件路径、缓冲配置、过滤规则
*/

package server

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

// AuditEvent 表示一次受审计的行为。所有字段都是可选的，但建议至少提供 Actor、Action 和 Outcome。
// Metadata 用于扩展自定义业务字段，例如请求体摘要、资源变更前后的对比信息等。
type AuditEvent struct {
	Actor        string         // 触发行为的主体（用户名、服务名等）
	ActorID      string         // 主体唯一标识，可选
	Action       string         // 执行的动作，如 create_user、disable_policy
	ResourceType string         // 资源类型，例如 user、policy
	ResourceID   string         // 资源唯一标识
	Target       string         // 行为影响的目标，可与 ResourceID 不同，例如 IP、角色名称
	Outcome      string         // 结果：success、fail、deny 等
	ErrorMessage string         // 若失败，可记录错误信息（脱敏）
	RequestID    string         // 关联一次请求的唯一 ID，便于串联日志
	IP           string         // 操作者来源 IP
	UserAgent    string         // 客户端 UA
	Metadata     map[string]any // 额外的业务数据
	OccurredAt   time.Time      // 行为发生时间，若为空将在 Submit 时自动补齐
}

// Clone 返回当前审计事件的浅拷贝，确保后台异步处理不会受到调用方后续修改的影响。
func (e AuditEvent) Clone() AuditEvent {
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
	Write(context.Context, AuditEvent) error
}

// SinkFunc 允许使用函数快速实现 Sink 接口，简化调用方自定义。
type SinkFunc struct {
	SinkName string
	Fn       func(context.Context, AuditEvent) error
}

// Name 实现 Sink 接口。
func (s SinkFunc) Name() string { return s.SinkName }

// Write 实现 Sink 接口。
func (s SinkFunc) Write(ctx context.Context, event AuditEvent) error {
	if s.Fn == nil {
		return nil
	}
	return s.Fn(ctx, event)
}

// LogSink 是默认的落地实现，会使用项目统一的日志系统输出结构化审计信息。
type LogSink struct{}

// Name 实现 Sink 接口。
func (LogSink) Name() string { return "log" }

// Write 实现 Sink 接口。
func (LogSink) Write(_ context.Context, event AuditEvent) error {
	fields := map[string]any{
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
		fields["metadata"] = event.Metadata
	}
	log.Infow("audit event", fields)
	return nil
}

// Config 用于构建审计管理器。
type Config struct {
	Enabled         bool          // 是否开启审计，关闭时 Submit 为 no-op
	BufferSize      int           // 异步通道容量，默认 256
	ShutdownTimeout time.Duration // 关闭等待耗尽队列的超时
	Sinks           []Sink        // 事件落地通道，若为空则使用 LogSink
}

// Manager 负责异步收集并分发审计事件。
//
// 调用流程：
//  1. 通过 NewManager 创建实例（可选配置多个落地 Sink）；
//  2. 业务代码调用 Submit 记录事件；
//  3. 应用退出前调用 Shutdown，确保剩余事件被处理。
type Manager struct {
	cfg    Config
	sinks  []Sink
	events chan AuditEvent

	ctx    context.Context
	cancel context.CancelFunc

	once sync.Once
	wg   sync.WaitGroup
}

// NewManager 创建审计管理器。若 cfg.Enabled 为 false，则返回一个可安全调用的空实现。
func NewManager(cfg Config) *Manager {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 256
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}

	if len(cfg.Sinks) == 0 {
		cfg.Sinks = []Sink{LogSink{}}
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		cfg:    cfg,
		sinks:  cfg.Sinks,
		events: make(chan AuditEvent, cfg.BufferSize),
		ctx:    ctx,
		cancel: cancel,
	}

	if cfg.Enabled {
		m.wg.Add(1)
		go m.loop()
	} else {
		close(m.events)
	}
	return m
}

// Submit 发送审计事件到后台处理。若审计未开启则直接返回。
// 调用方无需关心并发问题，该方法是并发安全的。
func (m *Manager) Submit(ctx context.Context, event AuditEvent) {
	if !m.cfg.Enabled {
		return
	}

	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now()
	}
	cloned := event.Clone()

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
// 若在超时时间前处理完毕，返回 nil；否则返回 context.DeadlineExceeded。
func (m *Manager) Shutdown(ctx context.Context) error {
	var err error
	m.once.Do(func() {
		if !m.cfg.Enabled {
			return
		}
		m.cancel()
		close(m.events)
		ch := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(ch)
		}()

		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-ch:
		}
	})
	return err
}

// loop 负责消费事件并分发给配置的 sinks。
func (m *Manager) loop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			// 在关闭阶段继续耗尽事件
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

// dispatch 将事件写入所有 Sink，若某个 Sink 失败会记录日志但不阻塞其他 Sink。
func (m *Manager) dispatch(event AuditEvent) {
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

// BuildEventFromRequest 根据 HTTP 请求和响应元信息快速构建一个审计事件骨架。
// 业务方可在返回的事件上补充 Action、Outcome 等字段后再调用 Submit。
func BuildEventFromRequest(req *http.Request) AuditEvent {
	if req == nil {
		return AuditEvent{}
	}

	event := AuditEvent{
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
