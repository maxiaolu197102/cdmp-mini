package practice

import "context"

// State 表示 OperationPipeline 中一个请求的状态。
// 你可以根据文档中的状态机选择具体枚举值。
type State string

const (
	StatePending   State = "PENDING"
	StateProcessing      = "PROCESSING"
	StateSucceeded       = "SUCCEEDED"
	StateFailed          = "FAILED"
)

// Request 表示待处理的操作请求（简化版）。
type Request struct {
	ID   string
	Data interface{}
}

// StateStore 抽象请求状态存储。
//
// 验收：
//   - 支持按 ID 读写状态；
//   - 能在测试中模拟“乱序更新”并验证幂等性。
type StateStore interface {
	Get(id string) (State, bool)
	Set(id string, state State)
}

// Queue 抽象操作队列。
//
// 验收：
//   - Submit 后可被 ProcessOnce 按 FIFO 顺序取出；
//   - 队列为空时 ProcessOnce 应表现为 no-op（可返回标记）。
type Queue interface {
	Submit(req Request)
	Dequeue() (Request, bool)
}

// Pipeline 是最小可用的操作流水线接口：
//   - Submit: 入队一个请求；
//   - ProcessOnce: 取出一个请求并执行。
//
// 验收：
//   - 单个请求按文档中的状态机依次流转（Pending → Processing → Succeeded/Failed）；
//   - 相同请求不会被重复处理（幂等性保证）。
type Pipeline interface {
	Submit(ctx context.Context, req Request) error
	ProcessOnce(ctx context.Context) error
}

// MemoryStateStore 是内存实现的 StateStore 骨架。
// TODO: 使用 map + 互斥锁实现并发安全的读写。
type MemoryStateStore struct {
	// TODO: 补充字段
}

// MemoryQueue 是内存实现的 Queue 骨架。
// TODO: 使用切片或 channel 实现 FIFO 队列。
type MemoryQueue struct {
	// TODO: 补充字段
}

// SimplePipeline 是基于内存队列 + 状态存储的最小 Pipeline 实现骨架。
// TODO: 在实现中串联 Queue 和 StateStore，完成 Submit/ProcessOnce 逻辑。
type SimplePipeline struct {
	// TODO: 补充字段（例如 queue Queue, store StateStore 等）
}

// NewSimplePipeline 构造函数，注入 Queue 和 StateStore。
//
// 验收：
//   - 使用内存实现时，能通过后续测试中的 Submit/ProcessOnce 用例。
// TODO: 根据需要扩展参数。
func NewSimplePipeline(q Queue, store StateStore) *SimplePipeline {
	// TODO: 实现构造函数
	return nil
}

// Submit 入队一个请求并设置初始状态。
// TODO: 设置状态为 Pending，并将请求放入队列。
func (p *SimplePipeline) Submit(ctx context.Context, req Request) error {
	// TODO: 实现 Submit
	return nil
}

// ProcessOnce 取出一个请求并执行：
//   - 将状态标记为 Processing；
//   - 执行“伪业务逻辑”（可以是简单的函数调用或回调）；
//   - 根据结果标记为 Succeeded 或 Failed。
//
// TODO: 在实现时注意：对于已在终态的请求，应避免重复处理。
func (p *SimplePipeline) ProcessOnce(ctx context.Context) error {
	// TODO: 实现 ProcessOnce
	return nil
}
