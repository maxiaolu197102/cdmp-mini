package practice

import "testing"

// TestSimplePipeline_SubmitAndProcessOnce 用于练习最小流水线：
//   - Submit 一个请求；
//   - 调用一次 ProcessOnce；
//   - 期望状态从 Pending → Processing → Succeeded（或 Failed）。
//
// 验收：
//   - 通过状态存储断言最终状态；
//   - 确认 Request 只被处理一次。
func TestSimplePipeline_SubmitAndProcessOnce(t *testing.T) {
	t.Skip("TODO: 实现 Submit/ProcessOnce 流程测试，并删除本行")
}

// TestSimplePipeline_IdempotentStateStore 用于练习 StateStore 幂等写入：
//   - 构造乱序状态更新场景（例如先写 Succeeded，再写 Pending）；
//   - 期望最终状态仍为终态而不是被回滚。
//
// 验收：通过多组用例验证“终态不可被回滚”的规则。
func TestSimplePipeline_IdempotentStateStore(t *testing.T) {
	t.Skip("TODO: 实现幂等状态存储测试，并删除本行")
}

// TestSimplePipeline_RetryAndFailure 用于练习重试与失败终态：
//   - 模拟业务执行函数在前几次失败，之后成功或一直失败；
//   - 验证达到最大重试次数后进入 Failed 终态，并不会无限重试。
//
// 验收：通过状态和调用次数的断言，区分“尚在重试的失败”和“最终失败”。
func TestSimplePipeline_RetryAndFailure(t *testing.T) {
	t.Skip("TODO: 实现重试/失败逻辑测试，并删除本行")
}
