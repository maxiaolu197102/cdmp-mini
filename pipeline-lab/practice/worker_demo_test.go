package practice

import "testing"

// TestWorkerLoop_Demo 用于练习最小 Worker 循环与补偿集成：
//   - 创建一个 SimplePipeline 实例；
//   - 启动一个“假 Worker 循环”（可以是 for + 有限次 ProcessOnce 调用）；
//   - 提交多个请求，部分设计为失败从而触发补偿逻辑（可以是简单的日志或计数器）。
//
// 验收：
//   - 成功请求最终进入 Succeeded 终态；
//   - 失败请求经过若干次重试后进入 Failed 终态，并触发补偿路径；
//   - 可以通过状态和补偿计数器的断言验证整个流程。
func TestWorkerLoop_Demo(t *testing.T) {
	t.Skip("TODO: 在完成基础 Pipeline 后，实现 Worker Demo 测试，并删除本行")
}
