package practice

import "testing"

// TestEvalPendingScript_SingleOwner 用于验证同一 key 只有一个持有者成功：
//   - 构造多个并发/顺序调用 EvalPendingScript 的场景；
//   - 期望只有一次返回 acquired=true，其余为 false。
//
// 验收：统计 acquired=true 的次数恰好为 1，且在 TTL 期间再次调用均视为已有租约。
func TestEvalPendingScript_SingleOwner(t *testing.T) {
	t.Skip("TODO: 实现单持有者原子性测试，并删除本行")
}

// TestEvalPendingScript_ExpireBehavior 用于验证过期语义：
//   - 在 TTL 期间调用 EvalPendingScript 再次尝试获取，应视为已有租约；
//   - TTL 到期后，再次调用应允许获取新租约。
//
// 验收：可以使用 fakeClock 或手动模拟 TTL 行为，确保逻辑与文档状态机一致。
func TestEvalPendingScript_ExpireBehavior(t *testing.T) {
	t.Skip("TODO: 实现过期行为测试，并删除本行")
}
