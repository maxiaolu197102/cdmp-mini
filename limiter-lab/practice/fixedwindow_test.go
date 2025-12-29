package practice

import "testing"

// TestFixedWindow_SingleKey 用于练习：
//   - 同一个 key 在一个窗口内，前 N 次通过，第 N+1 次被拒绝。
//
// 验收：构造多组用例覆盖：刚好等于 limit、超过 limit 一次、以及在窗口结束后再次放行的场景；
// 断言 Allow 返回值序列与预期完全一致。
// TODO: 按 checklist 自己设计表驱动用例和断言，然后删除 t.Skip。
func TestFixedWindow_SingleKey(t *testing.T) {
	// 提示：可以使用表驱动测试 + for 循环调用 Allow。
	t.Skip("TODO: 实现测试用例，并删除本行")
}

// TestFixedWindow_MultiKey 用于练习：
//   - 不同 key 之间的计数互不影响。
//
// 验收：为多个不同的 key 构造请求序列，验证：即使某个 key 已经被限流，
// 其它 key 仍可在各自的窗口内按 limit 正常通过；所有断言均通过。
// TODO: 按 checklist 自己设计表驱动用例和断言，然后删除 t.Skip。
func TestFixedWindow_MultiKey(t *testing.T) {
	t.Skip("TODO: 实现测试用例，并删除本行")
}

