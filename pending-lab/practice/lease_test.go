package practice

import "testing"

// TestMemoryCoordinator_AcquireAndRelease 用于练习基础租约行为：
//   - 第一次 Acquire 某个 key 应成功创建租约；
//   - 未过期前重复 Acquire 同一 key，不应创建新租约（可通过返回标记或对象相等性判断）；
//   - Release 后再次 Acquire，应视为新租约。
//
// 验收：通过多个子测试覆盖上述路径，并断言行为与预期一致。
func TestMemoryCoordinator_AcquireAndRelease(t *testing.T) {
t.Skip("TODO: 实现 Acquire/Release 行为测试，并删除本行")
}

// TestMemoryCoordinator_ExpireAndReacquire 用于练习 TTL 行为：
//   - 使用可注入时间源（可选）或人为控制时间前进；
//   - 在 TTL 未到期前 Acquire，视为同一租约；
//   - 超过 TTL 后再次 Acquire，应创建新租约。
//
// 验收：在测试中清晰区分“未过期时的重复 Acquire”和“过期后的重新 Acquire”。
func TestMemoryCoordinator_ExpireAndReacquire(t *testing.T) {
t.Skip("TODO: 实现 TTL 相关测试，并删除本行")
}

// TestMemoryCoordinator_SampleBackpressure 用于练习背压等级判定：
//   - 根据持有的租约数量或其它指标，预期返回 Normal/Warning/Critical；
//   - 覆盖临界值附近的输入，确保分级规则正确。
//
// 验收：构造表驱动用例，不同租约数量对应不同 BackpressureLevel，所有断言通过。
func TestMemoryCoordinator_SampleBackpressure(t *testing.T) {
t.Skip("TODO: 实现背压采样测试，并删除本行")
}

