package practice

import "time"

// Lease 表示 PendingCoordinator 中的一份“租约”，
// 你可以根据文档中的状态机选择是否需要额外字段（如过期时间、持有者ID等）。
type Lease struct {
// TODO: 按需要补充字段（例如 Key、ExpireAt、OwnerID 等）
}

// Coordinator 是练习用的租约管理接口：
//   - Acquire: 获取/创建租约；
//   - Release: 释放租约；
//   - SampleBackpressure: 基于当前状态返回背压等级。
//
// 验收：
//   - 同一 key 在未过期前重复 Acquire 时应表现为“已持有租约”（可返回同一 Lease 或带标记的结果）；
//   - TTL 到期后，再次 Acquire 应视为全新租约；
//   - Release 后应立刻可以再次 Acquire。
type Coordinator interface {
Acquire(key string, ttl time.Duration) (Lease, bool)
Release(key string)
SampleBackpressure() BackpressureLevel
}

// BackpressureLevel 表示当前背压等级。
type BackpressureLevel int

const (
BackpressureNormal BackpressureLevel = iota
BackpressureWarning
BackpressureCritical
)

// MemoryCoordinator 是内存实现的骨架版本。
//
// 验收：
//   - 使用 map + 互斥锁（或其它并发安全结构）实现最小可用的 Acquire/Release；
//   - 在单测中覆盖“重复 Acquire / 过期后重获 / 释放后重获”等典型路径。
type MemoryCoordinator struct {
// TODO: 补充字段（例如 leases map[string]Lease, mu sync.Mutex 等）
}

// NewMemoryCoordinator 创建一个新的内存版 Coordinator。
//
// TODO: 填充初始化逻辑（例如创建 map、设置初始配置等）。
func NewMemoryCoordinator() *MemoryCoordinator {
// TODO: 实现构造函数
return nil
}

// Acquire 尝试获取指定 key 的租约。
//
// TODO: 实现内存版 Acquire 逻辑：检查是否存在、是否过期、必要时创建新租约并返回。
func (c *MemoryCoordinator) Acquire(key string, ttl time.Duration) (Lease, bool) {
// TODO: 实现 Acquire
return Lease{}, false
}

// Release 释放指定 key 的租约。
//
// TODO: 实现内存版 Release 逻辑：从 map 中删除或标记释放，保证后续 Acquire 可以重新获取。
func (c *MemoryCoordinator) Release(key string) {
// TODO: 实现 Release
}

// SampleBackpressure 返回当前背压等级。
//
// 验收：
//   - 可以基于当前持有的租约数量/队列深度等简单指标返回 Normal/Warning/Critical；
//   - 单测中构造不同负载下的输入，验证返回等级与预期一致。
// TODO: 先给出一个简单实现（例如基于租约总数的阈值分级），后续可结合队列长度等指标优化。
func (c *MemoryCoordinator) SampleBackpressure() BackpressureLevel {
// TODO: 实现背压采样
return BackpressureNormal
}

