package bizid

import "sync"

// BizID 表示一个稳定的业务标识，用于跨路由做聚合（例如：订单、支付、转账）。
type BizID struct {
	Key        string // 稳定业务 ID，如 "order"、"payment"，用于拼接限流 key 等
	Name       string // 业务展示名，如 "下单"、"支付"，便于日志/监控查看
	Deprecated bool   // 是否已废弃，便于迁移和告警
}

var (
	mu        sync.RWMutex
	bizMap    = make(map[string]BizID) // bizKey -> BizID
	routeBiz  = make(map[string]string) // "METHOD path" -> bizKey
	bizLimits = make(map[string]int)   // bizKey -> 每个业务的写限流基础阈值（后续可由表/配置填充）
)

func routeKey(method, path string) string {
	return method + " " + path
}

// Register 在全局注册一个业务标识。通常在进程启动阶段调用。
func Register(b BizID) {
	if b.Key == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	bizMap[b.Key] = b
}

// Get 返回给定 key 对应的 BizID 副本；如果不存在则返回 nil。
func Get(key string) *BizID {
	if key == "" {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	b, ok := bizMap[key]
	if !ok {
		return nil
	}
	copy := b
	return &copy
}

// RegisterRouteBiz 将 HTTP 路由（method + path）与某个业务绑定。
// 建议在路由注册处集中调用，便于维护“路由 -> 业务”的映射关系。
func RegisterRouteBiz(method, path, bizKey string) {
	if method == "" || path == "" || bizKey == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	routeBiz[routeKey(method, path)] = bizKey
}

// ResolveBizByRoute 根据 HTTP 方法和路由路径解析出对应的业务标识。
// 如果未绑定业务或业务未注册，则返回 nil。
func ResolveBizByRoute(method, path string) *BizID {
	if method == "" || path == "" {
		return nil
	}
	mu.RLock()
	bizKey, ok := routeBiz[routeKey(method, path)]
	if !ok {
		mu.RUnlock()
		return nil
	}
	b, ok := bizMap[bizKey]
	mu.RUnlock()
	if !ok {
		return nil
	}
	copy := b
	return &copy
}

// SetBizLimit 为某个业务设置写限流基础阈值（per-identifier 维度）。
// 后续可以由配置/数据库表装载调用；limit<=0 表示禁用业务级限流。
func SetBizLimit(bizKey string, limit int) {
	if bizKey == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if limit <= 0 {
		delete(bizLimits, bizKey)
		return
	}
	bizLimits[bizKey] = limit
}

// GetBizLimit 返回某个业务当前配置的写限流阈值；未配置或<=0 视为 0（不启用业务级限流）。
func GetBizLimit(bizKey string) int {
	if bizKey == "" {
		return 0
	}
	mu.RLock()
	defer mu.RUnlock()
	return bizLimits[bizKey]
}
