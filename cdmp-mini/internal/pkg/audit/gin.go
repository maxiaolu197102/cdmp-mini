package audit

import "github.com/gin-gonic/gin"

const GinContextKey = "__audit_manager"

// InjectToGinContext 将审计管理器注入到 gin.Context，供后续处理链路使用。
func InjectToGinContext(c *gin.Context, mgr *Manager) {
	if c == nil || mgr == nil {
		return
	}
	c.Set(GinContextKey, mgr)
}

// FromGinContext 尝试从 gin.Context 中提取审计管理器。
func FromGinContext(c *gin.Context) *Manager {
	if c == nil {
		return nil
	}
	if v, exists := c.Get(GinContextKey); exists {
		if mgr, ok := v.(*Manager); ok {
			return mgr
		}
	}
	return nil
}
