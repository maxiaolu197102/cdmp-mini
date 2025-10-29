package user

import (
	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
)

func submitAudit(c *gin.Context, event audit.Event) {
	if c == nil {
		return
	}
	mgr := audit.FromGinContext(c)
	if mgr == nil {
		return
	}
	if event.Metadata == nil {
		event.Metadata = map[string]any{}
	}
	if route := c.FullPath(); route != "" {
		event.Metadata["route"] = route
	}
	if event.Metadata["method"] == nil {
		event.Metadata["method"] = c.Request.Method
	}
	mgr.Submit(c.Request.Context(), event)
}
