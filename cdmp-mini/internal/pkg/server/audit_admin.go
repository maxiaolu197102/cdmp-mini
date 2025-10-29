package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	apiserveropts "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/audit"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
)

const defaultAuditEventLimit = 50

// RegisterAuditAdminHandlers exposes debugging endpoints for audit diagnostics.
func RegisterAuditAdminHandlers(rg *gin.RouterGroup, mgr *audit.Manager, opts *apiserveropts.Options) {
	if rg == nil {
		return
	}
	rg.GET("/audit/events", func(c *gin.Context) {
		if opts != nil && opts.ServerRunOptions != nil && opts.ServerRunOptions.AdminToken != "" {
			provided := c.GetHeader("X-Admin-Token")
			if provided == "" || provided != opts.ServerRunOptions.AdminToken {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
		} else {
			if !isLocalOrDebug(c, opts) {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
		}

		limit := defaultAuditEventLimit
		if raw := c.Query("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				limit = parsed
			}
		}

		if limit > 500 {
			limit = 500
		}

		if mgr == nil || !mgr.Enabled() {
			core.WriteResponse(c, nil, gin.H{"events": []any{}, "enabled": false})
			return
		}

		events := mgr.Recent(limit)
		if events == nil {
			core.WriteResponse(c, nil, gin.H{"events": []any{}, "enabled": true})
			return
		}

		core.WriteResponse(c, nil, gin.H{"events": events, "enabled": true})
	})
}
