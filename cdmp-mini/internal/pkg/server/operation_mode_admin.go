package server

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	apiserveropts "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	usersvc "github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/service/v1/user"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/core"
)

type operationModeUpdateRequest struct {
	Mode           *string   `json:"mode"`
	RolloutPercent *int      `json:"rolloutPercent"`
	StickyHeader   *string   `json:"stickyHeader"`
	QueueKinds     *[]string `json:"queueKinds"`
	AllowUsers     *[]string `json:"allowUsers"`
	BlockUsers     *[]string `json:"blockUsers"`
}

func RegisterOperationModeAdminHandlers(rg *gin.RouterGroup, userService *usersvc.UserService, opts *apiserveropts.Options) {
	if rg == nil {
		return
	}

	group := rg.Group("/users")

	group.GET("/operation-mode", func(c *gin.Context) {
		if !authorizeAdminRead(c, opts) {
			return
		}
		if userService == nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		snapshot := userService.OperationModeSnapshot()
		metrics.PublishOperationModeSnapshot("user_service", snapshot.Mode.String(), snapshot.RolloutPercent, len(snapshot.AllowUsers), len(snapshot.BlockUsers))
		core.WriteResponse(c, nil, gin.H{"config": snapshot})
	})

	group.PUT("/operation-mode", func(c *gin.Context) {
		if !authorizeAdminWrite(c, opts) {
			return
		}
		if userService == nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		var req operationModeUpdateRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			core.WriteResponse(c, err, nil)
			return
		}

		current := userService.OperationModeSnapshot()
		updated := mergeOperationModeUpdate(current, req)
		snapshot := userService.UpdateOperationMode(updated)
		core.WriteResponse(c, nil, gin.H{"config": snapshot})
	})
}

func mergeOperationModeUpdate(base usersvc.OperationModeConfig, req operationModeUpdateRequest) usersvc.OperationModeConfig {
	cfg := base
	if req.Mode != nil {
		cfg.Mode = usersvc.OperationMode(strings.TrimSpace(strings.ToLower(*req.Mode)))
	}
	if req.RolloutPercent != nil {
		cfg.RolloutPercent = *req.RolloutPercent
	}
	if req.StickyHeader != nil {
		cfg.StickyHeader = strings.TrimSpace(*req.StickyHeader)
	}
	if req.QueueKinds != nil {
		cfg.QueueKinds = append([]string{}, (*req.QueueKinds)...)
	}
	if req.AllowUsers != nil {
		cfg.AllowUsers = append([]string{}, (*req.AllowUsers)...)
	}
	if req.BlockUsers != nil {
		cfg.BlockUsers = append([]string{}, (*req.BlockUsers)...)
	}
	return cfg
}

func authorizeAdminRead(c *gin.Context, opts *apiserveropts.Options) bool {
	if opts != nil && opts.ServerRunOptions != nil && opts.ServerRunOptions.AdminToken != "" {
		provided := c.GetHeader("X-Admin-Token")
		if provided == "" || provided != opts.ServerRunOptions.AdminToken {
			c.AbortWithStatus(http.StatusUnauthorized)
			return false
		}
		return true
	}
	if !isLocalOrDebug(c, opts) {
		c.AbortWithStatus(http.StatusForbidden)
		return false
	}
	return true
}

func authorizeAdminWrite(c *gin.Context, opts *apiserveropts.Options) bool {
	if opts != nil && opts.ServerRunOptions != nil && opts.ServerRunOptions.AdminToken != "" {
		provided := c.GetHeader("X-Admin-Token")
		if provided == "" || provided != opts.ServerRunOptions.AdminToken {
			c.AbortWithStatus(http.StatusUnauthorized)
			return false
		}
		return true
	}
	if !isLocalOrDebug(c, opts) {
		c.AbortWithStatus(http.StatusForbidden)
		return false
	}
	return true
}
