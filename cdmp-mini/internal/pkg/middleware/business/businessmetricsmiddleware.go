package business

import (
	"github.com/gin-gonic/gin"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
)

// BusinessMetricsMiddleware 业务监控中间件
func BusinessMetricsMiddleware(serviceName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		operation := getOperationFromContext(c)
		
		timer := metrics.StartBusinessOperation(serviceName, operation, "http")
		
		defer func() {
			// 恢复可能的panic，确保defer能继续执行
			if r := recover(); r != nil {
				// 记录错误并减少并发数
				var err error
				if c.Writer.Status() >= 400 {
					if lastErr := c.Errors.Last(); lastErr != nil {
						err = lastErr
					}
				}
				timer.EndBusinessOperation(err)
				panic(r) // 重新抛出panic
			}
			
			var err error
			if c.Writer.Status() >= 400 {
				if lastErr := c.Errors.Last(); lastErr != nil {
					err = lastErr
				}
			}
			timer.EndBusinessOperation(err)
		}()
		
		c.Next()
	}
}

// 从上下文获取操作名称
func getOperationFromContext(c *gin.Context) string {
	// 使用方法+路径作为操作标识
	method := c.Request.Method
	path := c.FullPath()
	if path == "" {
		path = "unmatched_route"
	}

	// 简化的操作名称生成
	return method + "_" + path
}

// UserServiceMiddleware 用户服务专用中间件
func UserServiceMiddleware() gin.HandlerFunc {
	return BusinessMetricsMiddleware("user_service")
}

// ProductServiceMiddleware 产品服务专用中间件
func ProductServiceMiddleware() gin.HandlerFunc {
	return BusinessMetricsMiddleware("product_service")
}

// APIServiceMiddleware API服务中间件
func APIServiceMiddleware() gin.HandlerFunc {
	return BusinessMetricsMiddleware("api_service")
}
