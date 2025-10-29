package common

import (
    "net/http"

    "github.com/gin-gonic/gin"
)

// LagProtectionFunc returns true when the system is currently in lag protection mode
type LagProtectionFunc func() bool

// LagProtectMiddleware returns a gin middleware that will reject write requests (429)
// when the provided lagProtection function reports true.
func LagProtectMiddleware(isProtected LagProtectionFunc) gin.HandlerFunc {
    return func(c *gin.Context) {
        if isProtected != nil && isProtected() {
            c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
                "code": 429,
                "message": "system under backpressure due to consumer lag, please retry later",
            })
            return
        }
        c.Next()
    }
}
