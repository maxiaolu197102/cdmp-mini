package server

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"

	authkeys "github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/auth/keys"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

// cleanupUserSessionsLua mirrors the Lua script used by the change-password flow to purge refresh tokens.
const cleanupUserSessionsLua = `
    -- KEYS[1]: 用户会话集合完整键名
    -- ARGV[1]: Refresh Token 键前缀（含哈希标签，以冒号结尾）

    local userSessionsKey = KEYS[1]
    local refreshTokenPrefix = ARGV[1]

    redis.log(redis.LOG_NOTICE, "开始清理用户会话: " .. userSessionsKey)

    local tokens = redis.call('SMEMBERS', userSessionsKey)
    redis.log(redis.LOG_NOTICE, "找到 " .. #tokens .. " 个refresh token")

    for _, token in ipairs(tokens) do
        local rtKey = refreshTokenPrefix .. token
        redis.call('DEL', rtKey)
        redis.log(redis.LOG_NOTICE, "已删除refresh token: " .. rtKey)
    end

    local delResult = redis.call('DEL', userSessionsKey)
    redis.log(redis.LOG_NOTICE, "删除用户会话集合结果: " .. delResult)

    return #tokens
`

// cleanupUserSessions removes all refresh-token state associated with the provided user ID.
func cleanupUserSessions(ctx context.Context, redisClient *storage.RedisCluster, userID uint64) error {
	if redisClient == nil || userID == 0 {
		return nil
	}

	userIDStr := strconv.FormatUint(userID, 10)
	userSessionsKey := authkeys.UserSessionsKey(userIDStr)
	refreshPrefix := authkeys.WithGenericPrefix(authkeys.RefreshTokenPrefix(userIDStr))

	result, err := redisClient.Eval(ctx, cleanupUserSessionsLua,
		[]string{userSessionsKey},
		[]interface{}{refreshPrefix},
	)
	if err != nil {
		if err == redis.Nil {
			log.Debugf("用户%s没有活跃会话", userIDStr)
			return nil
		}
		return err
	}

	if result != nil {
		if cleaned, ok := result.(int64); ok {
			log.Debugf("用户%s清理refresh token数量:%d", userIDStr, cleaned)
		}
	}

	return nil
}
