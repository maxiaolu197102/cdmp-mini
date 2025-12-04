package storage

import (
	redis "github.com/redis/go-redis/v9"
)

// SetRedisSingletonForTest injects a redis client into the storage singleton used by RedisCluster.
func SetRedisSingletonForTest(client redis.UniversalClient, cache bool) {
	storeSingleton(cache, client)
}

// SetRedisConnectedForTest overrides the global connectivity flag used by RedisCluster.Up.
func SetRedisConnectedForTest(up bool) {
	redisUp.Store(up)
	if up {
		disableRedis.Store(false)
	}
}

// ResetRedisForTest clears injected redis state after a test completes.
func ResetRedisForTest() {
	storeSingleton(false, nil)
	storeSingleton(true, nil)
	redisUp.Store(false)
	disableRedis.Store(false)
}
