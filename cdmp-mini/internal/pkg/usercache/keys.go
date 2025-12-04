package usercache

import (
	"fmt"
	"strings"
)

// Redis 缓存相关的键前缀与哨兵标记常量。
const (
	// userPrefix 用户主缓存前缀
	// 完整键格式: user:{username}
	// 存储结构: String，值为序列化的用户资料JSON
	// 用途: 提供按用户名的正向缓存，加速读取与一致性校验
	// 存储位置: Redis
	userPrefix = "user:"
	// emailPrefix 用户邮箱索引前缀
	// 完整键格式: user:email:{email}
	// 存储结构: String，值为对应的用户名
	// 用途: 通过邮箱快速查找用户名，用于登录与邮箱校验
	// 存储位置: Redis
	emailPrefix = "user:email:"
	// phonePrefix 用户手机号索引前缀
	// 完整键格式: user:phone:{phone}
	// 存储结构: String，值为对应的用户名
	// 用途: 通过手机号快速查找用户名，支撑手机号唯一校验
	// 存储位置: Redis
	phonePrefix = "user:phone:"
	// negativeCounterPrefix 负缓存计数器前缀
	// 完整键格式: user:negative-counter:{username}
	// 存储结构: String(Int)，值为递增的负缓存命中次数
	// 用途: 统计用户未命中频率，用于触发负缓存或黑名单保护
	// 存储位置: Redis
	negativeCounterPrefix = "user:negative-counter:"
	// blockCounterPrefix 黑名单预警计数器前缀
	// 完整键格式: user:block-counter:{username}
	// 存储结构: String(Int)，值为递增的风险计数
	// 用途: 统计写操作风险次数，判定是否需要加入黑名单
	// 存储位置: Redis
	blockCounterPrefix = "user:block-counter:"
	// blacklistPrefix 黑名单标记前缀
	// 完整键格式: user:blacklist:{username}
	// 存储结构: String，值为黑名单哨兵
	// 用途: 标记用户已进入黑名单，拦截后续登录或写操作
	// 存储位置: Redis
	blacklistPrefix = "user:blacklist:"
	// pendingCreatePrefix 创建幂等租约前缀
	// 完整键格式: user:pending:{username}
	// 存储结构: String(JSON)，值为 pendingLeaseSnapshot
	// 用途: 记录用户创建流程的租约信息，保障幂等与背压控制
	// 存储位置: Redis
	pendingCreatePrefix = "user:pending:"
	// pendingUserDepthPrefix 用户局部队列深度计数前缀
	// 完整键格式: user:pending:depth:{username}
	// 存储结构: String(Int)，记录用户级活跃租约数量
	// 用途: 捕获局部热点，控制单用户背压
	// 存储位置: Redis
	pendingUserDepthPrefix = "user:pending:depth:"
	// NegativeCacheSentinel 负缓存哨兵值
	// 完整键格式: 作为 user:{username} 键的值存储
	// 存储结构: String，特殊用户名占位符
	// 用途: 标记用户近期不存在并触发限流保护
	// 存储位置: Redis
	NegativeCacheSentinel = "rate_limit_prevention"
	// BlacklistSentinel 黑名单哨兵值
	// 完整键格式: 作为 user:{username} 或 user:blacklist:{username} 键的值存储
	// 存储结构: String，特殊用户名占位符
	// 用途: 表示用户已被强制拉黑，应拒绝读写请求
	// 存储位置: Redis
	BlacklistSentinel = "rate_limit_blacklisted"
)

// UserKey returns the cache key used for storing user payloads by username.
func UserKey(username string) string {
	return userPrefix + username
}

// NormalizeEmail lower cases and trims the email before using it as a cache key component.
func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// NormalizePhone trims the phone value before using it as a cache key component.
func NormalizePhone(phone string) string {
	return strings.TrimSpace(phone)
}

// EmailKey returns the cache key mapping email -> username.
func EmailKey(email string) string {
	normalized := NormalizeEmail(email)
	if normalized == "" {
		return ""
	}
	return emailPrefix + normalized
}

// PhoneKey returns the cache key mapping phone -> username.
func PhoneKey(phone string) string {
	normalized := NormalizePhone(phone)
	if normalized == "" {
		return ""
	}
	return phonePrefix + normalized
}

// NegativeCounterKey returns the redis key for tracking negative cache hits.
func NegativeCounterKey(username string) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	return negativeCounterPrefix + trimmed
}

// BlockCounterKey returns the redis key for tracking blacklist thresholds.
func BlockCounterKey(username string) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	return blockCounterPrefix + trimmed
}

// BlacklistKey returns the redis key used to mark username as blocked.
func BlacklistKey(username string) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	return blacklistPrefix + trimmed
}

// PendingCreateKey 返回记录用户创建幂等状态的 key。
func PendingCreateKey(username string) string {
	return userScopedKey(pendingCreatePrefix, username)
}

// PendingCreatePrefix 返回 pending 租约Key的前缀，便于扫描。
func PendingCreatePrefix() string {
	return pendingCreatePrefix
}

// PendingUserDepthKey 返回用户级队列深度的计数Key。
func PendingUserDepthKey(username string) string {
	return userScopedKey(pendingUserDepthPrefix, username)
}

// PendingUserDepthPrefix 返回用户级队列深度计数的前缀。
func PendingUserDepthPrefix() string {
	return pendingUserDepthPrefix
}

func userScopedKey(prefix, username string) string {
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	normalizedPrefix := normalizeKeyPrefix(prefix)
	if normalizedPrefix == "" {
		return fmt.Sprintf("%s:%s", userHashTag(trimmed), trimmed)
	}
	return fmt.Sprintf("%s%s:%s", normalizedPrefix, userHashTag(trimmed), trimmed)
}

func normalizeKeyPrefix(prefix string) string {
	normalized := strings.TrimSpace(prefix)
	if normalized == "" {
		return ""
	}
	if !strings.HasSuffix(normalized, ":") {
		normalized += ":"
	}
	return normalized
}

const pendingHashTag = "{pending}"

func userHashTag(username string) string {
	return pendingHashTag
}

func usernameFromTaggedKey(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "{user:") {
		if end := strings.Index(trimmed, "}"); end >= 0 {
			remainder := strings.TrimSpace(trimmed[end+1:])
			remainder = strings.TrimPrefix(remainder, ":")
			remainder = strings.TrimSpace(remainder)
			if remainder != "" {
				return remainder
			}
			inside := strings.TrimSpace(trimmed[len("{user:"):end])
			if inside != "" {
				return inside
			}
		}
	}
	if idx := strings.Index(trimmed, ":"); idx >= 0 {
		candidate := strings.TrimSpace(trimmed[idx+1:])
		if candidate != "" {
			return candidate
		}
	}
	return trimmed
}

func usernameFromKeyWithPrefix(key, prefix string) string {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return ""
	}
	normalizedPrefix := normalizeKeyPrefix(prefix)
	if normalizedPrefix != "" && strings.HasPrefix(trimmedKey, normalizedPrefix) {
		trimmedKey = strings.TrimSpace(strings.TrimPrefix(trimmedKey, normalizedPrefix))
	}
	return usernameFromTaggedKey(trimmedKey)
}
