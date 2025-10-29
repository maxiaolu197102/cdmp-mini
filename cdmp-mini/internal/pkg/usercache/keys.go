package usercache

import "strings"

const (
	userPrefix            = "user:"
	emailPrefix           = "user:email:"
	phonePrefix           = "user:phone:"
	negativeCounterPrefix = "user:negative-counter:"
	blockCounterPrefix    = "user:block-counter:"
	blacklistPrefix       = "user:blacklist:"
	pendingCreatePrefix   = "user:pending:"
	NegativeCacheSentinel = "rate_limit_prevention"
	BlacklistSentinel     = "rate_limit_blacklisted"
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
	trimmed := strings.TrimSpace(username)
	if trimmed == "" {
		return ""
	}
	return pendingCreatePrefix + trimmed
}
