package server

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type credentialEntry struct {
	result  bool
	expires time.Time
	touched time.Time
}

type credentialCache struct {
	mu         sync.RWMutex
	entries    map[string]credentialEntry
	ttl        time.Duration
	maxEntries int
}

func newCredentialCache(ttl time.Duration, maxEntries int) *credentialCache {
	if ttl <= 0 || maxEntries <= 0 {
		return nil
	}
	return &credentialCache{
		entries:    make(map[string]credentialEntry, maxEntries),
		ttl:        ttl,
		maxEntries: maxEntries,
	}
}

func (c *credentialCache) lookup(username, hashedPassword, plainPassword string) (bool, bool) {
	if c == nil {
		return false, false
	}
	key := cacheKey(username, hashedPassword, plainPassword)
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return false, false
	}
	if time.Now().After(entry.expires) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return false, false
	}
	return entry.result, true
}

func (c *credentialCache) store(username, hashedPassword, plainPassword string, result bool) {
	if c == nil {
		return
	}
	key := cacheKey(username, hashedPassword, plainPassword)
	now := time.Now()
	entry := credentialEntry{
		result:  result,
		expires: now.Add(c.ttl),
		touched: now,
	}

	c.mu.Lock()
	if len(c.entries) >= c.maxEntries {
		c.evictLocked()
	}
	c.entries[key] = entry
	c.mu.Unlock()
}

func (c *credentialCache) evictLocked() {
	if len(c.entries) == 0 {
		return
	}
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, v := range c.entries {
		if first || v.touched.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.touched
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

func cacheKey(username, hashedPassword, plainPassword string) string {
	sum := sha256.Sum256([]byte(plainPassword))
	return username + "|" + hashedPassword + "|" + hex.EncodeToString(sum[:])
}
