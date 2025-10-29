package server

import (
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/usercache"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
)

func buildContactCacheItems(user *v1.User) []storage.KeyValueTTL {
	if user == nil {
		return nil
	}
	var items []storage.KeyValueTTL
	if key := usercache.EmailKey(user.Email); key != "" {
		items = append(items, storage.KeyValueTTL{Key: key, Value: user.Name, TTL: 24 * time.Hour})
	}
	if key := usercache.PhoneKey(user.Phone); key != "" {
		items = append(items, storage.KeyValueTTL{Key: key, Value: user.Name, TTL: 24 * time.Hour})
	}
	return items
}
