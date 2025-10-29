package v1

import (
	"encoding/json"
	"strings"
	"time"

	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

// UserUpdateCommand enumerates the supported update execution modes for user messages.
type UserUpdateCommand string

const (
	// UserUpdateCommandFull represents the legacy full-field update behaviour.
	UserUpdateCommandFull UserUpdateCommand = "full"
	// UserUpdateCommandPatch represents a targeted update against a single user with field-level merges.
	UserUpdateCommandPatch UserUpdateCommand = "patch"
	// UserUpdateCommandBatch represents a condition-based batch update across multiple users.
	UserUpdateCommandBatch UserUpdateCommand = "batch"
)

// UserPatchSpec captures field-level mutations requested by patch style updates.
type UserPatchSpec struct {
	Status    *int               `json:"status,omitempty"`
	Nickname  *string            `json:"nickname,omitempty"`
	Email     *string            `json:"email,omitempty"`
	Phone     *string            `json:"phone,omitempty"`
	IsAdmin   *int               `json:"isAdmin,omitempty"`
	LoginedAt *time.Time         `json:"loginedAt,omitempty"`
	Password  *string            `json:"password,omitempty"`
	Extend    *ExtendPatchSpec   `json:"extend,omitempty"`
	Metadata  *MetadataPatchSpec `json:"metadata,omitempty"`
}

// ExtendPatchSpec describes how to mutate the JSON extend payload stored alongside the user record.
type ExtendPatchSpec struct {
	Merge   map[string]any `json:"merge,omitempty"`
	Replace map[string]any `json:"replace,omitempty"`
	Remove  []string       `json:"remove,omitempty"`
}

// MetadataPatchSpec restricts metadata mutations to allowed sub-sections such as extend.
type MetadataPatchSpec struct {
	Extend *ExtendPatchSpec `json:"extend,omitempty"`
}

// UserConditions transports the raw JSON predicates used for batch updates.
type UserConditions map[string]json.RawMessage

// Apply merges the patch specification into the target user in-place.
func (spec *UserPatchSpec) Apply(target *User) error {
	if spec == nil || target == nil {
		return nil
	}
	if spec.Status != nil {
		target.Status = *spec.Status
	}
	if spec.Nickname != nil {
		target.Nickname = strings.TrimSpace(*spec.Nickname)
	}
	if spec.Email != nil {
		target.Email = strings.TrimSpace(*spec.Email)
	}
	if spec.Phone != nil {
		target.Phone = strings.TrimSpace(*spec.Phone)
	}
	if spec.IsAdmin != nil {
		target.IsAdmin = *spec.IsAdmin
	}
	if spec.Password != nil {
		target.Password = *spec.Password
	}
	if spec.LoginedAt != nil {
		target.LoginedAt = spec.LoginedAt.UTC()
	}
	if spec.Extend != nil {
		if err := applyExtendPatch(&target.ObjectMeta, spec.Extend); err != nil {
			return err
		}
	}
	if spec.Metadata != nil && spec.Metadata.Extend != nil {
		if err := applyExtendPatch(&target.ObjectMeta, spec.Metadata.Extend); err != nil {
			return err
		}
	}
	return nil
}

// EnsureExtendShadow serialises the Extend map back into ExtendShadow.
func EnsureExtendShadow(meta *metav1.ObjectMeta) error {
	if meta == nil {
		return nil
	}
	ensureExtendMap(meta)
	raw, err := json.Marshal(meta.Extend)
	if err != nil {
		return err
	}
	meta.ExtendShadow = string(raw)
	return nil
}

func applyExtendPatch(meta *metav1.ObjectMeta, patch *ExtendPatchSpec) error {
	if meta == nil || patch == nil {
		return nil
	}
	extend := ensureExtendMap(meta)
	if patch.Remove != nil {
		for _, key := range patch.Remove {
			delete(extend, key)
		}
	}
	if patch.Replace != nil {
		for key, val := range patch.Replace {
			extend[key] = deepCopyValue(val)
		}
	}
	if patch.Merge != nil {
		mergeExtendMaps(extend, patch.Merge)
	}
	return nil
}

func ensureExtendMap(meta *metav1.ObjectMeta) metav1.Extend {
	if meta.Extend == nil {
		meta.Extend = make(metav1.Extend)
		shadow := strings.TrimSpace(meta.ExtendShadow)
		if shadow != "" {
			parsed := make(metav1.Extend)
			if err := json.Unmarshal([]byte(shadow), &parsed); err == nil {
				meta.Extend = parsed
			}
		}
	}
	if meta.Extend == nil {
		meta.Extend = make(metav1.Extend)
	}
	return meta.Extend
}

func mergeExtendMaps(dest metav1.Extend, src map[string]any) {
	if dest == nil {
		return
	}
	for key, val := range src {
		if existing, ok := dest[key]; ok {
			existingMap, okExisting := toStringInterfaceMap(existing)
			incomingMap, okIncoming := toStringInterfaceMap(val)
			if okExisting && okIncoming {
				mergeExtendMaps(existingMap, incomingMap)
				dest[key] = existingMap
				continue
			}
		}
		dest[key] = deepCopyValue(val)
	}
}

func deepCopyValue(val interface{}) interface{} {
	switch typed := val.(type) {
	case map[string]any:
		dup := make(map[string]any, len(typed))
		for k, v := range typed {
			dup[k] = deepCopyValue(v)
		}
		return dup
	case []any:
		dup := make([]any, len(typed))
		for i, v := range typed {
			dup[i] = deepCopyValue(v)
		}
		return dup
	default:
		return val
	}
}

func toStringInterfaceMap(val interface{}) (map[string]any, bool) {
	if typed, ok := val.(map[string]any); ok {
		return typed, true
	}
	return nil, false
}
