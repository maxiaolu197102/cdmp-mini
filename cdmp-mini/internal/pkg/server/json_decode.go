package server

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

// decodeUserMessage unmarshals a user payload into a pre-allocated v1.User without reflection-based struct creation.
func decodeUserMessage(data []byte, dst *v1.User) error {
	if dst == nil {
		return errors.New("decodeUserMessage: nil destination")
	}

	resetUser(dst)

	iter := jsonCodec.BorrowIterator(data)
	defer jsonCodec.ReturnIterator(iter)

	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "metadata":
			if iter.WhatIsNext() == jsoniter.ObjectValue {
				if err := decodeObjectMeta(iter, &dst.ObjectMeta); err != nil {
					return err
				}
			} else {
				iter.Skip()
			}
		case "status":
			dst.Status = iter.ReadInt()
		case "nickname":
			dst.Nickname = iter.ReadString()
		case "password":
			dst.Password = iter.ReadString()
		case "email":
			dst.Email = iter.ReadString()
		case "phone":
			dst.Phone = iter.ReadString()
		case "isAdmin":
			dst.IsAdmin = readIntOrBool(iter)
		case "totalPolicy":
			dst.TotalPolicy = iter.ReadInt64()
		case "role":
			dst.Role = iter.ReadString()
		case "loginedAt":
			dst.LoginedAt = parseTimestamp(iter)
		case "command":
			dst.Command = v1.UserUpdateCommand(iter.ReadString())
		case "expectedVersion":
			if iter.WhatIsNext() == jsoniter.NilValue {
				iter.ReadNil()
				dst.ExpectedVersion = nil
			} else {
				val := iter.ReadUint64()
				dst.ExpectedVersion = &val
			}
		case "conditions":
			raw := iter.SkipAndReturnBytes()
			if len(raw) > 0 {
				conditions := make(v1.UserConditions)
				if err := jsonCodec.Unmarshal(raw, &conditions); err != nil {
					return err
				}
				dst.Conditions = conditions
			}
		case "updates":
			raw := iter.SkipAndReturnBytes()
			if len(raw) > 0 {
				patch := &v1.UserPatchSpec{}
				if err := jsonCodec.Unmarshal(raw, patch); err != nil {
					return err
				}
				dst.Patch = patch
			}
		case "extend":
			raw := iter.SkipAndReturnBytes()
			if err := applyExtend(raw, &dst.ObjectMeta); err != nil {
				return err
			}
		case "extendShadow":
			dst.ObjectMeta.ExtendShadow = strings.TrimSpace(iter.ReadString())
		default:
			iter.Skip()
		}
	}

	return iter.Error
}

func resetUser(u *v1.User) {
	if u == nil {
		return
	}
	*u = v1.User{}
}

func decodeObjectMeta(iter *jsoniter.Iterator, meta *metav1.ObjectMeta) error {
	if meta == nil {
		return nil
	}
	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "id":
			meta.ID = iter.ReadUint64()
		case "instanceID":
			meta.InstanceID = iter.ReadString()
		case "name":
			meta.Name = iter.ReadString()
		case "extend":
			raw := iter.SkipAndReturnBytes()
			if err := applyExtend(raw, meta); err != nil {
				return err
			}
		case "extendShadow":
			meta.ExtendShadow = strings.TrimSpace(iter.ReadString())
		case "createdAt":
			meta.CreatedAt = parseTimestamp(iter)
		case "updatedAt":
			meta.UpdatedAt = parseTimestamp(iter)
		case "version":
			meta.Version = iter.ReadUint64()
		default:
			iter.Skip()
		}
	}
	return iter.Error
}

func applyExtend(raw []byte, meta *metav1.ObjectMeta) error {
	if meta == nil {
		return nil
	}
	cleaned := bytes.TrimSpace(raw)
	if len(cleaned) == 0 || bytes.Equal(cleaned, []byte("null")) {
		meta.Extend = nil
		meta.ExtendShadow = ""
		return nil
	}
	meta.ExtendShadow = string(cleaned)
	ext := make(metav1.Extend)
	if err := jsonCodec.Unmarshal(cleaned, &ext); err != nil {
		return err
	}
	meta.Extend = ext
	return nil
}

func parseTimestamp(iter *jsoniter.Iterator) time.Time {
	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		return time.Time{}
	case jsoniter.StringValue:
		raw := strings.TrimSpace(iter.ReadString())
		if raw == "" {
			return time.Time{}
		}
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t
		}
		return time.Time{}
	case jsoniter.NumberValue:
		val := iter.ReadFloat64()
		if val == 0 {
			return time.Time{}
		}
		sec, frac := math.Modf(val)
		nsec := int64(frac * 1e9)
		return time.Unix(int64(sec), nsec).UTC()
	default:
		iter.Skip()
		return time.Time{}
	}
}

func readIntOrBool(iter *jsoniter.Iterator) int {
	switch iter.WhatIsNext() {
	case jsoniter.BoolValue:
		if iter.ReadBool() {
			return 1
		}
		return 0
	default:
		return iter.ReadInt()
	}
}
