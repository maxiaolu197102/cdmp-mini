package usercache

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
)

var codec = jsoniter.ConfigFastest

// Payload represents the trimmed user data we persist in Redis.
type Payload struct {
	ID           uint64    `json:"id,omitempty"`
	InstanceID   string    `json:"instanceID,omitempty"`
	Name         string    `json:"name,omitempty"`
	Status       int       `json:"status,omitempty"`
	Nickname     string    `json:"nickname,omitempty"`
	Email        string    `json:"email,omitempty"`
	Phone        string    `json:"phone,omitempty"`
	IsAdmin      int       `json:"isAdmin,omitempty"`
	Password     string    `json:"password,omitempty"`
	TotalPolicy  int64     `json:"totalPolicy,omitempty"`
	Role         string    `json:"role,omitempty"`
	LoginedAt    time.Time `json:"loginedAt,omitempty"`
	CreatedAt    time.Time `json:"createdAt,omitempty"`
	UpdatedAt    time.Time `json:"updatedAt,omitempty"`
	Version      uint64    `json:"version,omitempty"`
	ExtendShadow string    `json:"extendShadow,omitempty"`
}

// FromUser constructs the payload from the full user object.
func FromUser(user *v1.User) Payload {
	if user == nil {
		return Payload{}
	}
	return Payload{
		ID:           user.ID,
		InstanceID:   user.InstanceID,
		Name:         user.Name,
		Status:       user.Status,
		Nickname:     user.Nickname,
		Email:        user.Email,
		Phone:        user.Phone,
		IsAdmin:      user.IsAdmin,
		Password:     user.Password,
		TotalPolicy:  user.TotalPolicy,
		Role:         user.Role,
		LoginedAt:    user.LoginedAt,
		CreatedAt:    user.CreatedAt,
		UpdatedAt:    user.UpdatedAt,
		Version:      user.ObjectMeta.Version,
		ExtendShadow: user.ExtendShadow,
	}
}

// ToUser reconstructs a v1.User from the payload.
func (p Payload) ToUser() *v1.User {
	if p.Name == "" {
		return nil
	}
	return &v1.User{
		ObjectMeta: metav1.ObjectMeta{
			ID:           p.ID,
			InstanceID:   p.InstanceID,
			Name:         p.Name,
			CreatedAt:    p.CreatedAt,
			UpdatedAt:    p.UpdatedAt,
			Version:      p.Version,
			ExtendShadow: p.ExtendShadow,
		},
		Status:      p.Status,
		Nickname:    p.Nickname,
		Email:       p.Email,
		Phone:       p.Phone,
		IsAdmin:     p.IsAdmin,
		Password:    p.Password,
		TotalPolicy: p.TotalPolicy,
		Role:        p.Role,
		LoginedAt:   p.LoginedAt,
	}
}

// Marshal encodes the provided user model into the trimmed cache representation.
func Marshal(user *v1.User) ([]byte, error) {
	cached := FromUser(user)
	return codec.Marshal(&cached)
}

// Unmarshal decodes the cache payload back into a full user model.
func Unmarshal(data []byte) (*v1.User, error) {
	var cached Payload
	if err := codec.Unmarshal(data, &cached); err != nil {
		return nil, err
	}
	return cached.ToUser(), nil
}
