package server

import (
	"time"

	"github.com/google/uuid"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
)

type CreateUserMessage struct {
	RequestID string        `json:"request_id"`
	User      *v1.User      `json:"user"`
	Opts      CreateOptions `json:"opts"`
	CreatedAt time.Time     `json:"created_at"`
	SourceIP  string        `json:"source_ip"`
	UserAgent string        `json:"user_agent"`
}

type CreateOptions struct {
	Timeout time.Duration `json:"timeout"`
}

type CreateUserResponse struct {
	RequestID string    `json:"request_id"`
	Success   bool      `json:"success"`
	Error     string    `json:"error,omitempty"`
	UserID    uint64    `json:"user_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func NewCreateUserMessage(user *v1.User, opts CreateOptions, sourceIP, userAgent string) *CreateUserMessage {
	return &CreateUserMessage{
		RequestID: uuid.New().String(),
		User:      user,
		Opts:      opts,
		CreatedAt: time.Now(),
		SourceIP:  sourceIP,
		UserAgent: userAgent,
	}
}
