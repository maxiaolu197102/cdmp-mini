package store

import "context"

type contextKey string

const forcePrimaryKey contextKey = "store.forcePrimary"

// WithForcePrimary marks the request context as requiring primary-only reads.
func WithForcePrimary(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, forcePrimaryKey, true)
}

// ForcePrimaryFromContext checks whether primary-only reads were requested.
func ForcePrimaryFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, ok := ctx.Value(forcePrimaryKey).(bool)
	return ok && value
}
