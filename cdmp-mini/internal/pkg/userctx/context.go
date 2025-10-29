package userctx

import "context"

type contextKey string

const createStateKey contextKey = "cdmp.create.state"

// createState keeps mutable flags for a single create request.
type createState struct {
	degraded bool
}

// WithCreateState ensures the create request state holder exists on the context chain.
func WithCreateState(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(createStateKey).(*createState); ok {
		return ctx
	}
	return context.WithValue(ctx, createStateKey, &createState{})
}

// getCreateState returns the state struct if present.
func getCreateState(ctx context.Context) *createState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(createStateKey).(*createState)
	return state
}

// MarkCreateDegraded toggles the degraded flag; returns true when it flips from false to true.
func MarkCreateDegraded(ctx context.Context) bool {
	state := getCreateState(ctx)
	if state == nil {
		return false
	}
	if state.degraded {
		return false
	}
	state.degraded = true
	return true
}

// IsCreateDegraded reports whether the create flow is currently degraded.
func IsCreateDegraded(ctx context.Context) bool {
	state := getCreateState(ctx)
	return state != nil && state.degraded
}
