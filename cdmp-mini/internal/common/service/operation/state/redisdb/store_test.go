package redisdb

import (
	"context"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/storage"
)

type fakeRedis struct {
	data map[string]string
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: make(map[string]string)}
}

func (f *fakeRedis) GetKey(_ context.Context, key string) (string, error) {
	if val, ok := f.data[key]; ok {
		return val, nil
	}
	return "", storage.ErrKeyNotFound
}

func (f *fakeRedis) SetKey(_ context.Context, key, value string, _ time.Duration) error {
	f.data[key] = value
	return nil
}

func TestStoreLifecycle(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	now := time.Date(2025, 11, 29, 10, 0, 0, 0, time.UTC)
	store := &Store{
		redis:     newFakeRedis(),
		db:        db,
		keyPrefix: "",
		ttl:       time.Hour,
		now:       func() time.Time { return now },
	}

	if err := store.ensureMigrated(); err != nil {
		t.Fatalf("ensureMigrated: %v", err)
	}

	env := &operation.OperationEnvelope{
		ID:       "op-123",
		Kind:     operation.OperationCreate,
		Resource: "users",
	}

	ctx := context.Background()
	if err := store.Upsert(ctx, env, operation.StateQueued); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	state, err := store.Get(ctx, env.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if state.State != operation.StateQueued {
		t.Fatalf("expected queued state, got %s", state.State)
	}

	if err := store.Advance(ctx, env.ID, operation.StateQueued, operation.StateExecuting); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	state, err = store.Get(ctx, env.ID)
	if err != nil {
		t.Fatalf("Get after advance: %v", err)
	}
	if state.State != operation.StateExecuting {
		t.Fatalf("expected executing state, got %s", state.State)
	}

	const reason = "temporary failure"
	if err := store.RecordFailure(ctx, env.ID, 2, reason); err != nil {
		t.Fatalf("RecordFailure: %v", err)
	}
	state, err = store.Get(ctx, env.ID)
	if err != nil {
		t.Fatalf("Get after failure: %v", err)
	}
	if state.State != operation.StateFailed {
		t.Fatalf("expected failed state, got %s", state.State)
	}
	if state.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", state.Attempts)
	}
	if state.LastError != reason {
		t.Fatalf("expected last error %q, got %q", reason, state.LastError)
	}
}
