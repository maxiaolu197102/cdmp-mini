package user

import (
	"context"
	"testing"

	operationpkg "github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
)

func TestOperationModeDefaultQueueKinds(t *testing.T) {
	controller := newOperationModeController(defaultOperationModeConfig())
	cases := map[operationpkg.OperationKind]OperationMode{
		operationpkg.OperationCreate: controller.Decide(context.Background(), operationpkg.OperationCreate, "alice"),
		operationpkg.OperationUpdate: controller.Decide(context.Background(), operationpkg.OperationUpdate, "alice"),
		operationpkg.OperationDelete: controller.Decide(context.Background(), operationpkg.OperationDelete, "alice"),
		operationpkg.OperationBatch:  controller.Decide(context.Background(), operationpkg.OperationBatch, "alice"),
	}
	for kind, mode := range cases {
		if mode != OperationModeQueue {
			t.Fatalf("expected kind %s to default to queue mode, got %s", kind, mode)
		}
	}
}

func TestOperationModeSyncGlobal(t *testing.T) {
	controller := newOperationModeController(OperationModeConfig{Mode: OperationModeSync})
	for _, kind := range []operationpkg.OperationKind{operationpkg.OperationCreate, operationpkg.OperationUpdate, operationpkg.OperationDelete, operationpkg.OperationBatch} {
		if mode := controller.Decide(context.Background(), kind, "bob"); mode != OperationModeSync {
			t.Fatalf("expected sync mode for kind %s, got %s", kind, mode)
		}
	}
}

func TestOperationModeAllowAndBlockLists(t *testing.T) {
	cfg := OperationModeConfig{
		Mode:       OperationModeQueue,
		AllowUsers: []string{"vip-user"},
		BlockUsers: []string{"legacy"},
	}
	controller := newOperationModeController(cfg)

	if mode := controller.Decide(context.Background(), operationpkg.OperationUpdate, "VIP-USER"); mode != OperationModeQueue {
		t.Fatalf("allowlist should force queue mode, got %s", mode)
	}
	if mode := controller.Decide(context.Background(), operationpkg.OperationDelete, "legacy"); mode != OperationModeSync {
		t.Fatalf("blocklist should force sync mode, got %s", mode)
	}
}

func TestOperationModeQueueKindsOverride(t *testing.T) {
	cfg := OperationModeConfig{
		Mode:       OperationModeQueue,
		QueueKinds: []string{string(operationpkg.OperationCreate)},
	}
	controller := newOperationModeController(cfg)

	if mode := controller.Decide(context.Background(), operationpkg.OperationCreate, "alice"); mode != OperationModeQueue {
		t.Fatalf("create should remain queued, got %s", mode)
	}
	if mode := controller.Decide(context.Background(), operationpkg.OperationUpdate, "alice"); mode != OperationModeSync {
		t.Fatalf("update not in queueKinds should fall back to sync, got %s", mode)
	}
}
