package delete_force

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func TestOperationModeDeleteForceFlows(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "delete_modes")
	defer recorder.Flush(t)

	original, err := env.GetUserOperationMode()
	if err != nil {
		t.Fatalf("fetch operation mode: %v", err)
	}
	queueKinds := []string{"create", "update", "delete", "batch"}

	t.Cleanup(func() {
		if _, restoreErr := env.SetUserOperationMode(original); restoreErr != nil {
			t.Logf("restore operation mode: %v", restoreErr)
		}
	})

	const basePassword = "InitPassw0rd!"

	cases := []struct {
		name string
		cfg  framework.OperationModeConfig
	}{
		{
			name: "sync",
			cfg: framework.OperationModeConfig{
				Mode:           "sync",
				RolloutPercent: 0,
				StickyHeader:   original.StickyHeader,
				QueueKinds:     queueKinds,
				AllowUsers:     nil,
				BlockUsers:     nil,
			},
		},
		{
			name: "queue",
			cfg: framework.OperationModeConfig{
				Mode:           "queue",
				RolloutPercent: 100,
				StickyHeader:   original.StickyHeader,
				QueueKinds:     queueKinds,
				AllowUsers:     nil,
				BlockUsers:     nil,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			spec := env.NewUserSpec("mode_delete_"+tc.name+"_", basePassword)
			env.CreateUserAndWait(t, spec, 15*time.Second)
			cleanupName := spec.Name
			defer func() {
				if cleanupName != "" {
					env.ForceDeleteUserIgnore(cleanupName)
				}
			}()

			applied, err := env.SetUserOperationMode(tc.cfg)
			if err != nil {
				t.Fatalf("set operation mode %s: %v", tc.name, err)
			}
			if applied.Mode != tc.cfg.Mode {
				t.Fatalf("operation mode mismatch: want %s got %s", tc.cfg.Mode, applied.Mode)
			}

			start := time.Now()
			checks := map[string]bool{}
			var resp *framework.APIResponse
			var reqErr error
			defer func() {
				success := reqErr == nil && resp != nil && (resp.HTTPStatus() == http.StatusOK || resp.HTTPStatus() == http.StatusNoContent) && resp.Code == code.ErrSuccess && !t.Failed()
				recordCase(recorder, fmt.Sprintf("delete_mode_%s", tc.name), fmt.Sprintf("force delete in %s mode", tc.cfg.Mode), resp, time.Since(start), success, checks)
			}()

			resp, reqErr = env.ForceDeleteUser(spec.Name)
			if reqErr != nil {
				t.Fatalf("force delete request %s: %v", tc.name, reqErr)
			}
			if resp.HTTPStatus() != http.StatusOK && resp.HTTPStatus() != http.StatusNoContent {
				t.Fatalf("unexpected status: %d", resp.HTTPStatus())
			}
			if resp.Code != code.ErrSuccess {
				t.Fatalf("unexpected business code: %d message=%s", resp.Code, resp.Message)
			}
			checks["response"] = true

			if err := waitForUserGone(env, spec.Name, 30*time.Second); err != nil {
				t.Fatalf("wait for user deletion: %v", err)
			}
			checks["user_removed"] = true
			cleanupName = ""
		})
	}
}
