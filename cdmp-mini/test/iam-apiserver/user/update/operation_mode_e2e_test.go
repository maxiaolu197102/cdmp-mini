package update

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func TestOperationModeUpdateFlows(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "update_modes")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping operation mode update flows")
	}

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
		name     string
		cfg      framework.OperationModeConfig
		nickname string
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
			nickname: "mode_sync_update",
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
			nickname: "mode_queue_update",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			spec := env.NewUserSpec("mode_update_"+tc.name+"_", basePassword)
			env.CreateUserAndWait(t, spec, 15*time.Second)
			defer env.ForceDeleteUserIgnore(spec.Name)

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
				recorder.AddCase(framework.CaseResult{
					Name:        fmt.Sprintf("update_mode_%s", tc.name),
					Description: fmt.Sprintf("update user in %s mode", tc.cfg.Mode),
					Success:     reqErr == nil && resp != nil && resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && !t.Failed(),
					HTTPStatus:  statusOrZero(resp),
					Code:        codeOrZero(resp),
					Message:     messageOrEmpty(resp),
					DurationMS:  time.Since(start).Milliseconds(),
					Checks:      checks,
					Notes:       []string{fmt.Sprintf("nickname=%s", tc.nickname)},
				})
			}()

			payload := map[string]any{
				"metadata": map[string]any{"name": spec.Name},
				"nickname": tc.nickname,
				"version":  1,
			}

			resp, reqErr = env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), payload)
			if reqErr != nil {
				t.Fatalf("update request %s: %v", tc.name, reqErr)
			}
			if resp.HTTPStatus() != http.StatusOK {
				t.Fatalf("unexpected status: %d", resp.HTTPStatus())
			}
			if resp.Code != code.ErrSuccess {
				t.Fatalf("unexpected business code: %d message=%s", resp.Code, resp.Message)
			}
			checks["response"] = true

			user, unsupported := waitForPublicUser(t, env, spec.Name, 30*time.Second, func(u *publicUser) bool {
				return u != nil && u.Username == spec.Name && u.Nickname == tc.nickname
			})
			if unsupported {
				t.Skip("backend missing version column; skipping update verification")
			}
			if user == nil {
				t.Fatalf("nickname not updated within timeout")
			}
			checks["state_applied"] = true
		})
	}
}

func TestOperationModeBatchPatchFlows(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "batch_modes")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping operation mode batch flows")
	}

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
		name   string
		cfg    framework.OperationModeConfig
		marker string
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
			marker: "batch_sync",
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
			marker: "batch_queue",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			users := make([]framework.UserSpec, 3)
			for i := range users {
				users[i] = env.NewUserSpec("mode_batch_"+tc.name+"_", basePassword)
				env.CreateUserAndWait(t, users[i], 15*time.Second)
				defer env.ForceDeleteUserIgnore(users[i].Name)
			}

			applied, err := env.SetUserOperationMode(tc.cfg)
			if err != nil {
				t.Fatalf("set operation mode %s: %v", tc.name, err)
			}
			if applied.Mode != tc.cfg.Mode {
				t.Fatalf("operation mode mismatch: want %s got %s", tc.cfg.Mode, applied.Mode)
			}

			target := []string{users[0].Name, users[1].Name}
			payload := map[string]any{
				"updates": map[string]any{
					"nickname": tc.marker,
				},
				"conditions": map[string]any{
					"name": map[string]any{
						"in": target,
					},
				},
			}

			start := time.Now()
			checks := map[string]bool{}
			var resp *framework.APIResponse
			var reqErr error
			defer func() {
				recorder.AddCase(framework.CaseResult{
					Name:        fmt.Sprintf("batch_mode_%s", tc.name),
					Description: fmt.Sprintf("batch patch in %s mode", tc.cfg.Mode),
					Success:     reqErr == nil && resp != nil && resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && !t.Failed(),
					HTTPStatus:  statusOrZero(resp),
					Code:        codeOrZero(resp),
					Message:     messageOrEmpty(resp),
					DurationMS:  time.Since(start).Milliseconds(),
					Checks:      checks,
					Notes:       []string{fmt.Sprintf("marker=%s", tc.marker)},
				})
			}()

			resp, reqErr = env.AdminRequest(http.MethodPatch, "/api/users", payload)
			if reqErr != nil {
				t.Fatalf("batch patch request %s: %v", tc.name, reqErr)
			}
			if resp.HTTPStatus() != http.StatusOK {
				t.Fatalf("unexpected status: %d", resp.HTTPStatus())
			}
			if resp.Code != code.ErrSuccess {
				t.Fatalf("unexpected business code: %d message=%s", resp.Code, resp.Message)
			}
			checks["response"] = true

			for i, spec := range users {
				s := spec
				expected := spec.Nickname
				if i < 2 {
					expected = tc.marker
				}
				user, unsupported := waitForPublicUser(t, env, s.Name, 30*time.Second, func(u *publicUser) bool {
					if u == nil || u.Username != s.Name {
						return false
					}
					return u.Nickname == expected
				})
				if unsupported {
					t.Skip("backend missing version column; skipping batch verification")
				}
				if user == nil {
					t.Fatalf("batch update not observed for %s", s.Name)
				}
			}
			checks["state_applied"] = true
		})
	}
}

func statusOrZero(resp *framework.APIResponse) int {
	if resp == nil {
		return 0
	}
	return resp.HTTPStatus()
}

func codeOrZero(resp *framework.APIResponse) int {
	if resp == nil {
		return 0
	}
	return resp.Code
}

func messageOrEmpty(resp *framework.APIResponse) string {
	if resp == nil {
		return ""
	}
	return resp.Message
}
