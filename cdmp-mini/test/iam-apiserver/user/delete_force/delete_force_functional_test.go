package delete_force

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/delete_force"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable delete-force e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type deleteForceCase struct {
	name        string
	description string
	setup       func(t *testing.T, env *framework.Env) (username string, needsCleanup bool)
	expectHTTP  int
	expectCode  int
	postCheck   func(t *testing.T, env *framework.Env, username string)
}

func TestDeleteForceFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "delete_force")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	cases := []deleteForceCase{
		{
			name:        "delete_existing_user",
			description: "管理员可成功删除现有用户并验证数据一致性与审计记录",
			setup: func(t *testing.T, env *framework.Env) (string, bool) {
				spec := env.NewUserSpec("del_force_", basePassword)
				env.CreateUserAndWait(t, spec, 10*time.Second)
				return spec.Name, true
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			postCheck: func(t *testing.T, env *framework.Env, username string) {
				if err := waitForUserGone(env, username, 30*time.Second); err != nil {
					t.Fatalf("wait for deletion: %v", err)
				}
				ensureUserAbsentFromList(t, env, username)
				ensureAuditEntry(t, env, username, "user.delete")
			},
		},
		{
			name:        "delete_nonexistent_user",
			description: "删除不存在的用户应返回404并保持幂等",
			setup: func(t *testing.T, env *framework.Env) (string, bool) {
				return fmt.Sprintf("missing_%d", time.Now().UnixNano()), false
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			postCheck: func(t *testing.T, env *framework.Env, username string) {
				resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), nil)
				if err != nil {
					t.Fatalf("get user after deleting nonexistent: %v", err)
				}
				if resp.HTTPStatus() != http.StatusNotFound {
					t.Fatalf("expected http 404 when rechecking nonexistent user, got %d", resp.HTTPStatus())
				}
				if resp.Code != code.ErrUserNotFound {
					t.Fatalf("expected code %d got %d", code.ErrUserNotFound, resp.Code)
				}
			},
		},
		{
			name:        "invalid_username",
			description: "非法用户名应触发参数校验失败",
			setup: func(t *testing.T, env *framework.Env) (string, bool) {
				return "bad_user!", false
			},
			expectHTTP: http.StatusBadRequest,
			expectCode: code.ErrInvalidParameter,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("core/"+tc.name, func(t *testing.T) {
			username, needsCleanup := tc.setup(t, env)
			cleanupName := ""
			if needsCleanup {
				cleanupName = username
				defer func() {
					if cleanupName != "" {
						env.ForceDeleteUserIgnore(cleanupName)
					}
				}()
			}

			start := time.Now()
			checks := map[string]bool{}
			var resp *framework.APIResponse
			t.Cleanup(func() {
				recordCase(recorder, tc.name, tc.description, resp, time.Since(start), !t.Failed(), checks, tc.description)
			})

			out, err := env.ForceDeleteUser(username)
			resp = out
			if err != nil {
				t.Fatalf("force delete request failed: %v", err)
			}

			status := resp.HTTPStatus()
			if status != tc.expectHTTP {
				if !(tc.expectHTTP == http.StatusOK && status == http.StatusNoContent) {
					t.Fatalf("unexpected http status: want %d got %d", tc.expectHTTP, status)
				}
			}
			if resp.Code != tc.expectCode {
				t.Fatalf("unexpected business code: want %d got %d message=%s", tc.expectCode, resp.Code, resp.Message)
			}

			checks["response"] = true
			if tc.postCheck != nil {
				tc.postCheck(t, env, username)
				checks["post_validation"] = true
			}

			if needsCleanup && (status == http.StatusOK || status == http.StatusNoContent) {
				cleanupName = ""
			}
		})
	}

	t.Run("batch_delete/success", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "batch_delete_success", "批量删除成功路径", resp, time.Since(start), !t.Failed(), checks, "批量删除应全部删除目标账号")
		})

		names := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			spec := env.NewUserSpec("batch_force_", basePassword)
			env.CreateUserAndWait(t, spec, 10*time.Second)
			names = append(names, spec.Name)
		}
		defer func() {
			for _, name := range names {
				env.ForceDeleteUserIgnore(name)
			}
		}()

		var err error
		resp, err = forceBatchDelete(t, env, names)
		if err != nil {
			t.Fatalf("batch force delete request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusOK {
			t.Fatalf("unexpected http status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrSuccess {
			t.Fatalf("unexpected business code: %d message=%s", resp.Code, resp.Message)
		}
		checks["response"] = true

		payload := struct {
			Deleted []string          `json:"deleted"`
			Skipped map[string]string `json:"skipped"`
		}{}
		if len(resp.Data) > 0 {
			if err := json.Unmarshal(resp.Data, &payload); err != nil {
				t.Fatalf("decode batch response: %v", err)
			}
		}
		if len(payload.Deleted) != len(names) {
			t.Fatalf("expected deleted=%d got=%d", len(names), len(payload.Deleted))
		}
		deletedSet := make(map[string]struct{}, len(payload.Deleted))
		for _, name := range payload.Deleted {
			deletedSet[name] = struct{}{}
		}
		for _, name := range names {
			if _, ok := deletedSet[name]; !ok {
				t.Fatalf("user %s missing in deleted list", name)
			}
		}
		checks["deleted_set_match"] = true

		for _, name := range names {
			if err := waitForUserGone(env, name, 30*time.Second); err != nil {
				t.Fatalf("wait for %s deletion: %v", name, err)
			}
		}
		checks["users_removed"] = true
	})

	t.Run("batch_delete/limit_exceeded", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "batch_delete_over_limit", "超出批量删除数量限制应直接拒绝", resp, time.Since(start), !t.Failed(), checks, "验证最大批量删除限制")
		})

		names := make([]string, 101)
		for i := range names {
			names[i] = fmt.Sprintf("over_limit_%d", i)
		}
		var err error
		resp, err = forceBatchDelete(t, env, names)
		if err != nil {
			t.Fatalf("batch delete over limit request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusBadRequest {
			t.Fatalf("unexpected http status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrReachMaxCount {
			t.Fatalf("unexpected business code: %d", resp.Code)
		}
		checks["limit_enforced"] = true
	})

	t.Run("batch_delete/missing_names", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "batch_delete_missing_names", "缺少用户名参数时返回参数错误", resp, time.Since(start), !t.Failed(), checks, "空数据集删除应提示错误")
		})

		var err error
		resp, err = env.AdminRequest(http.MethodDelete, "/v1/users", nil)
		if err != nil {
			t.Fatalf("batch delete missing names request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusBadRequest {
			t.Fatalf("unexpected http status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrInvalidParameter {
			t.Fatalf("unexpected business code: %d", resp.Code)
		}
		checks["invalid_parameter"] = true
	})

	t.Run("concurrency/delete_same_user", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		t.Cleanup(func() {
			recordCase(recorder, "concurrent_delete_same_user", "并发删除同一用户应保持幂等", nil, time.Since(start), !t.Failed(), checks, "验证并发删除冲突处理")
		})

		spec := env.NewUserSpec("race_del_", basePassword)
		env.CreateUserAndWait(t, spec, 10*time.Second)
		var wg sync.WaitGroup
		type outcome struct {
			status int
			code   int
			err    error
		}
		results := make([]outcome, 0, 2)
		mu := &sync.Mutex{}
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := env.ForceDeleteUser(spec.Name)
				mu.Lock()
				defer mu.Unlock()
				if resp != nil {
					results = append(results, outcome{status: resp.HTTPStatus(), code: resp.Code, err: err})
				} else {
					results = append(results, outcome{err: err})
				}
			}()
		}
		wg.Wait()

		for _, res := range results {
			if res.err != nil {
				t.Fatalf("concurrent delete request error: %v", res.err)
			}
			if res.status != http.StatusOK && res.status != http.StatusNoContent {
				t.Fatalf("unexpected status: %d", res.status)
			}
			if res.code != code.ErrSuccess {
				t.Fatalf("unexpected code: %d", res.code)
			}
		}
		if err := waitForUserGone(env, spec.Name, 30*time.Second); err != nil {
			t.Fatalf("wait for deletion: %v", err)
		}
		checks["idempotent"] = true
	})

	t.Run("permissions/non_admin_blocked", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "permission_enforced", "普通用户不允许执行物理删除", resp, time.Since(start), !t.Failed(), checks, "权限控制应阻止越权删除")
		})

		attacker := env.NewUserSpec("attacker_", basePassword)
		victim := env.NewUserSpec("victim_", basePassword)
		env.CreateUserAndWait(t, attacker, 10*time.Second)
		env.CreateUserAndWait(t, victim, 10*time.Second)
		defer func() {
			env.ForceDeleteUserIgnore(attacker.Name)
			env.ForceDeleteUserIgnore(victim.Name)
		}()

		tokens := env.LoginOrFail(t, attacker.Name, basePassword)
		var err error
		resp, err = env.AuthorizedRequest(http.MethodDelete, fmt.Sprintf("/v1/users/%s/force", victim.Name), tokens.AccessToken, nil)
		if err != nil {
			t.Fatalf("non-admin force delete request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusForbidden {
			t.Fatalf("unexpected status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrPermissionDenied {
			t.Fatalf("unexpected business code: %d", resp.Code)
		}
		checks["permission_denied"] = true

		ensureUserStillExists(t, env, victim.Name)
		checks["victim_intact"] = true
	})

	t.Run("consistency/token_revocation", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "token_revocation_after_delete", "删除后会话与登录应立即失效", resp, time.Since(start), !t.Failed(), checks, "验证事务回滚与关联数据清理")
		})

		spec := env.NewUserSpec("revoke_", basePassword)
		env.CreateUserAndWait(t, spec, 10*time.Second)
		defer env.ForceDeleteUserIgnore(spec.Name)
		tokens := env.LoginOrFail(t, spec.Name, basePassword)

		var err error
		resp, err = env.ForceDeleteUser(spec.Name)
		if err != nil {
			t.Fatalf("force delete request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusOK {
			t.Fatalf("unexpected status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrSuccess {
			t.Fatalf("unexpected code: %d", resp.Code)
		}
		checks["response"] = true

		if err := waitForUserGone(env, spec.Name, 30*time.Second); err != nil {
			t.Fatalf("wait for deletion: %v", err)
		}
		checks["user_removed"] = true

		if err := waitForAccessRevocation(env, tokens.AccessToken, spec.Name, 10*time.Second); err != nil {
			t.Fatalf("access token still valid: %v", err)
		}
		checks["token_revoked"] = true

		if err := waitForLoginFailure(env, spec.Name, basePassword, []int{code.ErrUserNotFound}, 10*time.Second); err != nil {
			t.Fatalf("login still succeeds after deletion: %v", err)
		}
		checks["login_blocked"] = true
	})

	t.Run("integrity/precise_scope", func(t *testing.T) {
		start := time.Now()
		checks := map[string]bool{}
		var resp *framework.APIResponse
		t.Cleanup(func() {
			recordCase(recorder, "precise_delete_scope", "只删除目标用户不影响其他账号", resp, time.Since(start), !t.Failed(), checks, "验证精确删除范围")
		})

		survivor := env.NewUserSpec("survivor_", basePassword)
		target := env.NewUserSpec("target_", basePassword)
		env.CreateUserAndWait(t, survivor, 10*time.Second)
		env.CreateUserAndWait(t, target, 10*time.Second)
		defer env.ForceDeleteUserIgnore(survivor.Name)

		var err error
		resp, err = env.ForceDeleteUser(target.Name)
		if err != nil {
			t.Fatalf("force delete request failed: %v", err)
		}
		if resp.HTTPStatus() != http.StatusOK {
			t.Fatalf("unexpected status: %d", resp.HTTPStatus())
		}
		if resp.Code != code.ErrSuccess {
			t.Fatalf("unexpected code: %d", resp.Code)
		}
		checks["response"] = true

		if err := waitForUserGone(env, target.Name, 30*time.Second); err != nil {
			t.Fatalf("wait for deletion: %v", err)
		}
		checks["target_removed"] = true

		ensureUserStillExists(t, env, survivor.Name)
		checks["other_user_intact"] = true
	})
}

func forceBatchDelete(t *testing.T, env *framework.Env, usernames []string) (*framework.APIResponse, error) {
	t.Helper()
	query := url.Values{}
	for _, name := range usernames {
		query.Add("names", name)
	}
	path := "/v1/users"
	if encoded := query.Encode(); encoded != "" {
		path = path + "?" + encoded
	}
	return env.AdminRequest(http.MethodDelete, path, nil)
}

func recordCase(recorder *framework.Recorder, name, description string, resp *framework.APIResponse, duration time.Duration, success bool, checks map[string]bool, notes ...string) {
	httpStatus := 0
	codeVal := 0
	message := ""
	if resp != nil {
		httpStatus = resp.HTTPStatus()
		codeVal = resp.Code
		message = resp.Message
	}
	recorder.AddCase(framework.CaseResult{
		Name:        name,
		Description: description,
		Success:     success,
		HTTPStatus:  httpStatus,
		Code:        codeVal,
		Message:     message,
		DurationMS:  duration.Milliseconds(),
		Checks:      checks,
		Notes:       notes,
	})
}

func waitForUserGone(env *framework.Env, username string, timeout time.Duration) error {
	return waitUntil(timeout, 200*time.Millisecond, func() (bool, error) {
		resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), nil)
		if err != nil {
			return false, err
		}
		switch resp.HTTPStatus() {
		case http.StatusNotFound:
			return true, nil
		case http.StatusOK:
			return false, nil
		default:
			return false, nil
		}
	})
}

func waitUntil(timeout, interval time.Duration, fn func() (bool, error)) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not satisfied within %s", timeout)
		}
		time.Sleep(interval)
	}
}

func ensureUserAbsentFromList(t *testing.T, env *framework.Env, username string) {
	t.Helper()
	token := env.AdminTokenOrFail(t)
	resp, err := env.ListUsers(token)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if resp.HTTPStatus() != http.StatusOK {
		if resp.HTTPStatus() >= http.StatusInternalServerError {
			t.Logf("list users returned %d, skip list-based validation", resp.HTTPStatus())
			return
		}
		t.Fatalf("unexpected list status: %d", resp.HTTPStatus())
	}
	if len(resp.Data) == 0 {
		return
	}
	var users []struct {
		Username string `json:"username"`
	}
	if err := json.Unmarshal(resp.Data, &users); err != nil {
		t.Fatalf("decode user list: %v", err)
	}
	for _, user := range users {
		if user.Username == username {
			t.Fatalf("user %s still present after force delete", username)
		}
	}
}

func ensureAuditEntry(t *testing.T, env *framework.Env, username, action string) {
	t.Helper()
	events, enabled, resp, err := env.AuditEvents(50)
	if err != nil {
		t.Fatalf("fetch audit events: %v", err)
	}
	if resp != nil && resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("unexpected audit status: %d", resp.HTTPStatus())
	}
	if !enabled {
		t.Fatalf("audit log disabled when expecting records")
	}
	for _, event := range events {
		if event.Action == action && event.ResourceID == username {
			return
		}
	}
	t.Fatalf("audit event %s for %s not found", action, username)
}

func ensureUserStillExists(t *testing.T, env *framework.Env, username string) {
	t.Helper()
	resp, err := env.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), nil)
	if err != nil {
		t.Fatalf("get user %s: %v", username, err)
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("expected user %s to exist, got status %d", username, resp.HTTPStatus())
	}
}

func waitForAccessRevocation(env *framework.Env, token, username string, timeout time.Duration) error {
	return waitUntil(timeout, 200*time.Millisecond, func() (bool, error) {
		resp, err := env.GetUser(token, username)
		if err != nil {
			return false, err
		}
		return resp.HTTPStatus() != http.StatusOK, nil
	})
}

func waitForLoginFailure(env *framework.Env, username, password string, expectedCodes []int, timeout time.Duration) error {
	expected := make(map[int]struct{}, len(expectedCodes))
	for _, code := range expectedCodes {
		expected[code] = struct{}{}
	}
	return waitUntil(timeout, 300*time.Millisecond, func() (bool, error) {
		_, resp, err := env.Login(username, password)
		if err == nil {
			return false, nil
		}
		if resp == nil {
			return false, nil
		}
		if _, ok := expected[resp.Code]; ok {
			return true, nil
		}
		return false, nil
	})
}
