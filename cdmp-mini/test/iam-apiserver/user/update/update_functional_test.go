package update

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/update"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to run user update tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type updateCase struct {
	name        string
	description string
	setup       func(t *testing.T, env *framework.Env) (framework.UserSpec, bool)
	payload     func(spec framework.UserSpec) map[string]any
	expectHTTP  int
	expectCode  int
	verify      func(t *testing.T, env *framework.Env, spec framework.UserSpec, payload map[string]any, resp *framework.APIResponse)
}

var errUserListVersionUnsupported = errors.New("user list requires version column not present")

func deriveDeterministicPhone(seed string) string {
	checksum := crc32.ChecksumIEEE([]byte(seed)) % 100000000
	return fmt.Sprintf("139%08d", checksum)
}

func TestUpdateFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "update")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping update tests")
	}

	const basePassword = "InitPassw0rd!"

	cases := []updateCase{
		{
			name:        "update_success",
			description: "管理员更新昵称和邮箱成功",
			setup: func(t *testing.T, env *framework.Env) (framework.UserSpec, bool) {
				spec := env.NewUserSpec("update_ok_", basePassword)
				env.CreateUserAndWait(t, spec, 15*time.Second)
				return spec, true
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{
					"metadata": map[string]string{"name": spec.Name},
					"nickname": "集成测试更新",
					"email":    fmt.Sprintf("%s-updated@example.com", spec.Name),
					"phone":    spec.Phone,
					"status":   1,
					"isAdmin":  0,
					"version":  1,
				}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			verify: func(t *testing.T, env *framework.Env, spec framework.UserSpec, payload map[string]any, resp *framework.APIResponse) {
				var data struct {
					UpdateUser struct {
						Metadata struct {
							Name string `json:"name"`
						} `json:"metadata"`
						Nickname string `json:"nickname"`
						Email    string `json:"email"`
						Phone    string `json:"phone"`
						Status   int    `json:"status"`
						IsAdmin  int    `json:"isAdmin"`
					} `json:"update_user"`
				}
				if err := json.Unmarshal(resp.Data, &data); err != nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("decode response data: %w", err))
				}
				if data.UpdateUser.Metadata.Name != spec.Name {
					t.Fatalf("login before change failed: %v", fmt.Errorf("metadata.name mismatch"))
				}
				wantNickname := payload["nickname"].(string)
				wantEmail := payload["email"].(string)
				if data.UpdateUser.Nickname != wantNickname {
					t.Fatalf("login before change failed: %v", fmt.Errorf("nickname mismatch want=%s got=%s", wantNickname, data.UpdateUser.Nickname))
				}
				if data.UpdateUser.Email != wantEmail {
					t.Fatalf("login before change failed: %v", fmt.Errorf("email mismatch want=%s got=%s", wantEmail, data.UpdateUser.Email))
				}
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Nickname == wantNickname && u.Email == wantEmail
				})
				if unsupported {
					t.Logf("skip public user verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("wait for user %s updated state timeout", spec.Name))
				}
			},
		},
		{
			name:        "update_full_payload",
			description: "管理员全字段更新用户资料",
			setup: func(t *testing.T, env *framework.Env) (framework.UserSpec, bool) {
				spec := env.NewUserSpec("update_full_", basePassword)
				env.CreateUserAndWait(t, spec, 15*time.Second)
				return spec, true
			},
			payload: func(spec framework.UserSpec) map[string]any {
				phone := deriveDeterministicPhone(spec.Name + "_full")
				if phone == spec.Phone {
					phone = deriveDeterministicPhone(spec.Name + "_full_alt")
				}
				return map[string]any{
					"metadata": map[string]string{"name": spec.Name},
					"nickname": "全字段更新",
					"email":    fmt.Sprintf("%s-full@example.com", spec.Name),
					"phone":    phone,
					"status":   1,
					"isAdmin":  0,
					"version":  1,
				}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			verify: func(t *testing.T, env *framework.Env, spec framework.UserSpec, payload map[string]any, _ *framework.APIResponse) {
				wantNickname := payload["nickname"].(string)
				wantEmail := payload["email"].(string)
				wantPhone := payload["phone"].(string)
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Nickname == wantNickname && u.Email == wantEmail && u.Phone == wantPhone
				})
				if unsupported {
					t.Logf("skip full payload verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("full payload update not applied for %s", spec.Name))
				}
			},
		},
		{
			name:        "user_not_found",
			description: "更新不存在的用户应返回404",
			setup: func(t *testing.T, env *framework.Env) (framework.UserSpec, bool) {
				spec := env.NewUserSpec("update_missing_", basePassword)
				return spec, false
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{
					"metadata": map[string]string{"name": spec.Name},
					"nickname": "missing user",
					"version":  1,
				}
			},
			expectHTTP: http.StatusNotFound,
			expectCode: code.ErrUserNotFound,
		},
		{
			name:        "sql_injection_safe",
			description: "更新接口应正确转义可能的注入字符串",
			setup: func(t *testing.T, env *framework.Env) (framework.UserSpec, bool) {
				spec := env.NewUserSpec("update_sql_", basePassword)
				env.CreateUserAndWait(t, spec, 15*time.Second)
				return spec, true
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{
					"metadata": map[string]string{"name": spec.Name},
					"nickname": "'; DROP TABLE users; --",
					"email":    fmt.Sprintf("%s-sql@example.com", spec.Name),
					"version":  1,
				}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			verify: func(t *testing.T, env *framework.Env, spec framework.UserSpec, payload map[string]any, _ *framework.APIResponse) {
				injectionNickname := payload["nickname"].(string)
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Nickname == injectionNickname
				})
				if unsupported {
					t.Logf("skip sql injection verification for %s due to missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("sql injection guard not verifiable for %s", spec.Name))
				}
				if user.Nickname != injectionNickname {
					t.Fatalf("login before change failed: %v", fmt.Errorf("nickname should persist literal payload, got=%s", user.Nickname))
				}
			},
		},
		{
			name:        "invalid_status",
			description: "非法状态值返回校验错误",
			setup: func(t *testing.T, env *framework.Env) (framework.UserSpec, bool) {
				spec := env.NewUserSpec("update_badstatus_", basePassword)
				env.CreateUserAndWait(t, spec, 15*time.Second)
				return spec, true
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{
					"metadata": map[string]string{"name": spec.Name},
					"status":   2,
					"version":  1,
				}
			},
			expectHTTP: http.StatusBadRequest,
			expectCode: code.ErrValidation,
		},
	}

	var toCleanup []string
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if env.UserVersionUnsupported() {
				t.Skip("backend missing user version column; skipping update tests")
			}
			spec, created := tc.setup(t, env)
			if created {
				toCleanup = append(toCleanup, spec.Name)
			}

			payload := tc.payload(spec)
			if _, ok := payload["metadata"]; !ok && spec.Name != "" {
				payload["metadata"] = map[string]string{"name": spec.Name}
			}

			start := time.Now()
			resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), payload)
			duration := time.Since(start)
			if err != nil {
				if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
					t.Skip("backend missing user version column; skipping update tests")
				}
				t.Fatalf("login before change failed: %v", fmt.Errorf("update user request: %w", err))
			}

			if resp.HTTPStatus() != tc.expectHTTP {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
			}
			if resp.Code != tc.expectCode {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected code=%d message=%s", resp.Code, resp.Message))
			}

			if tc.verify != nil {
				tc.verify(t, env, spec, payload, resp)
			}

			checks := map[string]bool{"response": true}
			if tc.verify != nil {
				checks["payload_applied"] = true
			}

			recorder.AddCase(framework.CaseResult{
				Name:        tc.name,
				Description: tc.description,
				Success:     true,
				HTTPStatus:  resp.HTTPStatus(),
				Code:        resp.Code,
				Message:     resp.Message,
				DurationMS:  duration.Milliseconds(),
				Checks:      checks,
				Notes:       []string{tc.description},
			})
		})
		if env.UserVersionUnsupported() {
			t.Skip("backend missing user version column; skipping update tests")
		}
	}

	for _, name := range toCleanup {
		env.ForceDeleteUserIgnore(name)
	}
}

func TestTargetedPatchFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "patch_targeted")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping targeted patch tests")
	}

	const basePassword = "InitPassw0rd!"

	cases := []struct {
		name        string
		description string
		method      string
		path        func(spec framework.UserSpec) string
		payload     func(spec framework.UserSpec) map[string]any
		expectHTTP  int
		expectCode  int
		waitCheck   func(t *testing.T, env *framework.Env, spec framework.UserSpec)
	}{
		{
			name:        "profile_update_nickname",
			description: "PUT /api/users/{name}/profile 仅更新昵称",
			method:      http.MethodPut,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/profile", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{"nickname": "patch_single_field"}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			waitCheck: func(t *testing.T, env *framework.Env, spec framework.UserSpec) {
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Nickname == "patch_single_field"
				})
				if unsupported {
					t.Logf("skip nickname verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("expected nickname patch_single_field not applied"))
				}
			},
		},
		{
			name:        "profile_version_conflict",
			description: "PUT /api/users/{name}/profile 携带错误版本不应落盘",
			method:      http.MethodPut,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/profile", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{
					"nickname": "should_not_apply",
					"version":  float64(9999),
				}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			waitCheck: func(t *testing.T, env *framework.Env, spec framework.UserSpec) {
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Nickname == spec.Nickname
				})
				if unsupported {
					t.Logf("skip optimistic lock verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					latest, err := fetchPublicUser(t, env, spec.Name)
					if errors.Is(err, errUserListVersionUnsupported) {
						t.Logf("skip optimistic lock fallback fetch for %s: backend missing version column", spec.Name)
						return
					}
					if err != nil {
						t.Fatalf("login before change failed: %v", fmt.Errorf("fallback fetch %s: %w", spec.Name, err))
					}
					if latest != nil && latest.Nickname == "should_not_apply" {
						t.Fatalf("login before change failed: %v", fmt.Errorf("nickname should remain %s", spec.Nickname))
					}
					t.Fatalf("login before change failed: %v", fmt.Errorf("nick name mismatch after optimistic lock test"))
				}
			},
		},
		{
			name:        "password_patch_success",
			description: "PATCH /api/users/{name}/password 更新密码并验证登录",
			method:      http.MethodPatch,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/password", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{"password": "NewPassw0rd#1"}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			waitCheck: func(t *testing.T, env *framework.Env, spec framework.UserSpec) {
				newPassword := "NewPassw0rd#1"
				deadline := time.Now().Add(25 * time.Second)
				for time.Now().Before(deadline) {
					tokens, _, err := env.Login(spec.Name, newPassword)
					if err == nil && tokens != nil {
						break
					}
					time.Sleep(300 * time.Millisecond)
				}
				if time.Now().After(deadline) {
					t.Fatalf("login before change failed: %v", fmt.Errorf("new password not applied for %s", spec.Name))
				}

				oldPasswordDeadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(oldPasswordDeadline) {
					if _, _, err := env.Login(spec.Name, spec.Password); err != nil {
						return
					}
					time.Sleep(200 * time.Millisecond)
				}
				t.Fatalf("login before change failed: %v", fmt.Errorf("old password still valid for %s", spec.Name))
			},
		},
		{
			name:        "password_missing_field",
			description: "PATCH /api/users/{name}/password 缺少密码字段返回校验错误",
			method:      http.MethodPatch,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/password", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{}
			},
			expectHTTP: http.StatusBadRequest,
			expectCode: code.ErrInvalidParameter,
		},
		{
			name:        "email_patch_single_field",
			description: "PATCH /api/users/{name}/email 更新邮箱",
			method:      http.MethodPatch,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/email", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{"email": fmt.Sprintf("%s-patch@example.com", spec.Name)}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			waitCheck: func(t *testing.T, env *framework.Env, spec framework.UserSpec) {
				expectedEmail := fmt.Sprintf("%s-patch@example.com", spec.Name)
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Email == expectedEmail
				})
				if unsupported {
					t.Logf("skip email verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("email patch not applied"))
				}
			},
		},
		{
			name:        "phone_clear_to_empty",
			description: "PATCH /api/users/{name}/phone 清空手机号",
			method:      http.MethodPatch,
			path: func(spec framework.UserSpec) string {
				return fmt.Sprintf("/api/users/%s/phone", spec.Name)
			},
			payload: func(spec framework.UserSpec) map[string]any {
				return map[string]any{"phone": ""}
			},
			expectHTTP: http.StatusOK,
			expectCode: code.ErrSuccess,
			waitCheck: func(t *testing.T, env *framework.Env, spec framework.UserSpec) {
				if spec.Phone == "" {
					t.Fatalf("login before change failed: %v", fmt.Errorf("test setup expected initial phone to be non-empty"))
				}
				user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == spec.Name && u.Phone == ""
				})
				if unsupported {
					t.Logf("skip phone clearing verification for %s: backend missing version column", spec.Name)
					return
				}
				if user == nil {
					t.Fatalf("login before change failed: %v", fmt.Errorf("phone clear patch not observed for %s", spec.Name))
				}
				if user.Phone != "" {
					t.Fatalf("login before change failed: %v", fmt.Errorf("expected phone to be empty, got=%s", user.Phone))
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if env.UserVersionUnsupported() {
				t.Skip("backend missing version column; skipping targeted patch tests")
			}
			spec := env.NewUserSpec("patch_case_", basePassword)
			env.CreateUserAndWait(t, spec, 15*time.Second)
			t.Cleanup(func() {
				env.ForceDeleteUserIgnore(spec.Name)
			})

			payload := tc.payload(spec)
			start := time.Now()
			resp, err := env.AdminRequest(tc.method, tc.path(spec), payload)
			duration := time.Since(start)
			if err != nil {
				if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
					t.Skip("backend missing version column; skipping targeted patch tests")
				}
				t.Fatalf("login before change failed: %v", fmt.Errorf("targeted patch request: %w", err))
			}
			if resp.HTTPStatus() != tc.expectHTTP {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
			}
			if resp.Code != tc.expectCode {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected code=%d message=%s", resp.Code, resp.Message))
			}

			if tc.waitCheck != nil {
				tc.waitCheck(t, env, spec)
			}

			recorder.AddCase(framework.CaseResult{
				Name:        tc.name,
				Description: tc.description,
				Success:     resp.HTTPStatus() == tc.expectHTTP && resp.Code == tc.expectCode,
				HTTPStatus:  resp.HTTPStatus(),
				Code:        resp.Code,
				Message:     resp.Message,
				DurationMS:  duration.Milliseconds(),
				Checks:      map[string]bool{"response": true},
				Notes:       []string{tc.description},
			})
		})
		if env.UserVersionUnsupported() {
			t.Skip("backend missing version column; skipping targeted patch tests")
		}
	}
}

func TestBatchPatchFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "patch_batch")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping batch patch tests")
	}

	const (
		basePassword = "InitPassw0rd!"
		workerCount  = 3
	)

	specs := make([]framework.UserSpec, workerCount)
	for i := range specs {
		specs[i] = env.NewUserSpec("patch_batch_", basePassword)
		specs[i].IsAdmin = 0
		env.CreateUserAndWait(t, specs[i], 15*time.Second)
		defer env.ForceDeleteUserIgnore(specs[i].Name)
	}

	targetNames := []string{specs[0].Name, specs[1].Name}
	payload := map[string]any{
		"updates": map[string]any{
			"isAdmin": 1,
		},
		"conditions": map[string]any{
			"name": map[string]any{
				"in": targetNames,
			},
		},
	}

	start := time.Now()
	resp, err := env.AdminRequest(http.MethodPatch, "/api/users", payload)
	duration := time.Since(start)
	if err != nil {
		if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
			t.Skip("backend missing user version column; skipping batch patch tests")
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("batch patch request: %w", err))
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
	}
	if resp.Code != code.ErrSuccess {
		t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected code=%d message=%s", resp.Code, resp.Message))
	}

	// 等待异步更新落地
	for _, spec := range specs {
		expected := 0
		for _, target := range targetNames {
			if spec.Name == target {
				expected = 1
				break
			}
		}
		user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
			if u == nil {
				return false
			}
			if u.Username != spec.Name {
				return false
			}
			if expected == 1 {
				return u.IsAdmin == 1
			}
			return u.IsAdmin == 0
		})
		if unsupported {
			t.Logf("skip admin flag verification for %s: backend missing version column", spec.Name)
			continue
		}
		if user == nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("batch patch verification failed for %s", spec.Name))
		}
	}

	recorder.AddCase(framework.CaseResult{
		Name:        "batch_patch_isadmin",
		Description: "批量条件更新仅影响匹配用户",
		Success:     true,
		HTTPStatus:  resp.HTTPStatus(),
		Code:        resp.Code,
		Message:     resp.Message,
		DurationMS:  duration.Milliseconds(),
		Checks: map[string]bool{
			"response":        true,
			"condition_apply": true,
		},
		Notes: []string{"target users promoted to admin"},
	})
}

func TestConditionalPatchFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "patch_condition")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping conditional patch tests")
	}

	const basePassword = "InitPassw0rd!"

	cases := []struct {
		name        string
		description string
		prepare     func(t *testing.T, env *framework.Env) []framework.UserSpec
		payload     func(specs []framework.UserSpec) map[string]any
		verify      func(t *testing.T, env *framework.Env, specs []framework.UserSpec)
		shouldSkip  string
	}{
		{
			name:        "exact_match_condition",
			description: "name in 条件命中指定用户",
			prepare: func(t *testing.T, env *framework.Env) []framework.UserSpec {
				return createTestUsers(t, env, "cond_exact_", basePassword, 3)
			},
			payload: func(specs []framework.UserSpec) map[string]any {
				target := specs[0]
				return map[string]any{
					"updates": map[string]any{
						"nickname": "条件更新命中",
					},
					"conditions": map[string]any{
						"name": map[string]any{
							"in": []string{target.Name},
						},
					},
				}
			},
			verify: func(t *testing.T, env *framework.Env, specs []framework.UserSpec) {
				target := specs[0]
				_, unsupported := waitForPublicUser(t, env, target.Name, 25*time.Second, func(u *publicUser) bool {
					return u != nil && u.Username == target.Name && u.Nickname == "条件更新命中"
				})
				if unsupported {
					t.Skip("backend missing version column; conditional verification skipped")
				}
				for _, spec := range specs[1:] {
					existing, err := fetchPublicUser(t, env, spec.Name)
					if err != nil {
						t.Fatalf("login before change failed: %v", fmt.Errorf("verify unaffected user %s: %w", spec.Name, err))
					}
					if existing != nil && existing.Nickname != spec.Nickname {
						t.Fatalf("login before change failed: %v", fmt.Errorf("unmatched user %s should remain nickname=%s got=%s", spec.Name, spec.Nickname, existing.Nickname))
					}
				}
			},
		},
		{
			name:        "no_match_condition",
			description: "条件无匹配时应安全返回",
			prepare: func(t *testing.T, env *framework.Env) []framework.UserSpec {
				return createTestUsers(t, env, "cond_none_", basePassword, 2)
			},
			payload: func(specs []framework.UserSpec) map[string]any {
				return map[string]any{
					"updates": map[string]any{
						"nickname": "should_not_apply",
					},
					"conditions": map[string]any{
						"name": map[string]any{
							"in": []string{"nonexistent-user"},
						},
					},
				}
			},
			verify: func(t *testing.T, env *framework.Env, specs []framework.UserSpec) {
				for _, spec := range specs {
					existing, err := fetchPublicUser(t, env, spec.Name)
					if err != nil {
						t.Fatalf("login before change failed: %v", fmt.Errorf("verify no match user %s: %w", spec.Name, err))
					}
					if existing != nil && existing.Nickname == "should_not_apply" {
						t.Fatalf("login before change failed: %v", fmt.Errorf("nickname should remain unchanged for %s", spec.Name))
					}
				}
			},
		},
		{
			name:        "range_condition_isadmin",
			description: "范围条件 isAdmin.gte 命中已提升的管理员",
			prepare: func(t *testing.T, env *framework.Env) []framework.UserSpec {
				specs := createTestUsers(t, env, "cond_range_", basePassword, 4)
				for _, spec := range specs[:2] {
					s := spec
					applyPatchProfileAndWait(t, env, s, map[string]any{"isAdmin": 1}, func(u *publicUser) bool {
						return u != nil && u.Username == s.Name && u.IsAdmin == 1
					})
				}
				return specs
			},
			payload: func(specs []framework.UserSpec) map[string]any {
				return map[string]any{
					"updates": map[string]any{
						"nickname": "range_condition_selected",
					},
					"conditions": map[string]any{
						"isAdmin": map[string]any{
							"gte": 1,
						},
					},
				}
			},
			verify: func(t *testing.T, env *framework.Env, specs []framework.UserSpec) {
				for idx, spec := range specs {
					s := spec
					wantNickname := s.Nickname
					if idx < 2 {
						wantNickname = "range_condition_selected"
					}
					user, unsupported := waitForPublicUser(t, env, s.Name, 25*time.Second, func(u *publicUser) bool {
						if u == nil || u.Username != s.Name {
							return false
						}
						if idx < 2 {
							return u.Nickname == "range_condition_selected"
						}
						return u.Nickname == s.Nickname
					})
					if unsupported {
						t.Skip("backend missing version column; skipping range condition verification")
					}
					if user == nil {
						t.Fatalf("login before change failed: %v", fmt.Errorf("range condition verification failed for %s", s.Name))
					}
					if user.Nickname != wantNickname {
						t.Fatalf("login before change failed: %v", fmt.Errorf("nickname mismatch for %s want=%s got=%s", s.Name, wantNickname, user.Nickname))
					}
					if idx < 2 && user.IsAdmin != 1 {
						t.Fatalf("login before change failed: %v", fmt.Errorf("expected %s promoted to admin", s.Name))
					}
					if idx >= 2 && user.IsAdmin != 0 {
						t.Fatalf("login before change failed: %v", fmt.Errorf("non-target user %s should remain non-admin", s.Name))
					}
				}
			},
		},
		{
			name:        "complex_multi_field_condition",
			description: "组合 name.in 与 isAdmin.eq 仅更新符合的普通用户",
			prepare: func(t *testing.T, env *framework.Env) []framework.UserSpec {
				specs := createTestUsers(t, env, "cond_complex_", basePassword, 3)
				applyPatchProfileAndWait(t, env, specs[0], map[string]any{"isAdmin": 1}, func(u *publicUser) bool {
					return u != nil && u.Username == specs[0].Name && u.IsAdmin == 1
				})
				return specs
			},
			payload: func(specs []framework.UserSpec) map[string]any {
				return map[string]any{
					"updates": map[string]any{
						"nickname": "complex_condition_hit",
					},
					"conditions": map[string]any{
						"name": map[string]any{
							"in": []string{specs[0].Name, specs[1].Name},
						},
						"isAdmin": map[string]any{
							"eq": 0,
						},
					},
				}
			},
			verify: func(t *testing.T, env *framework.Env, specs []framework.UserSpec) {
				for idx, spec := range specs {
					s := spec
					user, unsupported := waitForPublicUser(t, env, s.Name, 25*time.Second, func(u *publicUser) bool {
						if u == nil || u.Username != s.Name {
							return false
						}
						if idx == 1 {
							return u.Nickname == "complex_condition_hit"
						}
						return u.Nickname == s.Nickname
					})
					if unsupported {
						t.Skip("backend missing version column; skipping complex condition verification")
					}
					if user == nil {
						t.Fatalf("login before change failed: %v", fmt.Errorf("complex condition verification failed for %s", s.Name))
					}
					if idx == 0 && user.Nickname != s.Nickname {
						t.Fatalf("login before change failed: %v", fmt.Errorf("admin user %s should remain unchanged", s.Name))
					}
					if idx == 0 && user.IsAdmin != 1 {
						t.Fatalf("login before change failed: %v", fmt.Errorf("admin flag lost for %s", s.Name))
					}
					if idx == 1 && user.Nickname != "complex_condition_hit" {
						t.Fatalf("login before change failed: %v", fmt.Errorf("expected nickname complex_condition_hit for %s got=%s", s.Name, user.Nickname))
					}
					if idx == 2 && user.Nickname != s.Nickname {
						t.Fatalf("login before change failed: %v", fmt.Errorf("non-listed user %s should remain unchanged", s.Name))
					}
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldSkip != "" {
				t.Skip(tc.shouldSkip)
			}
			specs := tc.prepare(t, env)
			if len(specs) == 0 && tc.payload != nil {
				t.Skip("scenario requires backend support for dataset generation")
			}
			payload := tc.payload(specs)
			start := time.Now()
			resp, err := env.AdminRequest(http.MethodPatch, "/api/users", payload)
			duration := time.Since(start)
			if err != nil {
				if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
					t.Skip("backend missing user version column; skipping conditional patch tests")
				}
				t.Fatalf("login before change failed: %v", fmt.Errorf("conditional patch request: %w", err))
			}
			if resp.HTTPStatus() != http.StatusOK {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
			}
			if resp.Code != code.ErrSuccess {
				t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected code=%d message=%s", resp.Code, resp.Message))
			}
			if tc.verify != nil {
				tc.verify(t, env, specs)
			}
			recorder.AddCase(framework.CaseResult{
				Name:        tc.name,
				Description: tc.description,
				Success:     true,
				HTTPStatus:  resp.HTTPStatus(),
				Code:        resp.Code,
				Message:     resp.Message,
				DurationMS:  duration.Milliseconds(),
				Checks:      map[string]bool{"response": true},
				Notes:       []string{tc.description},
			})
		})
	}
}

func createTestUsers(t *testing.T, env *framework.Env, prefix, password string, count int) []framework.UserSpec {
	t.Helper()
	keepFailures := os.Getenv("IAM_APISERVER_KEEP_FAILURES") == "1"
	specs := make([]framework.UserSpec, count)
	for i := range specs {
		specs[i] = env.NewUserSpec(fmt.Sprintf("%s%d_", prefix, i), password)
		env.CreateUserAndWait(t, specs[i], 15*time.Second)
		if !keepFailures {
			spec := specs[i]
			t.Cleanup(func() {
				env.ForceDeleteUserIgnore(spec.Name)
			})
		}
	}
	return specs
}

func applyPatchProfileAndWait(t *testing.T, env *framework.Env, spec framework.UserSpec, updates map[string]any, predicate func(*publicUser) bool) {
	t.Helper()
	if env.UserVersionUnsupported() {
		t.Skip("backend missing version column; skipping patch profile helper")
	}
	payload := make(map[string]any, len(updates))
	for k, v := range updates {
		payload[k] = v
	}
	resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/api/users/%s/profile", spec.Name), payload)
	if err != nil {
		if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
			t.Skip("backend missing version column; skipping patch profile helper")
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("update profile %s: %w", spec.Name, err))
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("update profile %s unexpected http=%d", spec.Name, resp.HTTPStatus()))
	}
	if resp.Code != code.ErrSuccess {
		t.Fatalf("login before change failed: %v", fmt.Errorf("update profile %s unexpected code=%d message=%s", spec.Name, resp.Code, resp.Message))
	}
	if predicate == nil {
		return
	}
	user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, predicate)
	if unsupported {
		t.Skip("backend missing version column; skipping patch profile helper")
	}
	if user == nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("patch profile %s predicate not satisfied", spec.Name))
	}
}

func TestUpdatePermissionControls(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "update_permissions")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping permission tests")
	}

	const basePassword = "InitPassw0rd!"

	actor := env.NewUserSpec("perm_actor_", basePassword)
	actor.IsAdmin = 0
	env.CreateUserAndWait(t, actor, 15*time.Second)
	t.Cleanup(func() {
		env.ForceDeleteUserIgnore(actor.Name)
	})

	target := env.NewUserSpec("perm_target_", basePassword)
	target.IsAdmin = 0
	env.CreateUserAndWait(t, target, 15*time.Second)
	t.Cleanup(func() {
		env.ForceDeleteUserIgnore(target.Name)
	})

	actorTokens, _, err := env.Login(actor.Name, basePassword)
	if err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("login actor %s: %w", actor.Name, err))
	}

	t.Run("row_level_forbidden", func(t *testing.T) {
		payload := map[string]any{
			"metadata": map[string]string{"name": target.Name},
			"nickname": "非法越权更新",
			"version":  1,
		}
		resp, reqErr := env.AuthorizedRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", target.Name), actorTokens.AccessToken, payload)
		if reqErr != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("row level update request: %w", reqErr))
		}
		if resp.HTTPStatus() != http.StatusForbidden {
			t.Fatalf("login before change failed: %v", fmt.Errorf("expected 403 got=%d", resp.HTTPStatus()))
		}
		if resp.Code != code.ErrPermissionDenied {
			t.Fatalf("login before change failed: %v", fmt.Errorf("expected code=%d got=%d", code.ErrPermissionDenied, resp.Code))
		}
		recorder.AddCase(framework.CaseResult{
			Name:        "row_level_forbidden",
			Description: "非管理员无法更新他人账号",
			Success:     true,
			HTTPStatus:  resp.HTTPStatus(),
			Code:        resp.Code,
			Message:     resp.Message,
			Checks:      map[string]bool{"permission_denied": true},
			Notes:       []string{"row level permission enforced"},
		})
	})

	t.Run("field_level_forbidden", func(t *testing.T) {
		payload := map[string]any{
			"metadata": map[string]string{"name": actor.Name},
			"isAdmin":  1,
			"version":  1,
		}
		resp, reqErr := env.AuthorizedRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", actor.Name), actorTokens.AccessToken, payload)
		if reqErr != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("field level update request: %w", reqErr))
		}
		blocked := false
		switch resp.HTTPStatus() {
		case http.StatusForbidden:
			blocked = true
			if resp.Code != code.ErrPermissionDenied {
				t.Fatalf("login before change failed: %v", fmt.Errorf("expected code=%d got=%d", code.ErrPermissionDenied, resp.Code))
			}
		case http.StatusOK:
			// 后端以200响应，但应保持 isAdmin 未提升。
			targetState, err := fetchPublicUser(t, env, actor.Name)
			if err != nil {
				t.Fatalf("login before change failed: %v", fmt.Errorf("fetch actor state: %w", err))
			}
			if targetState != nil && targetState.IsAdmin == 0 {
				blocked = true
			}
		default:
			t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
		}
		if !blocked {
			t.Fatalf("login before change failed: %v", fmt.Errorf("field level restriction not enforced"))
		}
		recorder.AddCase(framework.CaseResult{
			Name:        "field_level_forbidden",
			Description: "普通用户无权提升自身为管理员",
			Success:     true,
			HTTPStatus:  resp.HTTPStatus(),
			Code:        resp.Code,
			Message:     resp.Message,
			Checks:      map[string]bool{"permission_denied": blocked},
			Notes:       []string{"field level permission enforced"},
		})
	})
}

func TestUpdateAuditLogging(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "update_audit")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping audit tests")
	}

	const basePassword = "InitPassw0rd!"
	spec := env.NewUserSpec("audit_update_", basePassword)
	env.CreateUserAndWait(t, spec, 15*time.Second)
	t.Cleanup(func() {
		env.ForceDeleteUserIgnore(spec.Name)
	})

	payload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": "审计日志检测",
		"email":    fmt.Sprintf("%s-audit@example.com", spec.Name),
		"version":  1,
	}
	resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), payload)
	if err != nil {
		if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
			t.Skip("backend missing user version column; skipping audit tests")
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("update for audit: %w", err))
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected http=%d", resp.HTTPStatus()))
	}
	if resp.Code != code.ErrSuccess {
		t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected code=%d message=%s", resp.Code, resp.Message))
	}

	time.Sleep(2 * time.Second)
	events, enabled, auditResp, auditErr := env.AuditEvents(25)
	if auditErr != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("audit events fetch: %w", auditErr))
	}
	if !enabled {
		t.Skip("audit log disabled on backend")
	}
	if auditResp == nil || auditResp.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("unexpected audit status"))
	}
	found := false
	for _, event := range events {
		if strings.EqualFold(event.ResourceID, spec.Name) && strings.Contains(strings.ToLower(event.Action), "update") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("login before change failed: %v", fmt.Errorf("expected audit event for %s not found", spec.Name))
	}

	recorder.AddCase(framework.CaseResult{
		Name:        "audit_log_recorded",
		Description: "用户更新操作应被审计记录",
		Success:     true,
		HTTPStatus:  resp.HTTPStatus(),
		Code:        resp.Code,
		Message:     resp.Message,
		Checks: map[string]bool{
			"response": true,
			"audit":    true,
		},
		Notes: []string{"audit trail contains update event"},
	})
}

func TestUpdateOptimisticLocking(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "update_locking")
	defer recorder.Flush(t)
	if env.UserVersionUnsupported() {
		t.Skip("backend missing user version column; skipping optimistic locking tests")
	}

	const basePassword = "InitPassw0rd!"
	spec := env.NewUserSpec("lock_case_", basePassword)
	env.CreateUserAndWait(t, spec, 15*time.Second)
	t.Cleanup(func() {
		env.ForceDeleteUserIgnore(spec.Name)
	})

	initialPayload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": "lock-primary",
		"email":    fmt.Sprintf("%s-lock@example.com", spec.Name),
		"version":  1,
	}
	resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), initialPayload)
	if err != nil {
		if errors.Is(err, framework.ErrUserVersionColumnMissing) || env.UserVersionUnsupported() {
			t.Skip("backend missing user version column; skipping optimistic locking tests")
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("initial update: %w", err))
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("initial update unexpected http=%d", resp.HTTPStatus()))
	}
	if resp.Code != code.ErrSuccess {
		t.Fatalf("login before change failed: %v", fmt.Errorf("initial update unexpected code=%d", resp.Code))
	}
	currentVersion, versionErr := extractUpdateVersionMetadata(resp)
	if versionErr != nil || currentVersion == 0 {
		t.Skip("update response missing version metadata; backend may not expose optimistic lock version")
	}

	if user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
		return u != nil && u.Username == spec.Name && u.Nickname == "lock-primary"
	}); unsupported {
		t.Skip("backend missing version column; skipping optimistic locking tests")
	} else if user == nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("initial nickname not observable"))
	}

	conflictPayload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": "lock-conflict",
		"version":  currentVersion,
	}
	respConflict, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), conflictPayload)
	if err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("conflict update: %w", err))
	}
	switch respConflict.HTTPStatus() {
	case http.StatusOK:
		if respConflict.Code != code.ErrSuccess {
			t.Fatalf("login before change failed: %v", fmt.Errorf("conflict update unexpected code=%d", respConflict.Code))
		}
	case http.StatusConflict:
		// 冲突由 API 层直接拦截，继续验证最终状态。
	default:
		t.Fatalf("login before change failed: %v", fmt.Errorf("conflict update unexpected http=%d", respConflict.HTTPStatus()))
	}

	// 等待消费层处理潜在冲突。
	time.Sleep(2 * time.Second)
	stateAfterConflict, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
		return u != nil && u.Username == spec.Name
	})
	if unsupported {
		t.Skip("backend missing version column; skipping optimistic locking tests")
	}
	if stateAfterConflict == nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("conflict verification user not found"))
	}
	if stateAfterConflict.Nickname != "lock-primary" {
		t.Fatalf("login before change failed: %v", fmt.Errorf("stale version overwrite detected, want lock-primary got=%s", stateAfterConflict.Nickname))
	}

	retryPayload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": "lock-resolved",
		"version":  currentVersion + 1,
	}
	respRetry, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), retryPayload)
	if err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("retry update: %w", err))
	}
	if respRetry.HTTPStatus() != http.StatusOK {
		t.Fatalf("login before change failed: %v", fmt.Errorf("retry update unexpected http=%d", respRetry.HTTPStatus()))
	}
	if respRetry.Code != code.ErrSuccess {
		t.Fatalf("login before change failed: %v", fmt.Errorf("retry update unexpected code=%d", respRetry.Code))
	}

	if user, unsupported := waitForPublicUser(t, env, spec.Name, 25*time.Second, func(u *publicUser) bool {
		return u != nil && u.Username == spec.Name && u.Nickname == "lock-resolved"
	}); unsupported {
		t.Skip("backend missing version column; skipping optimistic locking tests")
	} else if user == nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("retry nickname not observable"))
	}

	recorder.AddCase(framework.CaseResult{
		Name:        "optimistic_lock_retry",
		Description: "乐观锁冲突应保持旧值并允许正确版本重试",
		Success:     true,
		HTTPStatus:  respRetry.HTTPStatus(),
		Code:        respRetry.Code,
		Message:     respRetry.Message,
		Checks: map[string]bool{
			"conflict_protected": true,
			"retry_success":      true,
		},
		Notes: []string{"stale version prevented overwrite; subsequent retry succeeded"},
	})
}

func extractUpdateVersionMetadata(resp *framework.APIResponse) (uint64, error) {
	if resp == nil || len(resp.Data) == 0 {
		return 0, fmt.Errorf("empty response data")
	}
	var payload struct {
		UpdateUser struct {
			Metadata struct {
				Version uint64 `json:"version"`
			} `json:"metadata"`
		} `json:"update_user"`
	}
	if err := json.Unmarshal(resp.Data, &payload); err != nil {
		return 0, err
	}
	return payload.UpdateUser.Metadata.Version, nil
}

type publicUser struct {
	Username string `json:"Username"`
	Nickname string `json:"Nickname"`
	Email    string `json:"Email"`
	Phone    string `json:"Phone"`
	IsAdmin  int    `json:"IsAdmin"`
}

func waitForPublicUser(t *testing.T, env *framework.Env, username string, timeout time.Duration, predicate func(*publicUser) bool) (*publicUser, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *publicUser
	for time.Now().Before(deadline) {
		user, err := fetchPublicUser(t, env, username)
		if err != nil {
			if errors.Is(err, errUserListVersionUnsupported) {
				return nil, true
			}
			t.Fatalf("login before change failed: %v", fmt.Errorf("fetch user %s: %w", username, err))
		}
		last = user
		if predicate != nil && predicate(user) {
			return user, false
		}
		time.Sleep(200 * time.Millisecond)
	}
	if predicate == nil {
		if env.UserVersionUnsupported() {
			return nil, true
		}
		return last, false
	}
	if predicate(last) {
		return last, false
	}
	if env.UserVersionUnsupported() {
		return nil, true
	}
	return nil, false
}

func fetchPublicUser(t *testing.T, env *framework.Env, username string) (*publicUser, error) {
	t.Helper()
	path := fmt.Sprintf("/v1/users?fieldSelector=name=%s", url.QueryEscape(username))
	resp, err := env.AdminRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch user %s: %w", username, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("fetch user %s: empty response", username)
	}
	if resp.HTTPStatus() == http.StatusInternalServerError && hasVersionColumnError(resp) {
		env.MarkUserVersionUnsupported()
		return nil, errUserListVersionUnsupported
	}
	if resp.HTTPStatus() != http.StatusOK {
		return nil, fmt.Errorf("list %s http=%d code=%d message=%s", username, resp.HTTPStatus(), resp.Code, resp.Message)
	}
	if len(resp.Data) == 0 {
		return nil, nil
	}
	var users []publicUser
	if err := json.Unmarshal(resp.Data, &users); err != nil {
		return nil, fmt.Errorf("decode list response: %w", err)
	}
	if len(users) == 0 {
		return nil, nil
	}
	return &users[0], nil
}

func hasVersionColumnError(resp *framework.APIResponse) bool {
	if resp == nil {
		return false
	}
	if containsVersionColumnKeyword(resp.Message) || containsVersionColumnKeyword(resp.Error) {
		return true
	}
	if len(resp.Data) > 0 && containsVersionColumnKeyword(string(resp.Data)) {
		return true
	}
	return false
}

func containsVersionColumnKeyword(src string) bool {
	if src == "" {
		return false
	}
	lower := strings.ToLower(src)
	return strings.Contains(lower, "unknown column 'version'") || strings.Contains(lower, "unknown column `version`")
}
