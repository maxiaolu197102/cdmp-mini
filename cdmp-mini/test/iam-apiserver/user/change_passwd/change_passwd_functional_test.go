package changepasswd

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/change_passwd"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable change-password e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type functionalScenario struct {
	name        string
	description string
	run         func(t *testing.T, env *framework.Env) (framework.CaseResult, error)
}

func waitForCondition(t *testing.T, timeout, interval time.Duration, fn func() (bool, error)) error {
	t.Helper()
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

func TestChangePasswordFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "change_password")
	defer recorder.Flush(t)

	const initialPassword = "InitPassw0rd!"

	callChangePassword := func(token, username string, payload map[string]string) (*framework.APIResponse, error) {
		path := fmt.Sprintf("/v1/users/%s/change-password", username)
		body := map[string]string{}
		for k, v := range payload {
			body[k] = v
		}
		return env.AuthorizedRequest(http.MethodPut, path, token, body)
	}

	scenarios := []functionalScenario{
		{
			name:        "self_change_success",
			description: "用户可成功修改自身密码并使所有旧会话失效",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_self_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				//defer env.ForceDeleteUserIgnore(spec.Name)

				primaryTokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}
				secondaryTokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("second login: %w", err)
				}

				newPassword := fmt.Sprintf("New%06d!", time.Now().UnixNano()%1000000)

				start := time.Now()
				resp, err := callChangePassword(primaryTokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": newPassword,
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d code=%d message=%s", resp.HTTPStatus(), resp.Code, resp.Message)
				}

				checks := map[string]bool{
					"http_200":              resp.HTTPStatus() == http.StatusOK,
					"code_ok":               resp.Code == code.ErrSuccess,
					"login_new":             false,
					"old_token":             false,
					"old_login":             false,
					"refresh_rev":           false,
					"secondary_refresh_rev": false,
				}

				if err := waitForCondition(t, 5*time.Second, 150*time.Millisecond, func() (bool, error) {
					userResp, err := env.GetUser(primaryTokens.AccessToken, spec.Name)
					if err != nil {
						return false, err
					}
					return userResp.HTTPStatus() != http.StatusOK, nil
				}); err != nil {
					return framework.CaseResult{}, fmt.Errorf("primary access token still valid: %w", err)
				}
				checks["old_token"] = true

				if secondaryTokens != nil {
					if err := waitForCondition(t, 5*time.Second, 150*time.Millisecond, func() (bool, error) {
						secondaryRefreshResp, err := env.Refresh(secondaryTokens.AccessToken, secondaryTokens.RefreshToken)
						if err != nil {
							return false, err
						}
						return secondaryRefreshResp.HTTPStatus() != http.StatusOK, nil
					}); err != nil {
						return framework.CaseResult{}, fmt.Errorf("secondary refresh token remained valid: %w", err)
					}
					checks["secondary_refresh_rev"] = true
				}

				if err := waitForCondition(t, 5*time.Second, 150*time.Millisecond, func() (bool, error) {
					refreshResp, err := env.Refresh(primaryTokens.AccessToken, primaryTokens.RefreshToken)
					if err != nil {
						return false, err
					}
					return refreshResp.HTTPStatus() != http.StatusOK, nil
				}); err != nil {
					return framework.CaseResult{}, fmt.Errorf("refresh token remained valid: %w", err)
				}
				checks["refresh_rev"] = true

				t.Logf("changed password for %s to %s", spec.Name, newPassword)

				const (
					loginMaxAttempts = 4
					loginRetryDelay  = 500 * time.Millisecond
				)
				var (
					newTokens *framework.AuthTokens
					newResp   *framework.APIResponse
					loginErr  error
				)
				time.Sleep(1 * time.Second)
				for attempt := 0; attempt < loginMaxAttempts; attempt++ {
					newTokens, newResp, loginErr = env.Login(spec.Name, newPassword)
					if loginErr == nil {
						checks["login_new"] = true
						break
					}
					if newResp != nil && newResp.Code == code.ErrPasswordIncorrect {
						if attempt < loginMaxAttempts-1 {
							time.Sleep(loginRetryDelay)
						}
						continue
					}
					if newResp != nil && newResp.Code == code.ErrAccountLocked {
						break
					}
					break
				}
				if !checks["login_new"] {
					if newResp != nil {
						return framework.CaseResult{}, fmt.Errorf("login with new password: code=%d message=%s", newResp.Code, newResp.Message)
					}
					return framework.CaseResult{}, fmt.Errorf("login with new password: %w", loginErr)
				}
				_ = newTokens

				if _, oldResp, err := env.Login(spec.Name, initialPassword); err == nil {
					return framework.CaseResult{}, errors.New("old password login unexpectedly succeeded")
				} else if oldResp != nil && oldResp.Code != code.ErrPasswordIncorrect {
					return framework.CaseResult{}, fmt.Errorf("unexpected code for old password login: %d", oldResp.Code)
				}
				checks["old_login"] = true

				return framework.CaseResult{
					Name:        "self_change_success",
					Description: "正常改密后新密码可用，旧令牌/刷新令牌全部失效",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"验证旧访问令牌和刷新令牌全部失效",
						"校验并发会话的刷新令牌撤销",
					},
				}, nil
			},
		},
		{
			name:        "reject_weak_password",
			description: "弱密码应当被拒绝并保持原凭证可用",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_weak_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": "abc12345",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusBadRequest {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d", resp.HTTPStatus())
				}
				if resp.Code != code.ErrInvalidParameter && resp.Code != code.ErrBind {
					return framework.CaseResult{}, fmt.Errorf("unexpected code=%d", resp.Code)
				}

				if _, _, err := env.Login(spec.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("original password login should still succeed: %w", err)
				}

				return framework.CaseResult{
					Name:        "reject_weak_password",
					Description: "密码复杂度不足返回400",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_400":        resp.HTTPStatus() == http.StatusBadRequest,
						"code_invalid":    resp.Code == code.ErrInvalidParameter,
						"login_preserved": true,
					},
				}, nil
			},
		},
		{
			name:        "reject_same_password",
			description: "新旧密码相同需拒绝",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_same_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": initialPassword,
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusBadRequest {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d", resp.HTTPStatus())
				}
				if resp.Code != code.ErrInvalidParameter && resp.Code != code.ErrBind {
					return framework.CaseResult{}, fmt.Errorf("unexpected code=%d", resp.Code)
				}

				if _, _, err := env.Login(spec.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("old password login should still succeed: %w", err)
				}

				return framework.CaseResult{
					Name:        "reject_same_password",
					Description: "新旧密码相同返回400",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_400":        resp.HTTPStatus() == http.StatusBadRequest,
						"code_invalid":    resp.Code == code.ErrInvalidParameter,
						"login_preserved": true,
					},
				}, nil
			},
		},
		{
			name:        "reject_wrong_old_password",
			description: "旧密码错误时应提示旧密码校验失败",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_wrong_old_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": "WrongPass@123",
					"newPassword": "ValidPassw0rd!",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusUnauthorized || resp.Code != code.ErrPasswordIncorrect {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}

				if _, _, err := env.Login(spec.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("original password should still work: %w", err)
				}

				return framework.CaseResult{
					Name:        "reject_wrong_old_password",
					Description: "旧密码校验失败返回401",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_401":                resp.HTTPStatus() == http.StatusUnauthorized,
						"code_password_incorrect": resp.Code == code.ErrPasswordIncorrect,
						"login_preserved":         true,
					},
				}, nil
			},
		},
		{
			name:        "reject_missing_new_password",
			description: "缺少新密码字段返回参数错误",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_missing_new_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": initialPassword,
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusBadRequest {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d", resp.HTTPStatus())
				}
				if resp.Code != code.ErrInvalidParameter && resp.Code != code.ErrBind {
					return framework.CaseResult{}, fmt.Errorf("unexpected code=%d", resp.Code)
				}

				if _, _, err := env.Login(spec.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("original password should still work: %w", err)
				}

				return framework.CaseResult{
					Name:        "reject_missing_new_password",
					Description: "缺少新密码字段返回400",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_400":        resp.HTTPStatus() == http.StatusBadRequest,
						"code_invalid":    resp.Code == code.ErrInvalidParameter,
						"login_preserved": true,
					},
				}, nil
			},
		},
		{
			name:        "reject_whitespace_passwords",
			description: "纯空白密码应被视为无效",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_blank_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": strings.Repeat(" ", 5),
					"newPassword": "   ",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusBadRequest {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d", resp.HTTPStatus())
				}
				if resp.Code != code.ErrInvalidParameter && resp.Code != code.ErrBind {
					return framework.CaseResult{}, fmt.Errorf("unexpected code=%d", resp.Code)
				}

				if _, _, err := env.Login(spec.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("original password should still work: %w", err)
				}

				return framework.CaseResult{
					Name:        "reject_whitespace_passwords",
					Description: "空白密码触发参数校验错误",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_400":        resp.HTTPStatus() == http.StatusBadRequest,
						"code_invalid":    resp.Code == code.ErrInvalidParameter,
						"login_preserved": true,
					},
				}, nil
			},
		},
		{
			name:        "unauthenticated_request",
			description: "缺少认证头应返回未认证",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_unauth_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				start := time.Now()
				resp, err := callChangePassword("", spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": "NewPassw0rd!",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusUnauthorized {
					return framework.CaseResult{}, fmt.Errorf("expected unauthorized, got http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}

				return framework.CaseResult{
					Name:        "unauthenticated_request",
					Description: "未携带令牌被拒绝",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_401": resp.HTTPStatus() == http.StatusUnauthorized,
					},
					Notes: []string{"接口要求Bearer Token"},
				}, nil
			},
		},
		{
			name:        "invalid_token_request",
			description: "非法令牌应被识别并拒绝",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_invalid_token_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				start := time.Now()
				resp, err := callChangePassword("invalid-token", spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": "NewPassw0rd!",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusUnauthorized && resp.HTTPStatus() != http.StatusForbidden {
					return framework.CaseResult{}, fmt.Errorf("unexpected http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}

				return framework.CaseResult{
					Name:        "invalid_token_request",
					Description: "伪造令牌被拒绝",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_401_or_403": resp.HTTPStatus() == http.StatusUnauthorized || resp.HTTPStatus() == http.StatusForbidden,
					},
				}, nil
			},
		},
		{
			name:        "non_admin_cannot_change_others",
			description: "普通用户修改他人密码应被拒绝",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				victim := env.NewUserSpec("cp_target_", initialPassword)
				actor := env.NewUserSpec("cp_actor_", initialPassword)
				env.CreateUserAndWait(t, victim, 5*time.Second)
				env.CreateUserAndWait(t, actor, 5*time.Second)
				defer env.ForceDeleteUserIgnore(victim.Name)
				defer env.ForceDeleteUserIgnore(actor.Name)

				actorTokens, _, err := env.Login(actor.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("actor login: %w", err)
				}

				start := time.Now()
				resp, err := callChangePassword(actorTokens.AccessToken, victim.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": "NewPassw0rd!",
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusForbidden && resp.HTTPStatus() != http.StatusUnauthorized {
					return framework.CaseResult{}, fmt.Errorf("unexpected http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}
				if resp.Code != code.ErrPermissionDenied {
					return framework.CaseResult{}, fmt.Errorf("unexpected code=%d", resp.Code)
				}

				if _, _, err := env.Login(victim.Name, initialPassword); err != nil {
					return framework.CaseResult{}, fmt.Errorf("victim original password should still work: %w", err)
				}

				return framework.CaseResult{
					Name:        "non_admin_cannot_change_others",
					Description: "权限校验拦截普通用户跨账户操作",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"code_permission_denied": resp.Code == code.ErrPermissionDenied,
						"victim_password_intact": true,
					},
				}, nil
			},
		},
		{
			name:        "admin_change_other_user",
			description: "管理员可在提供旧密码的情况下重置其他用户密码",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				target := env.NewUserSpec("cp_admin_target_", initialPassword)
				env.CreateUserAndWait(t, target, 5*time.Second)
				defer env.ForceDeleteUserIgnore(target.Name)

				adminToken := env.AdminTokenOrFail(t)
				newPassword := fmt.Sprintf("AdminReset%06d!", time.Now().UnixNano()%1000000)

				start := time.Now()
				resp, err := callChangePassword(adminToken, target.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": newPassword,
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}

				var (
					targetTokens *framework.AuthTokens
					targetResp   *framework.APIResponse
					loginErr     error
				)
				for attempt := 0; attempt < 5; attempt++ {
					targetTokens, targetResp, loginErr = env.Login(target.Name, newPassword)
					if loginErr == nil {
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
				if loginErr != nil {
					if targetResp != nil {
						return framework.CaseResult{}, fmt.Errorf("target login with new admin password: code=%d message=%s", targetResp.Code, targetResp.Message)
					}
					return framework.CaseResult{}, fmt.Errorf("target login with new admin password: %w", loginErr)
				}
				_ = targetTokens
				if _, oldResp, err := env.Login(target.Name, initialPassword); err == nil {
					return framework.CaseResult{}, errors.New("old password still valid")
				} else if oldResp != nil && oldResp.Code != code.ErrPasswordIncorrect {
					return framework.CaseResult{}, fmt.Errorf("unexpected code for old password login: %d", oldResp.Code)
				}

				// 确保管理员可重新获取令牌以继续后续管理操作
				if _, err := env.AdminRequest(http.MethodGet, "/v1/users", nil); err != nil {
					return framework.CaseResult{}, fmt.Errorf("admin token refresh after change: %w", err)
				}

				return framework.CaseResult{
					Name:        "admin_change_other_user",
					Description: "管理员跨账户改密成功并保持管理能力",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"http_200":              resp.HTTPStatus() == http.StatusOK,
						"target_login_new":      true,
						"old_password_rejected": true,
					},
					Notes: []string{"管理员改密会使当前管理令牌失效，需要自动刷新"},
				}, nil
			},
		},
		{
			name:        "audit_log_recorded",
			description: "密码修改应产生日志与审计记录",
			run: func(t *testing.T, env *framework.Env) (framework.CaseResult, error) {
				t.Helper()
				spec := env.NewUserSpec("cp_audit_", initialPassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, initialPassword)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("initial login: %w", err)
				}

				newPassword := fmt.Sprintf("Audit%06d!", time.Now().UnixNano()%1000000)
				start := time.Now()
				resp, err := callChangePassword(tokens.AccessToken, spec.Name, map[string]string{
					"oldPassword": initialPassword,
					"newPassword": newPassword,
				})
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("change password request: %w", err)
				}
				if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
					return framework.CaseResult{}, fmt.Errorf("unexpected response http=%d code=%d", resp.HTTPStatus(), resp.Code)
				}

				time.Sleep(300 * time.Millisecond)

				events, enabled, _, err := env.AuditEvents(20)
				if err != nil {
					return framework.CaseResult{}, fmt.Errorf("fetch audit events: %w", err)
				}
				if !enabled {
					t.Skip("audit log disabled")
				}

				found := false
				for _, evt := range events {
					if evt.Action == "user.change_password" && evt.ResourceID == spec.Name {
						if strings.ToLower(evt.Outcome) == "success" {
							found = true
							break
						}
					}
				}
				if !found {
					return framework.CaseResult{}, errors.New("audit event not found for change password")
				}

				return framework.CaseResult{
					Name:        "audit_log_recorded",
					Description: "审计日志记录用户改密成功事件",
					Success:     true,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks: map[string]bool{
						"audit_event_found": found,
					},
					Notes: []string{"验证 action=user.change_password 审计事件"},
				}, nil
			},
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			result, err := scenario.run(t, env)
			if err != nil {
				t.Fatalf("scenario %s failed: %v", scenario.name, err)
			}
			recorder.AddCase(result)
		})
	}
}
