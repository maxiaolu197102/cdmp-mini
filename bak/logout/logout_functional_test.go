package logout

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "test/iam-apiserver/user/logout"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable logout e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type logoutFunctionalCase struct {
	name        string
	description string
	expectHTTP  []int
	expectCodes []int
	run         func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error)
}

func TestLogoutFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "logout")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	cases := []logoutFunctionalCase{
		{
			name:        "logout_success",
			description: "登录后注销应立即失效刷新令牌",
			expectHTTP:  []int{http.StatusOK},
			expectCodes: []int{code.ErrSuccess},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("logout_fn_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				logoutResp, err := env.Logout(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("logout request: %w", err)
				}

				refreshResp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("refresh after logout: %w", err)
				}

				checks := map[string]bool{
					"login_success":       true,
					"logout_success":      logoutResp.HTTPStatus() == http.StatusOK && logoutResp.Code == code.ErrSuccess,
					"refresh_token_block": refreshResp.HTTPStatus() != http.StatusOK,
				}
				return logoutResp, checks, nil
			},
		},
		{
			name:        "logout_with_invalid_refresh",
			description: "使用无效刷新令牌注销应失败",
			expectHTTP:  []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden},
			expectCodes: []int{code.ErrInvalidParameter, code.ErrTokenInvalid, code.ErrRespCodeRTRevoked, code.ErrExpired},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("logout_fn_invalid_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Logout(tokens.AccessToken, "invalid-refresh-token")
				if err != nil {
					return nil, nil, fmt.Errorf("logout invalid refresh token: %w", err)
				}

				checks := map[string]bool{"invalid_refresh_handled": resp.HTTPStatus() != http.StatusOK}
				return resp, checks, nil
			},
		},
		{
			name:        "logout_missing_refresh",
			description: "缺少刷新令牌字段应返回参数错误",
			expectHTTP:  []int{http.StatusBadRequest},
			expectCodes: []int{code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("logout_fn_missing_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Logout(tokens.AccessToken, "")
				if err != nil {
					return nil, nil, fmt.Errorf("logout missing refresh token: %w", err)
				}

				checks := map[string]bool{"login_success": true}
				return resp, checks, nil
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			resp, checks, err := tc.run(t, env)
			duration := time.Since(start)
			if err != nil {
				t.Fatalf("logout functional case %s failed: %v", tc.name, err)
			}
			if resp == nil {
				t.Fatalf("logout functional case %s returned nil response", tc.name)
			}

			if !containsInt(tc.expectHTTP, resp.HTTPStatus()) {
				t.Fatalf("logout functional case %s unexpected http=%d", tc.name, resp.HTTPStatus())
			}
			if !containsInt(tc.expectCodes, resp.Code) {
				t.Fatalf("logout functional case %s unexpected code=%d message=%s", tc.name, resp.Code, resp.Message)
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
	}
}

func containsInt(list []int, target int) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}
