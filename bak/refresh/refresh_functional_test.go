package refresh

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "test/iam-apiserver/user/refresh"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable refresh e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type refreshFunctionalCase struct {
	name        string
	description string
	expectHTTP  []int
	expectCodes []int
	run         func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error)
}

func TestRefreshFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "refresh")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	cases := []refreshFunctionalCase{
		{
			name:        "refresh_success",
			description: "登录后使用刷新令牌应返回新的访问令牌",
			expectHTTP:  []int{http.StatusOK},
			expectCodes: []int{code.ErrSuccess},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("refresh_fn_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("refresh request: %w", err)
				}

				var data struct {
					AccessToken  string `json:"access_token"`
					RefreshToken string `json:"refresh_token"`
				}
				if len(resp.Data) > 0 {
					if err := json.Unmarshal(resp.Data, &data); err != nil {
						return nil, nil, fmt.Errorf("decode refresh response: %w", err)
					}
				}

				checks := map[string]bool{
					"login_success":   true,
					"refresh_issued":  resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess,
					"tokens_returned": data.AccessToken != "" && data.RefreshToken != "",
				}
				return resp, checks, nil
			},
		},
		{
			name:        "refresh_with_invalid_token",
			description: "使用无效令牌刷新应失败",
			expectHTTP:  []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden},
			expectCodes: []int{code.ErrInvalidParameter, code.ErrTokenInvalid, code.ErrRespCodeRTRevoked, code.ErrExpired},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("refresh_fn_invalid_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Refresh(tokens.AccessToken, "invalid-refresh-token")
				if err != nil {
					return nil, nil, fmt.Errorf("refresh invalid token: %w", err)
				}

				checks := map[string]bool{"invalid_refresh_handled": resp.HTTPStatus() != http.StatusOK}
				return resp, checks, nil
			},
		},
		{
			name:        "refresh_missing_token",
			description: "缺少刷新令牌应返回参数错误",
			expectHTTP:  []int{http.StatusBadRequest},
			expectCodes: []int{code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("refresh_fn_missing_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Refresh(tokens.AccessToken, "")
				if err != nil {
					return nil, nil, fmt.Errorf("refresh missing token: %w", err)
				}

				checks := map[string]bool{"login_success": true}
				return resp, checks, nil
			},
		},
		{
			name:        "refresh_after_logout",
			description: "注销后使用旧刷新令牌应被拒绝",
			expectHTTP:  []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest},
			expectCodes: []int{code.ErrExpired, code.ErrRespCodeRTRevoked, code.ErrTokenInvalid, code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("refresh_fn_logout_", basePassword)
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
				if logoutResp.Code != code.ErrSuccess {
					return nil, nil, fmt.Errorf("unexpected logout response code=%d", logoutResp.Code)
				}

				resp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("refresh after logout: %w", err)
				}

				checks := map[string]bool{
					"logout_success": true,
				}
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
				t.Fatalf("refresh functional case %s failed: %v", tc.name, err)
			}
			if resp == nil {
				t.Fatalf("refresh functional case %s returned nil response", tc.name)
			}

			if !containsInt(tc.expectHTTP, resp.HTTPStatus()) {
				t.Fatalf("refresh functional case %s unexpected http=%d", tc.name, resp.HTTPStatus())
			}
			if !containsInt(tc.expectCodes, resp.Code) {
				t.Fatalf("refresh functional case %s unexpected code=%d message=%s", tc.name, resp.Code, resp.Message)
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
