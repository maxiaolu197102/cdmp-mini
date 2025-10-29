package jwt

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "test/iam-apiserver/user/jwt"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable jwt e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type jwtFunctionalCase struct {
	name        string
	description string
	expectHTTP  []int
	expectCodes []int
	run         func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error)
}

func TestJWTFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "jwt")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	cases := []jwtFunctionalCase{
		{
			name:        "logout_success",
			description: "登录后注销应返回成功并撤销刷新令牌",
			expectHTTP:  []int{http.StatusOK},
			expectCodes: []int{code.ErrSuccess},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("jwt_logout_success_", basePassword)
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

				checks := map[string]bool{
					"logout_success": logoutResp.HTTPStatus() == http.StatusOK && logoutResp.Code == code.ErrSuccess,
				}
				return logoutResp, checks, nil
			},
		},
		{
			name:        "refresh_success",
			description: "登录后使用刷新令牌应获得新的访问令牌",
			expectHTTP:  []int{http.StatusOK},
			expectCodes: []int{code.ErrSuccess},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("jwt_fn_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, loginResp, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}
				if loginResp.HTTPStatus() != http.StatusOK || loginResp.Code != code.ErrSuccess {
					return nil, nil, fmt.Errorf("unexpected login response http=%d code=%d", loginResp.HTTPStatus(), loginResp.Code)
				}

				refreshResp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("refresh request: %w", err)
				}

				checks := map[string]bool{
					"login_success":        true,
					"refresh_token_issued": refreshResp.HTTPStatus() == http.StatusOK,
				}
				return refreshResp, checks, nil
			},
		},
		{
			name:        "refresh_after_logout",
			description: "注销后使用旧刷新令牌应被拒绝",
			expectHTTP:  []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest},
			expectCodes: []int{code.ErrExpired, code.ErrRespCodeRTRevoked, code.ErrTokenInvalid, code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("jwt_logout_", basePassword)
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
					return nil, nil, fmt.Errorf("unexpected logout response http=%d code=%d", logoutResp.HTTPStatus(), logoutResp.Code)
				}

				refreshResp, err := env.Refresh(tokens.AccessToken, tokens.RefreshToken)
				if err != nil {
					return nil, nil, fmt.Errorf("refresh after logout request: %w", err)
				}

				checks := map[string]bool{
					"logout_success": true,
				}
				return refreshResp, checks, nil
			},
		},
		{
			name:        "refresh_missing_token",
			description: "缺少刷新令牌字段应返回参数错误",
			expectHTTP:  []int{http.StatusBadRequest},
			expectCodes: []int{code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("jwt_missing_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				tokens, _, err := env.Login(spec.Name, basePassword)
				if err != nil {
					return nil, nil, fmt.Errorf("login request: %w", err)
				}

				resp, err := env.Refresh(tokens.AccessToken, "")
				if err != nil {
					return nil, nil, fmt.Errorf("refresh missing token request: %w", err)
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
				t.Fatalf("jwt functional case %s failed: %v", tc.name, err)
			}
			if resp == nil {
				t.Fatalf("jwt functional case %s returned nil response", tc.name)
			}

			if !containsInt(tc.expectHTTP, resp.HTTPStatus()) {
				t.Fatalf("jwt functional case %s unexpected http=%d", tc.name, resp.HTTPStatus())
			}
			if !containsInt(tc.expectCodes, resp.Code) {
				t.Fatalf("jwt functional case %s unexpected code=%d message=%s", tc.name, resp.Code, resp.Message)
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
