package list

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "test/iam-apiserver/user/list"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable list e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type listFunctionalCase struct {
	name        string
	description string
	expectHTTP  []int
	expectCodes []int
	run         func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error)
}

type publicUser struct {
	Username string `json:"username"`
}

func TestListFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "list")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	cases := []listFunctionalCase{
		{
			name:        "list_single_user",
			description: "基于 fieldSelector=name 精确查询应返回目标用户",
			expectHTTP:  []int{http.StatusOK},
			expectCodes: []int{code.ErrSuccess},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				spec := env.NewUserSpec("list_fn_", basePassword)
				env.CreateUserAndWait(t, spec, 5*time.Second)
				defer env.ForceDeleteUserIgnore(spec.Name)

				query := url.Values{}
				query.Set("fieldSelector", "name="+spec.Name)
				query.Set("limit", "10")

				resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+query.Encode(), nil)
				if err != nil {
					return nil, nil, fmt.Errorf("list request: %w", err)
				}

				var users []publicUser
				if len(resp.Data) > 0 {
					if err := json.Unmarshal(resp.Data, &users); err != nil {
						return nil, nil, fmt.Errorf("decode list response: %w", err)
					}
				}

				checks := map[string]bool{
					"user_returned": len(users) == 1,
				}
				if len(users) == 1 {
					checks["username_match"] = users[0].Username == spec.Name
				}
				return resp, checks, nil
			},
		},
		{
			name:        "missing_field_selector",
			description: "缺少 fieldSelector 时应返回参数错误",
			expectHTTP:  []int{http.StatusBadRequest},
			expectCodes: []int{code.ErrInvalidParameter},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				resp, err := env.AdminRequest(http.MethodGet, "/v1/users", nil)
				if err != nil {
					return nil, nil, fmt.Errorf("list without selector: %w", err)
				}
				return resp, map[string]bool{"validation_triggered": true}, nil
			},
		},
		{
			name:        "user_not_found",
			description: "查询不存在用户应返回未找到",
			expectHTTP:  []int{http.StatusNotFound},
			expectCodes: []int{code.ErrUserNotFound},
			run: func(t *testing.T, env *framework.Env) (*framework.APIResponse, map[string]bool, error) {
				query := url.Values{}
				query.Set("fieldSelector", "name="+env.RandomUsername("missing_"))

				resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+query.Encode(), nil)
				if err != nil {
					return nil, nil, fmt.Errorf("list missing user: %w", err)
				}
				return resp, map[string]bool{"user_absent": true}, nil
			},
		},
	}

	for _, tc := range cases {
		caseStart := time.Now()
		resp, checks, err := tc.run(t, env)
		if err != nil {
			t.Fatalf("list functional case %s failed: %v", tc.name, err)
		}
		if resp == nil {
			t.Fatalf("list functional case %s returned nil response", tc.name)
		}

		if !containsInt(tc.expectHTTP, resp.HTTPStatus()) {
			t.Fatalf("list functional case %s unexpected http=%d", tc.name, resp.HTTPStatus())
		}
		if !containsInt(tc.expectCodes, resp.Code) {
			t.Fatalf("list functional case %s unexpected code=%d message=%s", tc.name, resp.Code, resp.Message)
		}

		recorder.AddCase(framework.CaseResult{
			Name:        tc.name,
			Description: tc.description,
			Success:     true,
			HTTPStatus:  resp.HTTPStatus(),
			Code:        resp.Code,
			Message:     resp.Message,
			DurationMS:  time.Since(caseStart).Milliseconds(),
			Checks:      checks,
			Notes:       []string{tc.description},
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
