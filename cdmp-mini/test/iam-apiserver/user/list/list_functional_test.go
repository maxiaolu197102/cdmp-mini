package list

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/list"

// 环境初始化
func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to enable list e2e tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type listScenario struct {
	name        string
	description string
	run         func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error)
}

func TestListFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "list")
	defer recorder.Flush(t)

	const basePassword = "InitPassw0rd!"

	data := newListDataset(t, env, basePassword)
	defer data.cleanupAll(env)

	scenarios := []listScenario{
		{
			name:        "id_filter_match",
			description: "id 参数应精确匹配用户记录",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("id", strconv.FormatUint(data.Primary.Snapshot.ID, 10))
				values.Set("limit", "1")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Primary.Spec.Name
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"single_result":  len(users) == 1,
					"username_match": len(users) == 1 && users[0].Username == data.Primary.Spec.Name,
				}
				return framework.CaseResult{
					Description: "id 参数应精确匹配用户记录",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "exact_name_filter",
			description: "name 参数应返回目标用户",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("name", data.Primary.Spec.Name)
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Primary.Spec.Name
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"single_result":  len(users) == 1,
					"username_match": len(users) == 1 && users[0].Username == data.Primary.Spec.Name,
				}
				return framework.CaseResult{
					Description: "name 参数应返回目标用户",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "default_status_excludes_disabled",
			description: "默认状态过滤应排除禁用用户",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("name", data.MultiDisabled.Spec.Name)
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 0
				checks := map[string]bool{
					"http_ok":          resp.HTTPStatus() == http.StatusOK,
					"code_ok":          resp.Code == code.ErrSuccess,
					"no_disabled_seen": len(users) == 0,
				}
				return framework.CaseResult{
					Description: "默认状态过滤应排除禁用用户",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "status_override_returns_disabled",
			description: "显式 status=0 应返回禁用用户",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("name", data.MultiDisabled.Spec.Name)
				values.Set("status", "0")
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.MultiDisabled.Spec.Name
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"single_result":  len(users) == 1,
					"username_match": len(users) == 1 && users[0].Username == data.MultiDisabled.Spec.Name,
				}
				return framework.CaseResult{
					Description: "显式 status=0 应返回禁用用户",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "multi_status_combination",
			description: "status=0,1 结合 email[like] 应返回多条记录",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("status", "0,1")
				values.Set("email[like]", data.MultiEmailPrefix)
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				usernames := make(map[string]struct{}, len(users))
				for _, u := range users {
					usernames[u.Username] = struct{}{}
				}
				containsActive := containsAll(usernames, data.MultiActive.Spec.Name)
				containsDisabled := containsAll(usernames, data.MultiDisabled.Spec.Name)
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && containsActive && containsDisabled
				checks := map[string]bool{
					"http_ok":           resp.HTTPStatus() == http.StatusOK,
					"code_ok":           resp.Code == code.ErrSuccess,
					"multi_results":     len(users) >= 2,
					"contains_active":   containsActive,
					"contains_disabled": containsDisabled,
				}
				return framework.CaseResult{
					Description: "status=0,1 结合 email[like] 应返回多条记录",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"query=" + values.Encode(),
						fmt.Sprintf("returned=%d", len(users)),
					},
				}, nil
			},
		},
		{
			name:        "is_admin_true_filter",
			description: "isAdmin=true 应仅返回管理员用户",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("name", data.Admin.Spec.Name)
				values.Set("isAdmin", "true")
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				isAdmin := len(users) == 1 && users[0].IsAdmin == 1 && users[0].Username == data.Admin.Spec.Name
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && isAdmin
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"single_result":  len(users) == 1,
					"username_match": len(users) == 1 && users[0].Username == data.Admin.Spec.Name,
					"is_admin":       len(users) == 1 && users[0].IsAdmin == 1,
				}
				return framework.CaseResult{
					Description: "isAdmin=true 应仅返回管理员用户",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "email_like_filter",
			description: "email[like] 可模糊匹配邮箱",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				local := data.Contact.Spec.Email
				if idx := strings.Index(local, "@"); idx != -1 {
					local = local[:idx]
				}
				values := url.Values{}
				values.Set("email[like]", local)
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				containsContact := false
				for _, u := range users {
					if u.Username == data.Contact.Spec.Name {
						containsContact = true
						break
					}
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && containsContact
				checks := map[string]bool{
					"http_ok":          resp.HTTPStatus() == http.StatusOK,
					"code_ok":          resp.Code == code.ErrSuccess,
					"non_empty":        len(users) >= 1,
					"contains_contact": containsContact,
				}
				return framework.CaseResult{
					Description: "email[like] 可模糊匹配邮箱",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"query=" + values.Encode(),
						fmt.Sprintf("returned=%d", len(users)),
					},
				}, nil
			},
		},
		{
			name:        "phone_like_filter",
			description: "phone[like] 可模糊匹配手机号",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("phone[like]", data.ContactPhonePrefix)
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				containsContact := false
				for _, u := range users {
					if u.Username == data.Contact.Spec.Name {
						containsContact = true
						break
					}
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && containsContact
				checks := map[string]bool{
					"http_ok":          resp.HTTPStatus() == http.StatusOK,
					"code_ok":          resp.Code == code.ErrSuccess,
					"non_empty":        len(users) >= 1,
					"contains_contact": containsContact,
				}
				return framework.CaseResult{
					Description: "phone[like] 可模糊匹配手机号",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"query=" + values.Encode(),
						fmt.Sprintf("returned=%d", len(users)),
					},
				}, nil
			},
		},
		{
			name:        "created_at_range_filter",
			description: "createdAt 窗口应准确筛选用户",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				target := data.Primary.Snapshot.CreatedAt
				values := url.Values{}
				values.Set("name", data.Primary.Spec.Name)
				values.Set("createdAt[gte]", target.Add(-1*time.Minute).Format(time.RFC3339))
				values.Set("createdAt[lte]", target.Add(1*time.Minute).Format(time.RFC3339))
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				inWindow := len(users) == 1 && users[0].Username == data.Primary.Spec.Name
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && inWindow
				checks := map[string]bool{
					"http_ok":         resp.HTTPStatus() == http.StatusOK,
					"code_ok":         resp.Code == code.ErrSuccess,
					"returned_single": len(users) == 1,
					"within_window":   inWindow,
				}
				return framework.CaseResult{
					Description: "createdAt 窗口应准确筛选用户",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"query=" + values.Encode(),
						fmt.Sprintf("target_created_at=%s", target.Format(time.RFC3339)),
					},
				}, nil
			},
		},
		{
			name:        "pagination_offset_limit",
			description: "limit/offset 应按 id DESC 分页",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				if len(data.Pagination) < 3 {
					return framework.CaseResult{}, fmt.Errorf("insufficient pagination data")
				}
				start := time.Now()
				ordered := make([]userRecord, len(data.Pagination))
				copy(ordered, data.Pagination)
				sort.Slice(ordered, func(i, j int) bool {
					return ordered[i].Snapshot.ID > ordered[j].Snapshot.ID
				})
				expected := ordered[1:3]

				values := url.Values{}
				values.Set("status", "1")
				values.Set("email[like]", data.PaginationEmailPrefix)
				values.Set("limit", "2")
				values.Set("offset", "1")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				match := len(users) == 2 && users[0].Username == expected[0].Spec.Name && users[1].Username == expected[1].Spec.Name
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && match
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"returned_two":   len(users) == 2,
					"order_expected": match,
				}
				return framework.CaseResult{
					Description: "limit/offset 应按 id DESC 分页",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes: []string{
						"query=" + values.Encode(),
						fmt.Sprintf("expected=[%s,%s]", expected[0].Spec.Name, expected[1].Spec.Name),
					},
				}, nil
			},
		},
		{
			name:        "field_selector_compatibility",
			description: "fieldSelector=name=xxx 仍保持兼容",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("fieldSelector", fmt.Sprintf("name=%s", data.Primary.Spec.Name))
				values.Set("limit", "5")
				users, resp, err := listUsersWithAdmin(t, env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				if resp == nil {
					return framework.CaseResult{}, fmt.Errorf("nil response")
				}
				success := resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Primary.Spec.Name
				checks := map[string]bool{
					"http_ok":        resp.HTTPStatus() == http.StatusOK,
					"code_ok":        resp.Code == code.ErrSuccess,
					"single_result":  len(users) == 1,
					"username_match": len(users) == 1 && users[0].Username == data.Primary.Spec.Name,
				}
				return framework.CaseResult{
					Description: "fieldSelector=name=xxx 仍保持兼容",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "invalid_status_parameter",
			description: "非法 status 值应返回参数错误",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("status", "not-a-number")
				resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+values.Encode(), nil)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				checks := map[string]bool{
					"http_bad_request": resp.HTTPStatus() == http.StatusBadRequest,
					"code_invalid":     resp.Code == code.ErrInvalidParameter,
				}
				success := resp.HTTPStatus() == http.StatusBadRequest && resp.Code == code.ErrInvalidParameter
				return framework.CaseResult{
					Description: "非法 status 值应返回参数错误",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "invalid_time_parameter",
			description: "非法时间格式应返回参数错误",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("createdAt[gte]", "not-a-time")
				resp, err := env.AdminRequest(http.MethodGet, "/v1/users?"+values.Encode(), nil)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				checks := map[string]bool{
					"http_bad_request": resp.HTTPStatus() == http.StatusBadRequest,
					"code_invalid":     resp.Code == code.ErrInvalidParameter,
				}
				success := resp.HTTPStatus() == http.StatusBadRequest && resp.Code == code.ErrInvalidParameter
				return framework.CaseResult{
					Description: "非法时间格式应返回参数错误",
					Success:     success,
					HTTPStatus:  resp.HTTPStatus(),
					Code:        resp.Code,
					Message:     resp.Message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
		{
			name:        "unauthorized_without_token",
			description: "未携带凭证应返回 401",
			run: func(t *testing.T, env *framework.Env, data *listDataset) (framework.CaseResult, error) {
				start := time.Now()
				values := url.Values{}
				values.Set("limit", "1")
				httpStatus, codeValue, message, err := performUnauthorizedList(env, values)
				duration := time.Since(start)
				if err != nil {
					return framework.CaseResult{}, err
				}
				checks := map[string]bool{
					"http_unauthorized": httpStatus == http.StatusUnauthorized,
				}
				success := httpStatus == http.StatusUnauthorized
				return framework.CaseResult{
					Description: "未携带凭证应返回 401",
					Success:     success,
					HTTPStatus:  httpStatus,
					Code:        codeValue,
					Message:     message,
					DurationMS:  duration.Milliseconds(),
					Checks:      checks,
					Notes:       []string{"query=" + values.Encode()},
				}, nil
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			result, err := sc.run(t, env, data)
			if err != nil {
				t.Fatalf("scenario %s failed: %v", sc.name, err)
			}
			result.Name = sc.name
			if result.Description == "" {
				result.Description = sc.description
			}
			if sc.description != "" {
				result.Notes = append(result.Notes, sc.description)
			}
			recorder.AddCase(result)
			if !result.Success {
				t.Fatalf("scenario %s did not meet expectations", sc.name)
			}
		})
	}
}

func containsAll(items map[string]struct{}, names ...string) bool {
	if len(names) == 0 {
		return true
	}
	if items == nil {
		return false
	}
	for _, name := range names {
		if _, ok := items[name]; !ok {
			return false
		}
	}
	return true
}

func performUnauthorizedList(env *framework.Env, values url.Values) (int, int, string, error) {
	base := env.BaseURL + "/v1/users"
	if encoded := values.Encode(); encoded != "" {
		base += "?" + encoded
	}
	req, err := http.NewRequest(http.MethodGet, base, nil)
	if err != nil {
		return 0, 0, "", err
	}
	resp, err := env.Client.Do(req)
	if err != nil {
		return 0, 0, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, 0, "", err
	}
	var payload struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	return resp.StatusCode, payload.Code, payload.Message, nil
}
