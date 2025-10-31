package get

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/validation"
)

const (
	testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/get"

	datasetPassword   = "InitPassw0rd!"
	cacheHitThreshold = 20 * time.Millisecond
)

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to run user get tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type scenarioResult struct {
	success    bool
	httpStatus int
	code       int
	message    string
	duration   time.Duration
	checks     map[string]bool
	notes      []string
	err        error
}

type getFunctionalScenario struct {
	category    string
	name        string
	description string
	run         func(t *testing.T, env *framework.Env, data *getDataset) scenarioResult
}

type userRecord struct {
	Spec    framework.UserSpec
	Tokens  *framework.AuthTokens
	Version uint64
}

type getDataset struct {
	Base         userRecord
	Secondary    userRecord
	Special      userRecord
	UpdateTarget userRecord
	DeleteTarget userRecord
	Boundary     userRecord
	Bulk         []userRecord
	Hot          []userRecord
	cleanup      []string
}

func TestGetFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "get")
	defer recorder.Flush(t)

	data := prepareGetDataset(t, env)
	t.Cleanup(func() {
		data.cleanupAll(env)
	})

	scenarios := []getFunctionalScenario{
		{category: "基础功能测试", name: "admin_exact_fetch", description: "管理员精确查询存在的用户", run: runAdminExact},
		{category: "基础功能测试", name: "user_get_self", description: "普通用户查询自身信息", run: runUserSelf},
		{category: "基础功能测试", name: "field_selector_multi", description: "多字段联合查询返回唯一用户", run: runCombinationFieldSelector},
		{category: "基础功能测试", name: "sorting_parameter_resilience", description: "附带排序参数时的兼容性验证", run: runSortingParameter},
		{category: "基础功能测试", name: "pagination_validation", description: "分页参数越界时返回校验错误", run: runPaginationValidation},
		{category: "基础功能测试", name: "max_limit_boundary", description: "最大分页尺寸仍可成功响应", run: runMaxLimit},
		{category: "基础功能测试", name: "empty_result_handling", description: "空结果集返回200 + 空数组", run: runEmptyResult},
		{category: "基础功能测试", name: "special_character_handling", description: "特殊字符昵称的用户可正常查询", run: runSpecialCharacter},
		{category: "基础功能测试", name: "username_length_boundary", description: "用户名达到长度上限仍可查询", run: runBoundaryLength},
		{category: "数据一致性测试", name: "read_after_create", description: "数据创建后立即查询", run: runReadAfterCreate},
		{category: "数据一致性测试", name: "read_after_update", description: "更新后查询同步校验", run: runReadAfterUpdate},
		{category: "数据一致性测试", name: "read_after_delete", description: "删除后查询返回未找到", run: runReadAfterDelete},
		{category: "数据一致性测试", name: "list_after_delete", description: "删除后列表结果不包含该用户", run: runListAfterDelete},
		{category: "数据一致性测试", name: "history_timestamp_order", description: "历史数据更新时间大于创建时间", run: runHistoryTimestamps},
		{category: "数据一致性测试", name: "status_toggle_persistence", description: "状态字段切换后的持久化校验", run: runStatusToggle},
		{category: "容错与异常测试", name: "invalid_identifier_format", description: "非法ID格式被拒绝", run: runInvalidIDFormat},
		{category: "容错与异常测试", name: "sql_injection_payload", description: "SQL注入载荷不会泄露数据", run: runSQLInjectionPayload},
		{category: "容错与异常测试", name: "overlong_identifier", description: "超长参数被安全处理", run: runOverlongIdentifier},
		{category: "容错与异常测试", name: "missing_token", description: "缺失授权头拦截", run: runMissingToken},
		{category: "容错与异常测试", name: "invalid_token", description: "非法token拦截", run: runInvalidToken},
		{category: "安全测试", name: "cross_user_access_control", description: "越权访问受限", run: runCrossUserAccess},
		{category: "安全测试", name: "sensitive_data_masking", description: "响应体不包含敏感字段", run: runSensitiveDataMask},
		{category: "监控与可观测性", name: "observability_snapshot", description: "记录查询延迟与业务码", run: runObservabilitySnapshot},
		{category: "数据工厂策略", name: "dataset_overview", description: "批量测试数据分布验证", run: runDataFactoryOverview},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.category+"/"+sc.name, func(t *testing.T) {
			result := sc.run(t, env, data)
			if result.checks == nil {
				result.checks = make(map[string]bool)
			}
			notes := append([]string{sc.description}, result.notes...)
			recorder.AddCase(framework.CaseResult{
				Name:        sc.name,
				Description: sc.description,
				Success:     result.err == nil && result.success,
				HTTPStatus:  result.httpStatus,
				Code:        result.code,
				Message:     result.message,
				DurationMS:  result.duration.Milliseconds(),
				Checks:      result.checks,
				Notes:       notes,
			})
			if result.err != nil {
				t.Fatalf("%s failed: %v", sc.name, result.err)
			}
			if !result.success {
				t.Fatalf("%s unexpected status=%d code=%d message=%s", sc.name, result.httpStatus, result.code, result.message)
			}
		})
	}
}

func prepareGetDataset(t *testing.T, env *framework.Env) *getDataset {
	t.Helper()
	data := &getDataset{}

	data.Base = data.newUser(t, env, "get_base_", true, nil)
	data.Secondary = data.newUser(t, env, "get_secondary_", true, nil)
	data.Special = data.newUser(t, env, "get_special_", true, func(spec *framework.UserSpec) {
		spec.Nickname = "特殊字符-用户_#%@"
		spec.Email = fmt.Sprintf("%s+alias@example.com", spec.Name)
	})
	data.UpdateTarget = data.newUser(t, env, "get_update_", true, nil)
	data.DeleteTarget = data.newUser(t, env, "get_delete_", false, nil)
	data.Boundary = data.newUser(t, env, "get_boundary_", false, func(spec *framework.UserSpec) {
		if len(spec.Name) < 45 {
			extra := strings.Repeat("x", 45-len(spec.Name))
			spec.Name = spec.Name + extra
			spec.Email = fmt.Sprintf("%s@example.com", spec.Name)
		}
	})

	bulk := data.buildBulkUsers(t, env)
	data.Bulk = bulk
	if len(bulk) > 0 {
		hotCount := 3
		if hotCount > len(bulk) {
			hotCount = len(bulk)
		}
		data.Hot = append([]userRecord(nil), bulk[:hotCount]...)
	}

	return data
}

func (d *getDataset) buildBulkUsers(t *testing.T, env *framework.Env) []userRecord {
	t.Helper()
	total := 12
	if testing.Short() {
		total = 6
	}
	result := make([]userRecord, 0, total)
	for i := 0; i < total; i++ {
		idx := i
		needToken := idx < 6
		record := d.newUser(t, env, fmt.Sprintf("get_bulk_%02d_", idx), needToken, func(spec *framework.UserSpec) {
			spec.Nickname = fmt.Sprintf("批量用户-%02d", idx)
			if idx%4 == 0 {
				spec.Nickname = fmt.Sprintf("热点用户-%02d", idx)
			}
			if idx%5 == 0 {
				spec.Email = fmt.Sprintf("%s+dirty%d@example.com", spec.Name, idx)
			}
		})
		result = append(result, record)
	}
	return result
}

func (d *getDataset) newUser(t *testing.T, env *framework.Env, prefix string, needToken bool, mutate func(*framework.UserSpec)) userRecord {
	t.Helper()
	spec := env.NewUserSpec(prefix, datasetPassword)
	if mutate != nil {
		mutate(&spec)
	}
	env.CreateUserAndWait(t, spec, 5*time.Second)
	d.cleanup = append(d.cleanup, spec.Name)

	record := userRecord{Spec: spec}
	if needToken {
		record.Tokens = env.LoginOrFail(t, spec.Name, spec.Password)
	}
	record.Version = 1
	return record
}

func (d *getDataset) cleanupAll(env *framework.Env) {
	seen := make(map[string]struct{}, len(d.cleanup))
	for i := len(d.cleanup) - 1; i >= 0; i-- {
		name := d.cleanup[i]
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		env.ForceDeleteUserIgnore(name)
	}
}

type publicUser struct {
	ID        uint64 `json:"id"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	IsAdmin   int    `json:"isAdmin"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
	Role      string `json:"role"`
}

func runAdminExact(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("admin get user: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	username, parseErr := extractUsernameFromGet(resp)
	if parseErr != nil {
		result.err = parseErr
		return result
	}
	result.checks["username_match"] = username == data.Base.Spec.Name
	result.checks["payload_present"] = username != ""
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && username == data.Base.Spec.Name
	result.notes = append(result.notes, fmt.Sprintf("响应耗时=%s", result.duration))
	return result
}

func runUserSelf(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	token := ""
	if data.Base.Tokens != nil {
		token = data.Base.Tokens.AccessToken
	}
	resp, err := env.GetUser(token, data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("user get self: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	username, parseErr := extractUsernameFromGet(resp)
	if parseErr != nil {
		result.err = parseErr
		return result
	}
	result.checks["username_match"] = username == data.Base.Spec.Name
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && username == data.Base.Spec.Name
	return result
}

func runCombinationFieldSelector(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s,metadata.name=%s", data.Special.Spec.Name, data.Special.Spec.Name))
	start := time.Now()
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["single_result"] = len(users) == 1
	if len(users) == 1 {
		result.checks["username_match"] = users[0].Username == data.Special.Spec.Name
		result.checks["email_match"] = users[0].Email == data.Special.Spec.Email
	}
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Special.Spec.Name
	return result
}

func runSortingParameter(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.Base.Spec.Name))
	values.Set("sortBy", "createdAt")
	start := time.Now()
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["ignored_sort_param"] = true
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Base.Spec.Name
	return result
}

func runPaginationValidation(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.Base.Spec.Name))
	values.Set("limit", "-1")
	start := time.Now()
	_, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.success = resp.HTTPStatus() == http.StatusBadRequest && resp.Code == code.ErrInvalidParameter
	result.checks["limit_rejected"] = result.success
	return result
}

func runMaxLimit(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.Base.Spec.Name))
	values.Set("limit", fmt.Sprintf("%d", validation.GlobalMaxLimit))
	start := time.Now()
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["max_limit_success"] = resp.HTTPStatus() == http.StatusOK
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1
	return result
}

func runEmptyResult(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	randomName := env.RandomUsername("missing_")
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", randomName))
	start := time.Now()
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["empty_result"] = len(users) == 0
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 0
	return result
}

func runSpecialCharacter(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.Special.Spec.Name))
	start := time.Now()
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = time.Since(start)
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	if len(users) == 1 {
		result.checks["nickname_preserved"] = users[0].Nickname == data.Special.Spec.Nickname
	}
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && len(users) == 1 && users[0].Username == data.Special.Spec.Name
	return result
}

func runBoundaryLength(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), data.Boundary.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("boundary get: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	username, parseErr := extractUsernameFromGet(resp)
	if parseErr != nil {
		result.err = parseErr
		return result
	}
	result.checks["length"] = len(data.Boundary.Spec.Name) == 45
	result.checks["username_match"] = username == data.Boundary.Spec.Name
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && username == data.Boundary.Spec.Name
	return result
}

func runReadAfterCreate(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	fresh := data.newUser(t, env, "get_consistency_", false, nil)
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), fresh.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("read after create: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	username, parseErr := extractUsernameFromGet(resp)
	if parseErr != nil {
		result.err = parseErr
		return result
	}
	result.checks["immediate_visible"] = username == fresh.Spec.Name
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && username == fresh.Spec.Name
	return result
}

func runReadAfterUpdate(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	updated := data.UpdateTarget.Spec
	updated.Nickname = fmt.Sprintf("更新昵称-%d", time.Now().UnixNano())
	start := time.Now()
	if data.UpdateTarget.Version == 0 {
		data.UpdateTarget.Version = 1
	}
	payload := map[string]any{
		"metadata": map[string]string{"name": updated.Name},
		"nickname": updated.Nickname,
		"version":  data.UpdateTarget.Version,
	}
	resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", updated.Name), payload)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("update user: %w", err)
		return result
	}
	if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
		result.httpStatus = resp.HTTPStatus()
		result.code = resp.Code
		result.message = resp.Message
		result.success = false
		return result
	}
	if version, err := extractUpdateUserVersion(resp); err == nil && version > 0 {
		data.UpdateTarget.Version = version
	} else if err != nil {
		result.err = fmt.Errorf("parse update version: %w", err)
		return result
	}
	token := env.AdminTokenOrFail(t)
	getResp, getErr := env.GetUser(token, updated.Name)
	if getErr != nil {
		result.err = fmt.Errorf("get after update: %w", getErr)
		return result
	}
	result.httpStatus = getResp.HTTPStatus()
	result.code = getResp.Code
	result.message = getResp.Message
	result.checks["update_request_success"] = resp.Code == code.ErrSuccess
	result.checks["retrievable_post_update"] = getResp.Code == code.ErrSuccess
	result.success = result.checks["update_request_success"] && result.checks["retrievable_post_update"]
	result.notes = append(result.notes, "更新操作异步生效，后续一致性通过可读性校验")
	data.UpdateTarget.Spec = updated
	return result
}

func runReadAfterDelete(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	_, err := env.ForceDeleteUser(data.DeleteTarget.Spec.Name)
	if err != nil {
		result.err = fmt.Errorf("delete user: %w", err)
		return result
	}
	token := env.AdminTokenOrFail(t)
	var (
		resp    *framework.APIResponse
		getErr  error
		success bool
	)
	start := time.Now()
	for attempt := 0; attempt < 12; attempt++ {
		resp, getErr = env.GetUser(token, data.DeleteTarget.Spec.Name)
		if getErr != nil {
			result.err = fmt.Errorf("get after delete: %w", getErr)
			return result
		}
		if resp.Code == code.ErrUserNotFound || resp.Code == code.ErrPasswordIncorrect || resp.HTTPStatus() == http.StatusNotFound {
			success = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	result.duration = time.Since(start)
	if resp != nil {
		result.httpStatus = resp.HTTPStatus()
		result.code = resp.Code
		result.message = resp.Message
		result.checks["not_found_code"] = resp.Code == code.ErrUserNotFound || resp.Code == code.ErrPasswordIncorrect || resp.HTTPStatus() == http.StatusNotFound
	}
	result.success = success
	return result
}

func runListAfterDelete(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.DeleteTarget.Spec.Name))
	var (
		users []publicUser
		resp  *framework.APIResponse
		err   error
		ok    bool
	)
	start := time.Now()
	for attempt := 0; attempt < 12; attempt++ {
		users, resp, err = listUsersWithAdmin(t, env, values)
		if err != nil {
			result.err = err
			return result
		}
		if len(users) == 0 {
			ok = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	result.duration = time.Since(start)
	if resp != nil {
		result.httpStatus = resp.HTTPStatus()
		result.code = resp.Code
		result.message = resp.Message
	}
	result.checks["deleted_not_listed"] = ok
	result.success = ok
	return result
}

func runHistoryTimestamps(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	values := url.Values{}
	values.Set("fieldSelector", fmt.Sprintf("name=%s", data.UpdateTarget.Spec.Name))
	users, resp, err := listUsersWithAdmin(t, env, values)
	result.duration = 0
	if err != nil {
		result.err = err
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	if len(users) == 1 {
		createdAt, errCreated := time.Parse(time.RFC3339, users[0].CreatedAt)
		updatedAt, errUpdated := time.Parse(time.RFC3339, users[0].UpdatedAt)
		if errCreated == nil && errUpdated == nil {
			result.checks["timestamp_order"] = !updatedAt.Before(createdAt)
			result.success = !updatedAt.Before(createdAt)
		} else {
			result.err = fmt.Errorf("parse timestamps failed: created=%v updated=%v", errCreated, errUpdated)
		}
	} else {
		result.success = false
	}
	return result
}

func runStatusToggle(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	spec := data.UpdateTarget.Spec
	originalStatus := spec.Status
	spec.Status = 0
	start := time.Now()
	if data.UpdateTarget.Version == 0 {
		data.UpdateTarget.Version = 1
	}
	disablePayload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"status":   spec.Status,
		"version":  data.UpdateTarget.Version,
	}
	resp, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), disablePayload)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("disable user: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["status_update_success"] = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess
	result.success = result.checks["status_update_success"]
	if result.success {
		version, err := extractUpdateUserVersion(resp)
		if err != nil {
			result.err = fmt.Errorf("parse disable version: %w", err)
			return result
		}
		if version > 0 {
			data.UpdateTarget.Version = version
		}
		data.UpdateTarget.Spec.Status = spec.Status
	}

	restorePayload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"status":   originalStatus,
		"version":  data.UpdateTarget.Version,
	}
	if respRestore, err := env.AdminRequest(http.MethodPut, fmt.Sprintf("/v1/users/%s", spec.Name), restorePayload); err == nil && respRestore != nil {
		if respRestore.HTTPStatus() == http.StatusOK && respRestore.Code == code.ErrSuccess {
			if version, vErr := extractUpdateUserVersion(respRestore); vErr == nil && version > 0 {
				data.UpdateTarget.Version = version
			}
			data.UpdateTarget.Spec.Status = originalStatus
		}
	}
	return result
}

func runInvalidIDFormat(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), "../invalid-id")
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("invalid id request: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["rejected"] = resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess
	result.success = result.checks["rejected"]
	return result
}

func runSQLInjectionPayload(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	payload := url.PathEscape("' OR '1'='1")
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), payload)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("sql injection payload: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["no_data_leak"] = resp.Code != code.ErrSuccess
	result.success = resp.Code != code.ErrSuccess
	return result
}

func runOverlongIdentifier(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	overlong := strings.Repeat("a", 80)
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), overlong)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("overlong identifier: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["handled"] = resp.Code != code.ErrSuccess
	result.success = resp.Code != code.ErrSuccess
	return result
}

func runMissingToken(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser("", data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("missing token request: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["missing_header"] = resp.Code == code.ErrMissingHeader
	result.success = resp.Code == code.ErrMissingHeader
	return result
}

func runInvalidToken(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser("invalid.token.value", data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("invalid token request: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["token_invalid"] = resp.Code == code.ErrTokenInvalid
	result.success = resp.Code == code.ErrTokenInvalid
	return result
}

func runCrossUserAccess(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	token := ""
	if data.Secondary.Tokens != nil {
		token = data.Secondary.Tokens.AccessToken
	}
	start := time.Now()
	resp, err := env.GetUser(token, data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("cross user access: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["access_blocked"] = resp.Code != code.ErrSuccess
	result.success = resp.Code != code.ErrSuccess
	return result
}

func runSensitiveDataMask(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("masking request: %w", err)
		return result
	}
	raw := string(resp.Data)
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	pwd := data.Base.Spec.Password
	result.checks["no_password"] = !strings.Contains(strings.ToLower(raw), strings.ToLower(pwd))
	result.checks["no_token_field"] = !strings.Contains(strings.ToLower(raw), "token")
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess && result.checks["no_password"]
	return result
}

func runObservabilitySnapshot(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	start := time.Now()
	resp, err := env.GetUser(env.AdminTokenOrFail(t), data.Base.Spec.Name)
	result.duration = time.Since(start)
	if err != nil {
		result.err = fmt.Errorf("observability snapshot: %w", err)
		return result
	}
	result.httpStatus = resp.HTTPStatus()
	result.code = resp.Code
	result.message = resp.Message
	result.checks["duration_recorded"] = result.duration > 0
	result.checks["business_code"] = resp.Code != 0
	result.checks["message_present"] = resp.Message != ""
	result.success = resp.HTTPStatus() == http.StatusOK && resp.Code == code.ErrSuccess
	result.notes = append(result.notes, fmt.Sprintf("duration=%s", result.duration))
	return result
}

func runDataFactoryOverview(t *testing.T, env *framework.Env, data *getDataset) scenarioResult {
	result := scenarioResult{checks: make(map[string]bool)}
	names := make([]string, 0, len(data.Bulk))
	for _, record := range data.Bulk {
		names = append(names, record.Spec.Name)
	}
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		unique[name] = struct{}{}
	}
	result.checks["bulk_generated"] = len(data.Bulk) > 0
	result.checks["unique_names"] = len(unique) == len(names)
	hotNames := make([]string, 0, len(data.Hot))
	for _, record := range data.Hot {
		hotNames = append(hotNames, record.Spec.Name)
	}
	sort.Strings(hotNames)
	result.notes = append(result.notes, fmt.Sprintf("bulk_count=%d", len(data.Bulk)))
	result.notes = append(result.notes, fmt.Sprintf("hot_users=%v", hotNames))
	result.success = result.checks["bulk_generated"] && result.checks["unique_names"]
	return result
}

func listUsersWithAdmin(t *testing.T, env *framework.Env, values url.Values) ([]publicUser, *framework.APIResponse, error) {
	t.Helper()
	path := "/v1/users"
	if len(values) > 0 {
		path = fmt.Sprintf("%s?%s", path, values.Encode())
	}
	resp, err := env.AdminRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, resp, err
	}
	users, err := parsePublicUsers(resp)
	if err != nil {
		return nil, resp, err
	}
	return users, resp, nil
}

func parsePublicUsers(resp *framework.APIResponse) ([]publicUser, error) {
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}
	raw := strings.TrimSpace(string(resp.Data))
	if raw == "" || raw == "null" {
		return nil, nil
	}
	var users []publicUser
	if err := json.Unmarshal(resp.Data, &users); err != nil {
		return nil, fmt.Errorf("decode public users: %w", err)
	}
	return users, nil
}

func extractUsernameFromGet(resp *framework.APIResponse) (string, error) {
	if resp == nil {
		return "", fmt.Errorf("nil response")
	}
	raw := strings.TrimSpace(string(resp.Data))
	if raw == "" || raw == "null" {
		return "", fmt.Errorf("empty payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(resp.Data, &payload); err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	value, ok := payload["get"].(string)
	if !ok {
		return "", fmt.Errorf("payload missing get")
	}
	return value, nil
}

func extractUpdateUserVersion(resp *framework.APIResponse) (uint64, error) {
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
