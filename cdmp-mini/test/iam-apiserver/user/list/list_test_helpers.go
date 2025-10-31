package list

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

type publicUser struct {
	ID        uint64    `json:"id"`
	Username  string    `json:"username"`
	Nickname  string    `json:"nickname"`
	Email     string    `json:"email"`
	Phone     string    `json:"phone"`
	IsAdmin   int       `json:"isAdmin"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Role      string    `json:"role"`
}

type userRecord struct {
	Spec     framework.UserSpec //用户数据的内部定义,相当于表结构
	Snapshot publicUser         //用户数据外部展，，用于接口返回给前端/外部系统的脱敏数据
	Status   int
}

// listDataset 用于集中管理E2E测试所需的所有用户数据，包含各类测试场景专用用户、辅助参数及清理信息
type listDataset struct {
	password              string        // 所有测试用户共用的基础密码（如"InitPassw0rd!"），创建用户时统一使用
	Primary               userRecord    // 主测试用户，用于ID精确匹配、用户名精确匹配等基础筛选场景
	Disabled              userRecord    // 禁用状态用户，用于验证默认查询是否排除禁用用户的场景
	Admin                 userRecord    // 管理员用户（IsAdmin=1），用于验证管理员筛选逻辑的场景
	Contact               userRecord    // 带特征联系方式的用户，用于邮箱/手机号模糊匹配场景
	MultiActive           userRecord    // 多状态组合中的活跃用户（状态=1），配合MultiDisabled验证多状态筛选
	MultiDisabled         userRecord    // 多状态组合中的禁用用户（状态=0），配合MultiActive验证多状态筛选
	Pagination            []userRecord  // 分页测试专用用户列表（至少3个），用于验证limit/offset分页逻辑
	MultiEmailPrefix      string        // 多状态组合测试用户的邮箱共同前缀，确保可通过email[like]同时匹配
	PaginationEmailPrefix string        // 分页测试用户的邮箱共同前缀，用于筛选分页测试数据
	ContactPhonePrefix    string        // 联系方式用户的手机号前缀，用于验证手机号模糊匹配
	cleanup               []string      // 需清理的测试用户ID列表，测试结束后自动删除这些用户
	rnd                   *rand.Rand    // 随机数生成器，用于生成唯一的用户名、邮箱等，避免数据冲突
}

// 测试数据准备
func newListDataset(t *testing.T, env *framework.Env, password string) *listDataset {
	t.Helper()

	ds := &listDataset{
		password: password,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	ds.Primary = ds.create(t, env, "list_fn_primary_", nil)

	ds.Disabled = ds.create(t, env, "list_fn_disabled_", func(spec *framework.UserSpec) {
		spec.Status = 0
		spec.Email = fmt.Sprintf("disabled.%s@example.com", spec.Name)
	})

	ds.Admin = ds.create(t, env, "list_fn_admin_", func(spec *framework.UserSpec) {
		spec.IsAdmin = 1
		spec.Email = fmt.Sprintf("admin.%s@example.com", spec.Name)
	})

	multiPrefix := fmt.Sprintf("multi%d", time.Now().UnixNano())
	ds.MultiEmailPrefix = multiPrefix
	ds.MultiActive = ds.create(t, env, "list_fn_multi_a_", func(spec *framework.UserSpec) {
		spec.Email = fmt.Sprintf("%s-active@example.com", multiPrefix)
	})
	ds.MultiDisabled = ds.create(t, env, "list_fn_multi_d_", func(spec *framework.UserSpec) {
		spec.Status = 0
		spec.Email = fmt.Sprintf("%s-disabled@example.com", multiPrefix)
	})

	ds.Contact = ds.create(t, env, "list_fn_contact_", func(spec *framework.UserSpec) {
		spec.Email = fmt.Sprintf("contact.%s@example.com", spec.Name)
		spec.Phone = ds.randomPhone()
	})
	if len(ds.Contact.Spec.Phone) >= 6 {
		ds.ContactPhonePrefix = ds.Contact.Spec.Phone[:6]
	} else {
		ds.ContactPhonePrefix = ds.Contact.Spec.Phone
	}

	ds.PaginationEmailPrefix = fmt.Sprintf("page%d", time.Now().UnixNano())
	for i := 0; i < 4; i++ {
		idx := i
		rec := ds.create(t, env, fmt.Sprintf("list_fn_page_%d_", idx), func(spec *framework.UserSpec) {
			spec.Email = fmt.Sprintf("%s-%02d@example.com", ds.PaginationEmailPrefix, idx)
		})
		ds.Pagination = append(ds.Pagination, rec)
	}

	return ds
}

// 数据清理
func (d *listDataset) cleanupAll(env *framework.Env) {
	if d == nil {
		return
	}
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

func (d *listDataset) createBatch(t *testing.T, env *framework.Env, prefix string, count int, mutate func(idx int, spec *framework.UserSpec)) []userRecord {
	t.Helper()
	if count <= 0 {
		return nil
	}
	records := make([]userRecord, 0, count)
	for i := 0; i < count; i++ {
		idx := i
		rec := d.create(t, env, fmt.Sprintf("%s%d_", prefix, idx), func(spec *framework.UserSpec) {
			if mutate != nil {
				mutate(idx, spec)
			}
		})
		records = append(records, rec)
	}
	return records
}

func (d *listDataset) create(t *testing.T, env *framework.Env, prefix string, mutate func(*framework.UserSpec)) userRecord {
	t.Helper()
	spec := env.NewUserSpec(prefix, d.password)
	if mutate != nil {
		mutate(&spec)
	}
	env.CreateUserAndWait(t, spec, 5*time.Second)
	d.cleanup = append(d.cleanup, spec.Name)
	snapshot := fetchSingleUser(t, env, spec.Name)
	return userRecord{Spec: spec, Snapshot: snapshot, Status: spec.Status}
}

func (d *listDataset) randomPhone() string {
	if d.rnd == nil {
		d.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return fmt.Sprintf("151%08d", d.rnd.Intn(100000000))
}

func fetchSingleUser(t *testing.T, env *framework.Env, username string) publicUser {
	t.Helper()
	values := url.Values{}
	values.Set("limit", "1")
	values.Set("name", username)
	values.Set("status", "0,1")
	users, resp, err := listUsersWithAdmin(t, env, values)
	if err != nil {
		t.Fatalf("list users for %s: %v", username, err)
	}
	if resp == nil {
		t.Fatalf("list users for %s returned nil response", username)
	}
	if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
		t.Fatalf("list users for %s unexpected http=%d code=%d message=%s", username, resp.HTTPStatus(), resp.Code, resp.Message)
	}
	if len(users) == 0 {
		t.Fatalf("list users for %s returned empty slice", username)
	}
	return users[0]
}

func listUsersWithAdmin(t *testing.T, env *framework.Env, values url.Values) ([]publicUser, *framework.APIResponse, error) {
	t.Helper()
	path := "/v1/users"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	resp, err := env.AdminRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("admin list users: %w", err)
	}
	if resp.HTTPStatus() != http.StatusOK || resp.Code != code.ErrSuccess {
		return nil, resp, nil
	}
	users, err := parsePublicUsers(resp)
	if err != nil {
		return nil, nil, err
	}
	return users, resp, nil
}

func parsePublicUsers(resp *framework.APIResponse) ([]publicUser, error) {
	if resp == nil {
		return nil, fmt.Errorf("nil api response")
	}
	if len(resp.Data) == 0 {
		return []publicUser{}, nil
	}
	var users []publicUser
	if err := json.Unmarshal(resp.Data, &users); err != nil {
		return nil, fmt.Errorf("decode public users: %w", err)
	}
	return users, nil
}
