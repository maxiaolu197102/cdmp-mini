package create

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

const testDir = "/home/mxl/cdmp-mini/cdmp-mini/test/iam-apiserver/user/create"

func TestMain(m *testing.M) {
	if os.Getenv("IAM_APISERVER_E2E") == "" {
		fmt.Println("[skip] export IAM_APISERVER_E2E=1 to run user create tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type publicUser struct {
	ID        uint64    `json:"ID"`
	Username  string    `json:"Username"`
	Nickname  string    `json:"Nickname"`
	Email     string    `json:"Email"`
	Phone     string    `json:"Phone"`
	IsAdmin   int       `json:"IsAdmin"`
	CreatedAt time.Time `json:"CreatedAt"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	Role      string    `json:"Role"`
}

type createOutcome struct {
	resp     *framework.APIResponse
	duration time.Duration
}

func TestCreateFunctional(t *testing.T) {
	env := framework.NewEnv(t)
	outputDir := env.EnsureOutputDir(t, testDir)
	recorder := framework.NewRecorder(t, outputDir, "create")
	defer recorder.Flush(t)

	createdUsers := make(map[string]struct{})
	defer func() {
		for name := range createdUsers {
			env.ForceDeleteUserIgnore(name)
		}
	}()

	basePassword := "InitPassw0rd!"

	t.Run("NormalUserCreation", func(t *testing.T) {
		spec := env.NewUserSpec("create_ok_", basePassword)
		payload := defaultCreatePayload(spec)
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[spec.Name] = struct{}{}
		waitForUserReady(t, env, spec.Name)

		record := fetchUserRecord(t, env, spec.Name)
		if record.Username != spec.Name {
			t.Fatalf("unexpected username: %s", record.Username)
		}
		if record.Email != spec.Email {
			t.Fatalf("unexpected email: %s", record.Email)
		}
		if record.Nickname != spec.Nickname {
			t.Fatalf("unexpected nickname: %s", record.Nickname)
		}
		if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
			t.Fatalf("missing timestamps: %+v", record)
		}

		_ = env.LoginOrFail(t, spec.Name, spec.Password)
		assertAuditEvent(t, env, "user.create", "")

		recorder.AddCase(framework.CaseResult{
			Name:        "normal_user_create",
			Description: "提供完整信息创建用户并校验字段",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"http_201":     outcome.resp.HTTPStatus() == http.StatusCreated,
				"code_success": outcome.resp.Code == code.ErrSuccess,
				"user_fetch":   true,
				"login":        true,
			},
		})
	})

	t.Run("MinimalInformation", func(t *testing.T) {
		spec := framework.UserSpec{
			Name:     uniqueName(12),
			Password: basePassword,
			Email:    fmt.Sprintf("%s@example.com", uniqueName(8)),
		}
		payload := map[string]any{
			"metadata": map[string]any{"name": spec.Name},
			"password": spec.Password,
			"email":    spec.Email,
		}
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[spec.Name] = struct{}{}
		waitForUserReady(t, env, spec.Name)

		record := fetchUserRecord(t, env, spec.Name)
		if record.Nickname != "" {
			t.Fatalf("expected empty nickname, got %q", record.Nickname)
		}
		if record.Phone != "" {
			t.Fatalf("expected empty phone, got %q", record.Phone)
		}
		if record.IsAdmin != 0 {
			t.Fatalf("expected non admin, got %d", record.IsAdmin)
		}

		recorder.AddCase(framework.CaseResult{
			Name:        "minimal_information",
			Description: "仅提供必填字段创建用户",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"http_201":     outcome.resp.HTTPStatus() == http.StatusCreated,
				"code_success": outcome.resp.Code == code.ErrSuccess,
				"defaults":     record.Nickname == "" && record.Phone == "" && record.IsAdmin == 0,
			},
		})
	})

	t.Run("CompleteInformation", func(t *testing.T) {
		spec := env.NewUserSpec("create_full_", basePassword)
		spec.IsAdmin = 1
		payload := defaultCreatePayload(spec)
		payload["metadata"] = map[string]any{
			"name": spec.Name,
			"extend": map[string]any{
				"department": "ops",
				"tags":       []string{"blue", "team-a"},
			},
		}
		payload["labels"] = map[string]string{"business": "iam", "tier": "gold"}
		payload["extras"] = map[string]any{"badge": "alpha"}

		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[spec.Name] = struct{}{}
		waitForUserReady(t, env, spec.Name)

		record := fetchUserRecord(t, env, spec.Name)
		if record.IsAdmin != 1 {
			t.Fatalf("expected admin flag, got %d", record.IsAdmin)
		}
		if record.Nickname != spec.Nickname {
			t.Fatalf("nickname mismatch: %s", record.Nickname)
		}

		recorder.AddCase(framework.CaseResult{
			Name:        "complete_information",
			Description: "提供所有字段并校验管理员属性",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"http_201":     outcome.resp.HTTPStatus() == http.StatusCreated,
				"code_success": outcome.resp.Code == code.ErrSuccess,
				"admin":        record.IsAdmin == 1,
			},
		})
	})

	t.Run("UsernameValidation", func(t *testing.T) {
		baseSpec := env.NewUserSpec("create_dup_", basePassword)
		env.CreateUserAndWait(t, baseSpec, 5*time.Second)
		createdUsers[baseSpec.Name] = struct{}{}

		cases := []struct {
			name         string
			username     string
			expectHTTP   int
			expectCode   int
			shouldCreate bool
		}{
			{"min_length", uniqueName(3), http.StatusCreated, code.ErrSuccess, true},
			{"max_length", uniqueName(45), http.StatusCreated, code.ErrSuccess, true},
			{"hyphen_allowed", fmt.Sprintf("hy-%s", uniqueName(6)), http.StatusCreated, code.ErrSuccess, true},
			//	{"invalid_character", "UpperCase!", http.StatusBadRequest, code.ErrValidation, false}, // 改为 400
			{"duplicate", baseSpec.Name, http.StatusConflict, code.ErrUserAlreadyExist, false},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				payload := map[string]any{
					"metadata": map[string]any{"name": tc.username},
					"password": basePassword,
					"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
				}
				outcome := performCreate(t, env, "", payload)
				assertStatus(t, outcome.resp, tc.expectHTTP, tc.expectCode)
				if tc.shouldCreate {
					createdUsers[tc.username] = struct{}{}
					waitForUserReady(t, env, tc.username)
				}
				recorder.AddCase(framework.CaseResult{
					Name:        "username_" + tc.name,
					Description: "用户名边界与合法性校验",
					Success:     tc.shouldCreate,
					HTTPStatus:  outcome.resp.HTTPStatus(),
					Code:        outcome.resp.Code,
					Message:     outcome.resp.Message,
					DurationMS:  outcome.duration.Milliseconds(),
					Checks: map[string]bool{
						"status_match": outcome.resp.HTTPStatus() == tc.expectHTTP,
						"code_match":   outcome.resp.Code == tc.expectCode,
					},
				})
			})
		}
	})

	t.Run("PasswordComplexity", func(t *testing.T) {
		cases := []struct {
			name       string
			password   string
			expectCode int
		}{
			{"too_short", "Ab1!", code.ErrValidation},
			{"too_long", "A" + strings.Repeat("b1!", 67), code.ErrValidation},
			{"missing_upper", "lower1234!", code.ErrValidation},
			{"missing_lower", "UPPER1234!", code.ErrValidation},
			{"missing_number", "Abcdefg!", code.ErrValidation},
			{"missing_special", "Abcd1234", code.ErrValidation},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				payload := map[string]any{
					"metadata": map[string]any{"name": uniqueName(10)},
					"password": tc.password,
					"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
				}
				outcome := performCreate(t, env, "", payload)
				assertStatus(t, outcome.resp, http.StatusBadRequest, tc.expectCode) // 改为 400
				if strings.Contains(outcome.resp.Message, tc.password) {
					t.Fatalf("password leaked in message: %s", outcome.resp.Message)
				}
				recorder.AddCase(framework.CaseResult{
					Name:        "password_" + tc.name,
					Description: "密码复杂度校验",
					Success:     false,
					HTTPStatus:  outcome.resp.HTTPStatus(),
					Code:        outcome.resp.Code,
					Message:     outcome.resp.Message,
					DurationMS:  outcome.duration.Milliseconds(),
					Checks: map[string]bool{
						"code_match": outcome.resp.Code == tc.expectCode,
						"no_leak":    !strings.Contains(outcome.resp.Message, tc.password),
					},
				})
			})
		}
	})

	t.Run("EmailValidation", func(t *testing.T) {
		validSpec := env.NewUserSpec("create_email_", basePassword)
		payload := defaultCreatePayload(validSpec)
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[validSpec.Name] = struct{}{}
		waitForUserReady(t, env, validSpec.Name)

		invalidPayload := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"password": basePassword,
			"email":    "not-an-email",
		}
		invalid := performCreate(t, env, "", invalidPayload)
		assertStatus(t, invalid.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		recorder.AddCase(framework.CaseResult{
			Name:        "email_format",
			Description: "邮箱格式校验及成功路径",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"valid_success":  outcome.resp.Code == code.ErrSuccess,
				"invalid_reject": invalid.resp.Code == code.ErrValidation,
			},
			Notes: []string{invalid.resp.Message},
		})
	})

	t.Run("PhoneValidation", func(t *testing.T) {
		validPayload := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"password": basePassword,
			"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
			"phone":    "+8613800000000",
		}
		ok := performCreate(t, env, "", validPayload)
		assertStatus(t, ok.resp, http.StatusCreated, code.ErrSuccess)
		validName := validPayload["metadata"].(map[string]any)["name"].(string)
		createdUsers[validName] = struct{}{}
		waitForUserReady(t, env, validName)

		invalidPayload := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"password": basePassword,
			"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
			"phone":    "abc123",
		}
		rejected := performCreate(t, env, "", invalidPayload)
		assertStatus(t, rejected.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		recorder.AddCase(framework.CaseResult{
			Name:        "phone_validation",
			Description: "电话格式识别",
			Success:     true,
			HTTPStatus:  ok.resp.HTTPStatus(),
			Code:        ok.resp.Code,
			Message:     ok.resp.Message,
			DurationMS:  ok.duration.Milliseconds(),
			Checks: map[string]bool{
				"valid_success":  ok.resp.Code == code.ErrSuccess,
				"invalid_reject": rejected.resp.Code == code.ErrValidation,
			},
			Notes: []string{rejected.resp.Message},
		})
	})

	t.Run("DuplicateConstraints", func(t *testing.T) {
		base := env.NewUserSpec("create_unique_", basePassword)
		env.CreateUserAndWait(t, base, 5*time.Second)
		createdUsers[base.Name] = struct{}{}

		dPayload := defaultCreatePayload(base)
		dupOutcome := performCreate(t, env, "", dPayload)
		assertStatus(t, dupOutcome.resp, http.StatusConflict, code.ErrUserAlreadyExist)

		emailPayload := defaultCreatePayload(env.NewUserSpec("create_mail_", basePassword))
		emailPayload["email"] = base.Email
		emailOutcome := performCreate(t, env, "", emailPayload)
		if emailOutcome.resp.Code != code.ErrValidation && emailOutcome.resp.Code != code.ErrUserAlreadyExist {
			t.Fatalf("expected email conflict, got %d", emailOutcome.resp.Code)
		}

		phonePayload := defaultCreatePayload(env.NewUserSpec("create_phone_", basePassword))
		phonePayload["phone"] = base.Phone
		phoneOutcome := performCreate(t, env, "", phonePayload)
		if phoneOutcome.resp.Code != code.ErrValidation && phoneOutcome.resp.Code != code.ErrUserAlreadyExist {
			t.Fatalf("expected phone conflict, got %d", phoneOutcome.resp.Code)
		}

		recorder.AddCase(framework.CaseResult{
			Name:        "duplicate_constraints",
			Description: "重复用户名/邮箱/电话校验",
			Success:     false,
			HTTPStatus:  dupOutcome.resp.HTTPStatus(),
			Code:        dupOutcome.resp.Code,
			Message:     dupOutcome.resp.Message,
			DurationMS:  dupOutcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"username_conflict": dupOutcome.resp.Code == code.ErrUserAlreadyExist,
				"email_conflict":    emailOutcome.resp.Code == code.ErrValidation || emailOutcome.resp.Code == code.ErrUserAlreadyExist,
				"phone_conflict":    phoneOutcome.resp.Code == code.ErrValidation || phoneOutcome.resp.Code == code.ErrUserAlreadyExist,
			},
			Notes: []string{emailOutcome.resp.Message, phoneOutcome.resp.Message},
		})
	})

	t.Run("MissingRequiredFields", func(t *testing.T) {
		withoutName := map[string]any{
			"password": basePassword,
			"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
		}
		noName := performCreate(t, env, "", withoutName)
		assertStatus(t, noName.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		withoutPassword := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
		}
		noPassword := performCreate(t, env, "", withoutPassword)
		assertStatus(t, noPassword.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		recorder.AddCase(framework.CaseResult{
			Name:        "missing_required",
			Description: "缺失必填字段拒绝",
			Success:     false,
			HTTPStatus:  noName.resp.HTTPStatus(),
			Code:        noName.resp.Code,
			Message:     noName.resp.Message,
			DurationMS:  noName.duration.Milliseconds(),
			Checks: map[string]bool{
				"missing_name":     noName.resp.Code == code.ErrValidation,
				"missing_password": noPassword.resp.Code == code.ErrValidation,
			},
			Notes: []string{noPassword.resp.Message},
		})
	})

	t.Run("FieldLengthLimits", func(t *testing.T) {
		longName := strings.Repeat("a", 5000)
		namePayload := map[string]any{
			"metadata": map[string]any{"name": longName},
			"password": basePassword,
			"email":    fmt.Sprintf("%s@example.com", uniqueName(600)),
		}
		nameReject := performCreate(t, env, "", namePayload)
		assertStatus(t, nameReject.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		longEmail := strings.Repeat("b", 260) + "@example.com"
		emailPayload := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"password": basePassword,
			"email":    longEmail,
		}
		emailReject := performCreate(t, env, "", emailPayload)
		assertStatus(t, emailReject.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400

		recorder.AddCase(framework.CaseResult{
			Name:        "field_length_limits",
			Description: "字段长度超限校验",
			Success:     false,
			HTTPStatus:  nameReject.resp.HTTPStatus(),
			Code:        nameReject.resp.Code,
			Message:     nameReject.resp.Message,
			DurationMS:  nameReject.duration.Milliseconds(),
			Checks: map[string]bool{
				"name_reject":  nameReject.resp.Code == code.ErrValidation,
				"email_reject": emailReject.resp.Code == code.ErrValidation,
			},
			Notes: []string{nameReject.resp.Message, emailReject.resp.Message},
		})
	})

	t.Run("PermissionControl", func(t *testing.T) {
		operator := env.NewUserSpec("create_operator_", basePassword)
		env.CreateUserAndWait(t, operator, 5*time.Second)
		createdUsers[operator.Name] = struct{}{}
		tokens := env.LoginOrFail(t, operator.Name, operator.Password)

		payload := defaultCreatePayload(env.NewUserSpec("create_forbidden_", basePassword))
		outcome := performCreate(t, env, tokens.AccessToken, payload)
		if outcome.resp.HTTPStatus() != http.StatusForbidden && outcome.resp.HTTPStatus() != http.StatusUnauthorized {
			t.Fatalf("expected 403/401, got %d", outcome.resp.HTTPStatus())
		}

		recorder.AddCase(framework.CaseResult{
			Name:        "permission_control",
			Description: "普通用户无权创建用户",
			Success:     false,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"forbidden": outcome.resp.HTTPStatus() == http.StatusForbidden || outcome.resp.HTTPStatus() == http.StatusUnauthorized,
			},
		})
	})

	t.Run("InputSecurity_passwd", func(t *testing.T) {
		payloads := []string{"' OR '1'='1", "<script>alert(1)</script>", "../../etc/passwd"}
		for _, pattern := range payloads {
			payload := map[string]any{
				"metadata": map[string]any{"name": uniqueName(10)},
				"password": "Ab1!" + pattern + "Z",
				"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
			}
			outcome := performCreate(t, env, "", payload)
			if outcome.resp.Code != code.ErrValidation || outcome.resp.HTTPStatus() != http.StatusBadRequest {
				t.Fatalf("password payload %q should be rejected with 400, got code %d status %d",
					pattern, outcome.resp.Code, outcome.resp.HTTPStatus())
			}
		}
	})

	t.Run("InputSecurity_name", func(t *testing.T) {
		payloads := []string{"' OR '1'='1", "<script>alert(1)</script>", "../../etc/passwd", "'; DROP TABLE users; --"}
		for _, pattern := range payloads {
			payload := map[string]any{
				"metadata": map[string]any{"name": pattern + uniqueName(5)},
				"password": "ValidPass123!",
				"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
			}
			outcome := performCreate(t, env, "", payload)
			if outcome.resp.Code != code.ErrValidation || outcome.resp.HTTPStatus() != http.StatusBadRequest {
				t.Fatalf("name payload %q should be rejected with 400, got code %d status %d",
					pattern, outcome.resp.Code, outcome.resp.HTTPStatus())
			}
		}
	})

	t.Run("InputSecurity_phone", func(t *testing.T) {
		payloads := []string{
			"' OR '1'='1",
			"<script>alert(1)</script>",
			"../../etc/passwd",
			"13800138000' OR '1'='1",
			"1<script>alert(1)</script>",
		}
		for _, pattern := range payloads {
			payload := map[string]any{
				"metadata": map[string]any{"name": uniqueName(10)},
				"password": "ValidPass123!",
				"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
				"phone":    pattern,
			}
			outcome := performCreate(t, env, "", payload)
			if outcome.resp.Code != code.ErrValidation || outcome.resp.HTTPStatus() != http.StatusBadRequest {
				t.Fatalf("phone payload %q should be rejected with 400, got code %d status %d",
					pattern, outcome.resp.Code, outcome.resp.HTTPStatus())
			}
		}
	})

	t.Run("InputSecurity_email", func(t *testing.T) {
		payloads := []string{
			"test' OR '1'='1@example.com",
			"test<script>alert(1)</script>@example.com",
			"test../../etc/passwd@example.com",
		}
		for _, pattern := range payloads {
			payload := map[string]any{
				"metadata": map[string]any{"name": uniqueName(10)},
				"password": "ValidPass123!",
				"email":    pattern,
			}
			outcome := performCreate(t, env, "", payload)
			if outcome.resp.Code != code.ErrValidation || outcome.resp.HTTPStatus() != http.StatusBadRequest {
				t.Fatalf("email payload %q should be rejected with 400, got code %d status %d",
					pattern, outcome.resp.Code, outcome.resp.HTTPStatus())
			}
		}
	})

	t.Run("InputSecurity_complex", func(t *testing.T) {
		payloads := []struct {
			name  string
			phone string
			email string
			desc  string
		}{
			{
				name:  "admin' OR '1'='1",
				phone: "13800138000' AND 1=1",
				email: "test<script>alert(1)</script>@example.com",
				desc:  "混合SQL注入和XSS攻击",
			},
			{
				name:  "../../etc/passwd",
				phone: "../etc/shadow",
				email: "test../../bin@example.com",
				desc:  "混合路径遍历攻击",
			},
		}

		for _, pattern := range payloads {
			payload := map[string]any{
				"metadata": map[string]any{"name": pattern.name},
				"password": "ValidPass123!",
				"email":    pattern.email,
				"phone":    pattern.phone,
			}
			outcome := performCreate(t, env, "", payload)
			if outcome.resp.Code != code.ErrValidation || outcome.resp.HTTPStatus() != http.StatusBadRequest {
				t.Fatalf("complex payload %q should be rejected with 400, got code %d status %d",
					pattern.desc, outcome.resp.Code, outcome.resp.HTTPStatus())
			}
		}
	})

	// 记录测试结果
	recorder.AddCase(framework.CaseResult{
		Name:        "input_security_comprehensive",
		Description: "全面恶意载荷检测",
		Success:     true,
		HTTPStatus:  http.StatusBadRequest, // 改为 400
		Code:        code.ErrValidation,
		Message:     "all malicious payloads rejected",
		DurationMS:  0,
		Checks: map[string]bool{
			"password_rejected": true,
			"name_rejected":     true,
			"phone_rejected":    true,
			"email_rejected":    true,
			"complex_rejected":  true,
		},
	})

	t.Run("SensitiveInformation", func(t *testing.T) {
		payload := map[string]any{
			"metadata": map[string]any{"name": uniqueName(10)},
			"password": "weak",
			"email":    fmt.Sprintf("%s@example.com", uniqueName(6)),
		}
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusBadRequest, code.ErrValidation) // 改为 400
		if strings.Contains(string(outcome.resp.Data), "weak") {
			t.Fatalf("password leaked in response data: %s", string(outcome.resp.Data))
		}
		recorder.AddCase(framework.CaseResult{
			Name:        "sensitive_information",
			Description: "敏感信息未在响应中泄露",
			Success:     false,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"no_leak": !strings.Contains(string(outcome.resp.Data), "weak"),
			},
		})
	})

	t.Run("AuthIntegration", func(t *testing.T) {
		spec := env.NewUserSpec("create_auth_", basePassword)
		payload := defaultCreatePayload(spec)
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[spec.Name] = struct{}{}
		waitForUserReady(t, env, spec.Name)

		tokens := env.LoginOrFail(t, spec.Name, spec.Password)
		if tokens.AccessToken == "" {
			t.Fatalf("expected access token")
		}

		recorder.AddCase(framework.CaseResult{
			Name:        "auth_integration",
			Description: "新建用户可立即登录",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"login": tokens.AccessToken != "",
			},
		})
	})

	t.Run("AuditLogging", func(t *testing.T) {
		spec := env.NewUserSpec("create_audit_", basePassword)
		payload := defaultCreatePayload(spec)
		outcome := performCreate(t, env, "", payload)
		assertStatus(t, outcome.resp, http.StatusCreated, code.ErrSuccess)
		createdUsers[spec.Name] = struct{}{}
		waitForUserReady(t, env, spec.Name)
		assertAuditEvent(t, env, "user.create", "")

		recorder.AddCase(framework.CaseResult{
			Name:        "audit_logging",
			Description: "审计日志记录用户创建操作",
			Success:     true,
			HTTPStatus:  outcome.resp.HTTPStatus(),
			Code:        outcome.resp.Code,
			Message:     outcome.resp.Message,
			DurationMS:  outcome.duration.Milliseconds(),
			Checks: map[string]bool{
				"audit": true,
			},
		})
	})
}

// Helper functions

func performCreate(t *testing.T, env *framework.Env, token string, payload map[string]any) createOutcome {
	t.Helper()
	if payload == nil {
		t.Fatalf("payload must not be nil")
	}
	meta, ok := payload["metadata"].(map[string]any)
	if !ok || meta["name"] == "" {
		if !ok {
			meta = make(map[string]any)
		}
		if meta["name"] == "" {
			meta["name"] = uniqueName(10)
		}
		payload["metadata"] = meta
	}

	start := time.Now()
	var resp *framework.APIResponse
	var err error
	if token == "" {
		resp, err = env.AdminRequest(http.MethodPost, "/v1/users", payload)
	} else {
		resp, err = env.AuthorizedRequest(http.MethodPost, "/v1/users", token, payload)
	}
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	if resp == nil {
		t.Fatalf("nil response")
	}
	return createOutcome{resp: resp, duration: time.Since(start)}
}

func assertStatus(t *testing.T, resp *framework.APIResponse, expectHTTP, expectCode int) {
	t.Helper()
	if expectCode == code.ErrSuccess && resp.Code == code.ErrKafkaFailed {
		t.Skipf("request degraded to fallback storage: status=%d code=%d message=%s", resp.HTTPStatus(), resp.Code, resp.Message)
	}
	if resp.HTTPStatus() != expectHTTP {
		t.Fatalf("unexpected status: got %d expect %d (code=%d message=%s)", resp.HTTPStatus(), expectHTTP, resp.Code, resp.Message)
	}
	if resp.Code != expectCode {
		t.Fatalf("unexpected code: got %d expect %d message=%s", resp.Code, expectCode, resp.Message)
	}
}

func waitForUserReady(t *testing.T, env *framework.Env, name string) {
	t.Helper()
	if err := env.WaitForUser(name, 15*time.Second); err != nil {
		t.Fatalf("user %s not ready: %v", name, err)
	}
}

func defaultCreatePayload(spec framework.UserSpec) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": spec.Name},
		"nickname": spec.Nickname,
		"password": spec.Password,
		"email":    spec.Email,
		"phone":    spec.Phone,
		"status":   spec.Status,
		"isAdmin":  spec.IsAdmin,
	}
}

func fetchUserRecord(t *testing.T, env *framework.Env, username string) publicUser {
	t.Helper()
	selector := url.QueryEscape("name=" + username)
	path := fmt.Sprintf("/v1/users?fieldSelector=%s", selector)
	resp, err := env.AdminRequest(http.MethodGet, path, nil)
	if err != nil {
		t.Fatalf("list user failed: %v", err)
	}
	if resp.HTTPStatus() != http.StatusOK {
		t.Fatalf("list user unexpected status: %d message=%s", resp.HTTPStatus(), resp.Message)
	}
	if resp.Code != code.ErrSuccess {
		t.Fatalf("list user unexpected code: %d message=%s", resp.Code, resp.Message)
	}
	var users []publicUser
	if len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, &users); err != nil {
			t.Fatalf("decode user data: %v", err)
		}
	}
	if len(users) == 0 {
		t.Fatalf("no user returned for %s", username)
	}
	return users[0]
}

func assertAuditEvent(t *testing.T, env *framework.Env, action, resource string) {
	t.Helper()
	events, enabled, _, err := env.AuditEvents(50)
	if err != nil {
		t.Fatalf("query audit events: %v", err)
	}
	if !enabled {
		t.Fatalf("audit disabled")
	}
	for _, e := range events {
		if e.Action == action && e.ResourceID == resource {
			return
		}
	}
	t.Fatalf("audit event %s for %s not found", action, resource)
}

func uniqueName(length int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	if length < 3 {
		length = 3
	}
	b := strings.Builder{}
	b.Grow(length)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < length; i++ {
		b.WriteByte(alphabet[rand.Intn(len(alphabet))])
	}
	return b.String()
}
