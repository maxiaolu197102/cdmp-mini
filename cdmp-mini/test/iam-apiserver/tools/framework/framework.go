package framework

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/ratelimiter"
	"golang.org/x/time/rate"
)

type APIResponse struct {
	Code       int             `json:"code"`
	Message    string          `json:"message"`
	Error      string          `json:"error"`
	Data       json.RawMessage `json:"data"`
	httpStatus int
}

func (r *APIResponse) HTTPStatus() int {
	if r == nil {
		return 0
	}
	return r.httpStatus
}

type AuthTokens struct {
	Username     string
	UserID       string
	AccessToken  string
	RefreshToken string
}

type LoginOptions struct {
	Headers map[string]string
}

type AuditEvent struct {
	Actor        string         `json:"Actor"`
	ActorID      string         `json:"ActorID"`
	Action       string         `json:"Action"`
	ResourceType string         `json:"ResourceType"`
	ResourceID   string         `json:"ResourceID"`
	Target       string         `json:"Target"`
	Outcome      string         `json:"Outcome"`
	ErrorMessage string         `json:"ErrorMessage"`
	RequestID    string         `json:"RequestID"`
	IP           string         `json:"IP"`
	UserAgent    string         `json:"UserAgent"`
	Metadata     map[string]any `json:"Metadata"`
	OccurredAt   time.Time      `json:"OccurredAt"`
}

// Env 存储E2E测试环境的核心配置与状态信息，包含服务地址、认证信息、客户端实例及限流控制等
type Env struct {
	BaseURL                   string                             // 被测服务的基础URL（如"http://api.iam.com"）
	AdminUsername             string                             // 管理员账号用户名（用于测试中获取管理员权限）
	AdminPassword             string                             // 管理员账号密码（配合用户名登录获取令牌）
	AdminToken                string                             // 管理员访问令牌（缓存的有效令牌，避免重复登录）
	Client                    *http.Client                       // HTTP客户端实例（用于发送测试请求）
	OutputRoot                string                             // 测试输出目录根路径（用于存储测试报告、日志等）
	random                    *rand.Rand                         // 随机数生成器（用于生成唯一测试数据，如随机用户名）
	adminTokenMu              sync.Mutex                         // 管理员令牌的互斥锁（保证多协程下令牌读写安全）
	adminTokenTTL             time.Duration                      // 管理员令牌的有效期（用于判断令牌是否过期）
	adminTokenFetchedAt       time.Time                          // 管理员令牌的获取时间（用于计算是否过期）
	limiters                  map[string]*rate.Limiter           // 按接口/场景划分的限流控制器映射（key为场景标识，value为对应限流器）
	defaultLimiter            *rate.Limiter                      // 默认限流控制器（未指定场景时使用的通用限流规则）
	producerLimiter           *ratelimiter.RateLimiterController // 生产者专用限流控制器（可能用于消息队列等组件的限流）
	rateLimiterInfo           rateLimiterSnapshot                // 限流控制器的快照信息（记录当前限流配置与状态，用于测试验证）
	rateLimiterOnce           sync.Once                          // 限流控制器的初始化同步器（确保限流器只初始化一次）
	lazyAdminLogin            bool                               // 是否启用管理员令牌懒加载（true表示首次需要时才登录获取令牌）
	userVersionMu             sync.RWMutex                       // 用户版本信息的读写锁（多协程下安全读写用户版本数据）
	userVersionMissing        bool                               // 用户版本信息是否缺失（标记是否需要重新获取用户版本）
	clientRateLimiterDisabled atomic.Bool                        // 客户端限流开关（原子布尔值，并发安全地控制是否禁用客户端限流）
}

var (
	ErrUserVersionColumnMissing = errors.New("user version column missing")
	globalUserVersionMissing    atomic.Bool
)

const (
	defaultBaseURL   = "http://192.168.10.8:8088"
	defaultAdminUser = "admin"
	defaultAdminPass = "Admin@2021"
	requestTimeout   = 60 * time.Second
)

func NewEnv(t *testing.T) *Env {
	t.Helper() // 标记此函数为辅助函数

	if os.Getenv("IAM_APISERVER_E2E") == "" {
		t.Fatalf("login before change failed: %v", errors.New("IAM_APISERVER_E2E not set"))
	}

	baseURL := os.Getenv("IAM_APISERVER_BASEURL")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	adminUser := os.Getenv("IAM_APISERVER_ADMIN_USER")
	if adminUser == "" {
		adminUser = defaultAdminUser
	}
	adminPass := os.Getenv("IAM_APISERVER_ADMIN_PASS")
	if adminPass == "" {
		adminPass = defaultAdminPass
	}

	env := &Env{
		BaseURL:       strings.TrimRight(baseURL, "/"),
		AdminUsername: adminUser,
		AdminPassword: adminPass,
		Client:        &http.Client{Timeout: requestTimeout},
		OutputRoot:    "output",
		random:        rand.New(rand.NewSource(time.Now().UnixNano())),
		adminTokenTTL: 4 * time.Minute,
	}

	if flag := strings.TrimSpace(os.Getenv("IAM_APISERVER_LAZY_ADMIN_LOGIN")); flag != "" {
		switch strings.ToLower(flag) {
		case "0", "false", "no", "off":
			env.lazyAdminLogin = false
		default:
			env.lazyAdminLogin = true
		}
	} else {
		env.lazyAdminLogin = true
	}

	if flag := strings.TrimSpace(os.Getenv("IAM_APISERVER_DISABLE_CLIENT_RATE_LIMITER")); flag != "" {
		if parsed, err := strconv.ParseBool(flag); err == nil {
			env.clientRateLimiterDisabled.Store(parsed)
		}
	}

	opts := options.NewServerRunOptions()
	opts.Complete()
	if flag := strings.TrimSpace(os.Getenv("IAM_APISERVER_ENABLE_RATE_LIMITER")); flag != "" {
		if parsed, err := strconv.ParseBool(flag); err == nil {
			opts.EnableRateLimiter = parsed
		} else {
			opts.EnableRateLimiter = false
		}
	} else {
		opts.EnableRateLimiter = false
	}
	kafkaOpts := options.NewKafkaOptions()
	kafkaOpts.Complete()
	applyKafkaOverrides(kafkaOpts)
	env.rateLimiterInfo = rateLimiterSnapshot{
		Enabled:              opts.EnableRateLimiter,
		ClientLimiterEnabled: !env.clientRateLimiterDisabled.Load(),
		StartingRate:         float64(kafkaOpts.StartingRate),
		MinRate:              float64(kafkaOpts.MinRate),
		MaxRate:              float64(kafkaOpts.MaxRate),
		AdjustPeriod:         kafkaOpts.AdjustPeriod.String(),
	}
	if opts.EnableRateLimiter {
		env.limiters = make(map[string]*rate.Limiter)
		if loginLimiter := newRateLimiter(opts.LoginRateLimit, opts.LoginWindow); loginLimiter != nil {
			env.limiters["login"] = loginLimiter
		}
		if writeLimiter := newRateLimiter(opts.WriteRateLimit, time.Minute); writeLimiter != nil {
			env.limiters["write"] = writeLimiter
			env.defaultLimiter = writeLimiter
		}
		env.initProducerLimiter(t, kafkaOpts)
	}
	if !opts.EnableRateLimiter {
		env.rateLimiterInfo.StatsSource = ""
	}

	t.Cleanup(func() {
		if env.producerLimiter != nil {
			env.producerLimiter.Stop()
		}
	})

	env.ensureOutputRoot(t)
	if !env.lazyAdminLogin {
		env.ensureAdminToken(t)
	}
	if globalUserVersionMissing.Load() {
		env.userVersionMu.Lock()
		env.userVersionMissing = true
		env.userVersionMu.Unlock()
	}
	return env
}

// MarkUserVersionUnsupported records that the backing store is missing the expected version column.
func (e *Env) MarkUserVersionUnsupported() {
	e.userVersionMu.Lock()
	if !e.userVersionMissing {
		fmt.Fprintln(os.Stderr, "[warn] user version column appears to be unsupported by backend; falling back to login verification")
	}
	e.userVersionMissing = true
	e.userVersionMu.Unlock()
	globalUserVersionMissing.Store(true)
}

// UserVersionUnsupported reports whether the backend is missing the user version column.
func (e *Env) UserVersionUnsupported() bool {
	e.userVersionMu.RLock()
	defer e.userVersionMu.RUnlock()
	return e.userVersionMissing
}

func (e *Env) ensureOutputRoot(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll(e.OutputRoot, 0o755); err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("create output root: %w", err))
	}
}

func (e *Env) ensureAdminToken(t *testing.T) {
	t.Helper()
	if err := e.ensureAdminTokenInternal(false); err != nil {
		t.Fatalf("login before change failed: %v", err)
	}
}

func (e *Env) ensureAdminTokenInternal(force bool) error {
	e.adminTokenMu.Lock()
	defer e.adminTokenMu.Unlock()
	if e.adminTokenTTL <= 0 {
		e.adminTokenTTL = 4 * time.Minute
	}
	if !force && e.AdminToken != "" && !e.adminTokenFetchedAt.IsZero() {
		if time.Since(e.adminTokenFetchedAt) < e.adminTokenTTL {
			return nil
		}
	}
	tok, resp, err := e.Login(e.AdminUsername, e.AdminPassword)
	if err != nil {
		if resp != nil && isVersionColumnError(resp) {
			e.MarkUserVersionUnsupported()
			return ErrUserVersionColumnMissing
		}
		return fmt.Errorf("admin login: %w", err)
	}
	if tok == nil || tok.AccessToken == "" {
		return fmt.Errorf("admin login returned empty tokens")
	}
	e.AdminToken = tok.AccessToken
	e.adminTokenFetchedAt = time.Now()
	return nil
}

func (e *Env) AdminTokenOrFail(t *testing.T) string {
	t.Helper()
	if err := e.ensureAdminTokenInternal(false); err != nil {
		t.Fatalf("login before change failed: %v", err)
	}
	return e.AdminToken
}

func (e *Env) newRequest(method, path string, body []byte) (*http.Request, error) {
	url := e.BaseURL + path
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (e *Env) do(req *http.Request) (*APIResponse, error) {
	e.waitRateLimit(req.Method, req.URL.Path)
	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var apiResp APIResponse
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &apiResp); err != nil {
			return nil, fmt.Errorf("decode api response: %w: %s", err, string(raw))
		}
	}
	apiResp.httpStatus = resp.StatusCode
	return &apiResp, nil
}

func (e *Env) Login(username, password string) (*AuthTokens, *APIResponse, error) {
	return e.LoginWithOptions(username, password, nil)
}

func (e *Env) LoginWithOptions(username, password string, opts *LoginOptions) (*AuthTokens, *APIResponse, error) {
	payload := map[string]string{
		"username": username,
		"password": password,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	req, err := e.newRequest(http.MethodPost, "/login", body)
	if err != nil {
		return nil, nil, err
	}
	if opts != nil && len(opts.Headers) > 0 {
		for k, v := range opts.Headers {
			req.Header.Set(k, v)
		}
	}
	apiResp, err := e.do(req)
	if err != nil {
		return nil, nil, err
	}
	if apiResp.httpStatus != http.StatusOK {
		return nil, apiResp, fmt.Errorf("unexpected status: %d", apiResp.httpStatus)
	}
	var data struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		UserID       string `json:"user_id"`
	}
	if len(apiResp.Data) > 0 {
		if err := json.Unmarshal(apiResp.Data, &data); err != nil {
			return nil, apiResp, fmt.Errorf("decode login data: %w", err)
		}
	}
	return &AuthTokens{Username: username, AccessToken: data.AccessToken, RefreshToken: data.RefreshToken, UserID: data.UserID}, apiResp, nil
}

func (e *Env) AuditEvents(limit int) ([]AuditEvent, bool, *APIResponse, error) {
	if limit < 0 {
		limit = 0
	}
	path := "/admin/audit/events"
	if limit > 0 {
		path = fmt.Sprintf("%s?limit=%d", path, limit)
	}
	resp, err := e.AdminRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, false, resp, err
	}
	if resp == nil {
		return nil, false, nil, fmt.Errorf("nil response fetching audit events")
	}
	if resp.HTTPStatus() != http.StatusOK {
		return nil, false, resp, fmt.Errorf("unexpected status: %d", resp.HTTPStatus())
	}
	var payload struct {
		Events  []AuditEvent `json:"events"`
		Enabled bool         `json:"enabled"`
	}
	if len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, &payload); err != nil {
			return nil, false, resp, fmt.Errorf("decode audit events: %w", err)
		}
	}
	return payload.Events, payload.Enabled, resp, nil
}

func (e *Env) SetLoginRateLimit(value int) (*APIResponse, error) {
	payload := map[string]int{"value": value}
	return e.AdminRequest(http.MethodPost, "/admin/ratelimit/login", payload)
}

func (e *Env) ResetLoginRateLimit() (*APIResponse, error) {
	return e.AdminRequest(http.MethodDelete, "/admin/ratelimit/login", nil)
}

func (e *Env) LoginRateLimiterEnabled() bool {
	return e.rateLimiterInfo.Enabled
}

func (e *Env) LoginOrFail(t *testing.T, username, password string) *AuthTokens {
	t.Helper()
	tokens, _, err := e.Login(username, password)
	if err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("login %s: %w", username, err))
	}
	if tokens == nil || tokens.AccessToken == "" {
		t.Fatalf("login before change failed: %v", fmt.Errorf("login %s returned empty tokens", username))
	}
	return tokens
}

func (e *Env) AuthorizedRequest(method, path, token string, payload any) (*APIResponse, error) {
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}
	req, err := e.newRequest(method, path, body)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return e.do(req)
}

func (e *Env) AdminRequest(method, path string, payload any) (*APIResponse, error) {
	token, err := e.adminTokenValue()
	if err != nil {
		return nil, err
	}
	resp, reqErr := e.AuthorizedRequest(method, path, token, payload)
	if reqErr != nil {
		return resp, reqErr
	}
	if resp != nil && isAuthExpiredStatus(resp.HTTPStatus()) {
		if refreshErr := e.ensureAdminTokenInternal(true); refreshErr != nil {
			return resp, refreshErr
		}
		newToken, refreshTokenErr := e.adminTokenValue()
		if refreshTokenErr != nil {
			return nil, refreshTokenErr
		}
		return e.AuthorizedRequest(method, path, newToken, payload)
	}
	return resp, nil
}

func (e *Env) adminTokenValue() (string, error) {
	if err := e.ensureAdminTokenInternal(false); err != nil {
		return "", err
	}
	return e.AdminToken, nil
}

func isAuthExpiredStatus(status int) bool {
	return status == http.StatusUnauthorized || status == 419
}

func (e *Env) RandomUsername(prefix string) string {
	const maxLen = 45
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	suffix := fmt.Sprintf("%05d", e.random.Intn(100000))
	reserved := len(ts) + 1 + len(suffix)
	if reserved >= maxLen {
		candidate := ts + suffix
		if len(candidate) > maxLen {
			return candidate[:maxLen]
		}
		return candidate
	}
	if len(prefix) > maxLen-reserved {
		prefix = prefix[:maxLen-reserved]
	}
	return prefix + ts + "_" + suffix
}

type UserSpec struct {
	Name     string
	Nickname string
	Password string
	Email    string
	Phone    string
	Status   int
	IsAdmin  int
}

func (e *Env) NewUserSpec(prefix, password string) UserSpec {
	username := e.RandomUsername(prefix)
	checksum := crc32.ChecksumIEEE([]byte(username)) % 100000000
	return UserSpec{
		Name:     username,
		Nickname: "集成测试用户",
		Password: password,
		Email:    fmt.Sprintf("%s@example.com", username),
		Phone:    fmt.Sprintf("199%08d", checksum),
		Status:   1,
		IsAdmin:  0,
	}
}

func (e *Env) CreateUser(spec UserSpec) (*APIResponse, error) {
	payload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": spec.Nickname,
		"password": spec.Password,
		"email":    spec.Email,
		"phone":    spec.Phone,
		"status":   spec.Status,
		"isAdmin":  spec.IsAdmin,
	}
	//	fmt.Printf("Creating user: %+v\n", payload)
	return e.AdminRequest(http.MethodPost, "/v1/users", payload)
}

func (e *Env) CreateUserAndWait(t *testing.T, spec UserSpec, wait time.Duration) {
	t.Helper()
	resp, err := e.CreateUser(spec)
	if err != nil {
		if errors.Is(err, ErrUserVersionColumnMissing) {
			t.Skipf("backend missing user version column; admin login unsupported (%s)", spec.Name)
			return
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("create user %s: %w", spec.Name, err))
	}
	if resp.HTTPStatus() != http.StatusCreated {
		t.Fatalf("login before change failed: %v", fmt.Errorf("create user http=%d code=%d", resp.HTTPStatus(), resp.Code))
	}
	if wait <= 0 {
		wait = 15 * time.Second
	}
	if wait < 15*time.Second {
		wait = 15 * time.Second
	}
	if err := e.WaitForUser(spec.Name, wait); err != nil {
		if errors.Is(err, ErrUserVersionColumnMissing) {
			t.Logf("[warn] wait for user %s via lookup failed due to missing version column, falling back to login verification", spec.Name)
			if fallbackErr := e.waitForUserByLogin(spec, wait); fallbackErr != nil {
				t.Fatalf("login before change failed: %v", fmt.Errorf("wait for user %s via login: %w", spec.Name, fallbackErr))
			}
			return
		}
		t.Fatalf("login before change failed: %v", fmt.Errorf("wait for user %s: %w", spec.Name, err))
	}
}

func (e *Env) ForceDeleteUser(username string) (*APIResponse, error) {
	path := fmt.Sprintf("/v1/users/%s/force", username)
	req, err := e.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return nil, err
	}
	token, err := e.adminTokenValue()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return e.do(req)
}

func (e *Env) ForceDeleteUserIgnore(username string) {
	if username == "" {
		return
	}
	if _, err := e.ForceDeleteUser(username); err != nil {
		fmt.Fprintf(os.Stderr, "[warn] cleanup user %s failed: %v\n", username, err)
	}
}

func (e *Env) GetUser(token, username string) (*APIResponse, error) {
	path := fmt.Sprintf("/v1/users/%s", username)
	return e.AuthorizedRequest(http.MethodGet, path, token, nil)
}

func (e *Env) UpdateUser(spec UserSpec) (*APIResponse, error) {
	payload := map[string]any{
		"metadata": map[string]string{"name": spec.Name},
		"nickname": spec.Nickname,
		"email":    spec.Email,
		"phone":    spec.Phone,
		"status":   spec.Status,
		"isAdmin":  spec.IsAdmin,
	}
	path := fmt.Sprintf("/v1/users/%s", spec.Name)
	return e.AdminRequest(http.MethodPut, path, payload)
}

func (e *Env) ChangePassword(token, username, oldPassword, newPassword string) (*APIResponse, error) {
	path := fmt.Sprintf("/v1/users/%s/change-password", username)
	payload := map[string]string{
		"oldPassword": oldPassword,
		"newPassword": newPassword,
	}
	return e.AuthorizedRequest(http.MethodPut, path, token, payload)
}

func (e *Env) Logout(token, refreshToken string) (*APIResponse, error) {
	payload := map[string]string{"refresh_token": refreshToken}
	return e.AuthorizedRequest(http.MethodPost, "/logout", token, payload)
}

func (e *Env) Refresh(token, refreshToken string) (*APIResponse, error) {
	payload := map[string]string{"refresh_token": refreshToken}
	return e.AuthorizedRequest(http.MethodPost, "/refresh", token, payload)
}

func (e *Env) ListUsers(token string) (*APIResponse, error) {
	return e.AuthorizedRequest(http.MethodGet, "/v1/users", token, nil)
}

func (e *Env) EnsureOutputDir(t *testing.T, testDir string) string {
	t.Helper()
	full := filepath.Join(testDir, e.OutputRoot)
	fmt.Println("full", full)
	if err := os.MkdirAll(full, 0o755); err != nil {
		t.Fatalf("login before change failed: %v", fmt.Errorf("create output dir: %w", err))
	}
	e.writeRateLimiterSnapshot(t, full)
	return full
}

func (e *Env) WaitForUser(username string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	// 轻量级等待，避免立即查询产生噪音
	time.Sleep(100 * time.Millisecond)

	for {
		resp, err := e.AdminRequest(http.MethodGet, fmt.Sprintf("/v1/users/%s", username), nil)
		if err == nil && resp != nil {
			switch resp.HTTPStatus() {
			case http.StatusOK:
				return nil
			case http.StatusNotFound:
				// 资源尚未可见，继续等待
			case http.StatusInternalServerError:
				if isVersionColumnError(resp) {
					e.MarkUserVersionUnsupported()
					return ErrUserVersionColumnMissing
				}
			default:
				// 其他状态码保留最后一次结果，继续重试
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			status := 0
			if resp != nil {
				status = resp.HTTPStatus()
			}
			return fmt.Errorf("user %s not ready (status=%d)", username, status)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (e *Env) waitForUserByLogin(spec UserSpec, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tokens, _, err := e.Login(spec.Name, spec.Password)
		if err == nil && tokens != nil && tokens.AccessToken != "" {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("login verification timed out for user %s", spec.Name)
}

func isVersionColumnError(resp *APIResponse) bool {
	if resp == nil {
		return false
	}
	if containsVersionColumnError(resp.Message) || containsVersionColumnError(resp.Error) {
		return true
	}
	if len(resp.Data) > 0 && containsVersionColumnError(string(resp.Data)) {
		return true
	}
	return false
}

func containsVersionColumnError(msg string) bool {
	if msg == "" {
		return false
	}
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "unknown column 'version'") || strings.Contains(lower, "unknown column `version`")
}

func (e *Env) waitRateLimit(method, path string) {
	if e.clientRateLimiterDisabled.Load() {
		return
	}
	var limiter *rate.Limiter
	if strings.EqualFold(path, "/login") {
		limiter = e.limiters["login"]
	} else if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete {
		limiter = e.limiters["write"]
	}
	if limiter == nil {
		limiter = e.defaultLimiter
	}
	if limiter == nil {
		goto producerLimiter
	}
	_ = limiter.Wait(context.Background())

producerLimiter:
	if e.producerLimiter == nil {
		return
	}
	_ = e.producerLimiter.Wait(context.Background())
}

// DisableClientRateLimiter stops client-side pacing so performance tests can drive full load.
func (e *Env) DisableClientRateLimiter() {
	e.clientRateLimiterDisabled.Store(true)
}

// EnableClientRateLimiter restores client-side pacing for scenarios that expect it.
func (e *Env) EnableClientRateLimiter() {
	e.clientRateLimiterDisabled.Store(false)
}

// ClientRateLimiterDisabled reports whether client-side pacing is currently disabled.
func (e *Env) ClientRateLimiterDisabled() bool {
	return e.clientRateLimiterDisabled.Load()
}

func newRateLimiter(limit int, window time.Duration) *rate.Limiter {
	if limit <= 0 {
		return nil
	}
	if window <= 0 {
		window = time.Second
	}
	ratePerSecond := float64(limit) / window.Seconds()
	if ratePerSecond <= 0 {
		return nil
	}
	burst := limit
	if burst <= 0 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(ratePerSecond), burst)
}

type rateLimiterSnapshot struct {
	Enabled              bool    `json:"enabled"`
	ClientLimiterEnabled bool    `json:"client_limiter_enabled"`
	StartingRate         float64 `json:"starting_rate"`
	MinRate              float64 `json:"min_rate"`
	MaxRate              float64 `json:"max_rate"`
	AdjustPeriod         string  `json:"adjust_period"`
	StatsSource          string  `json:"stats_source,omitempty"`
}

func (e *Env) initProducerLimiter(t *testing.T, kafkaOpts *options.KafkaOptions) {
	statsURL := fmt.Sprintf("%s/metrics", e.BaseURL)
	statsProvider := e.newProducerStatsProvider(statsURL)
	e.producerLimiter = ratelimiter.NewRateLimiterController(
		float64(kafkaOpts.StartingRate),
		float64(kafkaOpts.MinRate),
		float64(kafkaOpts.MaxRate),
		kafkaOpts.AdjustPeriod,
		statsProvider,
	)
	e.rateLimiterInfo.StatsSource = statsURL
}

func (e *Env) writeRateLimiterSnapshot(t *testing.T, outputDir string) {
	e.rateLimiterOnce.Do(func() {
		snapshot := e.rateLimiterInfo
		snapshot.ClientLimiterEnabled = !e.clientRateLimiterDisabled.Load()
		if !snapshot.Enabled {
			snapshot.StatsSource = ""
		}
		raw, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("marshal rate limiter snapshot: %w", err))
		}
		path := filepath.Join(outputDir, "rate_limiter_snapshot.json")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatalf("login before change failed: %v", fmt.Errorf("write rate limiter snapshot: %w", err))
		}
	})
}

func applyKafkaOverrides(kafkaOpts *options.KafkaOptions) {
	if v := os.Getenv("IAM_APISERVER_KAFKA_STARTING_RATE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			kafkaOpts.StartingRate = parsed
		}
	}
	if v := os.Getenv("IAM_APISERVER_KAFKA_MIN_RATE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			kafkaOpts.MinRate = parsed
		}
	}
	if v := os.Getenv("IAM_APISERVER_KAFKA_MAX_RATE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			kafkaOpts.MaxRate = parsed
		}
	}
	if v := os.Getenv("IAM_APISERVER_KAFKA_ADJUST_PERIOD"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			kafkaOpts.AdjustPeriod = parsed
		}
	}
}

func (e *Env) newProducerStatsProvider(metricsURL string) func() (int, int) {
	client := &http.Client{Timeout: 5 * time.Second}
	return func() (int, int) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
		if err != nil {
			return 0, 0
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, 0
		}
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		total := 0.0
		fail := 0.0
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.HasPrefix(line, "kafka_producer_success_total") {
				total += parseMetricValue(line)
			} else if strings.HasPrefix(line, "kafka_producer_failures_total") {
				value := parseMetricValue(line)
				fail += value
				total += value
			}
		}
		return int(total), int(fail)
	}
}

func parseMetricValue(line string) float64 {
	idx := strings.LastIndex(line, " ")
	if idx == -1 {
		return 0
	}
	valueStr := strings.TrimSpace(line[idx+1:])
	if valueStr == "NaN" || valueStr == "+Inf" {
		return 0
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0
	}
	return value
}
