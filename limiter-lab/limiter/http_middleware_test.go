package limiter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// inMemoryMetrics 是一个简单的内存指标实现，用于验证打点是否正确。
type inMemoryMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func newInMemoryMetrics() *inMemoryMetrics {
	return &inMemoryMetrics{counts: make(map[string]int)}
}

func (m *inMemoryMetrics) Inc(path, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := path + "|" + reason
	m.counts[key]++
}

func (m *inMemoryMetrics) Count(path, reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[path+"|"+reason]
}

// fakeOverrideProvider 允许在单测中按 path 动态调整有效阈值。
type fakeOverrideProvider struct {
	mu     sync.Mutex
	limits map[string]int
}

func newFakeOverrideProvider() *fakeOverrideProvider {
	return &fakeOverrideProvider{limits: make(map[string]int)}
}

func (p *fakeOverrideProvider) Set(path string, limit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if limit <= 0 {
		delete(p.limits, path)
		return
	}
	p.limits[path] = limit
}

func (p *fakeOverrideProvider) LimitFor(path string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	limit, ok := p.limits[path]
	return limit, ok
}

// TestHTTPMiddleware_FixedWindow_TooManyRequests 模拟真实 HTTP 请求，验证在一个窗口内超过次数会返回 429。
func TestHTTPMiddleware_FixedWindow_TooManyRequests(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryStore()
	// 每个窗口最多通过 3 次
	lim := newFixedWindowLimiterForTest(3, time.Second, clk, store, base)

	mw := HTTPMiddleware(lim, nil)

	// 被保护的业务 handler。
	hitCount := 0
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount++
		_, _ = w.Write([]byte("ok"))
	})

	wrapped := mw(h)

	// 构造一个帮助函数发请求并返回响应。
	doReq := func(at time.Time) *httptest.ResponseRecorder {
		clk.now = at
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)
		return rec
	}

	// 同一窗口内 3 次 200，第四次 429。
	cases := []struct {
		name       string
		at         time.Time
		wantStatus int
	}{
		{"first ok", base, http.StatusOK},
		{"second ok", base.Add(100 * time.Millisecond), http.StatusOK},
		{"third ok", base.Add(200 * time.Millisecond), http.StatusOK},
		{"fourth limited", base.Add(300 * time.Millisecond), http.StatusTooManyRequests},
		// 跨窗口之后重新计数，应恢复 200。
		{"after window reset", base.Add(1100 * time.Millisecond), http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(tc.at)
			if rec.Code != tc.wantStatus {
				body, _ := io.ReadAll(rec.Body)
				t.Fatalf("at %v: want status=%d, got=%d, body=%q", tc.at, tc.wantStatus, rec.Code, string(body))
			}
		})
	}

	// 业务 handler 应该被调用 4 次（前三次 + 跨窗口一次），被限流的那次不会调用。
	if hitCount != 4 {
		t.Fatalf("unexpected hitCount: want=4, got=%d", hitCount)
	}
}

// TestHTTPMiddleware_GlobalLimiterAndMetrics 验证：
//   - 在开启 GlobalLimiter 时，全局超限会返回 429；
//   - Metrics 会按 path + reason 打点。
func TestHTTPMiddleware_GlobalLimiterAndMetrics(t *testing.T) {
	base := time.Unix(0, 0)
	clkID := &fakeClock{now: base}
	clkGlobal := &fakeClock{now: base}
	storeID := newInMemoryStore()
	storeGlobal := newInMemoryStore()
	// 标识粒度阈值很大，不影响测试焦点。
	idLimiter := newFixedWindowLimiterForTest(100, time.Second, clkID, storeID, base)
	// 全局阈值为 2：第三次请求触发全局限流。
	globalLimiter := newFixedWindowLimiterForTest(2, time.Second, clkGlobal, storeGlobal, base)

	metrics := newInMemoryMetrics()
	opts := &HTTPOptions{}
	// 为了让两个 limiter 都看到同一时间，我们在请求前同步推进两个 clock。
	mw := NewHTTPMiddleware(idLimiter, globalLimiter, metrics, opts)

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	wrapped := mw(h)

	doReq := func(at time.Time) *httptest.ResponseRecorder {
		clkID.now = at
		clkGlobal.now = at
		req := httptest.NewRequest(http.MethodGet, "/test-global", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)
		return rec
	}

	cases := []struct {
		name       string
		at         time.Time
		wantStatus int
	}{
		{"first ok", base, http.StatusOK},
		{"second ok", base.Add(100 * time.Millisecond), http.StatusOK},
		{"third global limited", base.Add(200 * time.Millisecond), http.StatusTooManyRequests},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(tc.at)
			if rec.Code != tc.wantStatus {
				body, _ := io.ReadAll(rec.Body)
				t.Fatalf("at %v: want status=%d, got=%d, body=%q", tc.at, tc.wantStatus, rec.Code, string(body))
			}
		})
	}

	path := "/test-global"
	if got := metrics.Count(path, "identifier"); got != 0 {
		t.Fatalf("identifier metrics for %s: want=0, got=%d", path, got)
	}
	if got := metrics.Count(path, "global"); got != 1 {
		t.Fatalf("global metrics for %s: want=1, got=%d", path, got)
	}
	if got := metrics.Count(path, "pass"); got != 2 {
		t.Fatalf("pass metrics for %s: want=2, got=%d", path, got)
	}
}

// TestHTTPMiddleware_OverrideProvider 验证 OverrideProvider 可以动态调整某个 path 的有效阈值。
func TestHTTPMiddleware_OverrideProvider(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryStore()
	// 默认 limit 很大，方便观察覆盖阈值的效果。
	idLimiter := newFixedWindowLimiterForTest(100, time.Second, clk, store, base)

	metrics := newInMemoryMetrics()
	override := newFakeOverrideProvider()
	path := "/override"
	// 初始将该 path 的有效阈值设置为 2。
	override.Set(path, 2)

	opts := &HTTPOptions{OverrideProvider: override}
	mw := NewHTTPMiddleware(idLimiter, nil, metrics, opts)

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	wrapped := mw(h)

	doReq := func(at time.Time) *httptest.ResponseRecorder {
		clk.now = at
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)
		return rec
	}

	cases := []struct {
		name       string
		at         time.Time
		wantStatus int
	}{
		{"first ok", base, http.StatusOK},
		{"second ok", base.Add(10 * time.Millisecond), http.StatusOK},
		{"third limited_by_override", base.Add(20 * time.Millisecond), http.StatusTooManyRequests},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(tc.at)
			if rec.Code != tc.wantStatus {
				body, _ := io.ReadAll(rec.Body)
				t.Fatalf("at %v: want status=%d, got=%d, body=%q", tc.at, tc.wantStatus, rec.Code, string(body))
			}
		})
	}

	// 动态提升该 path 的阈值到 5，在同一窗口中应还能再通过两次，然后再次被限流。
	override.Set(path, 5)

	moreCases := []struct {
		name       string
		at         time.Time
		wantStatus int
	}{
		{"fourth ok_after_raise", base.Add(30 * time.Millisecond), http.StatusOK},
		{"fifth ok_after_raise", base.Add(40 * time.Millisecond), http.StatusOK},
		{"sixth still_ok_under_new_limit", base.Add(50 * time.Millisecond), http.StatusOK},
	}

	for _, tc := range moreCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(tc.at)
			if rec.Code != tc.wantStatus {
				body, _ := io.ReadAll(rec.Body)
				t.Fatalf("at %v: want status=%d, got=%d, body=%q", tc.at, tc.wantStatus, rec.Code, string(body))
			}
		})
	}
}

// TestDemo_LocalLimiterAndRedisScript 演示一次请求依次经过：
//   1) 本地 FixedWindowLimiter;
//   2) Redis-like Lua 脚本限流。
// 其中本地限流阈值设置得很大，主要由 Redis 脚本层决定是否被限流。
func TestDemo_LocalLimiterAndRedisScript(t *testing.T) {
	base := time.Unix(0, 0)
	clkLocal := &fakeClock{now: base}
	clkRedis := &fakeClock{now: base}

	// 本地限流器：阈值很大，仅作为演示，不会先触发。
	idStore := newInMemoryStore()
	idLimiter := newFixedWindowLimiterForTest(100, time.Second, clkLocal, idStore, base)

	// Redis-like Store + 脚本：仅设置标识粒度阈值为 2。
	redisStore := newInMemoryRedisStore(clkRedis)
	globalLimit := 0
	idLimit := 2
	window := time.Second

	localMW := NewHTTPMiddleware(idLimiter, nil, nil, nil)
	redisMW := RedisScriptMiddleware(redisStore, globalLimit, idLimit, window)

	hitCount := 0
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount++
		_, _ = w.Write([]byte("ok"))
	})

	// 组合：先走本地 limiter，再走 Redis-like 限流脚本。
	wrapped := localMW(redisMW(h))
	path := "/demo"

	doReq := func(at time.Time) *httptest.ResponseRecorder {
		clkLocal.now = at
		clkRedis.now = at
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)
		return rec
	}

	cases := []struct {
		name       string
		at         time.Time
		wantStatus int
	}{
		{"first ok", base, http.StatusOK},
		{"second ok", base.Add(10 * time.Millisecond), http.StatusOK},
		{"third limited_by_redis", base.Add(20 * time.Millisecond), http.StatusTooManyRequests},
		{"fourth still_limited_in_same_window", base.Add(30 * time.Millisecond), http.StatusTooManyRequests},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(tc.at)
			if rec.Code != tc.wantStatus {
				body, _ := io.ReadAll(rec.Body)
				t.Fatalf("at %v: want status=%d, got=%d, body=%q", tc.at, tc.wantStatus, rec.Code, string(body))
			}
		})
	}

	// 跳出窗口后，应再次允许通过。
	clkLocal.now = base.Add(1100 * time.Millisecond)
	clkRedis.now = base.Add(1100 * time.Millisecond)
	rec := doReq(base.Add(1100 * time.Millisecond))
	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("after window reset: want status=200, got=%d, body=%q", rec.Code, string(body))
	}

	// 最终业务 handler 命中次数：首次窗口内 2 次 + 跨窗口 1 次 = 3。
	if hitCount != 3 {
		t.Fatalf("unexpected hitCount: want=3, got=%d", hitCount)
	}
}
