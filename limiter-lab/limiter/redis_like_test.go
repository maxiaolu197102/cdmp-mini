package limiter

import (
	"testing"
	"time"
)

// TestEvalWriteLimiterScript_IdentifierLimit 验证仅标识粒度限流时的 Lua 语义。
func TestEvalWriteLimiterScript_IdentifierLimit(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryRedisStore(clk)

	globalKey := "g:/path"
	idKey := "id:/path"
	window := time.Second
	globalLimit := 0
	idLimit := 3

	// 前三次不应被限流，且剩余配额依次为 2/1/0。
	tcs := []struct {
		name          string
		at            time.Time
		wantLimited   bool
		wantRemaining int64
	}{
		{"first allowed", base, false, 2},
		{"second allowed", base.Add(100 * time.Millisecond), false, 1},
		{"third allowed", base.Add(200 * time.Millisecond), false, 0},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clk.now = tc.at
			limited, retryAfter, scope := EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
			if limited != tc.wantLimited {
				t.Fatalf("limited mismatch: want=%v, got=%v (retryAfter=%d, scope=%s)", tc.wantLimited, limited, retryAfter, scope)
			}
			if retryAfter != tc.wantRemaining || scope != "ok" {
				t.Fatalf("unexpected (retryAfter,scope): want=(%d,ok), got=(%d,%s)", tc.wantRemaining, retryAfter, scope)
			}
		})
	}

	// 第四次应被标识粒度限流，scope=identifier。
	clk.now = base.Add(300 * time.Millisecond)
	limited, retryAfter, scope := EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
	if !limited || scope != "identifier" {
		t.Fatalf("expected identifier limited, got limited=%v, scope=%s", limited, scope)
	}

	// 跨窗口后应重新开始计数。
	clk.now = base.Add(1100 * time.Millisecond)
	limited, retryAfter, scope = EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
	if limited || scope != "ok" || retryAfter != int64(idLimit-1) {
		t.Fatalf("after window reset: limited=%v, scope=%s, retryAfter=%d", limited, scope, retryAfter)
	}
}

// TestEvalWriteLimiterScript_GlobalLimit 验证同时存在全局阈值和标识阈值时，全局优先生效。
func TestEvalWriteLimiterScript_GlobalLimit(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryRedisStore(clk)

	globalKey := "g:/path"
	idKey := "id:/path"
	window := time.Second
	globalLimit := 2
	idLimit := 5

	// 前两次通过，第三次被全局限流。
	clk.now = base
	limited, _, scope := EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
	if limited || scope != "ok" {
		t.Fatalf("first: expected ok, got limited=%v, scope=%s", limited, scope)
	}

	clk.now = base.Add(100 * time.Millisecond)
	limited, _, scope = EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
	if limited || scope != "ok" {
		t.Fatalf("second: expected ok, got limited=%v, scope=%s", limited, scope)
	}

	clk.now = base.Add(200 * time.Millisecond)
	limited, _, scope = EvalWriteLimiterScript(store, globalKey, idKey, globalLimit, idLimit, window)
	if !limited || scope != "global" {
		t.Fatalf("third: expected global limited, got limited=%v, scope=%s", limited, scope)
	}
}
