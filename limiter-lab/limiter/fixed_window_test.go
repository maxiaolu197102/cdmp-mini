package limiter

import (
	"testing"
	"time"
)

// fakeClock 实现 Clock 接口，允许在单测里手动推进时间。
type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

// TestFixedWindow_SingleKey 验证同一个 key 在单个窗口内的计数行为。
func TestFixedWindow_SingleKey(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryStore()
	lim := newFixedWindowLimiterForTest(3, time.Second, clk, store, base)

	tcs := []struct {
		name  string
		at    time.Time
		allow bool
	}{
		{"first in window", base, true},
		{"second in window", base.Add(100 * time.Millisecond), true},
		{"third in window", base.Add(200 * time.Millisecond), true},
		{"exceed in same window", base.Add(300 * time.Millisecond), false},
		// 跨窗口之后重新计数
		{"after window reset", base.Add(1100 * time.Millisecond), true},
		{"second after reset", base.Add(1200 * time.Millisecond), true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clk.now = tc.at
			got := lim.Allow("userA")
			if got != tc.allow {
				t.Fatalf("at %v: expected allow=%v, got=%v", tc.at, tc.allow, got)
			}
		})
	}
}

// TestFixedWindow_MultiKey 验证不同 key 之间的计数互不影响。
func TestFixedWindow_MultiKey(t *testing.T) {
	base := time.Unix(0, 0)
	clk := &fakeClock{now: base}
	store := newInMemoryStore()
	lim := newFixedWindowLimiterForTest(2, time.Second, clk, store, base)

	tcs := []struct {
		name  string
		key   string
		at    time.Time
		allow bool
	}{
		{"userA first", "userA", base, true},
		{"userA second", "userA", base.Add(100 * time.Millisecond), true},
		{"userA exceed", "userA", base.Add(200 * time.Millisecond), false},
		{"userB first", "userB", base.Add(50 * time.Millisecond), true},
		{"userB second", "userB", base.Add(150 * time.Millisecond), true},
		{"userB exceed", "userB", base.Add(250 * time.Millisecond), false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clk.now = tc.at
			got := lim.Allow(tc.key)
			if got != tc.allow {
				t.Fatalf("key=%s at %v: expected allow=%v, got=%v", tc.key, tc.at, tc.allow, got)
			}
		})
	}
}
