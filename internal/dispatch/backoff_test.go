package dispatch

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		code int
		err  error
		want Outcome
	}{
		{"200 成功", 200, nil, OutcomeSuccess},
		{"201 成功", 201, nil, OutcomeSuccess},
		{"204 成功", 204, nil, OutcomeSuccess},
		{"408 可重试", 408, nil, OutcomeRetryable},
		{"429 可重试", 429, nil, OutcomeRetryable},
		{"500 可重试", 500, nil, OutcomeRetryable},
		{"502 可重试", 502, nil, OutcomeRetryable},
		{"503 可重试", 503, nil, OutcomeRetryable},
		// 这四条是本函数存在的理由：重试它们只会浪费配额并可能触发对端风控。
		{"400 永久失败", 400, nil, OutcomePermanent},
		{"401 永久失败", 401, nil, OutcomePermanent},
		{"403 永久失败", 403, nil, OutcomePermanent},
		{"404 永久失败", 404, nil, OutcomePermanent},
		{"422 永久失败", 422, nil, OutcomePermanent},
		// 不跟随重定向，3xx 一律暴露为永久失败让调用方去修 URL。
		{"301 永久失败", 301, nil, OutcomePermanent},
		{"302 永久失败", 302, nil, OutcomePermanent},
		{"网络错误可重试", 0, errors.New("connection refused"), OutcomeRetryable},
		{"超时可重试", 0, errors.New("context deadline exceeded"), OutcomeRetryable},
		{"命中 SSRF 黑名单为永久失败", 0, fmt.Errorf("%w: 127.0.0.1", ErrBlockedTarget), OutcomePermanent},
		{"任务描述非法为永久失败", 0, fmt.Errorf("%w: bad body", ErrBadRequestSpec), OutcomePermanent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.code, c.err); got != c.want {
				t.Errorf("Classify(%d, %v) = %v, 期望 %v", c.code, c.err, got, c.want)
			}
		})
	}
}

func TestBackoff_WindowGrowsAndCaps(t *testing.T) {
	base, cap := time.Second, 8*time.Second
	// 取每次尝试的最大观测值来逼近窗口上界。
	for attempt, wantWindow := range map[int]time.Duration{
		1: 1 * time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
		4: 8 * time.Second, 5: 8 * time.Second, 20: 8 * time.Second,
	} {
		var maxSeen time.Duration
		for i := 0; i < 2000; i++ {
			d := Backoff(attempt, base, cap)
			if d > maxSeen {
				maxSeen = d
			}
			if d > wantWindow {
				t.Fatalf("第 %d 次退避 %v 超出窗口上界 %v", attempt, d, wantWindow)
			}
			if d < minDelay {
				t.Fatalf("第 %d 次退避 %v 低于下限 %v", attempt, d, minDelay)
			}
		}
		// 2000 次采样应当相当接近上界，否则说明窗口算错了。
		if maxSeen < wantWindow*8/10 {
			t.Errorf("第 %d 次退避的最大观测值 %v 远低于窗口 %v", attempt, maxSeen, wantWindow)
		}
	}
}

// TestBackoff_FullJitterSpread 验证 full jitter 的核心性质：
// 同一次尝试的退避值应当**铺满整个窗口**，而不是聚集在某处。
//
// 这正是决策 D-011 想要的效果 —— 对端恢复时，积压任务的重试时刻被打散，
// 而不是在同一秒里把刚恢复的对端再打垮一次。
func TestBackoff_FullJitterSpread(t *testing.T) {
	const window = 8 * time.Second
	buckets := make([]int, 8) // 把窗口切成 8 份
	const n = 4000
	for i := 0; i < n; i++ {
		d := Backoff(4, time.Second, window) // 第 4 次，窗口正好 8s
		idx := int(d * time.Duration(len(buckets)) / window)
		if idx >= len(buckets) {
			idx = len(buckets) - 1
		}
		buckets[idx]++
	}
	expect := n / len(buckets)
	for i, c := range buckets {
		// 每个桶都应有样本，且量级接近均匀 —— 允许 ±40% 的统计波动。
		if c < expect*6/10 || c > expect*14/10 {
			t.Errorf("第 %d 个分桶样本数 %d 偏离均匀分布（期望约 %d）: %v", i, c, expect, buckets)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	maxWindow := time.Hour

	cases := []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{"秒数", "30", 30 * time.Second, true},
		{"零秒取下限", "0", minDelay, true},
		{"超大秒数被封顶", "999999999", maxWindow, true},
		{"负数忽略", "-5", 0, false},
		{"HTTP 日期", now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second, true},
		{"过去的日期忽略", now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0, false},
		{"无法解析", "soon", 0, false},
		{"缺失", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := http.Header{}
			if c.value != "" {
				h.Set("Retry-After", c.value)
			}
			got, ok := ParseRetryAfter(h, now, maxWindow)
			if ok != c.ok {
				t.Fatalf("ok = %v, 期望 %v", ok, c.ok)
			}
			if ok && got != c.want {
				t.Errorf("时长 = %v, 期望 %v", got, c.want)
			}
		})
	}
}
