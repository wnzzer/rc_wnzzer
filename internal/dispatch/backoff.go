package dispatch

import (
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// Outcome 是一次投递尝试的判定结果。
type Outcome int

const (
	OutcomeSuccess   Outcome = iota
	OutcomeRetryable         // 重试有意义：对端暂时不可用
	OutcomePermanent         // 重试没有意义：再试一万次也不会变
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeRetryable:
		return "retryable"
	default:
		return "permanent"
	}
}

// Classify 判定一次投递的结果（spec §6.2）。
//
// 盲目重试所有失败是这类系统最常见的实现缺陷：它会对着一个永远返回 400 的请求
// 重试 24 次，既浪费自己的配额，也可能触发对端的风控。因此在重试之前先问一句
// 「重试有意义吗」。
func Classify(code int, err error) Outcome {
	if err != nil {
		// 请求本身就不该被发出（命中 SSRF 黑名单，或任务描述非法）。
		// 这两类不是对端故障，重试永远不会变好。
		if errors.Is(err, ErrBlockedTarget) || errors.Is(err, ErrBadRequestSpec) {
			return OutcomePermanent
		}
		// 其余网络层错误（连接被拒、DNS 失败、TLS 握手失败、超时）都可能是暂时的。
		return OutcomeRetryable
	}
	switch {
	case code >= 200 && code < 300:
		return OutcomeSuccess
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return OutcomeRetryable
	case code >= 500:
		return OutcomeRetryable
	case code >= 300 && code < 400:
		// 不跟随重定向（决策 D-010）：跟随会让 SSRF 校验被一个 302 绕过。
		// 把 3xx 作为永久失败暴露出去，让调用方去修 URL。
		return OutcomePermanent
	case code >= 400:
		// 401/403 尤其重要：凭据错误重试 24 次也不会变对，只会在对端积累一串
		// 认证失败记录，可能触发账号锁定。正确处理是立刻进死信 + 告警。
		return OutcomePermanent
	default:
		return OutcomeRetryable
	}
}

// minDelay 是退避的下限。
//
// 纯 full jitter 在第一次重试时可能算出接近 0 的延迟，形成短促的连打。
// 加一个下限不影响 jitter 的分散效果（窗口从第 4 次起就远大于它），
// 但能挡住这个退化情形。
const minDelay = 100 * time.Millisecond

// Backoff 计算第 attempt 次失败后的等待时长：指数窗口 + full jitter（决策 D-011）。
//
//	window = min(cap, base * 2^(attempt-1))
//	delay  = random_uniform(0, window)
//
// 为什么是 full jitter 而不是「固定指数 + 小随机扰动」：供应商宕机 10 分钟、
// 期间积压 5000 个任务时，确定性退避会让这 5000 次重试聚集在同一秒 —— 对端
// 刚恢复就被二次打垮。full jitter 把重试时刻均匀铺开到整个窗口，是三种做法中
// 对端压力峰值最低的一种。代价是单任务期望延迟变为窗口的一半，完全可接受：
// 我们优化的是**对端的存活概率**，不是单条通知的 P50。
func Backoff(attempt int, base, maxWindow time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	window := base
	// 用移位而非 math.Pow，同时防止 attempt 很大时的溢出。
	for i := 1; i < attempt; i++ {
		if window >= maxWindow {
			break
		}
		window *= 2
	}
	if window > maxWindow {
		window = maxWindow
	}
	d := time.Duration(rand.Int64N(int64(window) + 1))
	if d < minDelay {
		d = minDelay
	}
	return d
}

// ParseRetryAfter 解析 Retry-After 响应头，支持「秒数」和「HTTP 日期」两种形式。
//
// 对端明确告诉了我们什么时候再来，就应该听它的 —— 这比我们自己的退避估计更准，
// 也更不容易触发对端的限流。但仍然封顶，避免一个恶意或写错的
// "Retry-After: 999999999" 把任务钉死。
func ParseRetryAfter(h http.Header, now time.Time, maxWindow time.Duration) (time.Duration, bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return clampWindow(time.Duration(secs)*time.Second, maxWindow), true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			return 0, false
		}
		return clampWindow(d, maxWindow), true
	}
	return 0, false
}

func clampWindow(d, maxWindow time.Duration) time.Duration {
	if d > maxWindow {
		return maxWindow
	}
	if d < minDelay {
		return minDelay
	}
	return d
}
