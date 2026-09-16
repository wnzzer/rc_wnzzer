// Package dispatch 负责把队列中到期的任务真正投递出去。
package dispatch

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/model"
	"github.com/wnzzer/rc_wnzzer/internal/queue"
)

const (
	// hostBusyDelay 是目标主机并发满时的改期间隔。
	hostBusyDelay = 200 * time.Millisecond
	// bodySnippetMax 是记入日志的响应体长度。响应内容不属于本服务的职责
	// （边界 B3），只留一小段用于排障。
	bodySnippetMax = 512
	// bodyDrainMax 是关闭前最多丢弃的响应体字节数。读完剩余内容才能复用连接，
	// 但对超大响应体不值得，超过就直接断开。
	bodyDrainMax = 64 << 10
)

// Config 是投递层的可调参数。
type Config struct {
	Workers            int
	HostConcurrency    int
	BaseBackoff        time.Duration
	MaxBackoff         time.Duration
	DialTimeout        time.Duration
	DefaultTimeout     time.Duration
	DefaultMaxAttempts int
	DefaultDeadline    time.Duration
	AllowPrivate       bool
}

// Recorder 接收投递指标。允许为 nil。
type Recorder interface {
	ObserveAttempt(outcome string, code int, d time.Duration)
}

// Dispatcher 是调度循环 + worker 池。
type Dispatcher struct {
	cfg    Config
	q      *queue.Queue
	client *http.Client
	log    *slog.Logger
	rec    Recorder

	slots chan struct{} // 全局并发闸门，容量 = Workers

	semMu   sync.Mutex
	hostSem map[string]chan struct{}

	wg sync.WaitGroup
}

func New(cfg Config, q *queue.Queue, log *slog.Logger, rec Recorder) *Dispatcher {
	return &Dispatcher{
		cfg:     cfg,
		q:       q,
		client:  NewHTTPClient(cfg.AllowPrivate, cfg.DialTimeout),
		log:     log,
		rec:     rec,
		slots:   make(chan struct{}, cfg.Workers),
		hostSem: make(map[string]chan struct{}),
	}
}

// Run 是调度主循环。ctx 取消后停止取新任务并返回；在途投递由 Wait 负责等待。
func (d *Dispatcher) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		now := model.NowMS()
		t, next := d.q.Lease(now)
		if t == nil {
			if !d.idle(ctx, next, now) {
				return
			}
			continue
		}

		// 先抢目标主机的并发额度。抢不到就改期，**不计入尝试次数** ——
		// 本地拥塞不是对端的错，不该消耗任务的重试预算。
		hs := d.hostSemFor(t.Target.URL)
		select {
		case hs <- struct{}{}:
		default:
			d.q.Defer(t, now+hostBusyDelay.Milliseconds())
			continue
		}

		// 再抢全局 worker 额度。这里会阻塞 —— 正是我们想要的背压。
		select {
		case d.slots <- struct{}{}:
		case <-ctx.Done():
			<-hs
			d.q.Defer(t, now) // 放回队列，交给下次启动
			return
		}

		d.wg.Add(1)
		go func(t *model.Task) {
			defer d.wg.Done()
			defer func() { <-hs; <-d.slots }()
			d.deliver(t)
		}(t)
	}
}

// Wait 等待全部在途投递结束，最多等 grace。返回 false 表示超时仍有在途任务。
//
// 超时未结束的任务不做特殊处理：它们在 journal 里仍是 sending 之前的状态，
// 重启后按 pending 重放（spec §5.3）。这可能造成一次重复投递，落在
// at-least-once 契约内。
func (d *Dispatcher) Wait(grace time.Duration) bool {
	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		return false
	}
}

// idle 在没有到期任务时等待。返回 false 表示应当退出。
//
// next 是堆顶任务的到期时刻（0 表示堆为空）。用定时器精确等到那一刻，
// 而不是固定间隔轮询 —— 空闲时 CPU 占用为零。
func (d *Dispatcher) idle(ctx context.Context, next, now int64) bool {
	var timer <-chan time.Time
	if next > 0 {
		wait := time.Duration(next-now) * time.Millisecond
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		t := time.NewTimer(wait)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-ctx.Done():
		return false
	case <-d.q.Wake(): // 新任务入队，或有任务被改期到更早
	case <-timer:
	}
	return true
}

// hostSemFor 返回目标主机的并发信号量，按需创建。
//
// per-host 上限是必需的故障隔离（决策 D-013）：没有它，一个 RT 30s 的供应商
// 会占满全部 worker，其余所有供应商的通知一起饿死 —— 这是这类系统最典型的
// 生产事故，而修复成本只有这几行。
func (d *Dispatcher) hostSemFor(rawURL string) chan struct{} {
	host := hostOf(rawURL)
	d.semMu.Lock()
	defer d.semMu.Unlock()
	s, ok := d.hostSem[host]
	if !ok {
		s = make(chan struct{}, d.cfg.HostConcurrency)
		d.hostSem[host] = s
	}
	return s
}

func hostOf(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Host
	}
	return raw
}

// deliver 执行一次投递尝试并根据结果推进任务状态。
func (d *Dispatcher) deliver(t *model.Task) {
	attempt := t.Attempts + 1
	timeout := d.cfg.DefaultTimeout
	if t.Policy.TimeoutMS > 0 {
		timeout = time.Duration(t.Policy.TimeoutMS) * time.Millisecond
	}

	// 刻意不继承调度循环的 ctx：SIGTERM 后应当让在途投递**自然跑完**
	// （由 Wait 的 grace 兜底），而不是拦腰打断制造一次不必要的重复投递。
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	code, hdr, errMsg, err := d.doRequest(ctx, t, attempt)
	elapsed := time.Since(start)

	outcome := Classify(code, err)
	if d.rec != nil {
		d.rec.ObserveAttempt(outcome.String(), code, elapsed)
	}
	lg := d.log.With("task", t.ID, "attempt", attempt, "host", hostOf(t.Target.URL),
		"code", code, "耗时", elapsed.Round(time.Millisecond).String())

	switch outcome {
	case OutcomeSuccess:
		if err := d.q.OnSuccess(t, code); err != nil {
			lg.Error("记录投递成功失败", "err", err)
		}
		lg.Info("投递成功")
		return

	case OutcomePermanent:
		why := permanentReason(err)
		if e := d.q.OnDead(t, code, errMsg, why); e != nil {
			lg.Error("记录死信失败", "err", e)
		}
		lg.Warn("永久失败，进入死信", "原因", why, "err", errMsg)
		return
	}

	// —— 可重试：先算退避，再检查两个放弃条件（spec §6.4）——
	maxAttempts := d.cfg.DefaultMaxAttempts
	if t.Policy.MaxAttempts > 0 {
		maxAttempts = t.Policy.MaxAttempts
	}
	if attempt >= maxAttempts {
		if e := d.q.OnDead(t, code, errMsg, "max_attempts"); e != nil {
			lg.Error("记录死信失败", "err", e)
		}
		lg.Warn("达到重试上限，进入死信", "上限", maxAttempts)
		return
	}

	delay := Backoff(attempt, d.cfg.BaseBackoff, d.cfg.MaxBackoff)
	// 对端明确说了什么时候再来，就听它的 —— 比我们自己的估计更准。
	if hdr != nil {
		if ra, ok := ParseRetryAfter(hdr, start, d.cfg.MaxBackoff); ok {
			delay = ra
		}
	}
	nextAt := model.NowMS() + delay.Milliseconds()

	deadline := d.cfg.DefaultDeadline
	if t.Policy.DeadlineMS > 0 {
		deadline = time.Duration(t.Policy.DeadlineMS) * time.Millisecond
	}
	if nextAt >= t.CreatedAt+deadline.Milliseconds() {
		if e := d.q.OnDead(t, code, errMsg, "deadline_exceeded"); e != nil {
			lg.Error("记录死信失败", "err", e)
		}
		lg.Warn("下次重试将超过绝对时限，进入死信", "时限", deadline.String())
		return
	}

	if e := d.q.OnRetry(t, code, errMsg, nextAt); e != nil {
		lg.Error("记录重试失败", "err", e)
	}
	lg.Info("可重试失败，已排程", "退避", delay.Round(time.Millisecond).String(), "err", errMsg)
}

// doRequest 构造并发出一次 HTTP 请求。
// 返回状态码、响应头、错误摘要与原始错误（code 为 0 表示没拿到响应）。
func (d *Dispatcher) doRequest(ctx context.Context, t *model.Task, attempt int) (int, http.Header, string, error) {
	body, err := decodeBody(t.Target)
	if err != nil {
		// 不可解码的 body 是调用方的问题，且重试不会变好。
		return 0, nil, err.Error(), fmt.Errorf("%w: %v", ErrBadRequestSpec, err)
	}
	method := t.Target.Method
	if method == "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, t.Target.URL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err.Error(), fmt.Errorf("%w: 构造请求: %v", ErrBadRequestSpec, err)
	}
	for k, v := range t.Target.Headers {
		req.Header.Set(k, v)
	}
	// 本服务注入的头放在调用方之后，确保不会被覆盖。
	// X-Notify-Id 在同一任务的所有重试中**恒定不变** —— 这是我们在出口侧
	// 能为幂等做的全部：让愿意去重的供应商有一个稳定的键可用（spec §5.2）。
	req.Header.Set("X-Notify-Id", t.ID)
	req.Header.Set("X-Notify-Attempt", strconv.Itoa(attempt))
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "notifyd/1.0")
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, err.Error(), err
	}
	defer resp.Body.Close()

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, bodySnippetMax))
	_, _ = io.CopyN(io.Discard, resp.Body, bodyDrainMax) // 读完才能复用连接
	msg := ""
	if Classify(resp.StatusCode, nil) != OutcomeSuccess {
		msg = string(snippet)
	}
	return resp.StatusCode, resp.Header, msg, nil
}

func decodeBody(tg model.Target) ([]byte, error) {
	switch tg.BodyEncoding {
	case "", "utf8":
		return []byte(tg.Body), nil
	case "base64":
		return base64.StdEncoding.DecodeString(tg.Body)
	default:
		return nil, fmt.Errorf("不支持的 body_encoding: %q", tg.BodyEncoding)
	}
}

// permanentReason 把永久失败归因成一个稳定的死信原因字符串。
// 这个字符串会出现在死信列表和告警里，必须能直接指向问题类型。
func permanentReason(err error) string {
	switch {
	case err == nil:
		return "permanent_response" // 对端返回了 3xx 或非 408/429 的 4xx
	case errors.Is(err, ErrBlockedTarget):
		return "blocked_target"
	case errors.Is(err, ErrBadRequestSpec):
		return "bad_request_spec"
	default:
		return "permanent_error"
	}
}
