// Package testutil 提供测试基础设施。它只被测试导入，不会进入生产二进制。
package testutil

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"
)

// Recorded 是 mock 供应商收到的一次请求。
type Recorded struct {
	At       time.Time
	NotifyID string // X-Notify-Id：同一任务的所有重试中应保持不变
	Attempt  int    // X-Notify-Attempt
	Body     string
	Header   http.Header
}

// Vendor 是一个可编程的外部供应商替身。
//
// 它存在的意义：退避、失败分类、故障隔离这些行为，只有在对端**按剧本行动**时
// 才能被确定性地验证。用真实第三方或固定桩都做不到这一点。
type Vendor struct {
	srv *httptest.Server

	mu         sync.Mutex
	reqs       []Recorded
	status     int
	delay      time.Duration
	failUntil  time.Time
	failStatus int
	retryAfter string
}

// NewVendor 启动一个默认返回 200 的供应商。
func NewVendor() *Vendor {
	v := &Vendor{status: http.StatusOK, failStatus: http.StatusServiceUnavailable}
	v.srv = httptest.NewServer(http.HandlerFunc(v.serve))
	return v
}

func (v *Vendor) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	attempt, _ := strconv.Atoi(r.Header.Get("X-Notify-Attempt"))

	v.mu.Lock()
	v.reqs = append(v.reqs, Recorded{
		At: time.Now(), NotifyID: r.Header.Get("X-Notify-Id"),
		Attempt: attempt, Body: string(body), Header: r.Header.Clone(),
	})
	status, delay, ra := v.status, v.delay, v.retryAfter
	if !v.failUntil.IsZero() && time.Now().Before(v.failUntil) {
		status = v.failStatus
	}
	v.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if ra != "" {
		w.Header().Set("Retry-After", ra)
	}
	w.WriteHeader(status)
	io.WriteString(w, `{"ok":true}`)
}

func (v *Vendor) URL() string { return v.srv.URL + "/hook" }
func (v *Vendor) Close()      { v.srv.Close() }

// SetStatus 设置稳定返回的状态码。
func (v *Vendor) SetStatus(code int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.status = code
}

// SetDelay 设置每次响应的人为延迟，用于模拟慢供应商。
func (v *Vendor) SetDelay(d time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.delay = d
}

// SetRetryAfter 设置返回的 Retry-After 头。
func (v *Vendor) SetRetryAfter(s string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.retryAfter = s
}

// FailFor 让供应商在接下来的 d 时间内返回 failStatus，之后自动恢复。
// 用于模拟「宕机一段时间后恢复」。
func (v *Vendor) FailFor(d time.Duration, failStatus int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.failUntil = time.Now().Add(d)
	v.failStatus = failStatus
}

// Requests 返回收到的全部请求快照。
func (v *Vendor) Requests() []Recorded {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]Recorded, len(v.reqs))
	copy(out, v.reqs)
	return out
}

// Count 返回收到的请求总数。
func (v *Vendor) Count() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.reqs)
}

// CountFor 返回某个 X-Notify-Id 收到的请求数，用于验证重复投递情况。
func (v *Vendor) CountFor(notifyID string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	n := 0
	for _, r := range v.reqs {
		if r.NotifyID == notifyID {
			n++
		}
	}
	return n
}
