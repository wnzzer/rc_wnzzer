// Package metrics 手写 Prometheus 文本格式的指标暴露。
//
// 不引入 client_golang / OpenTelemetry（决策 D-024）：exposition format 是纯文本、
// 规范极简，手写不到百行；而这两个库会分别拖入一串传递依赖和一整套
// SDK + Collector 的世界观。为了暴露 7 个指标付这个代价不划算。
package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/model"
)

// buckets 是投递耗时直方图的分桶（秒）。
// 分桶按「外部 API 的实际响应特征」选：亚秒级细分用于看正常情况，
// 5s / 10s 对应默认超时附近，用于看有多少请求是被我们自己超时掐断的。
var buckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry 持有全部指标。
type Registry struct {
	mu sync.Mutex

	submitted map[string]int64 // result -> count
	attempts  map[string]int64 // outcome -> count
	codes     map[int]int64    // HTTP 状态码分布

	histCounts []int64
	histSum    float64
	histTotal  int64
}

func New() *Registry {
	return &Registry{
		submitted:  make(map[string]int64),
		attempts:   make(map[string]int64),
		codes:      make(map[int]int64),
		histCounts: make([]int64, len(buckets)),
	}
}

// ObserveSubmit 记录一次提交，result ∈ accepted|duplicate|rejected。
func (r *Registry) ObserveSubmit(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.submitted[result]++
}

// ObserveAttempt 记录一次投递尝试。实现 dispatch.Recorder。
func (r *Registry) ObserveAttempt(outcome string, code int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts[outcome]++
	if code > 0 {
		r.codes[code]++
	}
	secs := d.Seconds()
	r.histSum += secs
	r.histTotal++
	for i, b := range buckets {
		if secs <= b {
			r.histCounts[i]++
		}
	}
}

// Snapshot 是渲染指标时需要的队列侧数据。由调用方提供，避免 metrics 反向依赖 queue。
type Snapshot struct {
	States        map[model.State]int
	QueueDepth    int
	JournalBytes  int64
	OldestPending int64 // 最老未终结任务的创建时刻（Unix 毫秒），0 表示没有
}

// Render 输出 Prometheus exposition format。
func (r *Registry) Render(s Snapshot) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder
	gauge := func(name, help string, v any, labels ...string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		fmt.Fprintf(&b, "%s%s %v\n", name, fmtLabels(labels), v)
	}

	// 任务状态分布
	fmt.Fprintf(&b, "# HELP notifyd_tasks 各状态的任务数\n# TYPE notifyd_tasks gauge\n")
	for _, st := range []model.State{
		model.StatePending, model.StateWaiting, model.StateSending,
		model.StateSucceeded, model.StateDead,
	} {
		fmt.Fprintf(&b, "notifyd_tasks{state=%q} %d\n", st, s.States[st])
	}

	gauge("notifyd_queue_depth", "堆中已排程待投递的任务数", s.QueueDepth)
	gauge("notifyd_journal_bytes", "journal 文件当前大小", s.JournalBytes)

	// 最重要的一个指标：它单独一条就能回答「是不是有东西卡住了」。
	// 队列深度正常但最老任务年龄持续上涨，说明存在一个投不出去的目标在反复退避。
	// 告警应该建在它上面，而不是建在队列深度上。
	age := 0.0
	if s.OldestPending > 0 {
		age = float64(model.NowMS()-s.OldestPending) / 1000
	}
	gauge("notifyd_oldest_pending_age_seconds", "最老未终结任务的存在时长", strconv.FormatFloat(age, 'f', 3, 64))

	counter(&b, "notifyd_submitted_total", "提交请求计数", "result", r.submitted)
	counter(&b, "notifyd_attempts_total", "投递尝试计数", "outcome", r.attempts)

	fmt.Fprintf(&b, "# HELP notifyd_response_codes_total 外部 API 返回的状态码分布\n# TYPE notifyd_response_codes_total counter\n")
	for _, c := range sortedIntKeys(r.codes) {
		fmt.Fprintf(&b, "notifyd_response_codes_total{code=\"%d\"} %d\n", c, r.codes[c])
	}

	fmt.Fprintf(&b, "# HELP notifyd_attempt_duration_seconds 单次投递耗时\n# TYPE notifyd_attempt_duration_seconds histogram\n")
	for i, bk := range buckets {
		fmt.Fprintf(&b, "notifyd_attempt_duration_seconds_bucket{le=\"%g\"} %d\n", bk, r.histCounts[i])
	}
	fmt.Fprintf(&b, "notifyd_attempt_duration_seconds_bucket{le=\"+Inf\"} %d\n", r.histTotal)
	fmt.Fprintf(&b, "notifyd_attempt_duration_seconds_sum %g\n", r.histSum)
	fmt.Fprintf(&b, "notifyd_attempt_duration_seconds_count %d\n", r.histTotal)

	return b.String()
}

func counter(b *strings.Builder, name, help, label string, m map[string]int64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for _, k := range sortedKeys(m) {
		fmt.Fprintf(b, "%s{%s=%q} %d\n", name, label, k, m[k])
	}
}

func fmtLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels)/2)
	for i := 0; i+1 < len(labels); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%q", labels[i], labels[i+1]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedIntKeys(m map[int]int64) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
