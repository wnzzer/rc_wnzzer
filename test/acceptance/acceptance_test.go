package acceptance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/api"
	"github.com/wnzzer/rc_wnzzer/internal/testutil"
)

// A1：提交返回 202 后立即 kill -9，重启后任务不丢且最终被投递。
//
// 这是核心承诺 C1 的端到端验证，也是整个项目最重要的一个测试。
// 做法是让供应商在第一个进程存活期间保持不可用，确保任务**一定**还在队列里
// 就被 kill —— 否则可能在 kill 之前就投递成功，测不到该测的东西。
func TestA1_TaskSurvivesKill9(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable) // 先不让它成功

	s := newServer(t).set("NOTIFY_BACKOFF_BASE", "10s").start()
	got := s.submit("k-a1", v.URL(), http.StatusAccepted, nil)
	if got.ID == "" {
		t.Fatal("202 响应中没有任务 ID")
	}

	// 等第一次投递发生，确认任务确实处于「已收下、未送达」的状态。
	if !waitUntil(5*time.Second, func() bool { return v.Count() >= 1 }) {
		t.Fatal("首次投递未发生")
	}

	s.kill9()

	// 供应商恢复，重启 notifyd。
	v.SetStatus(http.StatusOK)
	s.start()

	if !waitUntil(15*time.Second, func() bool {
		return s.task(got.ID).State == "succeeded"
	}) {
		t.Fatalf("kill -9 后任务未被投递，当前状态 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}
	if n := v.CountFor(got.ID); n < 2 {
		t.Errorf("期望重启后至少再投递一次，实际总投递 %d 次", n)
	}
}

// A2：目标持续 503 时，重试间隔呈指数增长且带抖动；达上限后进入死信。
func TestA2_ExponentialBackoffThenDead(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable)

	s := newServer(t).
		set("NOTIFY_MAX_ATTEMPTS", "5").
		set("NOTIFY_BACKOFF_BASE", "100ms").
		set("NOTIFY_BACKOFF_CAP", "1s").
		start()

	got := s.submit("k-a2", v.URL(), http.StatusAccepted, nil)
	if !waitUntil(20*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatalf("任务未进入死信，当前 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}

	task := s.task(got.ID)
	if task.Attempts != 5 {
		t.Errorf("尝试次数 = %d, 期望 5", task.Attempts)
	}
	if task.DeadReason != "max_attempts" {
		t.Errorf("死信原因 = %q, 期望 max_attempts", task.DeadReason)
	}
	if n := v.CountFor(got.ID); n != 5 {
		t.Errorf("供应商实际收到 %d 次请求, 期望 5", n)
	}

	// 间隔应整体呈增长趋势。因为是 full jitter，单次间隔可能回落，
	// 所以断言的是「后半段的平均间隔 > 前半段」这个统计性质，而不是逐次单调。
	reqs := v.Requests()
	if len(reqs) >= 4 {
		var early, late time.Duration
		mid := len(reqs) / 2
		for i := 1; i <= mid; i++ {
			early += reqs[i].At.Sub(reqs[i-1].At)
		}
		for i := mid + 1; i < len(reqs); i++ {
			late += reqs[i].At.Sub(reqs[i-1].At)
		}
		avgEarly := early / time.Duration(mid)
		avgLate := late / time.Duration(len(reqs)-mid-1)
		if avgLate <= avgEarly {
			t.Errorf("退避未呈增长趋势: 前半均值 %v, 后半均值 %v", avgEarly, avgLate)
		}
	}
}

// A3：目标返回 400 时只尝试一次就进死信，不浪费重试预算。
func TestA3_PermanentFailureNotRetried(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusBadRequest)

	s := newServer(t).start()
	got := s.submit("k-a3", v.URL(), http.StatusAccepted, nil)

	if !waitUntil(5*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatalf("400 应立即进入死信，当前 = %+v", s.task(got.ID))
	}
	task := s.task(got.ID)
	if task.Attempts != 1 {
		t.Errorf("尝试次数 = %d, 期望 1（400 不该重试）", task.Attempts)
	}
	if task.DeadReason != "permanent_response" {
		t.Errorf("死信原因 = %q, 期望 permanent_response", task.DeadReason)
	}

	// 再等一会儿，确认确实没有偷偷重试。
	time.Sleep(1500 * time.Millisecond)
	if n := v.CountFor(got.ID); n != 1 {
		t.Errorf("供应商收到 %d 次请求, 期望 1 —— 永久失败被错误地重试了", n)
	}
}

// A4：同一 idempotency_key 并发提交 100 次，只产生 1 个任务。
func TestA4_IdempotentSubmission(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable) // 不让它成功，避免被保留期清理干扰

	s := newServer(t).set("NOTIFY_BACKOFF_BASE", "30s").start()

	const n = 100
	type res struct {
		id  string
		dup bool
	}
	results := make(chan res, n)
	for i := 0; i < n; i++ {
		go func() {
			body := map[string]any{
				"idempotency_key": "k-a4-same",
				"target":          map[string]any{"url": v.URL(), "method": "POST", "body": "{}"},
			}
			code, raw := s.do(http.MethodPost, "/v1/notifications", body)
			var out submitResp
			json.Unmarshal(raw, &out)
			if code != http.StatusAccepted && code != http.StatusOK {
				t.Errorf("并发提交返回 %d: %s", code, raw)
			}
			results <- res{out.ID, out.Duplicate}
		}()
	}

	ids := map[string]int{}
	dups := 0
	for i := 0; i < n; i++ {
		r := <-results
		ids[r.id]++
		if r.dup {
			dups++
		}
	}
	if len(ids) != 1 {
		t.Fatalf("产生了 %d 个不同任务, 期望 1: %v", len(ids), ids)
	}
	if dups != n-1 {
		t.Errorf("duplicate=true 的响应数 = %d, 期望 %d", dups, n-1)
	}
}

// A5：目标指向回环 / 云 metadata 地址时被拒绝（SSRF 防护）。
//
// 注意这个用例必须关掉 NOTIFY_ALLOW_PRIVATE_HOSTS —— 其他用例为了让 mock
// 供应商能被访问而开着它。
func TestA5_SSRFBlocked(t *testing.T) {
	s := newServer(t).set("NOTIFY_ALLOW_PRIVATE_HOSTS", "false").start()

	cases := []struct{ name, url string }{
		{"回环地址", "http://127.0.0.1:9/hook"},
		{"云 metadata", "http://169.254.169.254/latest/meta-data/"},
		{"私网地址", "http://10.1.2.3/hook"}, // redact-ok: SSRF 用例的被测输入
		{"IPv6 回环", "http://[::1]:9/hook"},
		{"IPv4-mapped IPv6 回环", "http://[::ffff:127.0.0.1]:9/hook"},
		{"运营商级 NAT", "http://100.64.0.1/hook"},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := map[string]any{
				"idempotency_key": fmt.Sprintf("k-a5-%d", i),
				"target":          map[string]any{"url": c.url, "method": "POST", "body": "{}"},
			}
			code, raw := s.do(http.MethodPost, "/v1/notifications", body)
			if code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d, 期望 400: %s", code, raw)
			}
			var e struct{ Code string }
			json.Unmarshal(raw, &e)
			if e.Code != "blocked_target" {
				t.Errorf("错误码 = %q, 期望 blocked_target", e.Code)
			}
		})
	}
}

// A6：签名错误或时间戳超窗时返回 401。
func TestA6_AuthRejection(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	s := newServer(t).start()

	body := []byte(`{"idempotency_key":"k-a6","target":{"url":"` + v.URL() + `","body":"{}"}}`)
	mk := func(mutate func(*http.Request)) int {
		req, _ := http.NewRequest(http.MethodPost, "http://"+s.addr+"/v1/notifications", bytes.NewReader(body))
		ts := time.Now().Unix()
		req.Header.Set(api.HeaderKeyID, testKeyID)
		req.Header.Set(api.HeaderTimestamp, fmt.Sprint(ts))
		req.Header.Set(api.HeaderSignature, api.Sign(testSecret, http.MethodPost, "/v1/notifications", ts, body))
		mutate(req)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := mk(func(r *http.Request) {}); got != http.StatusAccepted {
		t.Fatalf("正确签名应通过, 得到 %d", got)
	}
	tests := map[string]func(*http.Request){
		"签名被篡改":     func(r *http.Request) { r.Header.Set(api.HeaderSignature, "deadbeef") },
		"未知 key_id": func(r *http.Request) { r.Header.Set(api.HeaderKeyID, "nobody") },
		"时间戳超前 10 分钟": func(r *http.Request) {
			ts := time.Now().Add(10 * time.Minute).Unix()
			r.Header.Set(api.HeaderTimestamp, fmt.Sprint(ts))
			r.Header.Set(api.HeaderSignature, api.Sign(testSecret, http.MethodPost, "/v1/notifications", ts, body))
		},
		"时间戳滞后 10 分钟": func(r *http.Request) {
			ts := time.Now().Add(-10 * time.Minute).Unix()
			r.Header.Set(api.HeaderTimestamp, fmt.Sprint(ts))
			r.Header.Set(api.HeaderSignature, api.Sign(testSecret, http.MethodPost, "/v1/notifications", ts, body))
		},
		"缺少签名头": func(r *http.Request) { r.Header.Del(api.HeaderSignature) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if got := mk(mutate); got != http.StatusUnauthorized {
				t.Errorf("状态码 = %d, 期望 401", got)
			}
		})
	}
}

// TestA6b_SignatureCoversBody 验证签名覆盖 body 哈希 —— 篡改目标 URL 会被发现。
//
// 这条单独拎出来，是因为它对应 spec §4.1 里那个具体的攻击场景：
// 若签名不覆盖 body，中间人可以把「通知 CRM」改写成「把数据 POST 到攻击者服务器」。
func TestA6b_SignatureCoversBody(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	s := newServer(t).start()

	original := []byte(`{"idempotency_key":"k-a6b","target":{"url":"` + v.URL() + `","body":"{}"}}`)
	tampered := []byte(`{"idempotency_key":"k-a6b","target":{"url":"http://evil.example.com/steal","body":"{}"}}`)

	ts := time.Now().Unix()
	sig := api.Sign(testSecret, http.MethodPost, "/v1/notifications", ts, original)

	// 用原始 body 的签名，发送被篡改的 body。
	req, _ := http.NewRequest(http.MethodPost, "http://"+s.addr+"/v1/notifications", bytes.NewReader(tampered))
	req.Header.Set(api.HeaderKeyID, testKeyID)
	req.Header.Set(api.HeaderTimestamp, fmt.Sprint(ts))
	req.Header.Set(api.HeaderSignature, sig)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("篡改 body 后状态码 = %d, 期望 401 —— 签名未覆盖 body", resp.StatusCode)
	}
}

// A7：一个慢供应商不应拖垮其他供应商（per-host 并发隔离）。
//
// 这是这类系统最典型的生产事故，也是决策 D-013 存在的理由。
func TestA7_SlowHostDoesNotStarveOthers(t *testing.T) {
	slow := testutil.NewVendor()
	defer slow.Close()
	slow.SetDelay(3 * time.Second)

	fast := testutil.NewVendor()
	defer fast.Close()

	// worker 总数 4，单主机上限 2：慢供应商最多占 2 个 worker，
	// 剩下 2 个必须还能服务快供应商。
	s := newServer(t).
		set("NOTIFY_WORKERS", "4").
		set("NOTIFY_HOST_CONCURRENCY", "2").
		set("NOTIFY_TIMEOUT", "10s").
		start()

	// 先灌 10 个慢任务，把慢供应商的额度占满。
	for i := 0; i < 10; i++ {
		s.submit(fmt.Sprintf("k-a7-slow-%d", i), slow.URL(), http.StatusAccepted, nil)
	}
	time.Sleep(300 * time.Millisecond) // 让调度器先把慢任务铺开

	start := time.Now()
	fastID := s.submit("k-a7-fast", fast.URL(), http.StatusAccepted, nil)
	if !waitUntil(5*time.Second, func() bool { return s.task(fastID.ID).State == "succeeded" }) {
		t.Fatalf("快供应商的任务被慢供应商饿死了。日志:\n%s", s.logs())
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("快任务耗时 %v，明显受到了慢供应商的影响", elapsed)
	}
	// 慢供应商的并发确实被限制住了。
	if n := slow.Count(); n > 4 {
		t.Errorf("慢供应商同时收到 %d 个请求，per-host 上限未生效", n)
	}
}

// A8：供应商恢复后，积压任务的重试时刻应被 jitter 打散，不形成瞬时尖峰。
func TestA8_FullJitterSpreadsRetries(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable)

	// 退避窗口固定为 4s（base 与 cap 相同），这样第一次重试的排程时刻
	// 应当均匀分布在 [now, now+4s]，而不是全部挤在 now+4s。
	s := newServer(t).
		set("NOTIFY_BACKOFF_BASE", "4s").
		set("NOTIFY_BACKOFF_CAP", "4s").
		set("NOTIFY_WORKERS", "64").
		start()

	const n = 60
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = s.submit(fmt.Sprintf("k-a8-%d", i), v.URL(), http.StatusAccepted, nil).ID
	}
	if !waitUntil(15*time.Second, func() bool {
		for _, id := range ids {
			if s.task(id).Attempts < 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("并非所有任务都完成了首次尝试。日志:\n%s", s.logs())
	}

	// 收集第一次重试的排程时刻，看它们的分散程度。
	var minAt, maxAt int64
	buckets := map[int64]int{}
	for i, id := range ids {
		at := s.task(id).NextAt
		if at == 0 {
			t.Fatalf("任务 %s 没有排程下次重试", id)
		}
		if i == 0 || at < minAt {
			minAt = at
		}
		if at > maxAt {
			maxAt = at
		}
		buckets[at/500]++ // 按 500ms 分桶
	}
	spread := maxAt - minAt
	if spread < 1500 {
		t.Errorf("重试时刻跨度仅 %dms，jitter 未生效（确定性退避会让它们挤在一起）", spread)
	}
	if len(buckets) < 4 {
		t.Errorf("重试时刻只落在 %d 个 500ms 分桶里，分散度不足", len(buckets))
	}
	t.Logf("%d 个任务的首次重试时刻跨度 %dms，分布在 %d 个 500ms 分桶", n, spread, len(buckets))
}

// A9：压缩把大量重试记录折叠掉，文件明显变小，且重启后存活任务状态不丢。
//
// 压缩的收益来自折叠**冗余的 att 记录** —— 一个重试了 20 次的任务在 journal 里
// 有 1 条 enq + 20 条 att，压缩后只需 1 条 enq + 1 条状态记录。因此这个用例
// 必须先制造出大量重试，否则压缩无事可做（这一点是写测试时才想清楚的）。
func TestA9_CompactionPreservesState(t *testing.T) {
	flaky := testutil.NewVendor()
	defer flaky.Close()
	flaky.FailFor(3*time.Second, http.StatusServiceUnavailable) // 先失败，后自愈

	stuck := testutil.NewVendor()
	defer stuck.Close()
	stuck.SetStatus(http.StatusServiceUnavailable) // 永远失败，用来提供存活任务

	s := newServer(t).
		set("NOTIFY_BACKOFF_BASE", "50ms").
		set("NOTIFY_BACKOFF_CAP", "150ms").
		set("NOTIFY_MAX_ATTEMPTS", "500"). // 足够高，保证不会中途进死信
		set("NOTIFY_COMPACT_MIN_BYTES", "4000").
		set("NOTIFY_COMPACT_LIVE_RATIO", "0.5").
		set("NOTIFY_MAINTAIN_INTERVAL", "300ms").
		start()

	const okCount = 20
	var okIDs []string
	for i := 0; i < okCount; i++ {
		okIDs = append(okIDs, s.submit(fmt.Sprintf("k-a9-ok-%d", i), flaky.URL(), http.StatusAccepted, nil).ID)
	}
	var liveIDs []string
	for i := 0; i < 5; i++ {
		liveIDs = append(liveIDs, s.submit(fmt.Sprintf("k-a9-live-%d", i), stuck.URL(), http.StatusAccepted, nil).ID)
	}

	// 等 flaky 自愈后这批任务全部成功 —— 此时它们已各自积累了几十条 att 记录。
	if !waitUntil(30*time.Second, func() bool {
		for _, id := range okIDs {
			if s.task(id).State != "succeeded" {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("重试任务未全部成功。日志:\n%s", s.logs())
	}
	totalAttempts := 0
	for _, id := range okIDs {
		totalAttempts += s.task(id).Attempts
	}
	if totalAttempts < okCount*5 {
		t.Fatalf("重试次数不足（共 %d 次），压缩无事可做，用例失去意义", totalAttempts)
	}

	// 压缩必须确实发生过。
	if !waitUntil(15*time.Second, func() bool {
		return strings.Contains(s.logs(), "journal 压缩完成")
	}) {
		t.Fatalf("未观察到压缩发生（共 %d 次重试）。日志:\n%s", totalAttempts, s.logs())
	}

	// 核心性质：journal 体积**不随重试次数线性增长**。
	//
	// 不压缩时，每次重试都会追加一条 att 记录；%d 次重试至少要占十万字节量级。
	// 压缩让它稳定在「任务数 × 2 条记录」的水位上 —— 这正是压缩存在的意义，
	// 也比「压缩后比压缩前小」更能说明问题（压缩跟得上时，文件根本不会先涨起来）。
	bytes := metricValue(t, s, "notifyd_journal_bytes")
	const bound = 60000
	if bytes > bound {
		t.Fatalf("journal = %.0f 字节，超过界限 %d（共 %d 次重试）—— 压缩没跟上",
			bytes, bound, totalAttempts)
	}
	t.Logf("%d 次重试后 journal 仅 %.0f 字节（25 个任务）", totalAttempts, bytes)

	// 压缩 + 重启后：成功任务仍可查（幂等去重依赖它），存活任务仍未终结。
	before := make(map[string]taskResp, len(liveIDs))
	for _, id := range liveIDs {
		before[id] = s.task(id)
	}
	s.stop()
	s.start()

	for _, id := range okIDs {
		if got := s.task(id).State; got != "succeeded" {
			t.Errorf("成功任务 %s 重启后状态 = %q，墓碑丢失会破坏幂等去重", id, got)
		}
	}
	for id, want := range before {
		got := s.task(id)
		if got.State == "succeeded" || got.State == "dead" {
			t.Errorf("存活任务 %s 重启后状态 = %q, 期望仍未终结", id, got.State)
		}
		// 尝试次数只增不减：重启前后各自还在继续重试，断言单调而非相等。
		if got.Attempts < want.Attempts {
			t.Errorf("任务 %s 重启后尝试次数回退: %d -> %d", id, want.Attempts, got.Attempts)
		}
	}
	// 成功任务的幂等键仍然有效：重复提交应当返回 duplicate。
	dup := s.submit("k-a9-ok-0", flaky.URL(), http.StatusOK, nil)
	if !dup.Duplicate {
		t.Error("压缩 + 重启后幂等去重失效")
	}
	t.Logf("压缩 + 重启后 %d 个成功墓碑与 %d 个存活任务均一致", len(okIDs), len(liveIDs))
}

// A10：队列达到上限后，新提交返回 429，已有任务不受影响。
func TestA10_BackpressureOnQueueFull(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable)

	s := newServer(t).
		set("NOTIFY_QUEUE_MAX", "5").
		set("NOTIFY_BACKOFF_BASE", "60s"). // 让任务停在队列里
		start()

	for i := 0; i < 5; i++ {
		s.submit(fmt.Sprintf("k-a10-%d", i), v.URL(), http.StatusAccepted, nil)
	}
	// 第 6 个应当被拒。
	body := map[string]any{
		"idempotency_key": "k-a10-overflow",
		"target":          map[string]any{"url": v.URL(), "method": "POST", "body": "{}"},
	}
	code, raw := s.do(http.MethodPost, "/v1/notifications", body)
	if code != http.StatusTooManyRequests {
		t.Fatalf("队列满时状态码 = %d, 期望 429: %s", code, raw)
	}
	var e struct{ Code string }
	json.Unmarshal(raw, &e)
	if e.Code != "queue_full" {
		t.Errorf("错误码 = %q, 期望 queue_full", e.Code)
	}

	// 已有任务不受影响：仍可查询，且仍在正常重试。
	first := s.task(s.submit("k-a10-0", v.URL(), http.StatusOK, nil).ID)
	if first.State == "dead" {
		t.Error("背压不应影响已接收的任务")
	}
}

// TestDeadLetterRetry 覆盖死信的运维出口：手工重投。
// 没有它，死信就只是一座坟场（spec §4.3）。
func TestDeadLetterRetry(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusBadRequest) // 立即进死信

	s := newServer(t).start()
	got := s.submit("k-dlq", v.URL(), http.StatusAccepted, nil)
	if !waitUntil(5*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatal("任务未进入死信")
	}

	// 死信列表里应该能查到它。
	code, raw := s.do(http.MethodGet, "/v1/notifications?state=dead", nil)
	if code != http.StatusOK {
		t.Fatalf("查询死信列表: %d, %s", code, raw)
	}
	var list struct {
		Count int `json:"count"`
	}
	json.Unmarshal(raw, &list)
	if list.Count < 1 {
		t.Errorf("死信列表为空: %s", raw)
	}

	// 修好供应商后手工重投。
	v.SetStatus(http.StatusOK)
	code, raw = s.do(http.MethodPost, "/v1/notifications/"+got.ID+"/retry", nil)
	if code != http.StatusAccepted {
		t.Fatalf("重投死信: %d, %s", code, raw)
	}
	if !waitUntil(10*time.Second, func() bool { return s.task(got.ID).State == "succeeded" }) {
		t.Fatalf("重投后任务未成功，当前 = %+v", s.task(got.ID))
	}
}

// TestStableNotifyID 验证同一任务的所有重试携带**恒定不变**的 X-Notify-Id。
//
// 这是 notifyd 在出口侧为幂等能做的全部（spec §5.2）：让愿意去重的供应商
// 有一个稳定的键可用。这个键如果每次重试都变，整个 at-least-once 的补偿
// 机制就失效了。
func TestStableNotifyID(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable)

	s := newServer(t).
		set("NOTIFY_MAX_ATTEMPTS", "4").
		set("NOTIFY_BACKOFF_BASE", "100ms").
		set("NOTIFY_BACKOFF_CAP", "300ms").
		start()

	got := s.submit("k-stable", v.URL(), http.StatusAccepted, nil)
	if !waitUntil(15*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatal("任务未进入死信")
	}
	reqs := v.Requests()
	if len(reqs) < 3 {
		t.Fatalf("重试次数不足，只收到 %d 次", len(reqs))
	}
	seenAttempts := map[int]bool{}
	for i, r := range reqs {
		if r.NotifyID != got.ID {
			t.Errorf("第 %d 次重试的 X-Notify-Id = %q, 期望恒定为 %q", i+1, r.NotifyID, got.ID)
		}
		if seenAttempts[r.Attempt] {
			t.Errorf("X-Notify-Attempt 出现重复值 %d，它应当递增", r.Attempt)
		}
		seenAttempts[r.Attempt] = true
	}
}

// --- 辅助 ---

// metricValue 从 /metrics 里抓一个无标签指标的值。
func metricValue(t *testing.T, s *server, name string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + s.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		rest, ok := strings.CutPrefix(line, name+" ")
		if !ok {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("解析指标 %s 的值 %q: %v", name, rest, err)
		}
		return f
	}
	t.Fatalf("/metrics 中找不到指标 %s", name)
	return 0
}
