package acceptance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/testutil"
)

// 一个带辨识度的自定义请求头与 payload。
//
// 它们是本文件全部断言的锚点：如果死信重投后供应商收到的**不是**它们，
// 就说明磁盘上的 Target 没有被完整保留 —— 而这正是「死信落盘」这件事的实质。
const (
	dlHeaderName  = "X-Vendor-Signature"
	dlHeaderValue = "v1=abc123def456"
	dlPayload     = `{"contact_id":98765,"status":"paid","amount":"12.34"}`
)

// submitWithHeaders 提交一个带自定义 header 与 payload 的通知。
func submitWithHeaders(s *server, idem, url string, wantStatus int) submitResp {
	s.t.Helper()
	return submitWithCred(s, idem, url, dlHeaderValue, wantStatus)
}

// submitWithCred 同上，但可指定 header 值 —— 用于区分不同任务的"凭据"，
// 以便在 journal 文件里精确判断哪一条被清掉了、哪一条被保留了。
func submitWithCred(s *server, idem, url, cred string, wantStatus int) submitResp {
	s.t.Helper()
	return s.submit(idem, url, wantStatus, map[string]any{
		"target": map[string]any{
			"url":    url,
			"method": "POST",
			"headers": map[string]string{
				"Content-Type": "application/json",
				dlHeaderName:   cred,
			},
			"body": dlPayload,
		},
	})
}

// readJournal 读取被测进程的 WAL 原文。
//
// 直接翻开磁盘文件、而不是通过 API 间接推断，是验证"凭据有没有留在盘上"
// 这类性质的唯一可靠方式 —— API 本来就不返回 headers，查 API 什么也证明不了。
func readJournal(t *testing.T, s *server) string {
	t.Helper()
	// 文件名与 internal/store 中的 journalName 一致。
	b, err := os.ReadFile(filepath.Join(s.dataDir, "journal.log"))
	if err != nil {
		t.Fatalf("读取 journal: %v", err)
	}
	return string(b)
}

// assertVendorGotFullRequest 校验供应商收到的是**原封不动**的请求。
func assertVendorGotFullRequest(t *testing.T, v *testutil.Vendor, notifyID string) {
	t.Helper()
	var last *testutil.Recorded
	for i := range v.Requests() {
		if r := v.Requests()[i]; r.NotifyID == notifyID {
			last = &r
		}
	}
	if last == nil {
		t.Fatalf("供应商没有收到任务 %s 的任何请求", notifyID)
	}
	if last.Body != dlPayload {
		t.Errorf("body 与提交时不一致:\n实际: %s\n期望: %s", last.Body, dlPayload)
	}
	// 调用方自定义的 header 必须活过「落盘 → 崩溃 → 回放 → 重投」的整条链路。
	// 它比 body 更严格：body 在 enq 记录里是一个字符串字段，而 headers 是一个 map，
	// 序列化、压缩瘦身、回放任何一环出错都会让它丢失。
	if got := last.Header.Get(dlHeaderName); got != dlHeaderValue {
		t.Errorf("调用方自定义 header %s = %q, 期望 %q —— Target 未被完整保留",
			dlHeaderName, got, dlHeaderValue)
	}
	if got := last.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, 期望 application/json", got)
	}
	if last.NotifyID != notifyID {
		t.Errorf("X-Notify-Id = %q, 期望 %q", last.NotifyID, notifyID)
	}
}

func deadLetterIDs(t *testing.T, s *server) map[string]taskResp {
	t.Helper()
	code, raw := s.do(http.MethodGet, "/v1/notifications?state=dead&limit=1000", nil)
	if code != http.StatusOK {
		t.Fatalf("查询死信列表: %d, %s", code, raw)
	}
	var list struct {
		Count int        `json:"count"`
		Tasks []taskResp `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("死信列表不是合法 JSON: %s", raw)
	}
	out := make(map[string]taskResp, len(list.Tasks))
	for _, tk := range list.Tasks {
		out[tk.ID] = tk
	}
	return out
}

// TestE2E_DeadLetterSurvivesKill9 验证死信是**落盘**的，不是只活在内存里。
//
// 这是 A1（承诺 C1）在死信侧的对应用例。A1 证明「还没送达的任务」不丢；
// 这个用例证明「已经放弃的任务」同样不丢 —— 而后者更容易被忽略，
// 因为它已经离开了调度队列，直觉上像是「处理完了」。
//
// 验证方式刻意用 kill -9 而不是优雅退出：优雅退出会走 fsync 收尾，
// 等于绕开了最需要验证的那条路径。
func TestE2E_DeadLetterSurvivesKill9(t *testing.T) {
	v := testutil.NewVendor()
	defer v.Close()
	v.SetStatus(http.StatusServiceUnavailable) // 一直失败，直到耗尽重试

	s := newServer(t).
		set("NOTIFY_MAX_ATTEMPTS", "3").
		set("NOTIFY_BACKOFF_BASE", "50ms").
		set("NOTIFY_BACKOFF_CAP", "150ms").
		start()

	got := submitWithHeaders(s, "dl-kill9", v.URL(), http.StatusAccepted)
	if !waitUntil(15*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatalf("任务未进入死信，当前 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}
	before := s.task(got.ID)
	if before.Attempts != 3 || before.DeadReason != "max_attempts" {
		t.Fatalf("死信状态不符: %+v", before)
	}
	attemptsBeforeCrash := v.CountFor(got.ID)

	// —— 掉电 ——
	s.kill9()
	s.start()

	// 1. 死信必须还在。若 `dead` 记录随崩溃丢失，任务会重跑一遍失败路径，
	//    因此这里等待它收敛，而不是要求重启瞬间就是 dead。
	//    真正要证伪的是「死信整个消失了」—— 那才意味着没落盘。
	if _, ok := s.taskOpt(got.ID); !ok {
		t.Fatalf("kill -9 后任务 %s 完全消失 —— 死信没有落盘", got.ID)
	}
	if !waitUntil(15*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatalf("kill -9 后死信未收敛，当前 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}
	after := s.task(got.ID)
	if after.Attempts != before.Attempts {
		t.Errorf("重启后尝试次数 = %d, 崩溃前 = %d", after.Attempts, before.Attempts)
	}
	if after.DeadReason != "max_attempts" {
		t.Errorf("重启后死信原因 = %q, 期望 max_attempts", after.DeadReason)
	}

	// 2. 必须出现在死信列表里 —— 运维是从这里发现问题的。
	if _, ok := deadLetterIDs(t, s)[got.ID]; !ok {
		t.Errorf("重启后死信 %s 不在 /v1/notifications?state=dead 列表中", got.ID)
	}

	// 3. 崩溃可能让死信多被投递一次 —— 这是非对称 fsync 策略的直接后果，
	//    不是缺陷。`dead` 记录不同步落盘（只有 `enq` 同步），所以 kill -9 可能丢掉它；
	//    回放时任务停在最后一条 `att` 上，于是重跑一次失败路径再次进入死信。
	//
	//    这落在 at-least-once 契约内（决策 D-005 的表格里写了这一格），
	//    而且额外投递是**有界**的：每次崩溃最多一次，且必然收敛回 dead。
	//    这里断言的正是「有界 + 收敛」，而不是「一次都不会多投」。
	extra := v.CountFor(got.ID) - attemptsBeforeCrash
	if extra > 1 {
		t.Errorf("重启后额外投递 %d 次，期望至多 1 次（丢失 dead 记录只该让失败路径重跑一遍）", extra)
	}
	if st := s.task(got.ID).State; st != "dead" {
		t.Errorf("重跑失败路径后状态 = %q, 期望收敛回 dead", st)
	}
	if got2 := s.task(got.ID); got2.Attempts != before.Attempts {
		t.Errorf("收敛后尝试次数 = %d, 崩溃前 = %d —— 不该超过上限", got2.Attempts, before.Attempts)
	}

	// 4. 最关键的一条：供应商修好后手工重投，必须能用磁盘上的记录
	//    重建出**完整且原封不动**的请求。
	v.SetStatus(http.StatusOK)
	code, raw := s.do(http.MethodPost, "/v1/notifications/"+got.ID+"/retry", nil)
	if code != http.StatusAccepted {
		t.Fatalf("重投死信: %d, %s", code, raw)
	}
	if !waitUntil(15*time.Second, func() bool { return s.task(got.ID).State == "succeeded" }) {
		t.Fatalf("重投后未成功，当前 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}
	assertVendorGotFullRequest(t, v, got.ID)
}

// TestE2E_DeadLetterSurvivesCompactionAndKill9 覆盖更危险的一条路径：
// 死信经过**压缩**之后再崩溃。
//
// 压缩是唯一会主动重写 journal 的地方，而它恰好对成功任务做了瘦身
// （丢掉 Headers/Body 以免供应商凭据长期留在磁盘上）。如果瘦身的分支判断写错，
// 死信的 Target 会被一起清掉 —— 症状是 /retry 返回 202 却再也投不出去，
// 而且**只有在压缩发生过的机器上才复现**。这类 bug 靠读代码很难发现。
func TestE2E_DeadLetterSurvivesCompactionAndKill9(t *testing.T) {
	dead := testutil.NewVendor()
	defer dead.Close()
	dead.SetStatus(http.StatusBadRequest) // 400 → 一次尝试即进死信

	churn := testutil.NewVendor()
	defer churn.Close()
	churn.FailFor(2*time.Second, http.StatusServiceUnavailable) // 制造大量 att 记录后自愈

	s := newServer(t).
		set("NOTIFY_BACKOFF_BASE", "50ms").
		set("NOTIFY_BACKOFF_CAP", "150ms").
		set("NOTIFY_MAX_ATTEMPTS", "500").
		set("NOTIFY_COMPACT_MIN_BYTES", "4000").
		set("NOTIFY_COMPACT_LIVE_RATIO", "0.5").
		set("NOTIFY_MAINTAIN_INTERVAL", "300ms").
		start()

	// 先放一条死信进去。
	got := submitWithHeaders(s, "dl-compact", dead.URL(), http.StatusAccepted)
	if !waitUntil(10*time.Second, func() bool { return s.task(got.ID).State == "dead" }) {
		t.Fatalf("任务未进入死信: %+v", s.task(got.ID))
	}

	// 再制造足够的重试记录，把压缩逼出来。
	var churnIDs []string
	for i := 0; i < 20; i++ {
		churnIDs = append(churnIDs,
			s.submit(fmt.Sprintf("dl-churn-%d", i), churn.URL(), http.StatusAccepted, nil).ID)
	}
	if !waitUntil(30*time.Second, func() bool {
		for _, id := range churnIDs {
			if s.task(id).State != "succeeded" {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("陪跑任务未全部成功。日志:\n%s", s.logs())
	}
	if !waitUntil(15*time.Second, func() bool {
		return strings.Contains(s.logs(), "journal 压缩完成")
	}) {
		t.Fatalf("压缩未发生，本用例失去意义。日志:\n%s", s.logs())
	}

	// —— 压缩之后掉电 ——
	s.kill9()
	s.start()

	after := s.task(got.ID)
	if after.State != "dead" || after.DeadReason != "permanent_response" {
		t.Fatalf("压缩 + kill -9 后死信状态 = %+v", after)
	}

	// 用磁盘上被压缩过的记录重建请求。若瘦身逻辑误伤了死信，这里会失败。
	dead.SetStatus(http.StatusOK)
	code, raw := s.do(http.MethodPost, "/v1/notifications/"+got.ID+"/retry", nil)
	if code != http.StatusAccepted {
		t.Fatalf("重投死信: %d, %s", code, raw)
	}
	if !waitUntil(15*time.Second, func() bool { return s.task(got.ID).State == "succeeded" }) {
		t.Fatalf("压缩后重投未成功，当前 = %+v\n日志:\n%s", s.task(got.ID), s.logs())
	}
	assertVendorGotFullRequest(t, dead, got.ID)
}

// TestE2E_CompactionDropsSucceededCredentials 直接在磁盘上验证 D-033：
// 压缩后，**已成功**任务的供应商凭据必须从 journal 中消失，而**死信**的必须保留。
//
// 这一对性质是互相制约的，只测一半没有意义：
//   - 只测"死信没丢" → 可能是因为压缩根本没瘦身任何东西；
//   - 只测"成功的瘦了" → 可能是瘦身逻辑误伤，把死信也清了，而那会让 /retry 静默失效。
//
// 所以用两个不同的凭据值，在同一个 journal 里同时断言"一个在、一个不在"。
func TestE2E_CompactionDropsSucceededCredentials(t *testing.T) {
	const (
		credSucceeded = "v1=SUCCEEDED-CRED-must-be-erased"
		credDead      = "v1=DEAD-CRED-must-be-kept"
	)

	okVendor := testutil.NewVendor()
	defer okVendor.Close()
	// 先失败一段时间再自愈：让这个任务积累多条 att 记录，压缩才有活可干。
	okVendor.FailFor(1500*time.Millisecond, http.StatusServiceUnavailable)

	deadVendor := testutil.NewVendor()
	defer deadVendor.Close()
	deadVendor.SetStatus(http.StatusBadRequest) // 400 → 一次即死

	s := newServer(t).
		set("NOTIFY_BACKOFF_BASE", "50ms").
		set("NOTIFY_BACKOFF_CAP", "150ms").
		set("NOTIFY_MAX_ATTEMPTS", "500").
		// 空间收益那条线故意设得极高，确保压缩**只可能**由凭据清理线触发 ——
		// 否则通不过时无法区分是哪条线在起作用。
		set("NOTIFY_COMPACT_MIN_BYTES", "1000000000").
		set("NOTIFY_COMPACT_STRIP_THRESHOLD", "1").
		set("NOTIFY_MAINTAIN_INTERVAL", "300ms").
		start()

	okTask := submitWithCred(s, "cred-ok", okVendor.URL(), credSucceeded, http.StatusAccepted)
	deadTask := submitWithCred(s, "cred-dead", deadVendor.URL(), credDead, http.StatusAccepted)

	if !waitUntil(20*time.Second, func() bool {
		return s.task(okTask.ID).State == "succeeded" && s.task(deadTask.ID).State == "dead"
	}) {
		t.Fatalf("任务未到达预期终态: ok=%+v dead=%+v\n日志:\n%s",
			s.task(okTask.ID), s.task(deadTask.ID), s.logs())
	}

	// 压缩前：两个凭据都应该在盘上（证明测试确实在看正确的文件）。
	if before := readJournal(t, s); !strings.Contains(before, credSucceeded) {
		t.Fatalf("压缩前 journal 里就找不到成功任务的凭据 —— 测试本身有问题")
	}

	if !waitUntil(20*time.Second, func() bool {
		return strings.Contains(s.logs(), "journal 压缩完成")
	}) {
		t.Fatalf("压缩未发生，本用例失去意义。日志:\n%s", s.logs())
	}

	// —— 核心断言：压缩后翻开磁盘文件 ——
	after := readJournal(t, s)
	if strings.Contains(after, credSucceeded) {
		t.Errorf("成功任务的供应商凭据仍留在 journal 中 —— D-033 的瘦墓碑没生效")
	}
	if !strings.Contains(after, credDead) {
		t.Errorf("死信的供应商凭据被压缩清除了 —— /retry 将无法重建请求")
	}
	if !strings.Contains(after, okTask.ID) {
		t.Errorf("成功任务的墓碑整个消失了 —— 幂等去重会失效")
	}

	// —— 压缩 + kill -9 之后，两边的能力都要还在 ——
	s.kill9()
	s.start()

	if dup := submitWithCred(s, "cred-ok", okVendor.URL(), credSucceeded, http.StatusOK); !dup.Duplicate {
		t.Error("瘦身后的成功墓碑无法再去重 —— 瘦过头了")
	}
	if code, _ := s.do(http.MethodPost, "/v1/notifications/"+okTask.ID+"/retry", nil); code != http.StatusConflict {
		t.Errorf("重投已成功的任务返回 %d, 期望 409", code)
	}

	// 死信必须仍能用盘上的完整 Target 重投成功。
	if !waitUntil(15*time.Second, func() bool { return s.task(deadTask.ID).State == "dead" }) {
		t.Fatalf("重启后死信未收敛: %+v", s.task(deadTask.ID))
	}
	deadVendor.SetStatus(http.StatusOK)
	if code, raw := s.do(http.MethodPost, "/v1/notifications/"+deadTask.ID+"/retry", nil); code != http.StatusAccepted {
		t.Fatalf("重投死信: %d, %s", code, raw)
	}
	if !waitUntil(15*time.Second, func() bool { return s.task(deadTask.ID).State == "succeeded" }) {
		t.Fatalf("压缩+崩溃后死信重投失败: %+v\n日志:\n%s", s.task(deadTask.ID), s.logs())
	}
	// 重投出去的请求必须带着原始凭据。
	var found bool
	for _, r := range deadVendor.Requests() {
		if r.NotifyID == deadTask.ID && r.Header.Get(dlHeaderName) == credDead {
			found = true
		}
	}
	if !found {
		t.Error("重投的请求没有携带原始凭据 —— Target 未被完整保留")
	}
}
