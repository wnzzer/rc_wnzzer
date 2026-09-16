package queue

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/wnzzer/rc_wnzzer/internal/model"
	"github.com/wnzzer/rc_wnzzer/internal/store"
)

func testCfg() Config {
	return Config{
		QueueMax:         100,
		RetentionMS:      7 * 24 * 3600 * 1000,
		CompactMinBytes:  1 << 30, // 单测里不触发自动压缩
		CompactLiveRatio: 0.5,
	}
}

func newTestQueue(t *testing.T, dir string) (*Queue, *store.WAL) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := store.OpenWAL(dir, log)
	if err != nil {
		t.Fatal(err)
	}
	q := New(testCfg(), w, log)
	if err := q.Restore(); err != nil {
		t.Fatal(err)
	}
	return q, w
}

func newTask(idem string) *model.Task {
	return &model.Task{
		ID:        model.NewID(),
		IdemKey:   idem,
		Target:    model.Target{URL: "https://vendor.example.com/hook", Method: "POST", Body: "{}"},
		CreatedAt: model.NowMS(),
		KeyID:     "tester",
	}
}

func TestSubmit_Idempotency(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()

	first, dup, err := q.Submit(newTask("same-key"))
	if err != nil || dup {
		t.Fatalf("首次提交: dup=%v err=%v", dup, err)
	}
	second, dup, err := q.Submit(newTask("same-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !dup {
		t.Error("第二次提交应被标记为 duplicate")
	}
	if second.ID != first.ID {
		t.Errorf("重复提交返回了不同任务: %s vs %s", second.ID, first.ID)
	}
}

func TestSubmit_QueueFull(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()
	q.cfg.QueueMax = 2

	for i := 0; i < 2; i++ {
		if _, _, err := q.Submit(newTask(string(rune('a' + i)))); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := q.Submit(newTask("overflow")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, 期望 ErrQueueFull", err)
	}

	// 终态任务不占活跃名额：完成一个之后应当能再收一个。
	got, _ := q.Lease(model.NowMS())
	if got == nil {
		t.Fatal("没有到期任务")
	}
	if err := q.OnSuccess(got, 200, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Submit(newTask("after-success")); err != nil {
		t.Errorf("成功任务应释放活跃名额, err = %v", err)
	}
}

func TestLease_OrderedByNextAt(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()

	now := model.NowMS()
	// 三个任务，人为设定不同的到期时刻。
	for i, delta := range []int64{300, 100, 200} {
		tk := newTask(string(rune('a' + i)))
		if _, _, err := q.Submit(tk); err != nil {
			t.Fatal(err)
		}
		leased, _ := q.Lease(now + 1000)
		q.Defer(leased, now+delta)
	}

	var order []int64
	for i := 0; i < 3; i++ {
		tk, _ := q.Lease(now + 1000)
		if tk == nil {
			t.Fatalf("第 %d 次 Lease 返回 nil", i)
		}
		order = append(order, tk.NextAt-now)
	}
	want := []int64{100, 200, 300}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("出队顺序 = %v, 期望 %v", order, want)
		}
	}
}

func TestLease_NotDueYet(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()

	tk := newTask("future")
	if _, _, err := q.Submit(tk); err != nil {
		t.Fatal(err)
	}
	leased, _ := q.Lease(model.NowMS())
	q.Defer(leased, model.NowMS()+60_000)

	got, next := q.Lease(model.NowMS())
	if got != nil {
		t.Error("未到期的任务不应被 Lease 出来")
	}
	if next == 0 {
		t.Error("应返回下一个任务的到期时刻供调度器设置定时器")
	}
}

// TestRestore_SendingBecomesPending 验证 spec §5.3 的恢复语义：
// 崩溃时处于 sending 的任务无法判断是否已送达，一律重新投递。
func TestRestore_SendingBecomesPending(t *testing.T) {
	dir := t.TempDir()
	q, w := newTestQueue(t, dir)

	tk := newTask("in-flight")
	if _, _, err := q.Submit(tk); err != nil {
		t.Fatal(err)
	}
	leased, _ := q.Lease(model.NowMS()) // 进入 sending
	if leased.State != model.StateSending {
		t.Fatalf("Lease 后状态 = %q", leased.State)
	}
	w.Close() // 模拟进程退出，sending 状态从未落盘

	q2, w2 := newTestQueue(t, dir)
	defer w2.Close()
	got, ok := q2.Get(leased.ID)
	if !ok {
		t.Fatal("任务在回放后丢失")
	}
	if got.State != model.StatePending {
		t.Errorf("回放后状态 = %q, 期望 pending（重新投递优于丢失）", got.State)
	}
	if leased2, _ := q2.Lease(model.NowMS()); leased2 == nil {
		t.Error("回放后的任务应当立即可被 Lease")
	}
}

func TestRestore_PreservesTerminalAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	q, w := newTestQueue(t, dir)

	ok := newTask("k-ok")
	dead := newTask("k-dead")
	q.Submit(ok)
	q.Submit(dead)
	l1, _ := q.Lease(model.NowMS())
	l2, _ := q.Lease(model.NowMS())
	if err := q.OnSuccess(l1, 200, ""); err != nil {
		t.Fatal(err)
	}
	if err := q.OnDead(l2, 400, "bad", "permanent_response"); err != nil {
		t.Fatal(err)
	}
	w.Close()

	q2, w2 := newTestQueue(t, dir)
	defer w2.Close()
	if got, _ := q2.Get(l1.ID); got.State != model.StateSucceeded {
		t.Errorf("成功任务回放后状态 = %q", got.State)
	}
	if got, _ := q2.Get(l2.ID); got.State != model.StateDead || got.DeadReason != "permanent_response" {
		t.Errorf("死信回放后 = %+v", got)
	}
	// 终态墓碑必须继续参与幂等去重，否则重启后同 key 会被重复投递。
	if _, dup, _ := q2.Submit(newTask("k-ok")); !dup {
		t.Error("回放后幂等去重失效")
	}
	// 终态任务不占活跃名额。
	if q2.active != 0 {
		t.Errorf("回放后活跃任务数 = %d, 期望 0", q2.active)
	}
}

func TestRevive(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()

	tk := newTask("k-revive")
	q.Submit(tk)
	leased, _ := q.Lease(model.NowMS())
	if err := q.OnDead(leased, 503, "down", "max_attempts"); err != nil {
		t.Fatal(err)
	}

	if err := q.Revive(leased.ID); err != nil {
		t.Fatalf("Revive: %v", err)
	}
	got, _ := q.Get(leased.ID)
	if got.State != model.StatePending {
		t.Errorf("复活后状态 = %q, 期望 pending", got.State)
	}
	if got.Attempts != 0 {
		t.Errorf("复活后尝试次数 = %d, 期望重置为 0", got.Attempts)
	}
	if got.DeadReason != "" {
		t.Errorf("复活后仍残留死信原因 %q", got.DeadReason)
	}
	// 复活后必须立刻可被 Lease，否则运维按了按钮却什么也没发生。
	if l, _ := q.Lease(model.NowMS()); l == nil {
		t.Error("复活的任务未进入到期队列")
	}

	// 只有死信能被复活。
	if err := q.Revive(leased.ID); !errors.Is(err, ErrNotDead) {
		t.Errorf("复活非死信任务应返回 ErrNotDead, 得到 %v", err)
	}
	if err := q.Revive("no-such-id"); !errors.Is(err, ErrNotFound) {
		t.Errorf("复活不存在的任务应返回 ErrNotFound, 得到 %v", err)
	}
}

// TestPurgeExpired_ClosesIdempotencyWindow 验证边界 B8：
// 幂等去重窗口 = 终态保留期。这是一个必须写进 API 文档的限制，
// 用测试把它钉住，防止日后有人"顺手"改掉保留逻辑而不知道副作用。
func TestPurgeExpired_ClosesIdempotencyWindow(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()
	q.cfg.RetentionMS = 1 // 保留期 1ms

	tk := newTask("k-expire")
	q.Submit(tk)
	leased, _ := q.Lease(model.NowMS())
	q.OnSuccess(leased, 200, "")

	// 未过期前，幂等键仍然生效。
	if _, dup, _ := q.Submit(newTask("k-expire")); !dup {
		t.Fatal("保留期内幂等键应当生效")
	}

	origNow := model.NowMS
	model.NowMS = func() int64 { return origNow() + 10_000 }
	defer func() { model.NowMS = origNow }()

	if n := q.purgeExpired(); n == 0 {
		t.Fatal("过期任务未被清理")
	}
	if _, ok := q.Get(leased.ID); ok {
		t.Error("过期任务应已从内存移除")
	}
	// 过期后，同一个 key 会被当作新任务 —— 这就是 B8 描述的限制。
	if _, dup, _ := q.Submit(newTask("k-expire")); dup {
		t.Error("过保留期后幂等键不应再命中（这是已知限制，不是 bug）")
	}
}

func TestCompact_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	q, w := newTestQueue(t, dir)

	// 一个成功、一个死信、一个仍在重试。
	done := newTask("k-done")
	dead := newTask("k-dead")
	live := newTask("k-live")
	q.Submit(done)
	q.Submit(dead)
	q.Submit(live)
	l1, _ := q.Lease(model.NowMS())
	l2, _ := q.Lease(model.NowMS())
	l3, _ := q.Lease(model.NowMS())
	q.OnSuccess(l1, 200, "")
	q.OnDead(l2, 400, "bad request", "permanent_response")
	nextAt := model.NowMS() + 5000
	q.OnRetry(l3, 503, "unavailable", nextAt)

	if err := q.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	w.Close()

	q2, w2 := newTestQueue(t, dir)
	defer w2.Close()

	if got, _ := q2.Get(l1.ID); got.State != model.StateSucceeded {
		t.Errorf("成功任务压缩后 = %+v", got)
	}
	// 成功任务的墓碑被瘦身：Headers/Body 清掉（不再需要，且含供应商凭据），
	// 但 URL 保留（排障常用）。
	if got, _ := q2.Get(l1.ID); got.Target.Body != "" {
		t.Errorf("成功任务的 body 应在压缩时被清除, 仍为 %q", got.Target.Body)
	}
	if got, _ := q2.Get(l1.ID); got.Target.URL == "" {
		t.Error("成功任务应保留 URL 供排障")
	}
	// 死信必须保留完整请求，否则 /retry 无从重建。
	if got, _ := q2.Get(l2.ID); got.Target.Body != "{}" || got.DeadReason != "permanent_response" {
		t.Errorf("死信压缩后丢失了重投所需信息: %+v", got.Target)
	}
	// 存活任务的运行时状态必须完整还原。
	got, ok := q2.Get(l3.ID)
	if !ok {
		t.Fatal("存活任务压缩后丢失")
	}
	if got.Attempts != 1 {
		t.Errorf("存活任务尝试次数 = %d, 期望 1", got.Attempts)
	}
	if got.NextAt != nextAt {
		t.Errorf("存活任务 NextAt = %d, 期望 %d", got.NextAt, nextAt)
	}
	if q2.active != 1 {
		t.Errorf("压缩回放后活跃任务数 = %d, 期望 1", q2.active)
	}
}

// TestMaybeCompact_TwoIndependentTriggers 钉住压缩的两条触发线。
//
// 这两条线服务于**不同目的**，必须各自独立生效：
//   - 空间收益线：冗余记录太多，压缩能省磁盘；
//   - 凭据清理线：已成功任务的 Authorization 还留在盘上，压缩是唯一能抹掉它的手段。
//
// 只有线 1 时，凭据清理会退化成「碰巧压缩了才会发生」—— 一个低冗余的 journal
// 可以让凭据安稳躺满整个保留期。这个测试就是防止有人日后把线 2 当成冗余优化删掉。
func TestMaybeCompact_TwoIndependentTriggers(t *testing.T) {
	setup := func(t *testing.T, tune func(*Config)) (*Queue, *store.WAL) {
		t.Helper()
		q, w := newTestQueue(t, t.TempDir())
		tune(&q.cfg)
		// 造一个已成功任务：它的 enq 记录带着完整 Target，躺在盘上。
		tk := newTask("k-cred")
		tk.Target.Headers = map[string]string{"Authorization": "Bearer SECRET-TOKEN"}
		if _, _, err := q.Submit(tk); err != nil {
			t.Fatal(err)
		}
		leased, _ := q.Lease(model.NowMS())
		if err := q.OnSuccess(leased, 200, ""); err != nil {
			t.Fatal(err)
		}
		return q, w
	}

	onDisk := func(t *testing.T, w *store.WAL) bool {
		t.Helper()
		found := false
		if err := w.Replay(func(r *store.Record) error {
			if r.Task != nil && r.Task.Target.Headers["Authorization"] != "" {
				found = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return found
	}

	t.Run("两条线都不满足时不压缩", func(t *testing.T) {
		q, w := setup(t, func(c *Config) {
			c.CompactMinBytes = 1 << 30 // 空间线关掉
			c.StripThreshold = 100      // 凭据线阈值远未达到
		})
		defer w.Close()
		if err := q.maybeCompact(); err != nil {
			t.Fatal(err)
		}
		if !onDisk(t, w) {
			t.Error("不该压缩却压缩了")
		}
	})

	t.Run("仅凭据线满足也要压缩", func(t *testing.T) {
		q, w := setup(t, func(c *Config) {
			c.CompactMinBytes = 1 << 30 // 空间线明确关掉
			c.StripThreshold = 1        // 只靠凭据线
		})
		defer w.Close()
		if !onDisk(t, w) {
			t.Fatal("前置条件不成立：凭据本来就不在盘上")
		}
		if err := q.maybeCompact(); err != nil {
			t.Fatal(err)
		}
		if onDisk(t, w) {
			t.Error("凭据线触发后，成功任务的 Authorization 仍留在磁盘上")
		}
		// 压缩后计数归零，不该反复触发。
		if q.unstrippedCount() != 0 {
			t.Errorf("压缩后 unstripped = %d, 期望 0", q.unstrippedCount())
		}
	})

	t.Run("阈值为 0 表示关闭凭据线", func(t *testing.T) {
		q, w := setup(t, func(c *Config) {
			c.CompactMinBytes = 1 << 30
			c.StripThreshold = 0
		})
		defer w.Close()
		if err := q.maybeCompact(); err != nil {
			t.Fatal(err)
		}
		if !onDisk(t, w) {
			t.Error("阈值为 0 时不该压缩")
		}
	})

	t.Run("死信的凭据不受凭据线影响", func(t *testing.T) {
		q, w := newTestQueue(t, t.TempDir())
		defer w.Close()
		q.cfg.CompactMinBytes = 1 << 30
		q.cfg.StripThreshold = 1

		dead := newTask("k-dead-cred")
		dead.Target.Headers = map[string]string{"Authorization": "Bearer DEAD-TOKEN"}
		q.Submit(dead)
		l1, _ := q.Lease(model.NowMS())
		q.OnDead(l1, 400, "bad", "permanent_response")

		// 再来一个成功任务把凭据线顶上去。
		ok := newTask("k-ok-cred")
		ok.Target.Headers = map[string]string{"Authorization": "Bearer OK-TOKEN"}
		q.Submit(ok)
		l2, _ := q.Lease(model.NowMS())
		q.OnSuccess(l2, 200, "")

		if err := q.maybeCompact(); err != nil {
			t.Fatal(err)
		}

		var okCred, deadCred string
		if err := w.Replay(func(r *store.Record) error {
			if r.Task == nil {
				return nil
			}
			switch r.ID {
			case l1.ID:
				deadCred = r.Task.Target.Headers["Authorization"]
			case l2.ID:
				okCred = r.Task.Target.Headers["Authorization"]
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if okCred != "" {
			t.Errorf("成功任务的凭据未被清除: %q", okCred)
		}
		// 死信必须保留完整 Target，否则 /retry 无从重建请求。
		if deadCred != "Bearer DEAD-TOKEN" {
			t.Errorf("死信的凭据被误删了: %q —— /retry 将失效", deadCred)
		}
	})
}

// TestPurgeExpired_TieredRetention 验证成功任务与死信用**不同**的保留期。
//
// 最初的实现是 `if t.State.Terminal()` 一视同仁，7 天后成功记录和死信一起消失。
// 而死信代表「有一条通知确实没发出去」，是需要人来处理的证据 —— 和成功记录
// 同寿没有道理。这个测试防止有人日后"简化"回去。
func TestPurgeExpired_TieredRetention(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()
	q.cfg.RetentionMS = 1000      // 成功任务保留 1s
	q.cfg.DeadRetentionMS = 60000 // 死信保留 60s

	ok := newTask("k-ok")
	dead := newTask("k-dead")
	q.Submit(ok)
	q.Submit(dead)
	l1, _ := q.Lease(model.NowMS())
	l2, _ := q.Lease(model.NowMS())
	q.OnSuccess(l1, 200, `{"ok":true}`)
	q.OnDead(l2, 400, "bad", "permanent_response")

	// 推进 5 秒：越过成功任务的保留期，但远未到死信的。
	orig := model.NowMS
	model.NowMS = func() int64 { return orig() + 5000 }
	defer func() { model.NowMS = orig }()

	if n := q.purgeExpired(); n != 1 {
		t.Fatalf("清理数量 = %d, 期望 1（只该清成功任务）", n)
	}
	if _, ok := q.Get(l1.ID); ok {
		t.Error("成功任务已过保留期，应被清理")
	}
	if got, exists := q.Get(l2.ID); !exists || got.State != model.StateDead {
		t.Error("死信保留期未到，不该被清理 —— 它是通知没送达的唯一证据")
	}
}

// TestEnforceTaskCap 验证内存总量兜底，以及**牺牲顺序**。
//
// QueueMax 只管活跃任务，终态任务会 active--，所以
// 「提交 N 个 → 全部终结 → 再提交 N 个」可以让 map 一直涨。
// 这个兜底就是堵那条路（决策 D-017 当初只堵了活跃任务那一条）。
func TestEnforceTaskCap(t *testing.T) {
	q, w := newTestQueue(t, t.TempDir())
	defer w.Close()
	q.cfg.RetentionMS = 1 << 40 // 关掉按时间清理，隔离出容量逻辑
	q.cfg.DeadRetentionMS = 1 << 40

	// 3 个成功（时间从旧到新）+ 2 个死信 + 1 个活跃 = 6 个任务。
	var okIDs, deadIDs []string
	base := model.NowMS()
	mk := func(idem string, terminal func(*model.Task), at int64) string {
		orig := model.NowMS
		model.NowMS = func() int64 { return at }
		defer func() { model.NowMS = orig }()
		tk := newTask(idem)
		q.Submit(tk)
		l, _ := q.Lease(at)
		terminal(l)
		return l.ID
	}
	for i := 0; i < 3; i++ {
		okIDs = append(okIDs, mk(fmt.Sprintf("ok-%d", i),
			func(l *model.Task) { q.OnSuccess(l, 200, "") }, base+int64(i)))
	}
	for i := 0; i < 2; i++ {
		deadIDs = append(deadIDs, mk(fmt.Sprintf("dead-%d", i),
			func(l *model.Task) { q.OnDead(l, 400, "", "permanent_response") }, base+100+int64(i)))
	}
	live := newTask("live")
	q.Submit(live)

	if len(q.tasks) != 6 {
		t.Fatalf("前置条件：任务数 = %d, 期望 6", len(q.tasks))
	}

	// 上限设为 4 → 需要清掉 2 个，且必须是**最老的两个成功任务**。
	q.cfg.TasksMax = 4
	if n := q.enforceTaskCap(); n != 2 {
		t.Fatalf("清理数量 = %d, 期望 2", n)
	}
	for _, id := range okIDs[:2] {
		if _, ok := q.Get(id); ok {
			t.Errorf("最老的成功任务 %s 应被优先清理", id)
		}
	}
	if _, ok := q.Get(okIDs[2]); !ok {
		t.Error("较新的成功任务不该被清理")
	}
	// 死信丢了是永久失去证据，必须排在成功任务之后被牺牲。
	for _, id := range deadIDs {
		if _, ok := q.Get(id); !ok {
			t.Errorf("死信 %s 不该在还有成功任务可清时被牺牲", id)
		}
	}
	// 活跃任务永远不能丢 —— 那是承诺 C1 的范围。
	if _, ok := q.Get(live.ID); !ok {
		t.Error("活跃任务被清理了，违反承诺 C1")
	}
	if q.cfg.TasksMax = 0; q.enforceTaskCap() != 0 {
		t.Error("上限为 0 应表示不限制")
	}
}

// TestOnSuccess_KeepsResponseAndStripsCredentials 覆盖两件同时发生的事。
func TestOnSuccess_KeepsResponseAndStripsCredentials(t *testing.T) {
	dir := t.TempDir()
	q, w := newTestQueue(t, dir)

	tk := newTask("k-resp")
	tk.Target.Headers = map[string]string{"Authorization": "Bearer SECRET"}
	tk.Target.Body = `{"contact_id":1}`
	q.Submit(tk)
	l, _ := q.Lease(model.NowMS())
	// 供应商用 200 包裹业务错误 —— 正是留响应摘要要对付的场景。
	const body = `{"code":40001,"msg":"contact not found"}`
	if err := q.OnSuccess(l, 200, body); err != nil {
		t.Fatal(err)
	}

	got, _ := q.Get(l.ID)
	if got.LastResp != body {
		t.Errorf("响应摘要 = %q, 期望 %q", got.LastResp, body)
	}
	// 凭据必须**立刻**离开内存，不等压缩。
	if got.Target.Headers != nil {
		t.Errorf("成功后内存中仍持有 Headers: %v", got.Target.Headers)
	}
	if got.Target.Body != "" {
		t.Errorf("成功后内存中仍持有 Body: %q", got.Target.Body)
	}
	if got.Target.URL == "" {
		t.Error("URL 应保留供排障")
	}

	// 响应摘要要活过重启。
	w.Close()
	q2, w2 := newTestQueue(t, dir)
	defer w2.Close()
	after, _ := q2.Get(l.ID)
	if after.LastResp != body {
		t.Errorf("重启后响应摘要 = %q, 期望 %q", after.LastResp, body)
	}

	// 但压缩是它的过期点：排障信息的有用期是事故后几小时，
	// 不该跟着墓碑在磁盘上躺满整个保留期。
	if err := q2.Compact(); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	q3, w3 := newTestQueue(t, dir)
	defer w3.Close()
	compacted, ok := q3.Get(l.ID)
	if !ok {
		t.Fatal("压缩后墓碑丢失")
	}
	if compacted.LastResp != "" {
		t.Errorf("压缩后响应摘要应被丢弃, 仍为 %q", compacted.LastResp)
	}
}
