package queue

import (
	"errors"
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
	if err := q.OnSuccess(got, 200); err != nil {
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
	if err := q.OnSuccess(l1, 200); err != nil {
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
	q.OnSuccess(leased, 200)

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
	q.OnSuccess(l1, 200)
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
