// Package queue 持有 notifyd 的全部内存状态，并负责它与 WAL 的同步。
//
// 并发模型：一把 sync.RWMutex 保护全部结构（决策 D-016）。临界区都是纯内存
// map/heap 操作（微秒级），在 v1 的千级 QPS 目标下不会成为瓶颈。细粒度分片锁
// 会引入锁序与死锁风险，换来现阶段用不上的并发度 —— 等指标证明它是瓶颈再拆。
package queue

import (
	"container/heap"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/wnzzer/rc_wnzzer/internal/model"
	"github.com/wnzzer/rc_wnzzer/internal/store"
)

var (
	// ErrQueueFull 表示活跃任务已达上限。背压优于 OOM（决策 D-017）：
	// 拒绝时调用方拿到明确的 429 可以自行降级；OOM 时连已收下的任务一起丢。
	ErrQueueFull = errors.New("queue: 活跃任务已达上限")
	ErrNotFound  = errors.New("queue: 任务不存在")
	ErrNotDead   = errors.New("queue: 任务不处于死信状态")
)

// Config 是队列的容量与保留策略。
type Config struct {
	QueueMax int // 活跃（非终态）任务上限
	// TasksMax 是内存中任务总数（含保留期内的终态墓碑）的上限。
	//
	// QueueMax 只管活跃任务，挡不住墓碑：终态任务会 active--，于是
	// 「提交 10 万 → 全部终结 → 再提交 10 万」可以让 map 无限涨下去。
	// 保留期 × 持续流量在设计自己声明的目标量级上就已经是 GB 级了。
	TasksMax int
	// RetentionMS 是**成功**任务的保留时长，同时也是幂等去重窗口。
	RetentionMS int64
	// DeadRetentionMS 是**死信**的保留时长，通常应显著长于 RetentionMS。
	//
	// 成功任务留着只是为了幂等去重，7 天够了；而死信代表「有一条通知确实
	// 没发出去」，它是需要人来处理的**证据**，和成功任务同寿没有道理。
	DeadRetentionMS int64
	CompactMinBytes int64 // 触发压缩的最小文件体积
	// CompactLiveRatio 是触发压缩的冗余阈值：当「快照所需记录数 / 磁盘实际
	// 记录数」低于它时压缩。0.5 表示「磁盘上有一半以上是冗余记录才值得重写」。
	CompactLiveRatio float64
	// StripThreshold 是「凭据清理」这条独立的压缩触发线：累计这么多已成功任务
	// 的完整 Target 仍留在磁盘上时，即便没有空间收益也强制压缩一次。
	StripThreshold int
}

// Queue 是内存状态 + WAL 的组合体。
type Queue struct {
	cfg   Config
	store store.Store
	log   *slog.Logger

	mu     sync.RWMutex
	tasks  map[string]*model.Task // 全部任务（含保留期内的终态）
	idem   map[string]string      // 幂等键 -> 任务 ID
	due    dueHeap                // 已排程、待投递
	active int                    // 非终态任务数

	// unstripped 是自上次压缩以来新增的已成功任务数。
	//
	// 它们的 enq 记录仍带着完整 Target（含供应商 Authorization），而磁盘上的
	// 字节只能靠重写 journal 才能抹掉 —— 也就是只能靠压缩。没有这个计数，
	// 凭据清理就只是"碰巧压缩了才会发生"，而不是一个有上界的保证。
	unstripped int

	// wake 用于在有新任务或排程提前时唤醒调度循环。容量 1 + 非阻塞发送，
	// 使它成为一个「有事发生」的电平信号而非事件队列 —— 不会因为堆积而失真。
	wake chan struct{}
}

// New 构造一个空队列。调用方应紧接着调用 Restore 从 WAL 重建状态。
func New(cfg Config, s store.Store, log *slog.Logger) *Queue {
	return &Queue{
		cfg:   cfg,
		store: s,
		log:   log,
		tasks: make(map[string]*model.Task),
		idem:  make(map[string]string),
		wake:  make(chan struct{}, 1),
	}
}

// Wake 返回唤醒信号通道，供调度循环 select。
func (q *Queue) Wake() <-chan struct{} { return q.wake }

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default: // 已有未消费的信号，无需重复
	}
}

// Restore 回放 WAL 重建内存状态。只应在启动时调用一次。
//
// 关键语义：崩溃时处于 sending 的任务无法知道请求是否已送达，一律按 pending
// 重新投递。这是 at-least-once 在恢复路径上的直接体现 —— 我们选择「可能重复」
// 而不是「可能丢失」，因为承诺 C1 的优先级更高（spec §5.3）。
func (q *Queue) Restore() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.store.Replay(func(r *store.Record) error {
		switch r.T {
		case store.RecEnqueue:
			if r.Task == nil {
				return fmt.Errorf("enq 记录缺少 task 字段")
			}
			t := *r.Task // 拷贝，避免持有回放缓冲
			t.State = model.StatePending
			t.NextAt = t.CreatedAt
			t.UpdatedAt = r.TS
			q.tasks[t.ID] = &t
			if t.IdemKey != "" {
				q.idem[t.IdemKey] = t.ID
			}
		case store.RecAttempt:
			if t, ok := q.tasks[r.ID]; ok {
				t.Attempts = r.N
				t.NextAt = r.NextAt
				t.LastCode = r.Code
				t.LastErr = r.Err
				t.State = model.StateWaiting
				t.UpdatedAt = r.TS
			}
		case store.RecDone:
			if t, ok := q.tasks[r.ID]; ok {
				t.Attempts = r.N
				t.LastCode = r.Code
				t.LastResp = r.Resp
				t.State = model.StateSucceeded
				t.UpdatedAt = r.TS
				// 与 OnSuccess 保持一致：成功任务在内存中不持有凭据。
				t.Target = model.Target{URL: t.Target.URL, Method: t.Target.Method}
			}
		case store.RecDead:
			if t, ok := q.tasks[r.ID]; ok {
				t.Attempts = r.N
				t.DeadReason = r.Why
				t.State = model.StateDead
				t.UpdatedAt = r.TS
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 重建堆与计数。终态任务不入堆，只作为墓碑参与幂等去重与状态查询。
	var revived int
	for _, t := range q.tasks {
		if t.State.Terminal() {
			continue
		}
		t.State = model.StatePending // waiting/sending 一律回到 pending
		q.active++
		heap.Push(&q.due, t)
		revived++
	}
	q.log.Info("WAL 回放完成",
		"任务总数", len(q.tasks), "待投递", revived, "终态墓碑", len(q.tasks)-revived)
	q.signal()
	return nil
}

// Submit 接收一个新任务。
//
// 返回值 dup 为 true 时，返回的是幂等键命中的既有任务，未产生新任务。
//
// 顺序很重要：先 AppendEnqueue（同步 fsync）成功，再写入内存。反过来会出现
// 「内存里有、磁盘上没有」的窗口 —— 那正是承诺 C1 要排除的情况。
func (q *Queue) Submit(t *model.Task) (res *model.Task, dup bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if id, ok := q.idem[t.IdemKey]; ok {
		return q.tasks[id], true, nil
	}
	if q.active >= q.cfg.QueueMax {
		return nil, false, ErrQueueFull
	}
	if err := q.store.AppendEnqueue(t); err != nil {
		return nil, false, err
	}

	t.State = model.StatePending
	t.NextAt = t.CreatedAt
	t.UpdatedAt = t.CreatedAt
	q.tasks[t.ID] = t
	q.idem[t.IdemKey] = t.ID
	q.active++
	heap.Push(&q.due, t)
	q.signal()
	return t, false, nil
}

// Lease 取出一个到期任务并标记为 sending。没有到期任务时返回 nil。
//
// 第二个返回值是「下一个任务的到期时刻」，调度循环用它设置定时器，
// 避免空转轮询。堆为空时返回 0。
func (q *Queue) Lease(now int64) (*model.Task, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.due) == 0 {
		return nil, 0
	}
	if q.due[0].NextAt > now {
		return nil, q.due[0].NextAt
	}
	t := heap.Pop(&q.due).(*model.Task)
	t.State = model.StateSending
	t.UpdatedAt = now
	var next int64
	if len(q.due) > 0 {
		next = q.due[0].NextAt
	}
	return t, next
}

// OnRetry 记录一次可重试失败，并把任务按 nextAt 重新排程。
func (q *Queue) OnRetry(t *model.Task, code int, errMsg string, nextAt int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t.Attempts++
	t.LastCode = code
	t.LastErr = errMsg
	t.NextAt = nextAt
	t.State = model.StateWaiting
	t.UpdatedAt = model.NowMS()
	if err := q.store.AppendAttempt(t.ID, t.Attempts, store.ResRetry, code, errMsg, nextAt); err != nil {
		return err
	}
	heap.Push(&q.due, t)
	q.signal()
	return nil
}

// OnSuccess 把任务置为成功终态，并保存响应体摘要。
//
// 为什么成功也要留响应摘要：不少供应商用 HTTP 200 包裹业务错误
// （`200 {"code":40001,"msg":"contact not found"}`）。notifyd 按 2xx 判成功是
// 有意的窄定义（边界 B3），但把已经读到的那几百字节直接扔掉，会让这类故障
// **完全不可排查** —— 运营说「CRM 状态没变」，而你只能看到一个孤零零的 200。
// 留下它不改变任何判定逻辑，只是别把手里的信息丢了。
//
// 同时**立刻**从内存中抹掉 Headers/Body：
//   - 安全：Headers 里是供应商的 Authorization，成功之后没有任何理由继续持有；
//   - 内存：墓碑要在内存里待满整个保留期，而 Headers/Body 往往是任务里最大的部分。
//
// 磁盘上的那份要等压缩才能抹掉（字节只能靠重写 journal 消除，见 D-041），
// 但内存这份可以立即清 —— 没有理由等。
func (q *Queue) OnSuccess(t *model.Task, code int, resp string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t.Attempts++
	t.LastCode = code
	t.LastErr = ""
	t.LastResp = resp
	t.State = model.StateSucceeded
	t.UpdatedAt = model.NowMS()
	t.Target = model.Target{URL: t.Target.URL, Method: t.Target.Method}
	q.active--
	q.unstripped++
	return q.store.AppendDone(t.ID, t.Attempts, code, resp)
}

// OnDead 放弃任务并进入死信。code/errMsg 用于保留最后一次失败的现场。
func (q *Queue) OnDead(t *model.Task, code int, errMsg, why string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t.Attempts++
	t.LastCode = code
	t.LastErr = errMsg
	t.DeadReason = why
	t.State = model.StateDead
	t.UpdatedAt = model.NowMS()
	q.active--
	return q.store.AppendDead(t.ID, t.Attempts, why)
}

// Get 返回任务快照。返回的是副本，调用方可安全读取。
func (q *Queue) Get(id string) (model.Task, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	t, ok := q.tasks[id]
	if !ok {
		return model.Task{}, false
	}
	return *t, true
}

// ListByState 返回处于指定状态的任务快照，最多 limit 条。
func (q *Queue) ListByState(state model.State, limit int) []model.Task {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]model.Task, 0, min(limit, len(q.tasks)))
	for _, t := range q.tasks {
		if t.State != state {
			continue
		}
		out = append(out, *t)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// Revive 把一个死信任务重新投入队列（POST /{id}/retry）。
//
// 这是「供应商宕机三天」场景的运维出口。没有它，死信就只是一座坟场。
func (q *Queue) Revive(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if t.State != model.StateDead {
		return ErrNotDead
	}
	if q.active >= q.cfg.QueueMax {
		return ErrQueueFull
	}
	now := model.NowMS()
	t.Attempts = 0
	t.DeadReason = ""
	t.LastErr = ""
	t.State = model.StatePending
	t.NextAt = now
	t.UpdatedAt = now
	// 复活也要延长绝对 deadline，否则任务会在下一次尝试时立刻因超时再次死亡。
	t.CreatedAt = now
	q.active++
	// 用一条 att 记录把「重置为第 0 次尝试」写进 journal，让重放能还原复活结果。
	if err := q.store.AppendAttempt(t.ID, 0, store.ResRetry, 0, "revived", now); err != nil {
		return err
	}
	heap.Push(&q.due, t)
	q.signal()
	return nil
}

// Counts 返回各状态的任务数，供 /metrics 使用。
func (q *Queue) Counts() (map[model.State]int, int, int64) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	m := make(map[model.State]int, 5)
	var oldestPending int64
	for _, t := range q.tasks {
		m[t.State]++
		if !t.State.Terminal() && (oldestPending == 0 || t.CreatedAt < oldestPending) {
			oldestPending = t.CreatedAt
		}
	}
	return m, len(q.due), oldestPending
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Defer 把一个已 Lease 出来但暂时无法投递的任务放回堆里，不计入尝试次数、
// 不写 WAL。
//
// 不写 WAL 是有意的：这是纯粹的**排程**调整而非投递结果。即便此刻崩溃，
// 该任务回放后也会作为 pending 重新排程，结果完全一致 —— 为它落盘等于
// 用磁盘 IO 去持久化一个无需持久化的事实。
func (q *Queue) Defer(t *model.Task, nextAt int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t.State = model.StatePending
	t.NextAt = nextAt
	heap.Push(&q.due, t)
	q.signal()
}
