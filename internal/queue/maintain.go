package queue

import (
	"github.com/wnzzer/rc_wnzzer/internal/model"
	"github.com/wnzzer/rc_wnzzer/internal/store"
)

// Maintain 执行周期性维护：清理过期终态任务，必要时压缩 journal。
// 由主循环的定时器调用。
func (q *Queue) Maintain() {
	if n := q.purgeExpired(); n > 0 {
		q.log.Info("清理过期终态任务", "数量", n)
	}
	if err := q.maybeCompact(); err != nil {
		q.log.Error("压缩 journal 失败", "err", err)
	}
}

// purgeExpired 移除超出保留期的终态任务。
//
// 副作用（spec §8 / 边界 B8）：幂等去重窗口 = 保留期。任务被清理后，同一个
// idempotency_key 再次提交会被当作新任务。这个限制必须写进 API 文档，
// 而不是留给调用方去踩。
func (q *Queue) purgeExpired() int {
	cutoff := model.NowMS() - q.cfg.RetentionMS
	q.mu.Lock()
	defer q.mu.Unlock()

	n := 0
	for id, t := range q.tasks {
		if !t.State.Terminal() || t.UpdatedAt >= cutoff {
			continue
		}
		delete(q.tasks, id)
		if t.IdemKey != "" && q.idem[t.IdemKey] == id {
			delete(q.idem, t.IdemKey)
		}
		n++
	}
	return n
}

// maybeCompact 在「文件够大 **且** 冗余记录够多」时重写 journal。
//
// 关键在于「冗余」怎么量。初版用的是「终态记录占比」，那是**错的**：压缩的
// 收益来自折叠同一个任务的多条 att 记录（重试 100 次 = 100 条 att，压缩后
// 只剩 1 条），而这类记录对终态占比的贡献是 0。结果就是一个 99% 都是冗余
// 重试记录的 journal 永远不会被压缩 —— 恰好漏掉了最该压缩的那种情况。
//
// 正确的量法是直接比较「快照需要多少条记录」与「磁盘上实际有多少条」。
// 这个比值就是压缩收益本身，不需要任何间接推断。
func (q *Queue) maybeCompact() error {
	st := q.store.Stats()
	if st.Bytes < q.cfg.CompactMinBytes || st.Records == 0 {
		return nil
	}
	if float64(q.snapshotSize())/float64(st.Records) >= q.cfg.CompactLiveRatio {
		return nil // 冗余不足，压缩省不下什么
	}
	return q.Compact()
}

// snapshotSize 估算一次快照会产生多少条记录：每个任务最多 2 条（enq + 状态）。
// 取上界而非精确值 —— 它只用于比较，多算一点只会让压缩更保守。
func (q *Queue) snapshotSize() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return int64(len(q.tasks)) * 2
}

// Compact 立即重写 journal。导出是为了让验收测试能确定性地触发它。
func (q *Queue) Compact() error {
	snap := q.snapshot()
	return q.store.Compact(snap)
}

// snapshot 把当前内存状态编码成一串记录。
//
// 设计要点：快照复用**和正常写入完全相同的四种记录类型**，因此压缩产物走的是
// 同一条回放代码路径，不需要任何特殊分支。这意味着压缩的正确性被回放的测试
// 顺带覆盖了 —— 少一条需要独立验证的逻辑，就少一处可能出错的地方。
//
// 每个任务最多产生 2 条记录（enq + 状态记录），相对压缩前的完整尝试历史，
// 这已是可达的下界。
func (q *Queue) snapshot() []store.Record {
	q.mu.RLock()
	defer q.mu.RUnlock()

	out := make([]store.Record, 0, len(q.tasks)*2)
	for _, t := range q.tasks {
		snap := *t // 拷贝；运行时字段带 json:"-"，不会落盘
		if t.State == model.StateSucceeded {
			// 成功任务只需要留一个**瘦墓碑**：它此后的唯一用途是参与幂等去重
			// 和状态查询，Headers / Body 再也不会被用到。
			//
			// 这不只是省体积（虽然省得很多）。Headers 里装着供应商的
			// Authorization 凭据 —— 一个已经投递成功的任务，没有任何理由把
			// 别人的 token 在磁盘上再留 7 天。压缩顺带把它清掉。
			//
			// 保留 URL 与 Method：它们不敏感，而排障时「这条通知发去了哪」
			// 是最常被问到的问题。
			snap.Target = model.Target{URL: t.Target.URL, Method: t.Target.Method}
		}
		// 死信任务必须保留完整 Target —— 否则 POST /{id}/retry 无从重建请求。
		// 代价是死信会在磁盘上持有供应商凭据直到过保留期，已记入 spec §11。
		out = append(out, store.Record{
			T: store.RecEnqueue, ID: t.ID, TS: t.CreatedAt, Task: &snap,
		})
		switch {
		case t.State == model.StateSucceeded:
			out = append(out, store.Record{
				T: store.RecDone, ID: t.ID, TS: t.UpdatedAt, N: t.Attempts, Code: t.LastCode,
			})
		case t.State == model.StateDead:
			out = append(out, store.Record{
				T: store.RecDead, ID: t.ID, TS: t.UpdatedAt, N: t.Attempts, Why: t.DeadReason,
			})
		case t.Attempts > 0 || t.NextAt != t.CreatedAt:
			// 非终态且已有尝试历史（或被改过排程）：用一条 att 记录承载运行时状态。
			// 纯新任务不需要这条 —— 回放 enq 时 NextAt 会被置为 CreatedAt。
			out = append(out, store.Record{
				T: store.RecAttempt, ID: t.ID, TS: t.UpdatedAt, N: t.Attempts,
				Res: store.ResRetry, Code: t.LastCode, Err: t.LastErr, NextAt: t.NextAt,
			})
		}
	}
	return out
}
