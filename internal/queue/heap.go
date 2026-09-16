package queue

import "github.com/wnzzer/rc_wnzzer/internal/model"

// dueHeap 是按 NextAt 排序的最小堆，只容纳未终结且已排程的任务。
//
// 正在投递（sending）的任务不在堆内 —— 它被 Lease 取出后，由投递结果决定
// 重新入堆（OnRetry）还是进入终态（OnSuccess / OnDead）。这个不变量保证了
// 同一个任务永远不会被两个 worker 同时取到。
type dueHeap []*model.Task

func (h dueHeap) Len() int           { return len(h) }
func (h dueHeap) Less(i, j int) bool { return h[i].NextAt < h[j].NextAt }
func (h dueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dueHeap) Push(x any)        { *h = append(*h, x.(*model.Task)) }
func (h *dueHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil // 去掉底层数组对 Task 的引用，避免阻止 GC
	*h = old[:n-1]
	return t
}
