// Package store 定义 notifyd 的持久化边界。
//
// 这里定义 Store 接口是本项目唯一一处「为未来留的口子」（决策 D-022）。
// 允许它存在的条件有两个，缺一不可：
//  1. 几乎零成本 —— 存储与调度本来就要分层，定义接口不增加额外代码；
//  2. 不留的代价很高 —— 否则文件 IO 会渗透进调度逻辑，日后换存储等于重写。
//
// 它是一个有界抽象（一个接口、7 个方法），不是插件框架：没有实现注册表、
// 没有配置驱动的动态分发（那是决策 D-023 明确否决的）。
package store

import "github.com/wnzzer/rc_wnzzer/internal/model"

// 记录类型。四种，对应 spec §7.1。
const (
	RecEnqueue = "enq"
	RecAttempt = "att"
	RecDone    = "ok"
	RecDead    = "dead"
)

// 投递结果分类，写入 RecAttempt 的 Res 字段。
const (
	ResRetry     = "retry"     // 可重试失败，已安排下次投递
	ResPermanent = "permanent" // 永久失败（调用方会紧接着写一条 RecDead）
)

// Record 是 journal 中的一行。四种记录共用这一个结构，omitempty 让每种记录
// 只落盘自己用得到的字段 —— 换取的是「一个类型、一套编解码」的简单性。
type Record struct {
	T    string      `json:"t"`
	ID   string      `json:"id"`
	TS   int64       `json:"ts"`
	Task *model.Task `json:"task,omitempty"` // 仅 enq

	N      int    `json:"n,omitempty"`    // 第几次尝试
	Res    string `json:"res,omitempty"`  // 仅 att
	Code   int    `json:"code,omitempty"` // HTTP 状态码，0 表示未拿到响应
	Err    string `json:"err,omitempty"`  // 错误摘要（已截断）
	NextAt int64  `json:"next,omitempty"` // 仅 att：下次投递时刻
	Why    string `json:"why,omitempty"`  // 仅 dead：放弃原因
}

// Terminal 报告该记录是否让任务进入终态。用于统计压缩收益。
func (r *Record) Terminal() bool { return r.T == RecDone || r.T == RecDead }

// Stats 是压缩决策所需的最小信息。
type Stats struct {
	Records  int64 // 总记录数
	Terminal int64 // 其中终态记录数
	Bytes    int64 // 文件大小
}

// Store 是持久化层。实现必须保证：AppendEnqueue 返回时数据已 fsync 落盘，
// 其余 Append* 允许延迟落盘（决策 D-005 的非对称 fsync 策略）。
//
// 所有方法都是并发安全的。
type Store interface {
	// AppendEnqueue 落盘一个新任务，**返回前必须完成 fsync**。
	// 这是核心承诺 C1 的唯一交付点：它返回 nil，就意味着 kill -9 也丢不掉。
	AppendEnqueue(t *model.Task) error

	// AppendAttempt 记录一次失败的投递尝试及下次投递时刻。不保证立即落盘。
	AppendAttempt(id string, n int, res string, code int, errMsg string, nextAt int64) error

	// AppendDone 记录投递成功。不保证立即落盘。
	AppendDone(id string, n, code int) error

	// AppendDead 记录放弃投递。不保证立即落盘。
	AppendDead(id string, n int, why string) error

	// Replay 按写入顺序回放全部记录。遇到尾部截断会自行修复并继续（见 WAL.Replay）。
	// 只应在启动时调用一次。
	Replay(fn func(*Record) error) error

	// Stats 返回当前统计，供上层判断是否触发压缩。
	Stats() Stats

	// Compact 用 snapshot 原子替换整个 journal。
	// snapshot 应包含所有存活任务与保留期内的终态墓碑。
	Compact(snapshot []Record) error

	// Close 刷盘并关闭。
	Close() error
}
