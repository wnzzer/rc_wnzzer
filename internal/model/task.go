// Package model 定义 notifyd 的核心数据类型。
//
// 这里刻意把「任务的不可变部分」和「运行时状态」分开：前者在 enq 记录里落盘一次，
// 后者由 att/ok/dead 记录在重放时重建（见 spec §7.1）。
package model

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"time"
)

// State 是任务状态机的状态，取值见 spec §6.5。
type State string

const (
	StatePending   State = "pending"   // 已落盘，等待出队
	StateWaiting   State = "waiting"   // 投递失败，退避中
	StateSending   State = "sending"   // 投递进行中（崩溃后按 pending 重放）
	StateSucceeded State = "succeeded" // 终态
	StateDead      State = "dead"      // 终态
)

// Terminal 报告该状态是否为终态。终态任务只保留到保留期结束（spec §8）。
func (s State) Terminal() bool { return s == StateSucceeded || s == StateDead }

// Target 是调用方要求投递的 HTTP 请求。
//
// Body 是不透明字符串而非 JSON 对象：若由 notifyd 重新序列化，字段顺序与空白会变，
// 而不少供应商对原始 payload 字节做签名校验，重序列化会让签名失效（决策 D-008）。
type Target struct {
	URL          string            `json:"url"`
	Method       string            `json:"method,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Body         string            `json:"body,omitempty"`
	BodyEncoding string            `json:"body_encoding,omitempty"` // utf8（默认）| base64
}

// Policy 是调用方可覆盖的投递策略。零值表示「用服务端默认」。
type Policy struct {
	TimeoutMS   int   `json:"timeout_ms,omitempty"`
	MaxAttempts int   `json:"max_attempts,omitempty"`
	DeadlineMS  int64 `json:"deadline_ms,omitempty"`
}

// Task 是一个投递任务。
//
// 字段分两组：CreatedAt 及以上为不可变部分，落在 enq 记录里；
// State 及以下为运行时状态，重放时由后续记录重建，不重复落盘。
type Task struct {
	ID        string `json:"id"`
	IdemKey   string `json:"idem_key"`
	Target    Target `json:"target"`
	Policy    Policy `json:"policy,omitempty"`
	CreatedAt int64  `json:"created_at"` // Unix 毫秒
	KeyID     string `json:"key_id"`     // 提交方，审计用

	State      State  `json:"-"`
	Attempts   int    `json:"-"`
	NextAt     int64  `json:"-"` // Unix 毫秒，下次投递时刻
	UpdatedAt  int64  `json:"-"`
	LastCode   int    `json:"-"`
	LastErr    string `json:"-"`
	LastResp   string `json:"-"` // 最近一次响应体摘要（成功与失败都留），仅供排障
	DeadReason string `json:"-"`
}

// NowMS 返回当前 Unix 毫秒。集中一处便于测试替换。
var NowMS = func() int64 { return time.Now().UnixMilli() }

// idAlphabet 是 Crockford Base32 的变体：去掉 I/L/O/U，避免手抄时的歧义。
const idAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var idEncoding = base32.NewEncoding(idAlphabet).WithPadding(base32.NoPadding)

// NewID 生成一个按时间单调递增的 26 字符 ID（ULID 布局：48 位毫秒 + 80 位随机）。
//
// 用时间前缀而非纯随机，是为了让 journal 里的 ID 按字典序即时间序 —— 排障时
// grep 出来的结果天然有序。随机部分用 crypto/rand，避免同毫秒内碰撞。
func NewID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(NowMS())<<16)
	// 前 6 字节是时间戳，后 10 字节全随机（覆盖掉 PutUint64 写入的低 2 字节）
	if _, err := rand.Read(b[6:]); err != nil {
		panic("model: crypto/rand 不可用: " + err.Error())
	}
	return idEncoding.EncodeToString(b[:])
}
