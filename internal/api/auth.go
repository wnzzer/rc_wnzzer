package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 签名相关的请求头。
const (
	HeaderKeyID     = "X-Notify-Key-Id"
	HeaderTimestamp = "X-Notify-Timestamp"
	HeaderSignature = "X-Notify-Signature"
)

// sigVersion 让签名格式本身可演进：换算法或换签名串结构时递增它，
// 服务端可以在一段时间内同时接受新旧两版。
const sigVersion = "v1"

var errUnauthorized = errors.New("鉴权失败")

// Authenticator 校验请求的对称密钥签名。
type Authenticator struct {
	keys map[string]string // key_id -> secret
	skew time.Duration
}

func NewAuthenticator(keys map[string]string, skew time.Duration) *Authenticator {
	return &Authenticator{keys: keys, skew: skew}
}

// CanonicalString 构造待签名串。
//
// 签名覆盖 body 的哈希，而不只是时间戳 —— 这一点不是可选项：本服务的请求体里
// 装着**目标 URL**。若只签时间戳，任何能改写请求体的中间人都能把一条
// 「通知 CRM」的任务改写成「把用户数据 POST 到攻击者的服务器」。
// 把完整性和鉴权绑在一起，才谈得上安全。
func CanonicalString(method, path string, ts int64, body []byte) string {
	sum := sha256.Sum256(body)
	return strings.Join([]string{
		sigVersion,
		strings.ToUpper(method),
		path,
		strconv.FormatInt(ts, 10),
		hex.EncodeToString(sum[:]),
	}, "\n")
}

// Sign 计算签名。导出供调用方 SDK 与测试使用 —— 签名算法只有一份实现，
// 不会出现「客户端和服务端各写一遍、细节对不上」这种经典问题。
func Sign(secret, method, path string, ts int64, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(CanonicalString(method, path, ts, body)))
	return hex.EncodeToString(m.Sum(nil))
}

// Verify 校验请求签名，返回调用方的 key_id。
//
// 三步：key_id 存在 → 时间戳在容忍窗口内 → 常数时间比对签名。
//
// 注意这里**没有 nonce 缓存**（决策 D-004）：idempotency_key 强制必填后，
// 重放一个完整请求在应用层天然是 no-op（命中幂等索引，返回已存在的任务）。
// 时间戳窗口负责限制重放窗口，幂等键负责让重放无害，两者已经闭合；
// 再维护一张带淘汰策略的 nonce 表是用新的有状态组件解决已被解决的问题。
func (a *Authenticator) Verify(r *http.Request, body []byte) (string, error) {
	keyID := r.Header.Get(HeaderKeyID)
	secret, ok := a.keys[keyID]
	if !ok {
		return "", fmt.Errorf("%w: 未知的 key_id", errUnauthorized)
	}
	tsStr := r.Header.Get(HeaderTimestamp)
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("%w: 时间戳格式非法", errUnauthorized)
	}
	if d := time.Since(time.Unix(ts, 0)); d > a.skew || d < -a.skew {
		return "", fmt.Errorf("%w: 时间戳超出容忍窗口", errUnauthorized)
	}
	want := Sign(secret, r.Method, r.URL.Path, ts, body)
	got := r.Header.Get(HeaderSignature)
	// 常数时间比对：避免通过响应时间逐字节爆破签名。
	if !hmac.Equal([]byte(want), []byte(got)) {
		return "", fmt.Errorf("%w: 签名不匹配", errUnauthorized)
	}
	return keyID, nil
}
