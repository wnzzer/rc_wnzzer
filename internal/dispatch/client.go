package dispatch

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

// ErrBlockedTarget 表示目标地址落在禁止访问的网段内。
// 它被 Classify 判为永久失败：这不是对端故障，是这个请求本身就不该被发出。
var ErrBlockedTarget = errors.New("目标地址被禁止访问")

// ErrBadRequestSpec 表示任务描述本身不可投递（body 无法解码、method 非法等）。
// 同样是永久失败，但原因与 ErrBlockedTarget 不同 —— 分开两个哨兵，死信原因
// 才能如实反映问题出在哪，而不是让排障的人去追一个根本不存在的网段拦截。
var ErrBadRequestSpec = errors.New("任务描述不可投递")

// NewHTTPClient 构造投递用的 HTTP 客户端。
//
// 两处偏离默认行为，都是安全决策：
//
//  1. 不跟随重定向（决策 D-010）。用 ErrUseLastResponse 让 3xx 原样返回，
//     交给 Classify 判为永久失败 —— 而不是返回错误，那样会丢掉状态码信息。
//  2. Dial 层做 SSRF 校验（决策 D-015）。见 blockPrivate 的注释。
func NewHTTPClient(allowPrivate bool, dialTimeout time.Duration) *http.Client {
	d := &net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		d.Control = blockPrivate
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           d.DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// blockPrivate 在 TCP 连接建立**之前**检查即将连接的真实 IP。
//
// 为什么钩在这里而不是在提交时校验 URL：提交时做 DNS 解析再放行，存在
// DNS rebinding 的 TOCTOU 窗口 —— 第一次解析返回公网 IP 通过校验，实际连接时
// 解析到 169.254.169.254（云 metadata 服务）。net.Dialer.Control 拿到的是
// **最终要连接的地址**，没有这个窗口。
//
// 调用方是可信的内部系统，但一个业务侧的注入 bug 就足以把 notifyd 变成内网
// 探针 —— 典型的 confused deputy。这层防护的成本约 20 行，不做没有道理。
func blockPrivate(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: 地址无法解析: %s", ErrBlockedTarget, address)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("%w: 非法 IP: %s", ErrBlockedTarget, host)
	}
	if reason := blockedReason(ip); reason != "" {
		return fmt.Errorf("%w: %s (%s)", ErrBlockedTarget, ip, reason)
	}
	return nil
}

// blockedReason 返回该 IP 被拒绝的原因；返回空串表示放行。
func blockedReason(ip net.IP) string {
	// IPv4-mapped IPv6（::ffff:127.0.0.1）必须先归一化，否则会绕过 v4 判断。
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	switch {
	case ip.IsLoopback():
		return "回环"
	case ip.IsUnspecified():
		return "通配地址"
	case ip.IsPrivate(): // 10/8, 172.16/12, 192.168/16, fc00::/7
		return "私网"
	case ip.IsLinkLocalUnicast(): // 169.254/16, fe80::/10 —— 含云 metadata 服务
		return "链路本地"
	case ip.IsLinkLocalMulticast(), ip.IsInterfaceLocalMulticast(), ip.IsMulticast():
		return "组播"
	case isCGNAT(ip):
		return "运营商级 NAT"
	}
	return ""
}

// cgnat 是 RFC 6598 的 100.64.0.0/10。net.IP 没有内置判断，但它在云环境里
// 常被用作内部网段，放行等于留一个洞。
var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

func isCGNAT(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && cgnat.Contains(v4)
}

// ValidateTargetURL 在提交时做一次廉价校验。
//
// 它不是权威判定 —— 权威判定在 Dial 层（blockPrivate）。这里的价值是让大多数
// 明显错误的目标在 POST 时就拿到 400，而不是等到首次投递才失败。
//
// allowPrivate 必须与 Dial 层使用同一个值。同一条策略在两处实现时，最容易
// 出的错就是其中一处漏掉了开关，让预检比权威判定更严格 —— 那样开关就等于
// 没有，而症状（提交被拒）还会指向完全错误的方向。
func ValidateTargetURL(raw string, allowPrivate bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("url 无法解析: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("仅支持 http/https，收到 %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("url 缺少 host")
	}
	if allowPrivate {
		return nil
	}
	// 字面量 IP 可以立刻判定；域名留给 Dial 层，因为此刻解析的结果不可信。
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		if reason := blockedReason(ip); reason != "" {
			return fmt.Errorf("%w: %s (%s)", ErrBlockedTarget, ip, reason)
		}
	}
	return nil
}
