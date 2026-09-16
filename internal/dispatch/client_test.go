package dispatch

import (
	"errors"
	"net"
	"testing"
)

func TestBlockedReason(t *testing.T) {
	// 下面这些字面量就是本测试的被测对象：SSRF 规则必须用真实的私网/保留段
	// 才能验证。逐行标注豁免理由，而不是整个文件关掉检查。
	blocked := []struct{ ip, why string }{
		{"127.0.0.1", "回环"},
		{"127.10.20.30", "回环"},
		{"::1", "回环"},
		{"0.0.0.0", "通配地址"},
		{"10.0.0.5", "私网"},       // redact-ok: SSRF 规则的被测输入
		{"172.16.0.1", "私网"},     // redact-ok: SSRF 规则的被测输入
		{"172.31.255.254", "私网"}, // redact-ok: SSRF 规则的被测输入
		{"192.168.1.1", "私网"},    // redact-ok: SSRF 规则的被测输入
		{"fd00::1", "私网"},
		{"169.254.169.254", "链路本地"}, // 云 metadata 服务，最重要的一个
		{"fe80::1", "链路本地"},
		{"100.64.0.1", "运营商级 NAT"},
		{"::ffff:127.0.0.1", "回环"}, // IPv4-mapped，未归一化就会漏掉
		{"::ffff:10.0.0.1", "私网"},  // redact-ok: SSRF 规则的被测输入
		{"224.0.0.1", "组播"},
	}
	for _, c := range blocked {
		ip := net.ParseIP(c.ip)
		if ip == nil {
			t.Fatalf("测试用例里的 IP 无法解析: %s", c.ip)
		}
		if got := blockedReason(ip); got != c.why {
			t.Errorf("blockedReason(%s) = %q, 期望 %q", c.ip, got, c.why)
		}
	}

	allowed := []string{"1.1.1.1", "8.8.8.8", "203.0.113.10", "2001:4860:4860::8888", "172.32.0.1", "100.128.0.1"}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if got := blockedReason(ip); got != "" {
			t.Errorf("blockedReason(%s) = %q, 期望放行", s, got)
		}
	}
}

func TestValidateTargetURL(t *testing.T) {
	cases := []struct {
		name, url string
		allow     bool
		wantErr   bool
		blocked   bool
	}{
		{"正常 https", "https://vendor.example.com/hook", false, false, false},
		{"正常 http", "http://vendor.example.com/hook", false, false, false},
		{"不支持的协议", "ftp://vendor.example.com/f", false, true, false},
		{"file 协议", "file:///etc/passwd", false, true, false},
		{"缺少 host", "https://", false, true, false},
		{"字面量回环被拒", "http://127.0.0.1:8080/x", false, true, true},
		{"云 metadata 被拒", "http://169.254.169.254/x", false, true, true},
		// 放行开关必须同时对预检与 Dial 层生效，否则开关等于没有。
		{"放行开关下回环可通过", "http://127.0.0.1:8080/x", true, false, false},
		// 域名在提交时不解析：此刻的解析结果不可信，权威判定在 Dial 层。
		{"域名不在提交时解析", "http://localhost.example.com/x", false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateTargetURL(c.url, c.allow)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, 期望出错 = %v", err, c.wantErr)
			}
			if c.blocked && !errors.Is(err, ErrBlockedTarget) {
				t.Errorf("err = %v, 期望包裹 ErrBlockedTarget", err)
			}
		})
	}
}
