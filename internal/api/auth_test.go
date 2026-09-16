package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	kid    = "caller-1"
	secret = "unit-test-secret" // redact-ok: 单测固定值，不对应任何真实密钥
)

func signedReq(method, path string, ts int64, body []byte, sec string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set(HeaderKeyID, kid)
	r.Header.Set(HeaderTimestamp, fmt.Sprint(ts))
	r.Header.Set(HeaderSignature, Sign(sec, method, r.URL.Path, ts, body))
	return r
}

func TestVerify(t *testing.T) {
	a := NewAuthenticator(map[string]string{kid: secret}, 5*time.Minute)
	body := []byte(`{"idempotency_key":"k1"}`)
	now := time.Now().Unix()

	t.Run("正确签名通过", func(t *testing.T) {
		got, err := a.Verify(signedReq(http.MethodPost, "/v1/notifications", now, body, secret), body)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if got != kid {
			t.Errorf("key_id = %q, 期望 %q", got, kid)
		}
	})

	t.Run("密钥错误被拒", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, "wrong-secret")
		if _, err := a.Verify(r, body); err == nil {
			t.Error("错误密钥应被拒绝")
		}
	})

	t.Run("未知 key_id 被拒", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, secret)
		r.Header.Set(HeaderKeyID, "stranger")
		if _, err := a.Verify(r, body); err == nil {
			t.Error("未知 key_id 应被拒绝")
		}
	})

	// 签名覆盖 body 哈希 —— 请求体里装着目标 URL，篡改它等于改写投递目的地。
	t.Run("篡改 body 被发现", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, secret)
		if _, err := a.Verify(r, []byte(`{"idempotency_key":"k1","evil":true}`)); err == nil {
			t.Error("body 被篡改后应验签失败")
		}
	})

	// 签名覆盖路径与方法，防止把一个 GET 的签名挪用到 POST 上。
	t.Run("路径被替换被发现", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, secret)
		r.URL.Path = "/v1/notifications/x/retry"
		if _, err := a.Verify(r, body); err == nil {
			t.Error("路径被替换后应验签失败")
		}
	})

	t.Run("方法被替换被发现", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, secret)
		r.Method = http.MethodDelete
		if _, err := a.Verify(r, body); err == nil {
			t.Error("方法被替换后应验签失败")
		}
	})

	t.Run("时间戳超窗被拒", func(t *testing.T) {
		for _, skew := range []time.Duration{10 * time.Minute, -10 * time.Minute} {
			ts := time.Now().Add(skew).Unix()
			r := signedReq(http.MethodPost, "/v1/notifications", ts, body, secret)
			if _, err := a.Verify(r, body); err == nil {
				t.Errorf("偏移 %v 的时间戳应被拒绝", skew)
			}
		}
	})

	t.Run("时间戳非法被拒", func(t *testing.T) {
		r := signedReq(http.MethodPost, "/v1/notifications", now, body, secret)
		r.Header.Set(HeaderTimestamp, "not-a-number")
		if _, err := a.Verify(r, body); err == nil {
			t.Error("非法时间戳应被拒绝")
		}
	})
}

// TestCanonicalString_Stable 把签名串的格式钉住。
// 它是客户端与服务端之间的契约：格式一变，所有调用方的签名同时失效，
// 因此任何改动都必须走 sigVersion 递增，而不是悄悄改。
func TestCanonicalString_Stable(t *testing.T) {
	got := CanonicalString("post", "/v1/notifications", 1758000000, []byte("hello"))
	want := "v1\nPOST\n/v1/notifications\n1758000000\n" +
		"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got != want {
		t.Errorf("签名串格式变了:\n实际:\n%q\n期望:\n%q", got, want)
	}
}
