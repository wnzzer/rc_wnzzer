package acceptance

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/testutil"
)

// TestExampleClientScript 验证 scripts/notify-submit.sh 真的能跑通。
//
// 示例代码不被测试就一定会腐烂 —— 而签名脚本又恰好是最容易在 shell 转义上
// 翻车的东西（少一层反斜杠，签名就对不上，报错还是无信息量的 401）。
// 把它纳入验收测试，等于把「文档里的例子能用」这件事也变成可回归的断言。
func TestExampleClientScript(t *testing.T) {
	script, err := filepath.Abs("../../scripts/notify-submit.sh")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(script); err != nil {
		t.Skipf("示例脚本不存在: %v", err)
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("环境中没有 openssl，跳过")
	}

	v := testutil.NewVendor()
	defer v.Close()
	s := newServer(t).start()

	run := func(idem, payload string) []byte {
		t.Helper()
		cmd := exec.Command("bash", script, idem, v.URL(), payload)
		cmd.Env = append(os.Environ(),
			"NOTIFY_ENDPOINT=http://"+s.addr,
			"NOTIFY_KEY_ID="+testKeyID,
			"NOTIFY_SECRET="+testSecret,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("执行示例脚本失败: %v\n输出: %s", err, out)
		}
		return out
	}

	out := run("script-demo-1", `{"contact_id":123,"status":"paid"}`)
	var first submitResp
	if err := json.Unmarshal(out, &first); err != nil {
		t.Fatalf("脚本输出不是合法 JSON: %s", out)
	}
	if first.ID == "" || first.Duplicate {
		t.Fatalf("首次提交响应异常: %s", out)
	}

	// 通知应当被原样送达，payload 字节不被改写（字节忠实管道，决策 D-008）。
	if !waitUntil(10*time.Second, func() bool { return v.CountFor(first.ID) >= 1 }) {
		t.Fatalf("通知未送达供应商。日志:\n%s", s.logs())
	}
	got := v.Requests()[0]
	if got.Body != `{"contact_id":123,"status":"paid"}` {
		t.Errorf("供应商收到的 body = %q，与提交时不一致", got.Body)
	}
	if got.Header.Get("Content-Type") != "application/json" {
		t.Errorf("调用方指定的 Content-Type 未被透传: %q", got.Header.Get("Content-Type"))
	}

	// 同一幂等键重复提交应返回 duplicate。
	out = run("script-demo-1", `{"contact_id":123,"status":"paid"}`)
	var second submitResp
	json.Unmarshal(out, &second)
	if !second.Duplicate || second.ID != first.ID {
		t.Errorf("重复提交未被去重: %s", out)
	}

	// 密钥错误时脚本应拿到 401（验证签名确实生效，而不是碰巧通过）。
	cmd := exec.Command("bash", script, "script-demo-2", v.URL(), "{}")
	cmd.Env = append(os.Environ(),
		"NOTIFY_ENDPOINT=http://"+s.addr,
		"NOTIFY_KEY_ID="+testKeyID,
		"NOTIFY_SECRET=wrong-secret",
	)
	bad, _ := cmd.CombinedOutput()
	if !strings.Contains(string(bad), "unauthorized") {
		t.Errorf("错误密钥应被拒绝，实际输出: %s", bad)
	}
}
