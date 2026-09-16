// Package acceptance 实现 spec §13 的验收标准 A1-A10。
//
// 这些测试启动**真实的 notifyd 进程**并对它发起真实的 HTTP 请求，
// 因为要验证的正是「进程被 kill -9 之后会怎样」这类单元测试够不到的性质。
//
// 选择自研 WAL（决策 D-002）时接受了「正确性需自证」这个代价，这个包就是还债的地方。
package acceptance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/api"
)

const (
	testKeyID  = "acceptance"
	testSecret = "s3cr3t-for-tests-only"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "notifyd-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "创建临时目录失败:", err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "notifyd")
	args := []string{"build"}
	if raceEnabled {
		args = append(args, "-race")
	}
	args = append(args, "-o", binPath, "github.com/wnzzer/rc_wnzzer/cmd/notifyd")
	build := exec.Command("go", args...)
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "构建 notifyd 失败:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// server 是一个受测的 notifyd 进程实例。
type server struct {
	t       *testing.T
	cmd     *exec.Cmd
	addr    string
	dataDir string
	keyFile string
	env     map[string]string
	logBuf  *syncBuffer
}

// syncBuffer 让 exec 的拷贝协程与测试主协程可以安全地共用一个缓冲区。
// 裸 bytes.Buffer 在这里是数据竞争 —— os/exec 会起独立协程往 Stdout 写。
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// newServer 准备一个实例但不启动，便于测试先调整环境变量。
func newServer(t *testing.T) *server {
	t.Helper()
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys")
	if err := os.WriteFile(keyFile, []byte(testKeyID+":"+testSecret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &server{
		t:       t,
		addr:    "127.0.0.1:" + freePort(t),
		dataDir: filepath.Join(dir, "data"),
		keyFile: keyFile,
		env: map[string]string{
			// 测试里的 mock 供应商跑在 127.0.0.1 上，必须放行私网地址。
			// 这也顺带说明了这个开关为什么存在（spec §10）。
			"NOTIFY_ALLOW_PRIVATE_HOSTS": "true",
			"NOTIFY_BACKOFF_BASE":        "200ms",
			"NOTIFY_BACKOFF_CAP":         "2s",
			"NOTIFY_TIMEOUT":             "2s",
			"NOTIFY_MAINTAIN_INTERVAL":   "500ms",
		},
	}
}

func (s *server) set(k, v string) *server { s.env[k] = v; return s }

func (s *server) start() *server {
	s.t.Helper()
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"NOTIFY_ADDR="+s.addr,
		"NOTIFY_DATA_DIR="+s.dataDir,
		"NOTIFY_KEYS_FILE="+s.keyFile,
	)
	for k, v := range s.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	s.logBuf = &syncBuffer{}
	cmd.Stdout = s.logBuf
	cmd.Stderr = s.logBuf
	// 独立进程组：kill -9 时能确保子进程一起走，不留孤儿。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("启动 notifyd: %v", err)
	}
	s.cmd = cmd
	s.waitReady()
	s.t.Cleanup(s.stop)
	return s
}

func (s *server) waitReady() {
	s.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + s.addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("notifyd 未能就绪。日志:\n%s", s.logBuf.String())
}

// kill9 模拟机器掉电 / OOM kill：不给任何清理机会。
func (s *server) kill9() {
	s.t.Helper()
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	if err := s.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		s.t.Fatalf("SIGKILL: %v", err)
	}
	s.cmd.Wait()
	s.cmd = nil
	// 等端口真正释放，避免重启时 bind 失败。
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", s.addr, 50*time.Millisecond)
		if err != nil {
			return
		}
		c.Close()
		time.Sleep(20 * time.Millisecond)
	}
}

// stop 发送 SIGTERM 走有序退出路径。
func (s *server) stop() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	s.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.cmd.Process.Signal(syscall.SIGKILL)
		s.cmd.Wait()
	}
	s.cmd = nil
}

func (s *server) logs() string { return s.logBuf.String() }

// --- 签名 HTTP 客户端 ---

// do 发起一个已签名的请求。签名用的是生产代码里的 api.Sign —— 客户端与服务端
// 共用同一份实现，避免「两边各写一遍、细节对不上」这种经典问题。
func (s *server) do(method, path string, body any) (int, []byte) {
	s.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			s.t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, "http://"+s.addr+path, bytes.NewReader(raw))
	if err != nil {
		s.t.Fatal(err)
	}
	ts := time.Now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(api.HeaderKeyID, testKeyID)
	req.Header.Set(api.HeaderTimestamp, fmt.Sprint(ts))
	req.Header.Set(api.HeaderSignature, api.Sign(testSecret, method, urlPath(path), ts, raw))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// urlPath 去掉 query string —— 签名只覆盖路径部分。
func urlPath(p string) string {
	if i := strings.IndexByte(p, '?'); i >= 0 {
		return p[:i]
	}
	return p
}

type submitResp struct {
	ID        string `json:"id"`
	State     string `json:"state"`
	Duplicate bool   `json:"duplicate"`
}

type taskResp struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Attempts   int    `json:"attempts"`
	NextAt     int64  `json:"next_attempt_at"`
	LastCode   int    `json:"last_status_code"`
	DeadReason string `json:"dead_reason"`
}

// submit 提交一个通知，断言状态码符合预期并返回响应。
func (s *server) submit(idem, url string, wantStatus int, extra map[string]any) submitResp {
	s.t.Helper()
	body := map[string]any{
		"idempotency_key": idem,
		"target": map[string]any{
			"url":     url,
			"method":  "POST",
			"headers": map[string]string{"Content-Type": "application/json"},
			"body":    `{"event":"test"}`,
		},
	}
	for k, v := range extra {
		body[k] = v
	}
	code, raw := s.do(http.MethodPost, "/v1/notifications", body)
	if code != wantStatus {
		s.t.Fatalf("提交 %s: 状态码 = %d, 期望 %d, 响应 = %s", idem, code, wantStatus, raw)
	}
	var out submitResp
	json.Unmarshal(raw, &out)
	return out
}

func (s *server) task(id string) taskResp {
	s.t.Helper()
	code, raw := s.do(http.MethodGet, "/v1/notifications/"+id, nil)
	if code != http.StatusOK {
		s.t.Fatalf("查询任务 %s: 状态码 = %d, 响应 = %s", id, code, raw)
	}
	var out taskResp
	json.Unmarshal(raw, &out)
	return out
}

// taskOpt 查询任务，不存在时返回 false 而不是让测试失败。
func (s *server) taskOpt(id string) (taskResp, bool) {
	s.t.Helper()
	code, raw := s.do(http.MethodGet, "/v1/notifications/"+id, nil)
	if code != http.StatusOK {
		return taskResp{}, false
	}
	var out taskResp
	json.Unmarshal(raw, &out)
	return out, true
}

// waitUntil 轮询直到 cond 为真或超时。返回是否成功。
func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}
