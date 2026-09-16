// Package api 提供 notifyd 的 HTTP 接口。
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/wnzzer/rc_wnzzer/internal/dispatch"
	"github.com/wnzzer/rc_wnzzer/internal/metrics"
	"github.com/wnzzer/rc_wnzzer/internal/model"
	"github.com/wnzzer/rc_wnzzer/internal/queue"
	"github.com/wnzzer/rc_wnzzer/internal/store"
)

const (
	// maxBodyBytes 是提交请求的体积上限。通知 payload 本就该是小的结构化数据；
	// 设上限既防御误用，也防止单个请求撑爆 journal 的一行。
	maxBodyBytes     = 1 << 20
	maxIdemKeyLen    = 128
	defaultListLimit = 100
	maxListLimit     = 1000
)

// allowedMethods 限制可投递的 HTTP 方法。
// 不放开 CONNECT/TRACE 之类：它们对「发通知」没有意义，却能被用来做隧道或探测。
var allowedMethods = map[string]bool{
	http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodGet: true, http.MethodDelete: true,
}

// Server 装配 HTTP 接口。
type Server struct {
	q    *queue.Queue
	auth *Authenticator
	mx   *metrics.Registry
	st   store.Store
	log  *slog.Logger
	// allowPrivate 必须与 dispatch 层取同一个配置值，否则提交预检与实际投递
	// 的判定会不一致。
	allowPrivate bool
}

func NewServer(q *queue.Queue, auth *Authenticator, mx *metrics.Registry, st store.Store, log *slog.Logger, allowPrivate bool) *Server {
	return &Server{q: q, auth: auth, mx: mx, st: st, log: log, allowPrivate: allowPrivate}
}

// Handler 返回路由。
//
// 用标准库 ServeMux 的方法+路径模式（Go 1.22+），不引入 router 库：
// 本服务只有 6 个路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/notifications", s.handleSubmit)
	mux.HandleFunc("GET /v1/notifications", s.handleList)
	mux.HandleFunc("GET /v1/notifications/{id}", s.handleGet)
	mux.HandleFunc("POST /v1/notifications/{id}/retry", s.handleRetry)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

// --- 请求 / 响应类型 ---

type submitRequest struct {
	IdempotencyKey string       `json:"idempotency_key"`
	Target         model.Target `json:"target"`
	Policy         model.Policy `json:"policy"`
}

type submitResponse struct {
	ID        string      `json:"id"`
	State     model.State `json:"state"`
	Duplicate bool        `json:"duplicate"`
	CreatedAt int64       `json:"created_at"`
}

type taskView struct {
	ID         string      `json:"id"`
	IdemKey    string      `json:"idempotency_key"`
	State      model.State `json:"state"`
	URL        string      `json:"url"`
	Attempts   int         `json:"attempts"`
	NextAt     int64       `json:"next_attempt_at,omitempty"`
	LastCode   int         `json:"last_status_code,omitempty"`
	LastErr    string      `json:"last_error,omitempty"`
	LastResp   string      `json:"last_response,omitempty"`
	DeadReason string      `json:"dead_reason,omitempty"`
	CreatedAt  int64       `json:"created_at"`
	UpdatedAt  int64       `json:"updated_at"`
	KeyID      string      `json:"submitted_by"`
}

func viewOf(t model.Task) taskView {
	return taskView{
		ID: t.ID, IdemKey: t.IdemKey, State: t.State, URL: t.Target.URL,
		Attempts: t.Attempts, NextAt: t.NextAt, LastCode: t.LastCode,
		LastErr: t.LastErr, LastResp: t.LastResp, DeadReason: t.DeadReason,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt, KeyID: t.KeyID,
	}
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// --- 处理函数 ---

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large", "请求体超过上限")
		return
	}
	keyID, err := s.auth.Verify(r, body)
	if err != nil {
		s.log.Warn("鉴权失败", "err", err, "remote", r.RemoteAddr)
		writeErr(w, http.StatusUnauthorized, "unauthorized", "鉴权失败")
		return
	}

	var req submitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", "请求体不是合法 JSON")
		return
	}
	if code, msg := validate(&req, s.allowPrivate); code != "" {
		s.mx.ObserveSubmit("rejected")
		writeErr(w, http.StatusBadRequest, code, msg)
		return
	}

	now := model.NowMS()
	t := &model.Task{
		ID:        model.NewID(),
		IdemKey:   req.IdempotencyKey,
		Target:    req.Target,
		Policy:    req.Policy,
		CreatedAt: now,
		KeyID:     keyID,
	}
	res, dup, err := s.q.Submit(t)
	switch {
	case errors.Is(err, queue.ErrQueueFull):
		// 早拒绝好过晚崩溃（决策 D-017）：调用方拿到明确的 429 可以自行降级，
		// 而 OOM 会把已收下的任务一起丢掉。
		s.mx.ObserveSubmit("rejected")
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusTooManyRequests, "queue_full", "队列已满，请稍后重试")
		return
	case err != nil:
		s.log.Error("提交任务失败", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "提交失败")
		return
	}

	status := http.StatusAccepted
	if dup {
		status = http.StatusOK
		s.mx.ObserveSubmit("duplicate")
	} else {
		s.mx.ObserveSubmit("accepted")
	}
	// 走到这里意味着 AppendEnqueue 已经 fsync 成功 —— 202 的语义是硬的。
	writeJSON(w, status, submitResponse{
		ID: res.ID, State: res.State, Duplicate: dup, CreatedAt: res.CreatedAt,
	})
}

func validate(req *submitRequest, allowPrivate bool) (code, msg string) {
	switch {
	case req.IdempotencyKey == "":
		// 强制必填。它同时承担两个职责：入口去重，以及让签名重放无害（D-004）。
		return "missing_idempotency_key", "idempotency_key 为必填"
	case len(req.IdempotencyKey) > maxIdemKeyLen:
		return "idempotency_key_too_long", "idempotency_key 超过 128 字节"
	case req.Target.URL == "":
		return "missing_target_url", "target.url 为必填"
	}
	if m := req.Target.Method; m != "" && !allowedMethods[strings.ToUpper(m)] {
		return "method_not_allowed", "不支持的 target.method: " + m
	}
	if err := dispatch.ValidateTargetURL(req.Target.URL, allowPrivate); err != nil {
		if errors.Is(err, dispatch.ErrBlockedTarget) {
			return "blocked_target", err.Error()
		}
		return "invalid_target", err.Error()
	}
	switch req.Target.BodyEncoding {
	case "", "utf8", "base64":
	default:
		return "invalid_body_encoding", "body_encoding 仅支持 utf8 或 base64"
	}
	return "", ""
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !s.authed(w, r) {
		return
	}
	t, ok := s.q.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "任务不存在或已过保留期")
		return
	}
	writeJSON(w, http.StatusOK, viewOf(t))
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if !s.authed(w, r) {
		return
	}
	state := model.State(r.URL.Query().Get("state"))
	if state == "" {
		state = model.StateDead
	}
	limit := defaultListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, maxListLimit)
		}
	}
	tasks := s.q.ListByState(state, limit)
	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, viewOf(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{"state": state, "count": len(views), "tasks": views})
}

// handleRetry 手工复活一个死信任务。
//
// 这是「供应商宕机三天」场景的运维出口。没有它，死信就只是一座坟场。
func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	if !s.authed(w, r) {
		return
	}
	id := r.PathValue("id")
	err := s.q.Revive(id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "任务不存在或已过保留期")
	case errors.Is(err, queue.ErrNotDead):
		writeErr(w, http.StatusConflict, "not_dead", "只能重投处于死信状态的任务")
	case errors.Is(err, queue.ErrQueueFull):
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusTooManyRequests, "queue_full", "队列已满")
	case err != nil:
		s.log.Error("复活任务失败", "task", id, "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "重投失败")
	default:
		t, _ := s.q.Get(id)
		s.log.Info("死信任务已手工重投", "task", id)
		writeJSON(w, http.StatusAccepted, viewOf(t))
	}
}

// handleHealth 是存活探针。不检查任何外部依赖 —— 本服务没有外部依赖，
// 这正是零依赖架构的一个直接好处：健康检查不会因为别人的故障而误报。
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	states, depth, oldest := s.q.Counts()
	out := s.mx.Render(metrics.Snapshot{
		States:        states,
		QueueDepth:    depth,
		JournalBytes:  s.st.Stats().Bytes,
		OldestPending: oldest,
	})
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out)
}

// authed 对只读接口做鉴权。GET 请求没有 body，签名覆盖空 body 的哈希。
func (s *Server) authed(w http.ResponseWriter, r *http.Request) bool {
	if _, err := s.auth.Verify(r, nil); err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "鉴权失败")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResponse{Error: msg, Code: code})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
