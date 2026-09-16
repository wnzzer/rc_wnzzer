// notifyd 是一个把「调用外部 HTTP API」从业务主链路上摘下来的中继服务。
//
// 业务系统提交一个已构造完整的 HTTP 请求描述，notifyd 持久化后立即返回；
// 此后由 notifyd 负责反复投递，直到成功或放弃。
//
// 设计文档见 docs/spec.md，取舍记录见 docs/decisions.md。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/api"
	"github.com/wnzzer/rc_wnzzer/internal/config"
	"github.com/wnzzer/rc_wnzzer/internal/dispatch"
	"github.com/wnzzer/rc_wnzzer/internal/metrics"
	"github.com/wnzzer/rc_wnzzer/internal/queue"
	"github.com/wnzzer/rc_wnzzer/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "notifyd:", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	// 结构化日志写 stdout，交给宿主机的采集。不引入日志库（决策 D-001）。
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	keys, err := config.LoadKeys(cfg.KeysFile)
	if err != nil {
		return err
	}
	log.Info("配置就绪", "监听", cfg.Addr, "数据目录", cfg.DataDir,
		"调用方数量", len(keys), "worker", cfg.Workers,
		"单主机并发", cfg.HostConcurrency, "重试上限", cfg.MaxAttempts)

	wal, err := store.OpenWAL(cfg.DataDir, log)
	if err != nil {
		return err
	}
	defer wal.Close()

	q := queue.New(queue.Config{
		QueueMax:         cfg.QueueMax,
		RetentionMS:      cfg.Retention.Milliseconds(),
		CompactMinBytes:  cfg.CompactMinBytes,
		CompactLiveRatio: cfg.CompactLiveRatio,
	}, wal, log)

	// 启动即回放。这一步决定了「崩溃前收下的任务」能否回到队列 —— 承诺 C1 的下半段。
	if err := q.Restore(); err != nil {
		return fmt.Errorf("回放 WAL 失败: %w", err)
	}

	mx := metrics.New()
	d := dispatch.New(dispatch.Config{
		Workers:            cfg.Workers,
		HostConcurrency:    cfg.HostConcurrency,
		BaseBackoff:        cfg.BaseBackoff,
		MaxBackoff:         cfg.BackoffCap,
		DialTimeout:        cfg.DialTimeout,
		DefaultTimeout:     cfg.Timeout,
		DefaultMaxAttempts: cfg.MaxAttempts,
		DefaultDeadline:    cfg.Deadline,
		AllowPrivate:       cfg.AllowPrivateHosts,
	}, q, log, mx)

	if cfg.AllowPrivateHosts {
		log.Warn("SSRF 防护已关闭，允许投递到内网地址 —— 仅应用于本地联调")
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.NewServer(q, api.NewAuthenticator(keys, cfg.SkewTolerance), mx, wal, log, cfg.AllowPrivateHosts).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	// SIGTERM/SIGINT 触发有序退出。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); d.Run(ctx) }()

	maintainDone := make(chan struct{})
	go func() {
		defer close(maintainDone)
		t := time.NewTicker(cfg.MaintainInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				q.Maintain()
			case <-ctx.Done():
				return
			}
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		log.Info("开始监听", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	case <-ctx.Done():
	}

	// —— 有序退出（spec §10）——
	log.Info("收到退出信号，开始有序退出", "宽限期", cfg.ShutdownGrace.String())

	// 1. 停止接收新请求；已建立的连接正常处理完。
	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Warn("HTTP 服务关闭超时", "err", err)
	}

	// 2. 调度循环已随 ctx 停止取新任务。
	<-dispatchDone
	<-maintainDone

	// 3. 等待在途投递自然结束。超时未结束的任务重启后按 pending 重放，
	//    可能造成一次重复投递 —— 落在 at-least-once 契约内（spec §5.3）。
	if !d.Wait(cfg.ShutdownGrace) {
		log.Warn("仍有在途投递未结束，将由重启后的回放接管")
	}

	// 4. 最后一次刷盘由 wal.Close 的 defer 完成。
	log.Info("退出完成")
	return nil
}
