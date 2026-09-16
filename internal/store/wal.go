package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wnzzer/rc_wnzzer/internal/model"
)

const (
	journalName = "journal.log"
	tempName    = "journal.log.new"

	// syncInterval 是非关键记录的后台刷盘周期。
	// 它等于「崩溃时可能被重投的成功任务」的时间窗口（spec §11 已知限制 3）。
	syncInterval = 200 * time.Millisecond

	// errFieldMax 是落盘的错误摘要长度上限。错误信息可能很长（比如 TLS 链路报错），
	// 截断它避免单条记录撑爆 journal。
	errFieldMax = 256
)

// WAL 是基于 append-only 文件的 Store 实现。
//
// 并发模型：单写者。所有写入都在 mu 保护下串行发生，因此不需要依赖 O_APPEND
// 的原子性，也不会有交错写入。这个选择让「尾部截断是唯一可能的损坏形态」这个
// 前提成立，从而让 Replay 的修复逻辑足够简单（spec §7.3）。
type WAL struct {
	dir  string
	path string

	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	dirty bool // 有已写入 bufio/页缓存但未 fsync 的数据

	stats Stats

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	log      *slog.Logger
}

var _ Store = (*WAL)(nil)

// OpenWAL 打开（或创建）目录下的 journal，并启动后台刷盘协程。
// 调用方应紧接着调用 Replay 重建内存状态。
func OpenWAL(dir string, log *slog.Logger) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: 创建数据目录: %w", err)
	}
	path := filepath.Join(dir, journalName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: 打开 journal: %w", err)
	}
	w := &WAL{
		dir:  dir,
		path: path,
		f:    f,
		w:    bufio.NewWriterSize(f, 64<<10),
		stop: make(chan struct{}),
		done: make(chan struct{}),
		log:  log,
	}
	go w.syncLoop()
	return w, nil
}

// syncLoop 周期性把非关键记录刷到磁盘。
//
// 它存在的意义不是正确性（丢了这些记录也不违反 at-least-once），而是把
// 「崩溃后需要重做的工作量」限制在一个可预期的窗口内。
func (w *WAL) syncLoop() {
	defer close(w.done)
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			if w.dirty {
				if err := w.flushLocked(); err != nil {
					w.log.Error("后台刷盘失败", "err", err)
				}
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}

// flushLocked 把 bufio 内容写入文件并 fsync。调用方必须持有 mu。
func (w *WAL) flushLocked() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.dirty = false
	return nil
}

// append 序列化并写入一条记录。sync 为 true 时在返回前完成 fsync。
//
// 注意 Flush 与 Sync 的顺序：即使是非 sync 的记录，也必须先进 bufio；
// 而 sync 记录会连带把之前缓冲的记录一起刷下去，因此磁盘上的顺序
// 永远和写入顺序一致 —— 重放的正确性依赖这一点。
func (w *WAL) append(r *Record, sync bool) error {
	line, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("store: 序列化记录: %w", err)
	}
	line = append(line, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.w.Write(line)
	if err != nil {
		return fmt.Errorf("store: 写入 journal: %w", err)
	}
	w.dirty = true
	w.stats.Records++
	w.stats.Bytes += int64(n)
	if r.Terminal() {
		w.stats.Terminal++
	}
	if sync {
		if err := w.flushLocked(); err != nil {
			return fmt.Errorf("store: fsync journal: %w", err)
		}
	}
	return nil
}

// AppendEnqueue 落盘新任务并 fsync。这是唯一一个同步刷盘的写入路径（决策 D-005）。
func (w *WAL) AppendEnqueue(t *model.Task) error {
	return w.append(&Record{T: RecEnqueue, ID: t.ID, TS: model.NowMS(), Task: t}, true)
}

func (w *WAL) AppendAttempt(id string, n int, res string, code int, errMsg string, nextAt int64) error {
	return w.append(&Record{
		T: RecAttempt, ID: id, TS: model.NowMS(),
		N: n, Res: res, Code: code, Err: truncate(errMsg, errFieldMax), NextAt: nextAt,
	}, false)
}

func (w *WAL) AppendDone(id string, n, code int) error {
	return w.append(&Record{T: RecDone, ID: id, TS: model.NowMS(), N: n, Code: code}, false)
}

func (w *WAL) AppendDead(id string, n int, why string) error {
	return w.append(&Record{T: RecDead, ID: id, TS: model.NowMS(), N: n, Why: why}, false)
}

func (w *WAL) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// Replay 从头回放 journal，并在遇到尾部截断时自动修复。
//
// 修复逻辑：逐行解析，记住最后一个完好记录的结束偏移。一旦某行无法解析
// （JSON 语法错误，或读到文件末尾却没有换行符），就判定此处是崩溃时的半截写入，
// 把文件截断到最后一个完好偏移后继续启动。
//
// 这能处理什么：append-only + 单写者模式下崩溃唯一可能造成的损坏 —— 尾部截断。
// 不能处理什么：文件中段的位翻转 / 静默损坏。那是文件系统与硬件的职责，
// 在应用层加一层弱校验属于职责错配（决策 D-007，已列入 spec §11 自曝清单）。
func (w *WAL) Replay(fn func(*Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 正常情况下 Replay 只在启动时调用，缓冲区必然是空的。这里仍先刷盘，
	// 是因为下面的 w.w.Reset 会丢弃缓冲内容 —— 不刷就成了一个静默丢数据的
	// 陷阱。代价是启动时一次 no-op 判断。
	if w.dirty {
		if err := w.flushLocked(); err != nil {
			return fmt.Errorf("store: 回放前刷盘: %w", err)
		}
	}

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("store: 定位到 journal 开头: %w", err)
	}
	br := bufio.NewReaderSize(w.f, 64<<10)

	var good int64 // 最后一个完好记录的结束偏移
	var stats Stats
	truncated := false

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			// 没有换行符结尾 = 读到了文件末尾的半截写入。
			if err == io.EOF {
				w.log.Warn("journal 尾部存在未完成的写入，将截断",
					"offset", good, "残留字节", len(line))
				truncated = true
				break
			}
			var r Record
			if jerr := json.Unmarshal(line[:len(line)-1], &r); jerr != nil {
				w.log.Warn("journal 尾部记录损坏，将截断",
					"offset", good, "err", jerr)
				truncated = true
				break
			}
			if cberr := fn(&r); cberr != nil {
				return fmt.Errorf("store: 回放记录 %s/%s: %w", r.T, r.ID, cberr)
			}
			good += int64(len(line))
			stats.Records++
			if r.Terminal() {
				stats.Terminal++
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("store: 读取 journal: %w", err)
		}
	}

	if truncated {
		if err := w.f.Truncate(good); err != nil {
			return fmt.Errorf("store: 截断 journal 到 %d: %w", good, err)
		}
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("store: 截断后 fsync: %w", err)
		}
	}

	// 把写入位置移到末尾。此后所有 append 都从这里继续。
	if _, err := w.f.Seek(good, io.SeekStart); err != nil {
		return fmt.Errorf("store: 定位到 journal 末尾: %w", err)
	}
	w.w.Reset(w.f)
	stats.Bytes = good
	w.stats = stats
	return nil
}

// Compact 用 snapshot 原子替换整个 journal（spec §7.4）。
//
// 用「写新文件 + rename」而不是原地重写：rename 在同一文件系统内是原子的，
// 任何时刻崩溃剩下的要么是完整的旧文件、要么是完整的新文件，不存在半新半旧的
// 中间态。原地重写做不到这一点。
//
// 已知限制：整个过程持写锁，期间新的 enq 会被阻塞（预期百毫秒级）。改成无阻塞
// 需要双写新旧文件，复杂度翻倍，收益在当前量级下不存在。
func (w *WAL) Compact(snapshot []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmpPath := filepath.Join(w.dir, tempName)
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("store: 创建压缩临时文件: %w", err)
	}
	// 失败路径统一清理，避免残留的 .new 干扰下一次压缩。
	cleanup := func() { tmp.Close(); os.Remove(tmpPath) }

	bw := bufio.NewWriterSize(tmp, 64<<10)
	var next Stats
	for i := range snapshot {
		line, merr := json.Marshal(&snapshot[i])
		if merr != nil {
			cleanup()
			return fmt.Errorf("store: 压缩时序列化记录: %w", merr)
		}
		line = append(line, '\n')
		n, werr := bw.Write(line)
		if werr != nil {
			cleanup()
			return fmt.Errorf("store: 压缩时写入: %w", werr)
		}
		next.Records++
		next.Bytes += int64(n)
		if snapshot[i].Terminal() {
			next.Terminal++
		}
	}
	if err := bw.Flush(); err != nil {
		cleanup()
		return fmt.Errorf("store: 压缩时刷新缓冲: %w", err)
	}
	// 必须在 rename 之前 fsync 新文件内容，否则 rename 后崩溃会得到一个
	// 文件名正确但内容为空的 journal —— 那等于丢光所有存活任务。
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("store: 压缩时 fsync 新文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("store: 关闭压缩临时文件: %w", err)
	}

	// 旧句柄里可能还有缓冲数据，但它即将被整体替换，直接丢弃。
	w.w.Reset(io.Discard)
	if err := w.f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("store: 关闭旧 journal: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("store: 替换 journal: %w", err)
	}
	// fsync 目录，保证 rename 这条元数据变更本身落盘。漏掉这一步的话，
	// 掉电后文件名的指向可能还停留在页缓存里，journal 会「消失」。
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("store: fsync 数据目录: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("store: 压缩后重开 journal: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return fmt.Errorf("store: 压缩后定位末尾: %w", err)
	}
	w.f = f
	w.w = bufio.NewWriterSize(f, 64<<10)
	w.dirty = false
	w.stats = next
	w.log.Info("journal 压缩完成", "记录数", next.Records, "字节", next.Bytes)
	return nil
}

func (w *WAL) Close() error {
	w.stopOnce.Do(func() {
		close(w.stop)
		<-w.done
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// syncDir 对目录本身执行 fsync，让其中的 rename/create 元数据变更落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
