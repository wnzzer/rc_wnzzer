package store

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/wnzzer/rc_wnzzer/internal/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openTemp(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := OpenWAL(dir, testLogger())
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	return w
}

func replayAll(t *testing.T, w *WAL) []Record {
	t.Helper()
	var got []Record
	if err := w.Replay(func(r *Record) error {
		got = append(got, *r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

func mkTask(id, idem string) *model.Task {
	return &model.Task{
		ID:        id,
		IdemKey:   idem,
		Target:    model.Target{URL: "https://vendor.example.com/hook", Method: "POST", Body: `{"a":1}`},
		CreatedAt: 1700000000000,
		KeyID:     "test-caller",
	}
}

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w := openTemp(t, dir)
	if err := w.AppendEnqueue(mkTask("T1", "k1")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAttempt("T1", 1, ResRetry, 503, "boom", 1700000001000); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendDone("T1", 2, 200); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := openTemp(t, dir)
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 3 {
		t.Fatalf("回放记录数 = %d, 期望 3: %+v", len(got), got)
	}
	if got[0].T != RecEnqueue || got[0].Task == nil || got[0].Task.IdemKey != "k1" {
		t.Errorf("第一条应为完整 enq 记录, got %+v", got[0])
	}
	if got[1].T != RecAttempt || got[1].Code != 503 || got[1].NextAt != 1700000001000 {
		t.Errorf("第二条 att 记录字段不符, got %+v", got[1])
	}
	if got[2].T != RecDone || got[2].Code != 200 {
		t.Errorf("第三条应为 ok 记录, got %+v", got[2])
	}
	if st := w2.Stats(); st.Records != 3 || st.Terminal != 1 {
		t.Errorf("Stats = %+v, 期望 Records=3 Terminal=1", st)
	}
}

// TestWAL_EnqueueSurvivesCrash 是核心承诺 C1 的单元级验证。
//
// 模拟崩溃的方式：不调用 Close 就丢弃 WAL 对象。bufio 缓冲区在用户态，
// 丢弃对象即等价于进程被 kill -9 —— 未 flush 的数据真实丢失。
// 这样就能验证非对称 fsync 策略（决策 D-005）的两侧：
//   - enq 记录因同步 fsync 而幸存；
//   - 其后未刷盘的 att 记录丢失，且丢失不影响可恢复性。
func TestWAL_EnqueueSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	w := openTemp(t, dir)
	if err := w.AppendEnqueue(mkTask("T1", "k1")); err != nil {
		t.Fatal(err)
	}
	// 这条不 fsync，崩溃后应当丢失（且按 at-least-once 语义这是可接受的）。
	if err := w.AppendAttempt("T1", 1, ResRetry, 503, "boom", 1700000001000); err != nil {
		t.Fatal(err)
	}
	// 故意不 Close：模拟 kill -9。

	w2 := openTemp(t, dir)
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 1 {
		t.Fatalf("崩溃后回放记录数 = %d, 期望 1（只剩已 fsync 的 enq）: %+v", len(got), got)
	}
	if got[0].T != RecEnqueue || got[0].ID != "T1" {
		t.Fatalf("幸存的记录应为 enq/T1, got %+v", got[0])
	}
}

// TestWAL_RepairsTruncatedTail 覆盖 spec §7.3：崩溃导致的尾部半截写入。
func TestWAL_RepairsTruncatedTail(t *testing.T) {
	cases := []struct {
		name string
		junk string
	}{
		{"半截 JSON 且无换行", `{"t":"att","id":"T2","ts":17000`},
		{"完整一行但 JSON 非法", "{not json at all}\n"},
		{"全零字节", "\x00\x00\x00\x00\x00\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w := openTemp(t, dir)
			if err := w.AppendEnqueue(mkTask("T1", "k1")); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// 手工追加垃圾，模拟崩溃时的半截写入。
			path := filepath.Join(dir, journalName)
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(tc.junk); err != nil {
				t.Fatal(err)
			}
			f.Close()

			w2 := openTemp(t, dir)
			got := replayAll(t, w2)
			if len(got) != 1 || got[0].ID != "T1" {
				t.Fatalf("回放应只得到完好的 T1, got %+v", got)
			}

			// 修复必须是持久的：文件已被截断，且后续写入能正常接上。
			if err := w2.AppendDone("T1", 1, 200); err != nil {
				t.Fatal(err)
			}
			if err := w2.Close(); err != nil {
				t.Fatal(err)
			}
			w3 := openTemp(t, dir)
			defer w3.Close()
			got2 := replayAll(t, w3)
			if len(got2) != 2 || got2[1].T != RecDone {
				t.Fatalf("截断修复后应能正常续写, got %+v", got2)
			}
		})
	}
}

func TestWAL_Compact(t *testing.T) {
	dir := t.TempDir()
	w := openTemp(t, dir)
	// 3 个任务，其中 2 个已终结。
	for _, id := range []string{"T1", "T2", "T3"} {
		if err := w.AppendEnqueue(mkTask(id, "k-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.AppendDone("T1", 1, 200); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendDead("T2", 24, "max_attempts"); err != nil {
		t.Fatal(err)
	}
	before := w.Stats()
	if before.Records != 5 || before.Terminal != 2 {
		t.Fatalf("压缩前 Stats = %+v", before)
	}

	// 快照：只保留存活的 T3。
	snap := []Record{{T: RecEnqueue, ID: "T3", TS: 1, Task: mkTask("T3", "k-T3")}}
	if err := w.Compact(snap); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := w.Stats()
	if after.Records != 1 || after.Terminal != 0 {
		t.Fatalf("压缩后 Stats = %+v, 期望 Records=1", after)
	}
	if after.Bytes >= before.Bytes {
		t.Errorf("压缩后文件应变小: before=%d after=%d", before.Bytes, after.Bytes)
	}

	// 压缩后必须能继续写，且重启后状态一致。
	if err := w.AppendAttempt("T3", 1, ResRetry, 500, "x", 123); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// 临时文件不应残留。
	if _, err := os.Stat(filepath.Join(dir, tempName)); !os.IsNotExist(err) {
		t.Errorf("压缩后不应残留 %s", tempName)
	}

	w2 := openTemp(t, dir)
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 2 || got[0].ID != "T3" || got[1].T != RecAttempt {
		t.Fatalf("压缩后重启回放不符: %+v", got)
	}
}

// TestWAL_CompactIsAtomic 验证 rename 替换的原子性前提：
// 压缩失败（快照序列化不了）时，原 journal 必须完好无损。
func TestWAL_CompactIsAtomic(t *testing.T) {
	dir := t.TempDir()
	w := openTemp(t, dir)
	defer w.Close()
	if err := w.AppendEnqueue(mkTask("T1", "k1")); err != nil {
		t.Fatal(err)
	}

	// 制造一次注定失败的压缩：把临时文件路径预先占为一个目录，让 OpenFile 失败。
	// （Task 里全是可序列化类型，无法从 Marshal 侧构造失败。）
	snap := []Record{{T: RecEnqueue, ID: "T1", Task: mkTask("T1", "k1")}}
	if err := os.Mkdir(filepath.Join(dir, tempName), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := w.Compact(snap); err == nil {
		t.Fatal("期望压缩失败")
	}
	os.Remove(filepath.Join(dir, tempName))

	// 原文件必须仍然可读、内容完整。
	got := replayAll(t, w)
	if len(got) != 1 || got[0].ID != "T1" {
		t.Fatalf("压缩失败后原 journal 应完好, got %+v", got)
	}
}

func TestWAL_ErrorFieldIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w := openTemp(t, dir)
	defer w.Close()
	long := make([]byte, 4096)
	for i := range long {
		long[i] = 'x'
	}
	if err := w.AppendAttempt("T1", 1, ResRetry, 0, string(long), 0); err != nil {
		t.Fatal(err)
	}
	got := replayAll(t, w)
	if len(got) != 1 {
		t.Fatalf("got %d records", len(got))
	}
	if len([]rune(got[0].Err)) > errFieldMax+1 {
		t.Errorf("错误字段未被截断, 长度 = %d", len(got[0].Err))
	}
}
