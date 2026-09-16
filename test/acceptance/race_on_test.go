//go:build race

package acceptance

// raceEnabled 让 TestMain 知道要不要给被测二进制也加上 -race。
//
// 这一点容易被忽略：验收测试启动的是**独立进程**，对测试二进制开 -race
// 只能检测测试脚手架自己，检测不到 notifyd 内部的并发。而 notifyd 恰恰是
// 多 worker + 共享队列 + 后台刷盘的结构 —— 那才是真正需要被检测的地方。
const raceEnabled = true
