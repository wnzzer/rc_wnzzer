# notifyd —— 外部通知可靠投递服务

> 🚧 **进行中**：v1 设计已完成，待评审后进入实现阶段。

企业内部多个业务系统在关键事件发生时需要调用外部供应商的 HTTP(S) API。
notifyd 把这件事从业务主链路上摘下来：业务系统提交一个已构造完整的 HTTP 请求描述，
notifyd 持久化后立即返回，此后负责反复投递直到成功或放弃。

**核心承诺**

- **C1 收下就不丢** —— 返回 `202` 前任务已 fsync 落盘，`kill -9` / 掉电后仍在。
- **C2 不放弃得太早** —— 指数退避（full jitter）吸收外部系统从抖动到小时级宕机。

**工程约束**：零第三方依赖，只用 Go 标准库，单静态二进制，单进程常驻。

## 文档

| 文件 | 内容 |
|---|---|
| [`docs/spec.md`](docs/spec.md) | 技术规格 v1：系统边界、投递语义、WAL 设计、重试策略、演进路线、验收标准 |
| [`docs/decisions.md`](docs/decisions.md) | 取舍与否定记录，31 条（含 14 条已否决） |
| [`docs/ai-session-log.md`](docs/ai-session-log.md) | AI 协作工作流水账（已脱敏） |
| [`CLAUDE.md`](CLAUDE.md) | 本仓库的协作工作流与脱敏纪律 |
| `AI_USAGE.md` | 作业交付物，收尾时由上面两本账汇总生成 |

## 快速导航（作业必答题）

| 必答题 | 位置 |
|---|---|
| 系统边界：做什么 / 明确不做什么 | [spec §2](docs/spec.md#2-系统边界) |
| 投递语义 | [spec §5](docs/spec.md#5-投递语义与幂等) |
| 外部系统长期不可用的处理策略 | [spec §6](docs/spec.md#6-调度与重试) |
| 哪些 AI 建议属于过度设计、未采纳 | [decisions.md](docs/decisions.md) 中 14 条 ❌ |
| 未来演进路线 | [spec §12](docs/spec.md#12-演进路线) |
| 用/不用开源中间件的理由 | [D-018](docs/decisions.md#d-018)、[D-019](docs/decisions.md#d-019) |
