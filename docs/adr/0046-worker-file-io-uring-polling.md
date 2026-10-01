# ADR-0046: Worker File 的 io_uring 轮询与 syscall 收敛

- 日期：2026-10-01
- 状态：Busy/批量 one-shot/显式 drain multishot 已接入；未编译、未测试、未测量收益
- 范围：Linux `hammer-runtime::file` 与 Data Worker 调度；不改变任何具体设备的收发实现
- 前置：ADR-0009 的 `DataPlaneMain::FileMode`、ADR-0043 的 worker timerfd

## 问题与证据

Hammer 的 io_uring 目前只订阅 fd **就绪**，不提交 File 读写：`file/linux.rs:31-76` 为每个线程创建 ring；`:93-108,298-315,360-396` 每次 `PollAdd` 都提交、每次修改/删除都 `PollRemove` 并同步等待；`file/mod.rs:691-747` 每个 one-shot CQE 分派后重新提交。`file/mod.rs:402-474` 的 `readv/writev` 仍直接调用 libc。所以减少 readiness syscall 与减少实际 I/O syscall 是两个不同目标。

`main_loop.rs:200-268` 每轮先查 CQ，再跑 graph；只要本轮 `progress` 为 false 就 `park_timeout(idle_slice)`。每个线程都有自己的 File poller；`file/mod.rs:769-805` 的 thread 0 走异步等待，Data Worker 同步检查自己的 CQ。当前 worker 的 `park_timeout` 与 File 就绪检查分离：File 在 park 后就绪时，worker 最多等到下一次超时；不能称为 worker busy poll 或低延迟 File 等待。

`file/mod.rs:142-154,203-241,291-309` 用 `UnsafeCell<Poller>` 和 `&self -> &mut Poller` 允许控制线程在 worker 轮询时触达同一个 ring。VPP 的 `epoll_ctl` 可跨线程操作同一 epoll 实例，不等于 Rust `IoUring` 可并发取得两个 `&mut`；实施前必须给这条访问一个可执行的互斥生命周期。当前 `Pool<Box<File>>` 仅复用 index（`hammer-infra/src/pool.rs:98-149`），`file/linux.rs:376-377` 的 `user_data` 也仅含 index/类别；保留在 CQ 或本地 pending 中的旧 CQE 可能撞上复用后的 File。注释中的“generation-safe”不是现有 `Pool` 的事实。

| 来源 | 设计约束 |
| --- | --- |
| `third_party/vpp/src/vlib/file.c:26-76,108-278`；`src/vlib/main.c:1521-1524` | File 按 `polling_thread_index` 归属，热循环处理一批事件；忙时可跳过文件等待，空闲时才进入可睡眠等待。VPP 用 level-triggered epoll 默认语义。 |
| [io_uring setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)；[SQPOLL(7)](https://man7.org/linux/man-pages/man7/io_uring_sqpoll.7.html) | `SQPOLL` 省提交 syscall，但有独立内核线程、空闲唤醒和 CPU 成本；`IOPOLL` 是受设备/O_DIRECT 限制的存储 I/O 完成轮询，不是 File readiness busy poll。 |
| [multishot poll(3)](https://man7.org/linux/man-pages/man3/io_uring_prep_poll_multishot.3.html)；[multishot(7)](https://man7.org/linux/man-pages/man7/io_uring_multishot.7.html) | 一个 SQE 可产多个 CQE；只有 CQE 的 `IORING_CQE_F_MORE` 表明订阅仍活着。level-triggered fd 未处理完时可能重复通知。 |
| [poll update(3)](https://man7.org/linux/man-pages/man3/io_uring_prep_poll_update.3.html)；`third_party/io-uring/src/opcode.rs:328-390` | 内核支持更新兴趣掩码，但 vendored crate 目前只有 `PollAdd/PollRemove`，不应在设计里假定有可直接调用的 `PollUpdate`。 |
| [io_uring enter(2)](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html)；`third_party/io-uring/src/submit.rs:141-191` | 普通模式提交 SQE 要 `io_uring_enter`；SQPOLL 仅在不需唤醒 SQ 线程、不等待 CQ、不处理 CQ overflow 时可免这次 syscall。只读映射 CQ 不调用 syscall。 |

## 决策

1. **先优化 readiness，不改 File 的同步读写语义。** 每个线程继续拥有自己的 File poller；thread 0 异步，Data Worker 同步。Worker busy poll 指 Data Worker 连续查看自己映射的 CQ、穿插 graph/barrier/timer 调度，且不因“本轮没有事件”调用 `park_timeout`；它不是 `SQPOLL` 或 `IOPOLL`。默认仍保留现有空闲策略，busy 必须显式选择。thread 0 的异步 Tokio File adapter 与 macOS backend 不随之改造。
2. **先批量化 one-shot，后有条件启用 multishot。** Poller 只把重挂 SQE 放进本线程的 SQ；一轮回调结束或 SQ 满时提交一次。CQ 按固定预算读取并把 graph 调度交还主循环；不会为了把所有 CQE 搬进 `LocalRing` 而无界占用 worker。读兴趣稳定的 File 可以使用 multishot，但其 callback/后续 node 必须把 fd 读到 `WouldBlock`，或在预算耗尽时自行继续排程；普通 File 仍用 one-shot 并在批次尾重挂，以保留 VPP 的 level readiness。当前仅 session timerfd 的计数读取满足该契约并显式选择 `Drain`；其他调用方保持 `Level`。
3. **读写兴趣分开考虑。** 对同时有读和可选写兴趣的 File，稳定的读订阅不应因 write flag 变化而被取消。当前实现仍共用一个 poll 请求；`Drain` 与写兴趣并存时退回 one-shot，避免错误地保持混合 multishot 请求。拆分兴趣或使用 `POLL_UPDATE` 尚未实施，须先以真实 File 使用情况确认收益；不在本次顺手改 vendored crate。
4. **控制线程不能并发可变访问 worker ring。** 启动前可构造并登记；启动后的主线程 File 增删改须处于现有 `WorkerBarrier` 保护的 worker 停止窗口，worker 回调仅修改自己拥有的 poller；不加热路径锁，不新增跨线程裸 `&mut` 或第二套 File registry。若调用点无法满足此约束，应先走该 owner 的 worker 事件路径，不能绕过。删除 fd 后须使 token 失效，并在允许 Pool index 复用前解决在途/本地 CQE；内部 token 必须带每次注册变化的代次，不能仅用 u32 index。
5. **不用 SQPOLL 作为 busy 的同义词或默认值。** 第一阶段已用 CQ 映射轮询和批量提交消除重复注册/提交；再独立 A/B 测 `SQPOLL`，同时量化额外内核线程 CPU、抢占 Data Worker、唤醒次数与吞吐。只有在留出专用 CPU 且测量净收益时才考虑可选启用。当前控制线程和 worker 都会提交同一 ring，不能直接启用 `SINGLE_ISSUER`/`DEFER_TASKRUN`；`COOP_TASKRUN` 也需验证只读 CQ 的完成可见性与定期 task-run 进入内核的要求。`IOPOLL` 不用于本次 File readiness。
6. **同步 File I/O 的 syscall 不在 readiness 方案里冒充已消除。** `FileMain::readv/writev` 仍直接调用 libc；改成真正异步 I/O 涉及调用方缓冲区和 iovec 在 CQE 前的所有权，以及短读写、取消和关闭契约。那是独立的 File I/O 设计，不能仅凭本次轮询优化许诺收益，也不能把具体设备的 Buffer 生命周期移进通用 FileMain。

### 拟议接口与调度

以下是本次接入的接口形状；`FileReadinessMode` 字段归 `hammer-core::file::File` 所有，由 runtime 重导出，service session timerfd 只选择通用 `Drain` 契约，不接触 io_uring。现有 `FileMain::poll_for_worker`、`File::new`、`File::set_polling_thread_index` 和 `FileMode::{Sync,Async}` 保持原名、原边界。

配置入口拟为 `[worker] file_poll = "busy"`；省略时为 `adaptive`，不改变现有 `idle_slice`。这是 Data Worker 的轮询策略，不改变 thread 0 的异步模式，也不改变每个线程都有 File poller 的初始化契约。

```rust
// hammer-runtime::file；仅描述 worker 空闲策略，不选择 I/O backend。
pub enum WorkerFilePollMode {
    Adaptive, // 保留现有 idle_slice/park 行为；后续统一 wake 才能阻塞等 CQ。
    Busy,     // 每轮检查 CQ 和 graph，不 park，不调用 submit_and_wait(1)。
}

// 可选 File 契约。只对“持续就绪时会读到 WouldBlock 或自行再排程”的 File 启用。
pub enum FileReadinessMode {
    Level,      // one-shot + 批次末重挂；普通 File 的默认值。
    Drain,      // multishot；调用者承担有界 drain/再排程契约。
}

impl File {
    pub fn set_readiness_mode(&mut self, mode: FileReadinessMode);
}

impl FileMain {
    // 保持现有签名：从当前 worker 的 CQ 获取固定预算事件、分派 callbacks，
    // 回调后汇总重挂/取消 SQE，在返回 graph 前至多做一次普通提交。
    pub(crate) fn poll_for_worker(
        &self,
        thread_index: u32,
        graph: &mut NodeMain,
    ) -> RuntimeResult<usize>;
}
```

`Busy` 的一轮次序仍是 `barrier/refork -> File CQ -> handoff/interrupt -> graph -> timer/exit`，保留当前主循环的检查点；File CQ 无事件不代表远端 handoff、graph 或 timer 无工作。仅 busy 分支跳过 `park_timeout`；不能用 `submit_and_wait(1)` 阻塞 worker，因为现有 `thread::unpark` 和 barrier 并不能唤醒阻塞在 ring 的线程。若以后让 adaptive 直接等待 ring，须先让同一 worker 的 File、handoff、barrier 和退出使用一个可靠的唤醒入口，并证明入睡前检查/发布与唤醒无丢失；这不是打开一个 io_uring flag 就能完成的。

Poller 内部保留 CQE 的 `user_data`、`result` **和 `flags`**；先比较当前 token 与代次，分派前及回调后再次比较，以免同批次旧 CQE 投递到复用的 Pool index。`MORE` 为真时不重挂，`MORE` 为假时按 File 模式重挂；multishot `PollAdd` 若返回 `EINVAL`，该 poller 后续退回 one-shot 并重挂当前 File，不能把可选优化缺失变成 worker 退出。取消 CQE 与已失效 token 只作内部收敛，不分派给新 File。达到 CQ 预算就交还 graph，下一轮继续；thread 0 在异步等待前先处理尚未消费的 CQ，避免固定预算留下的 CQE 被漏等。CQ overflow 复用现有 `FileCompletionQueueFull` 类别；`RuntimeError::FilePollerIo` 保留底层 `io::Error` source，`FilePollerOperationUnsupported` 仍用于必需 opcode 缺失，普通 `WouldBlock` 不转成错误。不新增数字 retval 或日志替代错误。

### syscall 账本

| 路径 | 现在 | 目标与边界 |
| --- | --- | --- |
| 读 File 每次就绪 | one-shot `PollAdd` + `submit()`，每次回调后重挂 | 合格 File 用 multishot 去掉重复 SQE；其他 File 将一轮内重挂批量提交。无 SQPOLL 时每批仍需一次 enter。 |
| 修改/删除 File | `PollRemove` 提交并 `submit_and_wait(1)` | 保留删除完成/生命周期确认，但不在无关每包路径同步等待；多变更同批次处理，代次防 stale CQE。 |
| worker 空闲 | `park_timeout(idle_slice)`；File CQ 不唤醒 park | Busy 模式不 park，持续查 CQ；Adaptive 模式的统一唤醒是另一个前置设计。 |
| File `readv/writev` | 直接同步 syscall | readiness 调优不会去掉它们；异步 I/O 需要单独设计缓冲区生命周期。 |
| SQPOLL | 未启用 | 仅可选基准实验；可省部分提交 enter，不会自动消除同步 File 读写。 |

## 实施顺序与验证门槛

1. 基线：在独立 CI/lab 记录 worker 每秒 `io_uring_enter`、`readv/writev`、CQ overflow、CPU 利用率、File 到 graph 延迟和吞吐。只读 perf/strace 采样不得在性能结果中与未采样运行混算；本 ADR 不自行重启被叫停的本地测试。
2. 先修 owner/barrier 和 token 生命周期，再做提交批量化、CQ 预算和 Busy 模式；检查空 File、timerfd、worker shutdown、barrier 与远端 handoff 在 busy 下都能前进。默认 Adaptive 行为保持可用。
3. 对真正满足 drain 契约的 File 开 multishot；覆盖读写同 fd、取消与立即重建、index 复用、连续 readiness、CQ 满/overflow 和 `MORE` 消失。
4. 单独 A/B 测 SQPOLL，再决定是否增加配置；不因“用了 io_uring”就视为优化成功。同步 File I/O 的收敛另立设计。

本次新增 `WorkerFilePollMode`、`FileReadinessMode` 及一次 `File` 设置方法；它们分别表达 worker 空闲策略和 File 的 drain 契约，现有 `FileMode` 仅表达 Sync/Async owner，无法替代。同步取消合并、`PollUpdate`、读写兴趣拆分、统一 idle wake、SQPOLL 和异步 File I/O 未实施；不得把这次改动称为已消除所有 File I/O syscall。按用户要求本次不运行编译、测试或 CI；以上验证矩阵保留给后续验收。
