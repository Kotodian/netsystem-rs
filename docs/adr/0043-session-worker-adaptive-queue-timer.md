# ADR-0043: Session Worker 的 Queue Node 与自适应定时唤醒

- 日期：2026-09-28
- 状态：Accepted；代码已实施，按要求未编译、测试或执行静态检查
- 前置：ADR-0038、ADR-0039
- 范围：service Session worker、session-input/session-queue 的初始化、adaptive 定时唤醒
- 不包含：TCP 定时器、Session 业务事件重设计、thread zero 的数据面 Session queue

## VPP 源码依据

| 位置 | 实际语义 |
|---|---|
| `third_party/vpp/src/vnet/session/session.h:82-170` | `session_worker_t` 有 cacheline mark、Session pool、事件队列、`timerfd`、`timerfd_file`、flags 和 state；**没有** timer wheel。 |
| `third_party/vpp/src/vnet/session/session.h:1101-1116` | pending TX buffer 在 interrupt 状态唤醒 queue；`session_wrk_update_time` 是 `always_inline` 的时间赋值。 |
| `third_party/vpp/src/vnet/session/session.c:1737-1780,2018-2090` | worker/MQ 在 Session manager 首次 enable 时构造；仅在 `!no_adaptive && use_private_rx_mqs` 时为 worker 安装 adaptive timerfd。 |
| `third_party/vpp/src/vnet/session/session.c:2196-2270` | `session_node_enable_disable` 设置 input/queue/process node 状态；thread zero 与普通 worker 路径不同。 |
| `third_party/vpp/src/vnet/session/session_node.c:43-80` | `set_state` 改 worker state，并按 Polling/Interrupt/Idle 更新 timerfd；普通 worker 分别为停表、1 ms、100 ms。 |
| `third_party/vpp/src/vnet/session/session_node.c:1975-2030,2033-2177` | queue 先更新 worker/transport 时间，再读 MQ，处理 control/new/old IO，flush TX；末尾才按负载切换状态。 |
| `third_party/vpp/src/vnet/session/session_node.c:1756-1855,1919-1989` | control 派发先按事件类型选择 owner，handler 返回后释放未重链的元素；MQ 导入必须在释放 descriptor 前保存尾随 control 消息。 |
| `third_party/vpp/src/vnet/session/session.c:29-86,151-165` | RPC/IO/Session 三个 family 均锁目标 worker MQ 的 IO ring，提交后仅在 worker 为 Interrupt 时设置 queue node interrupt；普通 RPC 同线程直接执行，force 始终入队。 |
| `third_party/vpp/src/vlib/node_funcs.h:216-232` | 对远端 worker 设置 node interrupt 时原子置位并通知该线程；MQ 事件内容不经 packet handoff。 |
| `third_party/vpp/src/svm/message_queue.c:275-286` | `svm_msg_q_msg_data` 返回所选 ring 槽的消息地址，槽内容只能在 descriptor 回收前读取。 |
| `third_party/vpp/src/vnet/session/session_node.c:2168-2217` | queue node 初始 Disabled；timerfd 可读时只标记 queue interrupt 并读走计数；worker 持有 fd，FileMain 注册可读回调。 |
| `third_party/vpp/src/vlib/main.h:426-439`、`main.c:1064-1066,1685` | 每轮重置、按 internal pending frame 的 vector 数取最大值，并由 `vlib_last_vectors_per_main_loop` 读取；不是前一轮总包数。 |
| `third_party/vpp/src/vnet/session/session_node.c:925-945,1585,2157` | queue 计数器为 TX、TIMER、NO_BUFFER；本版本源码只实际增加 TX 和 NO_BUFFER，未增加 TIMER。 |
| `third_party/vpp/src/vnet/tcp/tcp.c:1361-1370` | TCP 自己的 `tcp_update_time` 到期 TCP timer wheel；Session worker 不拥有该 wheel。 |

Hammer 已有 `crates/hammer-runtime/src/file/mod.rs:290-307,691-730` 的 per-worker
`FileMain::add` 和可读回调。`crates/hammer-runtime/src/main_loop.rs:202-224` 在调度 input
node 之前先 poll worker 的 FileMain。现有 `FileMain::Deadline` 用内部 timerfd 实现
**单次到期后重装**，而 VPP `session_wrk_timerfd_update` 设置的是周期 timerfd；本设计
因此复用 FileMain 的通用 File 注册/dispatch，不以 `Deadline` 替代 worker 的 timerfd。

## 决定与边界

`SessionWorker.timer: TimerWheel1t2w2048sl<SessionHandle>` 删除。它目前没有调度或消费路径，
也不对应 VPP `session_worker_t`。worker 改为持有 `timer_fd: Option<OwnedFd>` 和
`timer_fd_file: Option<u32>`，分别对应 VPP 的 timerfd 与 FileMain 注册索引；不再使用
`-1`/`u64::MAX` 哨兵。注册 File 时复制一次 fd，FileMain 拥有复制的描述符并读取到期
计数；两个 fd 指向同一个内核 timerfd，worker 原 fd 用于 `timerfd_settime`。
`None` 表示未启用 adaptive，不表示创建失败。不得让 Session 消费 TCP timer token。
VPP timerfd 路径是 Linux 接口；Hammer 在非 Linux 平台只有请求 adaptive 时以
`TimerCreate` 保留 `Unsupported` OS source 拒绝启动，默认非 adaptive 路径不受影响。

`session-input` 和 `session-queue` 的 graph declaration 在主 graph 构造阶段注册，注册后立即
置 `Disabled`，对应 `session_input.c:404-408` 与 `session_node.c:2168-2177`。每个 Data Worker
的 worker-init 只绑定本 worker 的 NodeId、可选地创建并注册 timerfd，并依据当时的 Session enable
状态设置 `session-input = Interrupt`、`session-queue = Polling`。默认 Session disabled。
thread zero 在 Hammer 不执行数据面；本 ADR 不为它创建 timerfd，也不通过一个虚构的
`session-queue-process` 来驱动 worker queue。ADR-0039 关于 VPP thread-zero process/main
node 的记录仍是源码说明，不是本 ADR 的 worker 初始化前置条件。

service 拥有 queue 调度和 worker timerfd 的业务规则；runtime FileMain 只管理可读注册与
回调调度。plugin-session 只提供 IP 实例与 transport 注册；TCP/UDP 只通过已注册的
transport time 入口更新各自 worker。不能把 timerfd 或
Session queue node 放进 plugin-session/TCP，也不能把 TCP timer wheel 移回 service。

## 类型与方法

以下是目标接口和关键执行路径，不重新定义已有 Session event、FIFO 或 transport API。
代码块是设计片段，省略未变的字段和既有处理分支；**不是当前 Rust 实现**，不能把省略
部分当成新的 `Default` 或空操作。注释给出每个函数的 VPP 来源；`File`、`FileMain`、
`NodeState`、`CacheLineAlignMark` 均复用现有类型。`timer_fd` 始终是
`SessionWorker` 的字段，FileMain 仅持有指向同一内核 timerfd 的注册描述符。

```rust
// VPP session.h:82-170; session.c:2040-2062.
// 其余既有 SessionWorker 字段原位保留；只替换无效 timer 字段。
#[repr(C)]
pub struct SessionWorker {
    cacheline0: CacheLineAlignMark,
    sessions: Pool<Session>,
    event_queue_index: u32,
    worker_index: u32,
    last_time: f64,
    last_time_us: u64,
    input_node: Option<NodeId>,
    queue_node: Option<NodeId>,
    timer_fd: Option<OwnedFd>,
    timer_fd_file: Option<u32>,
    flags: SessionWorkerFlags,
    transport_time_subscriptions: Vec<u8>,
    // ...现有 event lists、pending buffers、migration、DMA 等字段
}

// VPP session.h:59-69; session_node.c:1989-2030.
pub enum SessionWorkerState {
    Polling,
    Interrupt,
    Idle,
}

pub struct SessionWorkerFlags {
    pub adaptive: bool,
}
```

`CacheLineAlignMark` 仍是结构体首字段，worker `Vec` 在 launch 前构造且不再移动。
`timer_fd_file` 只是 FileMain pool 索引，worker 不持有裸指针。`flags.adaptive` 只有
timerfd 创建、复制和 FileMain 注册全部成功后才设为 true；不能仅以 `!no_adaptive`
推断 timerfd 已存在。

`new` 在 `SessionMain::init` 构造固定 worker `Vec` 时运行：按已有配置预分配 Session
pool、event/control pool 和五条事件列，保存 worker/MQ index，时间缓存置零，NodeId 与
timerfd/FileMain index 均置 `None`，`adaptive=false`。`SessionMain.worker_states`
固定 `Vec<AtomicU8>` 是每个 worker 状态的唯一存储，初始 Polling 只表示
将来启用时的调度状态；两个 graph node 仍 Disabled。它不注册 File、不调用
`timerfd_create`、不开始周期唤醒。TCP timer wheel 不属于这个构造步骤。

`session_worker_init` 是 runtime 自动调用的 worker-init hook。只在此处按名字取得本 worker
graph 中的两个 node，并保存 `NodeId`；不存在是 graph 安装失败，返回现有
`NodeMissing`，不安装 timerfd。随后读取全局 Session enable 位；未启用则直接返回，
不改变 Disabled。已启用且
`use_private_rx_mqs && !no_adaptive` 时，先完成 timerfd 创建、fd 复制、FileMain 注册，
然后才把 input/queue 切到 Interrupt/Polling。已验证的 NodeId 拒绝这两个合法状态是
runtime 所有权不变量，不新增可恢复错误或引入部分启用后的撤销路径。
`enable_adaptive_mode` 成功前局部 `OwnedFd` 自动关闭；FileMain 注册成功时才把两个
fd 所有权及注册 index 写入 worker，并设置 adaptive flag。
worker-init 的现有 `Result` 向 WorkerThread 入口传播；timerfd 初始化失败时该 worker
不得继续进入未完成初始化的数据面循环。主线程启动流程目前不等待每个 worker 的
初始化结果，因此不能把此行为描述为同步的 daemon 启动失败。

```rust
impl SessionWorker {
    // VPP session.c:2040-2062; session_node.c:2168-2217. Construct the
    // worker slot and its event lists before launch; do not create a timer.
    fn new(worker_index: u32, config: SessionConfig) -> Self {
        let preallocated_per_worker = if config.worker_count == 1 {
            config.preallocated_sessions
        } else {
            ((u64::from(config.preallocated_sessions) * 11)
                / (u64::from(config.worker_count) * 10)) as u32
        };
        let event_capacity = config.configured_worker_mq_length.max(2_048) as usize;
        Self {
            cacheline0: CacheLineAlignMark,
            sessions: if preallocated_per_worker == 0 {
                Pool::new()
            } else {
                Pool::with_fixed_capacity(preallocated_per_worker)
            },
            event_queue_index: worker_index,
            last_time: 0.0,
            last_time_us: 0,
            worker_index,
            transport_time_subscriptions: Vec::new(),
            sessions_to_enqueue: Vec::new(),
            app_workers_pending: Bitmap::new(),
            input_node: None,
            queue_node: None,
            timer_fd: None,
            timer_fd_file: None,
            flags: SessionWorkerFlags { adaptive: false },
            tx_context: SessionTxContext::default(),
            event_elements: Pool::with_capacity(event_capacity),
            control_event_data: Pool::with_capacity(event_capacity),
            control_events: LinkedList::new(),
            new_events: LinkedList::new(),
            old_events: LinkedList::new(),
            pending_connects: LinkedList::new(),
            events_pending_main: LinkedList::new(),
            pending_tx_buffers: Vec::new(),
            pending_tx_nexts: Vec::new(),
            pending_connect_count: 0,
            pending_notifications: Vec::new(),
            rx_segments: Vec::new(),
            migration: SpinLock::new(SessionMigrationState {
                requests: Vec::new(),
                handling: Vec::new(),
            }),
            config_index: 0,
            dma_enabled: config.dma_enabled,
            dma_transfers: Vec::new(),
            dma_head: 0,
            dma_tail: 0,
            dma_size: 0,
            dma_batch_number: 0,
            dma_batch: 0,
            last_event_poll: 0.0,
        }
    }

    // VPP session.c:2059-2061; session_node.c:2180-2217.
    // 只在 enabled && use_private_rx_mqs && !no_adaptive 时调用。
    fn enable_adaptive_mode(
        &mut self,
        runtime: &DataPlaneMain,
    ) -> Result<(), SessionQueueError> {
        assert!(self.timer_fd.is_none() && self.timer_fd_file.is_none());
        // timerfd_create is the VPP OS boundary; OwnedFd closes on failure.
        let raw_fd = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
            )
        };
        if raw_fd < 0 {
            return Err(SessionQueueError::TimerCreate {
                worker: self.worker_index,
                source: std::io::Error::last_os_error(),
            });
        }
        let timer_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let registered_fd = timer_fd.try_clone().map_err(|source| {
            SessionQueueError::TimerDuplicate {
                worker: self.worker_index,
                source,
            }
        })?;
        let mut file = File::new(
            registered_fd,
            format!("session-wrk-tfd-{}", runtime.thread_index()),
            u64::from(self.queue_node.expect("Session queue NodeId was installed").slot()),
            FileFunctions {
                read: Some(session_queue_timer_ready),
                write: None,
                error: None,
            },
        );
        file.set_polling_thread_index(runtime.thread_index());
        let file_index = FILE_MAIN.get()
            .expect("FileMain initializes before Data Workers")
            .add(file)
            .map_err(|source| SessionQueueError::TimerRegistration {
                worker: self.worker_index,
                source,
            })?;
        self.timer_fd = Some(timer_fd);
        self.timer_fd_file = Some(file_index);
        self.flags.adaptive = true;
        Ok(())
    }

    // VPP session_node.c:43-80. Polling 停表；Interrupt 1 ms；Idle 100 ms。
    // 在 worker 上改变 timer 周期，不创建/注册 fd，也不调度 queue。
    // 成功设表后才提交 state，避免失败时声称 timer 已生效。
    #[inline]
    fn set_state(&mut self, state: SessionWorkerState) -> Result<(), SessionQueueError> {
        let timer_fd = self.timer_fd.as_ref()
            .expect("adaptive worker retains its timerfd");
        let nanoseconds = Self::timeout(state)
            .map_or(0, |timeout| libc::c_long::from(timeout.subsec_nanos()));
        let interval = libc::timespec { tv_sec: 0, tv_nsec: nanoseconds };
        let spec = libc::itimerspec {
            it_value: interval,
            it_interval: interval,
        };
        let status = unsafe {
            libc::timerfd_settime(timer_fd.as_raw_fd(), 0, &spec, std::ptr::null_mut())
        };
        if status < 0 {
            return Err(SessionQueueError::TimerArm {
                worker: self.worker_index,
                source: std::io::Error::last_os_error(),
            });
        }
        SessionMain::global()
            .expect("Session Main owns the published worker state")
            .set_worker_state(self.worker_index, state);
        Ok(())
    }

    // VPP session_node.c:56-66. Hammer 没有数据面 thread zero 分支。
    #[inline(always)]
    const fn timeout(state: SessionWorkerState) -> Option<Duration> {
        match state {
            SessionWorkerState::Polling => None,
            SessionWorkerState::Interrupt => Some(Duration::from_millis(1)),
            SessionWorkerState::Idle => Some(Duration::from_millis(100)),
        }
    }

    // VPP session.h:1112-1116. Only caches this worker's dispatch time;
    // transport subscribers consume `now` in the next queue step.
    #[inline(always)]
    fn update_time(&mut self, now: f64) {
        self.last_time = now;
        self.last_time_us = (now * 1_000_000.0) as u64;
    }

    // VPP session_node.c:1756-1855,2069-2092. Capture the control-list tail
    // at entry, dispatch only that prefix before new/old IO, and leave events
    // appended by a control handler for the next queue dispatch. An element
    // retained/relinked by its handler is not freed here. The handler owns
    // each existing SessionEventType's payload and cleanup; timerfd does not.
    fn dispatch_control_events(
        &mut self,
        runtime: &mut DataPlaneMain,
        main: &SessionMain,
    ) -> Result<(), SessionError>;

    // VPP session_node.c:1989-2030,2160-2164; vppinfra/llist.h:75.
    // Called after MQ import, control/IO dispatch and TX flush. Hammer
    // does not store VPP's five permanent list heads in event_elements.
    fn update_state(&mut self, runtime: &DataPlaneMain) {
        if !self.flags.adaptive {
            return;
        }
        let has_events = !self.event_elements.is_empty();
        let vectors = runtime.max_internal_frame_vectors();
        let next = match self.state() {
            SessionWorkerState::Polling if !has_events && vectors < 1 => {
                Some(SessionWorkerState::Interrupt)
            }
            SessionWorkerState::Interrupt if has_events || vectors > 1 => {
                Some(SessionWorkerState::Polling)
            }
            SessionWorkerState::Interrupt if self.sessions.is_empty() => {
                Some(SessionWorkerState::Idle)
            }
            // VPP's Idle branch sees at least the five list heads on every
            // queue dispatch, including a timer-only wakeup.
            SessionWorkerState::Idle => Some(SessionWorkerState::Interrupt),
            _ => None,
        };
        let Some(next) = next else { return };
        if let Err(error) = self.set_state(next) {
            // Timer failure must not strand a Session queue in interrupt mode.
            tracing::error!(worker = self.worker_index, %error, "Session timer arm failed");
            self.flags.adaptive = false;
            SessionMain::global()
                .expect("Session Main owns the published worker state")
                .set_worker_state(self.worker_index, SessionWorkerState::Polling);
            runtime.nodes().set_node_state(
                self.queue_node.expect("Session queue node was installed"),
                NodeState::Polling,
            ).expect("installed Session queue accepts Polling state");
            return;
        }
        if next == SessionWorkerState::Polling || next == SessionWorkerState::Interrupt {
            runtime.nodes().set_node_state(
                self.queue_node.expect("Session queue node was installed"),
                if next == SessionWorkerState::Polling {
                    NodeState::Polling
                } else {
                    NodeState::Interrupt
                },
            ).expect("installed Session queue accepts its worker state");
        }
    }
}
```

`enable_adaptive_mode` 中的 timerfd 是 worker 的**设表端**，FileMain 注册的是复制的
**读取端**；两者共享内核计数器，不是两套 timer。注册的 `private_data` 保存初始化时
取得的 `session-queue` NodeId slot，`polling_thread_index` 选同一 Data Worker 并保留
错误上下文所需的 worker 身份。任何一步创建/复制/注册失败
都返回带 OS 或 runtime source 的启动错误；worker 的两个 `Option` 与 adaptive flag
保持原值。FileMain 注册成功后，fd 生命周期随该 worker 的 FileMain 注册结束；
后续 timer 状态更新只借 worker 的 `timer_fd`，不再重复注册。

`set_state` 只负责该 worker 的周期和 `SessionWorkerState`，不决定 graph node 的
Polling/Interrupt。Polling 给 `timerfd_settime` 零初次到期和零周期，Interrupt 为
1 ms/1 ms，Idle 为 100 ms/100 ms；后两者都是周期唤醒，不是一次性 deadline。
`update_state` 仅在 `flags.adaptive` 时运行：Polling 无事件且本轮 internal frame 最大
vectors 小于 1 时转 Interrupt；Interrupt 有事件或本轮最大 vectors 大于 1 时转 Polling，
无 Session 时转 Idle；Idle 在下一次 queue 执行时转 Interrupt，即使只是 timerfd 唤醒。
Hammer 的 event list head 不占 `event_elements` pool，所以 Polling/Interrupt 分支的
`event_elements.is_empty()` 对应 VPP 的 `clib_llist_elts(event_elts) == 5`。
VPP Idle 分支却只判断 `clib_llist_elts(event_elts)` 是否非零；五个常驻 head 使该条件
恒成立（`vppinfra/llist.h:75`）。Rust 不能将这个分支误写成“有业务事件才转”。
`set_state` 先重设 worker-owned timerfd，再提交 state；Polling 传零 `itimerspec`
停表，Interrupt 和 Idle 分别设置相等的首次到期/周期，保持 VPP 的周期唤醒。
Idle 不更改 graph node 的 Interrupt 状态，只把唤醒间隔放大。设表失败时
`update_state` 将 queue 保持 Polling、关闭 adaptive 决策，并记录带 source 的错误；
继续轮询保证事件不会被一个失效定时器困住。`update_time` 只写本 worker 的秒/微秒
缓存，不触碰 TCP timer wheel；下一步 `update_transport_time` 依照现有订阅表调用
各插件自己的更新时间入口。

`dispatch_control_events` 以 **MQ 导入完成时** control list 的尾元素为边界：
从队首逐个取出并交给 service 应补齐的 control-event 分支，直到处理该尾元素；
分支新追加的事件保留到下一次 queue dispatch。VPP
`session_event_dispatch_ctrl` 在 handler 可能移动 event pool 后重新取得元素，
只释放未被重新链接的元素及其 control data。Hammer 现有的三个 transport
control handler 不重链，因此按 index 重新取得并释放；将来接入 VPP
`session_mq_connect_handler` 等 app handler 时，必须先让 pending list 持有
该 index，不能在此释放。不能保留跨 handler 的 `&SessionEventElement`。
`HalfClose`/`Close`/`Reset` 的状态判断归 service，具体调用归 transport 插件；
listen/connect 等 app control 请求的处理归 session/application owner，不归 timerfd
回调。本 ADR 不新增第二个事件队列。

IO 预算遵循 `session_node.c:2046,2096-2140`：进入 queue 时已有的
`pending_tx_buffers.len()` 先占用 128-vector 预算，再依次处理 new/old IO；
不能从零计数，否则本轮输出可能超过 VPP 的帧上限。

control ring 的元素可大于固定 18 字节 `SessionEvent`。infra 的
`SvmMsgQ::message_bytes(descriptor)` 借出所选 ring 的完整槽位；caller 独占已出队
descriptor，并在释放 descriptor 前完成复制。service 从槽位的事件头读取事件类型，
将 `SESSION_CTRL_EVT_BOUND` 起的尾随消息保存到现有 `SessionControlData` pool，
然后归还 MQ 槽。它不能用要求整个槽位与 `T` 等长的 `SvmMsgQ::read<T>` 丢弃尾随消息。
这对应 `session_node.c:1936-1989` 与 `message_queue.c:275-286`。

transport 的 `HalfClose`、`Close`、`Reset` 由 service 保留 Session 状态判断，
经按协议注册的单态化入口调用具体插件的 `Transport` trait；service 不持有 TCP 类型、
`dyn` 或 transport VFT。handler 不存在时先保留原 Session 状态，再返回已有
`SessionError::TransportNotRegistered`；当前 event element 在返回前释放。
VPP `session_send_evt_to_thread`（`session.c:29-86`）是三个事件 family 共用的
私有 IO-ring producer，不是 app-side `app_send_io_evt_to_vpp`。Hammer 按目标
worker index 找到其 MQ，`producer(Nowait)` 对应锁 MQ；检查 IO ring 空间，按 family
取得已分配槽位并按 family 原位写 RPC 索引与序号、IO Session index 或 Session
handle，提交并解锁。VPP `svm_msg_q_msg_data` 返回槽位地址，`session.c:47-76`
直接赋 union 字段；这里不先构造本地事件再 `producer.write` 复制 18 字节。
应用侧 `AppSession::send_io_event` 也按 `application_interface.h:595-628` 对已分配
槽位原位写 index 和 event type。
只有提交成功后且
目标 worker 状态为 `Interrupt`，才设置 session-queue node interrupt。`Busy`、`Full`
不提交也不设置 interrupt；signal-after-commit 已入队，仍执行状态判断。
状态唯一存放于 `SessionMain.worker_states: Vec<AtomicU8>`：worker 在 timer/graph
状态转换时 Release 发布，producer 在成功提交后 Acquire 读取。不能跨线程读取
`UnsafeCell<SessionWorker>` 的普通字段，也不创建 `Arc<AtomicBool>` 镜像。
runtime 的 `WorkerThread` 保存通用 node interrupt 原子位图；对应 VPP
`vlib/node_funcs.h:216-232` 的 `clib_interrupt_set_atomic`。远端线程通知是该 runtime
操作内部的行为，不是额外的 Session handoff 或独立事件投递。Polling/Idle 不调用它。
worker-init 只解析一次 queue `NodeId`，并通过 `SessionMain.queue_node: AtomicU32`
核对各 worker graph 的索引一致；内部未发布值仅用于启动断言，不作为错误返回。
runtime `start_workers` 在 launch 前为每个 `WorkerThread` 一次性填充固定 interrupt
位图，启动后不再改变位图分配；refork 后 graph 的 node count 可能增大，扫描只到
位图已分配的 word 数。此入口只发布启动前已注册的 session-queue NodeId，不支持
用它中断 refork 后新加入的 node。不为它加新 `OnceLock`。service 不让 runtime 依赖 Session。

IO family 仍由 `program_tx_io_event` 和 `program_transport_io_event` 限定类型；
Session family 增加 `send_control_event(handle, event)`，只接受
`HalfClose`/`Close`/`Reset`。VPP 接收 `session_t *` 后提取 handle；Rust 的
跨 worker API 直接接收 `SessionHandle`，不借用目标 worker 的 Session。
RPC family 也占 IO ring 的一个 `SessionEvent`，目标 queue 的 control 列先于 IO
处理。VPP 将 callback/opaque 指针直接放入事件；Rust 不从 SVM 字节调用地址，而在
service 固定容量 `Pool<SessionRpc>` 保存 `fn(u64)`、参数和序号，MQ payload 只保存
pool index 与序号。目标 worker 校验并取出请求，释放短时 `SpinLock` 后调用；
RPC 请求只在目标 MQ 上锁并确认 ring 有空间后才占用 pool；`Busy`/`Full` 不
保留请求。锁顺序固定为目标 MQ producer 再到 RPC pool；消费方归还 MQ descriptor
后才取得 RPC pool 锁，不在任何锁内调用 callback。同线程普通 RPC 直接调用，
force 始终入队，分别
对应 `session.c:151-165`。这不是第二条 queue 或 packet handoff。

```rust
// VPP session_node.c:1756-1790; session.c:1641-1709.
pub type SessionControlDispatch =
    fn(&mut SessionWorker, u32, SessionEventType) -> Result<(), SessionError>;

impl SessionMain {
    // VPP transport.c 的协议注册边界；插件将具体 Transport 调用单态化。
    pub fn register_transport_control(
        &self,
        protocol: u8,
        dispatch: SessionControlDispatch,
    ) -> Result<(), SessionError>;
}

// VPP session.c:142-148. Rust 以 handle 表达跨 worker 的 Session 身份。
pub fn send_control_event(
    handle: SessionHandle,
    event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError>;

// VPP session.c:29-86,151-165; session_node.c:1766-1770.
// 请求保存在 service-owned pool，MQ 只携带 index 与 sequence。
struct SessionRpc {
    callback: fn(u64),
    argument: u64,
    sequence: u64,
}

// VPP session.c:20-86. 仅用于 enqueue 时选择 union arm；MQ 内仍是 SessionEvent。
enum SessionQueueEvent {
    Io { event_type: SessionEventType, session_index: u32 },
    Session { event_type: SessionEventType, handle: SessionHandle },
    Rpc { callback: fn(u64), argument: u64 },
}

// VPP session_types.h:476-492. packed 的最后 16 字节对应 union 存储。
#[repr(C, packed)]
pub struct SessionEvent {
    pub event_type: u8,
    pub postponed: u8,
    pub session_index: u32,
    pub worker_index: u32,
    pub rpc_sequence: u64,
}

impl SessionMain {
    #[inline(always)]
    fn worker_state(&self, worker_index: u32) -> SessionWorkerState;

    // 锁目标 worker MQ，检查 IO ring，再按 enum 分支原位写事件并提交；
    // 只有 Interrupt 时调用 runtime 的 interrupt_worker_node。
    #[inline]
    fn enqueue_event(
        &self,
        worker_index: u32,
        event: SessionQueueEvent,
    ) -> Result<SessionEventEnqueue, SessionError>;
}

// VPP session.c:151-165. 普通同线程直接执行；force 总是进目标 worker MQ。
pub fn send_rpc_event(
    worker_index: u32,
    callback: fn(u64),
    argument: u64,
) -> Result<SessionEventEnqueue, SessionError>;
pub fn send_rpc_event_force(
    worker_index: u32,
    callback: fn(u64),
    argument: u64,
) -> Result<SessionEventEnqueue, SessionError>;

impl SvmMsgQ {
    // VPP message_queue.c:275-286; descriptor 释放前才能借用该槽。
    pub unsafe fn message_bytes(
        &self,
        descriptor: SvmMsgQDescriptor,
    ) -> Result<&[u8], SvmMsgQError>;
}

impl SvmMsgQProducerGuard<'_> {
    // VPP message_queue.c:275-280; descriptor 发布前由 producer guard 借用。
    pub unsafe fn message_bytes_mut(
        &mut self,
        descriptor: SvmMsgQDescriptor,
    ) -> Result<&mut [u8], SvmMsgQError>;
}
```

目前只有 transport 的三个 control 分支已接入；listen/connect 等 app control MQ
请求和回复仍缺具体 handler。对这些尚未接入的事件，当前实现按 VPP 的 default
分支告警并释放，不把它们描述为已支持的应用操作。TCP 的既有
`Transport::half_close/reset` 仍委托 `close`，也不能声称 TCP 关闭语义已对齐 VPP。

指标由 `DataPlaneMain` 提供，不放进 Session worker，也不是上一轮包数之和。
VPP 在 `dispatch_pending_node` 返回后，以该 internal pending frame 的
`f->n_vectors` 更新本轮最大值；`vlib_increment_main_loop_counter` 在轮末将其清零。
Hammer 的 `Frame` 在 dispatch 后可能被复用，所以在 dispatch 前保存这个 frame 的输入
vector 数，在成功 dispatch 后记录最大值；这保存的是同一个 pending frame 的规模，
不是 `Node::process` 返回的输出 packet 数。

```rust
// VPP vlib/main.h:91,426-439; vlib/main.c:1064-1066,1685.
// 只展示 DataPlaneMain 新增字段与读取方法，不重定义整个 runtime main。
pub struct DataPlaneMain {
    pub(crate) max_internal_frame_vectors: usize,
    // ...现有字段
}

impl DataPlaneMain {
    // VPP vlib/main.h:435-439, always_inline vlib_last_vectors_per_main_loop.
    #[inline(always)]
    pub fn max_internal_frame_vectors(&self) -> usize {
        self.max_internal_frame_vectors
    }
}

// In data_plane_main_loop, at the beginning of each worker loop iteration
// (equivalent to VPP main.h:426-431 resetting at the previous iteration's end):
main.max_internal_frame_vectors = 0;

// In DataPlaneMain::run_ready_function_nodes, only in the internal pending
// frame branch, around the existing dispatch (VPP main.c:1064-1066):
let input_vectors = frame.len();
self.dispatch_node(node, &mut frame)?;
self.max_internal_frame_vectors =
    self.max_internal_frame_vectors.max(input_vectors);
```

这两处是**现有函数内的插入点**，不是新增 `begin_main_loop`/
`record_internal_frame` API。每个 Data Worker 只写读自己的 `DataPlaneMain` 字段；
同一轮两次 `run_ready_nodes` 共享该轮的最大值。只统计 internal pending frame，
不统计 driver/pre-input 调用、Session TX 数、Node 返回值或所有 frame 的累计数。
queue node 在该轮实际调度位置调用 getter，供 `update_state` 比较 `< 1` 和 `> 1`。
当前 runtime 尚无这项计数，这是实施 adaptive 切换的前置项；无指标时不能私造
Session 负载估计器。

```rust
// VPP session_node.c:2180-2217; service 的 worker 持有 timerfd，
// FileMain 持有复制的 fd 并在 owner worker 上调用此可读回调。
fn session_queue_timer_ready(
    graph: &mut NodeMain,
    file: &mut File,
) -> RuntimeResult<()> {
    // VPP session_node.c:2180-2187 uses session_queue_node.index directly.
    // File.private_data holds this worker graph's NodeId slot from worker init.
    let queue = NodeId::new(u32::try_from(file.private_data())
        .expect("Session queue NodeId fits File private data"));
    graph.mark_interrupt_pending(queue)?;
    let worker = u32::try_from(DataWorkerId::try_from(file.polling_thread_index())?.slot())
        .expect("configured worker slot fits u32");
    let mut expirations = 0_u64;
    loop {
        let read = unsafe {
            libc::read(
                file.fd(),
                std::ptr::from_mut(&mut expirations).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if read >= 0 {
            assert_eq!(
                read,
                std::mem::size_of::<u64>() as isize,
                "timerfd returns exactly one expiry counter",
            );
            return Ok(());
        }
        let source = std::io::Error::last_os_error();
        match source.kind() {
            std::io::ErrorKind::Interrupted => continue,
            std::io::ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(SessionQueueError::TimerRead { worker, source }.into()),
        }
    }
}

// VPP session_input.c:404-408; session_node.c:2168-2177.
// 注册成功即设置 NodeState::Disabled，而不是依赖 worker-init 事后关掉主 graph。
pub fn register_session_input_node(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-input") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_driver(SessionInputNode)?;
    runtime.nodes().set_node_state(node, NodeState::Disabled)?;
    Ok(node)
}

pub fn register_session_queue_node(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-queue") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_driver(SessionQueueNode)?;
    runtime.nodes().set_node_state(node, NodeState::Disabled)?;
    Ok(node)
}

// VPP session.c:2040-2062,2209-2241; Hammer worker_init runs after
// the main graph has been cloned and before this worker enters its loop.
#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    let main = SessionMain::global()
        .expect("Session Main initializes before worker graph setup");
    let worker = unsafe { main.worker_mut(runtime) }
        .expect("Session worker slot was constructed before launch");
    let input = runtime.node_by_name("session-input")
        .ok_or(SessionQueueError::NodeMissing)?;
    let queue = runtime.node_by_name("session-queue")
        .ok_or(SessionQueueError::NodeMissing)?;
    worker.install_input_node(input);
    worker.install_queue_node(queue);
    let published = main.queue_node.compare_exchange(
        u32::MAX, queue.slot(), Ordering::AcqRel, Ordering::Acquire,
    ).unwrap_or_else(|published| published);
    assert!(published == u32::MAX || published == queue.slot());
    if !main.is_enabled() {
        return Ok(());
    }
    if main.config.use_private_rx_mqs && !main.config.no_adaptive {
        worker.enable_adaptive_mode(runtime)?;
    }
    runtime.nodes().set_node_state(input, NodeState::Interrupt)
        .expect("installed Session input accepts Interrupt state");
    runtime.nodes().set_node_state(queue, NodeState::Polling)
        .expect("installed Session queue accepts Polling state");
    Ok(())
}

// VPP session_node.c:2033-2165. 保持现有 Node trait 签名与 BufferIndex frame 语义。
impl Node for SessionQueueNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let main = SessionMain::global()
            .expect("Session Main precedes Session queue execution");
        // VPP session_node.c:2044-2050: cache Session time first, then let
        // registered transports advance their own worker timers once.
        let now = main.now();
        let worker = unsafe { main.worker_mut(runtime) }
            .expect("Session queue runs on its owning Data Worker");
        worker.update_time(now);
        worker.update_transport_time(runtime, main, now)
            .expect("registered transports update their own worker time");
        let queue = main.event_queue(worker.worker_index())
            .expect("Session worker retains its MQ");
        // VPP session_node.c:2061-2067: import only MQ items present at
        // entry; handler-produced items wait for another queue dispatch.
        worker.drain_event_queue(queue)
            .expect("Session MQ contains valid events");
        // VPP session_node.c:2069-2092: control before either IO list.
        worker.dispatch_control_events(runtime, main)
            .expect("registered Session control handlers accept their events");
        // VPP session_node.c:2094-2136: capture the old-list tail, then
        // process new IO before that old prefix, within the TX budget;
        // pending IO stays on the old list.
        worker.dispatch_io_events(runtime, main)
            .expect("registered Session IO dispatch accepts its events");
        // VPP session_input.c:350-393: app notifications belong to the
        // session-input node, not the timerfd callback.
        worker.schedule_pending_app_events(runtime);
        // VPP session_node.c:2146-2164: fan out BufferIndex/next pairs to
        // transport output edges before the adaptive decision.
        let packets = worker.flush_pending_tx_buffers(runtime, node, frame);
        worker.update_state(runtime);
        packets
    }
}
```

`register_session_input_node`/`register_session_queue_node` 在主 graph 构造时声明节点并
设置 Disabled；若 node 已存在，返回原 NodeId，不在重复注册时关闭运行中的 node。
`init_session_worker` 只在 owner Data Worker 启动阶段执行一次：用 runtime thread index
取预构造的 worker 槽，然后让该槽绑定本 worker 的两个 NodeId、按 enable/config
决定是否安装 timerfd，并设置 node 初态。它不是每轮执行的 callback，也不为
thread zero 创建一个数据面 timerfd。

timerfd 回调从 `File.private_data` 取初始化时保存的 NodeId，直接调用
`mark_interrupt_pending`，然后从 `file.fd()` 读走到期计数；它**不**每次执行
`node_by_name`。`WouldBlock` 不算失败。
`Interrupted` 重试，完整的 8 字节读取只消费到期次数，不按次数重复运行 queue；
其他读取失败携原始 `io::Error` 返回 worker main loop。FileMain 随后在同一轮 worker
main loop 中调度 interrupt node（`crates/hammer-runtime/src/main_loop.rs:202-224`）；
回调不借别的 worker、不处理 Session event、不获取 `SessionMain` 的可变引用。
queue node 执行顺序固定为：`update_time(now)`、transport `update_time` 订阅者、
MQ 导入、control/new/old IO、app notification 调度、TX fanout、adaptive
`update_state`。只有 queue 尾部选择 Polling/Interrupt/Idle；FileMain 不替它决策。
当前 `SessionQueueNode::process` 只调用 transport 更新时间，未更新 worker 自己的
`last_time/last_time_us`；接线时必须补上这一环。
`SessionInputNode::process` 仍按 VPP `session_input.c:350-393` 清空当前 worker 标记的
AppWorker 通知列：没有待发应用事件时结束，有剩余时重新标记自身 interrupt pending。
它不读 timerfd、不消费 SessionWorker MQ、不更新 transport 时间。当前
`dispatch_io_events` 只处理 new/old IO，并未处理 `control_events`；
`dispatch_control_events` 是接线时必须补齐的 service worker 操作，对齐 VPP
`session_node.c:2069-2092`。queue 仍负责
event 顺序、TX buffer fanout 和尾部 adaptive 判断，FileMain 不解释 Session event。

## Enable 与错误

`session_init` 创建固定 worker 槽和 MQ，node declaration 仍 Disabled；启动阶段的
`SessionMain::enable()` 只宣布目标状态，worker-init 才在 owner worker 上安装 timerfd 并
切换两个 node。当前 iperf3 的 enable 发生在 worker launch 之前，满足此顺序。
运行中 enable/disable 不能仅翻转 `AtomicBool`：VPP
`session.c:2209-2268` 同时修改每个 worker 的 node 状态。Hammer 当前没有可供 main thread
直接修改已启动 worker `NodeMain` 的安全借用；运行中切换必须先由 runtime owner 提供
worker-local graph 状态交付，再让各 worker 执行上述状态转换。在此前，不能声称当前
`SessionMain::enable/disable` 已实现 VPP 的运行中语义，也不能用跨线程裸指针补洞。

```rust
// VPP session_node.c:925-945,1585,2157：这些是 node 计数类别，不是 retval。
// 本版 VPP 未增加 Timer 计数；Hammer 不因每次 timerfd 到期自行增加它。
pub enum SessionQueueCounter {
    Tx,
    Timer,
    NoBuffer,
}

// VPP session_node.c:43-53,2180-2217 的 OS/File 错误边界；
// 这里只摘录现有 NodeMissing 与本 ADR 提议增加的 timerfd 变体，
// SessionQueueError 其余现有变体保持不变。保留原始 source，
// 不使用数字错误或 catch-all 文本错误。
#[hammer_component_macros::runtime_error(subsystem = "session queue")]
#[derive(Debug, Error)]
pub enum SessionQueueError {
    NodeMissing,
    TimerCreate {
        worker: u32,
        #[source]
        source: std::io::Error,
    },
    TimerDuplicate {
        worker: u32,
        #[source]
        source: std::io::Error,
    },
    TimerRegistration {
        worker: u32,
        #[source]
        source: RuntimeError,
    },
    TimerArm {
        worker: u32,
        #[source]
        source: std::io::Error,
    },
    TimerRead {
        worker: u32,
        #[source]
        source: std::io::Error,
    },
}
```

创建/复制/注册失败是启动失败，保持 node Disabled；FileMain 的 poll 失败使用既有
`RuntimeError`，timerfd read 失败由回调一次翻译为 `TimerRead` 上送 worker main loop。
运行中 `timerfd_settime` 失败时，保留原始错误、退出 adaptive 并将本 worker queue 留在
Polling，保证不会因 timerfd 停摆而丢失调度；这对应
VPP 的 warning 边界，但 Rust 不把可恢复的 OS 失败伪装成 Session packet error。
`WouldBlock` 是非阻塞读取的正常结果；不计为错误。没有新 `SessionError`、数字 retval
或 TCP 错误映射。

## 同步与 inline

- `SessionWorker` 的业务状态、flags、`OwnedFd` 与 FileMain 注册索引由其 Data Worker 独占；
  `DataPlaneMain.max_internal_frame_vectors` 也只由该 worker 写读；不为这些值加锁或
  `volatile`。每 worker 唯一调度状态为 `AtomicU8` 枚举：owner 在 timer/graph 状态转换时
  Release 发布，MQ producer 在成功提交后 Acquire 读取；
  VPP 的 `volatile pool_realloc_*` 是另一条池重分配协调路径，不是 timerfd 的发布规则。
- FileMain 已用自身的短临界区保护注册表，poller 属于选定 worker；service 不为
  timer 路径增加 `SpinLock`、`RwLock`、`ArcSwap` 或跨 worker timer 表。RPC 请求 pool
  使用固定容量 `SpinLock`，持锁只插入/取出记录，绝不执行 callback 或等待 MQ。
  worker fd 和 FileMain fd 仅通过
  `OwnedFd::try_clone` 共享同一内核 timerfd；可读回调与 queue node 在同一 worker
  顺序执行，不需要第二个同步协议。
- `SessionMain.is_enabled` 的现有 `AtomicBool` 只发布 enable 意图，Release/Acquire
  不能代替 node 状态发布。运行中 graph 状态交付须遵守 runtime 的 WorkerBarrier 与
  worker-local `NodeMain` 所有权；不得以 `volatile` 或 atomic pointer 直接修改 graph。
- 对齐 VPP 的 `always_inline`：只给 `timeout`、`update_time`、Session worker 简单访问器
  `#[inline(always)]`；VPP `static inline session_wrk_set_state` 对应 `#[inline]`。
  queue dispatch、FileMain callback、timerfd 注册和 lifecycle hook 不强制 inline。

## 实施边界

本次删除 service 的无消费者 timer wheel 和整数 timerfd 哨兵，改为 worker-owned
`OwnedFd` 与 FileMain 的注册索引，并在 service 完成 node 注册、worker-init、timerfd
与 queue 时间/状态接线。TCP 原有 timer wheel
及其 `Transport::update_time` 订阅保持原位。`DataPlaneMain` 的每轮 internal frame
最大 vectors 指标已在 runtime 接入；运行中 enable 的 worker-local graph 状态交付
仍是后续工作。不以额外
Session node、专属轮询线程
或私有调度器绕开它们。后续验证应覆盖默认 disabled、普通 enabled 不创建 timerfd、
adaptive 的 Polling/Interrupt/Idle 转换、无流量 timer 唤醒、timer 注册失败和 TCP
无报文时仍收到 transport time 更新。按本次用户要求未执行编译、测试或静态检查。
