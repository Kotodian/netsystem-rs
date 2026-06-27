# Plan — app/session 边界全部对齐 VPP(放弃 io_uring,跨进程 + 减少 syscall)

## Overview

放弃 io_uring 风格的 `AppRingHandle`(SQE/CQE 描述符 ring + `AppDataArea` chunk arena),把 hammer 的 app/session 边界全部对齐 VPP host stack 模型:
- **数据平面** = per-session `svm_fifo` 字节 ring(rx_fifo + tx_fifo),不是描述符 ring。
- **控制/通知平面** = `svm_msg_q` 事件队列(SESSION_EVT_* IO/ctrl 事件),不是 SQE/CQE。
- **跨进程** = shared memory segment(memfd/ssvm)承载 fifo + msg_q,app 和 VPP session layer 共享映射。
- **减少 syscall** = eventfd/semaphore 仅在 FIFO 状态翻转(空→非空、满→非满)时通知,批量 + 边沿触发,不 per-byte syscall。

这取代原 io_uring 优化计划(期1-4),是对 app/session 边界的架构重写。

## VPP 模型(调研结论,作为对齐基准)

**数据(svm_fifo)**:`svm_fifo_t` = 连续字节 ring(head/tail atomic,chunk 链可 grow),`svm_fifo_enqueue`(写 tail)、`svm_fifo_peek`/`svm_fifo_dequeue`(读 head)、`svm_fifo_dequeue_drop`(ACK 丢字节)。per-session `rx_fifo`(VPP→app)+ `tx_fifo`(app→VPP)。

**通知(svm_msg_q)**:`svm_msg_q_t` = shared memory 消息队列,承 `app_session_evt_t`(SESSION_EVT_ENQ/DEQ/CONNECT/CLOSE...)。VPP `app_worker_send_event` / `app_send_io_evt_to_vpp` 发 IO 事件;app poll 自己的 event queue。`svm_fifo_set_event` 标记 FIFO 有待通知,避免重复唤醒。

**跨进程**:fifo + msg_q 在 `fifo_segment_t`(ssvm / memfd backed)内,app attach 时映射。VCL(VPP Comms Library)封装 epoll/select/poll 给 app。

**syscall 减少**:msg_q 用 eventfd 或 mutex-condvar 信号;只在 FIFO 空→非空(生产者唤醒消费者)等翻转点发信号;`svm_fifo_set_event` 去重;批量 enqueue/dequeue 一次信号。

## 现状(要删/改的 io_uring 设计)

- `crates/hammer-runtime/src/app/ring.rs`:`AppRingHandle`、`AppSqeDescriptor`、`AppCqeDescriptor`、SQ/CQ/fill ring、`AppRingWaker` —— 全删,换 VPP fifo + msg_q。
- `crates/hammer-runtime/src/app/data.rs`:`AppDataArea`(chunk free-list arena)、`AppDataAddr`(chunk reference)、`AppDataChunk` —— 全删,换 `SvmFifo`(per-session 字节 ring)。
- `crates/hammer-runtime/src/app/context.rs`:`AppContext`、`APP_WORKER_RINGS` TLS、`CURRENT_APP_CONTEXT` TLS —— 重写为 VPP `application`/`app_worker` 模型。
- `crates/hammer-runtime/src/app/layout.rs`:`AppRingLayout`(SQ/CQ/fill/data offset)—— 换 `FifoSegmentLayout`(rx_fifo/tx_fifo/msg_q offset)。
- `crates/hammer-infra/src/ring.rs`:`LockFreeRing` MPMC + cacheline(期2/3)—— **保留作为通用 ring 原语**,`SvmFifo`/`SvmMsgQ` 可基于它或独立实现(VPP svm_fifo 是变长 byte ring,不复用 slot-based `LockFreeRing<u8>`;但 `LockFreeRing` 仍给其它 slot-based 用)。
- `crates/hammer-service/src/session/app.rs`:`SessionAppRuntime`(消费 SQE、`copy_send_into_session_chain`)—— 重写为从 `tx_fifo` peek 字节 + 处理 msg_q 事件。
- `crates/hammer-service/src/session/runtime.rs`:`SessionDriverRuntime` 的 app 交互、`dispatch_session_queue_pending`(#3 拷贝)—— 重写为 VPP session path(fifo peek → output buffer 1 次拷贝)。
- 期1 的 `ctor_bind` TLS、`AppContextGuard` —— 删,换 VPP app_worker attach 机制。
- 期2 MPMC/batch、期3 cacheline —— 部分复用(`SvmFifo` head/tail 用 cacheline + 必要时 batch),其余随 io_uring 设计一起删。

## 跨进程 + syscall 减少约束(一等设计目标)

### 跨进程
- `SvmFifo` / `SvmMsgQ` 存储在 shared memory segment(memfd-backed,可跨进程映射)。Rust 侧用 `mmap` + `Slice` over mapped region;所有状态用 atomic / offset(不用指针,跨进程地址不同)。
- segment 布局:`FifoSegmentLayout`(rx_fifo offset + capacity、tx_fifo offset + capacity、msg_q offset + capacity),VPP `fifo_segment_t` 等价。
- app attach 时映射 segment,拿 `SvmFifo` handle(offset 引用,非指针)。
- 进程内 fast path(本期先做):`SvmFifo` 可在堆上(单进程),但 API 设计为 offset-based,未来换 mmap 无需改 caller。
- generation 计数(fifo wrap 检测,跨进程共享)。

### syscall 减少
- FIFO 通知:edge-triggered,只在 空→非空(消费者待唤醒)时发信号。`svm_fifo_set_event`/`svm_fifo_unset_event` 等价:生产者写前检查 consumer 是否 sleeping,是则写后发信号;否则不发(消费者在 poll)。
- 信号机制:eventfd(单 syscall read/write 8 bytes)或 futex/semaphore。批量 enqueue/dequeue 末尾最多 1 次信号。
- app poll:1 次 `poll`/`epoll_wait`(或 eventfd read)阻塞等事件,被唤醒后批量处理 msg_q 所有事件 + fifo 所有可读字节。不 per-byte syscall。
- VPP `svm_msg_q` 用 eventfd 或 mutex-condvar;选 eventfd(跨进程 + 低开销)。
- batch:msg_q 事件批量 alloc/dequeue;fifo 字节批量 enqueue/peek/dequeue_drop(复用期2 batch 思路,但作用在 byte ring 上)。

## 任务(分阶段,每步可编译可测)

### 阶段 A — SvmFifo 字节 ring 原语(hammer-infra)

- 新 `crates/hammer-infra/src/svm_fifo.rs`:`SvmFifo` = 连续字节 ring(`head`/`tail` AtomicU32 + `cursors` CachePadded + `data: Slice<u8>` 或 mmap offset)。
  - `with_capacity(bytes)`(power-of-two)、`enqueue(&self, src: &[u8]) -> usize`、`peek(&self, offset, len, dst: &mut [u8]) -> usize`、`dequeue_drop(&self, len)`、`max_dequeue`/`max_enqueue`、`set_event`/`unset_event`/`needs_notification`。
  - SPSC(单 session:app 写 tx + VPP 读 tx;VPP 写 rx + app 读 rx —— 每方向 SPSC)。atomic head/tail。
  - chunk 链 grow:本期先固定容量,grow 后续。
  - offset-based API(跨进程 ready)。
- 验证:单元测试 enqueue/peek/dequeue_drop + wrap;SPSC 并发(单写单读)无丢失。

### 阶段 B — SvmMsgQ 事件队列(hammer-infra)

- 新 `crates/hammer-infra/src/svm_msg_q.rs`:`SvmMsgQ` = shared memory 消息 ring(固定大小 `AppSessionEvt` slot)+ eventfd 信号。
  - `alloc_evt`/`enqueue_evt`/`dequeue_evt` 批量;`signal`/`poll`(eventfd read)。
  - 事件类型:`SessionEvt { session_index, evt_type }`(SESSION_EVT_ENQ/DEQ/CONNECT/CLOSE...)。
  - eventfd 跨进程(给 app fd)。
- 验证:enqueue/dequeue 批量;eventfd signal/wake。

### 阶段 C — App/Session 边界重写(hammer-runtime + hammer-service)

- 删 `app/ring.rs`、`app/data.rs` 的 io_uring 类型(`AppRingHandle`/`AppSqeDescriptor`/`AppCqeDescriptor`/`AppDataArea`/`AppDataAddr`/`AppDataChunk`)。
- 新 `app/session.rs`(VPP `app_session` 等价):per-session `rx_fifo: SvmFifo` + `tx_fifo: SvmFifo` + `vpp_evt_q: SvmMsgQ`。
- 新 `app/application.rs`(VPP `application`/`app_worker`):app attach、worker、segment 映射(本期进程内,API 跨进程 ready)。
- `app/context.rs` 重写:`AppContext` → app_worker handle,不再 TLS ring。
- `app/layout.rs` 重写:`FifoSegmentLayout`(rx/tx/msg_q offset)。
- `SessionAppRuntime` 重写:从 `tx_fifo` peek 字节(= VPP session path),处理 `vpp_evt_q` 事件,发 IO 事件给 app(`app_worker_send_event` 等价)。
- `SessionDriverRuntime::new` 构造时拿 app_worker 的 per-session fifo(替代期1 ctor_bind TLS)。

### 阶段 D — send 路径对齐 VPP(1 次拷贝)

- `dispatch_session_queue_pending`:从 `tx_fifo.peek(offset, len, output_buffer.data)` 1 次拷贝(= VPP `tcp_prepare_segment`)。消除 #2(无 arena→chain)+ #3(无 chain→output)。
- `prepare_tx`/`tcp_output` 不变(TcpSegment constructor + prepend 头)。
- ACK:`tx_fifo.dequeue_drop(acked_len)`。
- retransmit:从 `tx_fifo` re-peek。
- send 路径 payload memcpy:1 次(fifo→output buffer)。

### 阶段 E — RX 路径对齐 VPP

- transport 收数据 → `rx_fifo.enqueue` → 发 SESSION_EVT_ENQ 给 app `vpp_evt_q`。
- app poll `vpp_evt_q` → `rx_fifo.dequeue` 读字节。
- edge-triggered 通知(空→非空才信号)。

### 阶段 F — 跨进程 segment(后续,本期 stub)

- `FifoSegment` = memfd-backed shared memory region,承载多个 `SvmFifo` + `SvmMsgQ`。
- app attach 映射 segment,拿 offset-based handle。
- 本期:进程内堆分配,但 API offset-based 不用指针,未来换 mmap 零改 caller。
- 跨进程完整实现作为后续一期(标记 TODO)。

### 阶段 G — 测试 + 清理

- 集成测试:send 1 次拷贝、ACK drop、retransmit re-peek、RX enqueue+事件、msg_q 通知、edge-triggered 不重复唤醒。
- 删期1-3 的 io_uring 测试(`session_app_ring_production_bind`、app_ring batch、MPMC concurrency、cacheline slot 等)—— 改测新 SvmFifo/SvmMsgQ。
- `cargo test --workspace`(pre-existing TCP/graph_registry 失败除外)PASS。
- `cargo fmt --all` clean。

## 关键文件(改/新/删)

- 新:`crates/hammer-infra/src/svm_fifo.rs`、`crates/hammer-infra/src/svm_msg_q.rs`
- 新:`crates/hammer-runtime/src/app/session.rs`、`crates/hammer-runtime/src/app/application.rs`
- 重写:`crates/hammer-runtime/src/app/context.rs`、`crates/hammer-runtime/src/app/layout.rs`、`crates/hammer-service/src/session/app.rs`、`crates/hammer-service/src/session/runtime.rs`
- 删:`crates/hammer-runtime/src/app/ring.rs`、`crates/hammer-runtime/src/app/data.rs`(内容删,文件可重命名/重用)
- 保留:`crates/hammer-infra/src/ring.rs`(`LockFreeRing` 通用原语,给其它用)、`TcpSegment`/`tcp_output`(不动)

## 跨期约束(AGENTS.md)

- VPP 对齐:`svm_fifo` + `svm_msg_q` 模型,不发明 io_uring 变体。
- 跨进程:offset-based,atomic,不用指针;本期进程内但 API 跨进程 ready。
- syscall 减少:edge-triggered eventfd 通知,批量,不 per-byte。
- `TcpSegment` 仍 constructor + `write_to_buffer`;TCP output prepend 头;session 持 TX 字节(fifo);ACK drop;retransmit re-peek。
- 不引入 TCP-specific runtime/buffer API;recovery 记录私有,只存 offset/len。
- 不用 `_value` 命名;复用 `hammer-infra::Slice`/`align`/`CachePadded`。
- 非平凡新类型(`SvmFifo`/`SvmMsgQ`)是 VPP 对齐必需的通用 infra 原语(等价 VPP svm_fifo/svm_msg_q),用户已明确要 VPP 对齐,视为已审批方向;具体 API 在实现时按 VPP 接口镜像。

## 验收

- `cargo build --workspace` PASS。
- `cargo test --workspace`(pre-existing 失败除外)PASS。
- send 路径 1 次 payload memcpy(fifo→output buffer)。
- RX 路径:enqueue + edge-triggered 事件通知,1 次 syscall 唤醒(不 per-byte)。
- 跨进程:API offset-based,无指针依赖;完整 mmap segment 留后续。
- `cargo fmt --all` clean。

## 注意

- 本期最大重构:删 io_uring ring + arena,换 VPP fifo + msg_q。波及 app/session/transport 边界全线。
- 分阶段(A→G)每步可编译可测;先 infra 原语,再边界重写,再 send/RX,再跨进程 stub,再测试清理。
- 期1-3 已提交的 io_uring 工作将被删除/取代 —— 这是用户明确要的方向转变。
- 3 pre-existing TCP 失败 + graph_registry.rs stale 仍 out of scope。
