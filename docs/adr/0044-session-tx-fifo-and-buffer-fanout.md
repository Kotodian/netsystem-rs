# ADR-0044: Session TX FIFO、Buffer 链与输出扇出

- 日期：2026-09-28
- 状态：Proposed；仅设计，未修改实现
- 前置：ADR-0037、ADR-0038、ADR-0039、ADR-0042、ADR-0043
- 取代：上述 ADR 中关于 `TxMode`/`TransportTxMode`、单包 TX、TX 参数快照、
  `SessionTxPacket` 和 TX output next 的设计；其非 TX 部分不变
- 范围：service 的 Session TX 与 queue 调度、plugin-session 的 IP Session 类型和
  output 注册、具体 transport 的 `Transport` trait 实现

## 源码依据与事实

以下行号均指本仓库 `third_party/vpp/`。VPP 是行为和所有权依据，Hammer 不引入 VFT。

| VPP 源码 | 必须保留的语义 |
|---|---|
| `src/vnet/session/transport_types.h:14-23`；`src/vnet/session/session.c:1819-1847`；`src/vnet/session/session_types.h:293-315` | VPP **确有** `TRANSPORT_TX_PEEK/DEQUEUE/INTERNAL/DGRAM`，但它们在 transport 注册时选择 `session_tx_fns[session_type]`；一个 transport protocol 对应两个 IP family 的 Session type，而不是注册两次产生两个 protocol id。DGRAM 选择 dequeue 函数，函数内部才有 datagram 分支。 |
| `src/vnet/session/session.h:22-43,82-170,210-225` | 每 worker 有 cacheline 分隔的 TX context、可重用 buffer 向量、pending buffer/next 平行向量、可选 DMA ring；`wrk->vm` 指向所属 dataplane main；Main 按 Session 类型保存 TX 入口和 output next。 |
| `src/vnet/session/session_node.c:923-973,1858-1910,2033-2161` | queue 以 `128` packet frame 预算处理 control、新 IO、旧 IO；TX/FLUSH 由 Session 类型分发；pending packet 在一次 queue trace 后集中 flush；本轮开始时已 pending 的 buffer 先占预算。 |
| `src/vnet/session/session_node.c:976-1071` | 普通路径从 FIFO `peek` 到目标 Data Plane Buffer；仅启用 DMA 时借 FIFO segments 安排传输。首 buffer 与链尾分别处理。 |
| `src/vnet/session/session_node.c:1073-1237` | 首 buffer 预留 `TRANSPORT_MAX_HDRS_LEN`；剩余 payload 链入后续 buffer，设置 `NEXT_PRESENT`、`next_buffer`、`TOTAL_LENGTH_VALID` 与 tail 长度。Peek 只推进本轮 `tx_offset`，dequeue 消费 FIFO；datagram 则按 header 的 `data_offset` 更新并在完整消费后丢弃整个 record。 |
| `src/vnet/session/session_node.c:1239-1435` | `session_tx_not_ready`、connection/listener 选择、MSS/发送窗口/可读长度、datagram 完整性/GSO、每段和每首 buffer 容量、全部 buffer 数量在分配前计算。 |
| `src/vnet/session/session_node.c:1437-1468` | 发送完成后先清 FIFO event，再重新检查可读字节并有条件重设 event；无可读字节才 deschedule。普通和 DMA pending buffer 分流。 |
| `src/vnet/session/session_node.c:1472-1677` | `TX_FLUSH` 先调用 transport `flush_data`；custom TX 先使用本轮 burst；再取得即时 send params、应用 pacer、一次分配整批 buffer、成批填充、一次调用 `push_header`、再放入 pending 向量。`n_left >= 4` 循环每轮预取后处理 **两个** packet，其后处理尾数；不能误写成每轮四包。 |
| `src/vnet/session/session_node.c:1680-1747` | peek/dequeue 是同一 FIFO algorithm 的两个入口；internal 是独立 custom TX 路径，具有自己的 burst、重排、FIFO event 与 dequeue 通知规则。 |
| `src/vnet/session/session_node.c:1965-1973,2132-2159`；`src/vnet/session/session.h:1093-1115` | worker 的 pending buffer/next 成对扇出，随后重用向量；transport 直接提交的 control packet 也进入同一 pending 列。DMA 完成前不得扇出未写完的 payload。 |
| `src/vnet/session/session.c:108-141,172-202,657-680,730-749,2119-2194` | TX event、custom TX 优先级、`session_tx_fifo_peek_bytes`/`session_tx_fifo_dequeue_drop`、应用 dequeue 通知及 DMA completion 是不同操作；DMA 配置失败时该 worker 走非 DMA 路径。 |
| `src/vnet/session/transport.h:21-55,75-90,204-208,330-369`；`src/vnet/tcp/tcp.c:1171-1196,1372-1380,1402-1431` | transport 实时给出 `snd_space/tx_offset/snd_mss/flags`，TX_FLUSH 通知 TCP 的 PSH 逻辑，pacer 在 Session TX 前后由 connection 更新。 |
| `src/vnet/tcp/tcp.c:1128-1196`；`src/vnet/tcp/tcp_output.c:1804-2065,2070-2186` | TCP 普通 FIFO TX 在 recovery 中停止；custom TX 除重传和 ACK 外，还会按 recovery/PRR 预算直接从同一 Session TX FIFO packetize 未发送的新数据。其 packet 计数用于 frame 预算，ACK buffer 分配失败不改变 `tcp_send_acks` 的计数。 |
| `src/vnet/tcp/tcp_output.c:884-1035`；`src/vnet/tcp/tcp.c:1590-1630,1743-1749` | TCP `push_header` 对整个 packet 批次更新 sequence、拥塞和重传 timer；TCP 注册 4/6 两个 output edge，worker 缓存对应 next，以便 ACK、重传和 timer control packet 进入同一 Session pending 列。 |
| `src/vnet/session/transport.h:81-88,183-191`；`src/vnet/session/session_node.c:1496-1504,1884-1893` | `flush_data` 只在 TX_FLUSH 分支、普通 TX 参数计算之前调用；RX IO event 调用 transport 的 `app_rx_evt`，它不是 AppWorker RX callback。未提供 RX callback 的 transport 在 VPP 是成功的 no-op。 |
| `src/vnet/session/session.h:917-975`；`src/vnet/tcp/tcp.c:1372-1401` | TCP flush 从 Session TX FIFO 的 consumer 可读量确定 `psh_seq`；App RX 只在曾发送零窗口时检查 RX FIFO 空间，阈值为 `clamp(size >> 3, 4 KiB, 128 KiB)`，不足时请求 dequeue 通知，否则发送 ACK。 |
| `src/vnet/tcp/tcp_output.c:917-930,1037-1058`；`src/vnet/tcp/tcp_types.h:115-125,490-499` | PSH 仅写入覆盖 `psh_seq` 的 TCP 段；独立 ACK 使用 Session 的 pending buffer/next，不构造应用通知或新的 Session TX event。ACK buffer 分配失败时 VPP 更新接收窗口并计 TCP worker `no_buffer`，不是 Session Queue node error。 |
| `src/vnet/tcp/tcp_input.c:494-545,885-895,1407-1412,2365-2370`；`src/vnet/session/session.c:738-749` | ACK 不逐包直接 drop Session TX FIFO：TCP worker 先合并 `burst_acked` 到 `pending_deq_acked`，输入 burst 结束后每 connection 只 drop 一次，触发应用 dequeue 通知，再重排、更新重传 timer 和 pacer。 |
| `src/vnet/session/session_types.h:495-517`；`src/vnet/session/session_node.c:1316-1383` | datagram 前缀为长度和偏移，其后是具体连接 metadata 与 GSO 大小；`ip46_address_t` 是 IP 实例，不属于通用 service。 |
| `src/svm/svm_fifo.h:70-108,467-475,690-718` | consumer 自己的 head 可 relaxed 读，producer 的 tail 必须 acquire 读；`set_event` 是 release 交换，`unset_event` 是 acquire 交换，随后必须重检 FIFO 再决定重排。 |

现状核对：`crates/hammer-service/src/session/core.rs` 的 `tx_fifo_peek_and_send` 每次只分配一个
buffer，预留 `60` 字节，按一个 MSS 拷贝并直接返回；没有 VPP 的整批 buffer 预算、链尾、
`n_left >= 4` 循环、pacer/custom TX/通知完整时序。`SessionTxContext.tx_mode`、
`SessionMain.session_tx_modes` 和 `TransportOptions.tx_mode` 是把 VPP 注册选择误做成了运行时状态。
当前 `SessionDmaTransfer` 只有两个标量，VPP 对应的是两条 pending 向量。TCP 注册 output next
为 `u32::MAX`，TX callback 自己按 IP family 选择 `tco_next_node`；目标应是注册期按 Session 类型
建立 queue 的 output edge，不让 service 解释 IP。TCP 的 ACK 路径目前直接调用 FIFO
`drop_dequeue`，漏掉 VPP `session_tx_fifo_dequeue_drop` 所执行的应用 dequeue 通知。
当前 service 还把 `Session.session_type` 直接当 transport protocol，并由每次
`register_transport_type` 分配一个新 protocol id；若直接按 IPv4/IPv6 调用两次，
同一个 TCP 会变成两个 protocol，RX/control/time 与 TX 注册无法对应。
以上均是迁移清单，**本 ADR 不改代码**。

## 分层决定

`hammer-service::session` 拥有 queue 的 128 packet 预算、Session state、FIFO、TX context、
buffer 链、pending/next 向量、FIFO event 重排与 dequeue 通知。它不定义 IP endpoint、TCP
sequence、TCP header 或具体 transport connection。`hammer-service::transport::Transport`
仍是 trait；`send_params`、`flush_data`、`custom_tx` 和批量 `push_header` 由具体插件实现。

`hammer-plugin-session` 为 TCP 的 IPv4/IPv6 等具体 Session 类型配置 output edge，并把每种
Session 类型的**单态化 TX 入口**注册给 service。同一 TCP protocol id 只分配一次，
plugin-session 为两个 IP family 给出不同的 opaque Session type；service 的 Session
分别保存 `session_type`（TX 函数/output next 索引）和 `transport_protocol`
（RX/control/time 入口索引），不解释 IP family。注册点选择 peek、dequeue、datagram 或
internal 的实际入口，不保存 `TransportTxMode`、其别名或改名后的等价运行时 mode。
`IpSessionFamily`、datagram 的 IP metadata 及 IP 地址处理只在 plugin-session。
TCP/UDP 依赖 plugin-session，TCP 以 peek 入口接到它实现的 `Transport`；未来 UDP 以
datagram 入口接入。两者的 `push_header`、`custom_tx` 仍由各自插件拥有。现有获批的按
protocol 单态化函数入口仅是 service 跨插件分发的边界，不是 transport VFT，也不在
每个 packet 上查找 `dyn` 对象。

现有 `SessionIoDispatch` 对 TX/RX 合并返回 `(packets, pending)`，不足以表示 VPP 的
head-old、tail-old、deschedule 和 no-buffer 四种处置。目标把 TX 入口与既有 RX 入口
分开；TX 入口由 service 的事件调度持有 event index，具体 transport callback 只取得
本 worker 的 Session/connection 并调用 service 的批量 FIFO 方法。`SessionTxPacket`、
per-Session send-params snapshot、每次 TX 的 `tx_mode` 和 transport 私有 payload copy
均不进入目标路径。

## 类型和注册

以下 Rust 块是**目标签名/执行契约**，不是当前实现；引用标在类型、方法旁。
复用已有 `CacheLineAlignMark`、`SvmFifo`、`Pool`、`BufferIndex`、`SessionHandle`、
`TransportSendParams`、`SessionEventType` 和 `Transport`。不新增 TX transport VFT。

```rust
// VPP session.h:22-43. Worker-owned scratch; no transport pointer or IP metadata.
#[repr(C)]
pub struct SessionTxContext {
    cacheline0: CacheLineAlignMark,
    session: Option<SessionHandle>,
    send_params: TransportSendParams,
    max_dequeue: u32,
    left_to_send: u32,
    max_len_to_send: u32,
    dequeue_per_first_buffer: u16,
    dequeue_per_buffer: u16,
    segments_per_event: u16,
    buffers_needed: u16,
    buffers_per_segment: u8,
    cacheline1: CacheLineAlignMark,
    tx_buffers: Vec<u32>,
    transport_pending_buffers: Vec<u32>,
}

// VPP session.h:59-69,139-155; session.c:2119-2134.
pub struct SessionDmaTransfer {
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
}

// These are TX outcomes, not SessionError values or numeric retval.
// VPP session_node.c:943-949,1472-1677.
pub enum SessionTxOutcome {
    Ok,
    NoData,
    NoBuffers,
}

// VPP session_node.c:1239-1273 returns three readiness decisions.
// Rust represents the decisions, never VPP's numeric return values.
enum SessionTxReadiness {
    Ready,
    Defer,
    Discard,
}

// VPP session_types.h:207-225; session.c:172-202.
// One added field in the existing SessionFlags, not a second flags type.
pub struct SessionFlags {
    // ... existing flags
    pub custom_tx: bool,
}

// VPP session_types.h:240-247,293-315. Existing Session fields only:
// IP family is encoded by plugin-session in the opaque session_type;
// service stores the protocol separately instead of decoding IP bits.
pub struct Session {
    // ... existing fields
    session_type: u8,
    transport_protocol: u8,
}

impl SessionWorker {
    // VPP session_types.h:293-315; session_node.c:1870-1890.
    // plugin-session supplies both identities when allocating an IP Session.
    pub fn allocate(
        &mut self, state: SessionState, session_type: u8,
        transport_protocol: u8, opaque: u32,
    ) -> SessionHandle;
}

impl Session {
    // VPP session_types.h:311-315. No service-side IP bit decoding.
    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8;
}

// VPP session.h:82-170. Existing fixed per-worker record in SessionMain;
// only its TX fields are shown. No second worker state is introduced.
#[repr(C)]
pub struct SessionWorker {
    cacheline0: CacheLineAlignMark,
    // ... sessions, event queue, time, timerfd and event lists
    tx_context: SessionTxContext,
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
    dma_enabled: bool,
    dma_transfers: Vec<SessionDmaTransfer>,
    dma_head: u16,
    dma_tail: u16,
    // ... other existing worker state
}
```

`SessionTxContext` 的第一组是每次事件覆盖的计算值，第二组是复用的 buffer scratch；两个
cacheline mark 对齐 `session_tx_context_t` 的分组。`session` 在 TX event 开始时从
event 设为 `Some(handle)`，结束时清空；它对应 VPP 的 `ctx->s`，使下列 helper
无需各传一份 Session/FIFO/offset。具体 `tc` 仍由单态化 transport 入口借用，
不把 plugin-owned connection 引用、不安全裸指针或假 `'static` 生命周期放进
service。`SessionDmaTransfer` 的两个
向量只在 DMA 真正启用时按 worker 初始化；普通路径无需分配 1024 份空向量。

```rust
// VPP session.c:2019-2090,2119-2194; session.h:22-43,82-170.
impl Default for SessionTxContext {
    // session begins None; counters are zero; scratch Vecs begin empty.
    fn default() -> Self;
}

impl SessionWorker {
    // Existing worker constructor: builds the TX context and paired pending
    // Vecs before workers start; DMA transfer slots only when DMA is active.
    fn new(worker_index: u32, config: SessionConfig) -> Self;
}

impl SessionMain {
    // Existing global lifecycle; no second TX Main or per-event initializer.
    pub fn init(config: SessionConfig, worker_mq_segment: SvmFifoSegment)
        -> Result<(), SessionError>;

}
```

`SessionMain.workers` 仍存现有 `SessionWorker`，不新增另一个 worker 记录。
VPP `wrk->vm` 是 worker 生命周期内的指针；Hammer 的 graph node 同时独占借用
`&mut DataPlaneMain`，故这里保留显式 runtime 参数，不把节点临时借用存入全局
worker，也不为传参再引入类型。一个 node dispatch 内只取得一次 Session worker，
将这两个独立 owner 的借用传入 TX/transport 方法。来源：VPP
`session.h:82-170,341-354`、`session_node.c:2033-2044`；
Hammer `session/core.rs:248-308,2496-2512`、`session/node.rs:176-207`。

```rust
// VPP session.c:1819-1847; session_node.c:1858-1910.
// Existing SessionIoDispatch is narrowed to RX; TX uses one monomorphized
// entry per session_type. Output next is a registered graph edge, not MAX.
// The return is VPP's TX status; packets is the sole packet count output.
pub type SessionTxDispatch = fn(
    worker: &mut SessionWorker,
    runtime: &mut DataPlaneMain,
    node: &mut NodeRuntime,
    event_index: u32,
    packets: &mut usize,
) -> SessionTxOutcome;

impl SessionMain {
    // VPP session.c:1883-1921. Allocate one protocol id during tcp_init.
    pub fn register_transport_protocol(&self) -> Result<u8, SessionError>;

    // VPP session.c:1827-1847. Called by each TCP output graph init after
    // SessionQueueNode::compile_output_next returns the existing next slot.
    // Startup publication, or WorkerBarrier if graph changes while running.
    pub fn register_transport(
        &self, session_type: u8, output_next: SessionQueueNext,
        tx: SessionTxDispatch,
    ) -> Result<(), SessionQueueError>;
}

impl IpSessionMain {
    // VPP tcp.c:1743-1749; session.c:1827-1847. Map protocol + family to
    // the opaque Session type, then publish the already compiled output next.
    pub fn register_transport(
        &self, protocol: u8, family: IpSessionFamily,
        output_next: SessionQueueNext, tx: SessionTxDispatch,
    ) -> Result<(), SessionError>;
}
```

上述 `IpSessionMain` 是 IP 实例的编排签名，不让 service 接触 `IpSessionFamily`。它不重新
注册 Graph Node：`init_tcp` 先通过 service 分配**一个** transport protocol id、
构造 TCP Main 并注册 RX/control/time 入口。现有 TCP4/TCP6 output graph init
已经调用 `SessionQueueNode::compile_output_next`；各自拿到编译好的
`SessionQueueNext` 后直接调用 `IpSessionMain::register_transport` 发布该 family 的
Session type/TX 入口，无新的 bind init 或 main-loop-enter 回调。
现有 `tcp_worker_init` 仍在 graph materialization 后把同一对
`SessionQueueNext` 写入 `TcpWorker::tco_next_node`，给 ACK、重传、timer control
packet 使用；普通 FIFO TX 使用 service 按 Session type 注册的同一个 next。
依据：Hammer `tcp/src/output.rs:43-74`、`tcp/src/lib.rs:988-1026`；
VPP `tcp.c:1743-1749`、`session.c:1827-1847`。service 在 worker 启动前
（或运行期 barrier 内）发布两项 TX/next 映射；
已注册 protocol 的 RX/control/time
入口仍只占一个槽。TCP worker 的 `tco_next_node` 缓存这两个已注册 next，供
ACK、重传和 timer control packet 使用，普通 FIFO TX 只从 Session type 表取 next。
`SessionWorker::allocate`、listening/half-open 构造以及 plugin-session 的
accept/connect 路径必须一同写入两种身份；不能只改 `transport_protocol()` getter
而让现有 `session_type == protocol` 的值继续流进 TX 表。
`init_tcp` 分配 protocol 前验证容量；两个 output graph init 各自验证已编译 next，
worker init 验证两项已齐。后续重复映射是注册者 bug，
不是需要在热路径回滚的错误。`SessionTxDispatch` 利用已获批的单态化入口，替换当前
`SessionIoDispatch` 的 TX 分支，不新增每包模式分派或函数指针表套函数指针表。

## Transport 与 TCP 契约

现有 `Transport` 是 service 定义、具体插件实现的 trait，保持这个边界。下面只列
Session IO 涉及的目标签名；listen/connect/close 不重写。显式 `runtime`
对应 VPP `wrk->vm`，`connection_index` 解析插件持有的 connection；
`SessionWorker` 是 service 正在持有的**同一个** owner borrow。
VPP 的 `tcp_update_burst_snd_vars` 经 connection 回查 RX/TX FIFO，
`tcp_push_one_header` 也验证 TX FIFO 可读量；Rust 不能在此时从全局 Main
再次借用该 worker，因此需要 FIFO 和 Buffer 的方法接收当前 worker 与 runtime，
不从全局 Main 再取 worker。
不另拆 Session FIFO、PSH 参数或
AppWorker。来源：
`transport.h:75-90,183-224,307-369`、`session_node.c:1472-1677,1884-1893`、
`tcp_output.c:300-329,965-978`、`tcp.h:365-367`。

```rust
// VPP transport.h:87; session_node.c:1507-1516,1696-1724.
// C passes either transport_connection_t* or session_t* through void*.
// This enum is the single Rust custom-TX target, selected by the registered
// Session TX entry; it is not a TX mode or a per-packet allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTxTarget {
    Connection(u32),
    Session(SessionHandle),
}

pub trait Transport<E> {
    type Connection;

    // VPP transport.h:81-85; tcp.c:1171-1196. Fill the caller's current
    // TX parameters after any flush/custom TX work.
    fn send_params(
        &self, runtime: &DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        params: &mut TransportSendParams,
    );

    // VPP transport.h:86; session_node.c:1496-1504; tcp.c:1372-1380.
    // Optional transport hook; absent VPP callback means successful no-op.
    fn flush_data(&self, _: &SessionWorker, _: u32) {}

    // VPP transport.h:88,183-191; session_node.c:1884-1893;
    // tcp.c:1382-1401. A transport without RX behavior implements no-op.
    fn app_rx_event(&self, _: &mut DataPlaneMain, _: &mut SessionWorker, _: u32) {}

    // VPP transport.h:81-88; tcp_output.c:980-1035,2154-2186.
    fn push_header(
        &self, runtime: &mut DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        buffers: &[u32], available_bytes: u32,
    );
    fn custom_tx(
        &self, runtime: &mut DataPlaneMain, worker: &mut SessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) -> usize;

    // VPP transport.h:210-224,307-369. Current Transport::connection gives
    // only &Connection, so service cannot mutate these owner-worker facts.
    fn is_descheduled(&self, runtime: &DataPlaneMain, connection_index: u32) -> bool;
    fn deschedule(&self, runtime: &DataPlaneMain, connection_index: u32);
    fn clear_descheduled(&self, runtime: &DataPlaneMain, connection_index: u32);
    fn is_tx_paced(&self, runtime: &DataPlaneMain, connection_index: u32) -> bool;
    fn tx_pacer_burst(&self, runtime: &DataPlaneMain, connection_index: u32) -> u32;
    fn tx_pacer_update_bytes(&self, runtime: &DataPlaneMain, connection_index: u32, bytes: u32);
    fn tx_pacer_reset_bucket(&self, runtime: &DataPlaneMain, connection_index: u32, bucket: u32);
}
```

`custom_tx` 仍只有一个 trait 方法。VPP 用一个 `void *` 槽：普通 FIFO TX
传 `transport_connection_t *`，internal TX 传 `session_t *`。Rust 不把不同
对象重新擦成裸指针；注册为 peek 的 TCP 只接受 `Connection`，注册为 internal 的
transport 只接受 `Session`，错误 variant 是注册者内部不变量，不能当作可恢复
Session error。`TransportTxTarget` 只在 custom TX 调用点构造，不进入 Session
record 或 TX context。`TransportSendParams` 复用现有 service 类型，由 `send_params`
填写调用方本轮持有的 `&mut TransportSendParams`，不保留跨事件快照；没有新 callback
包装结构、VFT、数字 retval 或 `TxMode`。`send_params`、`push_header` 和
`custom_tx` 也必须接通：当前 TCP `custom_tx` 恒返回零，`push_header` 忽略
`available_bytes`，并不符合 VPP 的 retransmit/ACK、cwnd-limited 和 timer 路径。
TCP 的 `send_params` 先执行 `tcp_update_burst_snd_vars`（包括待发送 PSH 边界、
TCP options、有效 MSS 与本地接收窗口），再把拥塞窗口与对端公告窗口共同允许的发送空间、
`snd_nxt - snd_una` 的 peek offset、MSS 和零发送空间时的 deschedule 标志写入
本轮 `params`，保留调用方预先设置的 burst 上限；来源：
`tcp.c:1128-1196`、`tcp_output.c:300-329`。recovery/Closed 时普通
`snd_space=0`，新数据若可在 recovery 中发送，也由 TCP custom TX 按拥塞/PRR
预算从同一 Session TX FIFO 的 `snd_nxt - snd_una` offset 直接填最终 Buffer，
并与重传共用本轮预算；不能让普通 Session FIFO TX 再发送一遍。
custom TX 只在借用 FIFO 可读片段并把 payload 写入最终 Buffer 的短作用域内持有
Session 借用；该借用结束后才调用 `SessionWorker::add_pending_tx_buffer`，不将
Session FIFO 借用与 `&mut SessionWorker` 的 pending 列修改重叠。
当前 `TcpMain::send_params` 复用的 `TcpConnection::tx_payload_budget`
（`tcp/src/lib.rs:503-541`、`connection.rs:1428-1486`）同时执行了
`pacing_ready`/`next_send_delay`、Nagle 和 recovery 门控；这是旧 packetized
发送决策，不能搬入新 `send_params`。目标发送空间先按 VPP 的
`tcp_snd_space_inline` 与对端窗口计算，随后由 service Session TX **一次**
应用 `TransportConnection.pacer`；TCP recovery 的 pacing 只在其 custom TX
分支扣账。普通发送不得再经 `pacing_ready`、`tx_intent_sequence` 或第二套
逐包预算筛选。现有 Nagle 配置可留作旧接口迁移信息，但不能悄悄改变此条
VPP 对齐路径；若保留 Nagle 行为，须另行决定其独立语义，不能伪称来自
`tcp_session_send_params`。来源：`tcp.c:1100-1196`、
`session_node.c:1553-1570`、`tcp_output.c:1818-1826,1989-1997`。
TCP `custom_tx` 先处理 recovery 中待重传和允许的新数据，再按剩余预算处理待发
ACK/dupACK；预算耗尽时重新编排 ACK。返回值是 VPP 用于 frame 预算的 packet
**计数**，不是可靠的已分配 Buffer 数：`tcp_send_acks` 在 ACK buffer 分配失败时
仍可能计入尝试次数，真正的 output 只由 pending buffer 列决定。
来源：`tcp.c:1128-1164`、`tcp_output.c:1804-2065,2070-2186`。
`push_header` 遍历已经填好的**整个** buffer
批次，仅在首 buffer headroom 内写 TCP header/options，依据各 buffer 链的实际
payload 长度递增 `snd_nxt`；它用 `available_bytes` 计算 cwnd-limited 事实，
随后按 VPP 启动 RTT 采样及重传 timer，不复制 payload 或逐包调用 Session。
VPP 的 `tcp_session_push_header` 返回常量零，Session 调用方不读取该值；Rust
trait 因而返回 `()`，不把零伪装成错误或 packet 计数。
VPP 的 `vlib_buffer_t **` 批次数组仅被遍历，Hammer 用 `&[u32]` 传首 Buffer
index；TCP 经 `runtime` 修改实际 Buffer，不能借口 VPP 指针可写而要求
`&mut [u32]`，否则会与同时借入的 `&SessionWorker` 冲突。
来源：`session_node.c:1652-1655`、`tcp_output.c:884-1035`。

上述 deschedule/pacer 操作只修改具体插件 owner worker 中已有的
`TransportConnection.flags` 和通用 `Pacer`：`deschedule` 置位，
`clear_descheduled` 清位且在 pacing 启用时把 bucket 重置为零；burst 查询、
发送后扣字节、无可发 FIFO 数据时重置 bucket，依次对应
`transport.h:210-224,307-369` 与 `session_node.c:1530-1588`。当前
`Transport::connection` 只给不可变引用，不能用它假装完成这些写入；直接的
trait 方法由 transport owner 在本 worker 执行，不引入新的 flags snapshot 或锁。

```rust
// VPP tcp_types.h:115-125,460-465,490-499;
// tcp.c:1372-1380; tcp_input.c:494-545.
// Existing TcpConnectionCacheline0 in connection.rs: only these fields are
// added beside its current hot fields; its cacheline mark stays in place.
#[repr(C)]
pub struct TcpConnectionCacheline0 {
    cacheline0: CacheLineAlignMark,
    // ... existing sequence, window, timer, and flag fields unchanged
    pub(crate) psh_pending: bool,
    pub(crate) psh_sequence: TcpSeq,
    pub(crate) deq_pending: bool,
    pub(crate) burst_acked: u32,
    pub(crate) retransmit_pending: bool,
    pub(crate) send_ack_pending: bool,
    pub(crate) pending_dupacks: u32,
}
```

这是**已有** `TcpConnectionCacheline0` 的字段增量，不另建 cacheline 类型、状态类型或锁。
连接构造时 `psh_pending=false`、`psh_sequence=TcpSeq::from(0)`、
`deq_pending=false`、`burst_acked=0`、`retransmit_pending=false`、
`send_ack_pending=false`、`pending_dupacks=0`；只有 owner worker
写入，`CacheLineAlignMark` 保持原位置。第一次 TX_FLUSH
设置 pending，并以 `snd_una + tx_fifo.max_dequeue() - 1` 的 TCP 序号回绕运算保存
最后待 push 字节；已有 pending 时直接返回，不能每次 flush 重置边界。取 FIFO
可读量时通过当前 `&SessionWorker` 的 Session backlink 借用，不从全局 Main 二次借用。
`tcp_update_burst_snd_vars` 在 pending 时按当前可读量更新边界；
`push_header` 只在 `psh_sequence` 落入该段的 `[snd_nxt, snd_nxt + data_len)` 时置
PSH；它用 `available_bytes` 计算 `snd_nxt - snd_una + available_bytes` 并更新
cwnd-limited 判断，同时按 VPP 更新发送序号、RTT 采样和重传 timer。
不能继续让 `flush_data` 为空、或让所有数据段默认置 PSH。
来源：`tcp.c:1372-1380`、`tcp_output.c:316-329,917-1035`、
`tcp_types.h:115-125,490-499`。

TCP 不新增发送状态容器。`TcpMain` 仍持有固定 worker 槽；`TcpWorker` 仍持有
connection pool、cached options、临时 buffer index、ACK/dequeue/cleanup 队列、
timer wheel 和 `tco_next_node`。下列代码只展示已有记录的 TX 相关字段及需补齐的
方法，省略其余原有字段；`cacheline0/1/2` 位置不移动，`runtime` 不存进 TCP
worker。来源：VPP `tcp.h:76-122`、`tcp_output.c:300-329,1037-1058,1123-1270,
1672-1708,1804-2186`；Hammer `tcp/src/worker.rs:14-90`、
`tcp/src/lib.rs:139-178`。

```rust
// VPP tcp.h:76-122; tcp.c:1361-1401; tcp_output.c:1037-1058.
#[repr(C)]
pub struct TcpWorker {
    cacheline0: CacheLineAlignMark,
    connections: Pool<TcpConnection>,
    pending_deq_acked: Vec<u32>,
    pending_disconnects: Vec<u32>,
    pending_resets: Vec<u32>,
    time_us: f64,
    time_tstamp: u32,
    max_timers_per_loop: u32,
    pending_timers: FifoQueue<TcpTimerToken>,
    cacheline1: CacheLineAlignMark,
    pub(super) cached_opts: [u8; 40],
    tx_buffers: Vec<u32>,
    pending_cleanups: FifoQueue<TcpCleanupRequest>,
    tco_next_node: [SessionQueueNext; 2],
    timer_wheel: TimerWheel1t2w2048sl<u32>,
    // ... existing timer bookkeeping, lookup and protocol
    cacheline2: CacheLineAlignMark,
}

impl TcpConnection {
    // VPP tcp.c:1100-1164. Recovery/Closed returns zero for *ordinary*
    // Session FIFO TX; round the available CC space by current snd_mss.
    #[inline]
    pub(crate) fn send_space(&self) -> u32;

    // VPP tcp_output.c:300-329. Recompute SACK/timestamp options,
    // effective MSS and receive window; write bytes to the existing
    // TcpWorker.cached_opts. If PSH is pending, refresh its FIFO boundary.
    pub(crate) fn update_burst_send_vars(
        &mut self, tx_fifo: &SvmFifo, cached_opts: &mut [u8; 40],
    );

    // VPP tcp.c:1372-1380. Repeated TX_FLUSH does not move the boundary.
    pub(crate) fn flush_data(&mut self, tx_fifo: &SvmFifo) {
        if self.psh_pending {
            return;
        }
        self.psh_pending = true;
        self.psh_sequence = self.snd_una.advance((tx_fifo.max_dequeue() as u32).wrapping_sub(1));
    }

    // VPP tcp_output.c:884-978. Set PSH only if psh_sequence lies in this
    // payload interval; write TCP header/options into the existing first
    // Buffer headroom, never copy its FIFO-sourced payload. Advance snd_nxt
    // and recovery bookkeeping once; a retransmit never advances snd_nxt.
    #[inline(always)]
    pub(crate) fn push_one_header(&mut self, buffer: &mut Buffer, cached_opts: &[u8; 40]);
}

impl TcpWorker {
    // VPP tcp_output.c:1037-1058. Allocate one Buffer from runtime; on
    // allocation failure update the receive window and return. Otherwise
    // build ACK, then append (buffer, tco_next_node[family]) to Session's
    // paired pending columns. No AppWorker event is emitted here.
    pub(super) fn send_ack(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32,
    );

    // VPP tcp_output.c:1123-1270,1672-1708. Peek the *same Session TX FIFO*
    // at snd_nxt - snd_una; copy directly into final allocated Buffer(s),
    // add TCP header, enqueue to the existing tco_next_node. Return the
    // number of successfully packetized segments; no private payload Vec.
    fn transmit_unsent(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize, available_bytes: u32,
    ) -> usize;

    // VPP tcp_output.c:1804-2065,2125-2152. Select SACK scoreboard,
    // byte-tracker/RACK or no-SACK recovery already owned by TcpConnection;
    // retransmit unacked FIFO ranges, then use remaining PRR/cwnd budget for
    // transmit_unsent. Reprogram retransmit when the burst cannot proceed.
    fn retransmit(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize,
    ) -> usize;

    // VPP tcp_output.c:2070-2122. Preserve pending dupACK/SACK count and
    // max-burst behavior. Return VPP's attempted ACK count even if send_ack
    // cannot allocate a Buffer; only allocated ACKs enter Session pending.
    fn send_acks(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize,
    ) -> usize;

    // VPP tcp_output.c:2154-2186. One custom-TX entry, not a second
    // session packetizer. Recovery may also send unsent FIFO bytes.
    pub(super) fn custom_tx(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, params: &TransportSendParams,
    ) -> usize {
        let retransmit = {
            let connection = self.connections.get_mut(connection_index)
                .expect("custom TX connection remains live");
            let pending = connection.recovery.in_recovery()
                && connection.retransmit_pending;
            if pending {
                connection.retransmit_pending = false;
            }
            pending
        };
        let packets = if retransmit {
            self.retransmit(runtime, sessions, connection_index,
                params.max_burst_size as usize)
        } else {
            0
        };
        let send_ack = {
            let connection = self.connections.get_mut(connection_index)
                .expect("custom TX connection remains live");
            let pending = connection.send_ack_pending;
            connection.send_ack_pending = false;
            pending && (packets == 0 || connection.pending_dupacks != 0)
        };
        if !send_ack {
            return packets;
        }
        if packets >= params.max_burst_size as usize {
            self.connections.get_mut(connection_index)
                .expect("custom TX connection remains live")
                .send_ack_pending = true;
            return packets;
        }
        packets + self.send_acks(runtime, sessions, connection_index,
            params.max_burst_size as usize - packets)
    }
}
```

`TcpConnection::send_space` 不是旧 `tx_payload_budget` 的别名：它不调用
`pacing_ready`、`next_send_delay` 或 Nagle；Session 统一应用通用 pacer。
`update_burst_send_vars` 和 `push_one_header` 操作 TCP 已有字段及 cached options，
不是在 service 增加 TCP 状态。`transmit_unsent`/`retransmit` 的 Buffer 只保留 index
到 `TcpWorker.tx_buffers` 或 Session pending，payload 均从 Session FIFO 读取。

当前 `TcpConnection::tx_segment` 在 established 路径无条件用
`ACK | PSH` 构造 `TcpSegment`（`connection.rs:1512-1515`）；这会抵消上述
PSH 边界设计。目标保留**已有** `TcpSegment` 作为 TCP output intent，由
connection 的现有构造路径根据 `psh_pending/psh_sequence` 为本段决定 PSH，
不在 Session 或 `TcpSegment::write_to_buffer` 里补改 flag。已有
`TcpSegment::write_to_buffer` 直接把 header 写入首 Buffer 的 headroom，
Session 已放入的 payload/chain 保持原位；`commit_payload_tx` 每段只执行一次，
不能由 Session 和 TCP 各推进一次 `snd_nxt` 或重复记录 recovery/timer。
来源：`tcp_output.c:884-1035`；Hammer `connection.rs:1487-1588`、
`segment.rs:104-129`。

TCP 的 `pending_deq_acked: Vec<u32>` **已经**在 `TcpWorker`，目标直接使用它。
在已有 `TcpConnectionCacheline0` 保存本 burst 的 `burst_acked: u32` 与
`deq_pending: bool`；首次 ACK 进 pending 向量，后续 ACK 累加字节，不重复排队。
TCP 输入先结束对具体 connection 的可变借用，再调用
`TcpWorker::program_dequeue(connection_index, bytes_acked)`；该方法自己同时修改
connection 的计数与 worker 向量，不在外部持有 connection 引用时重借 worker。
TCP 输入 node 在其一批 packet 处理结束、已有 Session RX enqueue event flush
之后且 disconnect 清理之前遍历该向量：清 pending 标志，
对有 ACK 字节的 connection 调用一次
`SessionWorker::tx_fifo_dequeue_drop(&connection.base, burst_acked)`，然后核对
FIFO 中仍保留的 `snd_nxt - snd_una`、必要时重新调度被 deschedule 的 Session，
更新重传 timer 与通用 pacer，最后清零 `burst_acked` 和向量。VPP 对应的
`delivered_time` 更新属于 TCP 已有的 delivery/CC owner，不塞入 Session。
当前 `established.rs`、`rcv_process.rs`、`syn_sent.rs` 的直接
`tx.drop_dequeue` 与逐 ACK `enqueue_ready` 均须收敛到这条 burst-end 路径；
TCP input 持有的 worker borrow 与 `&mut SessionWorker` 是两个 owner，调用 service
方法时不再经全局 Main 借第二份 Session worker。
来源：`tcp_input.c:494-545,1407-1413,2365-2371`、`session.c:738-749`。

```rust
impl TcpWorker {
    // VPP tcp_input.c:536-545. Coalesce ACKed bytes per connection for
    // this input burst; queue each connection index at most once.
    fn program_dequeue(&mut self, connection_index: u32, bytes_acked: u32);

    // VPP tcp_input.c:494-533,1407-1412,2365-2370. Called once after
    // the input burst, before leaving the TCP input node.
    fn handle_postponed_dequeues(&mut self, sessions: &mut SessionWorker);
}
```

`app_rx_event` 的 TCP 实现只处理零接收窗口恢复：若未发送过零窗口，立即返回，
不先修改 TCP 接收窗口。否则取 Session RX FIFO 的 size 和 producer 可入队空间，
空间不足阈值时对 **RX FIFO**
调用现有 dequeue-notification 请求，空间足够时按 `tcp_send_ack` 的路径申请
buffer、更新接收窗口、构造独立 ACK 并通过 service 的
`SessionWorker::add_pending_tx_buffer` 加入 TCP4/TCP6 output 的 pending 列。
`tcp_make_ack` 根据实际公告窗口更新 zero-window 标志；若 buffer 申请失败，
仍更新接收窗口后返回。VPP 在此处记 TCP worker 的 `no_buffer` 统计，
本 ADR 不引入先前排除的 stats，也不能错记为 Session Queue 的 `NoBuffer` node
counter。此处不返回 `SessionError::NotSupported` 或 recoverable `Result`。该入口不调用
`AppWorker::add_event`、不克隆 `TcpConnection` 试探 ACK，也不把 ACK 当普通
FIFO payload。RX FIFO size/空间读取在短借用中完成；需要 enqueue ACK 时先
结束该借用，再可变访问 `SessionWorker` 的 pending 列。
来源：`tcp.c:1382-1401`、`tcp_output.c:1037-1058`、
`session.h:917-975`。

```rust
// VPP tcp.c:1372-1401,1402-1431; tcp_output.c:917-930,1037-1058.
// TCP overrides the two optional service Transport hooks; this is the
// concrete plugin implementation, not another dispatch callback or node.
impl Transport<IpTransportEndpointConfig> for TcpMain {
    // VPP tcp.c:1171-1196; tcp_output.c:300-329. Update burst vars before
    // filling the caller's send-space, offset, MSS and flag fields.
    fn send_params(
        &self, runtime: &DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        params: &mut TransportSendParams,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("send params runs on the TCP owner worker");
        let TcpWorker { connections, cached_opts, .. } = &mut *tcp;
        let connection = connections.get_mut(connection_index)
            .expect("Session TX retains a live TCP connection");
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::tx_fifo)
            .expect("TCP data Session retains its TX FIFO");
        connection.update_burst_send_vars(fifo, cached_opts);
        let outstanding = connection.snd_nxt().wrapping_sub(connection.snd_una());
        params.send_mss = connection.send_goal_size() as u16;
        params.send_space = connection.send_space()
            .min(connection.snd_wnd().saturating_sub(outstanding));
        params.tx_offset = outstanding;
        params.flags.deschedule = params.send_space == 0;
    }

    // Read the current Session TX FIFO through worker, then set the existing
    // connection's PSH pending flag and wrapping sequence boundary once.
    fn flush_data(&self, worker: &SessionWorker, connection_index: u32) {
        let mut tcp = self.worker(worker.worker_index())
            .expect("flush runs on the TCP owner worker");
        let connection = tcp.connection_mut(connection_index)
            .expect("TX_FLUSH retains a live TCP connection");
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::tx_fifo)
            .expect("TCP data Session retains its TX FIFO");
        connection.flush_data(fifo);
    }

    // Check zero-window history; request RX FIFO dequeue notification below
    // the VPP threshold, otherwise build an ACK directly in one allocated
    // Buffer and add it to SessionWorker's pending TCP4/TCP6 output.
    fn app_rx_event(
        &self, runtime: &mut DataPlaneMain,
        worker: &mut SessionWorker, connection_index: u32,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("RX event runs on the TCP owner worker");
        let connection = tcp.connection(connection_index)
            .expect("RX event retains a live TCP connection");
        if !connection.zero_receive_window_sent() {
            return;
        }
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::rx_fifo)
            .expect("TCP data Session retains its RX FIFO");
        let min_free = (fifo.size() / 8).clamp(4 << 10, 128 << 10);
        if fifo.max_enqueue() < min_free {
            fifo.want_deq_notification();
            return;
        }
        tcp.send_ack(runtime, worker, connection_index);
    }

    // VPP tcp_output.c:980-1035. Headerize all first buffers in this burst;
    // use available_bytes for cwnd-limited accounting and update timers.
    fn push_header(
        &self, runtime: &mut DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        buffers: &[u32], available_bytes: u32,
    );

    // VPP tcp_output.c:1804-2065,2154-2186. Accept Connection only;
    // recovery may send unsent FIFO bytes too, followed by pending ACKs.
    // Return the frame-budget count, not the pending-buffer length.
    fn custom_tx(
        &self, runtime: &mut DataPlaneMain, worker: &mut SessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) -> usize {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP registers only the peek TX entry");
        };
        self.worker(runtime.thread_index())
            .expect("custom TX runs on the TCP owner worker")
            .custom_tx(runtime, worker, connection_index, params)
    }
}
```

service 的 IO dispatch 在 Session 仍未 `TransportClosed` 的 RX event 上调用已注册
transport 的 `app_rx_event`；TX_FLUSH 只在普通 FIFO TX 入口调用 `flush_data`
一次，再继续同一 event 的 send params/custom TX/packetization。未提供 RX 行为的
transport 实现 no-op，不能返回 `NotSupported`。具体 TCP/UDP worker、PSH/ACK、
output next 都不移到 service。现有 `tcp_session_io` 内自行实现的 RX/TxFlush
分支在迁移后删除：RX 由 service 的同一 IO dispatch 调用注册的 TCP trait
入口，TxFlush 由共用 FIFO TX 算法在取 send params 前调用 trait。调用期间
只保存 connection index；`tx_get_transport` 取得的 `&Connection` 在任何
`flush_data`、`send_params`、`app_rx_event` 或 `push_header` 可变 TCP 操作前结束
借用，不能把该引用跨操作留在 `SessionTxContext`。TCP callback 获取一次
`TcpMain`，但不跨上述调用保留 `&Connection`。以上 trait 改动是实现前须批准的公开 API；
本 ADR 只设计，不改源码。

`SessionTxContext.send_params` 仍是本 worker 的可复用 scratch。调用
`custom_tx`（传 `&mut SessionWorker`）或 `send_params`（传 `&SessionWorker`）前，
用 `std::mem::take` 将这一个小值暂移到栈上，填入本轮 burst 后传
`&mut TransportSendParams`；回调结束立即写回 context，再运行依赖它的
`tx_set_dequeue_params`。这只移动发送参数，不拷贝 FIFO payload，且不会同时
构造 `&self` 与 `&mut self.tx_context.send_params` 的别名。
`push_header` 借 context 中首 Buffer index 的不可变切片及 `&SessionWorker`，
两者均为共享借用；TCP 独占修改的是另一个 owner 的 Buffer。

## FIFO TX 算法

```rust
impl SessionWorker {
    // VPP session_node.c:1239-1293. State check distinguishes peek from
    // dequeue; Accepting/TransportClosed with custom TX is not ordinary data.
    #[inline(always)]
    fn tx_not_ready(session: &Session, peek_data: bool) -> SessionTxReadiness;

    // VPP session_node.c:1276-1293. Peek always selects connection;
    // dequeue selects listener only for a Listening Session.
    #[inline(always)]
    fn tx_get_transport<'transport, T, E>(
        &self, transport: &'transport T, peek_data: bool,
    ) -> &'transport T::Connection where T: Transport<E>;

    // VPP session_node.c:1296-1435. Read FIFO consumer availability now;
    // apply tx_offset only for peek, then snd_space/MSS/frame budget.
    #[inline(always)]
    fn tx_set_dequeue_params<M, const DGRAM: bool>(
        &mut self, runtime: &DataPlaneMain, max_segments: u32, peek_data: bool,
    );

    // VPP session_node.c:1046-1071. Ordinary path copies directly from
    // session-owned FIFO into final Data Plane Buffer storage; DMA path borrows
    // readable FIFO segments and records transfers before packet publication.
    // VPP session_node.c:976-1044. DMA uses the same target buffer and FIFO
    // offset; these methods are inactive until runtime has generic DMA.
    #[inline(always)]
    fn tx_fill_dma_transfers(&mut self, runtime: &mut DataPlaneMain, buffer: u32) -> u32;

    #[inline(always)]
    fn tx_fill_dma_transfers_tail(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    // VPP session_node.c:1046-1071. Ordinary copy or optional DMA dispatch.
    #[inline(always)]
    fn tx_copy_data(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    #[inline(always)]
    fn tx_copy_data_tail(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    // VPP session_node.c:1073-1157. Consumes exactly the selected segment's
    // remaining payload and connects chain headers using existing Buffer API.
    #[inline(always)]
    fn tx_fifo_chain_tail<M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, first: u32,
        remaining_buffers: &mut u16, peek_data: bool,
    );

    // VPP session_node.c:1159-1237. Set locally originated/zero error,
    // reserve TRANSPORT_MAX_HDRS_LEN, fill first buffer, then optional tail.
    #[inline(always)]
    fn tx_fill_buffer<M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, first: u32,
        remaining_buffers: &mut u16, peek_data: bool,
    );

    // VPP session_node.c:1437-1453. Unset FIFO event; recheck available
    // bytes against the transport offset; set event + head-old only if new
    // work appeared. Otherwise deschedule the owning connection.
    #[inline(always)]
    fn tx_maybe_reschedule<T, E>(
        &mut self, runtime: &DataPlaneMain, event_index: u32, transport: &T,
    )
    where T: Transport<E>;

    // VPP session_node.c:1456-1468. Ordinary TX and DMA pending lists.
    #[inline(always)]
    fn tx_add_pending_buffer(&mut self, buffer: u32, next: u16);

    // VPP session.h:1101-1115. Separate direct transport control packet API.
    #[inline(always)]
    pub fn add_pending_tx_buffer(&mut self, runtime: &DataPlaneMain, buffer: u32, next: u16);

    // VPP session_node.c:1472-1677. The shared peek/dequeue algorithm.
    // DGRAM is a registration-time specialization, never a stored TX mode.
    #[inline(always)]
    fn tx_fifo_read_and_send_i<T, E, M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, peek_data: bool, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session_node.c:1680-1694. TCP chooses peek; FIFO bytes stay until ACK.
    fn tx_fifo_peek_and_send<T, E>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session_node.c:1688-1694. Both ordinary and datagram dequeue
    // select this entry; M is used only for concrete datagram metadata.
    fn tx_fifo_dequeue_and_send<T, E, M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session.c:738-749. TCP ACK removes retained TX bytes through
    // Session ownership, including FIFO tuning and dequeue notification.
    #[inline(always)]
    pub fn tx_fifo_dequeue_drop<E, O>(
        &mut self, connection: &TransportConnection<E, O>, max_bytes: u32,
    ) -> u32;
}
```

`tx_fifo_read_and_send_i` 的执行顺序必须是：

1. 检查 Session state：peek 的 Ready 可发，Accepting 或 transport 已关闭但尚未
   Deleted 时仅允许 custom TX；未 Ready 的普通 peek defer，已 Deleted 丢弃。
   dequeue 仅在 transport 已 Deleted 或 FIFO 不存在时丢弃；它在 Listening 状态
   选 listener transport，其余状态选 connection。随后从
   `SessionMain.session_type_to_next` 取已注册 edge，计算
   `max_burst = SESSION_NODE_FRAME_SIZE - packets`。`TX_FLUSH` 先调用 `flush_data` 并把
   同一个 event 改为 TX，不产生第二个 TX event。
2. 如有 `SESSION_F_CUSTOM_TX`，先清 flag，再以
   `TransportTxTarget::Connection(connection_index)` 让具体 transport 用本轮剩余 burst
   执行 `custom_tx`，将返回的 frame-budget packet 数计入 `packets`（不以 pending
   buffer 增量替代）；关闭状态、耗尽预算、再次设置 custom flag
   的分支按 VPP 完成或放回 old list。custom TX 可能改变 event pool，后续只通过
   `event_index` 重新取得元素，不保留跨调用的元素引用。
3. 清 transport descheduled 状态，**即时**调用 `send_params`（TCP 会更新 MSS、窗口和
   `snd_nxt-snd_una` 的 peek offset）；按 `DESCHED`、`POSTPONE`、其他无发送空间分别
   deschedule、tail-old、head-old。若启用 pacing，按 bucket 限制并按 MSS 对齐；不足
   最小 burst 时 head-old。没有可发送 FIFO 数据时重置 pacer bucket 并执行 event
   unset/recheck，不生成 buffer。
4. 从 runtime 的默认 buffer 数据大小和 **140 字节 transport 最大 header 预留**计算
   首 buffer 容量、每后续 buffer 容量、每 MSS 所需链长以及整轮 `buffers_needed`。
   `max_len_to_send` 同时受 FIFO 可读量、offset、transport `snd_space` 和 frame 剩余
   packet 数约束。一次申请全部 buffer；不足就释放已申请的部分、event 放回 head-old，
   只增加 `NoBuffer` node counter，FIFO offset、消费位置和 pacer 扣款均不前进。
5. 申请成功后扣 pacer 字节预算；以 VPP `while n_left >= 4` 的双包流水循环填充，
   预取后续 buffer header，**每轮处理两个首 buffer**，最后 `while n_left != 0`
   处理尾包。每个 packet 的链尾取自同一批 buffer；预留 header 只在首 buffer，
   `Buffer::set_next_buffer`、`set_total_len_not_including_first` 表达 VPP chain flags。
   `transport_pending_buffers` 保存首 buffer index，不放自造 packet carrier。
6. 所有首 buffer 都填好后一次调用 `Transport::push_header`，参数为本轮首 buffer
   切片和实际 `max_dequeue`；具体 TCP 在该调用更新 sequence/重传 timer。释放任何
   未用的已分配 buffer，追加首 buffer 与相同数量的 output next，最后增加 packet
   计数。若本轮受 burst 截断，event 放 tail-old；否则 unset FIFO event、重检、
   必要时 head-old 或 deschedule。仅 dequeue 分支按实际消费字节触发
   `needs_deq_notification`/`session_dequeue_notify`；peek 的 ACK 后消费和通知仍由
   TCP 调用 service 的 `tx_fifo_dequeue_drop` 完成。重传读取用现有 `Session::tx_fifo`
   直接借用并 `peek`，不为纯借用另造 `session_tx_fifo_peek_bytes` helper。

普通 TX **确有一次** FIFO 到最终 packet buffer 的 payload copy，和 VPP 一致；不存在
中间 `Vec<u8>`、TCP 私有 payload copy 或 header 临时 buffer。`push_header` 只在
已有 buffer 的 headroom 中写 header。上述 `tx_copy_data` 不意味着把 FIFO 借用
直接挂进 packet chain；SVM FIFO 的生命周期和 Buffer Arena 的生命周期不同。

`session_tx_*` 签名与 VPP 的对应关系是：`session_worker_t *wrk` 变为
`SessionWorker` 的 `&mut self`；`wrk->ctx` 已是 self 的字段，不能再额外传一个
可变 `ctx`；`wrk->vm` 变成显式的 `&mut DataPlaneMain`；
`session_evt_elt_t *elt` 在 pool 可能因 custom TX 移动时变为同一元素的
`event_index`；`vlib_buffer_t *b` 变为现有 Buffer Pool 的 `u32` index，避免同时
借用整个 runtime 和其中一个 `&mut Buffer`。VPP 的 `u8 peek_data` 用 `bool`，
`int *n_tx_packets` 用 `&mut usize`，VFT 调用由额外的具体 `&T: Transport<E>`
单态化。除此之外，不给 helper 拆入 `fifo`、`offset`、`session`、buffer size
等 VPP 本来从 worker/context 取得的参数。`data0`/`data` 指向 `b` 内部，
Rust 由 `buffer` index 在 helper 内借目标 slice，不能与 `&mut Buffer` 同时外传。
`flush_pending_tx_buffers` 额外接收 `Frame`，仅因为当前
`DataPlaneMain::enqueue_to_next` 要求调用方提供 `&mut Frame`；它不再返回
pending 数量。queue 与 VPP 一样以自己的 `n_tx_packets` 为返回值。

`tx_fifo_dequeue_and_send` 同时覆盖普通 dequeue 与 VPP 的 datagram dequeue：
`const DGRAM` 和 metadata 范型 `M` 仅在注册时选择具体单态化入口，
不成为 Session/Transport/Context 的运行时 mode 字段。普通 dequeue 不读取 `M`；
datagram 的 `M` 是 plugin-session 拥有的 metadata。

## Datagram、Internal、DMA

```rust
impl SessionWorker {
    // VPP session_node.c:1696-1747. Custom/internal transport owns packet
    // generation; service owns burst, event lists and dequeue notification.
    fn tx_fifo_dequeue_internal<T, E>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime,
        event_index: u32, packets: &mut usize, transport: &T,
    ) -> usize where T: Transport<E>;

    // VPP session_node.c:1965-1973. Fan out paired slices in graph frames;
    // reset both Vec lengths only after graph accepts all of them.
    fn flush_pending_tx_buffers(
        &mut self, runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime, frame: &mut Frame,
    );
}
```

Datagram 不是 `connectionless` 布尔值的同义词。service 用现有
`SessionDatagramPrefix { data_length, data_offset }` 加 `size_of::<M>()` 和 GSO 字段
计算 record 长度；`M` 的 IP 实例及 endpoint 解释都留在 plugin-session。只有完整
record 才可发送；长度为零时按 VPP 丢弃该 header；未完整写入则留待下一轮。单包分段
受当前 record 剩余长度与 GSO size 约束，仅在该 datagram 不需 buffer chain 时合并
相等长度的后续完整 datagram。部分消费只覆写前缀的 `data_offset`，完整消费丢弃
整个 header + payload；需要连接 metadata 的 transport 从最终首 buffer 的预留区
取得该 record 的原始 metadata，不让 service 解码 IP。dequeue 通知的字节数包括
VPP 所计的 datagram header 长度；不可把 stream dequeue 规则直接套过来。
小 datagram 的合并扫描上限为 VPP 的 32 KiB，且只合并 payload 剩余长度相等、
header 和 payload 都已完整进入 FIFO 的 record。
datagram 走上述 `tx_fifo_dequeue_and_send` 的注册期具体化入口，复用共同的
窗口/预算、整批分配、双包流水、链尾和 pending fanout 算法，只在 record 解析与
FIFO 消费规则上分支；不复制第二套 packet scheduler。

```rust
// VPP tcp.c:1171-1196,1372-1380,1402-1431;
// tcp_output.c:980-1035,2154-2186; session.c:738-749.
// Concrete TCP entry registered for each IP Session type by plugin-session.
fn tcp_session_tx(
    worker: &mut SessionWorker,
    runtime: &mut DataPlaneMain,
    node: &mut NodeRuntime,
    event_index: u32,
    packets: &mut usize,
) -> SessionTxOutcome;
```

`tcp_session_tx` 是一个已注册的单态化 callback，不是第二个 TCP TX node；它取得
一次 TCP Main，再以 connection index 调用 service 的 peek 批处理；它**不能**
在进入 service 前保留 `RefMut<TcpWorker>` 或 `&Connection`。当前
`TcpMain::worker` 返回 `RefMut`，而 `Transport::send_params`、`push_header`、
`custom_tx` 等方法还要借同一个 TCP worker；跨 service 调用持有 guard 会在
第一个 trait 方法处触发重复可变借用。每个 trait 方法只在自己执行期间借
owner worker，释放 guard 后才允许下一次调用。来源：
`session_node.c:1494-1590,1652-1655`；Hammer `tcp/src/lib.rs:150-178,450-565`。
TCP 输入 burst 末尾才把 `connection.base` 借给
`SessionWorker::tx_fifo_dequeue_drop`，由 service 从 backlink 取得 Session 并决定
是否通知 AppWorker；TCP 自己仍负责 ACK 合并、timer、pacer 与重排。

Internal 路径不获取普通 FIFO send params，也不调用普通 FIFO `push_header`。它以
`TransportTxTarget::Session(handle)` 调用**同一个** `custom_tx` trait 方法，给出
不超过 `min(frame 剩余, TRANSPORT_PACER_MAX_BURST_PKTS)` 的预算；
`bytes_dequeued` 用于应用通知。custom flag 再次出现时 tail-old；否则按 VPP 清 FIFO
event、重检并可能 head-old。它保留为明确路径，即使当前没有插件使用；不以
`NotSupported` 假装这个 VPP 路径不存在。

DMA 是普通路径的可选实现，不改变 TX 语义。VPP `session.c:2119-2194` 和
`session_node.c:976-1071,1456-1468,2054-2063,2132-2145`：启用后从 FIFO
借 readable segments 向已分配 packet buffer 安排 DMA；本轮首 buffer/next 进入
`dma_transfers[dma_tail]`，提交 batch，completion 在所属 worker 将同一批成对移入
普通 pending 列，queue 后续轮次再 flush。ring 满或无法取得 batch 时本轮不消费
事件。当前 runtime 没有通用 DMA batch API，因此本次实施默认 `dma_enabled=false`
并用 VPP 的软件 fallback；不能让配置位开启一个没有 completion 的假 DMA 路径，
也不能在提交前提前扇出未完成的 packet。日后增加 runtime 通用 DMA 能力需独立批准，
不能在 service 私造 DMA engine。

## 初始化、同步、错误和 inline

`SessionMain::init` 构造固定 worker `Vec` 时初始化 TX scratch 与两个 pending 向量，
event queue/output edge/transport 单态化 TX 入口在 worker 开始处理该 Session 类型前
完成注册；cacheline mark 分隔 worker/context 的热字段。每次 TX 事件只重置本轮计数
与 scratch 向量长度，不重建 context，也不逐包查询全局 Main。

TX context、event lists、pending buffers 和 transport connection 都由同一个 Data
Worker 写，不需要 `SpinLock`、`RwLock`、`ArcSwap` 或 TX snapshot。跨 worker TX
只通过现有 Session event queue 投递；app 与 worker 之间的 FIFO head/tail、event
bit、dequeue notification 使用 SVM FIFO 的 acquire/release 协议。具体地，consumer
读 producer tail 使用 acquire、消费 head 使用 release；`set_event` 是 release
原子交换，`unset_event` 必须是 acquire 原子交换，然后重读 FIFO 可用量。
现有 `hammer-infra::svm::fifo::Fifo::unset_event` 是 release store，**尚未对齐**
`svm_fifo_unset_event`；TX 接入前在原 API 上修正这一原子契约，不新建第二套 event
bit，也不能用普通 bool 代替。Session state
继续使用现有 `AtomicU8` 的 acquire/release 读写表达 VPP 的 volatile 可见性；
`volatile` 不能替代 Rust 跨线程同步。注册或改 output edge 必须在启动前或
`WorkerBarrier` 内完成；barrier 是唯一的 graph publication 边界。DMA completion
必须回到 owner worker，不能从其他 worker 直接改 pending `Vec`。

```rust
// VPP session_node.c:925-949,1580-1587,2155-2159.
// Node counters, not control-plane Result or numeric retval.
pub enum SessionQueueNodeError {
    Tx,
    Timer,
    NoBuffer,
}
```

`NoData`（未 ready、窗口为零、pacer 等待、FIFO 不足或 datagram 未完整）和
`NoBuffers` 都是 TX outcome，不是 `SessionError`；仅后者增加 `NoBuffer` counter。
`Tx` 计数是成功 packet 数，`Timer` 保持 VPP 声明但本 TX 路径不自增。
`SessionTxOutcome::Ok` 只报告 VPP 的状态，`packets: &mut usize` 是唯一的
frame-budget 计数；TCP custom ACK 的返回值与 pending buffer 长度可能不同。
TCP 自己申请 ACK/重传 Buffer 失败时走 TCP worker 的资源路径，不记为
Session Queue `NoBuffer`。`TcpSegment::write_to_buffer` 在预留 140 字节 header
空间且 TCP options 不超过协议上限后失败，是 TCP output 的内部不变量：
按 connection/buffer 身份断言，不把每包 header 构造错误上翻为
`SessionError::TransportOpFailed`。来源：`tcp_output.c:884-1035,1037-1058`、
`session_node.c:1580-1587`。
缺少已注册 output edge、分配后的 FIFO 可读区间突变、链长度不匹配、
transport 返回超过 burst 的 packet 数，均是 owner 不变量，带 Session/worker
身份断言；不发明诸如 `MissingEgressEndpoints`、`TxModeUnsupported` 的恢复错误。
注册时 output graph 不存在则复用 `SessionQueueError::OutputMissing`，不接受
`u32::MAX` 哨兵进入热路径。外部配置与资源失败继续使用现有带 source 的
`SessionError`/`SessionQueueError`（VPP `session_types.h:519-561`）；不把 packet
失败翻译成 `i32`。

| 方法 | inline | VPP 依据 |
|---|---|---|
| `tx_not_ready`、`tx_get_transport`、`tx_set_dequeue_params`、`tx_fill_dma_transfers`/`tail`、`tx_copy_data`/`tail`、`tx_fifo_chain_tail`、`tx_fill_buffer`、`tx_maybe_reschedule` | `#[inline(always)]` | `session_node.c:976-1453` 的 `always_inline` |
| `tx_add_pending_buffer`、`add_pending_tx_buffer`、FIFO event 快路径 | `#[inline(always)]` | `session_node.c:1456-1468`、`session.h:1093-1115` |
| `tx_fifo_read_and_send_i` 共用读/发算法 | `#[inline(always)]` | `session_node.c:1472` 的 `always_inline` |
| peek、dequeue、internal 三个入口 | 不强制 inline | `session_node.c:1680-1747` 的普通函数；datagram 复用 dequeue，不造第四个入口 |
| `flush_pending_tx_buffers`、transport 注册、DMA setup/completion | 不标注 | `session_node.c:1965-1973`、`session.c:1827-1847,2119-2194` |
| `Transport::flush_data`、`app_rx_event` 及 TCP 实现 | 不强制 inline | `transport.h:86-88,183-191`、`tcp.c:1372-1401` 的 callback 非 `always_inline`；`transport_app_rx_evt` 包装虽为 `static inline`，Hammer 的单态化 dispatch 不需额外包装层 |
| TCP 批量 ACK 排队/释放与 `push_header` 外层 | 不强制 inline | `tcp_input.c:494-545` 的 `static` 函数、`tcp_output.c:980-1035` 的普通函数；内部 `tcp_push_one_header` 为 `always_inline`（`tcp_output.c:965-978`） |
| TCP 发送空间和 recovery FIFO 剩余量的小计算 | `#[inline]` | `tcp.c:1100-1164` 的 `static inline`、`tcp_output.c:1741-1746` 的 `static inline`；不把整个 `send_params` 强制内联 |
| TCP 逐首 buffer 写 header 的内层 | `#[inline(always)]` | `tcp_output.c:966-978` 的 `always_inline tcp_push_one_header`；外层批次循环不强制内联 |

## 迁移顺序与验收

1. 移除 `TransportTxMode`、`TransportOptions.tx_mode`、`SessionTxContext.tx_mode`、
   `SessionMain.session_tx_modes` 和 `IpSessionMain::register_transport_type(tx_mode, ...)`；
   `init_tcp` 只分配一次 TCP protocol id、注册 RX/control/time；两个 output
   graph node 在现有 init 内各自编译 next 并注册对应 Session type/TX 入口；
   plugin-session 派生两个
   Session type。Session 保存不相互替代的 type/protocol，TCP worker init
   缓存相同的两个 next 供 control packet 使用。保留 `Transport` trait；
   不把 VPP 的 VFT 或整个 `session_tx_fns[TRANSPORT_TX_N_FNS]` 复制进 Hammer。
2. 修正 worker scratch/cacheline、成对 pending 向量、整批预算/分配、140-byte
   headroom、buffer chain 和双包流水循环。复用 `Buffer` 现有 chain API 与 SVM
   FIFO 的 `peek`/`dequeue`/`drop_dequeue`/event/notification API，并先修正
   `unset_event` 的 acquire 交换语义；不增加 payload 搬运容器。
3. 接通 TCP peek、`TX_FLUSH`、pacer、custom TX、RX 零窗口恢复和 dequeue ACK 通知；
   将现有 `tcp_session_io` 的 RX/TxFlush 分支收敛到 `Transport` trait，删除
   RX 的连接克隆和 `NotSupported` 返回，ACK 通过 Session pending buffer 扇出；
   `TcpConnection::tx_segment` 去掉默认 PSH，`push_header` 批量 headerize 后每段
   只提交一次 sequence/recovery 状态；恢复期 custom TX 从 Session FIFO 直接
   读取未发送数据。TCP 输入复用 `pending_deq_acked`，在 burst 末尾统一执行
   `SessionWorker::tx_fifo_dequeue_drop`、timer/pacer 更新与重新调度，删掉各输入
   node 的直接 `drop_dequeue` 和逐 ACK `enqueue_ready`。再接 datagram/internal
   分支；DMA 待 runtime 通用能力存在才启用，默认软件路径。
4. 验收需覆盖单事件多 MSS、首包与多 buffer 链、`n_left` 为 1/2/3/4/5 的循环
   边界、部分 buffer allocation、窗口/offset/pacer、custom TX 后重入、TX_FLUSH、
   FIFO event 竞争、dequeue 通知、重复 flush 的 PSH 边界、RX FIFO 阈值上下界、
   zero-window ACK 的 buffer 不足、recovery 中的新数据只由 custom TX 发送、
   ACK burst 合并释放/应用通知/重新调度、单协议双 Session type、TCP worker
   guard 不跨 service 重借、init/graph/worker 启动顺序、
   send params 不重复执行旧 pacing 门控、datagram 部分/完整 record、output next 4/6 和
   pending flush 的多 frame 扇出。当前仅写 ADR，未运行编译、测试、静态检查或 CI。
