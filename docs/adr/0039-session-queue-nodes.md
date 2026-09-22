# ADR-0039: Session Queue Node 与 Transport 输出边界

- 日期：2026-09-22
- 状态：Proposed
- Issue：#356
- 前置 ADR：ADR-0038
- 范围：`session-queue`、`session-queue-process`、`session-queue-main`，以及它们与
  `hammer-plugin-session`、TCP、UDP 的 TX 接线
- 不包含：Application、AppWorker、Application Namespace、socket、Binary API、外部 client、stats

## 1. 决定

三个 Session Queue Node 都属于 `hammer-service`：

- `session-queue`：按 runtime thread index 驱动当前 `SessionWorker`；
- `session-queue-process`：仅在 thread zero 上提供 process event 与一秒 fallback；
- `session-queue-main`：VPP 同名 pre-input wrapper 的对应声明。

三者通过静态 graph declaration 注册，初始均为 `Disabled`。Node declaration 在 graph
materialization 时安装；`main_loop_enter` 不注册 Node，只在配置的 `session_enable_asap` 为 `true`
时调用 `SessionMain::enable`。默认 `session_enable_asap = false`。

不引入额外的 scheduler trait/type、泛型 Graph Node、marker field、VFT、函数指针注册表、`dyn`
trait、provider、mailbox、第二条 event queue 或 `NodeRuntime` 中的 Main 指针。

不存在额外的 transport graph node。仓库已有的 `TcpOutputNode` 和 `UdpOutputNode` 保持 packet node
语义：输入 frame 中的每个 `u32` 都是 `BufferIndex`，不是 Session event index。它们不以 empty frame
唤醒，不读取 `SessionWorker` 的 new/old event list，也不负责 Session 调度。

## 2. VPP 实际数据路径

VPP 的 `session-queue` 自己完成以下工作：

1. drain `svm_msg_q_t`；
2. 按 control、new IO、old IO 顺序消费 event element；
3. 对 TX event 读取 transport send params；
4. 从 Session TX FIFO 生成 packet buffer；
5. 调用 transport `push_header`；
6. 把 `u32` buffer index 按 `session_type_to_next` 送到 TCP/UDP output node。

VPP output node 接收的是 packet vector。源码没有一个介于 Session Queue 与 TCP/UDP output 之间、以
Session event index 为 frame 语义的 transport node：

- `third_party/vpp/src/vnet/session/session_node.c:1471-1676`：Session Queue packetize、
  `push_header`、记录 pending buffer/next；
- `third_party/vpp/src/vnet/session/session_node.c:1858-1917`：IO event 在 Session Queue 内 dispatch；
- `third_party/vpp/src/vnet/session/session_node.c:2033-2177`：完整 queue loop 与 pending buffer flush；
- `third_party/vpp/src/vnet/session/session.c:1826-1847`：Session type 只注册 TX mode 与 output next；
- `third_party/vpp/src/vnet/tcp/tcp_output.c:2400-2446`：TCP output node 接收 `u32` packet vector；
- `third_party/vpp/src/vnet/udp/udp_output.c:202-246`：UDP output node 接收 `u32` packet vector。

因此 Hammer 的 graph 边必须保持：

```text
SvmMsgQ
  -> session-queue
       -> BufferIndex frame -> TcpOutputNode -> IP lookup
       -> BufferIndex frame -> UdpOutputNode -> IP lookup
```

以下路径被否决：

```text
session-queue -> empty-frame wake -> output node -> pop Session event
session-queue -> Session-event frame -> output node expecting BufferIndex
```

第一条会把 Session 调度递归交给 output node；第二条让同一个 graph edge 上的 `u32` 同时表示两种
对象，破坏 Frame ABI。

## 3. 三层边界

```text
hammer-service
  Session<O> / SessionWorker<O> / SessionMain<O>
  SvmMsgQ drain, event pool/list, FIFO packetize, pending buffer fanout
  session-queue / session-queue-process / session-queue-main
  Transport trait, TransportSendParams, TransportTxMode, Pacer

hammer-plugin-session
  IpSessionMain { session, lookup, transport }
  IP Session/Transport composition and backlink validation
  transport -> Session TX/close/reset/delete notifications

hammer-plugin-tcp / hammer-plugin-udp
  concrete Transport implementation on TcpMain / UdpMain
  protocol worker state, send-param calculation and header construction
  existing TcpOutputNode / UdpOutputNode consume BufferIndex frames only
```

依赖方向固定为：

```text
TCP/UDP -> hammer-plugin-session -> hammer-service
TCP/UDP -> hammer-service::transport traits and values
hammer-service -X-> hammer-plugin-session/TCP/UDP
```

`hammer-service` 不能在一个非泛型静态 Graph Node 中直接调用 plugin 的 concrete `Transport`
implementation。若不允许 VFT、函数指针、`dyn` trait 和反向 crate 依赖，这个调用不可能由 service
向 plugin 发起。Hammer 因而把 VPP 的调用位置拆为两个静态方向，但保留同一数据语义：

- concrete TCP/UDP worker 先计算 `TransportSendParams`，通过 `IpSessionMain` 更新对应 Session；
- `session-queue` 使用这些 protocol-neutral send facts 从 SVM FIFO 生成 packet buffer；
- concrete TCP/UDP output node 对收到的 packet buffer 静态调用自己的 `Transport::push_header`。

这是对 VPP ownership 的 Rust 化，不得伪称 VPP 具有额外 transport node。`send_params`、
`push_header` 的 owner 仍是 transport；Session 仍拥有 TX FIFO、packetization、event 重排和 output
next。

## 4. Service 类型

ADR-0039 不重新定义 `Session<O>`、`SessionWorker<O>` 或 `SessionMain<O>`。完整字段只以
ADR-0038 第 3.5 节为准；其中已经包含 SVM FIFO、SVM MQ index、event/control pools、new/old lists、
TX context、pending buffer/next、DMA、migration 与 lifecycle state。本 ADR 只增加 queue/output 边界
需要的 `Session::tx_params` 和 per-buffer fact：

```rust
use std::sync::OnceLock;

use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::SvmMsgQ;

static SESSION_MAIN: OnceLock<SessionMain<u32>> = OnceLock::new();

impl SessionMain<u32> {
    // VPP source: session.c:2274-2294 and session.h:310.
    // All fallible construction completes before the global is installed.
    pub fn init(
        config: SessionConfig,
        worker_mq_segment: SvmFifoSegment,
    ) -> Result<(), SessionError>;

    #[inline(always)]
    pub fn global() -> Result<&'static Self, SessionError>;
}

impl<O> SessionMain<O> {
    // VPP source: session.h:341-345, always_inline session_main_get_worker.
    #[inline(always)]
    pub fn worker(&self, thread_index: u32) -> Option<&SessionWorker<O>>;

    // The runtime's current thread owns the selected UnsafeCell entry.
    #[inline(always)]
    pub fn worker_mut(&self, runtime: &mut DataPlaneMain) -> &mut SessionWorker<O>;

    // VPP source: session.h:355-359.
    #[inline(always)]
    pub fn event_queue(&self, thread_index: u32) -> Option<&SvmMsgQ>;

    #[inline(always)]
    pub fn is_enabled(&self) -> bool;

    // VPP source: session.c:1826-1847. Stores metadata only, never behavior.
    pub fn register_transport(
        &self,
        session_type: u8,
        tx_mode: TransportTxMode,
        output_next: u16,
    ) -> Result<(), SessionError>;
}
```

`SessionMain::init` 与 `SessionMain::global` 是唯一 Main lifecycle API；不存在额外 publication API 或
provider。

每个 Session 保存 transport 最近一次在 owner worker 上提交的 send facts。它不保存 TCP/UDP
connection，也不解释协议状态。ADR-0038 的 `Session<O>` 增加 private
`tx_params: Option<TransportSendParams>`，并暴露下列 hot-path accessors；这里不声明第二个 `Session`
类型：

```rust
impl<O> Session<O> {
    // Semantic source: third_party/vpp/src/vnet/session/transport.h:36-55.
    #[inline(always)]
    pub fn tx_params(&self) -> Option<TransportSendParams>;

    #[inline(always)]
    fn set_tx_params(&mut self, params: TransportSendParams);
}

// Hammer adaptation: protocol-neutral facts carried with one generated packet.
// VPP keeps equivalent facts reachable through session_t/transport_connection_t
// while session-queue calls push_header before fanout; this is not a VPP type.
// It is not a Session event and it does not own payload bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionTxPacket {
    pub session: SessionHandle,
    pub connection_index: u32,
    pub payload_len: u32,
}
```

`SessionTxPacket` 写入 `session-queue` 新建 packet buffer 的现有 opaque 区。payload 已经从
Session-owned SVM FIFO 进入 Data Plane Buffer；禁止中间 `Vec<u8>` 或 transport-private payload copy。

## 5. Session Worker 调度接口

```rust
impl<O> SessionWorker<O> {
    // VPP source: session_node.c:1975-2030, fixed MQ snapshot and event import.
    pub fn drain_event_queue(
        &mut self,
        queue: &SvmMsgQ,
    ) -> Result<usize, SessionError>;

    // VPP source: session_node.c:2033-2177.
    // Owns control -> new IO -> old IO ordering and the frame budget.
    pub fn run_queue(
        &mut self,
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;

    // Called only by the concrete transport on this Session's owner worker.
    // Semantic source: transport.h:193-208 and session_node.c:1496-1537.
    // VPP pulls these facts in session-queue; Hammer pushes the same facts to
    // preserve static crate dependencies.
    #[inline]
    pub fn update_tx(
        &mut self,
        session: SessionHandle,
        connection_index: u32,
        params: TransportSendParams,
    );

    // VPP source: session_node.c:1471-1676.
    fn packetize_tx(
        &mut self,
        runtime: &mut DataPlaneMain,
        event_index: u32,
        remaining_packets: usize,
    ) -> SessionTxResult;

    // VPP source: session_node.c:1436-1452 and 1661-1675.
    #[inline]
    fn defer_event(&mut self, event_index: u32);

    #[inline]
    fn retry_event_first(&mut self, event_index: u32);

    #[inline]
    fn complete_event(&mut self, event_index: u32);

    // VPP source: session_node.c:1455-1468.
    #[inline(always)]
    fn push_pending_buffer(&mut self, buffer: u32, next: u16);
}
```

`run_queue` 固定执行 VPP 顺序：更新时间、按调用开始时的 MQ size snapshot drain、处理 control、处理
new IO、只处理本轮开始前已存在的 old IO、flush pending buffers、更新 adaptive state。new/old list
直接使用现有 `LinkedList` 的 iterator；不增加 `walk` API，不按 protocol 拆第二组 list。

`TransportSendParams` 是 owner worker 上的 authoritative snapshot。transport 在 connection 建立、
ACK/window/congestion/pacing 变化以及 output commit 后更新它；Session Queue 与 transport 不跨 worker
共享这份可变状态。尚无 snapshot 的 TX event 按 `NoData` defer。stale Session event 释放并忽略；直接
调用时 backlink 不匹配是 transport owner bug，使用带 Session/connection identity 的 assertion，不滥用
VPP 的 `SESSION_E_OWNER`。无数据与无 buffer 分别返回 `SessionTxResult::NoData` 和
`SessionTxResult::NoBuffers`。event 完成、defer 或 retry 都由 `run_queue` 在同一 borrow 中决定。

## 6. 三个 Node

```rust
// VPP source:
// - session_node.c:2033-2177, session_queue_node_fn.
// - session_node.c:2238-2289, session_queue_process.
// - session_node.c:2291-2307, session_queue_pre_input_inline.

#[hammer_component_macros::graph_node(
    graph = service,
    kind = driver,
    state = disabled,
    name = "session-queue"
)]
pub struct SessionQueueNode;

#[hammer_component_macros::graph_node(
    graph = service,
    kind = pre_input,
    state = disabled,
    name = "session-queue-main"
)]
pub struct SessionQueueMainNode;

#[hammer_component_macros::process_node(name = "session-queue-process")]
fn session_queue_process(
    main: &mut DataPlaneMain,
) -> impl Future<Output = RuntimeResult<()>> + Send + 'static;

impl Node for SessionQueueNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;
}

impl Node for SessionQueueMainNode {
    // VPP source: session_node.c:2291-2300, static_always_inline wrapper.
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;
}
```

`SessionQueueNode::process` 调用 `SessionMain::global()`，按 runtime thread index 直接借用当前 worker，
再调用 `run_queue`。完整 queue loop 不标 `inline`。

`session-queue-process` 使用 runtime 已有 Process event receiver，接受 `RunOnMain`、`Stop` 和一秒
timeout。它不承载 Session payload，不创建 mailbox。由于 Hammer Process future 不能跨 `.await`
持有 `&mut DataPlaneMain`，唤醒 thread-zero queue 使用当前 Session Worker 已有的 readiness fd；FileMain
callback 只把 `session-queue` 标为 interrupt pending。这对应 VPP process 定期调用
`session_queue_run_on_main` 的效果，不是假造 transport dispatch。

`session-queue-main` 只在 worker 0 event queue 已存在时调用相同 `run_queue`。VPP 当前源码只声明该
pre-input node 为 disabled，并未在 `session_node_enable_disable` 中启用它；Hammer 不自行补一个启用
条件。

## 7. Plugin-session 接线

```rust
// VPP source: session.h:310 and session.c:490-506,996-1184.
pub struct IpSessionMain {
    session: &'static SessionMain<u32>,
    lookup: IpSessionLookup,
    transport: &'static IpTransportMain,
}

impl IpSessionMain {
    pub fn init(
        session_config: SessionConfig,
        table_config: IpSessionTableConfig,
        transport_config: IpTransportConfig,
        worker_mq_segment: SvmFifoSegment,
    ) -> Result<(), SessionError>;

    #[inline(always)]
    pub fn global() -> Result<&'static Self, SessionError>;

    #[inline(always)]
    pub fn session(&self) -> &SessionMain<u32>;

    // Transport -> Session direction. Validates the numeric backlink and
    // mutates only the current runtime worker's Session entry.
    #[inline]
    pub fn update_tx(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
        params: TransportSendParams,
    );

    pub fn register_transport_output(
        &self,
        runtime: &DataPlaneMain,
        session_type: u8,
        tx_mode: TransportTxMode,
        output: NodeId,
    ) -> Result<(), SessionError>;

    pub fn notify_closed(&self, session: SessionHandle, connection_index: u32);
    pub fn notify_reset(&self, session: SessionHandle, connection_index: u32);
    pub fn notify_deleted(&self, session: SessionHandle, connection_index: u32);
}
```

`register_transport_output` 先解析已经静态注册的 output `NodeId`，给 `session-queue` 编译 next arc，
最后一次性提交 `session_type -> { tx_mode, next }` metadata。它不注册 Graph Node，也不保存 protocol
callback。TCP/UDP 依赖这个 concrete IP composition；service 不依赖它。

## 8. TCP/UDP 输出接线

concrete transport 在 owner worker 上计算 send facts，再通知 Session：

```rust
// VPP source:
// - transport.h:193-208, transport_connection_snd_params.
// - tcp.c:1402-1431 and udp.c:611-632, concrete transport operations.
impl TcpMain {
    #[inline]
    fn update_session_tx(
        &'static self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let params = <Self as Transport<IpTransportEndpointConfig>>::send_params(
            self,
            connection_index,
            runtime.thread_index(),
        );
        IpSessionMain::global()?.update_tx(runtime, session, connection_index, params);
        Ok(())
    }
}

impl UdpMain {
    #[inline]
    fn update_session_tx(
        &'static self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError>;
}
```

已有 output node 只处理 `BufferIndex`：

```rust
// VPP source:
// - third_party/vpp/src/vnet/session/session_node.c:1647-1649,
//   transport header is applied to prepared buffers.
// - third_party/vpp/src/vnet/tcp/tcp_output.c:2400-2446 and
//   third_party/vpp/src/vnet/udp/udp_output.c:202-246,
//   output nodes consume u32 packets.
impl TcpOutputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;
}

impl UdpOutputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize;
}
```

每个 output node 从 buffer opaque 读取 `SessionTxPacket`，通过 `IpSessionMain` 验证 Session/connection
backlink，然后在 concrete `TcpMain`/`UdpMain` 上静态调用 `Transport::push_header`。随后按现有逻辑
提交 protocol TX state、通过 `IpSessionMain::update_tx` 刷新 send facts、写入网络层 endpoint facts 并
送往 IP lookup。这里没有 Session event、event list iterator、empty-frame wake 或第二条 queue。

VPP 在 Session Queue 内调用 `push_header`；Hammer 把该 concrete trait 调用移动到已有 output node，
原因是 service node 不能反向依赖 plugin，且已经明确禁止 runtime dispatch table。packet buffer、FIFO
ownership、send budget、next fanout 与 output node 的 packet-vector ABI 不变。

`TransportTxMode::Internal` 和依赖 `custom_tx` 直接生成 packet 的 transport 不在本 ADR 开放；注册时
返回 `SessionError::NotSupported`。本 ADR 只接入 TCP 的 peek TX 与 UDP 的 datagram TX，不能用一条
虚构的 node 路径掩盖尚未设计的 custom TX。

## 9. Enable/disable

```rust
// VPP source: session.c:2199-2248 and 2251-2314.
impl SessionMain<u32> {
    pub fn enable(
        &'static self,
        runtime: &mut DataPlaneMain,
    ) -> Result<(), SessionError>;

    pub fn disable(
        &'static self,
        runtime: &mut DataPlaneMain,
    ) -> Result<(), SessionError>;
}
```

默认值对齐 `session_main_init`：`is_enabled = false`、`session_enable_asap = false`、
`poll_main = false`。状态转换对齐 `session_node_enable_disable`：

| 位置 | `session-queue` | `session-queue-process` | `session-queue-main` |
|---|---|---|---|
| enable 前 | Disabled | Disabled | Disabled |
| thread zero，有 Data Worker，`poll_main=false` | Interrupt | started | Disabled |
| thread zero，有 Data Worker，`poll_main=true` | Polling | started | Disabled |
| Data Worker | Polling | 不运行 | Disabled |
| 只有 thread zero | Polling | 不需要 fallback | Disabled |
| disable 后 | Disabled | Stop | Disabled |

enable 在 WorkerBarrier 内先验证 worker MQ、readiness fd、三个 NodeId 和所有已注册 output next；验证
完成后状态发布不再失败。disable 先禁用 queue，再停止 process，最后释放 readiness/MQ 资源。worker
仍可能运行 node 时不得 unmap SVM segment。

## 10. Error 与 inline

control path 统一使用 ADR-0038 定义于 `hammer-service::session` 的 `SessionError`：

```rust
// VPP source: session_types.h:514-576, foreach_session_error.
// SESSION_E_NONE maps to Ok; internal Rust APIs never carry an i32 retval.
#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    // The complete variant list is defined by ADR-0038.
}

// VPP source: session_node.c:944-949.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTxResult {
    Sent { packets: usize },
    NoData,
    NoBuffers,
}

// VPP source: session_node.c:925-942.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionQueueNodeError {
    Tx,
    Timer,
    NoBuffer,
}
```

`SessionTxResult` 是 TX outcome，不是 error。`SessionQueueNodeError` 是数据面 node counter code，不进入
`Result`。stale event 被释放并忽略；pool/list 越界、释放 linked element、缺少已承诺的 next 是 owner
invariant，使用 assertion。

inline 仅用于 VPP 已标为 inline/always-inline 且处于每 event/packet hot path 的操作：

| Hammer 方法 | 属性 | VPP 依据 |
|---|---|---|
| `SessionMain::global/worker/worker_mut/event_queue` | `#[inline(always)]` | `session.h:341-359` |
| `SessionWorker::push_pending_buffer` | `#[inline(always)]` | `session_node.c:1455-1468` |
| event defer/retry/complete、transport `update_session_tx` | `#[inline]` | `session.h:392-457`、`transport.h:193-208` |
| `SessionQueueMainNode::process` | `#[inline(always)]` | `session_node.c:2291-2300` |
| queue loop、MQ drain、enable/disable、registration | 不标注 | cold path 或有界循环 |

## 11. 删除项

迁移时删除当前旧路径中的：

- `SessionQueueTransportDispatch`；
- `SessionQueueDispatchFn`、`SessionQueueUpdateTimeFn`；
- `install_worker_attachment`、`remove_worker_attachment`；
- `NodeRuntime` 中缓存的 `SessionMain` 地址；
- output node 消费或查询 Session event 的任何入口；
- worker init 中动态安装 transport callback 或强制启用 `session-queue` 的逻辑。

保留并改造已有 `TcpOutputNode`、`UdpOutputNode`；不创建新的 transport graph node。

## 12. 待批准的新 API

实现前需要明确批准以下最小新增面：

- `SessionTxPacket`：跨 `session-queue -> output` graph edge 的 per-buffer Session/connection facts；现有
  buffer opaque 没有这组事实，而 output node 不能再读取 Session event list；
- `SessionWorker::update_tx` 与 `IpSessionMain::update_tx`：替代 service 保存 transport callback，保持
  `TCP/UDP -> plugin-session -> service` 单向依赖；
- `SessionWorker::run_queue`：三个 service node 共用的非泛型 queue algorithm；
- `SessionMain::enable/disable/register_transport`：VPP lifecycle 与 `{tx_mode, output_next}` metadata。

除上述 API 外，本 ADR 不批准新的 queue、node、trait、Main、wrapper 或 error 类型。

## 13. VPP 源码依据

- `third_party/vpp/src/vnet/session/session.h:82-176`：`session_worker_t`；
- `third_party/vpp/src/vnet/session/session.h:211-320`：`session_main_t` 与三个 queue node declaration；
- `third_party/vpp/src/vnet/session/session.h:341-457`：worker/MQ accessors 与 event list helpers；
- `third_party/vpp/src/vnet/session/session_node.c:925-949`：node errors 与 TX outcomes；
- `third_party/vpp/src/vnet/session/session_node.c:1436-1676`：reschedule、packetize、pending buffer、
  transport header 与 no-buffer handling；
- `third_party/vpp/src/vnet/session/session_node.c:1745-1917`：control/IO dispatch 与 stale event；
- `third_party/vpp/src/vnet/session/session_node.c:1975-2177`：MQ drain、new/old 顺序、fanout 与 adaptive；
- `third_party/vpp/src/vnet/session/session_node.c:2238-2307`：process 与 pre-input wrapper；
- `third_party/vpp/src/vnet/session/session.c:1826-1847`：TX mode/output next registration；
- `third_party/vpp/src/vnet/session/session.c:2199-2314`：default disabled、enable/disable、enable-asap；
- `third_party/vpp/src/vnet/session/transport.h:36-55,193-208,231-240`：send params 与 output registration；
- `third_party/vpp/src/vnet/tcp/tcp.c:1402-1431,1590-1609`：TCP transport operations 与 output next；
- `third_party/vpp/src/vnet/udp/udp.c:611-658`：UDP transport operations 与 output registration；
- `third_party/vpp/src/vnet/tcp/tcp_output.c:2400-2446`：TCP output packet-vector ABI；
- `third_party/vpp/src/vnet/udp/udp_output.c:202-246`：UDP output packet-vector ABI；
- `third_party/vpp/src/vnet/tcp/tcp_input.c:105-132`：TCP 调用 Session reset/closed notification；
- `third_party/vpp/src/vnet/udp/udp.c:512-529`：UDP 调用 Session reset/closed notification。
