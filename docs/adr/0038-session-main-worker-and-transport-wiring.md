# ADR-0038: Session Main、Session Worker 与 Transport 的三层接线

- 日期：2026-09-21
- 状态：Accepted，Issue #354 实施；本次边界修订 Issue #356
- 范围：Session/Transport 核心，不包含 Application、AppWorker、Application Namespace、socket、
  Binary API 或外部 client
- VPP 参考：`third_party/vpp/src/vnet/session/session.h`、`session.c`、`session_node.c`、
  `session_lookup.c`、`session_table.c`、`transport.h`、`transport.c`、`transport_types.h`
- 前置 ADR：ADR-0035、ADR-0036、ADR-0037

本文定义并实施 Session、Session Worker、Session Main 与 Transport 的 ownership、worker handoff、
inline 和错误边界。Application 相关生命周期先保持在本文之外；本阶段只保留
Session/Transport 必须知道的 handle、FIFO ownership、listener/half-open 关系和 protocol
connection identity。

本文明确不把现有 Hammer 的 `SessionQueueTransportDispatch` 或旧 transport callback 表作为目标
Transport 接线。`SessionQueueNode` 仍是 service-owned Session scheduler 与 FIFO packetization owner；
其 graph/output 接线由 ADR-0039 定义。

## 1. 背景与问题

VPP 的三个事实必须同时保留：

1. `session_main_t` 是 process-wide authority，拥有 worker contexts、Session 类型元数据、
   transport registration progress、per-worker event queue segment 和配置；它不拥有某个 TCP
   或 UDP connection pool。
2. `session_worker_t` 是一个 runtime thread 的 Session owner，拥有该 worker 的 Session pool、
   event state、migration state、TX context；跨 worker 调用通过 event/RPC 或 migration
   handoff 完成。
3. `transport_connection_t` 是 transport-owned base state，保存 connection identity、Session
   反向关联、connection index、worker identity、flags 和 pacer；其中 `spacer_t`、flags 和
   backlink 是通用事实，网络 tuple 只是一个可替换的 endpoint payload。Session 只保存连接的
   数值事实，不接管 TCP/UDP 的 pool、timer 或 packet state。

当前 Hammer 的问题不是缺少一个更大的 common struct，而是职责混在一起：

- service `SessionMain` 直接读取 `TransportVft`，以 function pointer table 调用 TCP/UDP；
- `SessionWorker` 同时承载 Session 生命周期和 transport-specific dispatch；
- `SessionQueueNode` 被当成 transport 调度器，迫使 service 持有每个协议的 callback；
- plugin/session 的 IP lookup 与 `IpTransportMain` 没有成为 Session 生命周期的唯一接线点；
- TCP/UDP 既有自己的 worker state，又通过 service VFT 反向暴露一套不完整的 control surface。

目标是让每一层只有一个 owner，并让调用方向固定，而不是继续拆出没有 ownership 的 wrapper。

## 2. 决定

采用三层结构：

```text
hammer-service
  SessionMain / SessionWorker / Session / Transport<T> contract
  protocol-neutral lifecycle, FIFO ownership, worker handoff

hammer-plugin-session
  IpSessionMain
  IP session lookup/table and endpoint binding
  IpTransportMain and IP Session/Transport composition

hammer-plugin-tcp / hammer-plugin-udp
  TcpMain / TcpWorker / TcpConnection
  UdpMain / UdpWorker / UdpConnection
  concrete Transport<T> implementations
  packet, timer, lookup and protocol state
```

调用方向只能是：

```text
TCP/UDP -> plugin-session -> hammer-service
TCP/UDP -> hammer-service::Transport<T> contract
```

service 不依赖 plugin-session、TCP 或 UDP；plugin-session 不依赖 TCP/UDP；TCP/UDP 显式依赖
plugin-session 和 service。plugin-session 是 IP Session 与 IP Transport 的组合 owner，但不替
TCP/UDP 持有 protocol state。

`Transport<T>` 是静态 Rust trait contract。Session core 接收 concrete transport 调用方提交的
lifecycle 与 send facts，不从 registry 取 VFT/function pointer，不保存 `dyn Transport`，也不保存
erased protocol state。Session Queue 拥有 FIFO packetization；TCP/UDP 的 send-param 计算、transport
header 与 protocol state transition 由各自 protocol worker/output node 执行。

## 3. VPP ownership 依据

### 3.1 Session Main

VPP `session_main_t`（`session.h:211-308`）同时保存 worker contexts、Session type 到 output
next 的 metadata、connectionless transport 的 control thread、pool realloc 同步、worker MQ
segment、transport protocol allocation progress、session enable/config、table sizing、local
endpoint sizing 和 port range。

对 Hammer 的取舍如下：

- worker contexts、Session metadata、event queue publication、pool growth barrier 和 lifecycle
  config 属于 service `SessionMain`；
- IP lookup/table、endpoint key、namespace/FIB binding 属于 plugin-session；
- TCP/UDP connection/listener/half-open/timer/packet pool 属于对应 protocol Main；
- Session Main 不携带 IP key、FIB 字段或 protocol callback。

VPP `session_add_transport_proto`（`session.c:1905-1921`）只扩展 protocol metadata 和每 worker
待处理项，不把 protocol connection state 放入 Session Main。Hammer 保留这一 ownership 关系，
但用静态 trait 和 concrete plugin call 替代 C VFT。

### 3.2 Session Worker

VPP `session_worker_t`（`session.h:82-176`）包含 worker Session pool、`event_queue`、时间快照、
transport time-update subscriptions、每协议待处理 handle、event lists、pending TX、first-worker
connect/migration storage 和同步状态。统计字段不进入本阶段设计。

VPP 的 `session_main_get_worker`（`session.h:341-353`）按 thread index 取得固定 worker entry；
它不是从共享锁中临时借出任意 worker。Hammer 保持相同 ownership：Data Worker 只能取得自己的
`SessionWorker`，Main Thread 修改 worker-visible publication 时使用现有 WorkerBarrier。

### 3.3 Transport connection

VPP `transport_connection_t`（`transport_types.h:81-163`）保存 connection tuple/opaque identity、
Session index、transport pool index、thread index、flags 和 pacer，且把 protocol-specific state
放到后续 cache line。Hammer 的 `TcpConnection`/`UdpConnection` 保留这个边界：service 只存
protocol id、connection index、worker index 和 Session handle 等数值事实；具体 connection 由
protocol Main 查询和释放。

### 3.4 Event 与 migration

VPP `session_send_evt_to_thread`（`session.c:34-85`）按 event family 写目标 worker 的 MQ；MQ
满或加锁失败立即返回负 retval，成功后在 interrupt 状态唤醒 Session node。Migration request 和
handling storage 位于 worker（`session.h:143-159`），而不是由一个额外的共享队列 owner 管理。

Hammer 的 `event_queue` 直接复用 `hammer_infra::svm::msg_queue::SvmMsgQ`，其共享存储由
`hammer_infra::svm::fifo_segment::SvmFifoSegment` 在 SSVM 映射中分配或 attach。`SessionMain`
拥有 worker MQ segment；每个 `SessionWorker` 只绑定该 segment 中属于自己的 `SvmMsgQ`，不创建
第二种队列，也不引入新的 queue/container。producer 使用 `SvmMsgQ::producer`，consumer 使用
`SvmMsgQ::sub`，descriptor 完成后按 SVM message-queue 规则释放。event record 只携带 Session
handle、protocol id、operation fact 和有界 metadata；不携带 callback、transport object 或
payload copy。

### 3.5 三层 Rust 类型与方法设计

下列代码块记录本次实施后的类型/方法形状。每个关键方法旁的注释同时给出 VPP 依据；
ADR-0038 新路径以静态 trait contract 接线，不通过旧 function-pointer table 调度。

#### 3.5.1 `hammer-service`

```rust
use std::cell::UnsafeCell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::SvmMsgQ;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;

pub const SESSION_INDEX_INVALID: u32 = u32::MAX;

// VPP source: session_types.h:514-576, foreach_session_error.
// SESSION_E_NONE maps to Ok and is deliberately not an error variant.
// No numeric discriminant is part of the internal Rust contract.
#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("unknown Session failure")]
    Unknown,
    #[error("connection refused")]
    Refused,
    #[error("Session operation timed out")]
    TimedOut,
    #[error("Session object or memory allocation failed")]
    Allocation,
    #[error("Session object is not owned by the caller")]
    Owner,
    #[error("no route")]
    NoRoute,
    #[error("no resolving interface")]
    NoInterface,
    #[error("local interface has no IP address")]
    NoIp,
    #[error("no local port is available")]
    NoPort,
    #[error("operation is not supported")]
    NotSupported,
    #[error("endpoint is not listening")]
    NotListening,
    #[error("Session does not exist")]
    NoSession,
    #[error("Application is not attached")]
    NoApplication,
    #[error("Application is already attached")]
    ApplicationAttached,
    #[error("local port is already in use")]
    PortInUse,
    #[error("IP address is already in use")]
    IpInUse,
    #[error("IP and port pair is already listening")]
    AlreadyListening,
    #[error("address is not in use")]
    AddressNotInUse,
    #[error("invalid value")]
    Invalid,
    #[error("invalid remote IP address")]
    InvalidRemoteIp,
    #[error("invalid Application Worker")]
    InvalidApplicationWorker,
    #[error("invalid Application Namespace")]
    InvalidNamespace,
    #[error("Session segment has no space for a FIFO pair")]
    SegmentNoSpace,
    #[error("new Session segment has no space for a FIFO pair")]
    NewSegmentNoSpace,
    #[error("Session segment creation failed")]
    SegmentCreate,
    #[error("Session was filtered")]
    Filtered,
    #[error("requested Session scope is not supported")]
    ScopeNotSupported,
    #[error("Binary API connection has no file descriptor")]
    BinaryApiNoFileDescriptor,
    #[error("Binary API file descriptor send failed")]
    BinaryApiSendFileDescriptor,
    #[error("Binary API registration does not exist")]
    BinaryApiRegistrationMissing,
    #[error("Session message allocation failed")]
    MessageQueueAllocation,
    #[error("TLS handshake failed")]
    TlsHandshake,
    #[error("eventfd allocation failed")]
    EventFdAllocation,
    #[error("extended transport configuration is missing")]
    ExtendedConfigMissing,
    #[error("crypto engine is missing")]
    CryptoEngineMissing,
    #[error("crypto certificate/key pair is missing")]
    CryptoKeyPairMissing,
    #[error("local-scope connect failed")]
    LocalConnect,
    #[error("Application Namespace secret is incorrect")]
    WrongNamespaceSecret,
    #[error("system call failed")]
    Syscall,
    #[error("transport is not registered")]
    TransportNotRegistered,
    #[error("maximum stream count reached")]
    MaxStreamsReached,
}

// VPP: session.h:468-521, session_handle_tu_t carries worker/session pool identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionHandle {
    pub worker_index: u32,
    pub session_index: u32,
}

// VPP: session.h:211-308, session_main_t configuration and per-protocol metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionConfig {
    pub worker_count: u32,
    pub configured_worker_mq_length: u32,
    pub worker_mq_segment_size: usize,
    pub event_ring_capacity: u32,
    pub event_element_size: u32,
    pub session_capacity: u32,
    pub preallocated_sessions: u32,
    pub session_enable_asap: bool,
    pub poll_main: bool,
    pub use_private_rx_mqs: bool,
    pub no_adaptive: bool,
    pub dma_enabled: bool,
}

// VPP: transport_endpoint_cfg_t is transport-owned data carried by a
// protocol-neutral session endpoint, transport_types.h:237-259.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionEndpoint<T> {
    transport: T,
    transport_protocol: u8,
}

impl<T> SessionEndpoint<T> {
    // VPP: endpoint config is copied into the transport operation boundary.
    #[inline(always)]
    pub const fn new(transport: T, transport_protocol: u8) -> Self;

    // VPP: transport endpoint access is a small value accessor.
    #[inline(always)]
    pub const fn transport(&self) -> &T;

    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8;
}

// VPP: session_table_t owns established and half-open hashes,
// session_table.c:13-129. The hash types remain generic at service level.
pub struct SessionTable<S, H> {
    sessions: S,
    half_open: H,
}

impl<S, H> SessionTable<S, H> {
    #[inline(always)]
    pub const fn new(sessions: S, half_open: H) -> Self;

    #[inline(always)]
    pub const fn sessions(&self) -> &S;

    #[inline(always)]
    pub const fn half_open(&self) -> &H;
}

// VPP: session.h:45-73, 82-176. These records replace VPP's pointer-bearing
// event elements and DMA arrays with indexes/facts owned by this worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionEventElement {
    pub event: SessionEvent,
    pub next: u32,
    pub previous: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionControlData {
    pub bytes: [u8; 86],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionRxSegment {
    pub buffer_index: u32,
    pub offset: u32,
    pub length: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionDmaTransfer {
    pub pending_tx_buffers: u32,
    pub pending_tx_nexts: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionTxContext {
    pub session: SessionHandle,
    pub connection_index: u32,
    pub transport_protocol: u8,
    pub tx_mode: TransportTxMode,
    pub send_params: TransportSendParams,
    pub max_dequeue: u32,
    pub left_to_send: u32,
    pub max_length_to_send: u32,
    pub dequeue_per_first_buffer: u16,
    pub dequeue_per_buffer: u16,
    pub segments_per_event: u16,
    pub buffers_needed: u16,
    pub buffers_per_segment: u8,
    pub datagram_header: [u8; 32],
}

// VPP: transport_tx_fn_type_t, transport_types.h:16-23.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTxMode {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

// VPP: transport_snd_flags_t, transport.h:29-34.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportSendFlags {
    pub deschedule: bool,
    pub postpone: bool,
}

// VPP: transport_send_params_t, transport.h:36-55. The two groups are the
// same C union's send-parameter/custom-TX interpretations; no protocol field
// is embedded in this service record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportSendParams {
    pub send_space: u32,
    pub tx_offset: u32,
    pub send_mss: u16,
    pub max_burst_size: u32,
    pub bytes_dequeued: u32,
    pub flags: TransportSendFlags,
}

// VPP: transport_service_type_t, transport_types.h:25-30.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportServiceType {
    VirtualCircuit,
    Connectionless,
}

// VPP: transport_options_t, transport.h:21-27. Names are presentation data;
// the service keeps only stable protocol facts used by SessionMain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportOptions {
    pub tx_mode: TransportTxMode,
    pub service_type: TransportServiceType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWorkerState {
    Polling,
    Interrupt,
    Idle,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionWorkerFlags {
    pub adaptive: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionMigrationRequest {
    pub old: SessionHandle,
    pub new: SessionHandle,
}

pub struct SessionMigrationState {
    pub requests: Vec<SessionMigrationRequest>,
    pub handling: Vec<SessionMigrationRequest>,
}

pub struct PoolReallocationState {
    pub workers_at_barrier: u32,
    pub workers_doing_work: u32,
}

// VPP: session.h:82-176. Every field has one service owner; no protocol
// connection, IP lookup table, callback, or function table is stored here.
pub struct SessionWorker<O = u32> {
    // session_worker_t.sessions
    sessions: Pool<Session<O>>,
    // session_worker_t.vpp_event_queue: the queue remains owned by the SVM
    // segment in SessionMain; this is the stable queue index bound to worker.
    event_queue_index: u32,
    // session_worker_t.last_vlib_time / last_vlib_us_time
    last_time: f64,
    last_time_us: u64,
    // session_worker_t.vm: service stores the runtime worker identity only;
    // the node receives its &mut DataPlaneMain at the runtime boundary.
    worker_index: u32,
    // session_worker_t.update_time_fns: subscription facts, not callbacks.
    transport_time_subscriptions: Vec<u8>,
    // session_worker_t.session_to_enqueue
    pending_io_sessions: Vec<SessionHandle>,
    // session_worker_t.timerfd / timerfd_file and adaptive scheduling state.
    timer_fd: i32,
    timer_fd_file: u64,
    timer: TimerWheel1t2w2048sl<SessionHandle>,
    flags: SessionWorkerFlags,
    state: SessionWorkerState,
    // session_worker_t.ctx
    tx_context: SessionTxContext,
    // session_worker_t.event_elts / ctrl_evts_data and list heads.
    event_elements: Pool<SessionEventElement>,
    control_event_data: Pool<SessionControlData>,
    control_events: LinkedList<u32>,
    new_events: LinkedList<u32>,
    old_events: LinkedList<u32>,
    pending_connects: LinkedList<u32>,
    events_pending_main: LinkedList<u32>,
    // session_worker_t.pending_tx_buffers / pending_tx_nexts
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
    // session_worker_t.n_pending_connects / app_wrks_pending_ntf is kept as
    // protocol-neutral pending-count/notification facts; app policy is out.
    pending_connect_count: u32,
    pending_notifications: Vec<u64>,
    // session_worker_t.rx_segs
    rx_segments: Vec<SessionRxSegment>,
    // session_worker_t.session_migrate_requests/handling + lock. The lock
    // owns the queues it protects and is not an empty synchronization token.
    migration: hammer_infra::sync::SpinLock<SessionMigrationState>,
    config_index: u32,
    // session_worker_t.dma_* fields
    dma_enabled: bool,
    dma_transfers: Vec<SessionDmaTransfer>,
    dma_head: u16,
    dma_tail: u16,
    dma_size: u16,
    dma_batch_number: u16,
    dma_batch: u32,
    // session_worker_t.last_event_poll under SESSION_DEBUG.
    last_event_poll: f64,
}

// VPP: session.h:143-159. The request contains only copyable pool identities.
pub struct Session<O = u32> {
    handle: SessionHandle,
    // VPP: session_types.h:262-263 declares volatile session_state because
    // transport/app workers observe lifecycle changes without owning the same
    // C execution path. Rust expresses that visibility with an atomic; Rust
    // volatile reads alone would not establish a cross-thread happens-before.
    state: AtomicU8,
    session_type: u8,
    flags: SessionFlags,
    // VPP: session_types.h:240-244. These are the existing SVM FIFO type,
    // never hammer_infra::fifo_queue or a second legacy FIFO abstraction.
    rx_fifo: Option<SvmFifo>,
    tx_fifo: Option<SvmFifo>,
    connection_index: u32,
    // ADR-0039: owner transport's latest authoritative send facts. None means
    // no TX grant has been supplied yet; Session Queue defers the TX event.
    tx_params: Option<TransportSendParams>,
    listener_handle: SessionHandle,
    half_open_index: u32,
    // VPP: session_types.h:286-287. The session core never interprets this.
    opaque: O,
}

impl<O> Session<O> {
    // VPP: session_set_state, session.h:909-914. Release publishes the new
    // state; readers use acquire before acting on FIFO/connection facts.
    #[inline(always)]
    pub fn store_state(&self, state: SessionState) {
        self.state.store(u8::from(state), Ordering::Release);
    }

    // VPP: all direct session_state reads in session_node.c/application_worker.c.
    #[inline(always)]
    pub fn load_state(&self) -> SessionState {
        SessionState::from(self.state.load(Ordering::Acquire))
    }

    #[inline(always)]
    pub fn opaque(&self) -> &O {
        &self.opaque
    }

    #[inline(always)]
    pub fn opaque_mut(&mut self) -> &mut O {
        &mut self.opaque
    }

    #[inline(always)]
    pub fn rx_fifo(&self) -> Option<&SvmFifo> {
        self.rx_fifo.as_ref()
    }

    #[inline(always)]
    pub fn tx_fifo(&self) -> Option<&SvmFifo> {
        self.tx_fifo.as_ref()
    }
}

// VPP: session state enum, session_types.h:189-209. Ordering is retained so
// the hot-path comparisons (< READY, >= TRANSPORT_CLOSING) remain meaningful.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Created,
    Listening,
    Connecting,
    Accepting,
    Ready,
    Opened,
    TransportClosing,
    Closing,
    AppClosed,
    TransportClosed,
    Closed,
    TransportDeleted,
}

impl From<SessionState> for u8 {
    #[inline(always)]
    fn from(state: SessionState) -> Self {
        state as u8
    }
}

impl From<u8> for SessionState {
    #[inline(always)]
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Created,
            1 => Self::Listening,
            2 => Self::Connecting,
            3 => Self::Accepting,
            4 => Self::Ready,
            5 => Self::Opened,
            6 => Self::TransportClosing,
            7 => Self::Closing,
            8 => Self::AppClosed,
            9 => Self::TransportClosed,
            10 => Self::Closed,
            11 => Self::TransportDeleted,
            _ => panic!("invalid session state"),
        }
    }
}

// VPP: session/transport flags are facts, not dispatch callbacks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionFlags {
    pub connectionless: bool,
    pub half_open: bool,
    pub migrating: bool,
    pub rx_ready: bool,
    pub tx_ready: bool,
}

// VPP: session_event_t and session_send_evt_to_thread, session.c:34-85.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SessionEvent {
    pub event_type: u8,
    pub postponed: bool,
    pub protocol: u8,
    pub operation: u8,
    pub session: SessionHandle,
    pub control_data_index: u32,
    pub payload: [u64; 2],
}

// VPP source: session.c:34-85. Lock contention and queue capacity are
// enqueue ownership outcomes, not foreach_session_error values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEventEnqueue {
    Enqueued,
    Busy,
    Full,
}

// VPP: transport_types.h:64-72 and transport.h:273-346. Pacing is a
// transport-neutral connection fact; IP/TCP/UDP only choose how to update it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pacer {
    pub bytes_per_second: u64,
    pub bucket: i64,
    pub last_update: u64,
    pub tokens_per_period: f32,
    pub min_burst: u32,
    pub max_burst: u32,
}

impl Pacer {
    // VPP: transport_connection_is_tx_paced/clear_descheduled,
    // transport.h:273-346.
    #[inline(always)]
    pub const fn is_enabled(&self) -> bool;

    #[inline(always)]
    pub fn update(&mut self, now: u64) -> u32;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportConnectionFlags {
    pub tx_paced: bool,
    pub no_lookup: bool,
    pub descheduled: bool,
    pub connectionless: bool,
    pub error: bool,
}

// VPP: transport_connection_t, transport_types.h:81-163. The endpoint and
// opaque payload are associated types/parameters so this common base carries
// no IP address, FIB, TCP, or UDP field.
pub struct TransportConnection<E, O = u32> {
    pub session: SessionHandle,
    pub connection_index: u32,
    pub worker_index: u32,
    pub flags: TransportConnectionFlags,
    pub endpoint: E,
    pub pacer: Pacer,
    pub opaque: O,
}

impl<E, O> TransportConnection<E, O> {
    // VPP: transport.h:156-225, 329-346.
    #[inline]
    pub fn is_descheduled(&self) -> bool;

    #[inline]
    pub fn is_connectionless(&self) -> bool;

    #[inline(always)]
    pub fn is_tx_paced(&self) -> bool;

    #[inline(always)]
    pub fn clear_descheduled(&mut self, now: u64);
}

// VPP: transport_main_t is a concrete process instance, while transport.h exposes
// the operation contract. The service layer owns only this abstraction.
pub trait TransportMain: Sized {
    type Config;
    type Endpoint;
    type LocalEndpoint;

    // VPP: transport_init, transport.c:1251-1290. Configuration is owned by
    // the concrete transport plugin, not by protocol-neutral service state.
    fn init(config: Self::Config) -> Result<(), SessionError>;

    // Only a real process-global concrete instance may return 'static.
    fn global() -> Result<&'static Self, SessionError>;

    // VPP: transport_mark_used_local_endpoint, transport.c:717-743.
    fn mark_used(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError>;

    // VPP: transport_share_local_endpoint, transport.c:745-763.
    fn share(&self, endpoint: &Self::Endpoint);

    // VPP: transport_release_local_endpoint, transport.c:687-715.
    fn release(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError>;

    // VPP: transport_alloc_local_endpoint, transport.c:892-952.
    fn allocate_local(
        &self,
        endpoint: Self::Endpoint,
    ) -> Result<Self::Endpoint, SessionError>;

}

// VPP: transport protocol operations in transport.h:131-154 and the VFT
// members in transport.h:60-122. This is a static Rust capability contract;
// it is not a stored function table or dynamic object.
pub trait Transport<T> {
    type Connection;
    type Attribute;

    fn options(&self) -> TransportOptions;
    // VPP source: tcp.c:840-885 and udp.c:401-463 return either an index or
    // SESSION_E_*. Rust separates the two domains instead of encoding errors in i32.
    fn connect(
        &self,
        endpoint: &T,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn connect_stream(
        &self,
        endpoint: &T,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn start_listen(
        &self,
        endpoint: &T,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;
    // VPP: these operations are notifications with no retval; protocol
    // failures are reported through Session state or protocol-owned paths.
    fn half_close(&self, connection_index: u32, worker_index: u32);
    fn close(&self, connection_index: u32, worker_index: u32);
    fn reset(&self, connection_index: u32, worker_index: u32);
    fn cleanup(&self, connection_index: u32, worker_index: u32);
    fn cleanup_half_open(&self, connection_index: u32);

    // VPP: push_header/send_params/update_time/flush_data/custom_tx/app_rx_evt.
    fn push_header(
        &self,
        connection_index: u32,
        worker_index: u32,
        buffers: &mut [u32],
        available_bytes: u32,
    ) -> u32;
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams;
    fn update_time(&self, now: f64, worker_index: u32);
    fn flush_data(&self, connection_index: u32, worker_index: u32);
    // VPP custom_tx returns packet work, not an error code.
    fn custom_tx(&self, session: SessionHandle, params: &mut TransportSendParams) -> usize;
    fn app_rx_event(
        &self,
        connection_index: u32,
        worker_index: u32,
    ) -> Result<(), SessionError>;

    // VPP: transport_get_connection/listener/half_open and endpoint/attribute
    // retrieval wrappers, transport.h:145-190.
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection>;
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn endpoint(&self, connection_index: u32, worker_index: u32) -> (T, T);
    fn listener_endpoint(&self, connection_index: u32) -> (T, T);
    fn attribute(
        &self,
        connection_index: u32,
        worker_index: u32,
        attribute: &mut Self::Attribute,
    ) -> Result<(), SessionError>;
}

// VPP: session_main_t, session.h:211-308. IP table sizing, local endpoint
// sizing, source-port range, NAT lookup, API message ids, and app namespace
// state are deliberately absent: those belong to plugin-session or later app
// layers, not this protocol-neutral authority.
pub struct SessionMain<O = u32> {
    config: SessionConfig,
    workers: Vec<UnsafeCell<SessionWorker<O>>>,
    // session_main_t.session_tx_fns: mode metadata only; no VFT/function ptr.
    session_tx_modes: UnsafeCell<Vec<TransportTxMode>>,
    // session_main_t.session_type_to_next
    session_type_to_next: UnsafeCell<Vec<u32>>,
    // session_main_t.transport_cl_thread
    transport_cl_thread: u32,
    // session_main_t.pool_realloc_at_barrier / doing_work / lock. The typed
    // state is protected together; no unit lock is used.
    pool_reallocation: hammer_infra::sync::SpinLock<PoolReallocationState>,
    // session_main_t.wrk_mqs_segment
    worker_mq_segment: SvmFifoSegment,
    // session_main_t.last_transport_proto_type
    last_transport_protocol: AtomicU8,
    // session_main_t lifecycle/config flags
    is_enabled: AtomicBool,
    is_initialized: bool,
    dump_worker_segments: bool,
}

static SESSION_MAIN: OnceLock<SessionMain<u32>> = OnceLock::new();

impl SessionMain<u32> {
    // VPP: session_main_init/session_manager_main_enable, session.c:2018-2050, 2274-2294.
    pub fn init(
        config: SessionConfig,
        worker_mq_segment: SvmFifoSegment,
    ) -> Result<(), SessionError>;

    pub fn global() -> Result<&'static Self, SessionError>;
}

impl<O: Default> SessionMain<O> {

    // VPP: session worker MQ segment setup, session.h:238-239; Hammer SVM allocation is
    // SvmFifoSegment::allocate_message_queue, fifo_segment.rs:815-837.
    pub fn allocate_event_queues(&mut self) -> Result<(), SessionError>;

    // VPP: session_main_get_worker, session.h:341-353.
    #[inline(always)]
    pub fn worker(&self, worker_index: u32) -> Option<&SessionWorker<O>>;

    // VPP: pool publication is control-plane/barrier work, not a packet hot path.
    pub fn worker_mut(
        &self,
        runtime: &mut DataPlaneMain,
    ) -> &mut SessionWorker<O>;

    // VPP: session_main_get_vpp_event_queue, session.h:355-359.
    // Hammer SVM: SvmFifoSegment::message_queue, fifo_segment.rs:895-900.
    #[inline(always)]
    pub fn event_queue(&self, worker_index: u32) -> Option<&SvmMsgQ>;

    // VPP: session_add_transport_proto/session_register_transport metadata,
    // session.c:1883-1921. Registration stores only compact mode/next facts;
    // the concrete protocol owns its implementation and pool.
    pub fn register_transport_type(
        &self,
        protocol: u8,
        tx_mode: TransportTxMode,
        output_next: u32,
    ) -> Result<u8, SessionError>;

    // VPP: session_main_get_worker and pool reallocation barrier fields,
    // session.h:229-236, session.c:2018-2050.
    pub fn begin_pool_reallocation(&self);
    pub fn finish_pool_reallocation(&self);

    // VPP: session_main_is_enabled, session.h:361-365. Acquire observes the
    // control-thread publication before a worker processes events.
    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.is_enabled.load(Ordering::Acquire)
    }

    // VPP: session_alloc/session_alloc_for_connection, session.h:461-466 and session.c:490-506.
    pub fn allocate(
        &self,
        runtime: &mut DataPlaneMain,
        state: SessionState,
        protocol: u8,
    ) -> Result<SessionHandle, SessionError>;

    // VPP: transport/session backlink assignment, session.c:502-505.
    pub fn attach_transport(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        protocol: u8,
        connection_index: u32,
    ) -> Result<(), SessionError>;

    // VPP: session_transport_cleanup/session_program_cleanup ordering, session.c:344-385.
    pub fn detach_transport(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
    ) -> Result<(), SessionError>;

    // VPP: session_send_evt_to_thread, session.c:34-85; SvmMsgQ is the only queue owner.
    pub fn enqueue_event(
        &self,
        worker_index: u32,
        event: SessionEvent,
    ) -> Result<SessionEventEnqueue, SessionError>;

    // VPP: session migration request storage, session.h:143-159.
    pub fn program_migration(
        &self,
        request: SessionMigrationRequest,
    ) -> Result<(), SessionError>;

    // VPP: session_main_flush_enqueue_events, session.h:635-636.
    pub fn flush_enqueue_events(
        &self,
        protocol: u8,
        worker_index: u32,
    ) -> Result<(), SessionError>;
}

impl<O> SessionWorker<O> {
    // VPP: session_get/session_get_if_valid, session.h:468-499.
    #[inline(always)]
    pub fn session(&self, session_index: u32) -> Option<&Session<O>>;

    // VPP: same-thread mutable pool access; caller already owns this worker.
    #[inline(always)]
    pub fn session_mut(&mut self, session_index: u32) -> Option<&mut Session<O>>;

    // VPP: session_wrk_handle_mq and session queue processing, session.h:459 and
    // session_node.c:1926-2050. The queue is borrowed directly from SessionMain's segment.
    pub fn handle_event(&mut self, queue: &SvmMsgQ) -> Result<usize, SessionError>;

    // VPP: session_evt_alloc_ctrl/new/old and session_evt_add_old,
    // session.h:392-457. The lists contain event-pool indexes and never own
    // a second event representation.
    pub fn allocate_control_event(&mut self, event: SessionEvent) -> u32;
    pub fn allocate_new_event(&mut self, event: SessionEvent) -> u32;
    pub fn allocate_old_event(&mut self, event: SessionEvent) -> u32;

    // VPP: session_main_get_worker and session_get_from_handle_safe,
    // session.h:341-353, 488-521. The caller must already be on this worker
    // for mutable access; cross-worker callers enqueue a fact instead.
    #[inline(always)]
    pub fn session_from_handle(&self, handle: SessionHandle) -> Option<&Session<O>>;

    // VPP: session_set_state and FIFO notification paths, session.h:909-935.
    #[inline(always)]
    pub fn store_state(
        &self,
        handle: SessionHandle,
        state: SessionState,
    ) -> Result<(), SessionError>;

    // VPP: session_enqueue_stream_connection/session_enqueue_dgram_connection,
    // session.h:777-890. The worker records a handle/fact; Session Queue later
    // packetizes the same SVM FIFO and emits BufferIndex values to output.
    #[inline(always)]
    pub fn enqueue_ready(
        &mut self,
        handle: SessionHandle,
        protocol: u8,
    ) -> Result<(), SessionError>;

    // VPP: session_evt_ctrl_data_alloc/session_evt_ctrl_data_free,
    // session.h:407-437.
    pub fn allocate_control_data(&mut self, data: SessionControlData) -> u32;
    pub fn release_control_data(&mut self, index: u32) -> Result<(), SessionError>;

    // VPP: session_send_evt_to_thread, session.c:34-85. The target queue is
    // supplied by SessionMain; this worker only consumes its own queue.
    pub fn consume_event(&mut self, event: SessionEvent) -> Result<(), SessionError>;

    // VPP: session_worker migration lock/storage, session.h:157-159.
    pub fn queue_migration(
        &mut self,
        request: SessionMigrationRequest,
    ) -> Result<(), SessionError>;
    pub fn handle_migrations(&mut self) -> Result<(), SessionError>;

    // VPP: worker time update and adaptive queue scheduling, session.c:2044-2062.
    pub fn update_time(&mut self, now: f64, now_us: u64);
}
```

`SessionMain` 不定义 `TransportVft` 字段，也不调用 `Transport<T>` 的方法；protocol plugin 持有
自己的 Main 并主动调用 service primitive。`Transport<T>` 只是静态能力约束，避免 service 依赖
具体 TCP/UDP。

service 这一层同时拥有所有不含网络地址和协议状态的 transport 共性：`Pacer`、
`TransportConnectionFlags`、`TransportConnection<E, O>`、`TransportTxMode`、
`TransportServiceType`、TX context、
connection/session handle 关联、deschedule/pace flag accessor，以及 transport operation trait。
`IpPacer`、`IpTransportFlags` 或 plugin 内部的另一份 connection base 都不允许出现。

本次按 VPP 两个头文件逐项归属的通用清单如下：

- `transport_types.h:16-30` 的 TX mode/service type 和 `:40-72` 的 connection flags/pacer：service；
- `transport_types.h:81-163` 的 connection identity、worker/session index、flags、pacer：service
  泛型 `TransportConnection<E, O>`，其中 `E` 才由 plugin-session 提供；
- `session.h:22-73` 的 TX context、event element、control data、worker state/flags、DMA transfer：
  service；
- `session.h:82-176` 的 event lists、pending buffer facts、migration storage、time/adaptive state：
  service worker；
- `session.h:211-308` 的 worker collection、next metadata、realloc state/lock、MQ segment、
  lifecycle/config：service main。

因此 plugin-session 只新增 IP endpoint/key/table/local-endpoint allocator 和 IP concrete alias；
它不能再次声明 `Pacer`、flags、connection base、worker event/DMA record 或 Session TX context。

原 `hammer-service::transport::TransportMain<E, K, A>` generic storage 已降为这里的
`TransportMain` 抽象 contract；local endpoint pool/table、freelist、port
allocator 和 ALPN storage 全部移动到 plugin-session 的 `IpTransportMain` concrete instance。这样
service 只声明 VPP `transport_main_t` 的能力边界，不持有 IP 具体类型；`IpTransportConfig` 也只在
plugin-session 定义。

#### 3.5.2 `hammer-plugin-session`

```rust
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU32;

use hammer_infra::bihash::{Bihash, Bihash16x8, Bihash48x8, BihashIter};
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_service::session::{
    SessionConfig, SessionEndpoint, SessionHandle, SessionMain, SessionTable,
};
use hammer_service::transport::{TransportConnection, TransportMain};
use crate::endpoint::{
    IpSessionEndpoint, IpTransportConnectionId, IpTransportEndpoint,
    IpTransportEndpointConfig, IpTransportConfig,
};

type IpLocalEndpoint = SessionEndpoint<IpTransportEndpoint>;

pub struct LocalEndpointCleanupState {
    pub freelist: Vec<u32>,
    pub cleanup_pending: bool,
}

struct IpLocalEndpointState {
    endpoint: IpLocalEndpoint,
    references: AtomicU32,
}

// VPP: transport_types.h:81-163. The base is defined by service and merely
// specialized here with the IP endpoint/connection-id payload.
pub type IpTransportConnection = TransportConnection<IpTransportConnectionId>;

// VPP: transport_main_t endpoint/port allocator fields, transport.c:24-38.
// This configuration is IP transport policy, so it stays in plugin-session;
// the service trait receives it through its associated Config type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpTransportConfig {
    pub local_endpoint_buckets: u32,
    pub local_endpoint_memory: usize,
    pub min_source_port: u16,
    pub max_source_port: u16,
}

// VPP: transport_main_t fields, transport.c:24-38; all local endpoint state is concrete here.
pub struct IpTransportMain {
    local_endpoints_table: Bihash<IpLocalEndpointKey, 7>,
    local_endpoints: UnsafeCell<Pool<IpLocalEndpointState>>,
    port_allocator_seed: AtomicU32,
    port_allocator_min_src_port: u16,
    port_allocator_max_src_port: u16,
    // VPP: transport.c:661-685. The lock protects the actual freelist and
    // pending bit, not an empty unit value.
    local_endpoint_cleanup: SpinLock<LocalEndpointCleanupState>,
    alpn_protocols: AlpnProtocolTable,
}

impl TransportMain for IpTransportMain {
    type Config = IpTransportConfig;
    type Endpoint = IpSessionEndpoint;
    type LocalEndpoint = IpLocalEndpoint;

    // VPP: transport_init, transport.c:1251-1290.
    fn init(config: IpTransportConfig) -> Result<(), SessionError>;

    // VPP process-global tp_main; Hammer global publication is the plugin instance.
    fn global() -> Result<&'static Self, SessionError>;

    // VPP: transport_mark_used_local_endpoint, transport.c:717-743.
    fn mark_used(&self, endpoint: &IpSessionEndpoint) -> Result<(), SessionError>;

    // VPP: transport_share_local_endpoint, transport.c:745-763.
    fn share(&self, endpoint: &IpSessionEndpoint);

    // VPP: transport_release_local_endpoint and deferred cleanup, transport.c:687-715.
    fn release(&self, endpoint: &IpSessionEndpoint) -> Result<(), SessionError>;

    // VPP: transport_alloc_local_endpoint/transport_alloc_local_port, transport.c:771-952.
    fn allocate_local(
        &self,
        endpoint: IpSessionEndpoint,
    ) -> Result<IpSessionEndpoint, SessionError>;

}

// Existing plugin-session endpoint types are reused; no parallel endpoint model is introduced.
// VPP: transport_endpoint_cfg_t, transport_types.h:237-259.

// VPP: session_table_t/session_table_alloc/session_table_index,
// session_table.c:13-29, 62-129. Existing plugin table/config types are reused;
// every externally carried table index remains a plain u32.
type Ip4SessionTable = SessionTable<Bihash16x8, Bihash16x8>;
type Ip6SessionTable = SessionTable<Bihash48x8, Bihash48x8>;

enum IpSessionTableHashes {
    Ip4(Ip4SessionTable),
    Ip6(Ip6SessionTable),
    Local(LocalTable),
}

pub enum IpSessionFamily {
    Ip4,
    Ip6,
}

// Iterator output only. Lookup/add/remove continue to use the existing
// IpTransportConnectionId and IpSessionEndpoint domain values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpSessionLookupKey {
    pub words: [u64; 6],
    pub word_count: u8,
}

impl From<&IpTransportConnectionId> for IpSessionLookupKey {
    // VPP: make_v4_ss_kv/make_v6_ss_kv, session_lookup.c:73-117.
    #[inline(always)]
    fn from(connection: &IpTransportConnectionId) -> Self;
}

// VPP: session_lookup_main_t and fib/table mapping, session_lookup.c:23-31, 157-256.
struct IpSessionLookupState {
    tables: Pool<IpSessionTable>,
    fib_index_to_table_index: [Vec<u32>; 2],
}

pub struct IpSessionLookup {
    state: UnsafeCell<IpSessionLookupState>,
    config: IpSessionTableConfig,
}

impl IpSessionLookup {
    pub fn new(config: IpSessionTableConfig) -> Self;

    #[inline]
    pub fn table_index(&self, family: IpSessionFamily, fib_index: u32) -> u32;

    // VPP: session_table_get_or_alloc, session_lookup.c:164-214. Mutation is
    // Main Thread/WorkerBarrier work; lookup tables retain their existing API.
    pub fn get_or_alloc_table_index(&self, family: IpSessionFamily, fib_index: u32) -> u32;

    // VPP table traversal is exposed as Rust Iterator, not a callback API.
    // VPP: session_table.c:132-156.
    pub fn iter(&self, table_index: u32) -> Option<SessionTableIterator<'_>>;
}

// VPP: ip4_session_table_walk/ip6 table traversal, session_table.c:132-156.
pub enum SessionTableIterator<'table> {
    Short(BihashIter<'table, u128, 4>),
    Long(BihashIter<'table, [u64; 6], 4>),
    Local {
        short: BihashIter<'table, u128, 4>,
        long: BihashIter<'table, [u64; 6], 4>,
        short_complete: bool,
    },
}

impl Iterator for SessionTableIterator<'_> {
    type Item = (IpSessionLookupKey, u64);

    fn next(&mut self) -> Option<Self::Item>;
}

// VPP: session lookup and transport/session backlink paths, session.c:1923-1931.
pub struct IpSessionMain {
    session: &'static SessionMain<u32>,
    lookup: IpSessionLookup,
    transport: &'static IpTransportMain,
}

static IP_SESSION_MAIN: OnceLock<IpSessionMain> = OnceLock::new();

impl IpSessionMain {
    // VPP: session manager initialization followed by transport_init;
    // session.c:2018-2050, transport.c:1251-1290.
    pub fn init(
        session_config: SessionConfig,
        table_config: IpSessionTableConfig,
        transport_config: IpTransportConfig,
        worker_mq_segment: hammer_infra::svm::fifo_segment::SvmFifoSegment,
    ) -> Result<(), SessionError>;

    // VPP: session_main is one process-wide instance, session.h:310 and
    // session.c:2274-2294. Only this plugin publishes the concrete IP Session main.
    pub fn global() -> Result<&'static Self, SessionError>;

    #[inline(always)]
    pub fn session(&self) -> &SessionMain<u32>;

    #[inline(always)]
    pub fn transport(&self) -> &IpTransportMain;

    // VPP: session_lookup_6tuple/session lookup table selection,
    // session_lookup.c:217-241, 271-389; session_table.c:15-129.
    #[inline(always)]
    pub fn lookup(&self, key: &IpTransportConnectionId) -> Option<SessionHandle>;

    // VPP: established/half-open lookup publication and deletion,
    // session_lookup.c:271-389, 837-870.
    pub fn add_connection(
        &self,
        key: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError>;
    pub fn remove(&self, key: &IpTransportConnectionId) -> Result<(), SessionError>;

    // VPP: session_alloc_for_connection and transport backlink assignment,
    // session.c:490-506.
    pub fn allocate_for_connection(
        &self,
        runtime: &mut DataPlaneMain,
        endpoint: &IpSessionEndpoint,
        connection_index: u32,
    ) -> Result<SessionHandle, SessionError>;

    // VPP: endpoint mark/share/release before/after connection publication.
    pub fn prepare_endpoint(
        &self,
        endpoint: IpSessionEndpoint,
    ) -> Result<IpSessionEndpoint, SessionError>;
    pub fn release_endpoint(
        &self,
        endpoint: &IpSessionEndpoint,
    ) -> Result<(), SessionError>;

    // Direct borrows of the existing SVM FIFO; no callback/closure or legacy
    // FIFO wrapper is introduced. Fifo uses interior atomics for enqueue/dequeue.
    pub fn rx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, SessionError>;
    pub fn tx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, SessionError>;

    // VPP: transport close/reset/deleted notification ordering, session.c:344-385.
    pub fn notify_closed(&self, session: SessionHandle, connection_index: u32);
    pub fn notify_reset(&self, session: SessionHandle, connection_index: u32);
    pub fn notify_deleted(&self, session: SessionHandle, connection_index: u32);
}
```

`Transport<T>` 的方法接收 `&self`，因为 TCP/UDP Main 由 `OnceLock` 发布为唯一 process-global
实例；协议 Main 再按 runtime worker index 借用自己的 worker slot。这里的共享接收者不表示共享
修改任意 worker，也不引入锁或动态分派：调用者仍必须是目标 worker，listener/control 修改仍要求
Main Thread 与 WorkerBarrier。

`IpSessionMain` 是唯一把 service `SessionMain`、IP lookup/table 和 `IpTransportMain` 接线的实例。
`TransportMain` trait 只在 service 声明能力；`IpTransportMain` 在 plugin-session 实现 `init/global`
和 local-endpoint 语义。TCP/UDP 不看到 `IpSessionMain.session` 的 pool 字段，只调用这里列出的
concrete lookup、endpoint、FIFO 和 lifecycle 方法。

#### 3.5.3 `hammer-plugin-tcp` / `hammer-plugin-udp`

```rust
use hammer_plugin_session::{
    IpSessionEndpoint, IpSessionMain, IpTransportConnection, IpTransportEndpointConfig,
};
use hammer_infra::pool::Pool;
use hammer_service::session::SessionHandle;
use hammer_service::transport::{
    Transport, TransportOptions, TransportSendParams,
};

// VPP: transport connection base plus TCP-owned state, transport_types.h:81-163.
pub struct TcpConnection {
    pub base: IpTransportConnection,
    pub state: TcpState,
    pub sequence: TcpSequenceState,
    pub timers: TcpTimerState,
    pub recovery: TcpRecoveryState,
}

// VPP: session worker owns scheduling facts; TCP worker owns TCP state.
// Sources: session.h:82-176, session_node.c:1926-2050.
pub struct TcpWorker {
    pub connections: Pool<TcpConnection>,
    pub worker_index: u32,
}

// VPP: transport protocol registration and enable lifecycle, transport.c:1212-1241.
pub struct TcpMain {
    pub workers: Vec<TcpWorker>,
    pub listeners: Pool<TcpConnection>,
    pub half_opens: Pool<TcpConnection>,
    pub config: TcpConfig,
}

// VPP: transport_endpt_attr_t contains protocol-owned attributes such as
// congestion-control selection; it is not a service or IP record.
pub struct TcpTransportAttribute;

impl TcpMain {
    // VPP: transport enable/init sequence, transport.c:1223-1290.
    pub fn init(config: TcpConfig) -> Result<(), SessionError>;

    pub fn global() -> Result<&'static Self, SessionError>;

    // VPP: transport connection retrieval, transport.h:156-173.
    #[inline(always)]
    pub fn connection(&self, worker_index: u32, connection_index: u32) -> Option<&TcpConnection>;

    // VPP: transport_connect wrapper, transport.h:131-138; plugin-session owns endpoint preparation.
    pub fn connect(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;

    // VPP: transport_start_listen/stop_listen, transport.h:139-141.
    pub fn start_listen(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    pub fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;

    // VPP: connection cleanup/half-open cleanup, transport.h:142-144.
    pub fn cleanup(&self, connection_index: u32, worker_index: u32);
    pub fn cleanup_half_open(&self, connection_index: u32);

    // VPP: transport update-time callback is worker-local work, not Session callback dispatch.
    pub fn update_time(&self, worker_index: u32, now: f64);

    // VPP: transport_get_connection/session_get_transport, transport.h:156-173,
    // session.c:1923-1931; the call is concrete plugin-session attachment.
    pub fn attach_session(
        &self,
        sessions: &IpSessionMain,
        worker_index: u32,
        connection_index: u32,
    ) -> Result<(), SessionError>;
}

impl Transport<IpTransportEndpointConfig> for TcpMain {
    type Connection = TcpConnection;
    type Attribute = TcpTransportAttribute;

    fn options(&self) -> TransportOptions;
    fn connect(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn connect_stream(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn start_listen(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;
    fn half_close(&self, connection_index: u32, worker_index: u32);
    fn close(&self, connection_index: u32, worker_index: u32);
    fn reset(&self, connection_index: u32, worker_index: u32);
    fn cleanup(&self, connection_index: u32, worker_index: u32);
    fn cleanup_half_open(&self, connection_index: u32);
    fn push_header(&self, connection_index: u32, worker_index: u32, buffers: &mut [u32], available_bytes: u32) -> u32;
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams;
    fn update_time(&self, now: f64, worker_index: u32);
    fn flush_data(&self, connection_index: u32, worker_index: u32);
    fn custom_tx(&self, session: SessionHandle, params: &mut TransportSendParams) -> usize;
    fn app_rx_event(
        &self,
        connection_index: u32,
        worker_index: u32,
    ) -> Result<(), SessionError>;
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection>;
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn endpoint(&self, connection_index: u32, worker_index: u32) -> (IpTransportEndpointConfig, IpTransportEndpointConfig);
    fn listener_endpoint(&self, connection_index: u32) -> (IpTransportEndpointConfig, IpTransportEndpointConfig);
    fn attribute(
        &self,
        connection_index: u32,
        worker_index: u32,
        attribute: &mut Self::Attribute,
    ) -> Result<(), SessionError>;
}

// VPP: connectionless transport still uses transport_connection_t facts and Session handle.
pub struct UdpConnection {
    pub base: IpTransportConnection,
    pub state: UdpState,
}

// VPP: per-worker session event processing remains in session worker; UDP owns only UDP state.
pub struct UdpWorker {
    pub connections: Pool<UdpConnection>,
    pub worker_index: u32,
}

pub struct UdpMain {
    pub workers: Vec<UdpWorker>,
    pub listeners: Pool<UdpConnection>,
    pub config: UdpConfig,
}

pub struct UdpTransportAttribute;

impl UdpMain {
    // VPP: transport enable/init sequence, transport.c:1223-1290.
    pub fn init(config: UdpConfig) -> Result<(), SessionError>;

    pub fn global() -> Result<&'static Self, SessionError>;

    // VPP: transport_get_connection, transport.h:156-173.
    #[inline(always)]
    pub fn connection(&self, worker_index: u32, connection_index: u32) -> Option<&UdpConnection>;

    // VPP: transport_connect/transport_start_listen, transport.h:131-141.
    pub fn connect(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    pub fn start_listen(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    pub fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;

    // VPP: transport_cleanup/transport_cleanup_half_open, transport.h:142-144.
    pub fn cleanup(&self, connection_index: u32, worker_index: u32);
    pub fn cleanup_half_open(&self, connection_index: u32);

    // VPP: session_get_transport backlink path, session.c:1923-1931.
    pub fn attach_session(
        &self,
        sessions: &IpSessionMain,
        worker_index: u32,
        connection_index: u32,
    ) -> Result<(), SessionError>;
}

impl Transport<IpTransportEndpointConfig> for UdpMain {
    type Connection = UdpConnection;
    type Attribute = UdpTransportAttribute;

    fn options(&self) -> TransportOptions;
    fn connect(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn connect_stream(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn start_listen(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;
    fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;
    fn half_close(&self, connection_index: u32, worker_index: u32);
    fn close(&self, connection_index: u32, worker_index: u32);
    fn reset(&self, connection_index: u32, worker_index: u32);
    fn cleanup(&self, connection_index: u32, worker_index: u32);
    fn cleanup_half_open(&self, connection_index: u32);
    fn push_header(&self, connection_index: u32, worker_index: u32, buffers: &mut [u32], available_bytes: u32) -> u32;
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams;
    fn update_time(&self, now: f64, worker_index: u32);
    fn flush_data(&self, connection_index: u32, worker_index: u32);
    fn custom_tx(&self, session: SessionHandle, params: &mut TransportSendParams) -> usize;
    fn app_rx_event(
        &self,
        connection_index: u32,
        worker_index: u32,
    ) -> Result<(), SessionError>;
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection>;
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn endpoint(&self, connection_index: u32, worker_index: u32) -> (IpTransportEndpointConfig, IpTransportEndpointConfig);
    fn listener_endpoint(&self, connection_index: u32) -> (IpTransportEndpointConfig, IpTransportEndpointConfig);
    fn attribute(
        &self,
        connection_index: u32,
        worker_index: u32,
        attribute: &mut Self::Attribute,
    ) -> Result<(), SessionError>;
}
```

TCP/UDP 的 `connect/listen` 调用顺序由 protocol plugin 或 control caller 编排：先调用
`IpSessionMain::prepare_endpoint`，再调用具体 Main 的 `Transport<T>` 方法，成功后调用
`IpSessionMain::allocate_for_connection`/`add_connection`；失败按 VPP 的 half-open/local-endpoint
cleanup 顺序回滚。service 不反向调用 `TcpMain`/`UdpMain`。

## 4. Service 层：Session Core

### 4.1 `SessionMain`

service 的 `SessionMain` 是 protocol-neutral process authority。它可以由 plugin-session 持有和
发布，但字段类型只能来自 service/runtime/infra。它拥有：

- 固定数量、在 worker launch 前构造完成的 `SessionWorker` slots；
- Session handle allocation、listener/half-open relation、connection attach/detach 和状态转换 authority；
- Session protocol metadata：protocol id、`TransportTxMode`、output target 和 worker readiness facts；
- 每 worker `SvmMsgQ` publication、pool growth barrier state/lock、migration publication 和
  shutdown/enabled state；
- `SvmFifo` ownership policy、preallocation/configuration、DMA mode 和 runtime engine selection；
- service-owned common transport base (`TransportConnection` + `Pacer`) and its flag facts.

它不拥有：

- IP address、IP family、FIB index、interface index、namespace binding 或 IP key；
- TCP/UDP connection、listener、half-open、timer、retransmission 或 packet state；
- IP session lookup/table、local endpoint allocator、port range、FIB/interface/family fields；
- `TransportVft`、function pointer registry、`dyn Transport` 或 erased protocol state；
- plugin-specific type、plugin-owned reference 或 protocol-specific error。

`SessionMain` 的 global accessor 只返回唯一 process-global service core。若 plugin-session 将 core
嵌入 `IpSessionMain`，runtime/graph data 必须指向这一份 embedded core，禁止第二份 Main。

### 4.2 `SessionWorker`

每个 `SessionWorker` 只由一个 Data Worker 执行。它完整对应 `session_worker_t` 的 service-owned
部分：

- 本 worker 的 `Pool<Session<O>>`、SVM MQ index、worker index、时间快照和 transport-time
  subscription facts；
- `pending_io_sessions`、timer/adaptive fd facts、worker flags/state、`SessionTxContext`、pending
  TX buffer/next facts、RX segment scratch、DMA ring/batch facts；
- event element/control-data pools 以及 control/new/old/pending-connect/main-pending 的 five list
  heads（这些 list 存现有 infra 的 pool index）；
- migration request/handling storage 和 migration lock；
- Session entry 的 SVM FIFO ownership、原子 lifecycle state、flags、listener/half-open relation
  和 transport numeric backlink。

这里的锁不是带空 unit payload 的 `SpinLock`。这种锁只能表示“有一把锁”，却没有携带被保护的
domain state，既不能表达 VPP 的 migration request/handling 队列，也不能让临界区通过借用检查。
因此 migration 使用 `SpinLock<SessionMigrationState>`，`SessionMain` 的 pool reallocation 使用
`SpinLock<PoolReallocationState>`；锁 guard 直接借用真实队列或重分配状态，禁止再用空 unit
作为占位 payload。

它不拥有 TCP/UDP protocol connection object、IP lookup/table borrow、packet buffer、protocol timer
wheel entry、retransmit/congestion state，或另一个 worker 的 `&mut` borrow。worker 内的 timer 只
用于 Session queue/adaptive scheduling；TCP/UDP timer 仍由各自 protocol worker 持有。

worker-facing API 必须是直接借用和具名 domain operation。不得以 callback closure、`dyn` capability
或 cached pointer 暴露 worker state。跨 worker 的 IO/control/RPC 请求进入目标 `event_queue`，由
目标 worker 修改自己的 Session state；migration 使用 owner worker 的 migration request storage。

### 4.3 Session entry

Session entry 是 service-owned record，只保存下列事实：

- Session handle 的 worker index 与 pool index（数值句柄）；
- `AtomicU8` session state（对应 VPP 的 `volatile session_state`，由 `Acquire/Release` 读写）；
- 现有 infra 的 `Option<SvmFifo>` RX/TX FIFO ownership；half-open/listener 没有 FIFO 时使用
  `None`，不引入第二种 FIFO；
- transport protocol id、connection index、worker index 和 half-open/listener relation；
- connectionless、half-open、migration、readiness 等 flags；
- owner-supplied generic `opaque: O`，service 只借用它，不解释其业务语义。

VPP `app_wrk_index`、app listener index 和 app-specific notification bitmap 在本阶段不进入
service API；它们属于后续 app layer 的 owner。worker 仍保留同样位置的通用 pending-notification
事实，以免把 app policy倒灌进 Session core。

Session entry 不保存 `IpTransportConnectionId`、`TcpConnection`、`UdpConnection` 或 endpoint
config；plugin-session 与 protocol plugin 依据数值 handle 查询自己的实例。

### 4.4 Service primitive

service 只提供四组具名 primitive：

1. **Session allocation**：创建 listener、half-open 或 established Session，分配 handle、建立
   FIFO ownership，并在失败时回滚；
2. **Transport attachment**：绑定 protocol id + connection index，或解除该绑定；
3. **Transport notification**：接收 accepted、connected、closing、closed、reset、deleted 和
   half-open cleanup 事实，只推进 Session state，不猜测 protocol state；
4. **Worker access**：worker 通过自己的 index 取得自己的 `SessionWorker`，执行 FIFO、ready、
   close、migration 和 cleanup 操作。

这些 primitive 不返回或接收 VFT、trait object、callback registry，也不解析 IP endpoint 字段。
`Session::store_state/load_state` 是 `session_set_state` 和直接状态读取的 Rust 对应物；opaque
由 `Session<O>` 泛型承载，不再硬编码一个 service 解释的 `u32`。

### 4.5 `volatile session_state` 的迁移语义

VPP 在 `session_t` 上使用 `volatile session_state`，原因不是要把状态做成普通的“可变字段”
就够了：Session node、transport worker、control/app 路径可能在不同执行上下文读取同一个
Session，并且 `session_set_state` 后紧接着会有 state-change observation（`session.h:909-914`）。
C 的 `volatile` 只保证编译器不会删掉或缓存访问，它本身不提供 CPU 同步；VPP 的实际安全性还
依赖 owner-worker、event MQ 和 barrier 的调度约束。

Hammer 保留同一 ownership 约束，并把可观察状态显式建模为 `AtomicU8`：发布状态用
`Ordering::Release`，读取状态用 `Ordering::Acquire`；FIFO、connection backlink 和 flags 仍由
owner worker 的直接借用保护，不把所有 Session 字段都做成原子。无效的原始状态是 owner bug，
由 `From<u8> for SessionState` 的断言终止，不伪装成可恢复 Session error。这样既表达了 VPP
的跨上下文可见性，也没有把 Rust `volatile` 当作同步原语。

## 5. Plugin-session 层：IP 组合 owner

`hammer-plugin-session` 定义唯一 concrete `IpSessionMain`。它组合 service Session core、IP
lookup/table 和 IP transport，但不持有 TCP/UDP state。它拥有：

- 一个 service `SessionMain` core instance；
- IP session/half-open lookup tables、table binding 和 endpoint allocation policy；
- `IpTransportMain` 的 concrete local-endpoint/port allocator instance；
- `IpSessionEndpoint`、IP local endpoint key、`IpTransportConnectionId` 等 IP facts；
- `IpTransportConnection = TransportConnection<IpTransportConnectionId>` 这一具体化别名，但不
  重新定义 base、flags 或 pacer；
- 将 IP lookup result 转成 service Session handle 的 concrete operations；
- worker-facing IP lookup、connection attach/detach 和 endpoint mark/share/release operations。

`IpSessionMain::init/global` 是 plugin-session 的唯一 publication entry。`TransportMain::init`
和 `TransportMain::global` 只能暴露该 concrete IP instance；不得出现 service 一份 transport
global、plugin-session 另一份 transport global 的双份状态。

IP family/table 选择只发生在 `IpSessionMain`。service Session entry 不保存 family-specific FIB
字段，也不解析 namespace id；Application Namespace、socket/fd 和 attach 语义留到后续 ADR。

TCP/UDP 不直接修改 IP table pool 或 service Session pool，必须调用 `IpSessionMain` 的 concrete
endpoint/lookup/attach/cleanup operations，再由 plugin-session 调用 service primitive。这样
lookup publication 与 Session attachment 的顺序只有一个 owner。

## 6. TCP/UDP 层：具体 Transport 与 worker

`TcpMain`/`UdpMain` 分别拥有 protocol connection/listener/half-open pool、timer、packet graph、
lookup index、congestion/retransmit state 和 protocol configuration。连接中的通用
`TransportConnection`/`Pacer`/flags 来自 service；TCP/UDP 只持有它的 concrete endpoint 参数并负责
更新 pacing。`TcpWorker`/`UdpWorker`
是各自 protocol worker state 的唯一 owner。

它们实现 service 的 `Transport<T>` control contract，具体实现落在现有 Main，不创建
`TcpTransport`、`UdpTransport` marker 或 forwarding Main。它们不拥有 IP Session table、service
Session pool、另一个 SessionMain/Worker、VFT 或 erased registry。

worker 只在自己的 runtime worker 上借用同一 worker 的 service `SessionWorker`，并通过
`IpSessionMain` 做 IP lookup/attach。跨 worker 只传递可复制的 handle、tuple、generation facts
和 bounded event/RPC record；不借用另一个 worker 的 pool、timer 或 packet state。

连接建立/删除的顺序必须是：

1. protocol 创建或找到 concrete connection；
2. plugin-session 完成 IP table lookup/publication；
3. service 分配或取得 Session entry，并附着 FIFO；
4. plugin-session 将 Session handle/connection index 双向关联；
5. protocol 继续自己的 packet/timer state machine；
6. 关闭时先删除 lookup publication、推进 Session state，再执行 protocol connection cleanup。

transport 不得在 service 仍可见 Session 时直接释放 connection pool entry。

## 7. Session 到 Transport 的 TX 与 event 接线

`SessionQueueNode` 是 service-owned scheduler 和 FIFO packetization owner，但不是 protocol callback
dispatcher。`SessionMain` 不保存每协议 TX callback。目标路径是：

- TCP/UDP worker 在 concrete 类型上计算 `TransportSendParams`，通过 `IpSessionMain` 更新 owner
  worker 的 Session entry；
- SessionWorker 记录 FIFO readiness、protocol id、send facts 和待处理 event；
- Session Queue 按 new/old 顺序消费 event，从 Session-owned SVM FIFO 生成 Data Plane Buffer；
- Session Queue 按 `session_type_to_next` 把 `BufferIndex` frame 发送给已有 `TcpOutputNode` 或
  `UdpOutputNode`；
- output node 只消费 packet buffer，在 concrete `TcpMain`/`UdpMain` 上静态调用 transport header
  operation，再进入 IP lookup；
- service 不反向调用 protocol callback，output node 不读取 Session event list，也不直接借用
  Session FIFO。

VPP 在 `session_node.c:1471-1676` 内完成 FIFO packetization，并把 pending `u32` buffer index 发送到
`tcp_output.c:2400-2446` / `udp_output.c:202-246` 的 packet-vector node。Hammer 保持相同 frame
语义；为避免 VFT 与 service 到 plugin 的反向依赖，只把 concrete transport header 调用移动到已有
output node。完整 queue/node 设计见 ADR-0039。

ordinary IO/control/RPC 使用目标 worker 的 `event_queue`；Session migration 使用 owner worker 的
migration request/handling storage。生产者只入队并唤醒目标 worker，消费者才修改目标 worker 的
Session state。队列满、锁失败由 `SessionEventEnqueue::Full/Busy` 明确返回，目标 worker 无效返回
`SessionError::Invalid`，不静默丢弃。

## 8. 控制路径顺序

### 8.1 Listen

1. plugin-session 完成 endpoint/table policy；
2. service 分配 listener Session record；
3. concrete TCP/UDP transport 调用 `IpTransportMain` mark-used 并创建 listener；
4. plugin-session 绑定 listener connection index 与 Session handle；
5. 任一步骤失败都撤销 Session record、lookup publication 和 local endpoint reference。

### 8.2 Connect

1. plugin-session 完成 endpoint、table 和 local-port policy；
2. concrete transport 创建 connection/half-open；
3. service 创建 half-open/connecting Session；
4. plugin-session 发布 half-open lookup；
5. handshake/peer discovery 成功后，service promote 为 established；
6. 失败或取消按 half-open cleanup 清理，不伪造 established Session。

### 8.3 Accept

1. protocol packet path 通过 `IpSessionMain` 执行 established、half-open、exact/wildcard lookup；
2. 命中 listener 后创建 concrete connection；
3. plugin-session 请求 service 创建 accepting Session；
4. connection backlink 写入后进入 ready；早期关闭按同一 cleanup 顺序处理。

### 8.4 Close/reset

Session state transition 由 service 唯一负责。TCP/UDP 只报告 peer reset、timeout、transport
closed、connection deleted 等事实；service 按当前 state 删除 lookup、释放 FIFO、调用 concrete
transport cleanup。两个层不得复制 close state machine。

## 9. 初始化、publication 与 lifetime

初始化顺序固定为：

1. plugin-session 调用 `SessionMain::init`，构造并安装唯一 service Session core、worker slots、
   per-worker event queues、FIFO policy 和 metadata；
2. plugin-session 调用 `IpTransportMain::init`，再构造 IP lookup/table 并由 `IpSessionMain::init`
   安装唯一 IP composition；
3. 后续代码只通过各自 `global` accessor 取得已安装实例；不存在 `publish`/`bind` API；
4. TCP/UDP 在其后构造 protocol Main/Worker、connection pools 和 graph nodes；
5. Data Worker launch 前构造所有 service/protocol worker slots；
6. Data Worker 运行后只读取自己的 worker entry 和已发布 immutable metadata。

重复 init 是 lifecycle programming bug，使用现有 init assertion；未初始化 global 返回 service
`SessionError::Unknown`。普通 worker borrow 不使用 `'static`；只有真正的 process-global `OnceLock`
accessor 返回 `&'static Self`。

## 10. Inline 语义

inline 不是装饰性约定，而是按 VPP 调用频率和失败前提选择：

| VPP 形式/依据 | Hammer 规则 | 允许的操作 |
|---|---|---|
| `always_inline`，`session.h:335-365` | `#[inline(always)]` | Main/worker 取得、enabled flag 和固定索引 accessor |
| `always_inline`，`session.h:468-521` | `#[inline(always)]` | Session handle/pool lookup；带 assertion 的 fast path 与已验证 fast path |
| `always_inline`，`session.h:777-890` | `#[inline(always)]` | FIFO enqueue、ready flag 和 worker event-list enqueue 的短热路径 |
| `static inline`，`session.h:347-353` | `#[inline]` | 带边界检查的 worker lookup/helper，不强制展开 |
| `static inline`，`transport.h:156-225` | `#[inline]` | connection/listener retrieval wrapper 与 flag accessor |
| `always_inline`，`transport.h:329-346` | `#[inline(always)]` | deschedule/tx-paced flag fast path |

初始化、registration、pool allocation、barrier、lock、queue full handling、cleanup、错误构造和
复杂 state transition 不加 `#[inline]`。VPP 的 C `static inline` 不等于 Rust `#[inline(always)]`；
只有稳定的短 helper 才使用 `#[inline]`。public 方法不因“对齐 VPP”而全部 inline。

## 11. Error 语义

### 11.1 唯一 control-path error

`hammer-service::session::SessionError` 是 Session/Transport control path 的唯一共享 error enum。
它逐项对应 `session_types.h:514-576` 的 `foreach_session_error`，并使用已有
`#[runtime_error(subsystem = "session")]` 宏；不新增 error macro，不定义数字 discriminant，也不把
error 存成 `i32`。plugin-session、TCP 和 UDP 在 Session/Transport contract 上直接返回该类型，不再
声明同义的 endpoint/session retval error。

`SessionError::Unknown` 只对应 VPP 明确定义的 `SESSION_E_UNKNOWN`，不是字符串 catch-all。
`SESSION_E_NONE` 映射为 `Ok`。packet/node malformed input、checksum、invalid header 仍走所属 Node 的
counter/drop/punt，不转成 control-plane `Result`。

### 11.2 函数结果、sentinel 与正常 miss

- `session_send_evt_to_thread` 的 lock busy 与 queue/ring full 映射为
  `SessionEventEnqueue::Busy/Full`；它们是“事件尚未提交”的调用结果，不是 `SessionError`；
- `custom_tx` 返回 packet count，`SessionTxResult` 表达 sent/no-data/no-buffer；这些都不是 error code；
- message allocation failure 使用 `SessionError::MessageQueueAllocation`，eventfd allocation failure 使用
  `SessionError::EventFdAllocation`；
- `u32::MAX`、`ENDPOINT_INVALID_INDEX` 等 sentinel 只有在对应 VPP contract 明确使用时保留，不能
  被 `Option` 静默替换；
- `Option` 只表示正常 lookup miss，例如 `get_if_valid` 或未命中的 table 查询，不表示需要调用方
  处理的错误；
- VPP `clib_error_return` 的 Session/Transport 初始化或 enable 失败映射到对应 `SessionError` variant，
  不能变成 packet error；
- VPP `ASSERT`/`ALWAYS_ASSERT` 表示 owner 已破坏的不变量，映射为本地 assertion/panic，不翻译
  为 recoverable error。

### 11.3 具体 VPP transport error

`transport.c` 的现有类别必须保持语义，不另造同义错误：

- 未注册 transport：`SessionError::TransportNotRegistered`（`transport.c:495,504,535`）；
- local endpoint 冲突：`SessionError::PortInUse`（`transport.c:729,943`）；
- interface 无地址：`SessionError::NoIp`（`transport.c:839-856`）；
- 无路由/无解析 interface：`SessionError::NoRoute`、`SessionError::NoInterface`
  （`transport.c:869-889`）；
- 无可用 source port：`SessionError::NoPort`（`transport.c:925-930`）。

local endpoint mark/share/release 的 refcount 和 table 更新必须 failure-atomic；VPP 的
`transport.c:695-710` 先删除 table entry、再排入 cleanup，Hammer 不能在返回错误后留下半绑定
endpoint。`ASSERT` 例子是 pacer rate 非零（`transport.c:996-1009`），它不是调用方可恢复的
transport error。

### 11.4 Stats boundary

Stats、node counter、allocator counter 和任何统计索引都不属于本阶段。Session、SessionWorker、
SessionMain、TransportMain 以及 plugin-session concrete state 不增加 stats 字段、counter API 或
统计生命周期；后续 ADR 需要单独对齐 VPP 的 node/counter owner 后再设计。

## 12. 明确删除和禁止的旧接线

ADR-0038 新路径已删除或停止新增：

- `SessionMain` 读取 `TransportVft`、`transport_vft`、`register_transport` 或 operation table；
- `SessionWorker` 保存每协议 `SessionQueueTransportDispatch` callback；
- `SessionQueueNode` 保存或调用 TCP/UDP TX/control callback；
- service 读取 IP key、FIB、namespace 或 socket endpoint；
- TCP/UDP 直接修改 plugin-session table pool 或 service Session pool；
- `dyn Transport`、erased registry、plugin capability wrapper 或 cached pointer；
- 为连接三层再创建 `TcpTransport`、`UdpTransport`、family-specific generic wrapper 或第二份 Main。

`SessionQueueNode` 本身保留并迁入 ADR-0039 的 service-owned scheduler/FIFO packetization 设计；
删除的是 callback attachment 和 protocol dispatch table，不是这个 VPP 对应 node。

## 13. 迁移顺序

1. 在 service 中收敛 protocol-neutral Session core：worker slots、Session pool、listener/
   half-open relation、lifecycle 和 event queue，移除 VFT 读取。
2. 在 service 中定义 transport-independent Session/worker operations 与 `Transport<T>` contract。
3. 在 plugin/session 中组成唯一 `IpSessionMain`，完成 IP lookup、endpoint 和 Session attachment。
4. TCP/UDP 以自身 Main/Worker 实现 `Transport<T>`，删除对 service VFT 的新增依赖。
5. Session Queue 保留 TX FIFO packetization；TCP/UDP 的 send-param 计算、transport header、protocol
   timer、congestion/retransmit 和 migration work 迁到各自 protocol worker/output node。通用 `Pacer`
   数据保留在 service 的 `TransportConnection` base，更新由 concrete protocol 调用；SessionWorker
   保留完整 Session queue/event/DMA/FIFO facts 和 worker handoff。
6. 按本 ADR 的 listen/connect/accept/close 顺序迁移调用方，验证 failure-atomicity。
7. 所有 consumer 切换后删除旧 VFT compatibility layer 和仅为它存在的 dispatch fields。

## 14. 结果与取舍

该结构保留 VPP 的核心 ownership：Session Main 是 process authority，Session Worker 是 thread
owner，transport connection 是 protocol owner，Session 通过数值 handle 与 transport 关联，
worker 间通过 event queue/RPC/migration handoff 通信。它不照搬 VPP 的 C VFT；Session Queue 只保留
VPP 已有的调度、FIFO packetization 和 graph fanout authority，不增加 protocol callback authority。

代价是 control caller 必须在 concrete transport 类型上完成一次静态编排；这是避免 VFT、`dyn`
和 erased registry 的边界成本。以后增加其他 transport，只增加自己的 Main/Worker，并实现
plugin-session/service 的既有 concrete contract，不扩展 service 结构体字段。

## 15. 本次实施边界

- Application、AppWorker、Application Namespace attach/detach、socket/fd lifecycle；
- Binary API message schema、server/client transport 和 request/reply；
- TCP congestion-control algorithm ownership；
- Session FIFO memory layout、SVM segment format 或 protocol chain；
- stats/counter、端口分配最大尝试次数和 local-endpoint count API；
- 对旧 Application runtime、`TransportVft` 与 Session Queue callback attachment 兼容路径的删除。
  queue node 的静态声明、enable 和 packet output 迁移由 ADR-0039 单独实施。

## 16. VPP 源码依据

- `session_worker_t` 字段、worker event/migration storage：
  `third_party/vpp/src/vnet/session/session.h:82-176`
- `session_t` 的 SVM FIFO 指针、`volatile session_state`、flags、connection/listener relation 和
  `opaque`：
  `third_party/vpp/src/vnet/session/session_types.h:189-290`
- `session_set_state`、FIFO fast path 与 listener/half-open allocation：
  `third_party/vpp/src/vnet/session/session.h:777-935`、`1022-1088`
- `session_main_t` 字段与配置：
  `third_party/vpp/src/vnet/session/session.h:211-308`
- Main/worker/Session fast accessors：
  `third_party/vpp/src/vnet/session/session.h:335-365`、`468-521`
- FIFO enqueue 与 event-list hot path：
  `third_party/vpp/src/vnet/session/session.h:777-890`
- 跨 worker event MQ 写入、full/lock retval 和 interrupt wakeup：
  `third_party/vpp/src/vnet/session/session.c:34-85`
- transport protocol metadata registration：
  `third_party/vpp/src/vnet/session/session.c:1883-1921`
- Session worker allocation/publication：
  `third_party/vpp/src/vnet/session/session.c:2018-2050`
- Session Main defaults/init hook：
  `third_party/vpp/src/vnet/session/session.c:2274-2294`
- Session lookup key construction, table allocation, connection/listener/half-open add/remove：
  `third_party/vpp/src/vnet/session/session_lookup.c:23-31`、`73-149`、`157-256`、
  `259-414`、`508-681`、`837-870`
- Session table pool, hash initialization and iterator source traversal：
  `third_party/vpp/src/vnet/session/session_table.c:13-29`、`40-129`、`132-156`
- Transport operation contract：
  `third_party/vpp/src/vnet/session/transport.h:131-154`
- Transport retrieval/flag inline helpers：
  `third_party/vpp/src/vnet/session/transport.h:156-225`、`329-346`
- Transport connection base and endpoint config fields：
  `third_party/vpp/src/vnet/session/transport_types.h:81-163`、`243-259`
- Transport-neutral TX/service modes, flags and pacer fields：
  `third_party/vpp/src/vnet/session/transport_types.h:16-72`
- Transport registration and wrapper retval categories：
  `third_party/vpp/src/vnet/session/transport.c:24-38`、`250-540`
- Local endpoint refcount/table cleanup：
  `third_party/vpp/src/vnet/session/transport.c:661-743`
- Local endpoint allocator and port-range handling：
  `third_party/vpp/src/vnet/session/transport.c:771-837`
- Local IP/route/interface/port errors：
  `third_party/vpp/src/vnet/session/transport.c:839-950`
- Pacer invariant assertion：
  `third_party/vpp/src/vnet/session/transport.c:996-1009`
- Transport initialization and enable lifecycle：
  `third_party/vpp/src/vnet/session/transport.c:1223-1290`
- Existing Hammer boundary decisions：ADR-0035、ADR-0036、ADR-0037。
- Hammer SVM message queue and FIFO-segment ownership：
  `crates/hammer-infra/src/svm/msg_queue.rs:1-225`、`238-460`、`785-1004`；
  `crates/hammer-infra/src/svm/fifo_segment.rs:815-910`。
