# ADR-0040: Session Application 与 App Listener 边界

- 日期：2026-09-22
- 状态：Proposed
- 前置决定：ADR-0035、ADR-0036、ADR-0037、ADR-0038、ADR-0039
- VPP 基线：`third_party/vpp`，以当前 vendored source 为准
- 范围：把 application、app namespace 选择、app worker、app listener、cut-through 和
  app/session event lifecycle 接入现有 `hammer-service::session`，并定义
  `hammer-plugin-session` 的 IP concrete 接线
- 不包含：namespace socket、Binary API message declaration、外部 client、stats、TCP/UDP
  connection state、Transport trait 重设计

ADR-0039 的 queue-node 决定是本 ADR 的前置约束：`session-queue`、`session-queue-process` 和
`session-queue-main` 仍属于 `hammer-service`，默认 `Disabled`，事件仍通过现有 SVM
FIFO/MQ 和 Session worker event list 流动。本 ADR 只补齐 application 与 listener 的 owner 及其
与这些节点的接线；不增加 mailbox、multi-ring queue、dispatch queue 或新的 scheduler 类型。

## 1. 背景

VPP 把 application sublayer 放在 session 子系统中，而不是 TCP、UDP 或 IP 插件中：

- `app_main_t` 拥有 application pool、app-listener pool、按 API client/name 的查找表、每个
  worker 的 pending RX MQ 状态和 connect RPC 状态（`application.h:205-238`）。
- `application_t` 只拥有 application flags、callback table、segment-manager properties、
  worker mapping、name、namespace identity 和 RX MQ segment（`application.h:119-161`）。本 ADR
  将 namespace pool index 接到 Application；namespace 的 generic owner 仍是 ADR-0036 的
  `hammer-app`，IP/FIB binding 仍是 `hammer-plugin-session` 的 concrete state。
- `app_worker_t` 拥有 application worker identity、connect segment-manager index、listener table、
  half-open handles、per-worker application events 和 MQ congestion state；event queue 是其所
  属 segment 的非拥有引用
  （`application.h:32-81`）。
- `app_listener_t` 记录 accepting workers、accept rotor、global/local listening Session 和
  connectionless worker listener mapping（`application.h:88-101`）。
- `app_rx_mq_elt_t` 保存每个 application/worker 的 SVM MQ、eventfd/file 和 pending/postponed
  flags；`appsl_wrk_t` 只保存当前 worker 的 pending MQ 集合（`application.h:103-117`、
  `:196-203`）。
- cut-through 的 `ct_main_t`、`ct_worker_t` 和 `ct_connection_t` 属于 application-local session
  sublayer；它们通过 peer index、half-open reusable list、worker connect/cleanup lists 与
  Session/FIFO 接线（`application_local.c:14-118`、`application_local.h:31-45`）。
- transport 只创建并维护 listening/connected transport connection；Session 负责把它与
  application worker、FIFO、事件和 listener handle 接起来（`session.c:1215-1320`、
  `session.c:1322-1455`）。

当前 Hammer 的 application 代码已经在 `hammer-service::session`，SVM MQ/FIFO 也已经存在于
`ApplicationMain`/`AppWorker`，因此本 ADR 不把这些类型搬进 `hammer-plugin-session`。需要移动的
只有 IP endpoint、IP lookup/table 和 IP transport binding；它们是 plugin/session 的 concrete
composition，不是 application 抽象。

## 2. 决定

采用如下 ownership：

```text
hammer-service::session
  SessionMain / SessionWorker / Session<O>
  ApplicationMain / Application / AppWorker / ApplicationListener
  ApplicationCallbacks trait and app/session events
  generic CtMain<E, O> / CtWorker<E, O> / CtConnection<E, O> contracts
  SVM FIFO, SVM message queue, listener segment ownership
  session-queue nodes and worker scheduling

hammer-plugin-session
  IpSessionMain
  IpSessionEndpoint / IpTransportConnectionId
  IP lookup/table and endpoint-to-connection binding
  IP-specific listener lookup and transport notifications
  IpCtMain / IpCtWorker / IpCtConnection concrete CT instance

hammer-plugin-tcp / hammer-plugin-udp
  TcpMain / UdpMain, concrete connection/listener pools
  concrete Transport<T> implementation
  protocol timers, headers, packet nodes and protocol state
```

`hammer-app` remains the client SDK and generic namespace owner from ADR-0036. It may hold the
client-side App Session/FIFO handles returned by attach and supplies a raw namespace pool index
(`u32`) to the service attach request, but it does not own `ApplicationMain`, `AppWorker`,
`ApplicationListener` or cut-through state; those are server-side session state in
`hammer-service`.

依赖方向固定为：

```text
hammer-plugin-tcp/udp -> hammer-plugin-session -> hammer-service
hammer-plugin-tcp/udp -> hammer-service::transport::Transport<T>
hammer-service -X-> hammer-plugin-session/tcp/udp
```

因此 service 不调用 IP/TCP/UDP concrete implementation。listen/connect 的 control operation
由调用方按 VPP 顺序组合：service 分配 application/listener/session ownership，
`hammer-plugin-session` 校验并登记 IP identity，具体 TCP/UDP Main 调用自己的 `Transport<T>`。
任何失败都按反向顺序清理，不能留下半个 listener 或 lookup entry。

## 3. Service owner

### 3.1 ApplicationMain、Application 和生命周期

`ApplicationMain` 是 process-global service authority，与 VPP `static app_main` 和
`application_init` 对齐（`application.c:2090-2106`）。它只暴露 `init` 和 `global` 两个 Main
lifecycle 方法；不存在 `publish`、`publish_global`、provider、registry 或 callback-based
borrow API。`global()` 只提供只读 Main 借用；attach/detach、listener mutation 和 RX-MQ list
mutation 由持有 Main 的生命周期线程以 `&mut ApplicationMain` 执行，并在需要时经过现有
WorkerBarrier，避免用内部锁伪造可变借用。

```rust
use std::sync::OnceLock;
use std::collections::HashMap;
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;

// VPP: application.h:205-238; application.c:2090-2106.
// VPP appsl_wrk_t: application.h:196-203. The per-worker collection is a
// direct Vec; no ApplicationWorkerState wrapper is introduced.
pub struct ApplicationMain<'app> {
    applications: Pool<Application<'app>>,
    listeners: Pool<ApplicationListener>,
    application_by_api_client: HashMap<u32, u32>,
    // VPP app_main.app_by_name stores a borrowed memory key, not a copied
    // String and not a hash-only/index-only surrogate.
    application_by_name: HashMap<&'app String, u32>,
    // VPP appsl_wrk_t.pending_rx_mqs is an intrusive-list head per worker.
    pending_rx_mq_heads: Vec<u32>,
    pending_connects: SpinLock<Vec<u32>>,
    burst_connects: Vec<SessionControlData>,
    connect_data: Pool<SessionControlData>,
}

impl<'app> ApplicationMain<'app> {
    // VPP: application_init initializes app_main worker state and lookup tables.
    // All fallible allocation completes before OnceLock installation.
    pub fn init(worker_count: u32) -> Result<(), ApplicationError>;

    // This process-main accessor does not widen the separate 'app lifetime
    // carried by the borrowed name key.
    #[inline(always)]
    pub fn global() -> Result<&'static Self, ApplicationError>;

    // VPP: application_alloc_and_init, application.c:664-767.
    pub fn attach(&mut self, config: ApplicationConfig) -> Result<u32, ApplicationError>;

    // VPP application_t.ns_index, application.h:139-151. The service keeps
    // the raw namespace pool index; generic namespace ownership remains in
    // hammer-app and IP binding validation remains in plugin-session.
    #[inline(always)]
    pub fn namespace(&self, application: u32) -> Option<u32>;

    // VPP: application_alloc_worker_and_init/vnet_app_worker_add_del,
    // application.c:986-1074.
    pub fn attach_worker(&mut self, application: u32, api_client: u32)
        -> Result<u32, ApplicationError>;

    pub fn detach_worker(&mut self, application: u32, worker_map: u32)
        -> Result<(), ApplicationError>;

    // VPP: application_detach_process/application_free, application.c:771-861.
    pub fn detach(&mut self, application: u32) -> Result<(), ApplicationError>;

    #[inline]
    pub fn application(&self, application: u32) -> Option<&Application<'app>>;

    // VPP: application_lookup_name, application.c:321-330. The argument is
    // borrowed only for this lookup; the table retains a non-owning key to the
    // Application-owned bytes.
    #[inline]
    pub fn lookup_name(&self, name: &str) -> Option<&Application<'app>>;

    #[inline]
    pub fn worker(&self, worker: u32) -> Option<&AppWorker<'app>>;

    #[inline]
    pub fn listener(&self, listener: u32) -> Option<&ApplicationListener>;

    #[inline]
    pub fn listener_for_session(&self, session: SessionHandle) -> Option<u32>;
}
```

`Application` 保存 VPP 的 application-level facts，并保存所选 namespace 的 raw `u32` pool
index。它不保存 namespace object、socket、IP endpoint、FIB 或 protocol connection。ADR-0036
的 `hammer-app::AppNamespaceMain<B>` 仍拥有 generic namespace record，
`hammer-plugin-session::IpNamespaceMain` 仍拥有 IP concrete binding；这里仅保存 Application
到 namespace 的归属关系。`Application.name: String` 是唯一 name owner；`app_main.app_by_name`
保存 `&String` 借用键，不保存 `String` 副本，也不退化为只有 hash 的 `u64 -> Vec<u32>` 表。
Application name 的借用生命周期与 Application store 一致，不能写成无关的 `'static`；name 在
插入索引后不可修改，detach 时必须先删除 `app_by_name` 中的借用键，再释放 Application。
lookup 的调用参数仍是普通 `&str`。

本 ADR 对“拥有、借用、非拥有引用”的边界固定如下：

- `Application` 拥有 `name: String`、`rx_mq_segment: SvmFifoSegment` 和 `rx_mqs` 元数据；
  `app_by_name` 只借用 `&String`，其生命周期与 Application store 相同。
- `ApplicationRxMq.queue`、`AppWorker.event_queue` 和 `SegmentManager.event_queue` 对应 VPP
  的 `svm_msg_q_t *`，在 Rust 中保存 owner 生命周期内的 `&SvmMsgQ`；队列存储始终由 SVM
  segment/manager 拥有，禁止复制 `SvmMsgQ`。
- `Session.rx_fifo`、`Session.tx_fifo`、CT FIFO 和 SegmentManager 的 segment pool 是拥有值；
  `Session::rx_fifo()`/`tx_fifo()`、`SegmentManager::event_queue()` 等只返回调用者生命周期内
  的借用。跨 worker 或跨 pool 保存 `u32`/`SessionHandle`，不保存长生命周期 Rust 引用。
- `Option` 只表示 VPP 语义上的确实缺失，例如 listener 尚未建立 local/global Session、
  Session 尚未绑定 application，或 lookup 没有命中；已经完成初始化的 name、segment、MQ 和
  FIFO 不用 `Option`。

`pending_connects` 的 lock 只保护 VPP 对应的跨 worker connect RPC list
（`application.h:230-238`、`application.c:1384-1443`），不是 packet path 的通用锁，也不是
`SpinLock<()>`。队列元素是 `connect_data` pool 的 raw `u32` index，所有权转移后 producer
不再触碰该 entry；burst pool 只由 connect thread 消费。

```rust
// VPP: application_t, application.h:119-161.
pub struct Application<'app> {
    index: u32,
    flags: ApplicationFlags,
    segment: SegmentManagerProperties,
    worker_maps: Pool<ApplicationWorkerMap>,
    // Sole owner of the application name bytes. The name is immutable after
    // insertion because app_by_name retains a non-owning reference to it.
    name: String,
    namespace: u32,
    listeners: Pool<u32>,
    workers: Pool<u32>,
    rx_mq_segment: SvmFifoSegment,
    rx_mqs: Vec<ApplicationRxMq<'app>>,
}

impl<'app> Application<'app> {
    #[inline(always)]
    pub const fn index(&self) -> u32;

    #[inline(always)]
    pub const fn flags(&self) -> ApplicationFlags;

    #[inline(always)]
    pub fn name(&self) -> &str;

    #[inline]
    pub const fn namespace(&self) -> u32;

    // VPP: application_rx_mq_get, application.c:504-511. The worker index
    // selects the fixed Application RX MQ entry.
    #[inline(always)]
    pub fn app_rx_mq(&self, worker: DataWorkerId) -> Option<&ApplicationRxMq<'app>>;
}
```

`SessionAppVft`/`session_cb_vft_t` 是迁移兼容面，不是目标字段。目标是静态 trait contract：
ApplicationMain 不保存 callback table、function pointer 或 `dyn` object；具体 application owner
以泛型参数实现 trait，并在 event dispatch 时通过 `&mut C` 直接调用。这样多个 application
仍可共享同一 concrete callback owner，而 service 不需要异构 callback storage。

```rust
// VPP: application_t.cb_fns and callback sites in application_worker.c:934-1001,
// session_input.c:80-153. Rust uses static dispatch; there is no session_cb_vft_t.
pub trait ApplicationCallbacks {
    fn accepted(
        &mut self,
        application: u32,
        session: SessionHandle,
    ) -> Result<(), ApplicationError>;

    fn connected(
        &mut self,
        application: u32,
        session: Option<SessionHandle>,
        opaque: u64,
        error: Option<SessionError>,
    ) -> Result<(), ApplicationError>;

    fn disconnected(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn reset(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn transport_closed(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn rx(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn tx(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn cleanup(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn half_open_cleanup(&mut self, application: u32, session: SessionHandle)
        -> Result<(), ApplicationError>;

    fn listened(&mut self, application: u32, listener: u32, opaque: u64)
        -> Result<(), ApplicationError>;

    fn unlistened(&mut self, application: u32, listener: u32, opaque: u64)
        -> Result<(), ApplicationError>;
}

impl<'app> ApplicationMain<'app> {
    // VPP: app_worker_flush_events_inline and app_worker event callbacks,
    // session_input.c:80-153. C is monomorphized and borrowed directly.
    pub fn dispatch_event<C: ApplicationCallbacks>(
        &self,
        callbacks: &mut C,
        event: ApplicationEvent,
    ) -> Result<(), ApplicationError>;
}
```

The old `register_session_app(application, SessionAppVft)` and callback-table lookup are deprecated
for migration and are removed after all consumers use `ApplicationCallbacks`. No replacement
registration API, provider registry or callback closure is introduced.

```rust
// VPP: application_alloc_and_init, application.c:664-767; application_t.ns_index,
// application.h:119-151. The namespace value is the existing pool index.
pub struct ApplicationConfig {
    pub namespace: u32,
    pub flags: ApplicationFlags,
    pub name: String,
    pub segment: SegmentManagerProperties,
}

// VPP: app_rx_mq_flags_t, application.h:103-107.
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationRxMqFlags: u8 {
        const PENDING = 1 << 0;
        const POSTPONED = 1 << 1;
    }
}

// VPP: app_rx_mq_elt_t, application.h:109-117. `next`/`previous` are the
// intrusive pending-list links; `file_index` is the FileMain registration
// index; queue storage is the existing SVM message queue.
pub struct ApplicationRxMq<'app> {
    next: u32,
    previous: u32,
    // VPP app_rx_mq_elt_t.mq is svm_msg_q_t*. The queue is owned by
    // Application::rx_mq_segment; this field is only an owner-lifetime borrow.
    queue: &'app SvmMsgQ,
    file_index: usize,
    application: u32,
    flags: ApplicationRxMqFlags,
}

impl<'app> ApplicationRxMq<'app> {
    // VPP: application_rx_mq_get, application.c:504-511.
    #[inline(always)]
    pub fn queue(&self) -> &SvmMsgQ;

    #[inline]
    pub fn flags(&self) -> ApplicationRxMqFlags;
}
```

`Application.rx_mqs` 是按 Data Worker 固定长度的 `Vec<ApplicationRxMq>`，不是
`Vec<Option<_>>`；每个 entry 都有一个对应的 SVM MQ。`ApplicationMain.pending_rx_mq_heads` 是
每个 worker 的 intrusive-list head，entry 的 `next`/`previous` 直接复用 VPP 的 pending list
语义；不再把它错误地改成另一种 `Vec<Vec<u32>>` queue。`hammer-infra` 的 SVM MQ 和
FileMain eventfd/file registration 是唯一消息入口，不新增 mailbox 或 ring。

### 3.2 SegmentManager 与 FIFO ownership

`SegmentManager` 是 service 的协议无关 SVM resource owner。它不是 IP session 实例，也不是
TCP/UDP state；`SegmentManagerMain` 持有 manager pool，`AppWorker` 只持有 manager 的 `u32`
索引。VPP 的 segment pool、读写锁、owner worker、first-segment protection、event MQ 和 FIFO
allocation policy 均落在这个 owner 中（`segment_manager.h:15-80`、`segment_manager.c:389-488`）。

```rust
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::SvmMsgQ;
use hammer_runtime::sync::RwLock;

// VPP: segment_manager_props_t, segment_manager.h:15-37.
pub struct SegmentManagerProperties {
    pub rx_fifo_size: u32,
    pub tx_fifo_size: u32,
    pub event_queue_size: u32,
    pub preallocated_fifos: u32,
    pub preallocated_fifo_headers: u32,
    pub segment_size: usize,
    pub add_segment_size: usize,
    pub add_segment: bool,
    pub use_mq_eventfd: bool,
    pub max_fifo_size: u32,
    pub high_watermark: u8,
    pub low_watermark: u8,
    pub max_segments: u32,
}

// VPP: seg_manager_flag_t, segment_manager.h:39-50.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmentManagerFlags {
    pub detached: bool,
    pub detached_listener: bool,
    pub listener: bool,
    pub connects: bool,
}

// VPP: segment_manager_t, segment_manager.h:52-80. The segment pool is
// protected as one value; no SpinLock<()> or separate state wrapper exists.
pub struct SegmentManager<'segment> {
    segments: RwLock<Pool<SvmFifoSegment>>,
    owner_app_worker: u32,
    first_segment_protected: bool,
    // VPP segment_manager_t.event_queue is svm_msg_q_t*. The queue is owned
    // by the first SVM FIFO segment, not copied into the manager.
    event_queue: &'segment SvmMsgQ,
    flags: SegmentManagerFlags,
    max_fifo_size: u32,
    high_watermark: u8,
    low_watermark: u8,
}

// VPP: segment_manager_main_t, segment_manager.c:52-80. The service owns
// this pool and its defaults; an AppWorker stores only a raw manager index.
pub struct SegmentManagerMain<'segment> {
    segment_managers: Pool<SegmentManager<'segment>>,
    segment_name_counter: u32,
    defaults: SegmentManagerProperties,
}

impl<'segment> SegmentManagerMain<'segment> {
    // VPP: segment_manager_main_init, segment_manager.c:1880-1898.
    pub fn init(properties: SegmentManagerProperties) -> Result<Self, SegmentManagerError>;

    // VPP: segment_manager_alloc/get/get_if_valid, segment_manager.c:389-421.
    pub fn allocate(
        &mut self,
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
        event_queue: &'segment SvmMsgQ,
    ) -> Result<u32, SegmentManagerError>;

    #[inline(always)]
    pub fn get(&self, manager: u32) -> Option<&SegmentManager<'segment>>;

    pub fn free(&mut self, manager: u32) -> Result<(), SegmentManagerError>;
}

impl<'segment> SegmentManager<'segment> {
    // VPP: segment_manager_alloc/init, segment_manager.c:389-421.
    pub fn init(
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
        event_queue: &'segment SvmMsgQ,
    ) -> Result<Self, SegmentManagerError>;

    // VPP: segment_manager_event_queue, segment_manager.h:155-159.
    #[inline(always)]
    pub fn event_queue(&self) -> &'segment SvmMsgQ;

    // VPP: segment_manager_alloc_session_fifos,
    // segment_manager.c:744-892. The pair is allocated before a Session is
    // inserted into its worker pool; no FIFO field is optional afterwards.
    pub fn allocate_session_fifos(
        &self,
        worker: DataWorkerId,
    ) -> Result<(SvmFifo, SvmFifo), SegmentManagerError>;

    // VPP: segment_manager_alloc_session_fifos_ct,
    // segment_manager.c:1567-1678. CT allocation retains the same direct
    // FIFO ownership and only changes the segment accounting flags.
    pub fn allocate_cut_through_fifos(
        &self,
        worker: DataWorkerId,
    ) -> Result<(SvmFifo, SvmFifo), SegmentManagerError>;

    // VPP: segment_manager_dealloc_fifos/dealloc_fifos_ct,
    // segment_manager.h:136-148 and segment_manager.c:892-934.
    pub fn deallocate_session_fifos(
        &self,
        rx_fifo: SvmFifo,
        tx_fifo: SvmFifo,
    ) -> Result<(), SegmentManagerError>;

    // VPP: segment_manager_init_free/segment_manager_free_safe,
    // segment_manager.c:524-584.
    pub fn mark_detached(&mut self);
    pub fn free(self) -> Result<(), SegmentManagerError>;
}
```

The only `Option` at this boundary is an ordinary lookup result such as `ApplicationMain::application`
or a pool lookup. An initialized `Application`, `Session`, `AppWorker` or `CtConnection` always has
its required name, segment, event-queue address and FIFO values; `rx_fifo`/`tx_fifo` are not optional.
The SVM segment or manager owns queue storage, while application records retain VPP-style non-owning
queue references. Allocation failure is returned before publication; cleanup takes the two concrete
FIFO values back to the same `SegmentManager`.

### 3.3 AppWorker 与 SVM ownership

`AppWorker` 对齐 VPP `app_worker_t`，但所有 app/session exchange 使用仓库已有的
`hammer-infra` SVM FIFO/MQ。不得恢复旧 FIFO，不得引入第二种 ring 或 mailbox。真实的可选关系
才用 `Option` 表达；per-worker collection 直接使用 `Vec`，不能用 `Vec<Option<_>>` 伪造固定
worker/session table。

```rust
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_infra::svm::msg_queue::SvmMsgQ;

// VPP: app_worker_t, application.h:32-81; application_worker.c:15-134.
#[repr(C)]
pub struct AppWorker<'segment> {
    cacheline0: CacheLineAlignMark,
    worker_index: u32,
    worker_map_index: u32,
    application: u32,
    // VPP app_worker_t.event_queue is svm_msg_q_t*; the segment manager owns
    // storage and this field keeps only the segment-lifetime borrow.
    event_queue: &'segment SvmMsgQ,
    // VPP app_worker_t.connects_seg_manager is a pool index.
    connects_segment_manager: u32,
    listeners: std::collections::HashMap<SessionHandle, u32>,
    half_open: Pool<SessionHandle>,
    api_client: u32,
    app_is_builtin: bool,
    // VPP app_worker_t.wrk_evts and wrk_mq_congested,
    // application.h:70-74; direct per-worker Vec storage.
    events_by_worker: Vec<Vec<ApplicationEvent>>,
    mq_congested: bool,
    worker_mq_congested: Vec<bool>,
    detached_segment_managers: Vec<u32>,
}

impl<'segment> AppWorker<'segment> {
    // VPP: application_alloc_worker_and_init, application.c:986-1074.
    pub fn init(
        application: u32,
        worker_map_index: u32,
        connects_segment_manager: u32,
        event_queue: &'segment SvmMsgQ,
    )
        -> Result<Self, ApplicationError>;

    // VPP: app_worker_add_half_open, application_worker.c:633-643.
    pub fn add_half_open(&mut self, handle: SessionHandle)
        -> Result<u32, ApplicationError>;

    // VPP: app_worker_own_session, application_worker.c:728-760.
    pub fn own_session(&mut self, session: SessionHandle)
        -> Result<(), ApplicationError>;

    // VPP: app_worker_add_event/app_worker_add_event_custom,
    // application_worker.c:934-967. This is a bounded data-plane outcome,
    // not a numeric error code.
    #[inline]
    pub fn add_event(&mut self, worker: DataWorkerId, event: ApplicationEvent)
        -> ApplicationEventResult;

    // VPP: app_wrk_flush_wrk_events and app_worker_flush_events_inline,
    // session_input.c:80-389.
    pub fn flush_events(&mut self, worker: DataWorkerId)
        -> Result<(), ApplicationError>;

    // VPP: app_worker_free, application_worker.c:48-134.
    pub fn free(self) -> Result<(), ApplicationError>;
}
```

`event_queue` 是 application 给外部 app 接收 control/IO event 的 SVM MQ；
`events_by_worker` 是 Session worker 到达 app worker 的 bounded event storage。两者含义不同，
但都复用现有 SVM/infra primitive。producer 在移交 event 后不再访问该 event；MQ full、lock
failure 和 postponed event 使用 `ApplicationEventResult`，由 Session input node 决定保留/重试，
不把每一个 packet/event failure 变成 control-plane `Result`。

```rust
// VPP: session.c:34-85 and application_worker.c:969-1001.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplicationEventResult {
    Queued,
    Deferred,
    QueueFull,
    LockUnavailable,
}

// VPP: session_node.c/session_input.c event families.
#[derive(Debug)]
pub enum ApplicationEvent {
    Accepted { application: u32, session: SessionHandle },
    Connected {
        application: u32,
        session: Option<SessionHandle>,
        opaque: u64,
        error: Option<SessionError>,
    },
    Disconnected { application: u32, session: SessionHandle },
    Reset { application: u32, session: SessionHandle },
    TransportClosed { application: u32, session: SessionHandle },
    Cleanup { application: u32, session: SessionHandle },
    HalfOpenCleanup { application: u32, session: SessionHandle },
    Rx { application: u32, session: SessionHandle },
    Tx { application: u32, session: SessionHandle },
    Listened { application: u32, listener: u32, opaque: u64 },
    Unlistened { application: u32, listener: u32, opaque: u64 },
}
```

### 3.4 ApplicationListener

`ApplicationListener` 对齐 `app_listener_t`，不是 IP endpoint。它只记录 application owner、
accepting app workers、accept rotor、由 Session 分配的 global/local listening Session handle，
以及 connectionless worker listener 的 service-owned pool mapping。IP key 不进入该 record。

```rust
use hammer_infra::bitmap::Bitmap;

// VPP: app_listener_t, application.h:88-101; application.c:24-186.
pub struct ApplicationListener {
    index: u32,
    application: u32,
    workers: Bitmap<u32>,
    accept_rotor: u32,
    global_session: Option<SessionHandle>,
    local_session: Option<SessionHandle>,
    worker_sessions: Vec<u32>,
    opaque: u64,
}

impl ApplicationListener {
    #[inline(always)]
    pub const fn index(&self) -> u32;

    #[inline(always)]
    pub const fn handle(&self) -> Option<SessionHandle>;

    // VPP: app_listener_select_worker, application.c:172-185.
    #[inline]
    pub fn select_worker(&mut self) -> Result<u32, ApplicationError>;

    // VPP: app_listener_get_session/get_local_session/get_wrk_cl_session,
    // application.c:188-210.
    #[inline]
    pub fn global_session(&self) -> Option<SessionHandle>;

    #[inline]
    pub fn local_session(&self) -> Option<SessionHandle>;

}
```

Worker attach/detach follows `app_worker_start_listen`/`app_worker_stop_listen`
(`application_worker.c:338-384`, `:466-490`): the first worker creates the listening Sessions and
transport listener; each additional app worker only receives a listener segment and is added to the
worker set. The last-worker path is a two-owner cleanup: `IpSessionMain` removes the IP lookup entry
and the concrete plugin stops the transport, then `ApplicationMain::remove_listener` deallocates the
generic listener FIFOs/segments and frees the application listener. `ApplicationListener` itself
does not call an IP or transport operation.

## 4. Session record and app/session relation

Application ownership is a relation on the existing generic `Session<O>`, not a new
`ApplicationSession` wrapper. The `O` parameter is the session opaque, corresponding to VPP's
`session_t.opaque`; it lets each concrete owner retain its own fact without `void *` or a universal
context type.

VPP's app-facing `app_session_t` includes `volatile u8 session_state` and FIFO/session identity
(`application_interface.h:275-290`). Rust must not use `read_volatile` as a synchronization primitive:
the shared state is an `AtomicU8` with the existing acquire/release conversion, while the opaque is a
normal owned generic value. SVM FIFO ownership remains with the AppWorker/segment manager.

```rust
use std::sync::atomic::AtomicU8;
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::svm::fifo::Fifo as SvmFifo;

// VPP: app_session_t, application_interface.h:275-290;
// session_t allocation/cleanup, session.c:244-304.
#[repr(C)]
pub struct Session<O> {
    rx_fifo: SvmFifo,
    tx_fifo: SvmFifo,
    handle: SessionHandle,
    session_type: u8,
    state: AtomicU8,
    application: Option<u32>,
    app_worker: Option<u32>,
    listener: Option<SessionHandle>,
    connection_index: u32,
    opaque: O,
    cacheline_end: CacheLineAlignMark,
}

impl<O> Session<O> {
    // VPP: session_alloc/session_free, session.c:244-269.
    // SegmentManager allocates the pair before this record is published.
    pub fn allocate(
        handle: SessionHandle,
        session_type: u8,
        opaque: O,
        rx_fifo: SvmFifo,
        tx_fifo: SvmFifo,
    ) -> Self;

    #[inline(always)]
    pub fn state(&self) -> SessionState;

    #[inline(always)]
    pub fn store_state(&self, state: SessionState);

    #[inline(always)]
    pub fn opaque(&self) -> &O;

    #[inline(always)]
    pub fn opaque_mut(&mut self) -> &mut O;

    #[inline(always)]
    pub fn rx_fifo(&self) -> &SvmFifo;

    #[inline(always)]
    pub fn tx_fifo(&self) -> &SvmFifo;

    #[inline(always)]
    pub fn handle(&self) -> SessionHandle;
}
```

`SessionMain`/`SessionWorker` continue to own the pool and queue scheduling defined by ADR-0038/0039.
Application methods only set `application`, `app_worker`, `listener` and FIFO ownership at the VPP
lifecycle points; they do not add a second Session pool or a second event queue.

### 4.1 Cut-through application-local state

Cut-through (CT) is not a TCP/UDP transport plugin and is not an IP lookup feature. It is the
application-local Session path that pairs a client and server connection, shares the two SVM FIFO
directions, and defers cleanup until pending TX/RX events are drained. The service defines the
generic CT record and lifecycle operations; `hammer-plugin-session` supplies the concrete endpoint
parameter and owns the actual CT Main instance. Service therefore contains no IP endpoint, FIB,
namespace or concrete CT singleton.

```rust
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
use hammer_infra::svm::fifo::Fifo as SvmFifo;

// VPP: application_local.c:9-12 and :654-710.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CtCleanupRequest {
    pub connection: u32,
}

// VPP: foreach_ct_flags/application_local.h:12-29. These are semantic flags,
// not an integer error or a transport protocol enum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CtConnectionFlags {
    pub client: bool,
    pub half_open: bool,
    pub reset: bool,
}

// VPP: ct_connection_t, application_local.h:31-45; allocation defaults are
// application_local.c:46-86. This is the one CT extension record; it reuses
// the existing service TransportConnection<E, O> base and never substitutes a
// unit endpoint or defines a second transport base.
#[repr(C)]
pub struct CtConnection<E, O = u32> {
    pub base: TransportConnection<E, O>,
    pub client_worker: u32,
    pub server_worker: u32,
    pub client_opaque: u32,
    pub peer: u32,
    pub segment_handle: u64,
    pub segment_context: u32,
    pub ct_segment: u32,
    pub client_rx_fifo: SvmFifo,
    pub client_tx_fifo: SvmFifo,
    pub actual_transport: u8,
    pub flags: CtConnectionFlags,
}

impl<E, O> CtConnection<E, O> {
    // VPP: ct_session_get_peer, application_local.c:120-129.
    #[inline]
    pub fn peer_session(&self) -> Option<SessionHandle>;

    // VPP: ct_session_endpoint, application_local.c:131-140. The concrete
    // transport supplies endpoint details; CT stores only actual transport id.
    #[inline]
    pub fn actual_transport(&self) -> u8;
}

// VPP: ct_worker_t, application_local.c:14-23. Each worker entry is stored
// directly in CtMain.workers: Vec<CtWorker<E, O>>; no second per-worker state
// map and no protocol-specific service instance.
pub struct CtWorker<E, O = u32> {
    pub connections: Pool<CtConnection<E, O>>,
    pub pending_connects: SpinLock<Vec<u32>>,
    pub pending_cleanups: Vec<CtCleanupRequest>,
    pub new_connects: Vec<u32>,
    pub have_connects: bool,
    pub have_cleanups: bool,
}

// VPP: ct_main_t, application_local.c:25-36. Cumulative n_sessions is a
// VPP statistic and is intentionally not carried into this design. CtMain is
// a generic service shape; the concrete instance is owned by plugin-session.
pub struct CtMain<E, O = u32> {
    pub workers: Vec<CtWorker<E, O>>,
    pub reusable_half_open: SpinLock<Vec<u32>>,
    pub first_worker_pending_connects: Vec<Vec<u32>>,
    pub first_worker: u32,
    pub first_worker_flush: bool,
    pub initialized: bool,
}

impl<E, O> CtMain<E, O> {
    // VPP: ct_main initialization and worker setup, application_local.c:25-38.
    // This constructs the generic service shape; it does not publish a global
    // instance and does not choose an IP endpoint.
    pub fn init(worker_count: u32) -> Result<Self, ApplicationError>;

    // VPP: ct_connection_alloc/get/free, application_local.c:46-86.
    pub fn allocate_connection(&self, worker: DataWorkerId)
        -> Result<u32, ApplicationError>;

    #[inline]
    pub fn connection(
        &self,
        worker: DataWorkerId,
        connection: u32,
    ) -> Option<&CtConnection<E, O>>;

    // VPP: ct_half_open_alloc/get/add_reusable, application_local.c:88-118.
    pub fn allocate_half_open(&self) -> Result<u32, ApplicationError>;
    pub fn release_half_open(&self, connection: u32);

    // VPP: ct_accept_one and ct_accept_rpc_wrk_handler,
    // application_local.c:241-376.
    pub fn accept_pending(&self, worker: DataWorkerId) -> Result<usize, SessionError>;

    // VPP: ct_session_connect_notify, application_local.c:149-239.
    pub fn connect_notify(
        &self,
        session: SessionHandle,
        error: Option<SessionError>,
    ) -> Result<(), SessionError>;

    // VPP: ct_program_cleanup/ct_handle_cleanups/ct_session_postponed_cleanup,
    // application_local.c:633-710.
    pub fn program_cleanup(&self, worker: DataWorkerId, connection: u32)
        -> Result<(), SessionError>;

    // VPP: ct_session_close/ct_session_reset, application_local.c:726-764.
    pub fn close(&self, worker: DataWorkerId, connection: u32)
        -> Result<(), SessionError>;
    pub fn reset(&self, worker: DataWorkerId, connection: u32)
        -> Result<(), SessionError>;
}
```

`CtConnection<E, O>` is not a second `TransportConnection` type. `TransportConnection<E, O>` remains
the single service common base from ADR-0038; `CtConnection` is only the VPP CT-owned extension
record. The endpoint type `E` is chosen by the owning plugin. For the current IP composition,
`IpSessionMain` owns the concrete instance and its global lifecycle:

```rust
use hammer_service::session::CtMain;
use hammer_plugin_session::IpTransportConnectionId;

// VPP: ct_connection_t is embedded in the concrete transport pool;
// application_local.h:31-45. The IP type exists only in plugin-session.
pub type IpCtConnection = CtConnection<IpTransportConnectionId, u32>;
pub type IpCtWorker = CtWorker<IpTransportConnectionId, u32>;
pub type IpCtMain = CtMain<IpTransportConnectionId, u32>;

impl IpSessionMain {
    // VPP: application-local setup is part of the concrete session/transport
    // composition; application_local.c:38 and transport.c:1251-1290.
    pub fn init(config: IpSessionConfig) -> Result<(), IpSessionError>;

    #[inline(always)]
    pub fn global() -> Result<&'static Self, IpSessionError>;
}
```

The `IpCtMain` instance is constructed inside `IpSessionMain::init` and is never stored in
`hammer-service`, `SessionMain`, `ApplicationMain`, TCP/UDP Main, or a second registry. No
`TransportConnection<()>`, `CtTransportConnection`, `IpTransportConnection` copy, or erased CT
instance is introduced.

`CtWorker.pending_connects` is the only CT cross-thread critical section: the producer locks the
`Vec<u32>`, transfers half-open indices, and no longer touches transferred entries. `pending_cleanups`
is worker-owned and is retried when `SvmFifo` TX/event conditions still require it, matching
`ct_handle_cleanups` rather than freeing the connection immediately. CT does not add a mailbox,
multi-ring queue, stats field, IP table entry, TCP/UDP node or callback table.

## 5. Listen/connect lifecycle and static plugin connection

### 5.1 Service operations

Service allocates identity and ownership but does not interpret an IP endpoint. The following methods
are the only service-side listener/session hooks required by plugin/session:

```rust
// VPP: vnet_listen/vnet_unlisten, application.c:1276-1320 and :1465-1493;
// app_worker_start_listen/stop_listen, application_worker.c:338-490.
impl<'app> ApplicationMain<'app> {
    pub fn allocate_listener(
        &mut self,
        application: u32,
        worker: u32,
        opaque: u64,
    ) -> Result<u32, ApplicationError>;

    pub fn attach_listener_session(
        &mut self,
        listener: u32,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) -> Result<(), ApplicationError>;

    pub fn attach_listener_worker(
        &mut self,
        listener: u32,
        worker: u32,
    ) -> Result<(), ApplicationError>;

    pub fn detach_listener_worker(
        &mut self,
        listener: u32,
        worker: u32,
    ) -> Result<(), ApplicationError>;

    pub fn remove_listener(&mut self, listener: u32) -> Result<(), ApplicationError>;
}

// VPP: session_listen, session_open, session_open_stream,
// session.c:1280-1455 and :1467 onward.
impl SessionMain {
    pub fn allocate_listening_session<O>(
        &self,
        session_type: u8,
        opaque: O,
    ) -> Result<SessionHandle, SessionError>;

    pub fn attach_connection(
        &self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError>;

    pub fn allocate_connected_session<O>(
        &self,
        worker: u32,
        session_type: u8,
        opaque: O,
        listener: Option<SessionHandle>,
    ) -> Result<SessionHandle, SessionError>;

    // VPP: session_transport_closing_notify,
    // session_transport_closed_notify, session_transport_delete_notify,
    // session.c:964-1184.
    pub fn transport_closing(&self, session: SessionHandle) -> Result<(), SessionError>;
    pub fn transport_closed(&self, session: SessionHandle) -> Result<(), SessionError>;
    pub fn transport_deleted(&self, session: SessionHandle) -> Result<(), SessionError>;
}
```

These service methods do not accept `SocketAddr`, FIB ids, IP family fields or a TCP/UDP connection
object. `connection_index` is only the numeric backlink already present in VPP's `session_t`; its
meaning is owned by the concrete transport.

### 5.2 Plugin-session composition

`IpSessionMain` owns the concrete IP composition and uses the already initialized service
`SessionMain`. It does not contain a second SessionMain, an application pool, or application callbacks.
Its global lifecycle is also only `init` and `global`; there is no publication method.

```rust
use std::sync::OnceLock;
use hammer_service::session::SessionHandle;

// VPP: app_listener_lookup/application listener lookup,
// application.c:87-143; session_lookup_add_session_endpoint is the concrete
// lookup step performed by app_worker_listen_sep, application_worker.c:230-322.
pub struct IpSessionMain {
    lookup: IpSessionLookup,
    transport: IpTransportMain,
    ct: IpCtMain,
}

static IP_SESSION_MAIN: OnceLock<IpSessionMain> = OnceLock::new();

impl IpSessionMain {
    pub fn init(config: IpSessionConfig) -> Result<(), IpSessionError>;

    #[inline(always)]
    pub fn global() -> Result<&'static Self, IpSessionError>;

    // VPP: INVALID_NS handling in session_types.h:541-543 and application
    // namespace lookup before app_worker_listen_sep, application_worker.c:230-322.
    // The index stays a raw u32; no ApplicationNamespaceIndex is introduced.
    pub fn validate_application_namespace(
        &self,
        application: u32,
        namespace: u32,
    ) -> Result<(), IpSessionError>;

    #[inline]
    pub fn lookup(&self, connection: &IpTransportConnectionId)
        -> Option<SessionHandle>;

    // VPP: app_listener_lookup, application.c:87-143, backed by
    // session_lookup_endpoint_listener. The service maps the returned
    // listening Session back to its ApplicationListener index.
    pub fn lookup_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle>;

    // Adds the established/half-open connection identity to the IP table.
    // VPP: session_lookup_add_connection/session_lookup_add_half_open.
    pub fn add_connection(
        &self,
        connection: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), IpSessionError>;

    pub fn remove_connection(
        &self,
        connection: &IpTransportConnectionId,
        expected: SessionHandle,
    ) -> Result<(), IpSessionError>;

    // Binds a transport-created listening connection to the service listener.
    // The caller has already performed the concrete Transport<T> operation.
    pub fn add_listener(
        &self,
        connection: IpTransportConnectionId,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), IpSessionError>;

    pub fn remove_listener(
        &self,
        connection: &IpTransportConnectionId,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), IpSessionError>;
}
```

```rust
// VPP: SESSION_E_NOIP, SESSION_E_NOINTF, SESSION_E_NOROUTE,
// SESSION_E_PORTINUSE and SESSION_E_ADDR_NOT_IN_USE,
// session_types.h:526-539. These are IP endpoint/table failures, so their
// owner is plugin/session rather than the protocol-neutral ApplicationMain.
#[hammer_component_macros::runtime_error(subsystem = "session ip")]
#[derive(Debug, thiserror::Error)]
pub enum IpSessionError {
    #[error("IP endpoint is invalid for the requested transport")]
    InvalidEndpoint { protocol: u8 },
    #[error("IP listener endpoint is already in use")]
    ListenerInUse,
    #[error("IP listener endpoint is not present in the lookup table")]
    ListenerMissing,
    #[error("IP session lookup entry is missing")]
    LookupMissing,
    #[error("application namespace {namespace} does not exist")]
    InvalidNamespace { namespace: u32 },
    #[error("IP session lookup entry belongs to another Session")]
    SessionOwnerMismatch {
        expected: SessionHandle,
        actual: SessionHandle,
    },
    #[error("IP endpoint has no resolving interface")]
    InterfaceMissing,
    #[error("IP endpoint has no local address")]
    AddressMissing,
    #[error("IP route is unavailable for the endpoint")]
    RouteMissing,
}
```

`IpSessionMain` may call the service `ApplicationMain`/`SessionMain` directly by their concrete
owner APIs, but service never imports `IpSessionMain`. The IP plugin is therefore the only layer that
knows both `IpTransportConnectionId` and the generic `ApplicationListener` index. Attach order is:
`hammer-app` resolves/creates the namespace and supplies its `u32` index, service stores that index on
`Application`, and plugin-session validates that the index has an IP binding before any IP listener
or session-table operation. The Application record never receives `ip4_fib_id`, `ip6_fib_id`, a
socket path or a namespace binding object.

### 5.3 TCP/UDP call direction

TCP and UDP keep their concrete pools and implement the existing static `Transport<T>` trait. No
`TransportVft`, `dyn Transport`, `TcpSession`/`UdpSession` node, or protocol callback is introduced.

```rust
// VPP: application_worker.c:230-322 and session.c:1467-1505.
impl TcpMain {
    pub fn listen(
        &self,
        request: &IpSessionEndpoint,
        application: u32,
        worker: u32,
        opaque: u64,
    ) -> Result<u32, TcpError>;
}

impl UdpMain {
    pub fn listen(
        &self,
        request: &IpSessionEndpoint,
        application: u32,
        worker: u32,
        opaque: u64,
    ) -> Result<u32, UdpError>;
}
```

The concrete operation order is:

Before allocating anything, `vnet_listen` semantics are preserved: the caller supplies the raw
namespace index selected by `hammer-app`, `IpSessionMain::validate_application_namespace` verifies
that the application has an IP binding, then `IpSessionMain::lookup_listener` checks the concrete
endpoint identity and `ApplicationMain::listener_for_session` resolves the generic listener. An
existing listener owned by the same application only attaches another app worker; an existing listener
owned by another application returns `ApplicationError::AlreadyListening`.

For a new listener:

1. `ApplicationMain::allocate_listener` creates the generic app-listener record.
2. `SessionMain::allocate_listening_session` creates the service listening Session on the listener
   thread, matching VPP's `listen_session_alloc`.
3. TCP/UDP calls `Transport<IpTransportEndpointConfig>::start_listen` with the existing endpoint.
4. `IpSessionMain::add_listener` adds the IP lookup identity and validates the Session/connection
   backlink.
5. `ApplicationMain::attach_listener_session` and `attach_listener_worker` install the listener
   Session and allocate the worker's SVM FIFO segment.

If steps 3-5 fail, cleanup calls the concrete transport stop operation, removes the IP lookup entry,
stops/frees the service listening Session, and removes the application listener. Cleanup errors are
reported separately from the primary error; they never replace it. This mirrors VPP's
`app_worker_listen_sep` rollback and `app_listener_cleanup` (`application_worker.c:230-322`,
`application.c:145-170`).

Accept and connect use the same ownership direction:

- transport allocates its connection or half-open entry;
- `SessionMain` allocates/attaches the generic Session;
- `ApplicationMain` selects an accepting worker, allocates SVM FIFOs from that worker's listener or
  connect segment, and queues `Accepted`/`Connected`;
- `IpSessionMain` adds/removes the IP lookup identity;
- TCP/UDP sends only protocol notifications (`closing`, `closed`, `deleted`, `reset`) through the
  service Session methods.

This is the VPP sequence in `session.c:752-807`, `:1215-1276`, `:1322-1455` and
`application_worker.c:493-631`; no service method needs to know a TCP state machine or IP table.

## 6. Event and node integration

### 6.1 Application RX MQ

`app_rx_mq` is the application-to-session ingress path, not a second application event queue. Each
Application owns one SVM message queue per Data Worker in `rx_mqs`; FileMain watches that queue's
eventfd and adds the entry to `ApplicationMain.pending_rx_mq_heads[worker]` through its intrusive
`next`/`previous` links. The input node drains the queue through the existing `SessionWorker` MQ
handler, then either clears the pending flag or requeues the entry with the postponed flag when the
SVM MQ still contains messages.

```rust
impl<'app> ApplicationMain<'app> {
    // VPP: app_rx_mqs_alloc, application.c:513-564. The segment and all
    // per-worker SVM MQs are allocated as one application-owned resource.
    pub fn allocate_rx_mqs(
        &mut self,
        application: u32,
        worker_count: u32,
    ) -> Result<(), ApplicationError>;

    // VPP: app_rx_mq_fd_read_ready, application.c:428-453. FileMain calls
    // this on the queue's assigned worker.
    pub fn rx_mq_ready(
        &mut self,
        application: u32,
        worker: DataWorkerId,
    ) -> Result<(), ApplicationError>;

    // VPP: appsl_rx_mqs_input_node, application.c:374-426. A non-empty MQ
    // remains pending and causes the input node to be interrupted again.
    pub fn drain_rx_mqs(
        &mut self,
        worker: DataWorkerId,
    ) -> Result<usize, ApplicationError>;

    // VPP: application_enable_rx_mqs_nodes, application.c:580-587. The
    // default state is Disabled; enabling is explicit Session lifecycle/config.
    pub fn enable_rx_mq_node(&mut self, enabled: bool) -> Result<(), ApplicationError>;
}
```

`rx_mq_ready` is idempotent while `pending` is set, ignores an empty queue, and schedules the
existing `appsl-rx-mqs-input` node. `drain_rx_mqs` removes each current entry from the intrusive list
before handling it; when `SvmMsgQ` remains non-empty it sets `postponed` and appends the same entry to
that worker's pending list. If at least one Session event was handled while the Session worker is in interrupt mode,
the existing `session-queue` node is marked pending. There is no `dispatch_queue`, mailbox,
multi-ring message queue or new scheduler type.

The existing queue-node design remains unchanged:

```text
external app SVM MQ
    -> appsl-rx-mqs-input
    -> SessionWorker event queue
    -> session-queue / session-input

transport notification
    -> AppWorker per-worker event list
    -> appsl-rx-mqs-input/session-input
    -> Application event_queue (SVM MQ)
```

VPP evidence is `application.c:374-426` for `appsl-rx-mqs-input`, `session_input.c:80-153` and
`:350-404` for worker event flushing, and `session_node.c:1858-1917` for Session IO event dispatch.
`session-queue-process` keeps the one-second fallback and `session-queue-main` keeps the pre-input
wrapper from ADR-0039 (`session_node.c:2238-2307`). None of these nodes owns an Application pool,
transport callback, IP lookup table, or a new scheduler object.

The `appsl-rx-mqs-input` and session queue nodes are disabled by default, matching
`VLIB_NODE_STATE_DISABLED` (`application.c:422-426`) and ADR-0039. Application attach creates SVM
resources and FileMain registrations, but does not enable either node. Enable/disable remains
`SessionMain` lifecycle/configuration, not an Application side effect.

## 7. Error semantics

The operation owner defines typed errors in `hammer-service::session`; plugin/session defines only
IP endpoint/table errors, and TCP/UDP define protocol errors. Internal APIs never return a numeric
retval and never expose VPP's negative enum representation.

```rust
use thiserror::Error;

// VPP: foreach_session_error, session_types.h:519-561.
#[hammer_component_macros::runtime_error(subsystem = "session application")]
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("application {application} does not exist")]
    NoApplication { application: u32 },
    #[error("application {application} is already attached")]
    ApplicationAttached { application: u32 },
    #[error("application namespace index {namespace} is invalid")]
    InvalidNamespace { namespace: u32 },
    #[error("application worker {worker} does not belong to application {application}")]
    InvalidApplicationWorker { application: u32, worker: u32 },
    #[error("application listener {listener} does not exist")]
    NoListener { listener: u32 },
    #[error("application listener {listener} is already owned by this worker")]
    AlreadyListening { listener: u32 },
    #[error("application listener {listener} is owned by another application")]
    Owner { listener: u32 },
    #[error("application listener has no accepting worker")]
    NoAcceptingWorker,
    #[error("application listener segment could not be created")]
    SegmentCreate { source: SegmentError },
    #[error("application listener segment has no space for the FIFO pair")]
    SegmentNoSpace,
    #[error("application event message could not be allocated")]
    MessageAllocation { source: SvmMsgQError },
    #[error("application callback rejected the session")]
    CallbackRejected,
    #[error("application listener segment cleanup failed")]
    ListenerSegmentCleanup { listener: u32, source: SegmentError },
    #[error("application event queue detach failed")]
    EventQueueDetach { application: u32, source: SvmMsgQError },
}

// VPP: SESSION_E_ALLOC, SESSION_E_SEG_NO_SPACE, SESSION_E_SEG_CREATE and
// SESSION_E_EVENTFD_ALLOC, session_types.h:524-545 and :551-553; these errors
// belong to the service SegmentManager operation boundary.
#[hammer_component_macros::runtime_error(subsystem = "session segment")]
#[derive(Debug, Error)]
pub enum SegmentManagerError {
    #[error("segment could not be created")]
    SegmentCreate { source: SegmentError },
    #[error("segment has no space for the requested FIFO pair")]
    SegmentNoSpace,
    #[error("FIFO allocation failed")]
    FifoAllocation { source: SvmFifoError },
    #[error("event queue allocation failed")]
    EventQueueAllocation { source: SvmMsgQError },
    #[error("segment manager is already detached")]
    Detached,
}

// VPP: session.c lifecycle returns SESSION_E_NOSESSION, SESSION_E_OWNER,
// SESSION_E_TRANSPORT_NO_REG and SESSION_E_INVALID at the operation boundary.
#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session {session:?} does not exist")]
    NoSession { session: SessionHandle },
    #[error("session owner does not match the requested application or worker")]
    Owner { session: SessionHandle },
    #[error("transport protocol {protocol} is not registered")]
    TransportNotRegistered { protocol: u8 },
    #[error("session operation is invalid in the current state")]
    InvalidState { session: SessionHandle },
    #[error("session FIFO pair could not be allocated")]
    SegmentNoSpace { session: SessionHandle },
    #[error("transport close/delete notification arrived for a missing Session")]
    TransportSessionMissing { session: SessionHandle },
}
```

The names above are semantic names for VPP's `SESSION_E_*` categories; they do not contain numeric
discriminants, `code()` methods, `repr(i32)`, or a second error-code table. `Ok(())` represents
`SESSION_E_NONE`.

Application event queue congestion is an expected data-plane outcome (`ApplicationEventResult`), not
an `ApplicationError`. An invalid pool handle, a missing promised listener/session, or a broken
Session-to-transport backlink is an owner invariant and must assert at the owner boundary rather than
silently becoming `None`.

If a later Binary API request/reply exposes one of these categories, its server and client adapters
define their own request-specific retval enums and serialize the protocol-required `i32` only at that
ABI boundary. The service/application/session errors above are not those retval enums and are never
transported as numbers.

## 8. Cache-line and inline policy

### 8.1 Cache-line layout

VPP's `CLIB_CACHE_LINE_ALIGN_MARK` is a layout boundary, not a business field. Hammer already
provides `hammer_infra::align::CacheLineAlignMark`, the direct Rust equivalent. Use that existing
zero-sized marker at the same field boundary; do not replace it with `repr(align(64))`, add a new
marker type, or add a padding wrapper.

```rust
// VPP: app_worker_t.cacheline0, application.h:32-35.
#[repr(C)]
pub struct AppWorker {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    // hot worker fields
}

// VPP: session_worker_t.cacheline0, session.h:82-85.
#[repr(C)]
pub struct SessionWorker<O> {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    // ADR-0038 fields
}

// VPP: transport_connection_t end marker, transport_types.h:122-126.
#[repr(C)]
pub struct TransportConnection<E, O> {
    // ADR-0038 common base fields
    cacheline_end: hammer_infra::align::CacheLineAlignMark,
}

// VPP: ct_connection_t begins with transport_connection_t,
// application_local.h:31-45. CT-specific fields follow the existing base.
#[repr(C)]
pub struct CtConnection<E, O> {
    pub base: TransportConnection<E, O>,
    // peer, FIFO and CT lifecycle facts follow the base.
}
```

The concrete plugin chooses `E` and owns the pool allocation. `hammer-service` only defines the
layout contract. The implementation must verify the marker offsets at compile time and keep hot worker
fields before cold cleanup/configuration fields; it must not insert ad hoc cacheline fields beyond the
VPP source markers in Application, Session or transport records.

### 8.2 Inline policy

Inline attributes follow the actual VPP hot-path annotations; they are not applied to every accessor:

| Rust operation | Attribute | VPP source and reason |
| --- | --- | --- |
| `ApplicationMain::global`, pool-index/handle accessors | `#[inline(always)]` | VPP small `static inline` accessors in `application.h:262-303`; only a few loads/validations |
| `ApplicationListener::handle`, `select_worker` fast path | `#[inline]` | VPP listener handle/worker selection, `application.c:60-85`, `:172-185` |
| `Application::app_rx_mq`, `ApplicationRxMq::queue` | `#[inline(always)]` | VPP `application_rx_mq_get`, `application.c:504-511`; fixed vector/index access |
| `Application::name`, `SegmentManager::event_queue` | `#[inline(always)]` | direct initialized value access; VPP application/segment-manager accessors, `application.h:119-151`, `segment_manager.h:155-159` |
| `Session::state`, `store_state`, `opaque`, `handle` | `#[inline(always)]` | VPP session event/state hot path, `session.c:578-677`; Rust uses atomic acquire/release |
| `Session::rx_fifo`, `tx_fifo` | `#[inline(always)]` | direct FIFO pointer/value access after segment allocation, `segment_manager.c:858-892` |
| `CtConnection::peer_session`, `actual_transport`, `CtMain::connection` | `#[inline]` | VPP `ct_session_get_peer`/`ct_session_endpoint`, `application_local.c:120-140` |
| `AppWorker::add_event` fast enqueue check | `#[inline]` | VPP `app_worker_add_event`, `application_worker.c:934-967` |
| control-message enqueue helper | `#[inline(always)]` only for the bounded raw MQ write | VPP `app_wrk_send_ctrl_evt_inline`, `application_worker.c:969-1001` |
| event drain, listener start/stop, attach/detach, FIFO/segment allocation, cleanup, rollback, barrier | no inline annotation | VPP performs real pool, segment, transport and lifecycle work in ordinary functions |

`ApplicationMain::init`, `ApplicationMain::attach/detach`, `SessionMain::init`, app listener cleanup,
SVM segment mapping, WorkerBarrier entry and transport operations must not be `inline(always)`. A
process-global `global()` may return `'static`; ordinary Application, AppWorker, listener and Session
borrows retain their actual caller lifetime.

## 9. Deprecated migration surfaces

The following names describe the current migration input only; they are explicitly deprecated in the
target design and must not be extended:

| Deprecated surface | Replacement | VPP/Hammer evidence |
| --- | --- | --- |
| `SessionAppVft`, `session_cb_vft_t`, `register_session_app` and callback-table lookup | `ApplicationCallbacks` static trait plus `ApplicationMain::dispatch_event<C>` | VPP `application_t.cb_fns`, `application.h:127-129`; callback calls `application_worker.c:934-1001` |
| `TransportVft`, function-pointer protocol registry and `dyn Transport` | service `Transport<T>` trait implemented directly by concrete TCP/UDP Main | ADR-0038 static trait decision; VPP transport operations `transport.h:60-154` |
| `TransportConnection<()>`, `CtTransportConnection`, or a second common transport base | existing `TransportConnection<E, O>` plus generic `CtConnection<E, O>` extension, concretized as `IpCtConnection` in plugin-session | ADR-0038 `TransportConnection<E, O>`, VPP `ct_connection_t`, `application_local.h:31-45` |
| `ApplicationWorkerState`/`Box<[ApplicationWorkerState]>` and a separate pending queue | `ApplicationMain.pending_rx_mq_heads: Vec<u32>` plus `ApplicationRxMq.next/previous` | VPP `appsl_wrk_t`/`app_rx_mq_elt_t`, `application.h:103-117`, `:196-203` |
| old FIFO, `AppRing`, `multi_ring_msg_queue`, mailbox or dispatch queue | existing `hammer-infra` SVM FIFO/MQ and `appsl-rx-mqs-input` | VPP `application.c:374-426`, `:513-587` |
| CT statistics such as `ct_main_t.n_sessions` | no stats field in `CtMain` | VPP `application_local.c:25-36`; stats are outside this ADR |

These deprecated surfaces may remain temporarily for migration compilation, but no new consumer may
depend on them. The implementation deletes each compatibility path after its callers move; this ADR
does not preserve a second dispatch path.

## 10. Migration and non-decisions

1. Keep the existing service `ApplicationMain`, `AppWorker` and SVM FIFO/MQ as the migration starting
   point; replace the old callback surface with `ApplicationCallbacks` and move only fields that are
   IP/transport facts into `IpSessionMain`.
2. Replace numeric internal `SESSION_E_*` returns in the application/session path with the three
   owner-local typed error families above. Do not create `ApiError`, `error_codes!`, numeric code
   accessors or a universal boxed error.
3. Remove any duplicate application/listener pool or application callback storage from
   `hammer-plugin-session`; its only application-facing state is a raw `u32` listener/application
   identity passed to service APIs.
4. Keep namespace records and generic namespace CRUD under ADR-0036: `hammer-app` owns the generic
   namespace and supplies raw `u32`; this ADR adds that index to `Application` and validates its IP
   binding through `IpSessionMain`. Namespace socket work remains a later ADR. Do not add
   `ApplicationNamespaceIndex`, FIB fields, `ip4_fib_id + ip6_fib_id`, socket handles or a binding
   object to `Application`/`ApplicationListener`.
5. Do not add `Vec<Option<_>>`, a new mailbox, `multi_ring_msg_queue`, `SessionRuntimeEngine`,
   `SessionScheduler`, `publish` APIs, generic provider traits, `dyn` dispatch, or TCP/UDP Session
   nodes.
6. Do not change `hammer-ipc` ownership. Binary API declarations remain in the owner plugin and IPC
   remains the transport/codec capability as established by earlier ADRs.

## 11. Verification

The implementation phase must verify the boundary with focused compile/runtime tests rather than source
text assertions:

- application attach/detach allocates and releases exactly one AppWorker MQ set and listener pool;
- first listener worker creates the listening Session and concrete transport listener; a second worker
  only attaches its listener segment and worker mapping;
- accepted, connected, reset, closed and deleted transport notifications reach the owning AppWorker
  through the existing SVM event path;
- listener cleanup removes IP lookup identity before freeing the Session and application listener;
- a failed transport or FIFO allocation leaves no application listener, Session lookup entry or
  segment manager behind;
- service builds without a dependency on `hammer-plugin-session`, TCP or UDP.
