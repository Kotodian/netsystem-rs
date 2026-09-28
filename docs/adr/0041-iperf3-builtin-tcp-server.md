# ADR-0041: Builtin iperf3 TCP Server

- 日期：2026-09-26
- 状态：Proposed
- 前置决定：ADR-0037、ADR-0038、ADR-0039、ADR-0040
- VPP 基线：`third_party/vpp` 当前 vendored source；`vperf` 是 ownership 和事件路径参考
- 范围：在既有 session/application/FIFO 语义上设计一个 builtin iperf3 TCP server
- 非目标：本 ADR 不改代码、不新增 Binary API、不实现外部 client、不引入 UDP/TLS/QUIC、
  不引入 stats subsystem、不修改 `third_party/iperf/`

本文是设计契约，不是实现补丁。Rust 代码块只给出类型、方法签名和所有权边界；代码块中
的 `todo!()` 不是待提交实现。

## 1. 决定摘要

新增独立的 `plugin-iperf3` builtin application。它依赖 `hammer-service` 的 application/session
抽象、`plugin-session` 的 IP namespace/endpoint 组合和 TCP plugin 的 listener/connection 实例。
service 不知道 iperf3、IP 地址、TCP 状态或 iperf3 framing；TCP plugin 不知道 iperf3 控制协议；
iperf3 只通过 application callbacks、SessionHandle 和 service FIFO 操作完成业务。

```text
plugin-iperf3 (builtin business owner)
  Iperf3Main / Iperf3Worker / Iperf3Session
  control/data protocol state and callbacks
          |
          | attach/listen/SessionHandle; no TCP internals
          v
plugin-session
  IpSessionMain / namespace / endpoint / IP lookup
          |
          v
plugin-tcp
  TcpMain / listener / TcpConnection / timers / packet nodes
          |
          v
hammer-service::session
  ApplicationMain / AppWorker / SessionMain / FIFO / SVM MQ / session nodes
```

`hammer-app` 不是 builtin app owner。iperf3 不走 external-app `appsl-rx-mqs-input`，而是使用
VPP builtin application 的直接 callback 路径。Session queue nodes 仍由 service 注册和调度，
iperf3 只处理由 service 投递的 application event。

## 2. VPP 证据账本

| 语义 | VPP 来源 | 设计结论 |
| --- | --- | --- |
| builtin app attach、`IS_BUILTIN`、FIFO/segment 参数、callback table | `src/plugins/vperf/builtin/vperf_server.c:418-470` | iperf3 通过 service application attach；不复制 app pool |
| 控制 listener 与数据 listener 分开建立 | `vperf_server.c:491-523` | 两个 listener 都是同一 Application 的 listener record，角色由 plugin 保存 |
| 创建顺序与错误时 detach | `vperf_server.c:525-558` | attach -> control listen -> data listen；后一步失败按逆序清理 |
| 接收 callback、`session_state`、worker-local pool、`opaque` | `vperf_server.c:64-94` | accepted session 在所属 worker 分配 app record，opaque 仅保存 pool index |
| disconnect/reset 使用 session handle 调用 disconnect | `vperf_server.c:96-116` | callback 只发起 session-owned disconnect，不直接释放 TCP connection |
| builtin RX event 直接调用 app callback | `src/vnet/session/application_worker.c:80-149` | builtin app 不走外部 RX MQ 协议；业务 callback 消费 Session FIFO |
| accepted/connected/closed/reset cleanup 顺序 | `application_worker.c:157-295` | app callback 不可绕过 session state 和 cleanup event |
| stream RX：RX FIFO dequeue、TX FIFO backpressure、self-tap retry | `src/plugins/vperf/builtin/vperf_protos.c:59-155` | iperf3 复用 SVM FIFO；FIFO 满时设置 event 并让 session 重试 |
| stream TX：provision chunks、nocopy enqueue、TX event | `vperf_protos.c:224-251` | reverse/bidirectional 模式直接填充 FIFO reservation，不建立 payload Vec |
| app session 绑定 RX/TX FIFO、event queue、transport metadata | `src/plugins/vperf/vperf_test.h:214-229` | Rust 只借用 session-owned FIFO；不复制 TCP connection |
| per-worker pool、cacheline mark | `src/plugins/vperf/builtin/vperf_builtin.h:74-149` | worker-local business records；共享计数和 worker 热字段分离 |
| vperf errors FIFO full/grow/stuck | `src/plugins/vperf/builtin/vperf_error.def:6-9` | 热路径结果是 typed enum/counter 语义，不返回数字错误码 |
| session worker/main、volatile state、cacheline、pool barrier | `src/vnet/session/session.h:82-176`, `:211-308` | Rust 用 worker ownership、barrier 和原子；不把 volatile 当并发协议 |
| session state 为 volatile、opaque 为应用私有 | `src/vnet/session/session_types.h:240-290` | Rust Session 的 state 由拥有 worker 修改；opaque 是应用索引，不是类型擦除 |
| enable 默认关闭、初始化一次、节点 enable/disable | `src/vnet/session/session.c:2018-2107`, `:2208-2314` | iperf3 init 在 attach/listen 前调用既有 Session enable；不在 main-loop enter 注册 app node |
| segment manager FIFO/segment 生命周期与锁 | `src/vnet/session/segment_manager.h:15-80`, `segment_manager.c:125-245` | segment/FIFO owner 在 service；iperf3 只请求/归还资源 |
| TCP transport 将通知交给 session | `src/vnet/tcp/tcp_input.c:105-135`, `tcp.c:1127-1160` | TCP 只产生 transport event；协议业务不进入 TCP connection |
| transport registration 先写 transport slot，再通知 Session | `src/vnet/session/transport.h:60-122`, `transport.c:317-345` | registration slot 由 Session 分配；IP family/FIB 选择留在 plugin-session/TCP composition |
| Session registration 只建立 session type/output arc 与 TX dispatch | `src/vnet/session/session.c:1826-1847` | service 只保存 generic dispatch metadata，不保存 IP endpoint/FIB |
| Session listen 绑定 transport connection，但不产生 application listener handle | `src/vnet/session/session.c:1467-1515`, `:490-506` | listening Session、transport connection index、application listener handle 分层保存 |
| TCP init 注册同一个 TCP transport 到 IP4/IP6 output | `src/vnet/tcp/tcp.c:1402-1431`, `:1728-1750` | concrete IP registration 由 plugin-session/TCP plugin 完成，不下沉到 service |
| vperf CLI 在创建前 enable session 并用 worker barrier | `src/plugins/vperf/builtin/vperf_cli.c:86-110` | control-plane 创建/发布 listener 必须经过现有 WorkerBarrier |

## 3. 边界与依赖

### 3.1 service

service 只提供以下协议无关能力：Application、AppWorker、ApplicationListener、Session、
SessionMain/Worker、SessionEvent、SVM FIFO/MQ、SegmentManager 和 session queue nodes。它不出现
`Ip*`、`Tcp*`、`Iperf3*` 字段，也不保存 TCP listener 或 connection。

Transport 的生命周期由 Session 管理。具体 transport 在初始化时向 Session 注册；业务 app 不
直接取得 TCP Main，也不直接调用 TCP listener。`u8` protocol id 是 Session 的 registration
slot，不是 iperf3 自己定义的协议 enum。

```rust
// VPP: transport_register_protocol/transport_register_new_protocol,
// transport.c:317-345; session_register_transport, session.c:1826-1847.
use hammer_service::session::SessionEndpoint;

pub trait Transport<T> {
    fn start_listen(
        &self,
        endpoint: &SessionEndpoint<T>,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;

    fn stop_listen(&self, connection: u32) -> Result<(), SessionError>;
    fn close(&self, connection: u32, worker: u32);
    fn reset(&self, connection: u32, worker: u32);
    fn cleanup(&self, connection: u32, worker: u32);
}

impl SessionMain {
    // VPP: session_add_transport_proto, session.c:1905-1920.
    pub fn allocate_transport_protocol(&mut self) -> Result<u8, SessionError>;

    // VPP: session_register_transport, session.c:1826-1847. The caller
    // supplies the already selected service session type and output arc;
    // service does not interpret an IP family or FIB.
    pub fn register_transport(
        &mut self,
        protocol: u8,
        session_type: u8,
        output_node: u32,
    ) -> Result<(), SessionError>;

    // VPP: listen_session_alloc/session_listen, session.c:1467-1515.
    // The listening Session is distinct from the application listener and
    // from the concrete transport connection index.
    pub fn allocate_listening_session<O>(
        &self,
        session_type: u8,
        application_worker: u32,
        opaque: O,
    ) -> Result<SessionHandle, SessionError>;

    // VPP: session_alloc_for_connection and the transport/session backlink,
    // session.c:490-506.
    pub fn attach_connection(
        &self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError>;
}
```

Application callback 使用 ADR-0040 按事件拆分的 Rust `fn` 字段，它们直接保存在 service
Application record 中，不再包一个 callback 结构体或 `ApplicationCallbacks` trait。
service `session-input` 按运行时 application id 找到 Application，再按事件调用对应字段；
iperf3 的非捕获 callback 只访问当前 Session worker 的 `Iperf3Worker`。不新增 iperf3
event Node、第二条事件队列或 transport VFT。现有 `SessionAppVft` 和旧
`session::application` 模块标记弃用，旧 app 调用点不在本 ADR 中迁移。

`ApplicationError` 是 service 已有的 application owner-local error；它在需要时保留底层
`SessionError` source。VPP `SESSION_E_*` 语义仍在 service enum 中表达；不把 `i32`、数字
retval 或 plugin error 向上泄漏。预期的 FIFO 满/暂时不能投递属于 `ApplicationEventResult`，
不是一个新的宽泛 `SessionError`。

### 3.2 plugin-session

plugin-session 拥有 `IpSessionMain`、namespace 边界、IP endpoint 和 IP session table。它只负责
验证 namespace、构造 concrete endpoint identity 和维护 IP lookup；它不拥有 iperf3 records，
也不直接替代 Session 的 transport listen/attach 流程。

```rust
// ADR-0040 contract; VPP: transport.c:249-256 and
// session_lookup.c/session.c lookup paths.
impl IpSessionMain {
    pub fn validate_application_namespace(
        &self,
        application: u32,
        namespace: u32,
    ) -> Result<Option<()>, SessionError>;

    pub fn add_connection(
        &self,
        connection: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError>;

    pub fn remove_connection(
        &self,
        connection: &IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError>;
}
```

`IpSessionEndpoint` 只在 plugin-session 和其调用方出现。service 的 `ApplicationListener` 只
保存 listener/session handles；它不保存 endpoint、FIB 或两个族的字段。

TCP plugin 的 init 由 plugin-session 编排两次 service registration：一次为每个 concrete IP
session type/output arc。plugin-session 把 IP family、FIB/namespace 选择压缩成 service 的
opaque `session_type: u8` 和 output node index 后调用 `SessionMain::register_transport`；这两个
值在 service 中只是 session dispatch metadata，service 不读取、生成或存储 IP facts。该边界
对应 VPP `transport_register_protocol(..., FIB_PROTOCOL_IP4/6, output_node)` 先由 transport
选择 IP 组合，再由 `session_register_transport` 接收 session type/output arc。

### 3.3 TCP plugin

TCP plugin 实现 service 的 `Transport<T>` trait，拥有 TCP connection pool、listener dispatch、
timer 和 packet nodes。它在 init 时向 Session 注册 protocol id、TX mode 和 IPv4/IPv6 output
node。TCP 收到 SYN/FIN/RST 或发送窗口变化时，只通过 service transport/session 通知；它不调用
iperf3 callback，也不读取 `Iperf3Session`。

```rust
// VPP: tcp.c:1402-1431 and tcp.c:1728-1750.
pub struct TcpMain;

impl TcpMain {
    pub fn init(&self, session: &mut SessionMain) -> Result<u8, SessionError>;
}

impl Transport<IpTransportEndpointConfig> for TcpMain {
    // VPP: tcp_proto.start_listen -> tcp_session_bind;
    // transport.c:530-537 and session.c:1467-1491.
    fn start_listen(
        &self,
        endpoint: &IpSessionEndpoint,
        session: SessionHandle,
    ) -> Result<u32, SessionError>;

    fn stop_listen(&self, connection: u32) -> Result<(), SessionError>;
    fn close(&self, connection: u32, worker: u32);
    fn reset(&self, connection: u32, worker: u32);
    fn cleanup(&self, connection: u32, worker: u32);
}
```

`start_listen` 返回的是 TCP transport connection index；Session 随后写入 listening Session 的
`connection_index`，而不是返回 application listener handle。application listener handle 由
service `vnet_listen` 等价路径产生。连接 payload 始终由 service Session FIFO 保留，TCP 只读写
transport window/sequence 语义。TCP listener connection 保留请求已解析的 FIB identity；
`stop_listen` 用同一个 proto/FIB/IP/port key 归还本地端点，不能以默认 FIB 0 重建非默认
namespace 的 key。共享端点释放一次引用但尚未最终删除是正常成功状态；VPP 的 `-1`
仅表示未删除，并明确注明不是错误（`transport.c:688-715`）。

### 3.4 plugin-iperf3

iperf3 plugin 是业务 owner。它拥有配置、control/data listener role、worker-local session pool、
控制协议 parser/state 和每个 worker 的数据计数。它只能通过 service application callback 与
Session API 操作 FIFO；不得从 `IpSessionMain` 取 TCP connection 指针。

## 4. 类型与方法设计

### 4.1 配置、角色和 owner handles

```rust
// VPP analog: vperf_cfg_t (vperf_test.h:59-82), vperf_server_main_t
// (vperf_server.h:12-32). iperf3 framing remains plugin-owned.
pub struct Iperf3Config {
    // These concrete endpoint values are supplied by plugin-session. They
    // never cross into hammer-service or SessionMain.
    pub enable: bool,
    pub namespace: u32,
    pub control_endpoint: SocketAddr,
    pub data_endpoint: SocketAddr,
    pub duration: Duration,
}

impl Default for Iperf3Config {
    // Temporary startup policy: a loaded iperf3 plugin is enabled unless the
    // configuration explicitly sets `enable = false`.
    fn default() -> Self;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerRole {
    Control,
    Data,
}

```

省略 `namespace` 时，`Iperf3Config::default()` 选择默认 namespace 0；它不是必填配置。
显式给出无效索引时，由 plugin-session 的 endpoint 构造
返回 service `SessionError::InvalidNamespace`（VPP `application.c:1232-1269,1285-1298`）。

The plugin DSO owns this configuration section and its startup declarations:

```rust
// VPP: vperf configuration is consumed during startup before server creation;
// Hammer uses the existing ordered config/init registration instead of a CLI.
#[hammer_component_macros::config_function(
    name = "iperf3_config",
    section = "plugin.iperf3"
)]
fn configure_iperf3(config: Iperf3Config) -> RuntimeResult<()> {
    todo!()
}

#[hammer_component_macros::init_function(
    name = "iperf3_init",
    runs_after = ["session_init", "ip_transport_main_init", "session_lookup_init"]
)]
fn init_iperf3(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    todo!()
}

hammer_component_macros::declare_plugin!(
    name = "iperf3",
    load_after = ["session"],
    init_functions = [__INIT_FN_IPERF3_INIT],
    config_functions = [__CONFIG_FN_IPERF3_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    api_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = []
);
```

`session_capacity` 由 service `SessionConfig` 提供；FIFO pair 的容量和 segment 参数由
ADR-0040 `ApplicationConfig.segment: SegmentManagerProperties` 提供。iperf3 不定义同名
`fifo_capacity`/`session_capacity` 字段。配置中的两个 `SocketAddr` 只是输入值；
plugin-session 将 namespace/IP 事实解析成既有的
`IpSessionEndpointConfig = SessionEndpointConfig<IpTransportEndpointConfig>`；plugin-session
先按 Application Namespace 生成监听实例，Session/application 操作消费 worker、opaque 等请求
元数据，之后才把其中的 `IpSessionEndpoint` 交给 TCP 的
`Transport<IpTransportEndpointConfig>`，service 不接收 `SocketAddr` 作为 IP endpoint。

The config callback runs only when the iperf3 plugin image/DSO is loaded. Its section is optional and
defaults to `enable = true`; an explicit `enable = false` makes the init callback a no-op. When enabled,
`init_iperf3` performs the builtin attach/listen sequence only when enabled. It does not register a
worker-init callback and does not mutate any graph node state. Session enablement is one service-owned
lifecycle operation: the service session owner invokes the equivalent of VPP
`vnet_session_enable_disable(..., is_en = 1)` and `session_node_enable_disable(1)`, which applies the
correct per-thread state to `session-queue`, `session-input`, and `session-queue-process`. The iperf3
plugin only requests that existing service lifecycle; it never selects `Polling` itself. There is no
CLI command, CLI signal, `main_loop_enter` registration, or second runtime enable switch.

The startup document therefore uses one plugin section; the endpoint values are decoded by the
plugin-session concrete config path and are never added to `hammer-service`:

```toml
[plugin.iperf3]
enable = true
control_endpoint = "0.0.0.0:5201"
data_endpoint = "0.0.0.0:5202"
duration = "10s"
```

The literal endpoint representation above is configuration input only. `config_iperf3` hands the
validated concrete values to plugin-session; service receives only the generic listener/session
operations described in Section 5.1. If the DSO is absent, its config/init declarations do not exist
and the section has no effect; if it is present and `enable` is omitted, the temporary default is
enabled.

`Iperf3Main` 只保存 vperf 同样的 application/listener handles；不再引入第二套全局状态机。
handle 只有在对应 service 操作成功后才写入，失败时按逆序撤销已成功的 listener 和
application。Session 的 `Disabled`/enabled 状态仍由 service 持有，不由 iperf3 复制。

### 4.2 Main 与 per-worker 状态

```rust
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use std::sync::atomic::{AtomicBool, AtomicU32};

// VPP: vperf_builtin.h:74-124; session.h:82-176.
// The worker Vec is allocated and sized before data workers start.
#[repr(C)]
pub struct Iperf3Main {
    cacheline0: CacheLineAlignMark,
    config: Iperf3Config,
    workers: Vec<Iperf3Worker>,
    application: u32,
    control_listener: u32,
    data_listener: u32,
    transport_protocol: u8,
    stop_requested: AtomicBool,

    cacheline1: CacheLineAlignMark,
    accepted_connections: AtomicU32,
}

#[repr(C)]
pub struct Iperf3Worker {
    cacheline0: CacheLineAlignMark,
    sessions: Pool<Iperf3Session>,
    connection_handles: Vec<SessionHandle>,
    bytes_received: u64,
    bytes_sent: u64,

    cacheline1: CacheLineAlignMark,
    rx_events: u64,
    tx_events: u64,
}

impl Iperf3Main {
    // VPP: vperf_server_main_init, vperf_server.c:584-592; session_main_init.
    pub fn init(&mut self, config: Iperf3Config) -> Result<(), Iperf3Error>;

    // VPP: vperf CLI explicitly enables session before create,
    // vperf_cli.c:86-105. The caller must already hold WorkerBarrier.
    pub fn start(
        &mut self,
        session: &mut SessionMain,
        transport_protocol: u8,
    ) -> Result<(), Iperf3Error>;

    // VPP: vperf_server_detach, vperf_server.c:473-489.
    pub fn stop(
        &mut self,
        session: &mut SessionMain,
    ) -> Result<(), Iperf3Error>;

    #[inline(always)]
    pub fn worker(&mut self, worker: DataWorkerId) -> &mut Iperf3Worker;
}
```

`application`、listener handles 和 `transport_protocol` 只由 control/main owner 在 barrier 内
改变。它们对应 vperf 的 `app_index`、`ctrl_listener_handle` 和 `listener_handle`；不另加
generation 状态机。
`workers` 是直接 `Vec`，不是 `Vec<Option<_>>`；每个 data worker 的 `Pool<Iperf3Session>` 在
worker 启动前就存在。`accepted_connections` 仅用于 control-plane 观察，使用原子而不是锁；
业务 session records 和 FIFO 不通过它共享。

### 4.3 worker-local session record 与 opaque

```rust
// VPP: session_types.h:240-290 (session_state and opaque);
// vperf_server.c:72-81 (worker pool allocation and opaque assignment).
pub struct Iperf3Session {
    pub handle: SessionHandle,
    pub role: ListenerRole,
    pub phase: Iperf3SessionPhase,
    pub control: ControlParser,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub protocol_error: Option<Iperf3ProtocolError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Iperf3SessionPhase {
    Accepted,
    ControlNegotiating,
    DataReady,
    Running,
    Closing,
}

impl Iperf3Worker {
    // The returned index is written into the service Session opaque field.
    // VPP: vp_test_session_alloc and s->opaque assignment, vperf_builtin.h:126-134,
    // vperf_server.c:72-81.
    pub fn allocate_session(
        &mut self,
        handle: SessionHandle,
        role: ListenerRole,
    ) -> Result<u32, Iperf3Error>;

    #[inline(always)]
    pub fn session(&mut self, opaque: u32) -> Result<&mut Iperf3Session, Iperf3Error>;

    pub fn release_session(&mut self, opaque: u32) -> Result<(), Iperf3Error>;

}
```

`opaque` 是 worker-local pool index，不是 `Any`、trait object、指针或跨 worker 引用。callback
先使用 session 的 worker identity 找到 `Iperf3Worker`，再用 opaque 查 pool；找不到属于 owner
bug/cleanup race，应返回具体 `WorkerSessionMissing` 或在已确认不可能的内部路径断言。
Session 的 FIFO、state 和 transport handle 不复制到 `Iperf3Session`；需要时短暂借用
`SessionMain` 返回的 session。

### 4.4 Application callback 接线

VPP `vperf_server.c:408-454` 在 attach 参数中传入 C callback table，
`application.c:696-703` 将其复制进 `application_t`，`session_input.c:119-335` 按事件调用
不同槽。Hammer 不保留 table 包装，但保留槽的区分：

```rust
// VPP: vperf_server.c:408-416, :430-455; application_interface.h:15-59.
// Each non-capturing closure coerces to the corresponding Rust fn field.
let application = application_main.attach(ApplicationConfig {
    namespace: config.namespace,
    flags: ApplicationFlags::BUILTIN,
    name: String::from("iperf3"),
    segment,
    add_segment: |worker, segment| iperf3_add_segment(worker, segment),
    del_segment: |worker, segment| iperf3_del_segment(worker, segment),
    accepted: |worker, session| iperf3_accepted(worker, session),
    connected: |worker, opaque, session, error| {
        iperf3_connected(worker, opaque, session, error)
    },
    disconnected: |worker, session| iperf3_disconnected(worker, session),
    reset: |worker, session| iperf3_reset(worker, session),
    transport_closed: None,
    cleanup: Some(|worker, session, reason| iperf3_cleanup(worker, session, reason)),
    half_open_cleanup: None,
    migrated: None,
    listened: None,
    unlistened: None,
    builtin_rx: Some(|worker, session| iperf3_rx(worker, session)),
    builtin_tx: None,
})?;
```

这些名字表示直接交给 service 的事件函数，不是新注册 helper 或第二张表；函数内部按
`SessionWorker` 的当前 worker id 借用对应 `Iperf3Worker`，再处理该事件。`builtin_tx: None`
采用 VPP 的默认 no-op TX 语义。`cleanup` 明确释放 iperf3 的 worker-local record；
`transport_closed` 等为 `None` 仅表示 iperf3 对该通知没有额外动作，service 仍负责
Session/transport 的清理，不能把业务 record 的释放偷偷挪到 disconnect。
`ApplicationConfig` 和 `Application` 是已有配置/拥有者，不再加 callback 包装结构体。
attach/listen 必须在这些字段随 Application 一起发布后才允许 worker 处理事件；detach
先停止并 drain worker 事件，再释放 Application。VPP 的 event FIFO 顺序由 service 保证。
可拒绝 callback 的 plugin 错误在 application 边界转换一次为具体 `ApplicationError`
并保留 source，不用字符串或数字 retval。

## 5. 完整调用路径

### 5.1 init、enable、attach、listen

1. plugin DSO 被加载后，`config_function` 解析 `[plugin.iperf3]`；缺少 section 使用
   `Iperf3Config::default()`，其中 `enable = true` 是本阶段临时默认。config 阶段只保存配置，
   不创建 listener。
2. service `SessionMain::init`、`SegmentManagerMain::init`、plugin-session `IpSessionMain::init`
   和 TCP `TcpMain::init` 按既有 init graph 完成。session 默认 `Disabled`，与
   `session_main_init` 的 `is_enabled = 0` 对齐。
3. `init_function` 读取已保存的配置；`enable = false` 直接返回成功，不 attach application、
   不创建 listener，也不 enable Session。`enable = true` 调用既有 `SessionMain::enable`，
   然后通过现有 `WorkerBarrier` 停止 Data Workers，调用 plugin-session 的 builtin-app 编排：
   - service `ApplicationMain::attach`，flags 包含 `Builtin`，保存 raw namespace `u32`，并建立
     首个 AppWorker；这里不增加 builtin 专用包装，`Builtin` 只是 `ApplicationConfig.flags` 的一个值；
   - plugin-session 校验 namespace，用 `IpSessionMain::listen_endpoint_config` 从两个配置
     地址准备 concrete `IpSessionEndpointConfig`；这些值只在
     plugin-session/TCP plugin 的编排边界内存在；
   - 对每个 endpoint，plugin-session 的 `listen` 编排 service listener/listening Session、
     泛型 `Transport<IpTransportEndpointConfig>::start_listen` 和 IP lookup；Session 写入返回的
     transport connection index，并保持 listening Session 的 backlink；
   - plugin-session 的 `unlisten` 撤销 IP lookup、调用泛型 transport 的 `stop_listen`，再让
     service 清理其 listener/Session/segment；service 只接收 `SessionHandle`、`u32` listener
     handle、`u8` session type/protocol slot 和 generic endpoint 参数，不接收 IP endpoint；
   - 两个 listener 成功后才把两个 handle 写入 `Iperf3Main`；
   - barrier release 后 session queue/input nodes 继续运行。

这里的 endpoint request 复制只发生在 plugin-session 的具体 endpoint 编排中，不能复制
Application name、FIFO、TCP connection 或 listener record。service 不提供一个接收
`IpSessionEndpoint` 的 listen 方法，也不直接调用 `TcpMain`；它只提供 ADR-0040 已有的 generic
listener/session allocation 和 attach 方法。VPP 的真实调用关系由 plugin-session 编排为：

```rust
// VPP: vnet_listen/application.c:1277-1321;
// app_worker_start_listen/application_worker.c:230-322;
// session_listen/session.c:1467-1491; transport_start_listen/transport.c:530-537.
let mut request = plugin_session.listen_endpoint_config(address, namespace, transport_protocol)?;
request.opaque = Some(role as u32);
let (listener, listening_session) = plugin_session
    .listen(transport, application, worker_map, &request)?
    .expect("newly attached Application remains allocated");
```

`listen` 内部调用的 service 方法都是 ADR-0040 已有的 generic contract，不是本 ADR 新增的
TCP helper。`plugin_session.add_listener` 只在 plugin-session 内部参与 IP lookup 组合，不由
iperf3 直接调用。失败时 plugin-session 清理已创建的 transport/listener/Session，iperf3 不逐层
操作这些资源。
它对应 VPP `vnet_listen` 中的：

1. `application_get_if_valid(a->app_index)`；
2. `application_get_worker(app, a->wrk_map_index)`；
3. `session_endpoint_update_for_app` 和 namespace 校验；
4. `app_listener_lookup` 或 `app_listener_alloc`；
5. `app_worker_start_listen`；
6. `a->handle = app_listener_handle(app_listener)`。

因此 application listener handle 只能在 generic listener 和 concrete transport 都成功后写入
`Iperf3Main`；transport connection index、listening Session handle 和 application listener handle
是三个不同的值。VPP `vp_server_listen_ctrl`/`vp_server_listen` 在 `vnet_listen` 返回后读取
`args->handle`（`vperf_server.c:502-504`, `:520-522`）；Rust 保持同一结果方向，但失败时不
发布未定义 handle。

两次 VPP 调用的差异也必须保留：control path 直接把 `sep_ext.transport_proto` 设置为已
注册的 transport slot；data path 先由 vperf 选择测试 protocol，并在 vperf 中调整端口
（`vperf_server.c:507-523`）。本 ADR 的 TCP-only iperf3 由 plugin-session/TCP registration
提供同一个 `transport_protocol: u8` slot；iperf3 不写 `TransportProtocol::Tcp`，data port 由
iperf3 配置/协商决定，不能偷偷套用 vperf 的“端口加一”规则。两者最终都进入同一个
`vnet_listen` 语义，而不是新增两个 TCP listener API。

VPP `vperf_server_create` 先建立 worker slots，然后 attach、control listen、data listen
（`vperf_server.c:525-558`）。测试 Session pool 只在测试参数已知时按
`num_test_sessions / n_workers` 预留（`vperf_server.c:151-161,380-399`），不借用 service
Session pool capacity；iperf3 当前无启动时测试数，业务 pool 从 `Pool::new()` 按 accept 增长。
本设计保留 attach/listen 顺序，但把失败从 `int` 改成 typed enum。

### 5.2 accept

TCP plugin 完成 SYN/SYN-ACK 后只向 service 投递 accepted event。service 创建 Session、分配
RX/TX SVM FIFO、设置 session state，然后按 listener handle 产生 `ApplicationEvent::Accepted`。
iperf3 callback：

1. 由 SessionHandle 得到 worker，不跨 worker 借用 pool；
2. 从 listener role 判断 control/data；
3. 在该 worker 的 `Pool<Iperf3Session>` 分配 record；
4. 将 pool index 写入 Session opaque；
5. builtin accept callback 将 service Session 的 state 设为 `Ready`，同时由 iperf3 record
   区分 control/data phase；
6. 任何业务拒绝都调用 service 的 session detach/disconnect，不能直接 free TCP connection。

这对应 `vp_server_session_accept_callback` 与 `vp_server_session_alloc_and_init`
（`vperf_server.c:64-94`）：VPP 的 builtin callback 本身写入 `SESSION_STATE_READY`。
Hammer 仍由 service Session 持有 state，iperf3 callback 通过 Session 的状态方法修改它，
不会把状态镜像到 TCP connection 或 IP lookup。

### 5.3 control RX/TX

control RX callback 只从 Session RX FIFO 的 consumer side 读取完整 iperf3 control frame。
不创建 payload `Vec`，不把 frame 放入 TCP connection；解析器只保留结构化的控制字段和 phase。
控制响应直接写入 Session TX FIFO reservation，提交后调用 service 的 TX event。短消息的
编码错误是 `Iperf3ProtocolError`，不是 `SessionError`。

协议顺序由 iperf3 自身源码校准：服务端先读取 37 字节 cookie，然后发送
`PARAM_EXCHANGE`（`third_party/iperf/src/iperf_server_api.c:193-209`）；客户端随后发送
4 字节网络序长度和最多 8 KiB 的参数 JSON（`iperf_api.c:3197-3235`、
`iperf.h:460`）。参数解析成功后服务端发送 `CREATE_STREAMS`
（`iperf_api.c:2357-2394`），`TEST_START`/`TEST_RUNNING` 是服务端状态，
不能被误判为客户端发来的请求（`iperf_server_api.c:927-950`）。固定长度状态/长度头
用 `zerocopy` 类型解释；JSON 从 FIFO 按需读取，不复制整帧到临时 `Vec`。只有完整帧
解析成功才推进 FIFO head；同一次 RX 投递中连着到达的 cookie 和参数需持续消费。

```rust
// VPP analog: vp_server_rx_ctrl_callback, vperf_server.c:264-310;
// app_recv_stream/app_send_stream usage, vperf_protos.c:59-155.
// iperf3 parameter fields: third_party/iperf/src/iperf_api.c:2430-2610.
#[derive(serde::Deserialize)]
pub struct ControlParameters {
    pub tcp: bool,
    pub udp: bool,
    pub sctp: bool,
    pub parallel: Option<u32>,
    pub duration: Option<u32>,
    pub reverse: bool,
    pub bidirectional: bool,
}

pub enum ControlAction {
    SendState(ControlState),
    Parameters(ControlParameters),
    Close,
}

pub struct ControlParser;

impl ControlParser {
    pub fn consume(
        &mut self,
        rx: &SvmFifo,
    ) -> Result<Option<ControlAction>, Iperf3ProtocolError>;
}
```

`ControlParser` 是 iperf3 parser 的 concrete state；它不隐藏 session owner，也不接收整个
Application。若实现需要可替换的 parser 行为，使用外层静态泛型方法，不在 worker record 里放
trait object。

### 5.4 data RX/TX

普通 iperf3 server 只需接收并计数：从 RX FIFO 取得可消费字节，更新当前 worker/session
counter，完成 dequeue，并根据 VPP FIFO notification 规则重置/重新 program RX event。反向或
双向模式由 control phase 授权后，TX callback 直接从 FIFO producer reservation 生成 payload，
提交 `nocopy` 写入，再 program TX event。

`max_dequeue == 0` 是正常的无数据状态；data callback 直接 `drop_dequeue` 并更新当前
worker/session record。control callback 的 TX 写入使用现有 `AppSession::send_bytes`，FIFO
不足由 service FIFO event/backpressure 处理，不转换成数字 errno，也不创建 mailbox、
multi-ring 或 dispatch queue。

### 5.5 disconnect、reset、cleanup

TCP plugin 产生 closed/reset 后，service 先清除 `RX_READY`/closing 状态，再发
`disconnected` 或 `reset` callback。iperf3 callback 标记 phase `Closing`，释放 worker-local
record，随后调用 service disconnect（只对尚未由 session cleanup 接管的 handle）。cleanup event
到达时不得再次访问已释放的 record。

停止流程严格为：

1. 设置 `stop_requested`（Release）；
2. barrier 停止 worker，阻止新 accept；
3. 逐 worker disconnect 所有 session handle，等待 session cleanup event；
4. unlisten data，再 unlisten control；
5. service `ApplicationMain::detach`；plugin-session 的两次 `unlisten` 已完成各自的 IP endpoint
   binding 和 transport 清理，不再由 iperf3 逐层释放；
6. 清除 application/listener handles，释放 worker pools 和 configuration；
7. barrier release。

这对应 `vp_server_detach` 先 `vp_server_wrk_cleanup_sessions` 后
`vnet_application_detach`（`vperf_server.c:473-489`），并保留 application/segment/FIFO 的
反向释放顺序。

## 6. cacheline 与内存布局

- `Iperf3Worker` 使用已有 `CacheLineAlignMark`，worker-owned `Pool` 和 byte counters 位于
  worker 私有 cache line；不同 worker 不共享这些字段。
- `Iperf3Main` 的 startup handles/config 与 worker slots 由 cacheline marker 分隔；不额外引入
  观察原子或第二个共享状态机。
- `Session`、`AppWorker`、TCP connection 的 cacheline 规则由其 owner 保持；iperf3 不重新包裹
  或复制这些结构。VPP 对照：`session.h:82-176`、`session_types.h:240-290`、
  `vperf_builtin.h:74-124`。
- `Pool`/`Vec` 的 realloc 只在 worker barrier 内进行；worker 运行期间不持有可能因 realloc
  失效的跨线程 Rust 引用。跨 worker 只传 `SessionHandle`/`u32` opaque。

## 7. inline 选择

- `#[inline(always)]`：当前 worker 的 pool index lookup、role/phase 判定、FIFO 可读/可写容量
  检查、`DataIoResult` 分支和无分配的 event program helper；对应 VPP `always_inline` 的
  `vp_server_rx`（`vperf_server.c:339-364`）和 stream RX helper（`vperf_protos.c:59-127`）。
- `#[inline]`：只读 handle/accessor、`ApplicationMain`/`Iperf3Worker` 的边界 lookup；允许
  编译器在跨 crate 热路径内联但不强制代码膨胀。
- 不标注 attach/listen/detach、barrier、parser 状态迁移和错误构造；这些是低频
  control path，保持可诊断和 failure-atomic。
- 不使用 Rust `#[cold]` 代替 VPP `PREDICT_FALSE`；分支概率由 enum 结构和节点调用方表达。

## 8. 错误语义

错误 owner 是发生失败的 crate。VPP 的 `SESSION_E_*` 统一在 service `SessionError`；plugin-session
不另造 `IpSessionError`，IP endpoint/table 操作直接返回该 service error。TCP transport
错误在 TCP plugin，iperf3 framing/config 错误在 `Iperf3Error`。跨 crate 只在 attach/listen/
detach seam 转换一次并保留 source；不暴露 `i32` retval 或数字错误码。

```rust
// Service-owned failures are the existing SessionError family.
// VPP: session.h/session_api.h session error categories.
#[derive(Debug)]
pub enum Iperf3Error {
    Configuration { source: Iperf3ConfigError },
    ApplicationAttach { source: ApplicationError },
    ApplicationDetach { source: ApplicationError },
    TransportProtocol { source: RuntimeError },
    ListenerRegister { source: ApplicationError },
    ControlListen { source: SessionError },
    DataListen { source: SessionError },
    ControlUnlisten { source: SessionError },
    WorkerMissing { worker: usize },
    ListenerContext { context: u64 },
    SessionContext { context: u64 },
    SessionMissing { session_id: u32 },
    Protocol { source: Iperf3ProtocolError },
    ApplicationFifo { source: AppSessionError },
}
```

`ApplicationAttach`/`ApplicationDetach` 保留 service `ApplicationError` source；listener 错误区分
control/data operation，避免把失败归因成泛化的 `NoApplication`。FIFO hot path 的满/扩容结果
不把可重试 backpressure 当作控制面错误。不可恢复的 pool generation
或 ownership 破坏是 owner 内部断言，不构造 `Internal(String)`。

## 9. 同步、原子和 volatile

### 9.1 WorkerBarrier

- start/stop、application/listener table publication、worker `Vec`/Pool 的 realloc 和撤销均在
  现有 `WorkerBarrier` 内完成；barrier acknowledgement 是 worker 观察新表的同步边界。
- 不用 `AtomicPtr`、publication handle、第二个 completion counter 或 `publish()` API 替代
  barrier。VPP 对照：`vperf_cli.c:103-105`、`session.c:2307-2309`。

### 9.2 SpinLock/RwLock

- iperf3 session/FIFO 热路径不加锁；它们由当前 worker 独占。
- segment pool 的并发新增/删除继续使用 service `SegmentManager` 的现有 `RwLock`，对应
  VPP `segments_rwlock`（`segment_manager.h:52-80`、`segment_manager.c:147-234`）。
- 若 future control path 需要跨 worker pending connect，复用 service 已有 bounded lock/list
  contract；不在 iperf3 增加 `SpinLock<()>`、全局 session lock 或 callback 内持锁。

### 9.3 Atomic

- `stop_requested.store(true, Release)`；worker 用 `load(Acquire)` 观察停止请求。该原子只发布
  生命周期事实，不承载 session state、FIFO indices 或业务 parser。
- `accepted_connections` 是 control-plane 观察计数，使用 Relaxed fetch-add/load；丢失瞬时
  顺序不会影响 cleanup 或 protocol correctness。
- bytes/session counters 由 worker 独占 `u64`；不因统计方便改成共享原子，也不引入 stats
  subsystem。控制面聚合须在 barrier 后直接读取 worker Vec。

### 9.4 volatile

VPP 的 `volatile session_state` 是 C 编译器可见性和跨线程写入约束的一部分
（`session_types.h:262-264`）。Rust 设计不把普通 session state 标为 `volatile`：state 由
Session worker 独占修改，跨线程变化通过 SessionEvent/MQ 和 barrier 发布。`read_volatile`/
`write_volatile` 只允许在明确的 MMIO/FFI 协议；iperf3 app、FIFO、TCP state 均不得使用。

## 10. 初始化与失败原子性

`Iperf3Main::start` 必须先验证所有配置和 namespace，再执行第一次可观察 mutation。每一步
成功后的 cleanup action 是固定的：

```text
worker Vec/pools prepared
  -> Application attach
  -> control listener
  -> data listener
  -> store both listener handles
```

失败时：

- application attach 失败：不创建 listener；
- control listener 失败：detach application；
- data listener 失败：unlisten control，再 detach application；
- 每一次 listen 失败时，plugin-session 清理该次已创建的 listener、Session、transport 和 lookup，
  并将清理失败与原始 listen 错误一起返回；iperf3 不直接调用 `stop_listen` 或 service 的
  `remove_listener`；
- stop cleanup 的次要错误不能覆盖第一个 primary error，所有 listener/session 仍需尝试释放；
- 任何 listener/session handle 只有在 owner 成功返回后才写入 `Iperf3Main`，不能先写 sentinel
  再依赖调用方猜测是否有效。

## 11. iperf3 协议边界

- control listener 负责 iperf3 control exchange、参数协商、测试开始/结束和结果控制；
- data listener 负责一个或多个 TCP data session；data payload 不解析为 TCP segment，TCP plugin
  只提供 byte stream；
- iperf3 的端口协商、stream 数量、反向/双向模式和结束条件由 plugin-owned control state
  决定；不得把这些字段塞入 `SessionMain`、`TcpConnection` 或 `IpSessionMain`；
- 本 ADR 不承诺与所有 upstream iperf3 版本互操作。实现前必须选定 control framing 版本，
  并将 unsupported state 返回为 `Iperf3ProtocolError::StateUnsupported`。
- parser 只解析 iperf3 control cookie/state 的固定字节布局；固定布局使用 `zerocopy`
  的 `FromBytes`/`IntoBytes` 在 FIFO peek 的小型栈缓冲上解释，禁止 `Vec`、字符串复制或
  中间 payload。FIFO 数据本身仍由 service SVM FIFO 拥有，parser 不取得 TCP connection、
  不保存 FIFO 指针，也不改变 producer/consumer 所有权。
- control state 未达到完整固定长度时返回“尚未就绪”，保留 FIFO 可见位置不变；非法 state
  才返回 `Iperf3ProtocolError` 并由 callback 关闭 Session。

## 12. 明确弃用和不引入

- 不引入 transport VFT、`dyn`、`SessionRuntimeEngine`、`SessionScheduler`、mailbox、
  multi-ring queue、dispatch queue 或新的 app event queue；复用 service 的 callback、
  SVM MQ、FIFO 和 session queue nodes。
- 不把 builtin app 放进 `hammer-app`；`hammer-app` 仍是外部 SDK。
- 不把 IP endpoint、FIB、namespace object 或 TCP connection 指针下沉到 service。
- 不恢复旧 FIFO；只使用仓库现有 SVM FIFO/segment manager。
- 不增加 `ApplicationState`、`TransportConnection<()>`、NameRef、非拥有索引包装类型或
  `Arc<str>`；Application name/segment/FIFO 的 ownership 遵循 ADR-0040。
- 不在 `main_loop_enter` 注册 iperf3 node；session nodes 的 init/enable 由 service 既有路径
  管理，iperf3 只注册 application callback owner。
- VPP 的 `session_cb_vft_t` 作为分槽和返回语义来源；Hammer 把各 Rust `fn` 字段直接
  放在 Application，不新增 callback-table 结构体或 transport VFT。VPP 的 numeric
  `SESSION_E_*`/vperf error counter 只作为语义来源，Hammer 对外使用 enum。

## 13. 验证计划（本 ADR 不执行）

实现后必须验证：

1. `enable = false` 时 config/init 返回成功且 application/listener pool 无变化；Session Queue
   保持默认 `Disabled`；
2. control listen 成功、data listen 失败时 control listener/application/FIFO 全部回收；
3. accepted event 将 opaque 解析为正确 worker-local pool record，跨 worker handle 不会借用失效
   引用；
4. control frame 分片、FIFO 满、FIFO grow 失败、stale RX event 都只产生 typed result，不破坏
   FIFO producer/consumer positions；
5. reset/disconnect/cleanup 任意顺序下 record 只释放一次，TCP connection 由 TCP/service owner
   回收；
6. barrier 后 worker 观察到完整的 application/listener handle publication，stop 后不再接受新 session；
7. `cargo fmt --all -- --check`、`cargo test --workspace` 和专门的 session/plugin integration
   test 在实现阶段执行；本 ADR 阶段不运行构建或测试。

## 14. VPP 源码索引

- `third_party/vpp/src/plugins/vperf/builtin/vperf_server.c`
- `third_party/vpp/src/plugins/vperf/builtin/vperf_server.h`
- `third_party/vpp/src/plugins/vperf/builtin/vperf_builtin.h`
- `third_party/vpp/src/plugins/vperf/builtin/vperf_protos.c`
- `third_party/vpp/src/plugins/vperf/builtin/vperf_error.def`
- `third_party/vpp/src/plugins/vperf/builtin/vperf_cli.c`
- `third_party/vpp/src/plugins/vperf/vperf_test.h`
- `third_party/vpp/src/vnet/session/application.h`
- `third_party/vpp/src/vnet/session/application_interface.h`
- `third_party/vpp/src/vnet/session/application.c`
- `third_party/vpp/src/vnet/session/application_worker.c`
- `third_party/vpp/src/vnet/session/session.h`
- `third_party/vpp/src/vnet/session/session_types.h`
- `third_party/vpp/src/vnet/session/session.c`
- `third_party/vpp/src/vnet/session/session_input.c`
- `third_party/vpp/src/vnet/session/segment_manager.h`
- `third_party/vpp/src/vnet/session/segment_manager.c`
- `third_party/vpp/src/vnet/tcp/tcp.c`
- `third_party/vpp/src/vnet/tcp/tcp_input.c`
- `third_party/vpp/src/vnet/tcp/tcp_output.c`
- `third_party/vpp/src/vnet/tcp/tcp_timer.c`
