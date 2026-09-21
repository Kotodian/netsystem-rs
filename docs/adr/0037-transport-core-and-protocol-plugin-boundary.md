# ADR-0037: Transport Core 与协议插件边界

- 日期：2026-09-21
- 状态：Proposed
- Hammer 基线：`22d155a81eceb3d0711f5e869cf524716827d12a`
- VPP 基线：`third_party/vpp`，提交
  `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- VPP 范围：`vnet/session/transport.c`、`transport.h`、
  `transport_types.h`
- 前置决定：ADR-0035、ADR-0036

本文决定 `hammer-service::transport`、`hammer-plugin-session` 与具体 transport plugin
之间的边界。本文不设计 Session 如何按 protocol id 选择 transport，不设计 Application、
FIFO、MQ、namespace socket、SCM_RIGHTS 或 async socket 生命周期，也不改代码。

Endpoint 进入 transport 的方式属于本 ADR 范围。ADR-0035 已经定义的
`SessionEndpoint<T>`、`IpTransportEndpoint`、`IpTransportEndpointConfig`、
`IpSessionEndpoint` 和 `IpTransportConnectionId` 全部直接复用，不再定义第二套 endpoint。
本文同时修订 ADR-0035 第 7 节的 private key implementation shape：key layout 与 lookup 顺序
不变，但删除其中列出的 family-specific free-function builders，改用本 ADR 第 5.3 节的
`From`/`Into` conversion 和 table-owned lookup operation。

## 1. 决定

`hammer-service::transport` 提供三类协议无关能力：

1. 以现有 `hammer_service::session::SessionEndpoint<T>` 为参数的静态 `Transport<T>` trait；
2. options、flags、send params、pacer 和 common connection；
3. 完整参数化的 `TransportMain<E, K, A>`，抽象 VPP `transport_main_t` 的 local endpoint、
   source-port allocator、cleanup 和 ALPN state。

`hammer-service::transport` 不拥有 IP address、IP family、FIB、interface、IP key、TCP/UDP
connection pool 或具体 endpoint instance。service 定义 `TransportMain<E, K, A>`，但不发布
它的 process-global instance。

`hammer-plugin-session` 继续拥有 ADR-0035 的 IP concrete instance，并增加：

- `SessionEndpoint<IpTransportEndpointConfig>` 的 transport lifecycle instance；
- `TransportMain<IpTransportEndpoint, IpLocalEndpointKey, AlpnProtocolTable>` 的 concrete
  process-global owner `IpTransportMain`；
- IP route/interface address/source-port allocation policy；
- 与 VPP 相同的 local-endpoint 配置和精确 error 语义。

`TcpMain` 与 `UdpMain` 仍是各自协议唯一的 process-global owner。它们直接实现
`Transport<IpTransportEndpointConfig>`，因此 trait 方法中的
`SessionEndpoint<IpTransportEndpointConfig>` 就是已有 `IpSessionEndpoint`。TCP/UDP 直接依赖
plugin/session 发布的 `IpTransportMain` instance；不增加 `TcpTransport`、`UdpTransport`
marker、owner wrapper 或 forwarding Main。

目标设计不再使用 `TransportVft`、function-pointer table、`dyn Transport` 或 erased
registry。`Transport<T>` 是静态分派 contract。Session 的 numeric protocol dispatch 是后续
Session adapter 设计；该 adapter 不得把 trait 再擦除成 VFT 或 trait object。

依赖方向为：

```text
hammer-service::session::SessionEndpoint<T>
                    ^
                    |
hammer-service::transport::Transport<T>
                    ^
                    |
        +-----------+-----------+
        |                       |
hammer-plugin-tcp       hammer-plugin-udp
        |                       |
        +-----------+-----------+
                    |
          hammer-plugin-session
          IpSessionEndpoint + IpTransportMain
          concrete instances
```

## 2. 当前边界为什么错误

当前代码同时存在三条不一致的 endpoint 路径：

- `hammer-plugin-session` 已定义 `IpSessionEndpoint`，但 TCP/UDP 的 listen/connect 不接收它；
- `hammer-runtime` 另有 `SessionListenEndpoint` 与 `SessionConnectEndpoint`，其中 transport
  address 仍是 `SocketAddr`；
- `hammer-service::transport::TransportMain` 又私有定义
  `SocketAddr + fib_index` 的 `TransportEndpoint`。

因此 `IpTransportEndpointConfig` 目前只参与 Session lookup，实际 transport lifecycle 仍从
parallel runtime endpoint 取地址，UDP 构造 lookup identity 时甚至固定使用 FIB 0。现有
endpoint 不是 transport contract，只是一个没有接入 transport 的 concrete type。

目标状态只保留一条路径：

```text
SessionEndpoint<T>
    -> plugin/session 的 IpSessionEndpoint concrete instance
    -> TcpMain / UdpMain 的 Transport<IpTransportEndpointConfig> implementation
    -> IpTransportConnectionId
    -> concrete listener / half-open / established connection
```

`SocketAddr` 仍可作为 TCP/UDP packet header、checksum、format 或内部算法的临时值，但不能再
作为 listen/connect 的 competing control-plane endpoint contract。需要转换时使用
`From`/`Into`；不增加 `from_*`、`into_transport` 或其他平行 conversion 方法。

## 3. Service 的静态 Transport trait

### 3.1 Trait

trait 直接使用已有 `SessionEndpoint<T>`。`T` 是 concrete transport endpoint config，不增加
`Endpoint` associated type，也不要求 endpoint 实现新 trait。

```rust
use hammer_service::session::SessionEndpoint;

pub trait Transport<T> {
    type Error;

    const OPTIONS: Options;

    fn start_listen(
        &self,
        endpoint: SessionEndpoint<T>,
    ) -> Result<u32, Self::Error>;

    fn stop_listen(
        &self,
        connection_index: u32,
    ) -> Result<(), Self::Error>;

    fn connect(
        &self,
        endpoint: SessionEndpoint<T>,
    ) -> Result<u32, Self::Error>;

    fn half_close(&self, _: u32, _: u32) {}

    fn close(&self, connection_index: u32, thread_index: u32);

    fn reset(&self, connection_index: u32, thread_index: u32) {
        self.close(connection_index, thread_index);
    }

    fn cleanup(&self, connection_index: u32, thread_index: u32);

    fn cleanup_half_open(&self, _: u32) {}

    fn send_params(
        &self,
        connection_index: u32,
        thread_index: u32,
    ) -> Result<SendParams, Self::Error>;

    fn update_time(&self, _: f64, _: u32) {}

    fn enable(&self, _: bool) -> Result<(), Self::Error> {
        Ok(())
    }
}
```

这组 default 与 VPP 一致：

- `half_close` 和 `cleanup_half_open` 未实现时是 no-op；
- `reset` 未覆写时回退到 `close`；
- `update_time` 与 `enable` 未实现时是 no-op/success；
- required `start_listen`、`stop_listen`、`connect`、`close`、`cleanup` 和 `send_params` 没有
  “unsupported” default。

trait 不要求 `Send`、`Sync`、`'static`、`Clone` 或 `Copy`。这些约束属于 concrete owner。
`TcpMain`/`UdpMain` 的真实 global accessor 返回 `&'static`，因为对应 `OnceLock` 的确存活到
进程结束；普通 endpoint、connection 和 worker borrow 使用输入产生的普通 lifetime。

`connect_stream`、Session index attach、Session notification、FIFO header push/custom TX 和
application RX event 都需要先决定 Session adapter 所有权，本 ADR 不把它们塞进 trait。具体
TCP/UDP output Graph Node 继续拥有 header prepend。connection/listener/half-open retrieval
继续由 `TcpMain`/`UdpMain` 的 owner-local API 提供，不通过 trait 返回跨越 `RefCell` guard 的
借用。

### 3.2 Concrete implementation

不创建新 concrete transport object。实现直接落在现有 Main：

```rust
use hammer_plugin_session::IpTransportEndpointConfig;
use hammer_service::transport::Transport;

impl Transport<IpTransportEndpointConfig> for TcpMain {
    type Error = TcpError;

    const OPTIONS: Options = Options::new(
        "tcp",
        "T",
        TxMode::Peek,
        Service::VirtualCircuit,
    );

    // methods
}

impl Transport<IpTransportEndpointConfig> for UdpMain {
    type Error = UdpError;

    const OPTIONS: Options = Options::new(
        "udp",
        "U",
        TxMode::Datagram,
        Service::Connectionless,
    );

    // methods
}
```

`TcpMain`/`UdpMain` 的 worker slots、listener state、connection pools、timer、lookup 和 Graph
Nodes 不移动到 service，也不移动到 plugin/session。trait implementation 只在 owner 内执行
具名操作。

## 4. Service 的 common 值

模块路径已经提供 transport 语义，因此类型名不重复 `Transport` 前缀。

### 4.1 Options

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxMode {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    VirtualCircuit,
    Connectionless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    name: &'static str,
    short_name: &'static str,
    tx_mode: TxMode,
    service: Service,
}

impl Options {
    #[inline(always)]
    pub const fn new(
        name: &'static str,
        short_name: &'static str,
        tx_mode: TxMode,
        service: Service,
    ) -> Self;

    #[inline(always)]
    pub const fn name(&self) -> &'static str;

    #[inline(always)]
    pub const fn short_name(&self) -> &'static str;

    #[inline(always)]
    pub const fn tx_mode(&self) -> TxMode;

    #[inline(always)]
    pub const fn service(&self) -> Service;
}
```

这里的 `'static` 是正确的：名字来自 transport implementation 的 associated constant，不是
普通 input borrow。

### 4.2 Flags 与 send params

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ConnectionFlags: u8 {
        const TX_PACED = 1 << 0;
        const NO_LOOKUP = 1 << 1;
        const DESCHEDULED = 1 << 2;
        const CONNECTIONLESS = 1 << 3;
        const ERROR = 1 << 4;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct SendFlags: u8 {
        const DESCHEDULED = 1 << 0;
        const POSTPONE = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendParams {
    Packetized {
        send_space: u32,
        tx_offset: u32,
        mss: u16,
        flags: SendFlags,
    },
    Internal {
        max_burst_size: u32,
        bytes_dequeued: u32,
        flags: SendFlags,
    },
}

impl SendParams {
    #[inline(always)]
    pub const fn flags(&self) -> SendFlags;
}
```

flag bit 与 VPP 一致。`SendParams` 用一个 enum 表达 VPP union 的两种有效状态，不再增加
`SendLimits` wrapper，也不保留当前 `usize` / `send_goal_size`。

### 4.3 Pacer

```rust
#[derive(Debug)]
pub struct Pacer {
    bytes_per_second: u64,
    bucket: i64,
    last_update_micros: u64,
    tokens_per_microsecond: f32,
    min_burst: u32,
    max_burst: u32,
}

impl Pacer {
    pub const MIN_MSS: u32 = 1460;
    pub const MIN_BURST: u32 = Self::MIN_MSS;
    pub const MAX_BURST_PACKETS: u32 = 43;
    pub const MAX_BURST: u32 = Self::MAX_BURST_PACKETS * Self::MIN_MSS;
    pub const BURSTS_PER_RTT: u64 = 20;

    pub const fn new() -> Self;

    pub fn init(
        &mut self,
        now_micros: u64,
        bytes_per_second: u64,
        initial_bucket: u32,
        min_burst: u32,
        seconds_per_loop: f64,
    );

    pub fn reset(
        &mut self,
        now_micros: u64,
        bytes_per_second: u64,
        initial_bucket: u32,
        rtt_micros: u64,
        seconds_per_loop: f64,
    );

    pub fn update_rate(
        &mut self,
        bytes_per_second: u64,
        rtt_micros: u64,
        seconds_per_loop: f64,
    );

    #[inline]
    pub fn burst(&mut self, now_micros: u64) -> u32;

    #[inline(always)]
    pub const fn rate(&self) -> u64;

    #[inline(always)]
    pub fn reset_bucket(&mut self, now_micros: u64, bucket: u32);

    #[inline(always)]
    pub fn update_bytes(&mut self, bytes: u32);
}
```

算法保持 VPP `spacer_t` 语义：非零 rate 是 caller invariant；RTT/20 与 loop period 取最大后
clamp 到 1..=1000 微秒；burst clamp 到 `min_burst..=43*1460`；token 增量大于 10 才更新；
negative bucket 返回 0；发送字节直接从 signed bucket 扣除。Pacer 不拥有 clock、Session
scheduler 或 congestion controller。

### 4.4 Common connection

```rust
use hammer_infra::align::CacheLineAlignMark;

#[repr(C)]
#[derive(Debug)]
pub struct Connection<I> {
    identity: I,
    connection_index: u32,
    thread_index: u32,
    flags: ConnectionFlags,
    pacer: Pacer,
    cacheline_end: CacheLineAlignMark,
}

impl<I> Connection<I> {
    pub const fn new(identity: I, thread_index: u32) -> Self;

    pub fn set_index(&mut self, connection_index: u32);

    #[inline(always)]
    pub const fn identity(&self) -> &I;

    #[inline(always)]
    pub fn identity_mut(&mut self) -> &mut I;

    #[inline(always)]
    pub const fn index(&self) -> u32;

    #[inline(always)]
    pub const fn thread_index(&self) -> u32;

    #[inline(always)]
    pub const fn flags(&self) -> ConnectionFlags;

    #[inline(always)]
    pub fn insert_flags(&mut self, flags: ConnectionFlags);

    #[inline(always)]
    pub fn remove_flags(&mut self, flags: ConnectionFlags);

    #[inline(always)]
    pub const fn pacer(&self) -> &Pacer;

    #[inline(always)]
    pub fn pacer_mut(&mut self) -> &mut Pacer;

    #[inline(always)]
    pub fn update_tx_bytes(&mut self, bytes: u32);
}
```

`connection_index` 构造时为 `u32::MAX`，插入 concrete worker pool 后设置一次。重复设置、设置
invalid index 或超过两条 cache line 都是 owner bug，使用 assertion/compile-time assertion，
不是 recoverable error。

VPP common connection 的 `s_index` 是 Session association，本 ADR 明确不加入；后续 Session
adapter 必须决定它由 common connection 直接保存还是由 Session owner 关联，不能先用
`Option`、sentinel wrapper 或 side map 猜测。

TCP/UDP concrete connection 直接组合该值：

```rust
pub struct TcpConnection {
    connection: Connection<IpTransportConnectionId>,
    // TCP-owned state
}

pub struct UdpConnection {
    connection: Connection<IpTransportConnectionId>,
    // UDP-owned state
}
```

不实现 `Deref`/`DerefMut` 伪继承，也不把 concrete connection 暴露给 service。

## 5. Service TransportMain 与 IP concrete instance

### 5.1 Service 的完整抽象

VPP 的 `transport_main_t` 不是 local-endpoint helper。它是一个完整 transport authority：
local endpoint table/pool、delayed freelist、source-port allocator 状态、cleanup publication
以及 ALPN name index 必须属于同一个 Main。

service 参数化 endpoint、key 与 ALPN table，但保留完整 ownership：

```rust
use std::cell::UnsafeCell;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;

use hammer_infra::bihash::{Bihash, BihashKey};
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
use hammer_service::session::SessionEndpoint;

struct LocalEndpoint<E> {
    endpoint: SessionEndpoint<E>,
    references: AtomicU32,
}

struct EndpointCleanup {
    free: Vec<u32>,
    pending: bool,
}

struct PortAllocator {
    seed: u32,
    max_tries: u16,
    min_src_port: u16,
    max_src_port: u16,
}

pub struct TransportMain<E, K, A> {
    local_endpoints_table: Bihash<K, 4>,
    local_endpoints: UnsafeCell<Pool<LocalEndpoint<E>>>,
    endpoint_cleanup: SpinLock<EndpointCleanup>,
    port_allocator: UnsafeCell<PortAllocator>,
    alpn_protocol_by_name: A,
}

impl<E, K, A> TransportMain<E, K, A>
where
    E: Copy,
    K: BihashKey + Copy + Default,
    for<'a> K: From<&'a SessionEndpoint<E>>,
{
    pub fn init(
        storage: &'static OnceLock<Self>,
        config: Config,
        alpn_protocol_by_name: A,
    ) -> RuntimeResult<()>;

    pub fn global(
        storage: &'static OnceLock<Self>,
    ) -> RuntimeResult<&'static Self>;

    pub fn new(config: Config, alpn_protocol_by_name: A) -> Self;

    pub fn mark_used(
        &self,
        endpoint: SessionEndpoint<E>,
    ) -> bool;

    pub fn share(&self, endpoint: &SessionEndpoint<E>);

    pub fn release(&self, endpoint: &SessionEndpoint<E>) -> bool;

    pub fn reclaim(&self);

    pub fn next_source_port(&self) -> u16;

    pub fn record_port_allocation_tries(&self, tries: u16);

    #[inline(always)]
    pub fn max_port_allocation_tries(&self) -> u16;

    pub fn clear_port_allocation_stats(&self);

    #[inline(always)]
    pub const fn alpn_protocols(&self) -> &A;

    pub fn local_endpoints_in_use(&self) -> u32;

    pub fn cleanup_pending(&self) -> bool;
}
```

这里的 Rust field grouping 保留 VPP 的全部事实：

- `local_endpoints_table` 对应 24-byte-key endpoint table；
- `local_endpoints` 保存 endpoint、transport protocol 和 refcount；protocol 已由
  `SessionEndpoint<E>` 携带，不再另存一个重复字段；
- `EndpointCleanup::free` / `pending` 对应 `lcl_endpts_freelist` /
  `lcl_endpts_cleanup_pending`，并与同一 `SpinLock` 形成一个不变量；
- `PortAllocator` 对应 seed、max tries、min/max source port；
- `A` 是 concrete instance 选择的 ALPN name index，service 不规定 TLS/HTTP protocol
  identity。

`TransportMain<E, K, A>` 是 service 定义的通用状态、算法与 publication lifecycle，不是 service
发布的某个固定 global。`init(storage, ...)` / `global(storage)` 让 concrete owner 提供自己
拥有的 `OnceLock<TransportMain<...>>`；真正拥有这些字段和 static storage 的 concrete instance
只存在于 plugin/session。
通用定义不认识 IP/FIB，也不持有 plugin callback；它的所有 endpoint operation 都使用现有
`SessionEndpoint<E>`。

`mark_used == true` 表示从未占用并成功发布；已占用返回 `false`。`share` 查不到 endpoint
时 no-op。`release == true` 只表示 refcount 降为零、table entry 已删除并进入 delayed
freelist；不存在或仍有引用都返回 `false`。refcount overflow 和 zero entry 再减是 owner
bug。

`next_source_port` 只推进 Main 自己的 VPP-style random seed，并在配置的
`min_src_port..max_src_port` 中给出一个 candidate；IP six-tuple collision lookup 与
mark/share loop 由 concrete `IpTransportMain` 完成。`record_port_allocation_tries` 只保留
历史最大值。

### 5.2 Plugin/session 的 IP transport instance

`hammer-plugin-session` 保留 ADR-0035 的 endpoint，并给 service generic Main 提供 concrete
参数：

```rust
pub type IpSessionEndpoint =
    SessionEndpoint<IpTransportEndpointConfig>;

type IpLocalEndpoint =
    SessionEndpoint<IpTransportEndpoint>;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct IpLocalEndpointKey([u64; 3]);

struct AlpnProtocolTable {
    // Current VPP ALPN name-to-protocol instance.
}

pub struct IpTransportMain {
    main: TransportMain<
        IpTransportEndpoint,
        IpLocalEndpointKey,
        AlpnProtocolTable,
    >,
}

static TRANSPORT_MAIN: OnceLock<IpTransportMain> = OnceLock::new();

impl IpTransportMain {
    pub fn init(config: hammer_service::transport::Config) -> RuntimeResult<()>;

    pub fn global() -> RuntimeResult<&'static Self>;
}
```

`IpTransportMain` 不是只包 local endpoint table 的 facade。它是 plugin/session 中唯一的 IP
transport authority：完整拥有 service `TransportMain` instance，并在其上实现 IP route、
interface address、six-tuple collision 与 endpoint-config completion。private key 和 ALPN
representation 不泄漏给 TCP/UDP。

`IpTransportMain::init` 构造 service generic Main 和 concrete ALPN table，并发布到
`TRANSPORT_MAIN`；重复 init 是 init-function ordering bug，使用 assertion。`global` 在尚未发布时
返回现有 `RuntimeError::PluginStateNotInitialized { plugin: "session" }`，不增加 transport
initialization error。static storage 属于 plugin/session，但 lifecycle API 属于 concrete Main，
不再提供 free `transport_main()` accessor。

`IpLocalEndpoint` 只是已有 generic type 的 private alias，不是新 endpoint。key conversion
只针对 local endpoint instance：

```rust
impl<'a> From<&'a IpLocalEndpoint> for IpLocalEndpointKey {
    #[inline(always)]
    fn from(endpoint: &'a IpLocalEndpoint) -> Self;
}

impl From<IpSessionEndpoint> for IpTransportConnectionId {
    #[inline(always)]
    fn from(endpoint: IpSessionEndpoint) -> Self;
}
```

key 是 VPP 的 24-byte key：两个 address word，加
`fib_index << 32 | network_order_port << 8 | transport_protocol`。`sw_if_index`、remote endpoint、
MSS、DSCP、next node 和 transport flags 不进入 local-endpoint key。

connection identity 的 FIB、DSCP、protocol、local/peer address 和 network-order port 全部来自
同一个 `IpSessionEndpoint`，不再由 TCP/UDP 从 `SocketAddr` 重新拼装，更不能固定 FIB 0。

`IpTransportMain` 给依赖它的 TCP/UDP 暴露 concrete operation：

```rust
impl IpTransportMain {
    pub fn mark_used(
        &self,
        endpoint: &IpSessionEndpoint,
    ) -> Result<(), LocalEndpointError>;

    pub fn share(&self, endpoint: &IpSessionEndpoint);

    pub fn release(&self, endpoint: &IpSessionEndpoint) -> bool;

    pub fn allocate_local(
        &self,
        endpoint: IpSessionEndpoint,
    ) -> Result<IpSessionEndpoint, LocalEndpointError>;

    pub fn local_endpoints_in_use(&self) -> u32;

    pub fn max_port_allocation_tries(&self) -> u16;

    pub fn clear_port_allocation_stats(&self);
}
```

这些是 concrete owner 的 domain operations，不是 forwarding accessor 或 conversion helper。
它们把 `IpSessionEndpoint` 中的 local endpoint 投影成 private `IpLocalEndpoint` 后调用
service Main；值转换只使用 `From`/`Into`。

### 5.3 Plugin/session lookup conversion 收敛

现有 lookup implementation 把同一件事拆成 `family_index`、`ip_version`、
`connection_family`、两组 connection/listener/proxy key builder、两组 listener lookup 和
`ip4_word`/`ip6_words`。目标设计全部删除这些 free functions。

family conversion 使用标准 trait，不保留同义 helper：

```rust
impl From<IpSessionFamily> for usize {
    #[inline(always)]
    fn from(family: IpSessionFamily) -> Self;
}

impl From<IpSessionFamily> for IpVersion {
    #[inline(always)]
    fn from(family: IpSessionFamily) -> Self;
}

impl<'a> From<&'a IpTransportConnectionId> for IpSessionFamily {
    #[inline(always)]
    fn from(connection: &'a IpTransportConnectionId) -> Self;
}
```

lookup key 只保留一个 private owned value，不再保留 `Ip4SessionKey` / `Ip6SessionKey` alias，
也不允许 caller 选择错误的 family builder：

```rust
#[derive(Clone, Copy)]
enum Key {
    Ip4(u128),
    Ip6([u64; 6]),
}

impl<'a> From<&'a IpTransportConnectionId> for Key {
    #[inline(always)]
    fn from(connection: &'a IpTransportConnectionId) -> Self;
}

impl<'a> From<&'a IpSessionEndpoint> for Key {
    #[inline(always)]
    fn from(endpoint: &'a IpSessionEndpoint) -> Self;
}
```

第一个 conversion 生成 established/half-open exact key；第二个生成 listener exact key。
IP address 的 native-word encoding 直接写在这两个 `From` implementation 中，不再经过
`ip4_word`、`ip6_words` 或 `*_listener_words`。两个 conversion 都对输入 enum/`IpAddr` 做完备
match，因此不返回 `Result`、不使用 `TryFrom`，也没有 wrong-family panic。

listener fallback 属于 table lookup policy，收进 table owner 的一个方法：

```rust
impl IpSessionTable {
    #[inline]
    fn lookup_listener(
        &self,
        key: Key,
        use_wildcard: bool,
    ) -> Option<SessionHandle>;
}
```

该方法在各 family 分支内直接按 VPP 顺序把输入 key 归一成 listener exact key、可选清零 local
address 查 wildcard、再保留 local address 并清零 local port 查 proxy。它替代
`lookup_ip4_listener`、`lookup_ip6_listener`、四个 `*_listener_key*` 和四个
`*_proxy_key*`，不再把 exact/proxy 两个预计算参数来回传递。`IpSessionTable` 与 `Key` family
不一致只能由 plugin/session 自己的错误调用产生，使用 assertion；lookup miss 仍是
`Option::None`。

### 5.4 VPP 配置

service 定义 transport Main 自己消费的配置，字段和 default 语义与 VPP 一致：

```rust
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Config {
    pub local_endpoints_table_buckets: u32,
    pub local_endpoints_table_memory: u32,
    pub min_src_port: u16,
    pub max_src_port: u16,
}
```

- table bucket/memory 的 configured value 为 0 时，Main 初始化替换为 250,000 buckets 与
  512 MiB；
- source-port default 是 1024 inclusive 到 65535 exclusive，保持 VPP allocator 的范围语义；
- memory 使用 `u32`，反序列化边界排除 VPP 不接受的 `>= 2^32`；
- 不增加 minimum ratio、power-of-two、自动交换 min/max 或自定义 fallback；
- Bihash/Main Heap allocation failure沿用已有 process allocation boundary。

plugin/session 的 `IpSessionConfig` flatten 这个 service config，并继续拥有 ADR-0035 的 IP4/IP6
Session table 配置：

```rust
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IpSessionConfig {
    pub v4_session_table_buckets: u32,
    pub v4_session_table_memory: u32,
    pub v4_halfopen_table_buckets: u32,
    pub v4_halfopen_table_memory: u32,
    pub v6_session_table_buckets: u32,
    pub v6_session_table_memory: u32,
    pub v6_halfopen_table_buckets: u32,
    pub v6_halfopen_table_memory: u32,
    #[serde(flatten)]
    pub transport: hammer_service::transport::Config,
}
```

现有 `IpSessionTableConfig` 在迁移时扩展并改名为 `IpSessionConfig`；不并存第二份 concrete
transport config，也不让 service 持有 plugin/session config。

## 6. Endpoint 必须如何进入 TCP/UDP

### 6.1 Listener

TCP/UDP 的 `Transport::start_listen` 直接取得 `IpSessionEndpoint`：

1. 检查 `endpoint.transport_protocol()` 与 concrete Main 的 protocol 一致；不一致是 adapter
   bug，使用 assertion；
2. 使用 `endpoint.transport()` 中已有 IP/FIB/interface/port/MSS/DSCP/flags，不从
   `SessionListenEndpoint` 或 `SocketAddr` 补字段；
3. concrete plugin 按 VPP wildcard/interface-address 规则解析实际 bind endpoint；
4. TCP 对非 wildcard endpoint 调用 `IpTransportMain::global()?.mark_used`，已存在时按 VPP 调用
   `IpTransportMain::global()?.share`；UDP 保留自身 external port registration，并使用同一个
   `IpSessionEndpoint` 构造 listener；
5. listener 保存由该 endpoint `Into<IpTransportConnectionId>` 得到的 identity；
6. 后续失败必须释放本次取得的 local-endpoint reference，然后再返回 primary error。

`TcpMain::bind_tcp_listener(SocketAddr, ...)`、`UdpListener::new(SocketAddr, ...)` 是当前旁路，
目标签名必须接收 `IpSessionEndpoint` 或从它派生的 `Connection<IpTransportConnectionId>`。

### 6.2 Active open

TCP/UDP 的 `Transport::connect` 同样直接取得 `IpSessionEndpoint`：

1. plugin/session 的 `IpTransportMain::global()?.allocate_local` 根据 endpoint 中已经选择的 FIB、interface、
   requested local address/port 和 remote endpoint 补全 local endpoint；
2. 未指定 local address 时按 remote route 找 resolving interface，再取该 family 的 interface
   address；
3. 未指定 local port 时按 VPP source-port range 随机尝试；local address/port 已占用时，只在
   `session_lookup().lookup_exact` 未命中同一 six-tuple 时 share；这里直接调用同一
   plugin/session owner，不增加 callback/capability；
4. 返回补全后的同一个 `IpSessionEndpoint`，TCP/UDP 使用 `Into` 构造
   `IpTransportConnectionId`；
5. half-open/connection 插入 concrete worker pool 后，把 pool index 写入 common
   `Connection`；
6. open、publish 或首包发送失败时释放 local endpoint；正常 close/cleanup 也释放一次。

现有 TCP 要求 `SessionConnectEndpoint.local` 为 `Some`、UDP 使用 raw local/remote
`SocketAddr`、两者把 FIB 固定为 0 的路径全部是迁移对象。目标 transport connection 的唯一
identity 来源是 `IpSessionEndpoint`。

### 6.3 Session metadata 不得进入 transport endpoint

当前 `SessionConnectEndpoint` 同时包含 address 与 application、connection、parent handle、
flags、opaque、server name。后续 Session adapter 必须把这些 Session/Application facts 留在
自己的 request state，只把 `IpSessionEndpoint` 交给 `Transport<T>`。

本 ADR 不决定这些 metadata 的新 owner，也不在 `IpTransportEndpointConfig` 中增加字段。

## 7. Error 语义

service 的 `Transport<T>` 只有 associated `Error`，不定义 catch-all `TransportError`，也不把
所有 plugin failure 转成 `RuntimeError`。TCP、UDP 和 plugin/session 分别保留 owner-local
error。

IP local endpoint operation 的 error 位于 `hammer-plugin-session`，并使用现有 macro：

```rust
#[hammer_component_macros::runtime_error(subsystem = "transport")]
#[derive(Debug, thiserror::Error)]
pub enum LocalEndpointError {
    #[error("FIB {fib_index} has no route to {remote}")]
    NoRoute {
        fib_index: u32,
        remote: std::net::IpAddr,
    },
    #[error("FIB {fib_index} route to {remote} has no resolving interface")]
    NoResolvingInterface {
        fib_index: u32,
        remote: std::net::IpAddr,
    },
    #[error("interface {sw_if_index} has no local address for {remote}")]
    NoLocalAddress {
        sw_if_index: u32,
        remote: std::net::IpAddr,
    },
    #[error("transport {protocol} has no available local port")]
    NoLocalPort {
        protocol: u8,
    },
    #[error("local endpoint {endpoint:?} is already in use")]
    LocalPortInUse {
        endpoint: IpSessionEndpoint,
    },
}
```

这些 variants 分别对应 VPP `SESSION_E_NOROUTE`、`SESSION_E_NOINTF`、`SESSION_E_NOIP`、
`SESSION_E_NOPORT` 和 `SESSION_E_PORTINUSE`。Rust enum 不声明 numeric code。TCP/UDP 使用
`From<LocalEndpointError>` 转成自己的精确 error；未来 Session request/reply seam 再转换一次
为该 request/reply 的 retval。

以下情况不定义 recoverable error：

- duplicate initialization、错误线程 mutation、协议与 endpoint 不匹配、重复设置 connection
  index、refcount overflow：owner bug，assertion；
- `From` family/key conversion 是 total conversion；table/key family 不匹配是 owner bug，
  assertion，不返回 `false` 或自定义 error；
- `share` miss：no-op；
- `release` miss 或仍有引用：`false`；
- `half_close`/`cleanup_half_open` 缺失：no-op；
- `reset` 缺失：调用 `close`；
- packet parse、checksum、ICMP 或 connection-state rejection：owning Graph Node 的 typed
  error counter 与 next arc；
- allocation failure：已有 Main Heap/process failure boundary。

`SESSION_E_TRANSPORT_NO_REG` 属于后续 Session numeric protocol dispatch：找不到 concrete
`Transport<T>` implementation 时由该 request owner 返回。它不成为宽泛的 service
`OperationUnsupported`。

Session lookup/table 的返回值继续严格使用 ADR-0035 的 VPP 语义：add 在 table 存在时是
add-or-overwrite；显式 table index 不存在时返回 `false`；delete 成功为 `true`、table/key miss
为 `false`；ordinary lookup miss 为 `Option::None` 或 `SessionLookupResult::NotFound`；
wrong-thread 只返回 `SessionLookupResult::WrongThread`。本次 conversion 收敛不增加
`SessionLookupError`，也不改变这些返回值。

## 8. Inline 规则

VPP header 中的 `static inline` / `always_inline` 与 `.c` 中的 lifecycle operation 分开处理：

| Rust 标记 | 使用位置 |
|---|---|
| `#[inline(always)]` | 跨 crate 的纯 getter、flag test/set、pacer rate/bucket trivial operation、IP key hash、family/key/identity 的纯 `From`/`Into` conversion |
| `#[inline]` | pacer burst token update、`IpSessionTable::lookup_listener`、短小 owner lookup |
| 无 inline | `Transport<T>` lifecycle implementation、Pool/Bihash mutation、refcount loop、cleanup、route/interface lookup、port allocation、enable、format、error construction |

trait declaration本身不加 inline；标记只放在 concrete/default implementation。`const fn` 不
自动加 `inline(always)`。`mark_used`、`share`、`release` 虽然短，也涉及 shared mutation，
不强制 inline。

## 9. Main、lifetime 与同步

- `TcpMain`/`UdpMain` 各自拥有 protocol global state 和固定 worker slots；它们是 transport
  plugin 的真实 Main；
- service 定义并实现 generic `TransportMain<E, K, A>`，但不声明 static、不提供 global
  accessor，也不选择任何 concrete endpoint、key 或 ALPN table；
- plugin/session 的 `TRANSPORT_MAIN: OnceLock<IpTransportMain>` 是唯一 IP transport instance；
  `IpTransportMain` 完整拥有
  `TransportMain<IpTransportEndpoint, IpLocalEndpointKey, AlpnProtocolTable>`，不是只转发方法的
  facade；
- plugin/session 的 `SESSION_LOOKUP`、`NAMESPACES` 与 `TRANSPORT_MAIN` 是三个并列的真实
  authority，分别拥有 IP Session lookup、application namespace binding 和 IP transport
  state；不增加囊括它们的 umbrella Main；
- TCP/UDP 直接通过 plugin/session 的 `IpTransportMain::global()` 使用同一个 instance，不得
  各自复制 local endpoint table、endpoint pool、port allocator、cleanup list 或 ALPN table；
- `OPTIONS` 中的字符串和 concrete Main accessor 使用真实 `'static`；普通 borrow 不扩大
  lifetime；
- common connection 与 pacer 是 worker-owned mutable state，只通过 `&mut` 推进；
- local endpoint references 是 worker 间共享 scalar，使用 atomic；cleanup list 使用短临界区
  `SpinLock`；Pool growth/reclaim 只在 Main Thread + WorkerBarrier 下发生；
- 不增加 `Arc<Connection<_>>`、`thread_local!`、closure-mediated state access、generic
  per-thread container 或第二套 publication protocol。

## 10. `transport.c` 其余能力的归属

- protocol registration table：目标中删除，由 `Transport<T>` implementation 替代；
- `transport_update_time` / enable-all iteration：后续 composition owner 对 concrete
  implementation 做静态调用，不由 service 保存 erased registrations；
- format：TCP/UDP owner 的 `Display`/format operation；
- connection/listener/half-open lookup：TCP/UDP Main 与 worker pool；
- ICMP destination unreachable：IP Graph Node 与 concrete transport，使用 node counter；
- ALPN name index：保留为 service `TransportMain<E, K, A>` 的 `A` 字段；plugin/session 的唯一
  `IpTransportMain` instance 提供 concrete `AlpnProtocolTable`。service 只保存并暴露 generic
  index，不解释 ALPN identity；TLS/HTTP protocol behavior、verify 和 config 仍属于对应插件；
- `TRANSPORT_MAX_HDRS_LEN`：VPP buffer/session TX implementation constraint，不变成 Hammer
  transport-specific Buffer allocation/headroom policy；
- socket：本阶段不引入。后续单独设计 socket owner、fd lifecycle 与 namespace socket，不能
  反向污染 `SessionEndpoint<T>` 或 `Transport<T>`。

## 11. 迁移顺序

后续实施按以下顺序进行：

1. 在 `hammer-service::transport` 增加 `Transport<T>`、简化命名的 common values、pacer、
   `Connection<I>`、`Config` 与 generic `TransportMain<E, K, A>`；service 不新增 global Main；
2. 把 `SendFlags`/`SendParams` 从 `session/runtime.rs` 移回 transport；
3. plugin/session 把现有 `IpSessionTableConfig` 收敛为 `IpSessionConfig`，定义
   `IpTransportMain`，由 `IpTransportMain::init` 初始化唯一 `TRANSPORT_MAIN`，并由
   `IpTransportMain::global` 返回实例；
4. plugin/session 为该 instance 提供 private IP key、concrete ALPN table、IP route/interface、
   six-tuple collision 和 endpoint allocation operations；
5. plugin/session 删除 `family_index`、`ip_version`、`connection_family`、全部
   `ip4_*_key`/`ip6_*_key`、`ip4_word`/`ip6_words` 与两组 listener lookup free functions；用
   `From`、private `Key` 和 `IpSessionTable::lookup_listener` 替代；
6. TCP/UDP 显式依赖 plugin/session，`TcpMain`、`UdpMain` 实现
   `Transport<IpTransportEndpointConfig>`，先改 listen/connect 入口真正消费
   `IpSessionEndpoint`，并使用 plugin/session 的 `IpTransportMain::global()`；
7. TCP/UDP listener、half-open、connection 使用
   `Connection<IpTransportConnectionId>`，删除 raw `SocketAddr` control-plane identity 与 FIB 0
   fallback；
8. 接入同一 `IpTransportMain` 的 local endpoint mark/share/release 和失败回滚，再接入
   pacer/send params；
9. 后续 Session adapter 完成 static protocol composition 后，删除
   `SessionListenEndpoint`/`SessionConnectEndpoint` 中的 parallel transport address、
   `TransportVft`、`register_transport` 与 `transport_vft`；
10. 删除 service 旧的 concrete IP endpoint/key、旧 concrete `TransportMain` global
   `TRANSPORT_MAIN`、placeholder port defaults 和 catch-all `TransportError`；保留新的 generic
   `TransportMain<E, K, A>` 及其 ALPN 字段。

第 9 步需要后续 Session adapter ADR 先决定 numeric protocol selection；在该决定完成前，不能
把现有 VFT 扩展成 target API，也不能用 `dyn Transport` 临时代替。

## 12. 待批准类型与 API

本 ADR 提议的新增或替换 surface：

- service：`Transport<T>`；
- service：`TxMode`、`Service`、`Options`、`ConnectionFlags`、`SendFlags`、`SendParams`；
- service：`Pacer`、`Connection<I>`、`Config`、`TransportMain<E, K, A>` 及其 local endpoint、
  port allocator、cleanup、ALPN operations；
- plugin/session：`IpSessionConfig` 对现有 config 的替代；
- plugin/session：`IpTransportMain`、唯一 process-global instance，以及关联 `init` / `global`；
- plugin/session：`From<IpSessionEndpoint> for IpTransportConnectionId`；
- plugin/session（private implementation）：`Key`、family/key `From` implementations 与
  `IpSessionTable::lookup_listener`，替代现有 lookup helper family；这项不新增 public lookup
  API；
- plugin/session：local endpoint mark/share/release/allocate operations 与
  `LocalEndpointError`；
- TCP/UDP：在现有 `TcpMain`/`UdpMain` 上实现
  `Transport<IpTransportEndpointConfig>`。

不提议新的 endpoint type/trait、index newtype、VFT、registry、trait object、额外 transport
Main 或 socket API。`IpTransportMain` 是 plugin/session 对 service generic Main 的唯一
concrete owner，不是并行 wrapper 或第二份状态。实施前以本 ADR 的 review/acceptance 作为上述
新 API 的显式批准。

## 13. 后果

- service transport 只看见 `SessionEndpoint<T>`，不看见 IP/FIB；
- service 提供完整 generic `TransportMain<E, K, A>` 定义，plugin/session 提供并发布唯一
  `IpTransportMain` concrete instance；
- plugin/session 的既有 `IpSessionEndpoint` 从 lookup-only type 变成 TCP/UDP lifecycle 的
  唯一 concrete endpoint；
- TCP/UDP Main 只拥有各自 protocol connection/listener/worker state，并直接依赖
  plugin/session 的 IP Session 与 IP transport instances；
- local endpoint 引用计数算法只实现一次，IP key、route、interface 和 port policy 留在
  plugin/session concrete owner；
- VFT 不再是目标架构，但 Session numeric dispatch 必须由后续 ADR 完成后才能删除 legacy
  path；
- socket、Session association 与 application metadata 保持在本 ADR 边界之外。
