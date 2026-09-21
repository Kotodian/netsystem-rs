# ADR-0035: Session Endpoint 与 Table 的参数化边界

- 日期：2026-09-21
- 状态：Accepted，Issue #348 实施
- Hammer 基线：`22094eaa5177d5cb5f790904e88ae5934b6dcf0e`
- VPP 基线：`third_party/vpp`，提交
  `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- 第一阶段范围：`vnet/session/session_lookup.c`、`session_lookup.h`、
  `session_table.c`、`session_table.h`

本文决定类型、所有权和迁移顺序。Rust 代码块记录已批准的目标接口。

## 1. 决定

`hammer-service::session` 不定义任何 IP endpoint、IP key、IP family、FIB mapping 或具体
Bihash。它只定义三个正交部分：

1. `SessionEndpoint<T>`：transport endpoint 加 transport protocol 的参数化值；
2. `SessionTable<S, H>`：session/listener table 加 half-open table 的参数化存储；
3. `SessionLookup`：Session core 使用 lookup owner 的静态分派行为边界。

`crates/hammer-plugins/session/` 新建 `hammer-plugin-session`，并拥有唯一的 IP concrete
instance：

- VPP 语义的 IP transport endpoint、Session endpoint 和 connection identity；
- IP4 `Bihash16x8`、IP6 `Bihash48x8`；
- 每个 global table 只激活一个 IP family，session/listener 与 half-open 分表；
- FIB index 到 Session table index 的两组 mapping；
- `session_lookup.c` 的 IP4/IP6 key、增删和 lookup 顺序；
- `IpSessionLookup` 的 process-global instance。

这个边界使用静态泛型。不得使用 `dyn`、函数表、类型擦除、backend registration，service
也不得保存 plugin capability 或 plugin-owned reference。

第一阶段只迁移 global IP4/IP6 lookup。local table、rules、FIB lock/cleanup 和 application
namespace 不进入类型或 API。

## 2. Service 的数据抽象

### 2.1 SessionEndpoint

VPP 的 `session_endpoint_t` 是 transport endpoint config 加 `transport_proto`。service 保留
这个组合关系，但不规定 transport endpoint 的字段：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEndpoint<T> {
    transport: T,
    transport_protocol: u8,
}

impl<T> SessionEndpoint<T> {
    pub const fn new(transport: T, transport_protocol: u8) -> Self;
    pub const fn transport(&self) -> &T;
    pub const fn transport_protocol(&self) -> u8;
}
```

`SessionEndpoint<T>` 是拥有数据的参数化类型，不是空 marker trait。service 不增加
`address()`、`family()`、`fib_index()` 或 key builder；这些事实只存在于 concrete `T`。
service 也不声明无法通过 orphan rule 合法实现的 generic consuming conversion。concrete owner
按需要实现 `From<ConcreteSessionEndpoint> for ConcreteTransportEndpoint`，调用方使用 `Into`。

### 2.2 SessionTable

VPP table 的稳定存储关系是：一张 hash 保存 established Session 和 listener，另一张 hash
保存 half-open connection。service 抽象这个关系，不规定 key 宽度或 hash 类型：

```rust
pub struct SessionTable<S, H> {
    sessions: S,
    half_open: H,
}

impl<S, H> SessionTable<S, H> {
    pub const fn new(sessions: S, half_open: H) -> Self;
    pub const fn sessions(&self) -> &S;
    pub const fn half_open(&self) -> &H;
}
```

该类型不提供统一 key、不做 family dispatch，也不包含配置、FIB mapping、local/global flag、
rules handle 或 namespace state。它只是 `session_table.c` 中两类 hash 所共有的参数化结构。

Session table index 直接使用 `u32`，保留 VPP 的 `u32::MAX` invalid sentinel，不增加 index
newtype，也不用 `Option` 改写 mapping。

## 3. Service 的行为边界

`SessionTable<S, H>` 不承担 lookup policy。完整 lookup 行为由单独的 owner trait 表达，避免把
IP4/IP6 table 结构、listener fallback 和 Session core 混成一个“大 table trait”。

```rust
use hammer_runtime::app::SessionHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLookupResult<H> {
    Session(SessionHandle),
    HalfOpen(H),
    WrongThread,
    NotFound,
}

pub trait SessionLookup {
    type Endpoint;
    type ConnectionId;
    type HalfOpenHandle;

    fn add_connection(
        &self,
        connection: &Self::ConnectionId,
        handle: SessionHandle,
    );

    fn remove_connection(&self, connection: &Self::ConnectionId) -> bool;

    fn add_session_endpoint(
        &self,
        table_index: u32,
        endpoint: &Self::Endpoint,
        handle: SessionHandle,
    ) -> bool;

    fn remove_session_endpoint(
        &self,
        table_index: u32,
        endpoint: &Self::Endpoint,
    ) -> bool;

    fn add_half_open(
        &self,
        connection: &Self::ConnectionId,
        handle: Self::HalfOpenHandle,
    );

    fn remove_half_open(&self, connection: &Self::ConnectionId) -> bool;

    fn half_open_handle(
        &self,
        connection: &Self::ConnectionId,
    ) -> Option<Self::HalfOpenHandle>;

    fn lookup_connection(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_connection_on_thread(
        &self,
        connection: &Self::ConnectionId,
        thread_index: u32,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_session(
        &self,
        connection: &Self::ConnectionId,
    ) -> Option<SessionHandle>;

    fn lookup_exact(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_listener(
        &self,
        table_index: u32,
        endpoint: &Self::Endpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle>;
}
```

`SessionLookupResult` 不单列 listener：VPP 的 connection lookup 命中 established 或 listener
后都继续解析为 Session/transport connection。`WrongThread` 也不附加 owner index，因为 VPP
只返回 `SESSION_LOOKUP_RESULT_WRONG_THREAD`。rules 未进入第一阶段，因此没有 `Filtered`。

trait 不含 config、table allocation、FIB 或 IP family 方法。这些不是 Session core 对 lookup
owner 的需求，而是 IP concrete instance 的管理 API。

trait 本身不要求 `'static`、`Send`、`Sync`、`Clone` 或 `Copy`。这些不是 lookup 语义；某个
worker queue 需要移动或复制 `ConnectionId` 时，由该 queue 的 impl 加局部 bound。只有
plugin/session 发布的 concrete global 必须满足静态存储和跨线程访问条件。

## 4. Plugin 的 concrete endpoint

下列类型全部定义在 `hammer-plugin-session`。`IpAddr` 替代 C 的 `ip46_address_t + is_ip4`，
但字段含义、network byte order 和 invalid sentinel 与 VPP 一致。

```rust
use std::net::IpAddr;

use hammer_service::session::SessionEndpoint;

pub const ENDPOINT_INVALID_INDEX: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpTransportEndpoint {
    pub address: IpAddr,
    pub port: u16,
    pub sw_if_index: u32,
    pub fib_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpTransportEndpointConfig {
    pub local: IpTransportEndpoint,
    pub peer: IpTransportEndpoint,
    pub next_node_index: u32,
    pub next_node_opaque: u32,
    pub mss: u16,
    pub dscp: u8,
    pub transport_flags: u8,
}

pub type IpSessionEndpoint = SessionEndpoint<IpTransportEndpointConfig>;

impl From<IpSessionEndpoint> for IpTransportEndpointConfig {
    fn from(endpoint: IpSessionEndpoint) -> Self;
}
```

`port` 保持 network byte order。`mss == 0`、`sw_if_index == u32::MAX`、
`fib_index == u32::MAX` 和 `next_node_index == u32::MAX` 保留 VPP 的 sentinel 语义；不改成
`Option`，也不增加另一套 endpoint validation。

transport connection 的 lookup identity 对应 VPP `transport_connection_t` 的 connection-id
前缀。enum 使 mixed-family identity 无法构造：

```rust
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpTransportConnectionId {
    Ip4 {
        remote_address: Ipv4Addr,
        local_address: Ipv4Addr,
        fib_index: u32,
        remote_port: u16,
        local_port: u16,
        dscp: u8,
        transport_protocol: u8,
    },
    Ip6 {
        remote_address: Ipv6Addr,
        local_address: Ipv6Addr,
        fib_index: u32,
        remote_port: u16,
        local_port: u16,
        dscp: u8,
        transport_protocol: u8,
    },
}

impl From<(u32, SocketAddrV4, SocketAddrV4, u8)> for IpTransportConnectionId {
    fn from(value: (u32, SocketAddrV4, SocketAddrV4, u8)) -> Self;
}

impl From<(u32, SocketAddrV6, SocketAddrV6, u8)> for IpTransportConnectionId {
    fn from(value: (u32, SocketAddrV6, SocketAddrV6, u8)) -> Self;
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpHalfOpenHandle(u64);

impl IpHalfOpenHandle {
    pub const INVALID: Self = Self(u64::MAX);

    pub const fn new(handle: u64) -> Self;
    pub const fn value(self) -> u64;
}
```

`dscp` 是 connection identity 的一部分，但不进入 VPP Session Bihash key；key builder 必须显式
忽略它。

## 5. Plugin 的 concrete table

第一阶段的 global table 在 VPP 中通过 `active_fib_proto` 只初始化一个 family。Rust 用 enum
消除未初始化的另一组 hash，同时复用 service 的参数化 table：

```rust
use hammer_infra::bihash::{Bihash16x8, Bihash48x8};
use hammer_service::session::SessionTable;

type Ip4SessionTable = SessionTable<Bihash16x8, Bihash16x8>;
type Ip6SessionTable = SessionTable<Bihash48x8, Bihash48x8>;

enum IpSessionTable {
    Ip4(Ip4SessionTable),
    Ip6(Ip6SessionTable),
}
```

这与 global `session_table_t` 的有效状态一致：

- IP4 table：`v4_session_hash` 与 `v4_half_open_hash`；
- IP6 table：`v6_session_hash` 与 `v6_half_open_hash`；
- session hash 同时保存 established Session 和 listener；
- half-open hash 独立；
- IP4 不扩展成六个 `u64`，IP6 不压缩成 IP4 key。

lookup owner 保存 VPP 的 table pool 和两个 FIB mappings：

```rust
use std::cell::UnsafeCell;

use hammer_infra::pool::Pool;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpSessionFamily {
    Ip4,
    Ip6,
}

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

    pub fn table_index(
        &self,
        family: IpSessionFamily,
        fib_index: u32,
    ) -> u32;

    pub fn get_or_alloc_table_index(
        &self,
        family: IpSessionFamily,
        fib_index: u32,
    ) -> u32;

    pub fn table_memory_size(&self, table_index: u32) -> u64;
}
```

mapping slot 是 `u32`，未分配值是 `u32::MAX`：

```rust
mapping.resize(fib_index as usize + 1, u32::MAX);
```

初始化先分配 FIB index 0 的 IP4 table，再分配 FIB index 0 的 IP6 table，与
`session_lookup_init` 相同。`get_or_alloc_table_index` 先检查 mapping，再在 VPP 对应的稀有
allocation path 中建立 table；已经存在时返回原 index。与 VPP 的 `ASSERT (fib_index != ~0)`
一致，`u32::MAX` 不能进入 allocation path。

table pool 和 mapping 只在 startup 或 WorkerBarrier 已停止全部 Data Worker 时变更；worker
运行期间只读 mapping 和 table pool，Bihash 自身保留并发 lookup/add/delete 语义。
`UnsafeCell` 不包住 Bihash，也不提供公开借用 API；它只承载 barrier 保护的 pool/mapping
publication，不增加 `Mutex`、`RwLock` 或第二套 publication protocol。

## 6. VPP 配置语义

table 配置由 concrete plugin 拥有。字段为 `u32`，默认值全部为 zero；zero 表示没有 override，
table 初始化时才替换为 VPP 默认的 20,000 buckets 和 64 MiB。

```rust
#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IpSessionTableConfig {
    pub v4_session_table_buckets: u32,
    pub v4_session_table_memory: u32,
    pub v4_halfopen_table_buckets: u32,
    pub v4_halfopen_table_memory: u32,
    pub v6_session_table_buckets: u32,
    pub v6_session_table_memory: u32,
    pub v6_halfopen_table_buckets: u32,
    pub v6_halfopen_table_memory: u32,
}
```

字段名直接对应 VPP 的 `configured_v*_session_table_*` 与
`configured_v*_halfopen_table_*`。plugin/session 与 service 分别反序列化同一个
`[network.session]` table，因此两侧都允许另一 owner 的字段；这不改变 zero override、类型范围
或默认值语义。

```rust
const DEFAULT_SESSION_TABLE_BUCKETS: u32 = 20_000;
const DEFAULT_SESSION_TABLE_MEMORY: u32 = 64 << 20;
```

不增加 minimum、entry-capacity ratio、统一 capacity、power-of-two config restriction 或独立
effective-config 类型。非 zero bucket 值直接交给 Bihash；Bihash 仍按其 VPP-style 实现将
bucket count round up。memory 输入由 `u32` 限制在 VPP 接受的 `< 2^32` 范围，超范围沿用
现有配置反序列化错误，不定义 Session 配置错误。

当前 `Bihash::new(nbuckets)` 无法表达 VPP 的 `memory_size`。`hammer-infra` 只增加一个通用
构造方法，不增加 Session-specific allocator 或 error：

```rust
impl<K: BihashKey + Default, const KVP: usize> Bihash<K, KVP> {
    pub fn with_memory_size(nbuckets: u32, memory_size: u32) -> Self;
}
```

该方法的 `memory_size` 与 `clib_bihash_init2_*` 同义：它限制该 Bihash 的 page storage，普通
Rust 分配仍来自 Hammer Main Heap。构造或增长无法取得内存时沿用既有 Main Heap/process
allocation failure，不翻译成 lookup `Result`。

## 7. Concrete key

key 类型和 builder 仅存在于 `hammer-plugin-session`：

```rust
type Ip4SessionKey = u128;
type Ip6SessionKey = [u64; 6];

fn ip4_connection_key(connection: &IpTransportConnectionId) -> Ip4SessionKey;
fn ip4_listener_key(endpoint: &IpSessionEndpoint) -> Ip4SessionKey;
fn ip4_proxy_key(endpoint: &IpSessionEndpoint) -> Ip4SessionKey;

fn ip6_connection_key(connection: &IpTransportConnectionId) -> Ip6SessionKey;
fn ip6_listener_key(endpoint: &IpSessionEndpoint) -> Ip6SessionKey;
fn ip6_proxy_key(endpoint: &IpSessionEndpoint) -> Ip6SessionKey;
```

IP4 key：

```text
word 0: remote IP4 | local IP4
word 1: transport protocol | remote port | local port
```

IP6 key：

```text
word 0..1: local IP6
word 2..3: remote IP6
word 4:    transport protocol | remote port | local port
word 5:    zero
```

listener key 清零 remote address/port；wildcard lookup 再清零 local address；proxy key 保留
local address 和 protocol 并清零 port。builder 显式构造 key words，不依赖 Rust enum layout。

session/listener value 是 `u64::from(SessionHandle)`，不再拆成 session-index hash 和
thread-index hash。half-open value 是 `IpHalfOpenHandle::value()`。

## 8. Lookup 顺序

`lookup_connection_on_thread` 对齐 `session_lookup_connection_wt4/6`，第一阶段跳过 rules：

```text
established
  -> handle.thread_index != requested thread: WrongThread
half-open
listener exact
listener wildcard local address
proxy listener
NotFound
```

`lookup_connection` 使用相同顺序，但不做 requested-thread 比较。

`lookup_session` 对齐 `session_lookup_safe4/6`，不查 half-open：

```text
established
listener exact
listener wildcard local address
proxy listener
None
```

`lookup_exact` 对齐 `session_lookup_6tuple`：只查 established，再查 half-open，不查 listener。

`lookup_listener(..., use_wildcard)` 对齐 `session_lookup_listener4_i/6_i`：先 exact；参数为
`true` 时再查 wildcard local address；最后总是查 proxy listener。

IP4 和 IP6 保持两个 concrete fast path。实现可以像 VPP 一样重复短路径，不在 packet path
引入 enum-erased key、trait object 或统一六字 key。

## 9. Add、delete 与 miss 语义

不定义 `SessionLookupError`、`SessionTableError` 或 `BihashError`。方法返回值直接映射 VPP
lookup/table 的可观察语义：

- add 使用 `clib_bihash_add_del(..., is_add = 1)` 的 add-or-overwrite 语义；duplicate 覆盖旧值；
- connection/half-open add 的 table 由 `get_or_alloc` 保证存在，因此方法返回 `()`；
- `add_session_endpoint` 的显式 table index 无效时返回 `false`，成功 add/overwrite 返回
  `true`；
- delete 成功返回 `true`，table/key 不存在返回 `false`，对应 VPP 的 zero/negative status；
- ordinary lookup miss 使用 `Option::None` 或 `SessionLookupResult::NotFound`；
- half-open miss 使用 `Option::None`，对应 `HALF_OPEN_LOOKUP_INVALID_VALUE`；
- thread mismatch 只产生 `SessionLookupResult::WrongThread`；
- family 与 key builder 不匹配是 plugin 内部 bug，使用 assertion，不定义 recoverable error；
- allocation failure保留 Bihash/Main Heap 的既有 process failure boundary。

Session migration 使用的 compare-current replace/remove 不是 `session_lookup.c` API，因此不加入
`SessionLookup`。它们是 `IpSessionLookup` 的 concrete publication 操作，只供 UDP 保持既有迁移
原子性；不能借迁移需求改变上述 VPP add/delete 语义。

## 10. Concrete lookup global

worker-owned Session、FIFO、MQ、event、migration 和 state machine 继续由 service 实现。
service 的 `SessionMain` 不保存 lookup、endpoint 或 table，也不对 concrete lookup 类型参数化；
需要 IP lookup 的 transport plugin 显式依赖 `hammer-plugin-session`。

concrete lookup global 只定义在 `hammer-plugin-session`：

```rust
use std::sync::OnceLock;

static SESSION_LOOKUP: OnceLock<IpSessionLookup> = OnceLock::new();

pub fn session_lookup() -> &'static IpSessionLookup;
```

这里的 `'static` 来自 `static SESSION_LOOKUP` 本身，不传播到 `SessionLookup` trait 或关联类型。
`IpSessionLookup`、`IpSessionEndpoint` 和 `IpTransportConnectionId` 都是 concrete plugin 拥有的
owned 类型；其 `Send`/`Sync` 条件在 `OnceLock<IpSessionLookup>` 和 worker publication 的具体
实现处检查。

TCP、UDP 显式依赖 `hammer-plugin-session`；daemon 的插件构建清单包含该 artifact。transport
plugin 分别声明对 `ip` 与 `session` 的生命周期依赖，plugin/session 本身不依赖 plugin/ip。它们构造
`IpSessionEndpoint` / `IpTransportConnectionId` 并调用 concrete global；
`hammer-service` 不反向依赖 plugin/session 或 plugin/ip。

## 11. 第一阶段迁移顺序

1. 在 `hammer-service::session` 增加参数化 `SessionEndpoint<T>`、
   `SessionTable<S, H>`、`SessionLookup` 和 `SessionLookupResult<H>`。
2. 在 `hammer-infra::bihash` 增加 `with_memory_size`，保持既有 add/overwrite、delete 和 miss
   语义。
3. 新建 `hammer-plugin-session`，先移植 concrete endpoint、IP4/IP6 key builder、table pool、
   FIB mapping 和配置。
4. 移植 `session_table_init`、default FIB 0 tables 和 `get_or_alloc` table path。
5. 移植 connection、Session endpoint 和 half-open 的 add/delete/lookup。
6. 移植 `connection_wt4/6`、`connection4/6`、`safe4/6`、`listener4/6` 和 `6tuple` 的顺序。
7. 在 plugin/session 发布 concrete lookup global，TCP/UDP 显式依赖该 owner。
8. 删除 service 中的 `SocketAddr` lookup、统一六字 key、
   双 value Bihash 和旧 `SessionEndpointLookup`。

旧 lookup 只允许在迁移提交中作为尚未切换的实现存在；完成 consumer 切换后必须同一阶段删除，
不能让新旧 table 同时接收运行时写入。

## 12. 已批准 API

Issue #348 批准以下新增 surface：

- service：`SessionEndpoint<T>`、`SessionTable<S, H>`、`SessionLookup` 和
  `SessionLookupResult<H>`；
- plugin/session：本 ADR 中的 IP endpoint、connection identity、half-open handle、family、
  table config、lookup owner、global accessor，以及 UDP migration 使用的 compare-current
  replace/remove；
- infra：`Bihash::with_memory_size`。

第一阶段不得顺带加入 local table、rules、namespace、
FIB cleanup/lock、transport local-endpoint table或新的 recoverable error 类型。
