# ADR-0036: App Namespace 与 Session table

- 日期：2026-09-21
- 状态：Accepted，Issue #350 实施
- Hammer 基线：`fe259cc1bb8d297f0790b0584bf11575c8e0a286`
- VPP 基线：`third_party/vpp`，提交
  `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- 前置决定：ADR-0035

本文继续 ADR-0035，记录 App Namespace、Session table、当前 Binary API request/reply 和
外部 client 的实施决定。

本阶段不设计 namespace socket，也不改 `ApplicationMain`、Application attach、listener、
connection 或 MQ。它们在后续迁移中再接入 namespace。

## 1. 决定

`hammer-app` 只提供 generic namespace owner：

```rust
pub struct AppNamespace<B> {
    id: String,
    secret: u64,
    binding: B,
}

pub struct AppNamespaceMain<B> {
    state: UnsafeCell<AppNamespaceState<B>>,
}
```

`B` 是 namespace 直接拥有的 binding。`hammer-app` 不知道 IP、FIB、interface 或 Session
table，也不保存另一张 `u32 -> binding` side table。

`hammer-plugin-session` 拥有其余 concrete state：

```rust
pub struct IpNamespaceBinding {
    sw_if_index: u32,
    ip4_fib_index: u32,
    ip6_fib_index: u32,
    local_table_index: u32,
}

pub struct IpNamespaceMain {
    namespaces: AppNamespaceMain<IpNamespaceBinding>,
    fib_source: FibSource,
}
```

两个具名 FIB 字段只存在于 IP concrete binding，因为 VPP 的 IP namespace 分别保存 IP4 与
IP6 FIB。generic namespace 没有 FIB 概念，plugin 边界也不传 `[u32; 2]`；每次 table/FIB
操作只接收一个 `family + fib_index`。因此“两个 FIB”是 IP 实例事实，不是 namespace 抽象。

依赖关系为：

```text
hammer-plugin-session
  -> hammer-app                    generic namespace storage
  -> hammer-plugin-ip              interface/FIB operations
  -> hammer-service                generic Session table types
  -> hammer-ipc                    Binary API capability
```

`hammer-ipc` 只提供 `Api`、codec、message table、dispatch 和 reply 能力，不声明或拥有 App
Namespace 协议。`hammer-plugin-session` 直接声明当前 Session API message range。

namespace index 直接使用 pool index `u32`，invalid sentinel 是 `u32::MAX`。不增加 index
newtype、dynamic dispatch、capability registry 或 callback access。

## 2. `hammer-app` owner

### 2.1 类型

```rust
use std::cell::UnsafeCell;
use std::collections::HashMap;

use hammer_infra::pool::Pool;

pub struct AppNamespace<B> {
    id: String,
    secret: u64,
    binding: B,
}

impl<B> AppNamespace<B> {
    pub fn id(&self) -> &str;
    pub const fn secret(&self) -> u64;
    pub const fn binding(&self) -> &B;
}

struct AppNamespaceState<B> {
    entries: Pool<AppNamespace<B>>,
    index_by_id: HashMap<String, u32>,
}

pub struct AppNamespaceMain<B> {
    state: UnsafeCell<AppNamespaceState<B>>,
}
```

pool record 同时拥有 id、secret 和 binding。`index_by_id` 只索引 pool，不拥有第二份
namespace state。

### 2.2 方法

```rust
impl<B> AppNamespaceMain<B> {
    pub fn new() -> Self;

    pub fn insert(
        &self,
        id: String,
        secret: u64,
        binding: B,
    ) -> u32;

    pub fn replace(
        &self,
        index: u32,
        secret: u64,
        binding: B,
    ) -> B;

    pub fn remove(&self, id: &str) -> Option<AppNamespace<B>>;

    pub fn get(&self, index: u32) -> Option<&AppNamespace<B>>;

    pub fn find(&self, id: &str) -> Option<(u32, &AppNamespace<B>)>;

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (u32, &AppNamespace<B>)> + '_;
}
```

`insert` 的 caller 必须先确认 id 不存在；重复 id 是 plugin/session 的程序错误。`replace`
保留 id，替换 secret 和 binding，并返回旧 binding。`remove` 把缺失表达为 `None`，由
plugin/session 的 operation 映射成该 request/reply 的 retval。

读取直接返回 borrow 或 iterator。不提供 `walk`、`with_*`、borrow wrapper、
`Vec<Option<_>>` 或 namespace index newtype。

mutation 只允许 Main Thread 在 WorkerBarrier 下调用。generic CRUD 不要求 `B: 'static`、
`Clone` 或 `Copy`；只有真正 process-global 的 concrete instance 才要求相应的 `Sync`。

## 3. `AppNamespaceAddDel` retval

server 与 client 各自在自己的 owner crate 声明这个 request/reply 的 retval enum。两边的
Rust 类型互不依赖，Binary API 仍只传输 reply 中的 `i32`。

server declaration 位于 `hammer-plugin-session` 的 Binary API module：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppNamespaceAddDelRetval {
    Invalid,
    NotSupported,
}

impl From<AppNamespaceAddDelRetval> for i32 {
    fn from(retval: AppNamespaceAddDelRetval) -> Self;
}
```

client declaration 位于 `hammer-binary-api-protocol::session`：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppNamespaceAddDelRetval {
    Invalid,
    NotSupported,
}

impl TryFrom<i32> for AppNamespaceAddDelRetval {
    type Error = i32;

    fn try_from(retval: i32) -> Result<Self, Self::Error>;
}
```

`Invalid` 对应 VPP `SESSION_E_INVALID`，`NotSupported` 对应
`SESSION_E_NOSUPPORT`。enum 不使用 `repr(i32)`、显式 discriminant 或另一张数字表。成功的
reply 直接写零，不伪装成 error variant。未知 `i32` 由 client `TryFrom` 原样返回为
`Err(i32)`。`AppNamespaceAddDelRetval` 是这组 request/reply 的 retval 集，不实现
`std::error::Error`，也不通过 `runtime_error` 进入 Hammer runtime error。不存在
`SessionError`、`ApiError`、`retval()`、`from_retval()` 或其他平行 error/conversion surface。

本阶段 namespace operation 只产生：

- interface 无效、没有任何 binding、显式 FIB id 不存在、delete id 不存在：`Invalid`；
- 非空 `sock_name`：`NotSupported`。

Main Heap allocation failure仍是现有 process allocation boundary。内部顺序错误是 assertion。
Hammer 当前没有可关闭的 Session lifecycle，因此本阶段不产生
`VNET_API_ERROR_FEATURE_DISABLED`；后续增加 enable/disable 时由对应 request/reply 扩展自己的
retval enum。

## 4. Plugin/session concrete instance

### 4.1 Namespace owner

```rust
use std::sync::OnceLock;

use hammer_app::{AppNamespace, AppNamespaceMain};
use hammer_service::net::FibSource;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNamespaceBinding {
    sw_if_index: u32,
    ip4_fib_index: u32,
    ip6_fib_index: u32,
    local_table_index: u32,
}

impl IpNamespaceBinding {
    pub const fn new(
        sw_if_index: u32,
        ip4_fib_index: u32,
        ip6_fib_index: u32,
        local_table_index: u32,
    ) -> Self;

    pub const fn sw_if_index(&self) -> u32;

    pub const fn fib_index(&self, family: IpSessionFamily) -> u32;

    pub const fn local_table_index(&self) -> u32;

    pub fn fibs(
        &self,
    ) -> impl Iterator<Item = (IpSessionFamily, u32)> + '_;
}

pub struct IpNamespaceMain {
    namespaces: AppNamespaceMain<IpNamespaceBinding>,
    fib_source: FibSource,
}

impl IpNamespaceMain {
    pub fn get(&self, index: u32) -> Option<&AppNamespace<IpNamespaceBinding>>;

    pub fn find(
        &self,
        id: &str,
    ) -> Option<(u32, &AppNamespace<IpNamespaceBinding>)>;

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (u32, &AppNamespace<IpNamespaceBinding>)> + '_;

    pub fn add_or_rebind(
        &self,
        id: String,
        secret: u64,
        sw_if_index: u32,
        ip4_fib_id: u32,
        ip6_fib_id: u32,
    ) -> Result<u32, AppNamespaceAddDelRetval>;

    pub fn delete(&self, id: &str) -> Result<(), AppNamespaceAddDelRetval>;
}

static NAMESPACES: OnceLock<IpNamespaceMain> = OnceLock::new();

pub fn namespaces() -> &'static IpNamespaceMain;
```

`fibs` 只迭代 index 不是 `u32::MAX` 的具名 family。只有 `namespaces()` 返回 `'static`，因为
它借用真实的 process-global `OnceLock`；其他方法使用普通 borrow lifetime。

### 4.2 FIB resolution

plugin/session 调用 plugin/ip 的单-family API：

```rust
pub fn fib_table_get_index_for_sw_if_index(
    version: IpVersion,
    sw_if_index: u32,
) -> Option<u32>;

pub fn fib_table_find(
    version: IpVersion,
    table_id: u32,
) -> Option<u32>;

pub fn fib_table_lock(
    version: IpVersion,
    fib_index: u32,
    source: FibSource,
);

pub fn fib_table_unlock(
    version: IpVersion,
    fib_index: u32,
    source: FibSource,
);
```

plugin/ip 不出现 namespace 类型。plugin/session 注册并持有自己的 `FibSource`，table
association 与 FIB lock 使用同一 source。

输入规则为：

1. `sw_if_index != u32::MAX` 时先确认 interface 存在，再分别读取 interface 的 IP4/IP6 FIB
   index；request 中的 FIB id 被忽略。
2. interface 为 `u32::MAX` 且两个 FIB id 都为 `u32::MAX` 时返回 `Invalid`。
3. 每个不是 `u32::MAX` 的 FIB id 必须存在于对应 family；否则返回 `Invalid`。
4. 单个 family 可以为 `u32::MAX`，两个 family 不能同时无效。

这保留 VPP 的 interface precedence、invalid sentinel 与 table-id lookup 语义。FIB id 只存在
于 Binary API adapter 和 `add_or_rebind` 输入，不进入 generic namespace。

## 5. Session table

### 5.1 Local table

ADR-0035 的 global table 每张只激活一个 family。local table 在同一 index 下初始化 IP4/IP6
session 与 half-open hash：

```rust
struct LocalTable {
    ip4: Ip4SessionTable,
    ip6: Ip6SessionTable,
}

enum IpSessionTableHashes {
    Ip4(Ip4SessionTable),
    Ip6(Ip6SessionTable),
    Local(LocalTable),
}

struct IpSessionTable {
    hashes: IpSessionTableHashes,
    appns_indices: Vec<u32>,
}
```

`appns_indices` 对齐 `session_table_t.appns_index`。local table 只关联自己的 namespace；
global table 关联使用该 family/FIB 的 namespace。service 的 generic `SessionTable<S, H>` 和
`SessionLookup` 不增加 namespace 或 FIB policy。

### 5.2 Operations

```rust
impl IpSessionLookup {
    pub fn alloc_local(&self) -> u32;

    pub fn bind_local(
        &self,
        appns_index: u32,
        table_index: u32,
    );

    pub fn bind_global(
        &self,
        appns_index: u32,
        family: IpSessionFamily,
        fib_index: u32,
        source: FibSource,
    );

    pub fn unbind_global(
        &self,
        appns_index: u32,
        family: IpSessionFamily,
        fib_index: u32,
        source: FibSource,
    );

    pub fn free_local(
        &self,
        appns_index: u32,
        table_index: u32,
    );
}
```

`bind_global` get-or-alloc table、增加 association 并 lock FIB。`unbind_global` 使用同一 source
unlock、删除 association，并在 lock count 为零时释放 table 与 FIB mapping。调用者不分别
操作 table association 和 FIB lifetime。

重复 bind、缺失 association、invalid table 或 family/table mismatch 是 plugin 内部顺序错误，
使用 assertion。

## 6. Namespace transaction

### 6.1 Create

```text
resolve interface and configured FIB ids
allocate local table
construct IpNamespaceBinding
insert AppNamespace -> appns_index
bind local table
bind each valid global family/FIB
return appns_index
```

输入错误在 mutation 前返回 `Invalid`。输入通过后，pool/table allocation 使用现有 Main Heap
failure boundary；其余步骤不返回 recoverable error。

### 6.2 Rebind

```text
resolve the new binding
borrow the old binding
for each changed family, bind the new valid global table
replace secret and binding while retaining local_table_index
for each changed family, unbind the old valid global table
return the existing appns_index
```

IP4/IP6 在 concrete owner 内按具名 family 比较，不通过数组位置比较。local table 保持不变。

### 6.3 Delete

```text
find namespace id or return Invalid
remove each valid global family/FIB binding
free local table
remove namespace from pool and id index
return success
```

Application cleanup 不进入本阶段。在 Application 还没有 namespace field 的迁移阶段，不会有
Application 归属于非 default namespace；后续接入时，cleanup 必须插入 table/FIB release
之前。

## 7. Default namespace

plugin/session 初始化时创建 index 0：

```rust
let table_index = session_lookup.alloc_local();
let binding = IpNamespaceBinding::new(u32::MAX, 0, 0, table_index);
let appns_index = main.namespaces.insert("default".to_owned(), 0, binding);

assert_eq!(appns_index, 0);

session_lookup.bind_local(appns_index, table_index);
session_lookup.bind_global(
    appns_index,
    IpSessionFamily::Ip4,
    0,
    main.fib_source,
);
session_lookup.bind_global(
    appns_index,
    IpSessionFamily::Ip6,
    0,
    main.fib_source,
);
```

default namespace 是 generic pool 的第一个 record，不增加 singleton。

## 8. Binary API

### 8.1 Plugin/session declaration

server protocol declaration、handler、message range 和 hookup 全部在 `hammer-plugin-session`。
Rust identifier 不携带协议版本；版本只保留在外部 message identity literal：

```rust
use hammer_component_macros::{Api, api_message_range, api_reply};

#[derive(Clone, Debug, Api)]
#[api(
    name = "app_namespace_add_del_v4",
    returns = AppNamespaceAddDelReply
)]
pub struct AppNamespaceAddDel {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
    pub secret: u64,
    pub is_add: bool,
    pub sw_if_index: InterfaceIndex,
    pub ip4_fib_id: u32,
    pub ip6_fib_id: u32,
    #[api(string = 64)]
    pub namespace_id: String,
    #[api(string)]
    pub sock_name: String,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "app_namespace_add_del_v4_reply")]
pub struct AppNamespaceAddDelReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub appns_index: u32,
}

fn app_namespace_add_del(request: AppNamespaceAddDel);

api_message_range! {
    pub fn setup_message_id_table;
    range "session";
    AppNamespaceAddDel => app_namespace_add_del {
        is_mp_safe: false,
        traced: true,
        replay: false
    };
    AppNamespaceAddDelReply {
        is_mp_safe: false,
        traced: true,
        replay: false
    };
}
```

handler 直接调用 `IpNamespaceMain::add_or_rebind` 或 `delete`，并用 `i32::from(retval)` 填
reply。message 标记为 `is_mp_safe: false`，由现有 dispatch 在 WorkerBarrier 下执行。

### 8.2 Request semantics

- `is_add = true` 且 id 不存在：create；
- `is_add = true` 且 id 已存在：rebind，保留 local table；
- `is_add = false`：只读取 namespace id，其余 binding 字段忽略；
- create/rebind 成功返回实际 `appns_index`；
- delete 成功返回 `appns_index = 0`；
- namespace id 是固定 64-byte API string；
- malformed payload 由 Binary API codec/dispatch 拒绝，不转换为
  `AppNamespaceAddDelRetval`。

本阶段不增加 TOML 配置、get/dump message 或旧协议 compatibility。

### 8.3 Socket field

外部 schema 必须保留 `sock_name`，否则 CRC 和 message identity 不再匹配当前接口。本阶段只
接受空字符串；非空值返回 `AppNamespaceAddDelRetval::NotSupported`。namespace record 不保存 socket name，
也不增加 listener、accepted socket 或 shutdown lifecycle。

后续 socket ADR 再定义 `sock_name` 的 create-only 语义和 `session_sapi_enable_disable`。

## 9. `netsystem-client`

client repository 继续使用 protocol/client/RPC 分层；transport `Client` 不增加 namespace
method。

### 9.1 Protocol

`hammer-binary-api-protocol::session` 镜像当前 request/reply。Rust 类型不带版本后缀：

```rust
pub struct AppNamespaceAddDel {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
    pub secret: u64,
    pub is_add: bool,
    pub sw_if_index: InterfaceIndex,
    pub ip4_fib_id: u32,
    pub ip6_fib_id: u32,
    pub namespace_id: FixedString<64>,
    pub sock_name: String,
}

impl Api for AppNamespaceAddDel {
    const NAME: &'static str = "app_namespace_add_del_v4";
    const BLOCK: Block = APP_NAMESPACE_ADD_DEL_BLOCK;
    const CRC: u32 = APP_NAMESPACE_ADD_DEL_BLOCK.crc();
    const NAME_CRC: &'static str = APP_NAMESPACE_ADD_DEL_NAME_CRC;
    const SERVICE: Option<Service> = APP_NAMESPACE_ADD_DEL_SERVICE;

    fn set_request_header(
        &mut self,
        id: u16,
        client_index: u32,
        context: u32,
    );

    fn context(&self) -> Option<u32>;
}

pub struct AppNamespaceAddDelReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub appns_index: u32,
}

impl Api for AppNamespaceAddDelReply {
    const NAME: &'static str = "app_namespace_add_del_v4_reply";
    const BLOCK: Block = APP_NAMESPACE_ADD_DEL_REPLY_BLOCK;
    const CRC: u32 = APP_NAMESPACE_ADD_DEL_REPLY_BLOCK.crc();
    const NAME_CRC: &'static str = APP_NAMESPACE_ADD_DEL_REPLY_NAME_CRC;

    fn context(&self) -> Option<u32>;
}
```

`&'static str` 只来自现有 `Api` trait 的 compile-time metadata。

### 9.2 RPC

```rust
pub enum AppNamespaceAddDelError {
    Client(ClientError),
    Rejected { retval: AppNamespaceAddDelRetval },
    UnknownRetval { retval: i32 },
}

pub struct NamespaceService<'client> {
    client: &'client mut Client,
}

impl<'client> NamespaceService<'client> {
    pub fn new(client: &'client mut Client) -> Self;

    pub async fn add_or_rebind(
        &mut self,
        secret: u64,
        sw_if_index: u32,
        ip4_fib_id: u32,
        ip6_fib_id: u32,
        namespace_id: FixedString<64>,
    ) -> Result<u32, AppNamespaceAddDelError>;

    pub async fn delete(
        &mut self,
        namespace_id: FixedString<64>,
    ) -> Result<(), AppNamespaceAddDelError>;
}
```

RPC 像 `VpeService::show_version` 一样自行构造 request，header 初始为零，`sock_name` 为空，
再调用 `Client::invoke`。reply 的 `retval` 字段保持 `i32`：零表示成功，非零值通过
`AppNamespaceAddDelRetval::try_from(reply.retval)` 转成 client 本地的 retval enum，并进入
`AppNamespaceAddDelError::Rejected { retval }`；不属于当前 enum 的值原样进入
`AppNamespaceAddDelError::UnknownRetval`。RPC error 以 request 名称限定，只负责组合该请求的
transport failure 与 reply 结果，不定义 namespace-wide 或 API-wide error。
client transport 和 request correlation 不增加 namespace 分支。

## 10. 初始化与迁移

初始化顺序：

```text
IP FIB init
IpSessionLookup init
Session FibSource registration
IpNamespaceMain init and default namespace index 0
Session Binary API hookup
```

迁移顺序：

1. 在 `hammer-app` 增加 `AppNamespace<B>` 与 `AppNamespaceMain<B>`。
2. 在 plugin/ip 补 table-id lookup 和 source-based FIB lock/unlock。
3. 在 plugin/session 声明 request-specific `AppNamespaceAddDelRetval`，增加 local table、
   association operations、`IpNamespaceBinding`、`IpNamespaceMain` 和 default namespace。
4. 在 plugin/session 增加当前 request/reply、handler 与 API hookup。
5. 在 `netsystem-client` 增加 protocol declarations、本地
   `AppNamespaceAddDelRetval` 和 `NamespaceService<'client>`。
6. 后续迁移再接入 `ApplicationMain`、Application attach/cleanup 和 namespace socket。

## 11. 实施 surface

- hammer-app：`AppNamespace<B>`、`AppNamespaceMain<B>` 和本文列出的方法；
- plugin/ip：单-family table-id lookup、FIB lock/unlock；
- plugin/session：`AppNamespaceAddDelRetval`、local table、五个 table operations、
  `IpNamespaceBinding`、`IpNamespaceMain`、global accessor、default namespace 和当前 Binary
  API；
- `netsystem-client`：当前 protocol declarations、本地 `AppNamespaceAddDelRetval`、
  `NamespaceService` 和 `AppNamespaceAddDelError`。

本 ADR 不批准旧协议、`netns`、remote get/list、Application integration、namespace socket、
callback collection access、dynamic dispatch、namespace index newtype 或 parallel binding map。
