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
  ApplicationCallbacks callback table and existing SessionEvent queue entries
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

`ApplicationMain` 对齐 VPP `static app_main` 和 `application_init`
（`application.h:205-238`、`application.c:2090-2106`）。应用池、名称哈希表和 listener pool
由主线程控制路径修改；每个 `pending_rx_mq_heads[worker.slot()]` 只由对应 Data Worker 的
FileMain/Session node 操作（`application.c:332-453`）。VPP 的 thread 0 也运行图，Hammer
thread 0 不运行 dataplane：这些向量只有 `worker_count` 个 entry，以
`DataWorkerId::slot()` 索引。SessionHandle 中的 runtime thread index 仍为 `slot + 1`，
它不是向量下标。不能把 RX MQ 回调写成对全局 Main 的 `&mut` 借用，也不能声称一次
WorkerBarrier 就能替代运行中每个 worker 的独占访问。

```rust
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::ptr::NonNull;
use std::collections::hash_map::RandomState;
use hammer_infra::pool::Pool;

// VPP: application.h:205-238; application.c:2090-2106.
// VPP appsl_wrk_t: application.h:196-203. The per-worker collection is a
// direct Vec; ApplicationMain owns its fields directly.
pub struct ApplicationMain {
    applications: UnsafeCell<Pool<Application>>,
    listeners: UnsafeCell<Pool<ApplicationListener>>,
    // VPP keeps app_workers in application_worker.c:13-44. It is the same
    // service-owned application sublayer, indexed by app_worker_t.wrk_index.
    workers: UnsafeCell<Pool<AppWorker>>,
    application_by_api_client: UnsafeCell<HashMap<u32, u32>>,
    // VPP: app_by_name, application.c:298-329. This is the sole name table.
    // HashTable permits content hashing for both lookup and table growth.
    application_by_name: UnsafeCell<hashbrown::HashTable<(NonNull<str>, u32)>>,
    name_hasher: RandomState,
    // VPP: appsl_wrk_t.pending_rx_mqs, application.h:196-203. Each worker
    // owns the head pointer of one circular intrusive list.
    pending_rx_mq_heads: Vec<UnsafeCell<Option<NonNull<ApplicationRxMq>>>>,
    // VPP: application.h:230-238; application.c:1384-1443. Connect RPC
    // request storage needs its own service-level API decision; it is not
    // SessionControlData and is outside this ADR's proposed type surface.
}

// SAFETY: control-plane pool/table mutation requires the main thread and a
// held WorkerBarrier; each RX MQ list/head is accessed only by its Data Worker.
// All linked entry addresses remain fixed until detach unlinks every node.
unsafe impl Send for ApplicationMain {}
unsafe impl Sync for ApplicationMain {}

impl ApplicationMain {
    // VPP: application_init initializes app_main worker state and lookup tables.
    // All fallible allocation completes before OnceLock installation.
    pub fn init(worker_count: u32) -> Result<(), RuntimeError>;

    // Only the process-global Main borrow is 'static. Name references
    // returned from &self end with that borrow.
    #[inline(always)]
    pub fn global() -> Result<&'static Self, RuntimeError>;

    // VPP: application_alloc_and_init, application.c:664-767.
    pub fn attach(&self, config: ApplicationConfig) -> Result<u32, ApplicationError>;

    // VPP application_t.ns_index, application.h:139-151. The service keeps
    // the raw namespace pool index; generic namespace ownership remains in
    // hammer-app and IP binding validation remains in plugin-session.
    #[inline(always)]
    pub fn namespace(&self, application: u32) -> Option<u32>;

    // VPP: application_alloc_worker_and_init/vnet_app_worker_add_del,
    // application.c:986-1074.
    pub fn attach_worker(&self, application: u32, api_client: u32)
        -> Result<u32, ApplicationError>;

    pub fn detach_worker(&self, application: u32, worker_map: u32)
        -> Result<(), ApplicationError>;

    // VPP: application_detach_process/application_free, application.c:771-861.
    pub fn detach(&self, application: u32) -> Result<(), ApplicationError>;

    #[inline]
    pub fn application(&self, application: u32) -> Option<&Application>;

    // VPP: application_lookup_name, application.c:321-330. Both hash and
    // comparison use name contents, never the pointer address.
    pub fn lookup_name(&self, name: &str) -> Option<&Application> {
        let hash = self.name_hasher.hash_one(name);
        let names = unsafe { &*self.application_by_name.get() };
        let (_, index) = names.find(hash, |(key, _)| {
            // SAFETY: attach inserts only Application-owned name buffers;
            // detach removes the key before freeing its Application.
            unsafe { key.as_ref() == name }
        })?;
        unsafe { &*self.applications.get() }.get(*index)
    }

    #[inline]
    pub fn worker(&self, worker: u32) -> Option<&AppWorker>;

    #[inline]
    pub fn listener(&self, listener: u32) -> Option<&ApplicationListener>;

    #[inline]
    pub fn listener_for_session(&self, session: SessionHandle) -> Option<u32>;
}
```

`attach`/`detach`/listener 修改只在 main/control thread 借用对应字段，运行中先用
现有 WorkerBarrier 停止可能读取 pool 的 Data Worker；其 `&self` 不是并发写入许可。
新 `attach`/`attach_worker`/`detach` 路径在 barrier 安装后必须用现有
`worker_thread_barrier_sync!` 包住发布/移除操作；宏负责嵌套进入和作用域退出，不在新路径
手写 `barrier.sync` 或 `is_pending()` 分支。barrier 尚未安装的初始化阶段由 main thread
直接修改；上述要求不扩展到已标记弃用的旧 MQ/listener/connection 路径。
依据：`third_party/vpp/src/vlib/threads.h:109-116`、
`third_party/vpp/src/vlib/threads.c:1323-1450`；Hammer 宏定义在
`crates/hammer-runtime/src/lib.rs:118-130`，barrier 安装在
`crates/hammer-runtime/src/start_workers.rs:74-79`。
`lookup_name` 的返回借用不得越过 detach 的 barrier/release 边界。worker 的 RX MQ
callback 只修改对应 `UnsafeCell` entry，绝不借整个 `ApplicationMain` 为 `&mut`。

`Application` 保存 VPP 的 application-level facts，并保存所选 namespace 的 raw `u32` pool
index。它不保存 namespace object、socket、IP endpoint、FIB 或 protocol connection。ADR-0036
的 `hammer-app::AppNamespaceMain<B>` 仍拥有 generic namespace record，
`hammer-plugin-session::IpNamespaceMain` 仍拥有 IP concrete binding；这里仅保存 Application
到 namespace 的归属关系。VPP 在 `application.c:298-329` 调用的是不复制 key 的
`hash_set_mem`，不是显式分配并复制 key 的 `hash_set_mem_alloc`
（`vppinfra/hash.h:228-258`）；名称查找是哈希查找。Hammer 保持
`Application.name: String` 为唯一 owner，attach 将 `ApplicationConfig.name` 移入
Application，不复制字节。原有的 **一张** 名称表保存 `(NonNull<str>, u32)`，按名称内容
查找，不遍历 pool，也不为查询分配字符串。这里用 `hashbrown::HashTable` 实现 VPP
已有的 `app_by_name`，并非增加第二张表：它的 `find`、`insert_unique` 与扩容回调均可
使用相同的内容哈希。`HashMap<NonNull<str>, u32>` 即便 raw-entry 插入时显式传入
内容哈希，扩容也会调用指针键的 `Hash`，把表重算成地址哈希，不能用。
`hammer-infra::Bihash` 只接受固定大小的 `Copy` key，不能直接以可变长度名称作键。

`String` 移动时只移动其头部，已分配的名称字节地址不变；因此键指向 `str` 字节而
不是 pool 中的 `String` 头部，无需额外 `Pin<Box<Application>>` 分配。字段私有且
name 在插表后不可变，attach 在发布键前完成全部可能失败的分配和重名检查；表扩容
通过同一个 `RandomState` 对现存名称内容重算哈希。detach 先按相同内容哈希删除原键，
再释放 Application。`NonNull<str>` 只在这三个受控操作
内解引用，绝不作为对外借用或跨 detach 缓存。该局部 `unsafe` 生命周期不变量需要
迁移时由 attach/detach/重复名称测试覆盖；不能把 raw pointer 当作自动所有权保证。
名称表的查找、插入和删除都属于 Application Main 的控制路径；若 worker 可读这些
Application，则 detach 必须先通过现有 WorkerBarrier 停止读者，再删表、释放对象。
`lookup_name` 的借用不能跨越这个释放边界，也不能因为 Main 是 global 就把名称
引用标成 `'static`。
`Pin` 只能固定 `Application` 的地址，不能阻止其 `String` 在修改时重分配，更不能
解决同一 Main 内 `HashMap<&String, _>` 的自借用与 detach 生命周期。`Cow::Borrowed`
仍是这样的借用，`Cow::Owned` 会改变名称 owner 或产生第二份内容；两者都不提供
VPP `hash_set_mem` 所需的可删除、按内容哈希且不复制 key 的生命周期保证。

```rust
// VPP: application_name_table_add/del, application.c:296-309;
// hash_set_mem/hash_unset_mem, vppinfra/hash.h:234-261.
// Inside attach's main-thread control/barrier scope, after the Application
// is in the pool and duplicates are rejected:
let applications = unsafe { &mut *self.applications.get() };
let names = unsafe { &mut *self.application_by_name.get() };
let application = applications.get(application_index).expect("new application exists");
let name = application.name.as_str();
let hash = self.name_hasher.hash_one(name);
names.insert_unique(
    hash,
    (NonNull::from(name), application_index),
    |(key, _)| {
        // SAFETY: every key still points into a live, immutable Application.
        self.name_hasher.hash_one(unsafe { key.as_ref() })
    },
);

// Inside detach, before removing the Application from the pool:
let applications = unsafe { &mut *self.applications.get() };
let names = unsafe { &mut *self.application_by_name.get() };
let application = applications.get(application_index).expect("attached application exists");
let name = application.name.as_str();
let hash = self.name_hasher.hash_one(name);
let entry = names.find_entry(hash, |(key, index)| {
    *index == application_index && unsafe { key.as_ref() == name }
}).expect("attached application has a name entry");
entry.remove();
let application = applications.remove(application_index).expect("attached application exists");
drop(application);
```

本 ADR 对“拥有、借用、非拥有引用”的边界固定如下：

- `Application` 独占 `name: String`；名称表只保存指向这份字节的 `NonNull<str>`，
  查询参数是借用的 `&str`，不会复制名称内容。
  private RX MQ 的 segment 与 entry 向量一起存在或一起不存在。
- `ApplicationRxMq`、`AppWorker` 和 `SegmentManager` 保存 MQ 的 segment/manager 索引；
  取队列时经持有者返回 `&SvmMsgQ`。VPP 对应字段是 `svm_msg_q_t *`，但 Hammer 当前
  `SvmFifoSegment::mqs` 是可增长 `Vec`（`fifo_segment.rs:89-104`, `:815-902`），不能
  保存一个声称与 segment 同寿且不会失效的 `&SvmMsgQ`。
- SegmentManager 拥有 segment pool，Session 的 FIFO 字段保存现有 infra `Fifo`
  描述值；底层 SVM FIFO 存储仍由 segment 管理。跨 worker 或跨 pool 只保存
  `u32`/`SessionHandle`，查询接口返回随持有者借用结束的引用。
- `Option` 只表示 VPP 语义上的确实缺失，例如 private RX MQ 未启用、listener 尚未
  建立 local/global Session、或 listening/half-open Session 还没有 FIFO。已建立的
  connected Session 必须有完整 FIFO pair（`session_types.h:239-290`，
  `application.c:1179-1185`）。

VPP 的跨 worker connect RPC list 保存在 `app_main_t`（`application.h:230-238`、
`application.c:1384-1443`）。元素是完整 `vnet_connect_args_t`，不是 Session worker 的
86-byte `SessionControlData`。本 ADR 暂不声明 Hammer 的请求类型或容器；迁移 connect RPC
时应在 service 边界单独定义完整请求与锁的所有权，不能把这个差异藏在 `u32` index 中。

```rust
// VPP: application_t, application.h:119-161.
pub struct Application {
    index: u32,
    flags: ApplicationFlags,
    segment: SegmentManagerProperties,
    worker_maps: Pool<ApplicationWorkerMap>,
    // VPP app->name is the only owner; the hash key borrows these bytes.
    name: String,
    namespace: u32,
    proxied_transports: u16,
    // VPP: application.c:1179-1185. Only non-builtin applications with
    // private RX MQs enabled allocate these resources together.
    rx_mq_segment: Option<SvmFifoSegment>,
    rx_mqs: Vec<UnsafeCell<ApplicationRxMq>>,
}

impl Application {
    #[inline(always)]
    pub const fn index(&self) -> u32;

    #[inline(always)]
    pub const fn flags(&self) -> ApplicationFlags;

    #[inline(always)]
    pub fn name(&self) -> &str;

    #[inline]
    pub const fn namespace(&self) -> u32;

    // VPP: application_rx_mq_get, application.c:504-511. Queue index equals
    // VPP uses runtime thread index; Hammer's queue index is the Data Worker
    // slot because thread zero is not a dataplane consumer.
    pub fn app_rx_mq_queue(&self, worker: DataWorkerId) -> Option<&SvmMsgQ> {
        self.rx_mq_segment.as_ref()?.message_queue(worker.slot() as u32)
    }
}
```

Application 回调采用 VPP 的 per-application callback table，不把异构 owner 选择伪装成泛型
trait。`session-input` 已经有 application index，沿 `Application -> callbacks` 直接调用
owning plugin 注册的静态函数指针；调用仍在当前 Data Worker，同一事件不会绕到另一个 graph
node 或 external-app MQ（`application.h:119-129`、`session_input.c:80-204`）。TCP/UDP 的
协议操作则继续使用 `Transport<T>` trait；callback 和 trait 各自承担 VPP 中不同的调用边界。
不引入 `dyn`、provider registry 或第二套 application event dispatch。

```rust
// VPP: session_cb_vft_t, application_interface.h:15-75;
// application_t.cb_fns, application.h:127-129. Reuse the existing table;
// All 19 existing callback slots remain owner-defined. The migration keeps
// their current signatures until the core Session worker replaces the old
// runtime worker at this boundary.
pub struct ApplicationCallbacks {
    pub name: &'static str,
    pub add_segment: Option<fn(&mut SessionWorker, u64, u64) -> RuntimeResult<()>>,
    pub del_segment: Option<fn(&mut SessionWorker, u64, u64) -> RuntimeResult<()>>,
    pub accept: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub connected: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub disconnect: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub reset: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub transport_closed: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub cleanup: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub half_open_cleanup: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub migrate: Option<fn(&mut SessionWorker, u32, SessionHandle, u64) -> RuntimeResult<()>>,
    pub listened: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub unlistened: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub builtin_rx: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub builtin_tx: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub fifo_tuning: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub proxy_alloc_fifos: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub proxy_write_early_data: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub app_evt: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub crypto_async: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
}
```

现有 `SessionAppVft` 仅是同一 `ApplicationCallbacks` 的弃用名称；旧的 attach 后
`register_session_app` 路径仅保留兼容，不是本 ADR 新路径的验收前置条件。目标 attach
把 callbacks 与 Application 同步发布，避免事件先于回调注册。上述函数签名仍借旧 runtime
`SessionWorker`，迁入 ADR-0038 core worker 时必须保持 owner-worker 同步调用和 19 个
callback 的语义；不能只迁 accept/RX 就默默删掉其他槽位。

```rust
// VPP: foreach_app_options_flags, application_interface.h:196-215.
// These are semantic option flags, not Session error/retval numbers.
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationFlags: u32 {
        const ACCEPT_REDIRECT = 1 << 0;
        const ADD_SEGMENT = 1 << 1;
        const IS_BUILTIN = 1 << 2;
        const IS_TRANSPORT_APP = 1 << 3;
        const IS_PROXY = 1 << 4;
        const USE_GLOBAL_SCOPE = 1 << 5;
        const USE_LOCAL_SCOPE = 1 << 6;
        const EVT_MQ_USE_EVENTFD = 1 << 7;
        const MEMFD_FOR_BUILTIN = 1 << 8;
        const USE_HUGE_PAGE = 1 << 9;
        const GET_ORIGINAL_DST = 1 << 10;
        const EVT_COLLECTOR = 1 << 11;
        const NO_DUMP_SEGMENTS = 1 << 12;
    }
}

// VPP: application_alloc_and_init/vnet_application_attach,
// application.c:664-767,1112-1199; application_t.ns_index,
// application.h:119-151. The callback table is selected at attach, before
// the Application is published. Namespace is the existing pool index.
pub struct ApplicationConfig {
    pub namespace: u32,
    pub flags: ApplicationFlags,
    // VPP: app_init_args_t.name, application_interface.h:76-82. Attach moves
    // these bytes into Application before publishing the borrowed hash key.
    pub name: String,
    pub segment: SegmentManagerProperties,
    pub callbacks: ApplicationCallbacks,
}

// VPP: app_rx_mq_flags_t, application.h:103-107.
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationRxMqFlags: u8 {
        const PENDING = 1 << 0;
        const POSTPONED = 1 << 1;
    }
}

// VPP: app_rx_mq_elt_t, application.h:109-117. `next`/`prev` are the
// intrusive pending-list pointers; `file_index` is the FileMain registration
// index; queue storage is the existing SVM message queue.
pub struct ApplicationRxMq {
    next: Option<NonNull<ApplicationRxMq>>,
    prev: Option<NonNull<ApplicationRxMq>>,
    // VPP app_rx_mq_elt_t.mq points at the same thread index in
    // Application.rx_mq_segment; the immutable queue index is implicit.
    file_index: usize,
    application: u32,
    flags: ApplicationRxMqFlags,
}

impl ApplicationRxMq {
    #[inline]
    pub fn flags(&self) -> ApplicationRxMqFlags;
}
```

启用 private RX MQ 的非 builtin Application 才持有 `rx_mq_segment: Some(_)` 和按
Data Worker 固定长度的 `Vec<UnsafeCell<ApplicationRxMq>>`；否则 segment
为 `None` 且 Vec 为空。这两个字段在 attach/cleanup 时共同迁移，不额外引入包装类型。
`Application::app_rx_mq_queue` 返回跟本次 Application 借用同寿的 `&SvmMsgQ`。
`UnsafeCell` 仅供执行线程独占修改自己的 entry/link，不提供可跨线程借用的
`&mut Application`。`ApplicationMain.pending_rx_mq_heads` 是
每个 worker 的 intrusive-list head，entry 的 `next`/`prev` 与 head 都是非拥有指针。
VPP 的 add-tail 在空链表时令 entry 的两个链接指向自身，删除最后一个 entry 时将 head
及两个链接置空；多节点删除则改写相邻 entry 的链接和必要时的 head
（`application.c:332-370`）。这里的 `Option<NonNull<_>>` 只表达 C 的 null/non-null，
不是 `(application, worker)` 查找键，也不引入第二张索引表或 `Vec<Vec<u32>>` queue。

`rx_mqs` 在 attach 时先一次性建立 `worker_count` 个 entry，再登记 FileMain；首次可能
入链之后绝不增删、扩容或替换这个 Vec 的元素。Application 在 pool 中移动只会移动
Vec 头部，entry 所在的分配地址不变。每个 worker 只解引用自己链表里的指针；
FileMain readiness callback 和输入 node 在同一 Data Worker 上执行。detach 必须先阻止
新 readiness callback，按 VPP `app_rx_mqs_epoll_del` 清理 MQ、摘掉所有仍带 `PENDING`
标记的 entry 并注销 FileMain，再释放 MQ segment 和 `rx_mqs` 分配；不能让 head 或相邻
节点保留悬空指针（`application.c:428-453,488-503,793-807`）。跨线程销毁还须遵守
现有 WorkerBarrier/worker 所有权边界，不能在 worker 运行时从控制线程解引用链表。
`hammer-infra` 的 SVM MQ 和
FileMain eventfd/file registration 是唯一消息入口，不新增 mailbox 或 ring。
未启用 private RX MQ 时，`application_get_rx_mqs_segment` 返回 `SessionMain` 现有的
worker MQ segment，不为每个 Application 新建 segment；只有 private 分支的
`Application::app_rx_mq_queue` 返回私有队列（`application.c:567-577`）。

### 3.2 SegmentManager 与 FIFO ownership

`SegmentManager` 是 service 的协议无关 SVM resource owner。它不是 IP session 实例，也不是
TCP/UDP state；`SegmentManagerMain` 持有 manager pool，`AppWorker` 只持有 manager 的 `u32`
索引。VPP 的 segment pool、读写锁、owner worker、first-segment protection、event MQ 和 FIFO
allocation policy 均落在这个 owner 中（`segment_manager.h:15-80`、`segment_manager.c:389-488`）。

```rust
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::SvmMsgQ;
use hammer_infra::svm::ssvm::SsvmSegmentBackend;

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
    pub use_huge_page: bool,
    pub no_dump_segments: bool,
    pub n_slices: u8,
    pub segment_backend: SsvmSegmentBackend,
    pub max_fifo_size: u32,
    pub high_watermark: u8,
    pub low_watermark: u8,
    pub first_allocation_percent: u8,
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

// VPP: segment_manager_t, segment_manager.h:52-80. Rust owner mutation uses
// &mut self; no SpinLock<()> or separate state wrapper is introduced.
pub struct SegmentManager {
    segments: Pool<SvmFifoSegment>,
    owner_app_worker: u32,
    first_segment_protected: bool,
    // VPP segment_manager_t.event_queue is svm_msg_q_t*. The queue is owned
    // by the first SVM FIFO segment. Resolve it from that segment on borrow.
    event_queue_index: u32,
    flags: SegmentManagerFlags,
    max_fifo_size: u32,
    high_watermark: u8,
    low_watermark: u8,
}

// VPP: segment_manager_main_t, segment_manager.c:52-80. The service owns
// this pool and its defaults; an AppWorker stores only a raw manager index.
pub struct SegmentManagerMain {
    segment_managers: Pool<SegmentManager>,
    segment_name_counter: u32,
    defaults: SegmentManagerProperties,
    // VPP: segment_manager.c:25-69. CT custom segment contexts are service
    // SVM ownership, not plugin-session IP state and not statistics.
    custom_segment_contexts: Pool<CustomSegmentContext>,
    custom_segment_by_handle: HashMap<u64, u32>,
}

// VPP: custom_segment_t/custom_segments_ctx_t,
// segment_manager.c:25-46. Counts govern shared CT FIFO cleanup.
pub struct CustomSegment {
    client_sessions: AtomicU32,
    server_sessions: AtomicU32,
    context: u32,
    index: u32,
    segment: u32,
    client_detached: bool,
    server_detached: bool,
}

pub struct CustomSegmentContext {
    manager: u32,
    server_worker: u32,
    client_worker: u32,
    fifo_pair_bytes: u32,
    segments: Pool<CustomSegment>,
}

impl SegmentManagerMain {
    // VPP: segment_manager_main_init, segment_manager.c:1067-1082.
    pub fn init(properties: SegmentManagerProperties) -> Result<Self, SessionError>;

    // VPP: segment_manager_alloc/get/get_if_valid, segment_manager.c:389-421.
    pub fn allocate(
        &mut self,
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
    ) -> Result<u32, SessionError>;

    #[inline(always)]
    pub fn get(&self, manager: u32) -> Option<&SegmentManager>;

    pub fn free(&mut self, manager: u32) -> Result<(), SessionError>;

    // VPP: segment_manager_alloc_session_fifos_ct,
    // segment_manager.c:1495-1678. The Main owns the shared custom context
    // map; the CT record receives its segment/context identities here.
    pub fn allocate_cut_through_fifos<E, O>(
        &mut self,
        manager: u32,
        connection: &mut CtConnection<E, O>,
        thread_index: u32,
    ) -> Result<(SvmFifo, SvmFifo), SessionError>;

    // VPP: segment_manager_dealloc_fifos_ct, segment_manager.c:1306-1422.
    pub fn deallocate_cut_through_fifos(
        &mut self,
        rx_fifo: SvmFifo,
        tx_fifo: SvmFifo,
        client_side: bool,
    ) -> Result<(), SessionError>;
}

impl SegmentManager {
    // VPP: segment_manager_alloc/init, segment_manager.c:389-421.
    pub fn init(
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
    ) -> Result<Self, SessionError>;

    // VPP: segment_manager_event_queue, segment_manager.h:155-159.
    pub fn event_queue(&self) -> Option<&SvmMsgQ> {
        self.segments.get(0)?.message_queue(self.event_queue_index)
    }

    // VPP: segment_manager_alloc_session_fifos,
    // segment_manager.c:744-892. The pair is allocated before a Session is
    // attached to an established Session; listening and half-open Sessions
    // can exist without them (session_types.h:239-290).
    pub fn allocate_session_fifos(
        &mut self,
        thread_index: u32,
    ) -> Result<(SvmFifo, SvmFifo), SessionError>;

    // VPP: segment_manager_dealloc_fifos/dealloc_fifos_ct,
    // segment_manager.h:136-148 and segment_manager.c:892-934.
    pub fn deallocate_session_fifos(
        &mut self,
        rx_fifo: SvmFifo,
        tx_fifo: SvmFifo,
    ) -> Result<(), SessionError>;

    // VPP: segment_manager_init_free/segment_manager_free_safe,
    // segment_manager.c:524-584.
    pub fn mark_detached(&mut self);
    pub fn free(self) -> Result<(), SessionError>;
}
```

`SegmentManager` retains only the first segment's MQ index, not a long-lived borrow into its own
growable segment pool. This differs in representation from VPP's `svm_msg_q_t *`, but resolves the
same first-segment queue without copying it. Listening/half-open Sessions need no FIFO;
established Sessions require the pair. CT allocation/reclamation uses the custom context table,
custom segment pool and **client/server reference counts**, not the ordinary FIFO release path
(`segment_manager.c:1306-1422`, `:1495-1678`). Those counts are ownership accounting; only
`ct_main_t.n_sessions` is an omitted statistic.

The direct `Pool<SvmFifoSegment>` and `&mut self` signatures above are not yet a concurrency-complete
substitute for VPP's `segments_rwlock`: VPP's fast allocation holds a reader lock, its add-segment
path takes a writer lock and rechecks capacity, and custom contexts have a separate RW lock
(`segment_manager.c:760-892`, `:1608-1678`). Infra supplies a per-slice FIFO allocation/free
path: before worker launch the segment moves its private slices out as a `Vec`; each worker borrows
only its own `&mut FifoSlicePrivate` from the native Rust slice, whose FIFO storage is the existing
`Pool<Fifo>`. Shared allocation uses the segment's atomic bump index and the matching shared
`FifoSegmentSlice`; the old `&mut SvmFifoSegment` calls remain only for migration. Merely changing
the service field to `RwLock<Pool<_>>` would still serialize the fast path, and returning
`&SvmMsgQ` from a dropped guard would be unsound. Service must still add bounded publication and
VPP-style add-segment recheck before workers use this path. No worker may call the shown `&mut`
manager method through a global shared borrow; this remains an explicit service alignment gap,
not an implicit `UnsafeCell` or `SpinLock<()>` permission.

VPP 的 `fifo_segment_slice_t` 是共享 segment 中的 freelist/计数分区，
`fifo_slice_private_t` 则持有进程私有的 FIFO allocator 和 active RX 列表
（`third_party/vpp/src/svm/fifo_types.h:163-178`、`fifo_segment.c:833-852`）。
Hammer 已有的 `FifoSegmentSlice` 保持前者的共享职责；共享头存储显式
`slices_offset` 和 `n_slices`，映射时先验证范围、对齐，再构造 Rust
`&[FifoSegmentSlice]`，不使用 `slices[0]`/header 尾部推导地址的 C flexible-array
语义。后者继续以 `Pool<Fifo>` 持有私有 FIFO，不为并发分配另造存储容器。
对同一共享 slice 的 freelist 操作由借入唯一 `&mut FifoSlicePrivate` 的 worker
独占。`SvmFifoSegment: Sync` 只允许不同 worker 并发访问共享原子分配状态；
它不允许复制 private slice、跨 worker 借用同一个 `Pool<Fifo>`，也不替代 service
的 segment 生命周期同步。

### 3.3 AppWorker 与 SVM ownership

`AppWorker` 对齐 VPP `app_worker_t`，但所有 app/session exchange 使用仓库已有的
`hammer-infra` SVM FIFO/MQ。不得恢复旧 FIFO，不得引入第二种 ring 或 mailbox。真实的可选关系
才用 `Option` 表达；per-worker collection 直接使用 `Vec`，不能用 `Vec<Option<_>>` 伪造固定
worker/session table。

```rust
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_infra::svm::msg_queue::SvmMsgQ;
use hammer_infra::sync::SpinLock;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU8;

// VPP: app_worker_t, application.h:32-81; application_worker.c:15-134.
#[repr(C)]
pub struct AppWorker {
    cacheline0: CacheLineAlignMark,
    worker_index: u32,
    worker_map_index: u32,
    application: u32,
    // VPP app_worker_t.event_queue points into the first connects segment.
    // The index below locates its actual owner without a self-reference.
    connects_segment_manager: u32,
    listeners: std::collections::HashMap<SessionHandle, u32>,
    half_open: Pool<SessionHandle>,
    api_client: u32,
    app_is_builtin: bool,
    // VPP app_worker_t.wrk_evts and wrk_mq_congested,
    // application.h:70-74; Hammer has only Data Worker slots here.
    events_by_worker: Vec<UnsafeCell<Vec<SessionEvent>>>,
    // VPP: mq_congested is the count of congested runtime threads, changed
    // with relaxed atomic add/sub, not a process-wide boolean.
    mq_congested: AtomicU8,
    worker_mq_congested: Vec<UnsafeCell<bool>>,
    // VPP: detached_seg_managers_lock protects this list, application.h:76-80.
    detached_segment_managers: SpinLock<Vec<u32>>,
}

impl AppWorker {
    // VPP: application_alloc_worker_and_init, application.c:986-1074.
    pub fn init(
        application: u32,
        worker_map_index: u32,
        connects_segment_manager: u32,
    )
        -> Result<Self, ApplicationError>;

    // VPP: app_worker_t.event_queue, application.h:39-43;
    // segment_manager_event_queue, segment_manager.h:155-159.
    #[inline]
    pub fn event_queue<'a>(&self, managers: &'a SegmentManagerMain) -> Option<&'a SvmMsgQ> {
        managers.get(self.connects_segment_manager)?.event_queue()
    }

    // VPP: app_worker_add_half_open, application_worker.c:633-643.
    pub fn add_half_open(&mut self, handle: SessionHandle)
        -> Result<u32, ApplicationError>;

    // VPP: app_worker_own_session, application_worker.c:728-760.
    pub fn own_session(&mut self, session: SessionHandle)
        -> Result<(), ApplicationError>;

    // VPP: app_worker_add_event/app_worker_add_event_custom,
    // application_worker.c:934-967. Append and schedule on empty-to-nonempty;
    // congestion is handled later when session-input flushes the MQ.
    #[inline]
    pub fn add_event(&self, worker: DataWorkerId, event: SessionEvent);

    // VPP: app_wrk_flush_wrk_events and app_worker_flush_events_inline,
    // session_input.c:80-389.
    pub fn flush_events(&self, worker: DataWorkerId);

    #[inline]
    pub fn has_pending_events(&self, worker: DataWorkerId) -> bool;

    // VPP: app_worker_free, application_worker.c:48-134.
    pub fn free(self) -> Result<(), ApplicationError>;
}
```

`event_queue()` 借出 application 给外部 app 接收 control/IO event 的 SVM MQ；
`events_by_worker` 是 Session worker 到达 app worker 的 bounded event storage。两者含义不同，
但都复用现有 SVM/infra primitive。VPP 的 `wrk_evts`、`wrk_mq_congested` 长度是
`vlib_num_workers() + 1`，因为 VPP 的 thread 0 也运行图（`application_worker.c:24-29`）；
Hammer 只分配 `worker_count` 个 slot，执行线程用自己的 `DataWorkerId::slot()` 访问
`UnsafeCell`。`mq_congested` 是 VPP 的 `u8` 跨线程计数，用 relaxed 原子加减，
不是 `bool`（`application.h:65-74`，`application_worker.c:1005-1030`）。producer 在移交
event 后不再访问该 event；MQ full 或 try-lock 失败时 `session-input` 保留未发送事件，
设置相应线程的 congestion 标记，并在队列仍非空时再次调度。它们不是控制路径
`ApplicationError`，也不需要额外的 `ApplicationEventResult` 状态枚举
（`session_input.c:80-153`, `:340-389`）。

`events_by_worker` 复用 ADR-0038 的 `SessionEvent`；VPP 也用
`session_event_t` 同时承载这些 event families（`session_node.c`、`session_input.c:80-204`），
这里不再定义含相同事件的 `ApplicationEvent` 包装枚举。

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
    // VPP app_listener_t.ls_handle identifies whichever listening Session
    // was installed for this listener.
    handle: Option<SessionHandle>,
    worker_sessions: Vec<u32>,
}

impl ApplicationListener {
    #[inline(always)]
    pub const fn index(&self) -> u32;

    #[inline(always)]
    pub const fn handle(&self) -> Option<SessionHandle>;

    // VPP: app_listener_select_worker, application.c:172-185.
    #[inline]
    pub fn select_worker(&mut self) -> u32;

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
worker set. `select_worker` is called only after that set is nonempty; VPP asserts this condition
(`application.c:172-185`), so an empty set is an owner invariant, not a new recoverable error.
The last-worker path follows `app_listener_cleanup`: global `session_stop_listen` removes an IP
lookup only when the transport lacks `TRANSPORT_CONNECTION_F_NO_LOOKUP`, then stops the transport;
local CT scope removes its local-table entry before stopping the CT listener. The service listener
record itself performs neither IP lookup nor transport calls (`application.c:145-170`,
`session.c:1499-1520`). `opaque` belongs to the endpoint/listening Session, not `app_listener_t`
(`session.c:1467-1494`, `application.h:88-101`).

## 4. Session record and app/session relation

Application ownership is a relation on the existing generic `Session<O>`, not a new
`ApplicationSession` wrapper. The `O` parameter is the session opaque, corresponding to VPP's
`session_t.opaque`; it lets each concrete owner retain its own fact without `void *` or a universal
context type.

VPP 的 `session_t` 和 app-facing `app_session_t` 都保留可观察的 volatile state
（`session_types.h:239-290`、`application_interface.h:275-290`）。ADR-0038 的现有
`Session<O>` 已经用 `AtomicU8` acquire/release、`Option<SvmFifo>`、flags、listener/half-open
identity 和泛型 opaque 表达这些事实；这里不再定义第二版 Session，也不把 volatile
误写成 `read_volatile` 同步。需要补的只有 `session_t.app_wrk_index` 关系。

```rust
// VPP: session_t.app_wrk_index, session_types.h:264-271;
// app_worker_init_accepted, application_worker.c:493-520.
// Add this field to the existing ADR-0038 Session<O>, not a new Session type:
// app_worker_index: Option<u32>,
impl<O> Session<O> {
    #[inline(always)]
    pub fn app_worker_index(&self) -> Option<u32>;

    // VPP: app_worker_init_accepted/app_worker_init_connected,
    // application_worker.c:493-631. The owner worker has exclusive &mut.
    pub fn attach_app_worker(&mut self, app_worker_index: u32);

    // VPP: segment_manager_del_sessions_filter, segment_manager.c:716-743;
    // detached app sessions must not notify the former app worker.
    pub fn detach_app_worker(&mut self);
}
```

`SessionMain`/`SessionWorker` continue to own the pool and queue scheduling defined by ADR-0038/0039.
The application index is resolved through `AppWorker.application`, not copied into Session.
Listening/half-open Sessions can have no FIFO pair; established Sessions require both, exactly as
ADR-0038 already specifies. AppWorker/SegmentManager allocates the pair before the owning
`SessionWorker` attaches it. This ADR adds neither a second Session pool nor another event queue.

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
    // VPP: application_local.c:150-239. Client FIFOs are absent before
    // CT connect notification attaches the peer's FIFO pair.
    pub client_rx_fifo: Option<SvmFifo>,
    pub client_tx_fifo: Option<SvmFifo>,
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

// VPP: ct_worker_t, application_local.c:14-23. A worker mutates only its own
// slot. Hammer has no thread-zero dataplane CT worker.
pub struct CtWorker<E, O = u32> {
    pub connections: Pool<CtConnection<E, O>>,
    pub pending_connects: SpinLock<Vec<u32>>,
    pub pending_cleanups: Vec<CtCleanupRequest>,
    pub new_connects: Vec<u32>,
    pub have_connects: bool,
    pub have_cleanups: bool,
    // VPP ct_main_t.fwrk_pending_connects/fwrk_have_flush are written only
    // by the first worker; keep them with that worker's owned state.
    pub pending_by_worker: Vec<Vec<u32>>,
    pub flush_pending: bool,
}

// VPP: ct_main_t, application_local.c:25-36. Cumulative n_sessions is a
// VPP statistic and is intentionally not carried into this design. CtMain is
// a generic service shape; the concrete instance is owned by plugin-session.
pub struct CtMain<E, O = u32> {
    pub workers: Vec<UnsafeCell<CtWorker<E, O>>>,
    pub reusable_half_open: SpinLock<Vec<u32>>,
    pub first_worker: DataWorkerId,
    pub initialized: bool,
}

impl<E, O> CtMain<E, O> {
    // VPP: ct_main initialization and worker setup, application_local.c:25-38.
    // This constructs the generic service shape; it does not publish a global
    // instance and does not choose an IP endpoint.
    pub fn init(worker_count: u32) -> Result<Self, ApplicationError>;

    // VPP: ct_worker_get/ct_connection_alloc, application_local.c:40-86.
    // runtime identifies the executing worker; no other slot is lent out.
    pub fn worker_mut(&self, runtime: &mut DataPlaneMain) -> &mut CtWorker<E, O>;

    // VPP: ct_half_open_alloc/get/add_reusable, application_local.c:88-118;
    // ct_start_listen, application_local.c:477-510. Main/control operations
    // touch the first worker pool only while the WorkerBarrier holds it.
    pub fn allocate_half_open(&self) -> Result<u32, ApplicationError>;
    pub fn allocate_listener(&self) -> Result<u32, ApplicationError>;
    pub fn release_half_open(&self, connection: u32);

    // VPP: ct_accept_one and ct_accept_rpc_wrk_handler,
    // application_local.c:241-376.
    pub fn accept_pending(&self, runtime: &mut DataPlaneMain) -> Result<usize, SessionError>;

    // VPP: ct_session_connect_notify, application_local.c:149-239.
    pub fn connect_notify(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        error: Option<SessionError>,
    ) -> Result<(), SessionError>;

    // VPP: ct_program_cleanup/ct_handle_cleanups/ct_session_postponed_cleanup,
    // application_local.c:633-710.
    pub fn program_cleanup(&self, runtime: &mut DataPlaneMain, connection: u32)
        -> Result<(), SessionError>;

    // VPP: ct_session_close/ct_session_reset, application_local.c:726-764.
    pub fn close(&self, runtime: &mut DataPlaneMain, connection: u32)
        -> Result<(), SessionError>;
    pub fn reset(&self, runtime: &mut DataPlaneMain, connection: u32)
        -> Result<(), SessionError>;
}

impl<E, O> CtWorker<E, O> {
    // VPP: ct_connection_alloc/get/free, application_local.c:46-86.
    pub fn allocate_connection(&mut self) -> Result<u32, ApplicationError>;

    #[inline]
    pub fn connection(&self, connection: u32) -> Option<&CtConnection<E, O>>;
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

```

The `IpCtMain` instance is constructed inside `IpSessionMain::init` and is never stored in
`hammer-service`, `SessionMain`, `ApplicationMain`, TCP/UDP Main, or a second registry. No
`TransportConnection<()>`, `CtTransportConnection`, `IpTransportConnection` copy, or erased CT
instance is introduced.

VPP 把 CT listener 和 half-open pool 放在 thread 0（`application_local.c:477-510`），但
Hammer 的 thread 0 没有 Data Worker Session pool。因此 CT 的 listener/half-open 放在
`first_worker = DataWorkerId::new(0)` 的 CT pool；对应 listening Session 也在这一 Data
Worker 的 Session pool，handle 的 runtime thread index 是 1。main/control 需要同步建立或
移除 listener/half-open 时，在现有 WorkerBarrier 停住 Data Worker 后直接修改 first-worker
pool，不能借用 thread-zero `SessionWorker`，也不能在 worker 运行时从 `&self` 直接修改
`UnsafeCell`。这需要 ADR-0038 的 `SessionMain` 控制路径开放受 barrier 约束的 listener
pool 操作；仅有 `worker_mut(&mut DataPlaneMain)` 不足以完成同步 listen。实现前须批准这项
新 API，并保持 `CtMain` 和 `SessionMain` 在同一 barrier 范围内修改；否则此同步路径不能
视为已接线。运行中的 CT accept/connect/cleanup 只通过现有 Session worker event/RPC
路径到目标 worker，在 `worker_mut` 中独占对应 pool。

`CtWorker.pending_connects` 是 CT 跨线程入队的受锁临界区；生产者仅传 half-open index，
不借目标 worker 的 connection pool。`reusable_half_open` 锁保护回收 index，只有 first
worker 消费它并释放自己的 pool entry。`pending_by_worker`、`flush_pending` 只由 first
worker 访问；`pending_cleanups` 在 owner worker 重试，直到 SVM FIFO TX/event 条件允许
释放（`application_local.c:634-710`）。CT 不增加 mailbox、multi-ring queue、stats、IP
table entry、TCP/UDP node 或 callback table。

## 5. Listen/connect lifecycle and static plugin connection

### 5.1 Service operations

Service allocates identity and ownership but does not interpret an IP endpoint. The following methods
are the only service-side listener/session hooks required by plugin/session:

```rust
// VPP: vnet_listen/vnet_unlisten, application.c:1276-1320 and :1465-1493;
// app_worker_start_listen/stop_listen, application_worker.c:338-490.
impl ApplicationMain {
    pub fn allocate_listener(
        &self,
        application: u32,
        worker: u32,
    ) -> Result<u32, ApplicationError>;

    pub fn attach_listener_session(
        &self,
        listener: u32,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) -> Result<(), ApplicationError>;

    pub fn attach_listener_worker(
        &self,
        listener: u32,
        worker: u32,
    ) -> Result<(), ApplicationError>;

    pub fn detach_listener_worker(
        &self,
        listener: u32,
        worker: u32,
    ) -> Result<(), ApplicationError>;

    pub fn remove_listener(&self, listener: u32) -> Result<(), ApplicationError>;
}

// VPP: listen_session_alloc/session_alloc, session.c:244-304;
// session_listen, session.c:1467-1494. Pool mutation belongs to its worker.
impl<O> SessionWorker<O> {
    pub fn allocate_listening_session(
        &mut self,
        session_type: u8,
        opaque: O,
    ) -> Result<SessionHandle, SessionError>;

    pub fn attach_connection(
        &mut self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError>;

    pub fn allocate_connected_session(
        &mut self,
        session_type: u8,
        opaque: O,
        listener: Option<SessionHandle>,
    ) -> Result<SessionHandle, SessionError>;
}

impl<O> SessionMain<O> {
    // Main/control listener allocation is performed under WorkerBarrier;
    // the pool itself remains owned by the selected Session Worker.
    pub fn allocate_listening_session(
        &self,
        worker: u32,
        protocol: u8,
        opaque: O,
    ) -> Result<SessionHandle, SessionError>;

    pub fn attach_transport(
        &self,
        session: SessionHandle,
        protocol: u8,
        connection: u32,
    ) -> Result<(), SessionError>;

    pub fn remove(&self, session: SessionHandle) -> Result<(), SessionError>;

    // VPP: session_transport_closing_notify,
    // session_transport_closed_notify, session_transport_delete_notify,
    // session.c:964-1184. Cross-thread notification is queued to the owner;
    // this method never borrows another SessionWorker pool directly.
    pub fn transport_closing(&self, session: SessionHandle) -> Result<(), SessionError>;
    pub fn transport_closed(&self, session: SessionHandle) -> Result<(), SessionError>;
    pub fn transport_deleted(&self, session: SessionHandle) -> Result<(), SessionError>;
}
```

These service methods do not accept `SocketAddr`, FIB ids, IP family fields or a TCP/UDP connection
object. `connection_index` is only the numeric backlink already present in VPP's `session_t`; its
meaning is owned by the concrete transport. Listener allocation on the designated first Data
Worker is a main/control mutation under WorkerBarrier; connected-session allocation uses
`SessionMain::worker_mut` from ADR-0038 on the executing worker. A cross-worker call enqueues the
corresponding Session event instead of mutating another worker's pool.

### 5.2 Plugin-session composition

`IpSessionMain` owns the concrete IP composition and one instance of the generic service
`SessionMain<u32>`. It does not contain a second application pool or application callbacks. Its
global lifecycle is also only `init` and `global`; there is no publication method.

```rust
use std::sync::OnceLock;
use hammer_service::session::SessionHandle;

// VPP: app_listener_lookup/application listener lookup,
// application.c:87-143; session_lookup_add_session_endpoint is the concrete
// lookup step performed by app_worker_listen_sep, application_worker.c:230-322.
pub struct IpSessionMain {
    session: SessionMain<u32>,
    lookup: IpSessionLookup,
    transport: IpTransportMain,
}

static IP_SESSION_MAIN: OnceLock<IpSessionMain> = OnceLock::new();

impl IpSessionMain {
    pub fn init(config: IpSessionConfig) -> Result<(), SessionError>;

    #[inline(always)]
    pub fn global() -> Result<&'static Self, SessionError>;

    // VPP: INVALID_NS handling in session_types.h:541-543 and application
    // namespace lookup before app_worker_listen_sep, application_worker.c:230-322.
    // The index stays a raw u32; no ApplicationNamespaceIndex is introduced.
    pub fn validate_application_namespace(
        &self,
        application: u32,
        namespace: u32,
    ) -> Result<(), SessionError>;

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
    ) -> Result<(), SessionError>;

    pub fn remove_connection(
        &self,
        connection: &IpTransportConnectionId,
        expected: SessionHandle,
    ) -> Result<(), SessionError>;

    // Binds a transport-created listening endpoint to the service listener.
    // VPP stores the session endpoint in the family-specific lookup table.
    pub fn add_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError>;

    pub fn remove_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError>;

    // VPP: vnet_listen/vnet_disconnect_session, application.c:1277-1320,
    // :1518-1548. The concrete TCP/UDP Main is passed by the owning plugin.
    pub fn listen<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        endpoint: &IpSessionEndpoint,
    ) -> Result<SessionHandle, SessionError>;

    pub fn disconnect<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError>;
}
```

IP endpoint/table 操作也返回 service 已定义的 `SessionError`，不增加 `IpSessionError`。
VPP `SESSION_E_INVALID_NS`、`SESSION_E_NOINTF`、`SESSION_E_NOIP`、
`SESSION_E_NOROUTE`、`SESSION_E_ALREADY_LISTENING`、`SESSION_E_ADDR_NOT_IN_USE`
分别对应已有的 `InvalidNamespace`、`NoInterface`、`NoIp`、`NoRoute`、
`AlreadyListening`、`AddressNotInUse`（`session_types.h:519-576`）。
已登记 connection/listener 的 backlink 不匹配是 owner invariant，不包装成插件错误。

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
Their `Transport<IpTransportEndpointConfig>::start_listen/connect/stop_listen` methods remain
protocol operations (ADR-0038, VPP `transport.h:60-154`). A builtin caller follows the same
plugin-session listen/connect composition as any other application; TCP/UDP do not expose another
application-level `listen` API.

The concrete operation order is:

Before allocating anything, `vnet_listen` semantics are preserved: the caller supplies the raw
namespace index selected by `hammer-app`, `IpSessionMain::validate_application_namespace` verifies
that the application has an IP binding, then `IpSessionMain::lookup_listener` checks the concrete
endpoint identity and `ApplicationMain::listener_for_session` resolves the generic listener. An
existing listener owned by the same application only attaches another app worker; an existing listener
owned by another application returns the existing `SessionError::AlreadyListening`. A duplicate
worker on the same listener also returns that category (`application.c:1276-1320`,
`application_worker.c:338-349`).

For a new listener:

1. `ApplicationMain::allocate_listener` creates the generic app-listener record.
2. If local scope applies, the designated first Data Worker owns the CT listening Session and
   transport entry; main/control performs synchronous listener setup only while that worker is
   stopped by WorkerBarrier, then installs only the application-local lookup entry.
3. If global scope applies, it allocates the normal listening Session; TCP/UDP starts its transport
   through `Transport<IpTransportEndpointConfig>::start_listen`. Only if the resulting transport
   connection lacks `NO_LOOKUP` does `IpSessionMain` add the IP/FIB lookup identity.
4. `ApplicationMain::attach_listener_session` stores whichever local/global handles were created;
   each accepting AppWorker obtains its own listener SegmentManager and mapping for both scopes.

On failure, cleanup unwinds only resources actually installed: local lookup, conditional global
lookup, each started transport/listening Session, worker segment mapping, then application listener.
`session_stop_listen` removes a global lookup before stopping a transport unless `NO_LOOKUP` is set;
local CT lookup is removed explicitly before CT stop (`session.c:1499-1520`,
`application.c:145-170`). Cleanup errors remain observable without replacing the primary error.

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

### 5.4 Builtin application entry

VPP `vperf` 的 server/client Main 拥有各自的业务状态、per-worker session pool 和回调；它们
通过 `vnet_application_attach/detach`、`vnet_listen`、`vnet_connect`、
`vnet_disconnect_session` 使用 Session 子系统，而不是直接修改 `app_main_t` 或 CT/lookup
pool（`vperf_server.c:414-503`、`vperf_client.c:844-923,935-1025`）。VPP 没有独立的
`*_builtin` 操作族：`IS_BUILTIN` 是 attach flag，listen/connect/disconnect 仍是通用
Session 操作。Hammer 的 IP application 对外 attach/detach 入口属于
`hammer-plugin-session::IpSessionMain`；
`hammer-service` 只提供通用 Application/Session owner 操作，TCP/UDP 只提供具体
`Transport<IpTransportEndpointConfig>`。builtin app 本身仍是独立插件，不放到
`hammer-plugin-session` 或 `hammer-app`。

```rust
// VPP: vnet_application_attach/detach, application.c:1112-1219;
// vperf_server.c:420-485; vperf_client.c:854-925.
// Builtin is ApplicationConfig.flags.IS_BUILTIN, not another API family.
impl IpSessionMain {
    pub fn attach(&self, config: ApplicationConfig) -> Result<u32, SessionError>;
    pub fn detach(&self, application: u32) -> Result<(), SessionError>;

    // VPP: vnet_listen, application.c:1277-1320; vperf_server.c:490-528.
    // The default AppWorker is selected from the attached Application.
    pub fn listen<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        endpoint: &IpSessionEndpoint,
    ) -> Result<SessionHandle, SessionError>;

    // VPP: vnet_disconnect_session, application.c:1518-1548;
    // vperf_server.c:91-118,174-198. The Application must own the Session.
    pub fn disconnect<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError>;
}
```

`attach` 校验 namespace 的 IP binding，调用 service attach；builtin 调用方在
`ApplicationConfig.flags` 设置 `IS_BUILTIN`，plugin-session 不暗中替调用方改变身份。
`detach` 调用 service 的通用清理路径。这两个入口不增加 `BuiltinApplication` owner、
专用 dispatch 方法或 service 中的 IP API。VPP 的 builtin 默认 private segment，只有
显式设置 `MEMFD_FOR_BUILTIN` 才使用
memfd；`EVT_MQ_USE_EVENTFD` 仅在 memfd 情况有效；未指定 local/global scope 时默认
global（`application.c:674-708`）。`vperf` 在 local scope 时显式选择 memfd
（`vperf_server.c:445-451`）；Hammer 不能把“builtin”错误地等同于“永远没有共享
segment”。显式 detach 与 VPP `vnet_application_detach` 一样使用 main/control 清理路径；
`application_force_detach` 拒绝 builtin，不等于 builtin 不能主动 detach
（`application.c:861-879,1196-1219`，`vperf_server.c:470-485`）。

Builtin 的 listen/connect/disconnect 复用 5.1-5.3 的普通路径和已有
`Transport<IpTransportEndpointConfig>`；本节只把示例实际调用的通用 `listen` 和
`disconnect` 签名写出，不增设 `*_builtin` 同义方法。业务插件继续拥有自己的
Session opaque、FIFO 使用、协议策略和回调；`vperf_test.h:215-229` 的
`app_session_t` 缓存是 vperf 私有状态，不是新增 service Session 类型。

### 5.5 一个 builtin TCP server 的完整调用

这里以 `vperf` server 的 **一个 global-scope TCP data listener** 为例；它另外建立的
control listener 只是同一 `listen` 路径再调用一次，不是另一种 builtin API
（`vperf_server.c:490-528`）。`SessionMain::enable` 由 Session 生命周期在 graph
materialization 后执行，默认节点为 Disabled；它不是每个 app 的 attach 副作用
（ADR-0039 §9；VPP `session.c:2199-2248`）。下例的 `namespace` 已由通用 namespace
owner 创建，`tcp` 是 owning TCP plugin 的 Main，`segment` 是 app 选择的通用 FIFO/segment
配置；业务插件自身持有返回的 application index、listener handle 和 per-worker session
记录，service/plugin-session 不持有它的业务状态。

```rust
// VPP: vp_server_attach/vp_server_listen, vperf_server.c:420-528;
// vnet_application_attach/vnet_listen, application.c:1112-1199,1277-1320.
// Builtin plugin main/control path after Session enable, inside its own
// initialization operation returning Result<_, SessionError>.
let ip_session = IpSessionMain::global()?;
let application = ip_session.attach(ApplicationConfig {
    namespace,
    flags: ApplicationFlags::IS_BUILTIN | ApplicationFlags::USE_GLOBAL_SCOPE,
    name: "perf-server".to_owned(),
    segment,
})?;
let listener = match ip_session.listen(tcp, application, &endpoint) {
    Ok(listener) => listener,
    Err(primary_error) => {
        // VPP: vp_server_create, vperf_server.c:543-560.
        if let Err(cleanup_error) = ip_session.detach(application) {
            tracing::error!(application, ?cleanup_error, "builtin attach cleanup failed");
        }
        return Err(primary_error);
    }
};
// The owning builtin plugin retains application and listener.
```

正常路径按以下顺序完成，不经过 `appsl-rx-mqs-input`：

1. `attach` 按 name 检查重复、验证 namespace IP binding，service 创建 `Application`、默认
   `AppWorker`、connect SegmentManager 和 app event queue。`IS_BUILTIN` 令 segment 默认
   private；private RX MQ 只为非 builtin app 分配。VPP 来源：`application.c:1112-1185`、
   `:674-767`、`application_worker.c:15-44`。
2. `listen` 在 main/control + WorkerBarrier 下选择默认 AppWorker，更新 endpoint 的 namespace
   事实，建立 `ApplicationListener` 和 listening Session，令 TCP `Transport::start_listen`
   建立 transport listener；仅在未设置 `NO_LOOKUP` 时将 endpoint 放入 plugin-session 的
   IP lookup。返回 `SessionHandle` 给业务插件。VPP 来源：`application.c:1277-1320`、
   `application_worker.c:230-323`、`session.c:1467-1520`。
3. TCP accept 到某 Data Worker 后，service 为该 worker 分配 connected `Session`，按
   listener 选择 AppWorker，从其 listener SegmentManager 分配一对 SVM FIFO，并把
   `Accepted` 事件加入该 worker 的 AppWorker event list。VPP 来源：
   `application_worker.c:493-520,934-967`、`session_input.c:340-389`。
4. `session-input` 在同一 worker 处理 builtin 的 `Accepted`，调用 owning app 的 accept
   callback；`vperf` 将 Session 置 `READY`、保存 handle 和 RX/TX FIFO 指针、把业务 session
   index 写入 Session opaque。Hammer 的业务 worker 只保存自己的 Session handle/opaque，
   处理事件时从 owning `SessionWorker` 借用 FIFO，不把 pool 中的 FIFO 引用标为 `'static`。
   此时不会把该事件写到 external-app SVM event MQ。VPP 来源：
   `session_input.c:153-204`、`vperf_server.c:64-94`、`vperf_test.h:215-229`。
5. 后续 `RX` 事件由 `session-input` 直接送到该 builtin app 的 RX callback。业务代码从
   Session-owned RX FIFO 消费，向 TX FIFO 写入响应；TX 可用性和背压继续走已有 Session
   event queue/`session-queue`，不绕到 private app RX MQ。VPP `vperf` 在 TX FIFO 无空间
   时给 RX FIFO 设置 self-tap event 以重试，不丢弃未消费字节。VPP 来源：
   `session_input.c:80-155`、`vperf_server.c:327-378`、`vperf_protos.c:64-132`。
6. peer close/reset 触发该 app 的 disconnect/reset callback；业务插件以
   `(application, SessionHandle)` 调用同一 plugin-session 的通用 `disconnect`，由 service
   检查 owner、关闭 Session 并通过传入的 TCP `Transport` 处理 transport。停止 app 时，
   先对各 worker 尚存的业务 Session 执行这一步，再调用 `detach(application)`；detach
   释放 AppWorker/listener/segment/name，不能留下 IP lookup 或链表节点。VPP 来源：
   `vperf_server.c:91-118,174-198,470-485`、`application.c:771-814,1518-1548`。

```rust
// VPP: vp_server_wrk_cleanup_sessions, vperf_server.c:174-198;
// vnet_disconnect_session, application.c:1518-1548. Owning worker:
ip_session.disconnect(tcp, application, accepted_session)?;

// VPP: vp_server_detach, vperf_server.c:470-485;
// vnet_application_detach, application.c:1196-1219.
// Main/control, after every worker's cleanup has completed:
ip_session.detach(application)?;
```

`accepted_session` 只是 owning worker 当前处理的 handle；上述两句不表示 main/control
可以跨线程借用别的 worker 的业务 pool。VPP `vp_server_foreach_thread` 向每个 worker
发清理 RPC（`vperf_server.c:139-150,174-198`）；Hammer 使用现有 Session worker event
路径完成同样的 owner-thread 清理，等待完成后才在控制线程 detach。多个 disconnect
或 detach 失败时仍尝试剩余清理，保留主错误并单独报告 cleanup error，不用数字 retval。

步骤 4-5 使用 `Application.callbacks` 的直接回调，与 VPP
`application_t.cb_fns` 和 `session_input.c:80-204` 一致；builtin RX/TX 不经 external-app
event MQ。现有注册表已能按 application index 选择 callback，剩余实现工作是把它与
ADR-0038 的 `Session<O>`、AppWorker event list 和 attach 生命周期接通，不能以注册表已存在
代替端到端接线验证。

## 6. Event and node integration

### 6.1 Application RX MQ

`app_rx_mq` is the application-to-session ingress path, not a second application event queue. Only
a non-builtin Application with `use_private_rx_mqs` enabled owns one private SVM message queue per
Data Worker in `rx_mqs`. VPP also allocates thread-zero queue (`application.c:513-544`,
`:1179-1185`), but Hammer thread 0 does not consume dataplane messages. FileMain watches that queue's
eventfd and adds the entry to `ApplicationMain.pending_rx_mq_heads[worker.slot()]` through its
intrusive `next`/`prev` pointers. The input node drains the queue through the existing `SessionWorker` MQ
handler, then either clears the pending flag or requeues the entry with the postponed flag when the
SVM MQ still contains messages.

```rust
impl ApplicationMain {
    // VPP: app_rx_mqs_alloc, application.c:513-564; conditional call at
    // application.c:1179-1185. The segment and all per-worker SVM MQs are
    // allocated together only for non-builtin private-RX-MQ applications.
    pub fn allocate_rx_mqs(
        &self,
        application: u32,
        worker_count: u32,
    ) -> Result<(), ApplicationError>;

    // VPP: app_rx_mq_fd_read_ready, application.c:428-453. FileMain calls
    // this on the queue's assigned worker.
    pub fn rx_mq_ready(
        &self,
        application: u32,
        worker: DataWorkerId,
    ) -> Result<(), ApplicationError>;

    // VPP: appsl_rx_mqs_input_node, application.c:374-426. A non-empty MQ
    // remains pending and causes the input node to be interrupted again.
    pub fn drain_rx_mqs(
        &self,
        worker: DataWorkerId,
    ) -> Result<usize, ApplicationError>;

    // VPP: application_enable_rx_mqs_nodes, application.c:580-587. The
    // default state is Disabled; enabling is explicit Session lifecycle/config.
    pub fn enable_rx_mq_node(&self, enabled: bool) -> Result<(), ApplicationError>;
}
```

`rx_mq_ready` and `drain_rx_mqs` select only `pending_rx_mq_heads[worker.slot()]` and
`rx_mqs[worker.slot()]` through their owner-confined `UnsafeCell`; neither requests `&mut` of the
global Main from a FileMain callback. The vectors are built for `worker_count` before workers
launch. VPP uses `vlib_num_workers() + 1` because its thread 0 executes the graph
(`application.c:513-544`, `application_worker.c:24-29`); Hammer does not. A received
`SessionHandle.thread_index` is mapped once through the existing
`DataWorkerId::try_from(thread_index)` before selecting the MQ. Index 0 is rejected, not silently
treated as the first queue.
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
    -> session-input event flush
    -> Application event_queue (SVM MQ)
```

VPP evidence is `application.c:374-426` for `appsl-rx-mqs-input`, `session_input.c:80-153` and
`:350-404` for worker event flushing, and `session_node.c:1858-1917` for Session IO event dispatch.
Transport notifications do not pass through `appsl-rx-mqs-input`.
`session-queue-process` keeps the one-second fallback and `session-queue-main` keeps the pre-input
wrapper from ADR-0039 (`session_node.c:2238-2307`). None of these nodes owns an Application pool,
transport callback, IP lookup table, or a new scheduler object.

The `appsl-rx-mqs-input` and session queue nodes are disabled by default, matching
`VLIB_NODE_STATE_DISABLED` (`application.c:422-426`) and ADR-0039. Application attach creates
private RX MQ resources and FileMain registrations only under the condition above; it does not
enable either node. Session enable/disable toggles `appsl-rx-mqs-input` only when
`use_private_rx_mqs` is configured (`session.c:2243-2246`, `application.c:575-587`).

## 7. Error semantics

The Session operation owner defines `SessionError` in `hammer-service::session`; plugin-session
reuses it for IP endpoint/table operations, while TCP/UDP retain only their protocol-local errors.
Internal APIs never return a numeric
retval and never expose VPP's negative enum representation. This ADR reuses the already implemented
`hammer-service::session::SessionError` (`error.rs:28-110`) and
`hammer-service::session::ApplicationError` (`application.rs`). It does not define a
second incompatible `SessionError`, `ApplicationError`, or `SegmentManagerError`. For the
plugin-session application boundary, the existing `SessionError` gains concrete
`ApplicationAttach { source: ApplicationError }` and
`ApplicationDetach { source: ApplicationError }` variants so resource and cleanup failures keep
their original source. `AlreadyAttached` and `Missing` map to the existing
`ApplicationAttached` and `NoApplication` categories. This follows VPP's distinct
`vnet_application_attach`/`vnet_application_detach` outcomes
(`application.c:1112-1219`) without exposing numeric retval internally.

```rust
// VPP: foreach_session_error, session_types.h:519-576. Reuse the existing
// service variants NoSession, Owner, SegmentNoSpace, NewSegmentNoSpace,
// SegmentCreate and TransportNotRegistered; do not redefine this enum.
use hammer_service::session::SessionError;

// VPP: application.c:513-587. Reuse existing owner-local MQ/FileMain errors
// such as MqSegmentCreate, MqSegmentExhausted, MqInstall and MqDetachFailed.
use hammer_service::session::application::ApplicationError;
```

`SessionError::SegmentNoSpace` and `NewSegmentNoSpace` are distinct, matching VPP's
`SESSION_E_SEG_NO_SPACE` and `SESSION_E_NEW_SEG_NO_SPACE` (`session_types.h:537-545`).
`ApplicationError` retains current owner-local MQ/file/attach failures; VPP-style application
lifecycle boundaries translate once into the existing `SessionError` type, preserving resource
failures through the two source-bearing variants. No new error family, numeric discriminant,
`code()` method, `repr(i32)`, or second code table is introduced. `Ok(())` represents
`SESSION_E_NONE`.

Application event queue congestion retains pending worker events for retry; it is not an
`ApplicationError`. An invalid pool handle, a missing promised listener/session, or a broken
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
| `ApplicationMain::lookup_name` | no inline annotation | VPP `application_lookup_name`, `application.c:321-330`; hash lookup is an ordinary function |
| `Application::app_rx_mq_queue`, `SegmentManager::event_queue` | no inline annotation | VPP `application_rx_mq_get`, `application.c:504-511`, and `segment_manager_event_queue`, `segment_manager.h:155-159`, are ordinary functions |
| `Application::name` | `#[inline(always)]` | borrow Application-owned immutable name, VPP `application.h:119-151` |
| `Session::load_state`, `store_state`, `opaque`, `opaque_mut` | `#[inline(always)]` | ADR-0038 `Session<O>` accessors; VPP `session_set_state`, `session.h:909-914`, and direct state reads in `session_node.c`/`application_worker.c`; Rust uses atomic acquire/release |
| `Session::rx_fifo`, `tx_fifo` | `#[inline(always)]` | direct FIFO pointer/value access after segment allocation, `segment_manager.c:858-892` |
| `CtConnection::peer_session`, `actual_transport`, `CtWorker::connection` | `#[inline]` | VPP `ct_session_get_peer`/`ct_session_endpoint` and `ct_connection_get`, `application_local.c:67-75,120-140` |
| `AppWorker::add_event` fast enqueue check | `#[inline]` | VPP `app_worker_add_event`, `application_worker.c:934-967` |
| control-message enqueue helper | `#[inline(always)]` only for the bounded raw MQ write | VPP `app_wrk_send_ctrl_evt_inline`, `application_worker.c:969-1001` |
| `IpSessionMain::attach/detach/listen/disconnect` | no inline annotation | VPP ordinary application/session lifecycle calls in `vperf_server.c:420-528`, `:91-118`, and `application.c:1112-1320,1518-1548` |
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
| `SessionAppVft` name and attach-after-publication `register_session_app` | the same `ApplicationCallbacks` table supplied to attach and stored on `Application`; old callers are outside this ADR's new-path acceptance | VPP `application_t.cb_fns`, `application.h:127-129`; `vnet_application_attach`, `application.c:1112-1199`; direct builtin calls `session_input.c:80-204` |
| `TransportVft`, function-pointer protocol registry and `dyn Transport` | service `Transport<T>` trait implemented directly by concrete TCP/UDP Main | ADR-0038 static trait decision; VPP transport operations `transport.h:60-154` |
| `TransportConnection<()>`, `CtTransportConnection`, or a second common transport base | existing `TransportConnection<E, O>` plus generic `CtConnection<E, O>` extension, concretized as `IpCtConnection` in plugin-session | ADR-0038 `TransportConnection<E, O>`, VPP `ct_connection_t`, `application_local.h:31-45` |
| `ApplicationWorkerState`/`Box<[ApplicationWorkerState]>` and a separate pending queue | `ApplicationMain.pending_rx_mq_heads: Vec<UnsafeCell<Option<NonNull<ApplicationRxMq>>>>` plus pointer `ApplicationRxMq.next/prev`, one head per Data Worker | VPP `appsl_wrk_t`/`app_rx_mq_elt_t`, `application.h:103-117`, `:196-203`; Hammer runtime does not run thread-zero dataplane |
| old FIFO, `AppRing`, `multi_ring_msg_queue`, mailbox or dispatch queue | existing `hammer-infra` SVM FIFO/MQ and `appsl-rx-mqs-input` | VPP `application.c:374-426`, `:513-587` |
| CT statistics such as `ct_main_t.n_sessions` | no stats field in `CtMain` | VPP `application_local.c:25-36`; stats are outside this ADR |

These deprecated surfaces may remain temporarily for migration compilation, but no new consumer may
depend on them. The implementation deletes each compatibility path after its callers move; this ADR
does not preserve a second dispatch path.

## 10. Migration and non-decisions

1. Keep the existing service `ApplicationMain`, `AppWorker` and SVM FIFO/MQ as the migration starting
   point; rename the existing callback table to `ApplicationCallbacks` and move its installation
   into attach before publishing the Application. Move only IP/transport facts into
   `IpSessionMain`; its generic attach/detach API is the plugin-facing IP composition boundary.
2. Reuse the existing service `SessionError` and `ApplicationError` at their owner boundaries;
   do not add another `SegmentManagerError`, numeric internal `SESSION_E_*` returns, `ApiError`,
   `error_codes!`, code accessors or a universal boxed error.
3. Remove any duplicate application/listener pool or application callback storage from
   `hammer-plugin-session`; its only application-facing state is a raw `u32` listener/application
   identity passed to service APIs.
4. Keep namespace records and generic namespace CRUD under ADR-0036: `hammer-app` owns the generic
   namespace and supplies raw `u32`; this ADR adds that index to `Application` and validates its IP
   binding through `IpSessionMain`. Namespace socket work remains a later ADR. Do not add
   `ApplicationNamespaceIndex`, FIB fields, `ip4_fib_id + ip6_fib_id`, socket handles or a binding
   object to `Application`/`ApplicationListener`.
5. Do not use `Vec<Option<WorkerState>>` for fixed worker state. A nullable intrusive-list head
   (`Vec<UnsafeCell<Option<NonNull<ApplicationRxMq>>>>`) is a real empty-list state. Do not add a new mailbox,
   `multi_ring_msg_queue`, `SessionRuntimeEngine`,
   `SessionScheduler`, `publish` APIs, generic provider traits, `dyn` dispatch, or TCP/UDP Session
   nodes.
6. Do not change `hammer-ipc` ownership. Binary API declarations remain in the owner plugin and IPC
   remains the transport/codec capability as established by earlier ADRs.

## 11. Verification

The implementation phase must verify the boundary with focused compile/runtime tests rather than source
text assertions:

- application attach/detach allocates and releases exactly one AppWorker MQ set and listener pool;
- private RX MQ, AppWorker event vectors and CT pools have exactly `worker_count` entries; thread 0
  has none, and a SessionHandle thread index maps to `DataWorkerId::slot()` before MQ access;
- private RX MQ pending-list insertion/deletion preserves self-links for the sole entry and
  neighbor links for multiple entries; detach removes a pending entry before freeing its fixed
  vector allocation, leaving no dangling head or neighbor pointer;
- first-worker CT listener/half-open mutations occur only under a held WorkerBarrier, while
  accept/connect/cleanup mutate only the executing Data Worker's CT pool;
- name lookup uses the original bytes through attach, duplicate-name rejection, hash-table growth,
  detach and pool-slot reuse; after detach no stale pointer remains in the name table;
- first listener worker creates the listening Session and concrete transport listener; a second worker
  only attaches its listener segment and worker mapping;
- accepted, connected, reset, closed and deleted transport notifications reach the owning AppWorker
  through the existing SVM event path;
- listener cleanup removes IP lookup identity before freeing the Session and application listener;
- a failed transport or FIFO allocation leaves no application listener, Session lookup entry or
  segment manager behind;
- service builds without a dependency on `hammer-plugin-session`, TCP or UDP.
- independent builtin client/server plugins attach through `IpSessionMain` with `IS_BUILTIN` and
  reuse ordinary listen/connect/disconnect with their own state and callbacks; builtin RX/TX never enters the
  external-app SVM event-MQ flush, and callback routing is tested before the old table is removed.
- one global-scope builtin TCP server completes enable -> attach -> listen -> worker accept/FIFO ->
  direct builtin RX -> disconnect on the owner worker -> detach, including listener failure rollback
  and a close while RX/TX events remain pending.
