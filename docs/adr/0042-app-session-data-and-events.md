# ADR-0042: AppSession 与 Session 事件

- 日期：2026-09-27
- 状态：Proposed；仅设计，不改 Rust 代码
- 前置：ADR-0038、ADR-0039、ADR-0040
- 范围：server/builtin app、Session worker MQ、iperf3 TCP、通用 datagram I/O 契约
- 不包含：VCL、外部 client/socket、Binary API、TCP packetization、UDP app 接线

## 源码依据

完整核对 `third_party/vpp/src/vnet/session/application_interface.h`（1-987）和
`application_interface.c`（1-233）：

- `.h:15-76` 是 application 回调声明；`.h:78-264` 是 attach/listen/connect 等
  control 参数与选项，不是应用数据记录。
- `.h:266-290` 定义 `app_session_transport_t` 与 `app_session_t`：后者有 RX/TX
  FIFO、session type、volatile app-side state、**应用池** session index、transport
  地址事实、目标 Session worker MQ 和 datagram 标志。
- `.h:292-556` 是 control MQ 的具体消息；`.h:558-629` 分配 control ring 或向
  IO ring 放 `event_type + session_index`，非阻塞时区分锁忙和 ring 满。
- `.h:632-818` 是 app data path：stream 允许部分入队；datagram header+payload
  作为一个完整记录入队；TX FIFO event bit 抑制重复通知；接收完整 datagram
  后才消费，stream 可以 peek 或 dequeue。
- `.h:830-987` 是错误格式化、socket API 消息和 endpoint 扩展配置；不是
  `AppSession` 的另一套数据 API。
- `.c:1-233` 只实现 URI 解析、`vnet_bind_uri`、`vnet_unbind_uri`、
  `vnet_connect_uri`：解析成 endpoint 后交给已有 listen/connect。它没有
  `AppSession` 构造、数据读写或 `SessionEvent` 分发。此 ADR 不从 `.c` 发明
  app lifecycle 或额外调度层。

配套核对：`session_types.h:240-283,385-492` 是 server `session_t` 和
`session_event_t`；`session.c:26-143` 是 `session_program_*`；
`session_node.c:1858-1915,1950-2005,2033-2140` 消费 worker MQ；
`session_input.c:103-332` 将通知交给 AppWorker。VPP vperf 的实际拥有关系在
`plugins/vperf/builtin/vperf_builtin.h:75-110,127-134`、
`vperf_test.h:215-229`、`vperf_server.c:60-93`：**每 worker 的 vperf pool
持有完整的 `vp_test_session_t`；`session_t.opaque` 回指该 app pool index，
完整记录另存 server Session handle。**
`vperf_builtin.h:75-110` 在 `vp_test_session_t` 开头展开与
`application_interface.h:275-290` 相同的 `foreach_app_session_field`，后面才是
vperf 私有字段。`vppinfra/cache.h:28-29` 的 cache-line mark 是零长度对齐字段；
`vperf_test.h:215-229` 和 `vperf_protos.c:105-121` 的 cast 只是借用同一记录的
公共前缀，既不复制也不构造另一个 `app_session_t`。`app_recv_stream` 只访问
`rx_fifo`，`app_send_stream` 只访问 `tx_fifo/vpp_evt_q`
（`application_interface.h:724-745,795-810`）；它们不访问 vperf 私有字段。
这使 vperf 可以保持一条 worker-local pool 记录，同时复用通用 app I/O。
Rust 用字段组合表达这层借用，不复制 C 的前缀 cast 或布局依赖。
`vperf_test.h:215-229` 的 `vperf_app_session_init_` 直接把
`s->rx_fifo`、`s->tx_fifo` 和
`session_main_get_vpp_event_queue(s->thread_index)` 写入 app record。
`session.h:82-86,356-359` 表明事件队列属于目标 `session_worker_t`；
`session.c:1772-1787` 只是用 `session_main.wrk_mqs_segment` 为每个 worker
分配队列，再把队列指针交给该 worker。`Fifo::duplicate` 是 Hammer infra
已有的共享 SVM header、复制进程内私有状态的操作，不等价于上述 VPP
同进程的 FIFO 指针借用，本 ADR 不在 app accept 路径使用它。

## 所有权和边界

```text
hammer-service::session
  Session/O: server Session 和 FIFO 生命周期 owner
  SessionWorker: 该 worker 的事件队列绑定和消费 owner
  SessionMain: worker 集合和 MQ segment 存储 owner，不代表各队列的逻辑 owner
  ApplicationMain, AppWorker: 应用注册、listener、回调通知 owner
  AppSession<T>: 共享 server Session 的 FIFO、目标 worker 的 MQ；可选 T transport；不持有业务会话状态
  SessionEvent: SessionWorker MQ/list、AppWorker 事件列和 app MQ 的同一事件记录

hammer-plugin-session
  IP endpoint 的实例；IP datagram header 的具体字段解释

hammer-plugin-tcp/udp
  只用 Session/Transport 网络侧 API 和 service 的 program_*；不持有 app record

hammer-plugins/app/iperf3
  Iperf3Worker::sessions: Pool<Iperf3Session>
  Iperf3Session 内嵌 AppSession<AppSessionTransport>，并保存 parser/role/counters
```

`Iperf3Session` 是 **app pool 的持久记录**，内嵌的 `AppSession` 是同一记录的
通用应用侧部分，不是每次 callback 新建的 `&Session` wrapper。它共享 RX/TX
`Fifo`，不拥有 Segment Manager 中 FIFO 的分配/归还权。VPP
`vp_test_session_alloc` 把 pool 指针差写进
`es->session_index`，server accept 再把它写进 `s->opaque`
（`vperf_builtin.h:127-134`、`vperf_server.c:72-81`）。Rust `Pool::insert`
已直接返回该 index；iperf 只需将返回值写进 `Session.opaque`，不在
`AppSession` 再存一份。`AppSession.session_handle` 指 service Session。
`AppSession.state` 是
VPP app-side state，不替代 `Session.state` 的 server 生命周期；不能把两者
合成同一引用。

VPP `app_session_t` 指向同一对 `svm_fifo_t`；Rust 的 server `Session` 和
`AppSession` 用 worker 本地 `Rc<Fifo>` 持有**同一个** FIFO 对象，不调用
`Fifo::duplicate`。RX 的唯一 consumer 是 app，TX 的唯一
producer 是 app；相反方向仍由 server Session/transport 使用。同一 FIFO 的
两端并发访问遵循 infra 的单生产者/单消费者契约。

`SessionWorker.sessions` 的 pool 可增长并移动 `Session`；`Rc<Fifo>` 的分配
地址不随 pool 增长移动。service 在 application cleanup callback 删除 app
record 后才删除 server Session，并在归还 FIFO 给 Segment Manager 前断言
`Rc` 只有 server owner。`Fifo` 不实现 `Sync`，所以 `Rc` 不授权跨 worker
访问；`SessionMain` 和 `Iperf3Main` 的 worker slot 仍遵守固定 worker 独占
契约。没有 `'static` FIFO 引用、裸指针或第二份 FIFO 对象。

worker MQ 从目标 `SessionWorker` 的队列绑定取得。`SvmFifoSegment` 用
`Arc<SvmMsgQ>` 持有进程本地 MQ 对象，app record 克隆同一对象的句柄；
共享消息数据仍在 SVM 映射中。segment 负责队列存储，worker 负责队列
身份、消费和调度；队列先于 app record 创建、后于它销毁。

通用 app I/O 不需要知道 `Iperf3Session` 的具体类型，service 不以业务状态为
`AppSession` 参数；plugin 也不把完整业务状态交给 service 拥有。
`AppSession<T>` 不使用 `repr(C)` 或前缀 cast。`transport: Option<T>` 属于同一个
app record，具体 IP 类型只在 plugin-session 定义；service 只知道 `T`。
VPP 的 TCP vperf 初始化绑定
FIFO/MQ，`is_dgram` 保持 `pool_get_zero` 后的 false，未使用的 datagram 地址
字段保持零值（`vperf_test.h:215-229`）；Rust 用 `None` 表示 TCP 未初始化
的 datagram transport，显式保留 `is_dgram: bool`，
由 service Session 的 transport mode 在构造时决定，TCP 不额外传 `false`，
也不为它引入 `E = ()` 或 IP 字段。
`is_dgram` 是通用 FIFO 记录模式，不是 IP 地址；后文给出对应的 datagram
方法。service 只处理 VPP `session_dgram_pre_hdr_t` 的长度/偏移以及整记录
FIFO 语义。IP 地址/端口的具体 `T` 由 plugin-session 定义，service 只按值
保存；GSO 是单次 datagram 发送参数，不属于持久 transport。
`T` 表达 app-side transport 状态，不是 `SessionEndpointConfig<T>` 的请求配置；
service 不引入 IP 地址族/FIB。
本 ADR 不把这些 session/application 类型放进 runtime 来迎合后续 VCL。

## AppSession 目标类型和方法

代码块是接口设计，不是已实施代码。只保留当前 TCP 所需的通用 record 和数据
方法；`SvmMsgQ`、`SessionHandle`、`SessionError` 等复用现有类型。
下列签名只做 Rust 必需的形状变换：`self` 对应 VPP 原型的记录参数，
切片合并指针与长度，`bool` 对应 `u8` 标志。业务输入的顺序不变；
不传任何全局 Main。VPP 没有对应函数的纯字段 getter 不列为 API。

```rust
// VPP: session_types.h:496-500, session_dgram_pre_hdr_t.
// Native-endian lengths in the shared FIFO, not a network packet header.
#[repr(C)]
#[derive(KnownLayout, FromBytes, Immutable, IntoBytes)]
struct SessionDatagramPrefix {
    data_length: u32,
    data_offset: u32,
}

// VPP: application_interface.h:275-290, app_session_t;
// vperf_test.h:215-229, common fields used by vperf TCP.
// T is the plugin-owned app-side transport value, not endpoint configuration.
pub struct AppSession<T> {
    rx_fifo: Rc<Fifo>,                   // same FIFO object as the server Session
    tx_fifo: Rc<Fifo>,
    session_handle: SessionHandle,       // server Session identity
    session_type: u8,
    state: AtomicU8,                     // app-side state, not Session.state
    pub transport: Option<T>,            // None for TCP; Some for datagram
    event_queue: Arc<SvmMsgQ>,           // target SessionWorker's event queue
    is_dgram: bool,                      // VPP app_session_t.is_dgram
}

impl<T> AppSession<T> {
    // VPP: vperf_test.h:215-229, vperf_app_session_init_(as, s).
    // Self is the initialized `as`; session is the sole explicit input `s`.
    // The owning plugin fills transport for datagram Sessions before Ready.
    #[inline]
    pub fn new(session: &Session) -> Self;
}

impl<T> AppSession<T> {
    // VPP: application_interface.h:595-628, app_send_io_evt_to_vpp
    // (mq, session_index, evt_type, noblock). Self holds mq and session_index.
    #[inline]
    pub fn send_io_event(
        &self, event: SessionEventType, noblock: bool,
    ) -> Result<SessionEventEnqueue, SessionError>;

    // VPP: application_interface.h:739-744, app_send_stream(s, data, len, noblock).
    // Self is s; the slice combines data and len.
    #[inline(always)]
    pub fn send_stream(&self, data: &[u8], noblock: bool) -> Result<usize, SessionError>;

    // VPP: application_interface.h:804-810, app_recv_stream(s, buf, len).
    // Self is s; the mutable slice combines buf and len.
    #[inline(always)]
    pub fn recv_stream(&self, out: &mut [u8]) -> usize;
}

impl<T: Default + FromBytes + IntoBytes + Immutable> AppSession<T> {
    // VPP: application_interface.h:682-691, app_send_dgram_raw_gso
    // (f, at, vpp_evt_q, data, len, gso_size, evt_type, do_evt, noblock).
    // Self supplies f, at (= transport), and vpp_evt_q; the slice combines
    // data and len. Service does not inspect T's protocol fields.
    // None means the entire record did not fit. Some(0) is a sent empty dgram.
    #[inline(always)]
    pub fn send_dgram_raw_gso(
        &self, data: &[u8], gso_size: u16,
        event: SessionEventType, do_event: bool, noblock: bool,
    ) -> Result<Option<usize>, SessionError>;

    // VPP: application_interface.h:654-679, app_send_dgram_segs_raw
    // (f, at, vpp_evt_q, segs, nsegs, seg_len, evt_type, do_evt, noblock).
    // Self supplies f, at (= transport), and vpp_evt_q; the slice combines
    // segs and nsegs.
    // segment_len includes the datagram header, as VPP's seg_len does.
    #[inline(always)]
    pub fn send_dgram_segments_raw(
        &self, segments: &[&[u8]],
        segment_len: usize, event: SessionEventType, do_event: bool, noblock: bool,
    ) -> Result<Option<usize>, SessionError>;

    // VPP: application_interface.h:754-785, app_recv_dgram_raw
    // (f, buf, len, at, clear_evt, peek). Self supplies f and mutable
    // at (= transport); the slice combines buf and len.
    // Returns payload bytes copied. None means the next record is incomplete.
    #[inline(always)]
    pub fn recv_dgram_raw(
        &mut self, out: &mut [u8], clear_event: bool, peek: bool,
    ) -> Result<Option<usize>, SessionError>;
}
```

`AppSession::new` 只接收 server Session，对齐
`vperf_app_session_init_(app_session_t *, session_t *)`。构造时从 Session
owner 取得 FIFO 与目标 SessionWorker 已绑定的 MQ；不能要求业务回调
另传 `event_queue` 或 `is_dgram`。队列的存储仍由 SessionMain 的 MQ segment
持有，队列的身份与消费仍属于目标 SessionWorker；热路径不重复查询 Main。VPP 的
`vp_test_session_alloc` 先 `pool_get_zero`，`vperf_app_session_init_` 随后只绑定
FIFO/MQ（UDP 才复制 transport 地址）；`AppSession::new` 以 `None`
表达尚未初始化的 datagram transport。具体应用在 UDP accept 中通过 plugin-session 的
`From<IpTransportConnectionId>` 取得 `T`，赋给 `app.transport`，
并在发布 `Ready` 前完成；TCP 保持 `None`。
因此 app-side `session_state` 起始为
`Created`，**不是**从已置为 `Ready` 的 server `Session.state` 复制。
Rust 显式初始化每个实际保留的字段；`session_type` 从 server Session 读取，
不保留 VPP vperf 未使用的零值。缺少 FIFO/MQ 是 accept 所有权契约被破坏，
在 owner 内断言，不伪装成 VPP 的可恢复 session error。

`AppSession` 内部保留同一对 FIFO 的共享句柄，只供通用 app 收发使用；
它不把 infra FIFO 方法再次包装为 `available_rx/tx`、`readable_segments`
或 `consume_rx`，也不公开可绕过 TX event 发布的 FIFO 写入口。
VPP `vperf_protos.c:67-112,143-155` 在协议 RX 内层从参数 `session_t *s`
直接取 `s->rx_fifo/s->tx_fifo`。iperf 同样从 service `Session` 借用这两个
FIFO，直接调用 `Fifo::max_dequeue/max_enqueue`、
`readable_segments`、`drop_dequeue` 和 `needs_deq_notification`；这些操作的
含义和调用时机属于 iperf 的 RX/背压逻辑。`ControlParser::inspect` 只接收
`&Fifo`，不接收整个 `AppSession`。借出的可读段不能越过 FIFO 借用；
调用方提供非空输出切片，已验证范围内的 segment 错误是 FIFO owner
不变量。通用 `AppSession` 只实现 VPP `app_recv_stream/app_send_stream` 和
datagram 的完整记录收发，承担应用侧收发与 TX event/MQ 语义，不替
业务协议做容量预检、分段窥读、丢弃或 transport RX 通知。
`send_stream(data, noblock)` 使用共享的 TX FIFO 及所存 MQ，Rust 切片同时
表达 VPP 的 `data + len`；它对应 `app_send_stream`：
返回实际写入字节，成功写入时用 FIFO event bit 合并 TX 通知，并向所存
`event_queue` 的 IO ring 放目标 **server** Session index。
`send_io_event(event, noblock)` 仅抽出 VPP `app_send_io_evt_to_vpp` 的投递步骤：
MQ 和 server Session index 已由 `AppSession` 持有，不能再让业务调用者传入。
`recv_stream` 不把应用池 index 当作 server index。iperf 丢弃 RX 字节后，
用 infra `Fifo::needs_deq_notification(consumed)` 判断是否调用 service 的
`program_transport_io_event(handle, Rx)`，对应 VPP
`vperf_protos.c:102-112,145-155`；不在 `AppSession` 中藏这条业务通知。
网络侧 TCP 的 ACK、重传、packetization 仍按 VPP 由 Session/Transport
内部使用 FIFO。

Datagram 仍通过同一个 `AppSession` 的 FIFO 数据入口，而不是另建 app
数据 owner。VPP `application_interface.h:632-720` 在 IP transport 字段之前
生成 `data_length/data_offset`，用 `svm_fifo_enqueue_segments` 一次提交
header+payload；Hammer 的 raw datagram 方法构造 `SessionDatagramPrefix`，把它、
`self.transport` 中 `Some(T)` 的字节和借来的 payload slices 交给
infra `Fifo::enqueue_segments`。调用前用 `max_enqueue` 检查**整条**记录；
不足时返回 `Ok(None)`，不写部分 datagram、不设置 TX event。提交成功返回
`Some(payload_len)`，再按 `app_send_dgram_segs_raw` 设置 TX FIFO event bit，
仅当 `do_event` 为真且 event bit 从 0 变 1 时，按调用者给出的 `event`
向 worker MQ 投递；app 侧普通发送传 `Tx, true`。`segment_len` 对应 VPP
包含 header 的 `seg_len`，必须与生成的 header 和各段长度一致，不能由调用者
用 payload 长度冒充。FIFO 提交失败沿用已有
`SessionError::DatagramFifo { session_id, source }` 保留 infra source；
不是把 VPP 的负数 FIFO 结果伪装成新的 error code。若 event 信号在提交后
失败，调用方必须知道 FIFO 已提交、不得重投同一 datagram。

VPP `application_interface.h:754-793` 的读取先窥视前缀，确认
`header_len + data_length` 全部可读，然后把 transport 字段写回
`self.transport`、只复制 `min(out.len(), data_length - data_offset)` 字节；
VPP 即使 `peek = true` 也会先更新 `at`，故 Rust 接收方法需要 `&mut self`。
`recv_dgram_raw(peek = true)` 保留 FIFO；`peek = false` 无论输出是否截断，均丢弃完整
`header_len + data_length`，不把 transport RX 通知塞进 app data API。
需要通知的具体应用在自己的 RX 内层使用 FIFO 的通知原语，对应 VPP
`vperf_protos.c:310-425` 对 datagram 业务自行处理 FIFO 及事件。
前缀的 `data_offset > data_length` 是内部记录损坏，按 VPP 的 `ASSERT` 处理；
半条记录返回 `Ok(None)`，不消费。VPP 泛型函数用 `0` 同时表示未就绪与
零长报文，并由 vperf `vperf_protos.c:333-343` 单独丢弃零长报文；Rust
`Option` 区分两者，完整的零长报文返回 `Some(0)` 并推进 FIFO。

IP 地址、端口和地址族不属于 service；VPP `app_session_transport_t`
是 `app_session_t` 的**持久字段**，而 `gso_size` 只属于单次 datagram header
（`application_interface.h:266-290`、`session_types.h:496-516`）。
plugin-session 定义 `T` 的 IP 实例，从现有 `IpTransportConnectionId` 转换：

```rust
// VPP: application_interface.h:266-273, app_session_transport_t;
// vperf_test.h:215-229, UDP transport-connection copy at accept.
// Defined only in hammer-plugin-session, never in hammer-service.
#[repr(C, packed)]
#[derive(Default, KnownLayout, FromBytes, Immutable, IntoBytes)]
pub struct AppSessionTransport {
    remote_ip: [u8; 16],
    local_ip: [u8; 16],
    remote_port: [u8; 2],
    local_port: [u8; 2],
    is_ip4: u8,
}

// VPP: vperf_test.h:219-227, session_get_transport(s) and transport copy.
impl From<IpTransportConnectionId> for AppSessionTransport {
    fn from(connection: IpTransportConnectionId) -> Self;
}
```

`AppSession<AppSessionTransport>` 仅在 datagram 模式以 `Some` 保存这个 37 字节实例；
service 的泛型 datagram 方法只借用其 `IntoBytes` 表示或用 `FromBytes`
恢复该字段，不解释 IP。8 字节前缀、37 字节 transport 和单次发送的
2 字节 GSO 组成 VPP 的 47 字节 `session_dgram_hdr_t`。
service 不复制 IP 类型，也不引入默认 `E = ()`。
`is_dgram` 由 Session transport mode 决定；当前 iperf3 TCP
始终为 `false`，不在此 ADR 假装 UDP app 已接线。

## SessionEvent 与两条通知路径

保留 service 现有的 `SessionEvent` 名称，把它收敛成唯一的事件记录；
MQ 编码专用的 `SessionEventRecord` 和两个转换已删除，
不增加 `SessionEvt` 或 `ApplicationEvent`。`SessionEventType` 是标签枚举，
`SessionEventEnqueue` 只表示向 worker MQ 投递的结果，不是事件记录。
VPP 的 `session_event_t` 在 SessionWorker MQ、worker event list、
AppWorker 的 `wrk_evts[thread]` 和 app MQ 中都是同一记录
（`session_types.h:476-492`、`session_node.c:1936-1989`、
`application_worker.c:934-967`、`session_input.c:80-153`）。
`SessionEventElement` 仅加链表索引，不复制出第二种事件语义。

```rust
// VPP: session_types.h:476-492, session_event_t.
// One fixed record for SessionWorker and AppWorker event storage.
#[repr(C, packed)]
#[derive(Clone, Copy, KnownLayout, FromBytes, Immutable, IntoBytes)]
pub struct SessionEvent {
    pub(crate) event_type: u8,
    pub(crate) postponed: u8,
    pub(crate) session_index: u32,
    pub(crate) worker_index: u32,
    pub(crate) rpc_sequence: u64,
}

// VPP: session.c:56-71; application_worker.c:935-967.
// Index/handle are alternative payload arms, not distinct event records.
impl From<(SessionEventType, u32)> for SessionEvent {
    #[inline(always)]
    fn from(value: (SessionEventType, u32)) -> Self;
}

impl From<(SessionEventType, SessionHandle)> for SessionEvent {
    #[inline(always)]
    fn from(value: (SessionEventType, SessionHandle)) -> Self;
}

// VPP: session.h:45-49, session_evt_elt_t.
pub struct SessionEventElement {
    pub event: SessionEvent,
    pub next: u32,
    pub previous: u32,
}

// VPP: session.c:626-648, session_enqueue_notify(s).
// The service uses its global SessionMain internally; it is not an argument.
pub fn enqueue_notify(session: &Session) -> Option<()>;

// VPP: session.c:107-113, session_program_tx_io_evt(sh, evt_type).
pub fn program_tx_io_event(
    handle: SessionHandle, event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError>;

// VPP: session.c:133-140, session_program_transport_io_evt(sh, evt_type).
pub fn program_transport_io_event(
    handle: SessionHandle, event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError>;

impl SessionWorker {
    // VPP: session.c:115-131, session_program_rx_io_evt.
    // The receiver replaces vlib_get_thread_index() without a thread-local.
    // None: same-worker AppWorker notification or closing-state no-op;
    // Some: cross-worker MQ enqueue outcome.
    pub fn program_rx_io_event(&mut self, handle: SessionHandle)
        -> Result<Option<SessionEventEnqueue>, SessionError>;

    // VPP: session.c:578-599, session_program_io_event (always_inline).
    // The receiver supplies VPP's current-thread context; the four explicit
    // inputs are exactly (app_wrk, s, et, is_cl), in that order.
    // Private AppWorker event-list step, not a SessionWorker MQ producer.
    #[inline(always)]
    fn program_io_event(
        &mut self,
        app_worker: &AppWorker<'_>,
        session: &Session,
        event: SessionEventType,
        is_connectionless: bool,
    ) {
        let notification = if is_connectionless {
            let event = match event {
                SessionEventType::Rx => SessionEventType::BuiltinRx,
                SessionEventType::Tx => SessionEventType::TxMain,
                _ => unreachable!("connectionless IO notification must be RX or TX"),
            };
            SessionEvent::from((event, session.handle()))
        } else {
            app_worker.add_event(session, event);
            return;
        };
        app_worker.add_event_custom(self.worker_index(), &notification);
    }
}

impl AppWorker<'_> {
    // VPP: application_worker.c:934-952,
    // app_worker_add_event(app_wrk, s, evt_type). Self is app_wrk.
    fn add_event(&self, session: &Session, event: SessionEventType);

    // VPP: application_worker.c:954-967,
    // app_worker_add_event_custom(app_wrk, thread_index, evt). Self is app_wrk.
    fn add_event_custom(&self, thread_index: u32, event: &SessionEvent);
}
```

上述是目标参数契约，不是当前实现的签名：现有 `AppWorker::add_event`
还额外接收 `runtime` 和 `SessionWorker`。迁移前必须让当前执行的
`SessionWorker` 接收事件列从空变为非空的调度事实，并由已经持有
`DataPlaneMain` 的 session graph node 安排 `session-input`；VPP 在
`app_worker_add_event/custom` 内调用 `session_wrk_program_app_wrk_evts`
（`application_worker.c:934-967`、`session.c:565-576`）。不能为了省掉这条
接线把任何 Main 或 runtime 塞回上述函数参数，也不能从全局 Main 再取一份
与当前 `&mut SessionWorker` 重叠的可变借用。

`SessionEvent` 是 18 字节：两个标签字节和 16 字节 union 存储区，
Rust 用 `session_index`、`worker_index`、`rpc_sequence` 三个 packed 字段表达该
存储区的布局；三个字段不表示每种事件都同时拥有这些语义。SessionWorker MQ
producer 按 IO、Session、RPC 分支直接写已分配槽位（ADR-0043），不先构造本地
记录并复制整条消息。
不再使用原来的 32 字节平铺字段。
`SessionHandle` 当前 Rust 字段顺序为 `worker_index, session_index`，
VPP `session_handle_tu_t` 的顺序是 `session_index, thread_index`
（`session_types.h:366-385`）；`From<(SessionEventType, SessionHandle)>`
必须按事件格式填入 index/worker 字段，
不能拷贝 `SessionHandle` 的内存布局。解读 union 存储区必须同时知道**事件来源**：
AppWorker 的 `RESET` 带 Session index，而投往 SessionWorker 的 `RESET`
带完整 handle（`application_worker.c:672-676`、`session.c:64-71`）。
因此 `event_type` 单独不足以决定 union arm。

VPP 的通知路径分别是：

1. `session_send_evt_to_thread`（`session.c:28-86`）写**目标 SessionWorker MQ 的
   IO ring**。它按 IO、Session control、RPC 三种 family 填同一
   `session_event_t`；IO family 带目标 worker 的 Session pool index，
   CLOSE/HALF_CLOSE/RESET 带完整 handle。锁忙与 ring 满分别是未提交的
   `Busy`/`Full`；提交后若 infra 报 signal failure，事件已入队，
   不得作为未提交事件重投。VPP 只在目标 worker 为 interrupt 模式时
   标记 `session-queue` pending；Hammer 复用 runtime 现有跨 worker
   interrupt handoff，不在热路径读另一 worker 的非原子状态。
2. `session_program_tx_io_evt` 和 `session_program_transport_io_evt`
   （`session.c:107-140`）都调用上面的 MQ producer，分别限定 TX/TX_FLUSH
   和 transport IO 事件。`session_program_rx_io_evt` 同 worker 时先检查
   `TRANSPORT_CLOSING`，再调用 `session_enqueue_notify`；跨 worker 才把
   `BUILTIN_RX` 加进**目标 SessionWorker MQ**。后一种 `BUILTIN_RX` 在 MQ
   中带 **index**；queue node 找到 Session 后转为 AppWorker 的 RX 通知
   （`session_node.c:1891-1898`）。签名不额外要求调用者传
   `&mut SessionWorker` 或 `runtime`。
3. `session_program_io_event(app_wrk, s, et, is_cl)`（`session.c:578-599`）
   是**私有的 AppWorker 事件列步骤**。Rust 保持相同四个业务输入；
   `SessionWorker` 方法接收者只承载当前执行 worker，不替换 `&Session`
   为 handle，也不额外传 runtime。普通连接保留 RX/TX 类型、只带
   Session index，追加到 Session 所属 worker 的列；connectionless
   分支把 RX 改为 `BUILTIN_RX`、TX 改为 `TX_MAIN`，带完整 Session handle，
   追加到**当前执行 worker**的列。普通分支直接调用
   `app_worker.add_event`，connectionless 分支直接调用
   `app_worker.add_event_custom`；都不写 SessionWorker MQ，也不设置
   FIFO event bit。
`app_worker_add_event/custom` 在该列从空变为非空时安排 `session-input`
   （`application_worker.c:934-967`）。`session_input.c:119-147` 分别按
   index/handle 解析两类事件，然后才调用 builtin callback。
4. `session_enqueue_notify`（`session.c:626-654`）取得 AppWorker、调用上述
   `session_program_io_event`，再通知 FIFO subscribers；它**不**负责
   `svm_fifo_set_event`。正常 RX 的去重是 `SESSION_F_RX_EVT` 和
   `session_to_enqueue`（`session.h:811-825`），在 flush 时调用
   `session_enqueue_notify`（`session.c:689-714`）。vperf 的 self-tap
   则先 `svm_fifo_set_event(rx_fifo)`，只有 0→1 才调用
   `session_enqueue_notify`（`vperf_protos.c:80-99,124-125`）。
   Hammer `SessionWorker::enqueue_notify` 现在只入队 AppWorker RX 通知；
   它不读写 RX FIFO event bit。self-tap 的调用方先设置 bit，
   再根据 0→1 结果决定是否通知。
5. app 侧的 `app_send_io_evt_to_vpp`（`application_interface.h:595-628`）
   直接写保存的目标 SessionWorker MQ；`noblock` 使用 try-lock，阻塞模式
   等 producer。它不设置 FIFO event bit，也不经过 AppWorker 事件列。
   `app_send_stream_raw` 写 TX FIFO 后，仅在正数入队且 event bit 0→1
   时调用它（`.h:723-743`）。vperf 的 stream 发送使用阻塞模式
   （`vperf_protos.c:121`），不能套用 Session-side 的 `Busy/Full`
   返回路径。

以上四处存储都复用 `SessionEvent`，但生产路径和 payload 解释不能合并。
SessionWorker MQ 读出同一记录后，IO 进入 new event list，内部控制进入
control list；`SessionEventElement` 只保存链表链接，AppWorker 事件仍直接存
`SessionEvent`（`session_node.c:1936-1989`）。VPP 的 app control ring 可以在
18 字节事件头后存额外消息字节，worker list 再把较大 payload 移入
`SessionControlData` pool（`session_node.c:1941-1955`）。Hammer 现有
`SvmMsgQ::read/write<T>` 要求 `size_of::<T>() == ring element size`，因此它只
能直接读写定长 IO ring；控制 ring 的头加尾随字节须由 infra 提供通用的
消息字节借用能力，不能造第二个事件类型或假称现有 typed API 已支持。

## iperf3 TCP 的具体接线

VPP `vp_test_session_t` 把 `foreach_app_session_field` 展开为前缀，随后
直接放测试状态（`vperf_builtin.h:75-110`）：这是为 C 的指针强转和
`es->rx_fifo` 等直接字段访问服务，不是要求通用 app record 拥有 vperf
状态。Rust 的 `Iperf3Session` 内嵌 `AppSession`，worker pool 持有完整记录；
取 `&record.app` 即可复用通用 I/O，无第二次分配或布局强转。
`CacheLineAlignMark` 留在与 VPP `vp_test_session_t` 对应的 plugin record，
而不是 service 的 `AppSession`：

```rust
// VPP: vperf_builtin.h:75-134, vp_test_session_t/vp_test_worker_t;
// vperf_server.c:60-93, accept and opaque publication.
#[repr(C)]
pub struct Iperf3Session {
    cacheline0: CacheLineAlignMark,
    app: AppSession<AppSessionTransport>,
    role: ListenerRole,
    phase: SessionPhase,
    parser: ControlParser,
    parameters: Option<ControlParameters>,
    received: u64,
    sent: u64,
}

#[repr(C)]
pub struct Iperf3Worker {
    cacheline0: CacheLineAlignMark,
    sessions: Pool<Iperf3Session>,
}

impl Iperf3Session {
    // VPP: vperf_protos.c:142-155, vp_proto_server_stream_rx_no_echo(es, s).
    #[inline]
    fn server_stream_rx_no_echo(&mut self, session: &mut Session);
}

// VPP: vperf_server.c:71-93, vp_server_session_alloc_and_init(s).
// AppSession shares the server-owned FIFO objects across pool growth.
fn on_accept(session: &mut Session) -> Result<(), ApplicationError>;

// VPP: vperf_server.c:367-369, vp_server_rx_callback(s), and
// vperf_server.c:340-365, worker selection and s->opaque lookup.
fn on_rx(session: &mut Session);

// VPP: application_interface.h:35-37, session_cleanup_callback(s, ntf).
// VPP vperf does not register this callback. Hammer needs it to drop the
// Rust app-pool record before service releases the shared FIFO objects.
fn cleanup(session: &mut Session, notification: SessionCleanup);

```

`ApplicationConfig::builtin_rx` 使用 ADR-0040 修正后的
`Option<fn(&mut Session)>`（service `Session` 是具体 `u32` opaque 类型）。VPP `application_interface.h:47-49` 的回调只有
`session_t *` 一个参数，`session_input.c:119-138` 直接把找到的 Session
交给它；Hammer 的 service 派发者也在回调前解析 Session，回调不接收
`SessionWorker`、handle 或 runtime。其他接收 `session_t *` 的应用回调
同样按 ADR-0040 改为直接借用 Session。
`on_rx` 只按 `session.opaque` 找到所属 worker pool 中的
`Iperf3Session`，再把同一个 `&mut Session` 交给协议方法；不接收
runtime、SessionWorker、SessionMain、application id、handle 或 FIFO
副本。协议错误由 iperf owner 在 callback 内处理，不能从无返回值的
builtin callback 伪装传播。

`accept` 在同一次调用内取得 app pool index、写入 server `Session.opaque`，
最后发布 `Ready`（`vperf_server.c:71-93`）；应用的 accepted callback 只需
传入 VPP 所需的 `&mut Session`，不额外传 worker、handle 或 MQ。
`AppSession::new(&Session)` 的输入是短借用；返回值克隆同一个 FIFO/MQ
对象的引用计数句柄，不从这次借用推导长期生命周期。application cleanup
先从 iperf pool 删除记录，service 才归还 FIFO 到 Segment Manager；
这样不需要跨池的长期 `&Session`，也不复制 FIFO 原语。

1. `ApplicationMain::attach`、`IpSessionMain::listen`、TCP listener 和
   application callback 的 owner 不变。各 Main 是本层全局 owner，
   不进入事件函数参数；accept 从
   server `Session` 借用 FIFO 所有权事实，`AppSession::new(session)`
   绑定 handle、type、MQ 并以 `None` 初始化
   transport；本 ADR 的 iperf3 只有 TCP，因此不填 IP 地址。将来使用
   datagram 的具体应用在自己的 accept 中借助 plugin-session 的转换填入
   transport，随后构造自己的 app record。当前 iperf3 构造
   `Iperf3Session` 并插入
   `Pool<Iperf3Session>`，把返回的**应用池索引**写进 `Session.opaque`。
   handle 中的 `session_index` 则是**service Session 池索引**，绝不互换。
   最后发布 server `SessionState::Ready`，使 callback 返回后的观察者不会
   看见 Ready 却找不到 app record。VPP 在 callback 中也完成 Ready、
   pool 分配、FIFO/MQ 绑定和 opaque 写入（`vperf_server.c:71-93`）；
   Rust 只调整同一个回调内的发布顺序，不引入另一阶段或回滚 helper。
2. TCP 将 payload 放入 service `Session.rx_fifo`，service 的
   `session-input` 经 `AppWorker` 调用 iperf `on_rx`。callback 先读
   `Session.opaque`，再从所属 `Iperf3Worker.sessions` 取得
   `&mut Iperf3Session`。`record.role/parser/parameters` 只处理 iperf 协议；
   从同一个 server `Session` 借来的 RX/TX FIFO 交给 iperf RX 内层，
   后者直接用 infra FIFO 检查、窥读与消费。parser 按阶段先检查/借用 FIFO
   段，结束段借用后再更新
   `record.parser`、提交消费；不把 FIFO 原语复制成 AppSession 方法。
   data session 也直接对借来的 FIFO 消费并计数。
3. RX 内层用 `Fifo::needs_deq_notification(consumed)` 判断是否调用
   `program_transport_io_event(session.handle(), Rx)`，对齐
   `vperf_protos.c:105-112`。control reply 调用
   `record.app.send_stream(bytes, false)`，由 AppSession 设置 TX event bit、向
   server worker MQ 投递 TX；iperf 不直接调用 FIFO `enqueue`、
   `SessionWorker::enqueue_ready` 或 SVM MQ。Session Queue 仍消费 Session
   TX FIFO 并交给 TCP output（`application_interface.h:724-752`，
   `session_node.c:1858-1915`）。
4. `cleanup(session, notification)` 在 service 释放 FIFO 之前，按
   `Session.opaque` 移除整个
   `Iperf3Session`（包含其 `AppSession` 字段），随后 service Session/SegmentManager 的原
   owner 才释放 FIFO；iperf 不手动释放 FIFO。VPP vperf 的整体 stop 是
   先对池内 handle 请求 disconnect 再 `pool_free`（`vperf_server.c:181-201`）；
   VPP vperf **没有**注册逐 Session cleanup callback；Hammer 仅为 Rust
   FIFO 借用的析构顺序使用 VPP 通用 `session_cleanup_callback(s, ntf)` 契约，
   不能把 vperf 的 batch pool free 误写成它的逐 Session 实现。

### iperf3 stream RX：对照 `vp_proto_server_stream_rx_inline`

VPP `vperf_protos.c:58-127` 的 echo 内层先取 RX 可读量和 TX 可写量；
RX 为空返回，TX 满时 self-tap；否则读 RX、计数、按需通知 transport RX、
写 TX，处理后 RX 仍有数据则 self-tap。`:145-155` 的 no-echo 路径不检查
TX 空间，只丢弃已读 RX 并按需通知 transport。iperf3 **control** 是协议命令
而非字节 echo：必须先确认完整命令和回复空间，才消费 RX 并推进 parser；
**data** 按 no-echo 路径消费，且不清 RX FIFO event bit（VPP
`vperf_protos.c:143-155` 只执行 `svm_fifo_dequeue_drop`）。下面只声明有 `(es, s)` 直接对应物的 data
内层；control 留在接收 callback 内，不传 VPP echo 专用的 `rx_buf` 或
`test_bytes`，也不造第二条 FIFO 数据通路。

VPP `session_input.c:119-138` 在 builtin RX callback 前清的是
`SESSION_F_RX_EVT`，**不是** RX FIFO event bit。`app_recv_stream_raw`
在实际 dequeue/peek 前另行清 FIFO event bit
（`application_interface.h:794-810`）。iperf control 路径若绕开
`AppSession::recv_stream` 而直接借用 server `Session.rx_fifo` 检查和消费，
必须在本次接收前完成相同的 `rx_fifo.unset_event()`；半包则返回并等待
新的 RX enqueue。不能把 AppWorker 的 Session flag 清除误当成 FIFO
event bit 清除。随后调用 infra 的
`needs_deq_notification(consumed)`，它已完成该通知标志的原子清除；
不在 AppSession 加 `consume_rx` 代理。
TX 满且已有完整命令时才先 `rx_fifo.set_event()`，若发生 0→1 则通过
service `enqueue_notify(session)` self-tap；
不能因为 FIFO 里留着半个命令就反复自唤醒。

```rust
// VPP: vperf_protos.c:142-155, vp_proto_server_stream_rx_no_echo(es, s).
// Self is es; session is s. This is the iperf3 data-session path.
impl Iperf3Session {
    pub fn server_stream_rx_no_echo(&mut self, session: &mut Session);
}
```

`vp_proto_server_stream_rx_inline(es, s, rx_buf, test_bytes)` 是 vperf 的
**字节回显/校验**内层（`vperf_protos.c:58-127`）：`rx_buf` 是回显所需的
预分配复制缓冲区，`test_bytes` 选择校验。iperf3 control 解析不回显字节，
也不持有该缓冲区或校验模式，因此不声明一个同名却缺两个参数的伪移植方法。
control 命令直接在 `Iperf3Worker::on_rx(session)` 中按下述 FIFO 步骤处理；
data 路径调用上面的 `(es, s)` 内层。

`on_rx` 从 `Session.opaque` 得到 app pool index 后按以下顺序处理，期间只借
所属 worker 的 `Iperf3Session`；通用应用收发仍由 `record.app` 管理，
协议内层借用 server `Session` 的 FIFO 使用 infra 原语：

1. RX 为空立即返回。data role 读取 `rx_fifo.max_dequeue()`，
   一次 `drop_dequeue(available)` 并累加 `record.received`；不查 TX 空间、
   不回显。control role 用 `ControlParser::inspect` 检查 cookie、完整 JSON
   长度和状态字节。`ParameterInput` 直接从借来的 RX FIFO 调用
   `readable_segments`，仍以 `serde_json::from_reader` 增量解析，
   不复制 JSON 到临时 `Vec`。窥读期间不持有 `&mut record.parser`；
   结束借段后才更新 parser，因此 Rust 借用不冲突。inspect 的具体结果
   保持当前 iperf3 协议：cookie -> `ParameterExchange`，参数 JSON ->
   `CreateStreams`，`TestEnd` -> `ExchangeResults`，
   `ClientTerminate` -> `Close`（不回复）；不能把任意 RX 字节当作可立即
   dequeue 的完整命令。
2. 对会产生回复的 control action，先以 `tx_fifo.max_enqueue()`
   确认一字节响应可完整写入。如果 TX 已满，**不消费 RX、不推进 parser**，调用现有
   `rx_fifo.set_event()` 发生 0→1 时调用
   `enqueue_notify(session)` 安排 self-tap，然后返回；
   对 `Close` 等无回复 action 不施加无意义的 TX 背压。VPP 对照
   `vperf_protos.c:67-99`，但 iperf3 不是 echo，不能简单取
   `min(max_dequeue, max_enqueue)` 作为协议命令长度。
3. `rx_fifo.drop_dequeue(inspected_len)` 完成后才
   `record.parser.commit(action)`、累加 received；随后
   `rx_fifo.needs_deq_notification(consumed)` 为真时调用
   `program_transport_io_event(session.handle(), Rx)`。
   随后 `record.app.send_stream(reply, false)` 写回复并累加实际
   sent；预检一字节且同一 worker 独占 TX producer，成功字节数必须等于
   reply 长度。VPP 对照 `vperf_protos.c:102-122`。
4. 本轮结束还有完整 RX 命令而 TX 背压或处理预算已满时，先调用
   `rx_fifo.set_event()`；只有 0→1 才调用
   `enqueue_notify(session)` 自唤醒。VPP 原始两步在
   `vperf_protos.c:80-99,124-125`。service 的 `enqueue_notify`
   已移除 FIFO bit 操作；
   不创建 iperf node 或第二条 session queue。

`on_rx` 对应 VPP `vp_server_rx` 的 pool/opaque 查找；
`Iperf3Session::server_stream_rx_no_echo` 对应 vperf `(es, s)` 的 data 内层。
前者对应 VPP `always_inline vp_server_rx`，后者对应 VPP 普通 `static void`
函数，不强制内联。注册在 Application 中的
callback 仍是普通函数，负责取得所属 worker 并调用它。`ControlParser::inspect`
只能把协议输入错误归入现有 `Iperf3ProtocolError`；已验证的 Session/FIFO
借用若失效属于 owner 不变量，不伪装成 iperf 协议错误。VPP
`builtin_app_rx_callback` 虽返回 `int`（`application_interface.h:47-49`），
但 `session_input.c:119-138` 不消费这个返回值；它不是给 Hammer
`ApplicationConfig::builtin_rx` 扩参数或改成 `Result` 的理由。transport RX 事件的
`Busy/Full` 明确表示**本次没有投递**；VPP `vperf_protos.c:105-112` 对
`session_program_transport_io_evt` 的返回值未作重试，本 ADR 同样不声称
通知必达或暗设自动重试，验收只断言 `Enqueued` 时的到达顺序。

## 错误、同步、inline、弃用

- stream 部分写返回实际字节；datagram 不足整记录不提交。iperf builtin 的
  TX 事件使用 VPP `app_send_stream(..., noblock = 0)` 的等待语义
  （`vperf_protos.c:121,269`、`application_interface.h:580-627,724-745`），
  FIFO 写入后不能因 MQ 满而静默丢掉唯一的 TX 唤醒。独立的
  `program_transport_io_event` 仍返回现有 `SessionEventEnqueue::Busy/Full`
  表示事件未提交；VPP `session_send_evt_to_thread` 正是这两个队列结果
  （`session.c:26-86`）。不宣称当前 worker event list 会替失败的生产者
  自动重试。其他失败保留 service `SessionError` 与 infra source，不新建
  AppSessionError，也不以数字表示错误。
- `Session.state` 的 `AtomicU8` 仍是 server 生命周期状态；
  `AppSession.state` 是 VPP `app_session_t` 的 app-side 状态。Rust 用
  Acquire/Release 表达跨执行域观察，`volatile` 不作为同步原语。
  FIFO/MQ 沿用 infra 的发布机制；不加 AppSession 锁或快照。
- VPP `application_interface.h:595-628` 的 app MQ 发送是 `static inline`，
  对应 `send_io_event` 的 `#[inline]`；`:654-810` 的 stream/datagram raw
  收发是 `always_inline`，对应 `#[inline(always)]`。`session.c:107-140,645-648`
  的对外 `program_*`/`enqueue_notify`、`application_worker.c:934-967` 的
  `add_event/custom` 不是 inline，Rust 不强制内联。私有
  `session_program_io_event` 是 `always_inline`（`session.c:578-599`）；
  iperf 的 `on_rx` 对应 `always_inline vp_server_rx`，而 no-echo 内层是
  普通 `static void`（`vperf_server.c:339-369`、`vperf_protos.c:142-155`）。
- `hammer-runtime::app` **整个旧模块标记 deprecated**，包括其中
  `src/app/mod.rs`、`session.rs`、`session_msg_queue.rs`、`control.rs`、
  `layout.rs`、`error.rs` 及 `hammer-runtime/src/lib.rs` 的公开 re-export；
  `hammer-runtime::attach` 中专供旧 AppSession publication 的接口和
  `hammer-core::session::SessionEvt` 旧平面记录也标记弃用。
  `#[deprecated]` 只标识迁移方向，不加旧路径适配、双写或兼容 helper；
  `hammer-service::session::application` 已是旧路径，这里只接
  `hammer-service::session::app`。后续 client/VCL 另写决定，不把新类型
  挪到 runtime。

实现时须验证应用池 index 与 server Session index 不混用，iperf 应用数据
通过 server Session 的 FIFO 借用执行协议 RX 原语，通过 AppSession 执行通用
收发，不经第二份 FIFO 对象或 AppSession FIFO 代理方法；IO MQ 中各事件
携带正确 union 分支，TX event bit
合并通知，已提交的 RX dequeue notification 到达 transport，且 cleanup 不留下
悬挂 FIFO 句柄。运行行为需要独立的后续验证。
