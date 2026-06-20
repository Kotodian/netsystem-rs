# TCP Modules 7-9 Close Passive-Open Time-Wait Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 以当前代码为准，完整补齐 Module 7、Module 8、Module 9：本地 close 到 FIN 完成、被动打开 backlog 与 `SynRcvd` 重传/清理、`TIME-WAIT` tuple 保留与到期清理，并同步删掉这条路径里不该继续存在的 TCP 私有 session 容器。

**Architecture:** session runtime 继续只依赖 `SessionQueueProtocol<S>` 这个泛型边界。对 TCP 来说，`TcpConnectionState<C>` 只做 erased storage，typed `TcpConnection<S, C>` 继续承接真正的协议事件；session-side glue 只负责用现有 `session_mut()` / `session_state_mut()` 恢复 typed connection、调用 typed 事件、回写结果、收口 timer/index/output/cleanup，不能再自带一层 TCP 私有 session 状态容器。session 生命周期始终只由 runtime 管理。

**Tech Stack:** Rust 2024, `hammer-service` session runtime / TCP state machine / packet buffer runtime, `hammer-core::protocol::tcp`, `hammer-infra::{pool,map,fifo,vec}`, 本地 `third_party/vpp/src/vnet/{session,tcp}` 语义参考。

## Global Constraints

- 以当前代码为准，不回退到旧版 Module 7/8/9 方案。
- session runtime 只通过 `SessionQueueProtocol<S>` 与协议交互；app 只提交 intent，不持有、也不推进 session state。
- 不新增第二层协议抽象，不新增 TCP 私有 runtime API，不新增中间业务 carrier。
- TCP 继续只保留 `TcpConnection<S, C>` / `TcpConnectionState<C>` 这一层 transport state。
- 只有 typed TCP event 方法可以推进 TCP 状态；runtime/node/glue 只负责恢复、调度、回写与副作用收口。
- 协议侧落库只使用现有 `session_mut()` / `session_state_mut()`；不要新增 take/replace/transition 一类 session state 管理入口。
- session runtime 是唯一调度者；timer expiry 只能处理 runtime 传入的那个准确 token/kind。
- session 生命周期只由 runtime 管理；session/runtime 继续拥有 app/session 边界、timer 调度、索引回收、app completion；TCP 不直接完成 app op，不直接关闭 session，也不持有 session 生命周期决策。
- `TcpSegment` 继续是唯一 TCP output intent。
- 除 app/session 边界外，不复制 payload，不产生中间 payload `Vec`。
- 执行顺序必须先收口 typed state 落库边界，再收口 session/runtime 通用 RX 持有能力，最后再补 Module 7/8/9 业务路径。
- 需要把当前代码里不该继续存在的 TCP 私有 session 容器一起删掉，尤其是 `TcpSessionRx`、`rx_index`、`rx` pool 这一套；out-of-order 持有、buffer 生命周期、app completion 继续回到 session/runtime 的通用持有与投递路径。
- 未来其他协议接入时，对齐的是 `SessionQueueProtocol<S>`、erased storage、typed state 三层分工；不要把 TCP 当前实现名固化成全局标准。

---

## 当前代码事实

- `SessionDriverRuntime<S>` 已经提供 `session_mut()` / `session_state_mut()`，现有边界足够恢复、修改并回写协议状态。
- 现有 `session_mut()` / `session_state_mut()` 已经是协议侧唯一需要的落库入口，不需要新增额外 session state 生命周期接口。
- `SessionAppRuntime` 已经维护 `drained_closes`，并且当前代码里 `take_drained_closes()` 已经存在，不需要再围绕 close drain 发明新的 runtime 抽象。
- `SessionQueueProtocol<TcpConnectionState<C>>` 已经是 TCP 挂到 session runtime 的现有路径，不需要再发明新的协议层。
- `TcpConnectionState<C>` 已经能在 erased / typed 状态之间转换，typed `TcpConnection<S, C>` 已经承接大部分真实协议事件。
- 当前 TCP 主路径仍然大量依赖 `take_connection()` + `replace_session_state()` 这套 clone / take / replace 流程，这和“协议侧只用 `session_mut()` / `session_state_mut()` 落库”的边界还不一致。
- `session/runtime` 已经有 retained RX buffer 与 session RX completion 路径；当前问题不是缺基础设施，而是 TCP 在 `transport/tcp/session.rs` 里又叠了一层 `TcpSessionRx` 来维护 out-of-order、SACK 计算、retained-buffer 生命周期。
- `TIME-WAIT`、`SynRcvd`、close path 目前都还有一部分收口逻辑散落在 session-side glue 里，但没有统一约束成“TCP 只给协议事实，runtime 只做调度和副作用”。

---

## 本次必须删除的现有坏表面

- `crates/hammer-service/src/transport/tcp/session.rs` 中的 `TcpSessionRx`
- 同文件里围绕它存在的 `rx_index` / `rx` pool
- 同文件里只为维护这层 TCP 私有 RX 容器存在的入口：`rx()`、`rx_mut_or_alloc()`、`release_rx()`
- `crates/hammer-service/src/transport/tcp/session.rs` 中 `take_connection()` 这条 clone + `try_into` 恢复 typed state 的主路径
- TCP 主路径对 `replace_session_state()` 的依赖；前置任务完成后，TCP 事件收口必须改成基于现有 `session_mut()` / `session_state_mut()` 的原地落库
- 任何继续把 out-of-order retained buffer 生命周期绑在 TCP 私有容器上的逻辑

这些内容不是“后续优化”，而是 Module 7-9 这次一起清掉。

---

## 审批项

### 审批 A：新增 `session/backlog.rs`，只承载 backlog admission policy

涉及文件：

- `crates/hammer-service/src/session/backlog.rs`

拟新增结果：

```rust
const SESSION_BACKLOG_LIMIT: usize = 128;

pub(crate) struct SessionBacklog;

impl SessionBacklog {
    pub(crate) fn admit(pending: usize) -> bool {
        pending < SESSION_BACKLOG_LIMIT
    }
}
```

理由：

- backlog 是 session admission policy，不应该混在 TCP listen 状态机里。
- 这里只返回 admission 结论，不引入新的协议层或 listener 包装类型。

### 审批 B：给 `TcpPendingIndex` 增加按 listener local socket 统计 pending child 的只读能力

涉及文件：

- `crates/hammer-service/src/transport/tcp/session_index.rs`

拟新增结果：

```rust
impl TcpPendingIndex {
    pub(crate) fn pending_for_local(&self, local: SocketAddr) -> usize;
}
```

理由：

- backlog 判断必须回答“这个 listener 当前已经挂了多少个 pending child”。
- 现有按完整 tuple 查找不够用。

### 审批 C：在 session/runtime 的通用 RX 持有路径补齐 out-of-order 所需的通用能力

涉及文件：

- `crates/hammer-service/src/session/runtime.rs`
- `crates/hammer-service/src/session/protocol.rs`

拟修改结果：

- 不再让 TCP 自己维护 `TcpSessionRx`。
- 如果当前 `SessionDriverRuntime` 的 retained RX / session RX queue 能力不足以表达 out-of-order retained buffer、ready delivery、释放时机，就只补通用 session/runtime 能力，不能出现 TCP 语义字段或 TCP 专用 API 名称。

理由：

- 现在的 retained RX buffer 生命周期本来就属于 session/runtime。
- 这次要解决的是“能力落点不对”，不是再给 TCP 多造一层 wrapper。

---

## File Structure

### 需要修改的现有文件

- `crates/hammer-service/src/session/app.rs`
  - close submission drain 生产路径。

- `crates/hammer-service/src/session/runtime.rs`
  - 复用并补齐现有 session RX 持有/投递/释放路径，承接 out-of-order buffer 生命周期，替代 TCP 私有 RX 容器。

- `crates/hammer-service/src/session/protocol.rs`
  - 保留 protocol context，只承载 runtime 调度时需要的通用上下文；如果缺 session/runtime 通用 RX 入口，在这里暴露通用能力，不出现 TCP 专用命名。

- `crates/hammer-service/src/transport/tcp/session.rs`
  - 承载 TCP 的 session-side glue：通过现有 `session_mut()` / `session_state_mut()` 恢复 typed state、路由 typed 事件、回写结果、收口 timer/index/output/cleanup，同时删除 `TcpSessionRx` 及其配套管理逻辑。

- `crates/hammer-service/src/transport/tcp/state_machine.rs`
  - 承载 close、`SynRcvd` retransmit、`TIME-WAIT` 协议事件等纯 TCP 行为；继续只输出协议事实，不持有 session 级 buffer 容器。

- `crates/hammer-service/src/transport/tcp/listen.rs`
  - 接被动打开路径的 backlog admission。

- `crates/hammer-service/src/transport/tcp/time_wait.rs`
  - 接 `TIME-WAIT` input path。

- `crates/hammer-service/src/transport/tcp/session_index.rs`
  - backlog 所需的 pending child 统计与 pending cleanup。

### 需要新增的文件

- `crates/hammer-service/src/session/backlog.rs`
  - backlog admission policy。

- `crates/hammer-service/tests/tcp_close_path.rs`
  - close -> FIN/ACK 完整路径测试。

- `crates/hammer-service/tests/tcp_time_wait.rs`
  - `TIME-WAIT` duplicate FIN / expiry / cleanup 测试。

---

## 前置任务 1：先把 typed state 恢复/回写收回现有 session 落库边界

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_sent.rs`
- Modify: `crates/hammer-service/src/transport/tcp/syn_rcvd.rs`
- Modify: `crates/hammer-service/src/transport/tcp/established.rs`
- Modify: `crates/hammer-service/src/transport/tcp/close_wait.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait1.rs`
- Modify: `crates/hammer-service/src/transport/tcp/fin_wait2.rs`
- Modify: `crates/hammer-service/src/transport/tcp/closing.rs`
- Modify: `crates/hammer-service/src/transport/tcp/last_ack.rs`
- Modify: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Modify: `crates/hammer-service/tests/tcp_state_machine.rs`

**Interfaces:**
- Consumes:
  - `SessionDriverRuntime::session_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry<S>>`
  - `SessionDriverRuntime::session_state_mut(&mut self, id: SessionId) -> Option<&mut S>`
  - `TcpConnectionState<C>` 的 typed state variant
- Produces:
  - TCP 主路径不再依赖 `take_connection()`
  - TCP 主路径不再依赖 `replace_session_state()` 做 typed state 回写

**Deliverable:**

- close / ready / timer / passive-open / established receive 这些 TCP 主路径，统一改成通过现有 `session_mut()` / `session_state_mut()` 恢复状态、执行 typed TCP 事件、原地落库。
- `listen` / `syn_sent` / `syn_rcvd` / `established` / `close_wait` / `fin_wait1` / `fin_wait2` / `closing` / `last_ack` / `time_wait` 这些 node 入口同步迁移，不能留下继续依赖 `take_connection()` 的 active main path。
- `take_connection()` 从 TCP 主路径移除；如果迁移后没有剩余合法调用，直接删除。
- `replace_session_state()` 不再作为 TCP 主路径的状态回写方式；如果 runtime 仍为其他未迁移路径保留它，也必须从 TCP 主路径完全移除。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_state_machine.rs` 增加一个覆盖原地落库边界的测试模块，至少包含：

```rust
#[test]
fn tcp_close_path_updates_session_state_without_take_replace_flow() {
    let mut queue = test_tcp_queue();
    let session_id = queue.insert_session(established_connection_state());

    drive_close_submission(&mut queue, session_id);

    let state = queue.session_state(session_id).expect("state");
    assert!(matches!(state, TcpConnectionState::FinWait1(_)));
}

#[test]
fn tcp_syn_sent_timer_expiry_updates_session_state_without_take_replace_flow() {
    let mut queue = test_tcp_queue();
    let session_id = queue.insert_session(syn_sent_connection_state_with_retransmit_pending());

    expire_retransmit_timer(&mut queue, session_id);

    let state = queue.session_state(session_id).expect("state");
    let TcpConnectionState::SynSent(connection) = state else {
        panic!("expected syn-sent after retransmit expiry");
    };
    assert!(connection.tcp_timer_is_armed(TcpConnectionTimerKind::RETRANSMIT));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine tcp_close_path_updates_session_state_without_take_replace_flow
cargo test -p hammer-service --test tcp_state_machine tcp_syn_sent_timer_expiry_updates_session_state_without_take_replace_flow
```

Expected: FAIL

- [ ] **Step 3: 完整实现原地 typed state 落库**

在 `crates/hammer-service/src/transport/tcp/session.rs` 把以下路径统一收口成同一种模式：

```rust
// borrow current TcpConnectionState<C> via session_mut()/session_state_mut()
// match exact typed variant
// run typed TcpConnection<S, C> event
// write resulting typed state back in place
// let runtime own timer/index/output/session-lifecycle side effects
```

实现结果要求：

- 协议侧落库只使用现有 `session_mut()` / `session_state_mut()`。
- 不新增 take/replace/transition 风格的 session state 接口。
- TCP 事件仍然由 typed `TcpConnection<S, C>` 承担，不把状态机降级回手写 raw-state 判断。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/state_machine.rs \
  crates/hammer-service/src/transport/tcp/listen.rs \
  crates/hammer-service/src/transport/tcp/syn_sent.rs \
  crates/hammer-service/src/transport/tcp/syn_rcvd.rs \
  crates/hammer-service/src/transport/tcp/established.rs \
  crates/hammer-service/src/transport/tcp/close_wait.rs \
  crates/hammer-service/src/transport/tcp/fin_wait1.rs \
  crates/hammer-service/src/transport/tcp/fin_wait2.rs \
  crates/hammer-service/src/transport/tcp/closing.rs \
  crates/hammer-service/src/transport/tcp/last_ack.rs \
  crates/hammer-service/src/transport/tcp/time_wait.rs \
  crates/hammer-service/tests/tcp_state_machine.rs
git commit -m "tcp(Refactor): restore typed state through existing session storage boundary"
```

## 前置任务 2：先把 out-of-order retained RX 能力并回 session/runtime 通用路径

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/tests/tcp_passive_open.rs`

**Interfaces:**
- Consumes:
  - existing retained RX buffer pool in `SessionDriverRuntime`
  - existing session RX completion path in `SessionDriverRuntime`
- Produces:
  - out-of-order retained buffer 生命周期回到 session/runtime 通用路径
  - `TcpSessionRx` 及其配套表面可删除

**Deliverable:**

- session/runtime 侧承接 out-of-order retained buffer 的持有、ready delivery、释放时机，TCP 不再私自持有一套 session RX 容器。
- SACK/DSACK 仍由 TCP 根据协议事实生成，但依赖的数据来自 session/runtime 通用持有路径，而不是 `TcpSessionRx`。
- 这一前置任务完成后，`TcpSessionRx`、`rx_index`、`rx` pool、`rx()` / `rx_mut_or_alloc()` / `release_rx()` 必须可删除。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_passive_open.rs` 增加：

```rust
#[test]
fn established_out_of_order_payload_still_emits_sack_and_delivers_through_session_runtime() {
    let (mut queue, session_id) = established_session_with_sack_enabled();

    inject_out_of_order_payload(&mut queue, session_id, 11, b"bbb");
    let ack = inject_in_order_payload(&mut queue, session_id, 8, b"aaa");

    assert_eq!(segment_sack_blocks(&ack), []);
    assert_session_rx_bytes(&mut queue, session_id, b"aaabbb");
}

#[test]
fn duplicate_payload_still_emits_dsack_without_tcp_private_rx_container() {
    let (mut queue, session_id) = established_session_with_sack_enabled();

    inject_in_order_payload(&mut queue, session_id, 8, b"abcdef");
    let ack = inject_duplicate_payload(&mut queue, session_id, 8, b"abc");

    assert_eq!(
        segment_sack_blocks(&ack),
        [TcpSackBlock {
            left_edge: 8,
            right_edge: 11,
        }]
    );
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open established_out_of_order_payload_still_emits_sack_and_delivers_through_session_runtime
```

Expected: FAIL

- [ ] **Step 3: 完整实现通用 RX 持有能力并删除 TCP 私有 RX 容器**

在 `crates/hammer-service/src/session/runtime.rs` / `crates/hammer-service/src/session/protocol.rs` 只补通用 session/runtime 能力，要求：

```rust
// no TCP-specific runtime API
// no TCP-specific retained-RX owner type
// retain / order / enqueue / release remain generic session/runtime responsibilities
```

在 `crates/hammer-service/src/transport/tcp/session.rs` 完成：

```rust
// trim duplicate payload against rcv_nxt
// query out-of-order overlap from session/runtime-held buffers
// compute SACK/DSACK facts from protocol state + session/runtime-held buffers
// deliver ready buffers through existing session runtime completion path
```

同时删除以下现有坏表面：

- `TcpSessionRx`
- `protocol.rx_index`
- `protocol.rx`
- `rx()` / `rx_mut_or_alloc()` / `release_rx()`

实现结果要求：

- 不新增 TCP 私有 RX 容器替代品。
- out-of-order retained buffer 生命周期全部回到 session/runtime 通用路径。
- TCP 只保留 `rcv_nxt`、ACK/SACK/DSACK、状态迁移这些协议事实。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
  crates/hammer-service/src/session/protocol.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/tests/tcp_passive_open.rs
git commit -m "session(Refactor): move out-of-order rx retention back into session runtime"
```

---

## Task 1: 接通 Module 7 close -> FIN 路径

**Files:**
- Modify: `crates/hammer-service/src/session/app.rs`
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/tests/tcp_state_machine.rs`
- Create: `crates/hammer-service/tests/tcp_close_path.rs`

**Interfaces:**
- Consumes:
  - `SessionAppRuntime::take_drained_closes()`
  - `SessionDriverRuntime::session_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry<S>>`
  - `TcpConnection<Established, C>::on_close(...)`
  - `TcpConnection<CloseWait, C>::on_close(...)`
- Produces:
  - close submission routed to typed `TcpConnection<S, C>`
  - final ACK / close cleanup routed back through runtime close path

**Deliverable:**

- close submission 进入现有 `SessionQueueProtocol<TcpConnectionState<C>>` 路径。
- glue 恢复 typed connection，调用 local close typed event，回写 next state，enqueue FIN，并 arm retransmit timer。
- final ACK 仍通过 runtime close session，不能让 TCP 直接碰 app ring，也不能让 TCP 自己决定 session 生命周期。
- 本任务不新增任何新的 TCP session 容器，close 收口继续走现有 runtime 边界。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_state_machine.rs` 增加：

```rust
#[test]
fn established_on_close_advances_snd_nxt_and_returns_fin_wait1() {
    let connection = established_connection();
    let start = connection.snd_nxt();

    let (next, segment) = connection.on_close();

    assert_eq!(next.snd_nxt(), start.advance(1));
    assert_eq!(segment.sequence(), start);
    assert!(segment.flags().contains(TcpSegmentFlags::FIN));
    assert!(segment.flags().contains(TcpSegmentFlags::ACK));
}
```

在 `crates/hammer-service/tests/tcp_close_path.rs` 增加：

```rust
#[test]
fn close_submission_on_running_session_emits_fin_and_enters_fin_wait1() {
    let (mut queue, session_id) = running_established_session();

    let segment = submit_close_and_take_output(&mut queue, session_id);

    assert!(segment.flags().contains(TcpSegmentFlags::FIN));
    assert!(matches!(
        queue.session_state(session_id),
        Some(TcpConnectionState::FinWait1(_))
    ));
}

#[test]
fn close_submission_on_close_wait_emits_fin_and_enters_last_ack() {
    let (mut queue, session_id) = running_close_wait_session();

    let segment = submit_close_and_take_output(&mut queue, session_id);

    assert!(segment.flags().contains(TcpSegmentFlags::FIN));
    assert!(matches!(
        queue.session_state(session_id),
        Some(TcpConnectionState::LastAck(_))
    ));
}

#[test]
fn fin_retransmit_reemits_same_sequence() {
    let (mut queue, session_id, first_fin) = running_fin_wait1_session();

    let retransmit = expire_fin_retransmit_and_take_output(&mut queue, session_id);

    assert_eq!(retransmit.sequence(), first_fin.sequence());
    assert_eq!(retransmit.flags(), first_fin.flags());
}

#[test]
fn final_ack_completes_close_and_releases_session() {
    let (mut queue, session_id) = running_last_ack_session();

    deliver_final_ack(&mut queue, session_id);

    assert!(queue.session_state(session_id).is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine established_on_close_advances_snd_nxt_and_returns_fin_wait1
cargo test -p hammer-service --test tcp_close_path
```

Expected:

- local close typed event 失败或缺失。
- close path 集成测试失败，因为 close submission 还没走完整路径。

- [ ] **Step 3: 完整实现 close 路径**

在 `crates/hammer-service/src/transport/tcp/state_machine.rs` 完整补上 local close 事件，保证：

```rust
impl<C> TcpConnection<Established, C>
where
    C: CongestionController,
{
    pub(crate) fn on_close(self) -> (TcpConnection<FinWait1, C>, TcpSegment);
}

impl<C> TcpConnection<CloseWait, C>
where
    C: CongestionController,
{
    pub(crate) fn on_close(self) -> (TcpConnection<LastAck, C>, TcpSegment);
}
```

在 `crates/hammer-service/src/transport/tcp/session.rs` 把现有 close drain 完整接到 typed TCP 事件，收口内容必须包括：

```rust
// consume drained close submissions
// restore typed connection from TcpConnectionState<C>
// call typed TcpConnection<S, C>::on_close()
// store result back into TcpConnectionState<C>
// enqueue FIN through existing output path
// arm retransmit timer through runtime token path
// on final ACK, release session through runtime close path
```

实现结果要求：

- runtime 继续只依赖 `SessionQueueProtocol<S>`。
- local close 仍由 typed connection 承担。
- 不新增第二层抽象。
- 不把 close completion 挂到 TCP 私有状态里。
- session state 落库只走现有 `session_mut()` / `session_state_mut()`。
- session 生命周期决策只留在 runtime。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_state_machine
cargo test -p hammer-service --test tcp_close_path
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/app.rs \
  crates/hammer-service/src/session/runtime.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/state_machine.rs \
  crates/hammer-service/tests/tcp_state_machine.rs \
  crates/hammer-service/tests/tcp_close_path.rs
git commit -m "tcp(Refactor): route close through existing session queue protocol"
```

## Task 2: 接通 Module 8 backlog admission

**Files:**
- Create: `crates/hammer-service/src/session/backlog.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session_index.rs`
- Modify: `crates/hammer-service/src/transport/tcp/listen.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/tests/tcp_passive_open.rs`

**Interfaces:**
- Consumes:
  - `TcpPendingIndex::remember_pending_open(...)`
  - `TcpPendingIndex::lookup_pending_by_tuple(...)`
  - `TcpConnection<Listen, C>::accept_syn(...)`
- Produces:
  - `SessionBacklog`
  - `TcpPendingIndex::pending_for_local(local: SocketAddr) -> usize`

**Deliverable:**

- backlog policy 落在 `session/` 独立模块。
- listener 路径先判断 admission，再决定是否创建 pending child。
- backlog 只处理 listener admission，不顺手挂入任何 TCP 私有 RX/TX 容器。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_passive_open.rs` 增加：

```rust
#[test]
fn tcp_listen_drops_new_syn_when_listener_backlog_is_full() {
    let mut queue = listener_with_full_backlog();

    let output = deliver_listen_syn(&mut queue, syn_packet(10_001));

    assert!(output.is_none());
    assert_eq!(pending_child_count(&queue), session_backlog_limit());
}

#[test]
fn tcp_listen_keeps_existing_pending_child_when_overflow_syn_arrives() {
    let mut queue = listener_with_full_backlog();
    let original = pending_child_ids(&queue);

    let output = deliver_listen_syn(&mut queue, syn_packet(10_002));

    assert!(output.is_none());
    assert_eq!(pending_child_ids(&queue), original);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open tcp_listen_drops_new_syn_when_listener_backlog_is_full
```

Expected: FAIL

- [ ] **Step 3: 完整实现 backlog admission**

在 `crates/hammer-service/src/session/backlog.rs` 增加：

```rust
const SESSION_BACKLOG_LIMIT: usize = 128;

pub(crate) struct SessionBacklog;

impl SessionBacklog {
    pub(crate) fn admit(pending: usize) -> bool {
        pending < SESSION_BACKLOG_LIMIT
    }
}
```

在 `crates/hammer-service/src/transport/tcp/session_index.rs` 提供按 local socket 统计 pending child 的只读能力。

在 listener 被动打开路径里改成：

```rust
let pending = pending_index.pending_for_local(local);
if !SessionBacklog::admit(pending) {
    return Ok(());
}

let (child, segment) = listener.accept_syn(packet)?;
```

实现结果要求：

- backlog 只返回 admission 结论。
- overflow 分支直接 drop，不扩 syncookie。
- backlog 判断前后都不引入新的 queue wrapper 或 TCP 私有 session 容器。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/backlog.rs \
  crates/hammer-service/src/transport/tcp/session_index.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/listen.rs \
  crates/hammer-service/tests/tcp_passive_open.rs
git commit -m "session(Refactor): move passive-open backlog policy into session"
```

## Task 3: 接通 Module 8 `SynRcvd` retransmit 与 pending-open cleanup

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/tests/tcp_passive_open.rs`

**Interfaces:**
- Consumes:
  - `TcpConnection<SynRcvd, C>::accept_final_ack(...)`
  - `TcpPendingIndex::forget_pending_open(...)`
- Produces:
  - `TcpConnection<SynRcvd, C>::on_tcp_timer_expiry(timer: TcpConnectionTimerKind) -> Option<TcpSegment>`

**Deliverable:**

- `SynRcvd` retransmit 只处理 runtime 传入的那个准确 timer。
- final ACK / RST / timeout cleanup 都由现有 runtime 边界收口。
- protocol/glue 只负责回写连接状态与协议事实，不直接接管 session 生命周期。
- `SynRcvd` 收口完成后，不再继续增加新的 pending-open 辅助层。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_passive_open.rs` 增加：

```rust
#[test]
fn tcp_syn_rcvd_retransmit_timer_reemits_syn_ack() {
    let (mut queue, session_id, first_syn_ack) = syn_rcvd_session_with_timer();

    let retransmit = expire_syn_rcvd_retransmit_and_take_output(&mut queue, session_id);

    assert_eq!(retransmit.sequence(), first_syn_ack.sequence());
    assert_eq!(retransmit.flags(), first_syn_ack.flags());
}

#[test]
fn tcp_final_ack_removes_pending_open_and_promotes_connection() {
    let (mut queue, session_id, local, remote) = pending_syn_rcvd_session();

    deliver_final_ack_to_pending(&mut queue, session_id);

    assert!(queue.pending_route_by_tuple(local, remote).is_none());
    assert!(matches!(
        queue.session_state(session_id),
        Some(TcpConnectionState::Established(_))
    ));
}

#[test]
fn tcp_syn_rcvd_rst_releases_pending_open() {
    let (mut queue, session_id, local, remote) = pending_syn_rcvd_session();

    deliver_rst_to_pending(&mut queue, session_id);

    assert!(queue.pending_route_by_tuple(local, remote).is_none());
    assert!(queue.session_state(session_id).is_none());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open tcp_syn_rcvd_retransmit_timer_reemits_syn_ack
```

Expected: FAIL

- [ ] **Step 3: 完整实现 `SynRcvd` retransmit 与 cleanup**

在 `crates/hammer-service/src/transport/tcp/state_machine.rs` 补齐 `SynRcvd` timer 事件，要求它只消费 runtime 传入的那个 timer，不扫描全量 kind。

在 `crates/hammer-service/src/transport/tcp/session.rs` 把以下路径收口到现有 runtime 边界：

```rust
// forget pending-open on final ACK / RST / timeout
// remember established route when promotion succeeds
// enqueue retransmitted SYN-ACK through existing output path
// close session through runtime when promotion fails or reset arrives
```

实现结果要求：

- pending-open cleanup 不额外抽新的协议层。
- timer expiry 不扫描所有 kind。
- cleanup 后不会留下新的 pending child 索引悬挂。
- session promote / close 的最终生命周期动作只由 runtime 完成。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_passive_open
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/state_machine.rs \
  crates/hammer-service/tests/tcp_passive_open.rs
git commit -m "tcp(Fix): complete syn-rcvd retransmit and pending-open cleanup"
```

## Task 4: 接通 Module 9 `TIME-WAIT` retain / duplicate FIN / expiry cleanup，并删除 TCP 私有 RX 容器

**Files:**
- Modify: `crates/hammer-service/src/session/runtime.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/state_machine.rs`
- Modify: `crates/hammer-service/src/transport/tcp/time_wait.rs`
- Modify: `crates/hammer-service/tests/tcp_passive_open.rs`
- Create: `crates/hammer-service/tests/tcp_time_wait.rs`

**Interfaces:**
- Consumes:
  - `TcpConnection<TimeWait, C>::receive_time_wait(...)`
  - `SessionDriverRuntime::close_session(...)`
  - existing retained RX / session RX completion path in `SessionDriverRuntime`
- Produces:
  - `TimeWait` 路径上的 tuple retain/release 与 timer re-arm
  - out-of-order retained buffer 生命周期回到 session/runtime 通用路径

**Deliverable:**

- duplicate FIN 回 ACK 仍由 TCP 负责。
- tuple retain/release 与 expiry close 由现有 runtime 边界收口。
- 删除 `TcpSessionRx`、`rx_index`、`rx` pool 这套 TCP 私有 RX 容器；out-of-order retained buffer、ready delivery、释放路径全部并回 session/runtime 的现有通用 RX 持有与 completion 路径，TCP 只保留 `rcv_nxt`、SACK/DSACK 事实与状态迁移。
- 所有需要回写协议状态的地方只使用现有 `session_mut()` / `session_state_mut()`；session 生命周期动作继续只由 runtime 执行。

- [ ] **Step 1: 写失败测试**

在 `crates/hammer-service/tests/tcp_time_wait.rs` 增加：

```rust
#[test]
fn time_wait_duplicate_fin_returns_ack() {
    let connection = time_wait_connection();
    let packet = duplicate_fin_packet();

    let (_, segment, should_close) = connection.receive_time_wait(&packet);

    assert!(segment.expect("ack").flags().contains(TcpSegmentFlags::ACK));
    assert!(!should_close);
}

#[test]
fn time_wait_rst_requests_close() {
    let connection = time_wait_connection();
    let packet = rst_packet();

    let (_, segment, should_close) = connection.receive_time_wait(&packet);

    assert!(segment.is_none());
    assert!(should_close);
}
```

在 `crates/hammer-service/tests/tcp_passive_open.rs` 增加：

```rust
#[test]
fn tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry() {
    let (mut queue, session_id, local, remote) = fin_wait2_session();

    deliver_fin_to_fin_wait2(&mut queue, session_id);

    assert!(matches!(
        queue.session_state(session_id),
        Some(TcpConnectionState::TimeWait(_))
    ));
    assert!(queue.session_route_by_tuple(local, remote).is_some());
}

#[test]
fn tcp_time_wait_duplicate_fin_rearms_timer_and_reemits_ack() {
    let (mut queue, session_id) = time_wait_session();

    let segment = deliver_duplicate_fin_to_time_wait(&mut queue, session_id);

    assert!(segment.flags().contains(TcpSegmentFlags::ACK));
    assert!(time_wait_timer_is_armed(&queue, session_id));
}

#[test]
fn tcp_time_wait_timer_expiry_releases_tuple_and_session() {
    let (mut queue, session_id, local, remote) = time_wait_session_with_route();

    expire_time_wait_timer(&mut queue, session_id);

    assert!(queue.session_state(session_id).is_none());
    assert!(queue.session_route_by_tuple(local, remote).is_none());
}
```

补一个覆盖 RX 容器删除后的行为测试：

```rust
#[test]
fn established_out_of_order_payload_still_emits_sack_and_delivers_through_session_runtime() {
    let (mut queue, session_id) = established_session_with_sack_enabled();

    inject_out_of_order_payload(&mut queue, session_id, 11, b"bbb");
    let ack = inject_in_order_payload(&mut queue, session_id, 8, b"aaa");

    assert_eq!(segment_sack_blocks(&ack), []);
    assert_session_rx_bytes(&mut queue, session_id, b"aaabbb");
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cargo test -p hammer-service --test tcp_time_wait
cargo test -p hammer-service --test tcp_passive_open tcp_time_wait
```

Expected: FAIL

- [ ] **Step 3: 完整实现 `TIME-WAIT` 收口并删除 TCP 私有 RX 容器**

在 `crates/hammer-service/src/transport/tcp/state_machine.rs` 保持 TCP 只返回协议事实：

```rust
impl<C> TcpConnection<TimeWait, C>
where
    C: CongestionController,
{
    pub(crate) fn receive_time_wait(
        self,
        packet: &TcpPacket,
    ) -> (TcpConnection<TimeWait, C>, Option<TcpSegment>, bool);
}
```

在 `crates/hammer-service/src/session/runtime.rs` / `crates/hammer-service/src/session/protocol.rs` 把 out-of-order retained RX buffer 所需的能力收回 session/runtime 通用路径，要求：

```rust
// no TCP-specific runtime API
// no TCP-specific retained-RX owner type
// retain / enqueue / release remain generic session/runtime responsibilities
```

在 `crates/hammer-service/src/transport/tcp/session.rs` 完成：

```rust
// on duplicate FIN: enqueue ACK and rearm TIME_WAIT timer
// on reset/expiry: forget route and close session through runtime
// compute ACK/SACK facts from protocol state and session/runtime-held RX buffers
// deliver ready RX buffers through existing session runtime completion path
```

同时删除以下现有坏表面：

```rust
struct TcpSessionRx { /* delete */ }
```

- `TcpSessionRx`
- `protocol.rx_index`
- `protocol.rx`
- `rx()` / `rx_mut_or_alloc()` / `release_rx()`

实现结果要求：

- duplicate FIN 回 ACK 后重新 arm `TIME_WAIT` timer。
- 不新增 `TIME-WAIT` 专用 carrier 类型。
- out-of-order 保留仍然允许存在，但落点是 session/runtime 的通用 buffer 持有与 session RX 投递路径，不再由 TCP 自己持有一套 session buffer 容器。
- SACK/DSACK 仍由 TCP 根据协议事实生成，但 buffer 生命周期不再归 TCP 私有容器管理。
- 不新增任何 take/replace session state 风格的生命周期接口。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cargo test -p hammer-service --test tcp_time_wait
cargo test -p hammer-service --test tcp_passive_open
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/session/runtime.rs \
  crates/hammer-service/src/session/protocol.rs \
  crates/hammer-service/src/transport/tcp/session.rs \
  crates/hammer-service/src/transport/tcp/state_machine.rs \
  crates/hammer-service/src/transport/tcp/time_wait.rs \
  crates/hammer-service/tests/tcp_passive_open.rs \
  crates/hammer-service/tests/tcp_time_wait.rs
git commit -m "tcp(Refactor): remove tcp-private rx container and complete time-wait cleanup"
```

## Self-Review

### Spec coverage

- typed state 恢复/回写边界，已覆盖前置任务 1。
- out-of-order retained RX 通用持有能力，已覆盖前置任务 2。
- Module 7：close submission drain、local FIN、FIN retransmit、final ACK close cleanup，已覆盖 Task 1。
- Module 8：listener backlog admission，已覆盖 Task 2；`SynRcvd` retransmit 与 pending-open cleanup，已覆盖 Task 3。
- Module 9：`TIME-WAIT` retain、duplicate FIN re-ACK、expiry cleanup，以及删除 TCP 私有 RX 容器，已覆盖 Task 4。

### Placeholder scan

- 文档不再引入第二层协议抽象。
- 文档不再把当前 TCP 承载对象名字抬成未来协议标准。
- 文档把 `TcpSessionRx` 及其配套入口明确列入删除项，而不是改名保留。
