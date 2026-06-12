# Hammer TCP Worker Driver Node 设计

- **日期：** 2026-06-12
- **状态：** 草案，已按最新讨论收敛到 VPP worker input/driver node 对标
- **范围：** 只讨论 TCP 发送推进、定时器推进、app send/shutdown 输入推进的结构重做；不扩展到新的 TCP 协议功能

## 1. 背景

当前 TCP 发送路径在 `crates/hammer-service/src/service.rs` 上挂了一套 service 侧专用机制：

- `TcpOutputSignalRegistry`
- `start_tcp_flow_pump(...)`
- `drive_tcp_output(...)`
- `dequeue_tcp_output(...)`
- pacing / retransmit / persist timer 回调里直接 `signal(flow)`

这套模型的问题不是“能不能跑”，而是结构错位：

1. **发送推进被做成 per-flow async task**，而不是 worker 本地 data-plane 驱动行为。
2. **control plane / worker plane 的职责混在一起**，service 既管连接状态发布，又直接管发送推进时机。
3. **timer 事件没有对标 VPP worker 语义**。当前 `TcpWorkerEvent::TimerExpired` 在 `service.rs` 里基本是空操作，真正的推进靠 signal/pump，而不是 worker 自己的 input/driver tick。
4. **出现了额外特殊层**，例如 signal registry、output runnable/pump 一类机制；这不是 packet graph / worker driver 的自然形状。

用户要求的对标对象不是普通内部 node，而是 **VPP worker 侧 input node / driver node**。

## 2. VPP 对标结论

这次对标的关键不是“VPP 里有 timer wheel”，而是：

1. 每个 TCP 连接归属某个 worker。
2. 每个 worker 维护自己的 TCP 时间与 timer wheel。
3. worker 的 input/session queue node 周期性推进 transport time，并在同一 worker 上 expire/dispatch TCP timers。
4. 连接发送推进、timer expiry 后的处理、局部控制包发出，都发生在 **连接所属 worker**，而不是一个 service 侧 per-flow task。

也就是说，Hammer 里对标的形状应当是：

- **control thread**：只负责控制面状态发布、跨线程安全发布、共享 timer 注册入口；
- **worker driver/input node**：负责本 worker 的 TCP ready/drain/expire/process；
- **per-connection state**：仍然由连接自己持有，不搞全局“特殊发送器”。

## 3. Hammer 本地约束

### 3.1 DriverNode 语义

Hammer 已经有明确的 driver node 角色：

- `crates/hammer-adapter/src/node.rs`
  - `DriverNode` 是外部输入边界的运行时角色。
  - `register_driver(...)` 会把节点注册为 `NodeKind::Driver`。
- `crates/hammer-adapter/src/buffer.rs`
  - `schedule_driver_frame(node, frame)` 允许用空 frame 调度 driver node。
- `crates/hammer-adapter/src/node.rs`
  - `schedule_frame(..., allow_empty=true)` 最终进入 worker 本地 ready queue。
  - `run_ready_function_nodes(...)` 在 worker 上执行已调度节点。

所以 Hammer 现有 runtime 已经支持：

- driver node 注册；
- 空 frame 调度；
- worker 本地 ready 队列；
- driver/input node 风格的 tick。

这意味着 TCP 不需要再发明一套专用 wake/signal/pump 机制。

### 3.2 本地对标模板

`crates/hammer-service/src/tun/mod.rs` 里的 `TunInputDriverNode` 是当前最接近的本地模板：

1. 自己有 runtime slot / runtime data；
2. `Node::process` 不直接跑，真实逻辑走 descriptor `node_process`；
3. 通过 `DriverNode` 注册为 driver；
4. 依赖 runtime 调度，而不是 service 侧额外任务。

TCP 这次要对标的是这个 runtime 形状，而不是继续把发送推进塞进 `RuntimeAppControlHandle`。

## 4. 核心设计

### 4.1 新增一个 worker 侧 TCP driver/input node

新增一个 TCP worker driver node，职责是：

1. 在连接所属 worker 上推进 TCP 发送；
2. 在连接所属 worker 上处理 TCP timer expiry；
3. 在连接所属 worker 上吸收 app send/shutdown 输入；
4. 发出 ACK/SYN-ACK/FIN/RST/数据段等 TCP 输出。

这个 node 不是“业务协议节点”，而是 **worker 本地 TCP 驱动面**。

### 4.2 worker 侧 runtime 状态

每个 worker 的 TCP driver runtime 维护：

1. **ready connection 集**
   - 哪些连接需要再次尝试发送；
   - 包括新 app send、ACK 释放窗口、状态变更、timer expiry 后被重新标记 ready 的连接。

2. **timer ready 集 / timer wheel 观察结果**
   - 控制面 timer 到期后，不直接在 service 做 TCP 逻辑；
   - 只把“某连接某 timer 到期”投到对应 worker；
   - worker driver node 消费后执行真正的 retransmit/persist/delayed-ack/keepalive/time-wait 处理。

3. **worker-local app ingress**
   - app send：按 flow owner worker 进入本 worker；
   - app shutdown：同理；
   - driver node 在 worker 本地 drain，而不是启动 per-flow async pump。

4. **self-reschedule 状态**
   - 如果本轮还有未做完的 ready work，driver node 重新 schedule 自己；
   - 这替代今天的 `signal(flow)` 模式。

### 4.3 每个 connection 仍然管理自己的状态

这次设计不引入一个“共享 TCP 发送器”替连接做状态机。

仍然保持：

- 每个 connection 持有自己的发送/接收/关闭/拥塞/重传状态；
- driver node 只是 **遍历 ready 的连接并调用连接级 tx/timer step**；
- 连接的状态归属依旧是 per-connection，而不是全局 node state。

所以 user 提到的点是成立的：

- **每个 connection 管自己的状态**；
- 但 **推进它的执行面** 应该是 worker driver/input node，而不是 service pump。

### 4.4 为什么不是“一个独立 data plane node per connection”

不做 per-connection node，原因不是做不到，而是不符合本地 graph 和 VPP 形状：

1. Hammer 的 node registration 是静态 graph 节点，不适合把每个连接都做成一个 graph node。
2. VPP 也不是给每个 TCP 连接注册一个 node；它是 worker 侧 session/input node + per-connection state。
3. per-connection node 会让 graph 拓扑和生命周期管理过重，反而比现在更特殊。

所以正确形状是：

- **一个 worker driver/input node**
- **多个 per-connection state**
- **worker runtime 维护 ready set**

### 4.5 control plane 与 data plane 的交互

交互收敛成三类：

1. **publish**
   - control thread 发布 lookup / snapshot / shared tcp control state；
   - 这部分继续保留。

2. **wake**
   - control thread 或共享 timer 回调，不直接做发送；
   - 只负责把“连接 ready / timer expired”投递到 owner worker 的 TCP driver runtime。

3. **consume**
   - 真正的 tx drain / retransmit / persist / delayed ack / close emit 都在 worker driver node 里完成。

重点是：**control plane 不再直接 drive tcp output**。

## 5. app send / shutdown 输入设计

### 5.1 send

`hammer-runtime` 里已经有足够接近的 worker 本地入口：

- `AppRingHandle::pop_submission()`：同步取出 submission；
- `AppBackend::next_submission_entry()` / `AppRuntime::next_submission_entry()`：异步入口；
- `AppContext::send_on_flow(...)`：跨 worker 时会把 submission entry 投到 owner worker 的 ring。

这意味着：

- app send 不必继续绑定在 `next_send().await` 的 per-flow task 上；
- 可以改成 worker driver runtime 直接 drain flow owner worker 上的 submission entry。

### 5.2 shutdown

当前 shutdown 入口主要是：

- `try_push_tcp_shutdown(...)`
- `next_tcp_shutdown().await`

也就是 shutdown 现在更偏 async queue，而没有像 submission 一样的同步 pop 接口。

因此这次设计有两种可接受的落法，推荐前者：

1. **推荐**：为 tcp shutdown ring 增加同步 pop，然后并入 worker driver runtime drain；
2. 保守过渡：先保留 worker-local shutdown ingress，但收口到按 worker 一个 drain 面，而不是 per-flow 一个 pump。

无论哪种，都不能再保留 today 的 `start_tcp_shutdown_pump(flow, ...)` 形状。

## 6. 发送推进模型

driver node 每次被调度时做以下事情：

1. drain worker-local app send/shutdown 输入，标记相关 connection ready；
2. drain timer-expired 事件，标记相关 connection ready，并写入 connection 的 timer-fired 标志；
3. 对 ready connection 逐个执行 `tx_step`：
   - 如果 timer-fired，先做对应 timer handler；
   - 再检查当前发送窗口 / persist / delayed-ack / close state；
   - 生成要发出的 segment；
   - 通过现有 buffer + output backend 发出；
   - 更新 connection 的本地发送状态；
   - 决定是否仍需再次 ready。
4. 如果还有剩余 ready work，则重新 schedule driver node 自己。

这取代当前：

- `start_tcp_flow_pump`
- `drive_tcp_output`
- `TcpOutputSignalRegistry::signal(flow)`

## 7. timer 设计

当前 `RuntimeAppControlHandle::schedule_projected_tcp_worker_event(...)` 对
`TcpWorkerEvent::TimerExpired` 基本是空处理，这正是错位点。

重做后：

1. control/timer registry 仍然可以负责“某连接某 timer 到期”的注册和到期回调；
2. 到期回调只做一件事：把 timer-expired 事件投给 owner worker 的 TCP driver runtime，并 schedule driver node；
3. 真正的 retransmit / persist / delayed-ack / keepalive / time-wait 处理在 worker driver node 中执行。

也就是说：

- **timer registry 负责通知**
- **worker driver node 负责处理**

这才和 VPP worker input/session node 形状一致。

## 8. 对现有代码的裁剪原则

### 删除

- `TcpOutputSignalRegistry`
- `start_tcp_flow_pump(...)`
- per-flow `notify.notified()` output drain 模式
- service 侧把 pacing/retransmit/persist timer 直接翻译成 `signal(flow)` 的逻辑

### 下沉到 worker driver runtime

- `dequeue_tcp_output(...)` 背后的“找出下一个要发什么”逻辑
- pacing / persist / retransmit / close emit 的发送推进
- app send/shutdown drain

### 保留但改职责

- `TcpOutputRecord` / buffer-based segment emission
- `TcpOutputBackend`
- per-connection snapshot / congestion / retransmit queue
- shared tcp control plane / lookup publish / accept/syn-sent/established receive nodes

## 9. 非目标

这次结构改造 **不顺带** 做下面这些事：

- 新的 TCP 协议状态机功能；
- BBR/CUBIC/Reno 行为扩展；
- 重新设计整个 packet graph；
- 重构旧 DoH / 非 VPP 路径；
- 把每个 TCP 连接做成独立 graph node。

## 10. 最终结构判断

这次正确的落点应该是：

1. **TCP 输入处理**
   - 仍由现有 `TcpInputNode` / `TcpListenNode` / `TcpSynSentNode` / `TcpEstablishedNode` 负责接包。

2. **TCP 发送与 timer 推进**
   - 交给新的 **TCP worker driver/input node**。

3. **control plane**
   - 只做共享发布、注册、跨线程安全唤醒，不再直接 drive output。

换句话说，Hammer TCP 的完整形状应当是：

- receive path nodes
- worker driver/input tx/timer node
- per-connection state
- shared control plane

而不是：

- receive path nodes
- service-side special pump/signal layer
- per-flow async tasks
