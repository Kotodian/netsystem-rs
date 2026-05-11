<!--
TODO badges: CI / license / crates.io —— 等对应基础设施落地后再接，
本次先用 HTML 注释占位。
-->

# hammer-ios-rs

> 专为 iOS NetworkExtension（NetExt）打造的 Rust VPN 引擎。

读之前先记住三件事：

- iOS 优先。Swift 端通过 uniffi 调 Rust；整套运行时围绕 NEPacketTunnelProvider extension 能给的预算来设计。
- 热路径围着 NetExt 的约束转——内存上限、个位数 worker 线程、生命周期 suspend / wake、网络接口切换。
- 内建出口协议（hysteria2 / direct / urltest / block，可选 wireguard）+ DNS 传输（udp / tcp / doh / hosts / local）+ 一个 TUN 入口，全部由一份 TOML 配置驱动。

<!-- TODO: 快速上手 / 配置参考 / FFI 用法 / 开发指南 / 许可证 -->

## 架构

### Workspace 布局

工程拆成 6 个 crate，逐层往上叠：

| crate | 角色 |
|---|---|
| hammer-core | 基础类型——配置 schema、错误、生命周期、metrics、log、网络原语。零业务逻辑 |
| hammer-adapter | 跨 crate 的契约层——出入口、DNS、平台接口、各类管理器的抽象。让实现层跟 FFI 调用方解耦 |
| hammer-component-macros | 一个 proc macro，把出入口实现登记到 runtime 注册表 |
| hammer-runtime | 所有业务实现——出口协议、DNS 传输、router、TUN 入口、user-space IP 栈、双 tokio runtime |
| hammer-ffi | uniffi 生成的 Swift FFI——服务句柄、平台回调接口、文件描述符桥接 |
| hammer-uniffi-bindgen | 工具二进制，负责生成 Swift binding + xcframework |

依赖方向严格单向，避免循环：`ffi → runtime → {adapter, core}`，`adapter → core`。

### 分层图

```
                         ┌───────────────────────────────┐
            iOS Swift    │   NEPacketTunnelProvider      │
            (NetExt)     └──────────────┬────────────────┘
                                        │ uniffi
                                        ▼
        ┌─────────────────────────────────────────────────────┐
        │  FFI       服务句柄 / 平台回调 / 文件描述符桥        │
        └──────────────┬──────────────────────────────────────┘
                       ▼
        ┌─────────────────────────────────────────────────────┐
        │  Runtime                                            │
        │       ┌──────────────────────────────────────┐      │
        │       │ 控制面 (单线程)                       │      │
        │       │   生命周期调度 / 指标 / 日志 / 命令    │      │
        │       └──────────────────────────────────────┘      │
        │       ┌──────────────────────────────────────┐      │
        │       │ 数据面 (多线程)                       │      │
        │       │   入口 → 路由 → 出口 → 远端           │      │
        │       │   DNS 查询任务                        │      │
        │       └──────────────────────────────────────┘      │
        └──────────┬───────────────────────┬──────────────────┘
                   ▼                       ▼
       ┌──────────────────────┐  ┌──────────────────────────┐
       │  Adapter             │  │  Core                    │
       │  契约 trait 集合      │  │  配置 / 生命周期 / 指标   │
       └──────────────────────┘  └──────────────────────────┘
```

### Runtime 模型——控制面 vs 数据面

整套服务起来后有两条互不阻塞的执行链路：

**控制面**：单线程 runtime，负责生命周期阶段调度（启动、关闭、暂停、唤醒、网络重置）、周期 metrics 汇总、log 序列化、FFI 命令处理。设计目标只有一个——永远不被业务任务阻塞。即使某个代理连接卡住，log 和指标还能照常上报、FFI 命令照常响应。

**数据面**：多线程 runtime，跑所有"跟流量相关"的任务：TUN 设备读写、路由分发、出口拨号、DNS 查询、IP 栈轮询、隧道加解密。NetExt 上 worker 通常配 1–2 个，省 CPU 也省内存。

写代码不用关心当前在哪条 runtime——内部有自动机制把每个新任务投到数据面上。

### 生命周期阶段

启动是一个分四阶段的有序过程，所有子系统按同一顺序响应：

| 阶段 | 这一阶段在做什么 |
|---|---|
| 初始化 | 解析配置、构造各子系统（不做 IO）、注册平台监听器 |
| 启动 | 同步 IO——绑 socket、打开 fd、解析 bootstrap 地址 |
| 启动后 | spawn 后台任务——出口探测、warm-up 拨号、定时 metrics |
| 已运行 | 每个子系统把"对外可服务"旗标拨亮 |

各子系统之间还有先后顺序：证书要先于网络，网络要先于 DNS，DNS 要先于路由器，等等。

### 概念

读 dispatch 路径之前要先认这 4 个词。

| 名词 | 含义 |
|---|---|
| **Inbound** | 流量入口——数据怎么进入工程。可以是监听一个 SOCKS / HTTP 端口，也可以是接管 iOS NetExt 给的虚拟网卡（TUN）。每个 inbound 把读到的数据交给 router 决策 |
| **Outbound** | 流量出口——一条流量离开工程时实际走的方式：直连、走加密代理隧道、拒绝、按延迟自动挑最快的子节点。Router 决定每条流量走哪个 outbound，流量就从那里出去 |
| **Endpoint** | 比 outbound 高一层——同一个"协议端点"**同时具备 inbound 和 outbound 能力**。典型：WireGuard。隧道两头各是一个 endpoint，对端发来的流量从这里"入"，本端要发出去的流量从这里"出" |
| **Router** | 流量决策器。按规则匹配每条进来的流量（协议、域名、目的 IP、来自哪个 inbound 等），决定走哪个 outbound，或者直接拒绝。整个 hammer 的"分流"语义都集中在这里 |

其他细节（DNS 传输、user-space IP 栈、boringtun 等）在下面对应章节随上下文解读。

### Dispatch 路径

**TCP / UDP 流量**

iOS 把虚拟网卡的文件描述符递给 TUN 入口，入口把进来的 IP 包做两件事：一是把 TCP 流转交给宿主机内核的 TCP 栈（在本地起一个 listener、把目标地址改写到它身上，借内核帮忙完成连接重组）；二是把 UDP / ICMP 单独走 hijack 通道。两条路都会同时抽出五元组 + sniff 出来的域名作为路由的输入。

```
iOS 虚拟网卡
  ↓
TUN 入口
  ↓  (抽五元组 + sniff 域名)
Router 匹配规则
  ↓
选定 Outbound
  ↓
  ├─ hysteria2  →  QUIC 隧道       →  远端
  ├─ direct     →  系统 socket     →  远端
  ├─ wireguard  →  user-space IP 栈 + WireGuard 加密  →  远端 peer
  └─ urltest    →  转交给当前最快的子 outbound
```

user-space IP 栈在生产路径上**只在 WireGuard 出口内部出现一次**——因为 WireGuard 解密出来的还是 IP 包，必须再过一层 IP 栈才能拿到 TCP / UDP 连接。hysteria2 / direct / urltest 完全不碰 user-space IP 栈。

**DNS 查询**

```
DNS 查询 (被 hijack 的 53 端口，或者上层直接调)
  ↓
DNS 路由匹配 (按域名规则、带 LRU)
  ↓
选定 DNS 传输 (UDP / TCP / DoH / hosts / local)
  ↓
上游
  ↓
按 TTL 缓存 + 写入反向映射 (ip → 域名)
  ↓
返回调用方
```

### Feature 模型

cargo feature 分四组：

- **入口**：`inbound-tun`（默认开）。
- **出口**：`outbound-hysteria2 / outbound-direct / outbound-block / outbound-urltest`（默认全开）；WireGuard 是 endpoint 协议，runtime feature 为 `endpoint-wireguard`（默认关，iOS xcframework 默认不带，省体积）。
- **DNS**：`dns-udp / dns-tcp / dns-hosts / dns-local`（默认开），`dns-https`（默认关）。
- **基础设施**：`endpoint`（endpoint 协议的公共依赖）、`ipstack`（user-space IP 栈）、`probe`（出口延迟探测，默认开）。

设计原则：
- 任何独立出口 / 协议都挂在自己的 cargo feature 后面。
- 可观测性基础设施（metrics 等）带 gate 但默认**开**。
- 只有出口 / 协议这类独立功能模块默认**关**。

### Platform 集成

Swift 端的 NEPacketTunnelProvider 子类持有一个 hammer 服务句柄。服务对外暴露生命周期接口（启动、关闭、暂停、唤醒、网络重置）和观测接口（指标导出、延迟探测）。Swift 反向通过回调接口把平台能力喂回来：TUN 文件描述符、网卡监控、Wi-Fi 状态、证书提供方、日志输出。

iOS 的 path-update / wake 事件应当触发一次网络重置——它会扇出到所有出口，把睡眠期间变 stale 的 cached 连接干掉（睡眠唤醒后 QUIC 重连卡顿那种问题就靠这条路径解决）。

### 配置示例

格式是 **TOML**。下面三个例子按典型出口拆开，每个都带完整的 DNS 分流规则，字段名与 production demo（`HammerVPNDemo-rs-wg`）对齐。

**例 1 —— Hysteria2 代理（默认走代理 + 国内直连 + 拦截广告 + DNS 同步分流）**

```toml
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun"
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1408
stack = "system"
auto_route = true
sniff = true
hijack_dns = true

[dns]
final = "remote"
strategy = "ipv4_only"

[[dns.servers]]
type = "https"
id = "remote"
server = "https://1.1.1.1/dns-query"
via = "proxy"

[[dns.servers]]
type = "udp"
id = "domestic"
server = "223.5.5.5"

[[dns.rules]]
domain_keyword = ["doubleclick", "analytics"]
action = "reject"

[[dns.rules]]
domain_suffix = [".cn", "baidu.com"]
server = "domestic"

[[outbounds]]
type = "hysteria2"
id = "proxy"
server = "example.com"
server_port = 443
password = "demo"

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "block"
id = "reject"

[[route.rules]]
domain_keyword = ["doubleclick", "analytics"]
outbound = "reject"

[[route.rules]]
domain_suffix = [".cn", "baidu.com"]
outbound = "direct"

[route]
final = "proxy"
auto_detect_interface = true
```

**例 2 —— WireGuard endpoint（runtime 需启用 `endpoint-wireguard`；FFI/打包入口仍可用 `wireguard` feature，对应 production demo）**

```toml
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun"
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1400
stack = "system"
auto_route = true
sniff = true
hijack_dns = true

[dns]
final = "default"
strategy = "ipv4_only"

[[dns.servers]]
type = "udp"
id = "default"
server = "223.5.5.5"
via = "wg-out"

[[dns.servers]]
type = "local"
id = "local"

[[dns.rules]]
domain_suffix = ["ifconfig.so"]
server = "local"

[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "BASE64_PRIVATE_KEY"
address = ["10.0.0.2/32"]
[[endpoints.peers]]
public_key = "BASE64_PEER_PUBLIC_KEY"
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
persistent_keepalive_interval = 25

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "block"
id = "block"

[[route.rules]]
domain_suffix = ["ifconfig.so"]
outbound = "direct"

[route]
final = "wg-out"
auto_detect_interface = true
```

**例 3 —— Direct 直连（不挂代理；本地域名走 local 解析、广告关键词拦截）**

```toml
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun"
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1408
stack = "system"
auto_route = true
sniff = true
hijack_dns = true

[dns]
final = "default"
strategy = "ipv4_only"

[[dns.servers]]
type = "udp"
id = "default"
server = "223.5.5.5"

[[dns.servers]]
type = "local"
id = "local"

[[dns.rules]]
domain_suffix = [".lan", ".local"]
server = "local"

[[dns.rules]]
domain_keyword = ["doubleclick", "analytics"]
action = "reject"

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "block"
id = "block"

[[route.rules]]
domain_keyword = ["doubleclick", "analytics"]
outbound = "block"

[route]
final = "direct"
auto_detect_interface = true
```
