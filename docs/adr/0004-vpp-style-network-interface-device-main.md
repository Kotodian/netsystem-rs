# VPP-Style Network, Interface, and Device Main Ownership

Status: accepted

Hammer adopts VPP's separation between `vnet_main_t`,
`vnet_interface_main_t`, and `vnet_device_main_t`. `NetMain` is the network
entry authority and owns `InterfaceMain`; `DeviceMain` remains independent and
is limited to device-input worker scope, receive statistics, and device-level
scheduling. The decision removes the current split interface/device registries
so that interface instances, hardware interfaces, software interfaces, and
queues have one owner.

## Decision

`NetMain` is the service-owned network authority corresponding to VPP
`vnet_main_t`. It owns the network-wide coordination entry point, the embedded
`InterfaceMain`, and the built-in `local0` hardware/software interface indices.
`InterfaceMain` corresponds to `vnet_interface_main_t`; it is not exposed as a
second process-global. Its state includes the VPP-style pools for hardware
interfaces, software interfaces, RX queues, and TX queues, together with
interface callbacks and address/MTU state.

`InterfaceRegistrationImage` is a service-owned declaration image, separate
from runtime `PluginMain` and runtime `RegistrationImage`. It contains the four
concrete interface registration faces. Each dynamic DSO exposes its image
through a service-owned ABI export. After dependency-ordered loading,
`InterfaceMain`'s init function uses `PluginMain` only for read-only symbol
lookup, then interprets and consumes the service image itself. The image is
initialization input only: `InterfaceMain` installs the four registration faces
into its active class and callback state, and runtime queries use that active
state rather than the image. `PluginMain` keeps the DSO alive but does not own,
decode, or copy the image; the image does not own plugin metadata, DSO lifetime,
or interface instances.

`DeviceMain` corresponds to `vnet_device_main_t` and remains an independent
owner. It stores only the input-side worker range, one aggregate RX statistic
per worker, and device scheduling cursors. Device instances, hardware
interfaces, software interfaces, and RX/TX queue records are owned by
`InterfaceMain` (or by an explicitly nested hardware-interface state), not by
`DeviceMain`.

The process-global access surface is owner-local:

```rust
pub static NET_MAIN: OnceLock<NetMain> = OnceLock::new();
pub static DEVICE_MAIN: OnceLock<DeviceMain> = OnceLock::new();
```

`NetMain::global()` and `DeviceMain::global()` return those values. There is no
independent `INTERFACE_MAIN` cell. `InterfaceMain` is reached through
`NetMain`'s owner-defined API; generic business code does not receive a
registry entry or an erased capability for it.

The VPP names are translated to Hammer names without inventing a common
interface `kind`:

| VPP concept | Hammer concept |
| --- | --- |
| `vnet_device_class_t` | `DeviceClass` |
| `vnet_hw_interface_class_t` | `HwClass` |
| `vnet_hw_interface_t` | `HwInterface` |
| `vnet_sw_interface_t` | `SwInterface` |

There is no `InterfaceRecord`, no software-interface class hierarchy, and no
single `kind` discriminator. A device instance is a hardware-interface
relationship, not a third generic interface record.

Network components are declared with derive proc macros owned by the service
component API. A declaration such as

```rust
#[derive(DeviceClass)]
#[device_class(name = "tuntap", tx_function = tuntap_intfc_tx)]
pub struct TunDevice;
```

generates a concrete registration item. `linkme` gathers device classes,
hardware-interface classes, and interface callbacks into the registration
image; handwritten path arrays are removed. Helper attributes use the VPP
field names. `DeviceClass` and `HwClass` retain the complete VPP field set,
including fields that Hammer does not currently consume. Unspecified optional
callbacks are `None`; runtime-owned indexes, links, selected TX functions, and
error tables are filled by the owner during installation and are not plugin
inputs. Format callbacks use Rust's `Display`/`fmt::Formatter` contract, and
unformat callbacks use `FromStr` or an explicit parser.

Callbacks use concrete function pointers. Interface callback priority keeps
VPP's `LOW`/`HIGH` values, defaults to `LOW`, and executes from low to high.
Callback flags and interface indices remain `u32`; hardware flags use typed
Rust bitflags and TX hash selection uses a typed Rust enum.

Callback dispatch preserves VPP's layers. Hardware-interface add/delete calls
the selected `HwClass` callback, then the selected `DeviceClass` callback, then
the global hardware-interface callback list by priority. Software-interface
add/delete and the MTU and admin-state notifications use their corresponding
global callback lists.

Class installation follows VPP's registration behavior. `InterfaceMain` assigns
class indices in VPP head-insertion traversal order: later-loaded DSO images
are installed before earlier images, and entries within a prepended image are
traversed in the same direction. It retains every installed class and does not
reject duplicate class names: the name lookup points to the most recent class
while earlier classes retain their assigned indices.

Registration and initialization follow the vendored VPP lifecycle. Hammer's
owner init chain is:

```text
interface_main_init
    -> net_main_init
    -> device_main_init
```

`interface_main_init` creates the interface pools and indexes and consumes the
service `InterfaceRegistrationImage`. `net_main_init` publishes the `NetMain`
that owns it and creates `local0`; `device_main_init` publishes `DeviceMain`.
Runtime `PluginMain` remains the owner of plugin loading and lifetime. TUN/TAP
interface instances are configured in normal configuration. `InterfaceMain` is
the runtime owner of the active interface registration state; `NetMain` is the
outer network owner.

Runtime plugin loading after the main loop remains available for runtime-owned
registrations. Interface components are startup registrations: a DSO with a
non-empty `InterfaceRegistrationImage` must be loaded before
`interface_main_init`; it cannot add classes or interface callbacks after that
image has been consumed.

The service `InterfaceRegistrationImage` keeps four independent VPP-shaped
registration faces:
`device_class_registrations`, `hw_interface_class_registrations`,
`hw_interface_callbacks`, and `sw_interface_callbacks`. Derive and callback
proc macros contribute directly to their respective service-owned images.
Built-in and startup-loaded DSO service images are consumed by `InterfaceMain`
in the VPP head-insertion order above; the runtime does not merge them into an
erased component enum, a `kind` field, or a generic registration hook.

Interface registration returns a hardware-interface index. The corresponding
software-interface index is obtained from the installed hardware-interface
state. Interface and queue identities remain raw `u32` VPP-style pool indices;
they do not use `Descriptor<T>` generations. Index reuse requires the owner to
quiesce worker access and remove RX/TX queues before releasing the interface
pool slot.

## Consequences

- Network ownership is discoverable through `NetMain`; interface and device
  plugins depend on the owner-defined service API instead of a shared registry.
- Device input scheduling and RX accounting stay independent from interface
  identity and queue topology.
- The change is intentionally breaking: the old interface control handle,
  interface record, independent interface global, device registry, and manual
  registration arrays are removed rather than wrapped.
- Complete VPP class fields are part of the declared model now, while runtime
  behavior for unsupported capabilities is deferred until Hammer implements
  those capabilities.
- Class name lookup intentionally follows VPP's last-registration-wins map;
  class indices remain the authoritative identity for installed interfaces.

## 变更清单

### 新增

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `hammer-service::net::NetMain` | 网络总入口，持有 `InterfaceMain` 与 `local0` indices；提供 owner-local global access | 新增 service authority；插件改用 `NetMain` API | init order、`local0`、global access integration test |
| 类型 | `hammer-service::interface::InterfaceMain` | VPP-style interface owner；持有 `HwInterface`/`SwInterface`/`RxQueue`/`TxQueue` pools、地址/MTU、callbacks | 替代 `InterfaceControlPlane`；无独立 global | pool ownership、index mapping、barrier publication test |
| 类型 | `hammer-service::device::{DeviceClass,HwClass,HwInterface,SwInterface}` | VPP class/instance model；完整 class fields，运行时字段由 owner 填充 | 新模型；插件 derive migration | compile-time derive and registration image test |
| 类型 | `hammer-service::interface::{RxQueue,TxQueue}` | Interface-owned queue records and queue-to-interface linkage | 替代 device-local queue records | queue install/removal and worker assignment test |
| API | `NetMain::global`, `DeviceMain::global` | 通过 owner-local `OnceLock` 获取 Main | 无 registry/erased alias | duplicate init and lookup tests |
| API | `#[derive(DeviceClass)]`, `#[derive(HwClass)]` 及 callback component derives | 生成 VPP-field registration items并由 `linkme` 汇总 | 替代手写 registration arrays | macro compile tests and DSO registration test |

### 修改

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型 | `DeviceMain` | 删除设备/接口/queue registry；只保留 worker scope、RX aggregate stats、scheduling state | breaking ownership change | ownership compile checks and RX/scheduling tests |
| 类型 | `GlobalMain` / service registration image | service Main 由 init function 创建；component inventories通过 service image 汇总，DSO 符号仅由 runtime loader 查找 | lifecycle and image shape change | startup image order and late interface-image rejection test |
| API | interface registration/query | registration returns `hw_if_index`; `sw_if_index`由对应 `HwInterface` 查询 | callers migrate from generic interface index | hardware/software index mapping test |
| API | `DeviceClass`/`HwClass` callback fields | 使用具体函数指针、VPP priority、typed flags/hash enum、Display/FromStr contracts | plugin declarations are source-compatible only after derive migration | callback ordering and formatting/parser tests |
| API | TUN/TAP configuration | 从 early config 移到 normal config；实例由 `InterfaceMain` 安装 | startup config phase behavior change | lifecycle config test |

### 删除

| 类型/API | 位置或标识 | 变更内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| 类型/API | `InterfaceControlPlane`, `InterfaceControlHandle` | 删除旧快照控制面与 handle | 无兼容 facade；TUN/IP callers一次迁移 | workspace compile |
| 类型 | `InterfaceRecord` | 删除非 VPP 的统一接口记录 | `HwInterface`/`SwInterface`分别承担领域状态 | type/API compile checks |
| 全局 | `INTERFACE_MAIN: ArcSwapOption<_>` | 删除独立接口 global | 通过 `NetMain` owner API访问 | global lookup test |
| 类型/API | `DeviceRegistration`, `DeviceRxQueue`, `DeviceTxQueue`及旧 device queue registry API | 删除 DeviceMain 对实例、硬件接口、queue 的兼任 | queue records迁移到 `InterfaceMain` | queue ownership and removal tests |
| API | `configure_interfaces` | 删除通用接口配置 helper；owner init/config 负责实例安装 | TUN 等插件迁移到 normal config + `NetMain` API | config integration test |
| API | service/plugin registration image 中的手写 path arrays | 删除显式列举 class/callback 的注册方式 | `linkme` registration image 自动汇总 | built-in and DSO inventory test |

没有序列化、持久化、IPC wire format 或 Binary API handler ABI 的迁移；这是
一次源码级 breaking change，所有 in-tree plugins 与 callers 在同一变更中
迁移。

## 实现核对项

以下是实现时必须逐字段对照 VPP 和现有 crate 合同的核对项，不是待用户
决策的架构问题：

- 完整 VPP class 字段的 Rust 函数指针签名（flow、RSS、EEPROM、MAC、TM 等）
  必须逐字段核对；不得按名称猜测，也不得引入 `va_list` 或 erased callback。
- 动态 DSO 的四个独立 service-owned registration 面必须通过明确的
  service ABI export 进入 `InterfaceMain`；runtime 只提供符号查找，不得把
  image 塞进 runtime `RegistrationImage`、回退到手写路径数组，或引入泛化
  hook。
- class index 与 name lookup 必须保持 VPP 语义：按 image 遍历顺序分配 index，
  不拒绝重复 name，名称表由最后安装项覆盖，旧 index 不回收或重写。
- `HwInterface`、`SwInterface`、`RxQueue`、`TxQueue` 使用现有 `Pool<T>` 的
  raw `u32` index 和 reuse 合同；不得改用 `Descriptor<T>` generation 或另造
  一套索引语义。删除时必须先 quiesce worker 并移除 RX/TX queues，再释放
  interface pool slot。

callback 错误回滚细节按本轮明确指示暂不展开，不作为本 ADR 的未决决策。

## 依据与假设

### 当前项目事实

- `hammer-service::interface` 当前定义 `InterfaceControlPlane`、
  `InterfaceControlHandle`、`InterfaceRecord` 和 `INTERFACE_MAIN`。
- `hammer-service::device` 当前让 `DeviceMain` 持有 device、RX queue 和 TX
  queue vectors；TUN plugin 直接使用这些 API。
- `hammer-service` 和每个 plugin 当前通过
  `__declare_registration_image!` 手写列出 registrations。

### Vendored VPP 事实

- `third_party/vpp/src/vnet/vnet.h` 定义 `vnet_main_t` 并嵌入
  `vnet_interface_main_t`。
- `third_party/vpp/src/vnet/interface.h` 定义 device class、hardware/software
  interface 和 RX/TX queue 相关字段及 callback contracts。
- `third_party/vpp/src/vnet/interface.c` 通过 `vnet_main_t` 操作
  `vnet_interface_main_t`，并在注册 hardware interface 后创建对应 software
  interface。
- `third_party/vpp/src/vnet/devices/devices.h`/`devices.c` 将 device-main
  状态用于设备输入调度和统计，而不是通用接口注册表。
- `third_party/vpp/src/vnet/misc.c` 创建内建 `local0`；
  `third_party/vpp/src/vnet/unix/tuntap.c` 在设备配置路径创建 TUN/TAP
  interface。
- `third_party/vpp/src/vlib/init.c`、`main.c` 和 `unix/plugin.c` 规定配置、
  静态 registration、init function、normal config 与 main loop 的生命周期
  顺序。

### 已确认设计与实现推断

- Rust `OnceLock` 与 owner-local `global()` 能表达 Main 的一次性进程所有权，
  也能避免把 `InterfaceMain` 再包装成通用 registry capability。
- 将 class declaration 与 registration image 解耦需要 service-owned 的四个
  registration 面。动态 DSO 通过 service ABI export 暴露 image；
  `PluginMain` 只负责加载、保活和只读符号查找，`InterfaceMain` 负责解析和
  安装 service image，因此不改变现有 runtime/service 依赖方向，也不需要
  泛化 ABI hook。

### 历史记录复核

- 早期讨论中出现过“通过 `GlobalMain` registry 按类型获取 Main”的表述，后续
  决策改为 owner-local globals；实现前应检查历史 ADR/issue 是否仍要求 generic
  registry，避免两套发现机制并存。
- callback failure semantics 已按本轮指示跳过，不纳入本 ADR 的决策范围。
