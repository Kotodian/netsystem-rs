# ADR-0027: VPP 风格 tuntap 控制面第一阶段

Status: accepted

Date: 2026-09-19

Implementation: complete for issue #335

本文记录 issue #335 的实现设计。第一阶段完成 Linux TUN/TAP 创建与删除、tuntap
DeviceClass/HwClass 注册，以及 `InterfaceMain` 中 hw/sw interface 的创建与删除。IP 地址、
punt/inject packet path、normal-interface packet path、FileMain 和 RX/TX Node 均延期。

设计依据是 vendored VPP：

- `third_party/vpp/src/vnet/unix/tuntap.c`
- `third_party/vpp/src/vnet/unix/tuntap.h`
- `third_party/vpp/src/vnet/interface.c`
- `third_party/vpp/src/vnet/interface.h`

## 1. 实现前置能力

以下 Rust surface 必须先具备。代码块是目标类型和方法签名，不是本轮实现代码。

### 1.1 Plugin DeviceClass/HwClass 必须进入 InterfaceMain

VPP 的 `VNET_DEVICE_CLASS` 和 `VNET_HW_INTERFACE_CLASS` 在
`vnet_interface_init` 时进入 `vnet_interface_main_t`。Hammer 必须先完成 ADR-0004 已接受但
尚未落地完整的 DSO interface registration image，不能让 tuntap 在 config callback 中临时
构造 class，也不能假定 class index 为 0。

该前置项沿用 ADR-0004 已确定的 `InterfaceRegistrationImage` 与 service-owned DSO export。
registration image 如何从 startup DSO 到达 `InterfaceMain` 是 ADR-0004 的实现责任。本 ADR
不为此增加 PluginMain traversal、registration-image consumer 或 plugin-specific capability
API；只补齐 ADR-0004 已规定由 `InterfaceMain` 持有的class-name indexes。tuntap DSO只通过
下面两个class declarations向 ADR-0004 已定义的image作贡献。

service image 还必须声明 VPP `misc.c:27-34` 对应的内建 local classes：

```rust
#[derive(hammer_component_macros::DeviceClass)]
#[device_class(name = "local")]
pub struct LocalDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "local")]
pub struct LocalHwClass;
```

`NetMain::init` 必须从 `InterfaceMain` 已安装的 class-name index 取得 `local` 的两个class
index，再创建 `local0`。当前
`register_hardware_interface(0, 0, 0, 0)` 假设必须删除；否则 tuntap image 被消费后，plugin
class 可能占用 index 0，`local0` 会错误绑定到 tuntap class。class-name index 本来就属于
ADR-0004 的 `InterfaceMain` active class state，不增加第二套plugin index storage。

`interface_main_init` 的固定顺序是：

```text
construct unpublished InterfaceMain
-> consume service image
-> consume startup plugin images in load order
-> publish InterfaceMain through NetMain ownership
-> run normal config callbacks
```

### 1.2 tuntap class declarations

第一阶段必须真实注册两个 class。它们对应 VPP `tuntap_dev_class` 和
`tuntap_interface_class`，不是仅在文档里写名字。

```rust
pub type FormatDeviceNameFn = for<'a, 'b> fn(
    device_instance: u32,
    formatter: &'a mut std::fmt::Formatter<'b>,
) -> std::fmt::Result;

pub struct DeviceClass {
    // Existing class fields are unchanged.
    pub index: u32,
}

pub struct HwClass {
    // Existing class fields are unchanged.
    pub index: u32,
    pub flags: HwClassFlags,
}

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tuntap_interface_name
)]
struct TuntapDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(
    name = "tuntap",
    flags = HwClassFlags::P2P
)]
struct TuntapHwClass;

fn format_tuntap_interface_name(
    device_instance: u32,
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result;
```

class flag 必须是 typed flag：

```rust
bitflags::bitflags! {
    pub struct HwClassFlags: u32 {
        const P2P = 1 << 0;
    }
}
```

`#[derive(DeviceClass)]`/`#[derive(HwClass)]` 必须把 registration 加入当前 DSO 的 service
distributed slices，并支持上面的 `format_device_name`/`flags` attributes。registration中的
`index`是普通`u32`；`InterfaceMain`消费image时给安装到active class vector的副本写入index，
同时更新VPP同义的class-name indexes。derive不生成额外static、`OnceLock`、atomic或index
wrapper。当前只生成name constructor的derive不满足前置条件。

本阶段不设置 `TuntapDeviceClass::tx_function`。这不是空 TX Node：VPP
`vnet_register_interface` 在 device class 没有 TX function 时走 `no_output_nodes`
（`interface.c:883-884, 1054-1060`），仍创建 hw/sw interface。真实 tuntap TX function 与
Node 在第 8 节延期，不提交占位函数冒充 packet path。

### 1.3 InterfaceMain 创建、状态与删除 API

VPP 默认 punt/inject branch 调用 `vnet_register_interface`，然后将 hardware link 和
software admin 状态置 up。Hammer 第一阶段必须通过 `InterfaceMain` owner API完成同样关系。

```rust
bitflags::bitflags! {
    pub struct HwInterfaceFlags: u32 {
        const LINK_UP = 1 << 0;
    }

    pub struct SwInterfaceFlags: u32 {
        const ADMIN_UP = 1 << 0;
    }
}

impl InterfaceMain {
    pub fn device_class_index(&self, name: &str) -> u32;

    pub fn hw_class_index(&self, name: &str) -> u32;

    pub fn register_hardware_interface(
        &self,
        device_class_index: u32,
        device_instance: u32,
        hw_class_index: u32,
        hw_instance: u32,
    ) -> InterfaceResult<u32>;

    pub fn hardware_interface(
        &self,
        hw_if_index: u32,
    ) -> &HwInterface;

    pub fn set_hardware_flags(
        &self,
        hw_if_index: u32,
        flags: HwInterfaceFlags,
    ) -> InterfaceResult<()>;

    pub fn set_software_flags(
        &self,
        sw_if_index: u32,
        flags: SwInterfaceFlags,
    ) -> InterfaceResult<()>;

    pub fn delete_hardware_interface(
        &self,
        hw_if_index: u32,
    ) -> InterfaceResult<()>;
}
```

`device_class_index`与`hw_class_index`读取`InterfaceMain`在class安装时建立的两个name
indexes，对应VPP的`device_class_by_name`和`hw_interface_class_by_name`。它们只用于已完成
startup registration后的内建/plugin class；name缺失表示启动顺序或registration image违反
invariant，因此直接终止启动，不返回`Option`，也不定义recoverable `ClassMissing`。

`register_hardware_interface` 必须：

1. 校验两个 class index 已安装；
2. 用选中 class 的 `format_device_name(device_instance)` 得到 `tuntap-0`；
3. 创建相互关联的 `HwInterface` 和 `SwInterface`；
4. 执行 VPP 对应的 software/hardware create callbacks；
5. 返回 `hw_if_index`，由 `HwInterface.sw_if_index` 取得 software index；
6. callback 失败时回滚两个 pool slot、name index 和已执行 callback 的贡献。

`hardware_interface(hw_if_index)` 对应 VPP `vnet_get_hw_interface`，不是
`vnet_get_hw_interface_or_null`。调用者刚从成功的 `register_hardware_interface` 得到 index，
对应 pool slot 不存在只能是 `InterfaceMain` 自身违反 invariant，因此直接返回 borrow；不得把
该 impossible state 降级成 `Option` 或 tuntap recoverable error。

`delete_hardware_interface` 负责 flags down、callbacks、queue/address 清理以及 hw/sw pool
slot 释放。tuntap plugin 不取得 `InterfaceMain` 内部可变引用，也不复制一套 interface
registry。

### 1.4 tuntap returned errors

VPP `tuntap_config` 返回具体调用点产生的 `clib_error_t *`。Hammer 对应的 plugin-owned
Linux error set如下；variant 与 VPP 的 returned-error branch 及错误文本一一对应，不增加
catch-all `Io`、`InvalidConfig`、抽象 lifecycle 分类或 message-only variant：

```rust
#[hammer_component_macros::runtime_error(subsystem = "tuntap")]
#[derive(Debug, thiserror::Error)]
enum TuntapConfigError {
    #[error("open /dev/net/tun")]
    OpenDevNetTun {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl TUNSETIFF")]
    TunSetIff {
        #[source]
        source: std::io::Error,
    },
    #[error("TUNSETPERSIST")]
    TunSetPersist {
        #[source]
        source: std::io::Error,
    },
    #[error("socket")]
    Socket {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl SIOCGIFINDEX")]
    GetInterfaceIndex {
        #[source]
        source: std::io::Error,
    },
    #[error("bind")]
    Bind {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl FIONBIO")]
    SetNonblocking {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl SIOCSIFMTU")]
    SetMtu {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl SIOCGIFFLAGS")]
    GetInterfaceFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl SIOCSIFFLAGS")]
    SetInterfaceFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("ioctl SIOCGIFHWADDR")]
    GetHardwareAddress {
        #[source]
        source: std::io::Error,
    },
}
```

`InterfaceError` 仍由 `InterfaceMain` 拥有，不包装成
`TuntapConfigError::Interface`。这使 VPP syscall errors 与 Hammer owner errors保持可区分，也
避免凭空增加 tuntap error category。

`#[runtime_error(subsystem = "tuntap")]` 复用现有 owner-to-runtime conversion，将 concrete
`TuntapConfigError` 作为 source 放入现有 `RuntimeError::Subsystem`；它不改变上述VPP错误
分类。`InterfaceError` 已使用 `subsystem = "interface"`，因此 InterfaceMain failure保持其
owner subsystem。`subsystem = "tuntap"` 是错误声明的强制结构化metadata，不能省略、不能从
display text推断，也不能在调用点临时拼字符串。这里不新增 `RuntimeError` variant、boxed
helper或config callback API。

### 1.5 CI capability

真实测试必须在 privileged Linux CI job 的独立 network namespace 内运行并提供
`/dev/net/tun`。本地 agent 不创建 host TUN/TAP，不运行 daemon/lab。

## 2. 目标 Rust 类型与方法

### 2.1 配置

原来的 `encapsulation` 和 `mode` 是 Hammer 自行抽象，删除。第一阶段直接采用 VPP 配置
事实：

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TuntapConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    ethernet: bool,
}

impl Default for TuntapConfig {
    // enabled = false
    // name = "vnet"
    // mtu = 4096 + 256
    // ethernet = false
}
```

目标 TOML：

```toml
[plugin.tuntap]
enabled = true
name = "vnet"
mtu = 4352
ethernet = false
```

`ethernet = false` 对应 `IFF_TUN | IFF_NO_PI`，表示 fd 上收发的是三层 packet；它不负责给
接口设置 IP 地址。`ethernet = true` 对应 `IFF_TAP | IFF_NO_PI`，表示 fd 上收发 Ethernet
frame。

本阶段固定走 VPP 默认的 `have_normal_interface = 0` branch，因此不提供 `mode` 或
`have_normal_interface` 配置。punt callback 尚未接入并不改变 interface registration branch；
它在第 8 节延期。未实现字段由 `deny_unknown_fields` 拒绝，不能静默接受。

### 2.2 Plugin owner Main

`TuntapMain` 对应 VPP file-scope `tuntap_main_t`。它是 tuntap plugin 自己的 process-global
authority，按现有 plugin Main 模式由 owner-local `OnceLock<TuntapMain>` 按值持有，不进入
`GlobalMain`、runtime registry 或 `PluginMain`。

第一阶段的 Main 直接保留 VPP `tuntap_main_t` 中本阶段使用的字段，不增加 State/Instance
包装：

```rust
struct TuntapMain {
    dev_net_tun_fd: std::os::fd::RawFd,
    dev_tap_fd: std::os::fd::RawFd,
    is_ether: bool,
    tun_name: String,
    mtu_bytes: u32,
    ether_dst_mac: [u8; 6],
    hw_if_index: u32,
    sw_if_index: u32,
}

static TUNTAP_MAIN: std::sync::OnceLock<TuntapMain> =
    std::sync::OnceLock::new();

impl TuntapMain {
    fn init(config: TuntapConfig) -> RuntimeResult<Self> {
        let mut main = Self {
            dev_net_tun_fd: -1,
            dev_tap_fd: -1,
            is_ether: false,
            tun_name: config.name,
            mtu_bytes: config.mtu,
            ether_dst_mac: [0; 6],
            hw_if_index: 0,
            sw_if_index: 0,
        };

        // tuntap_config 对应的 Linux 与 InterfaceMain 初始化在这里完成。
        // 全部成功后返回完整 Main；错误时直接执行同一 owner 的清理。
        Ok(main)
    }
}
```

`dev_tap_fd` 沿用 VPP 字段名；它在第一阶段只承担 provisioning `PF_PACKET` socket，不是
Hammer 自行抽象出来的第二种设备。`TuntapMain::init(config)` 接收解析后的配置，与 VPP config
开头相同，将两个fd初始化为 `-1`、MAC初始化为全零，并从config写入name与MTU；随后在同一
owner 方法内完成 Linux 与 `InterfaceMain` 初始化。`is_ether`先保持`false`；只有enabled且
通过VPP同样的non-root early return后才写入`config.ethernet`。config callback只修改尚未发布
的局部`TuntapMain`；
Linux与InterfaceMain创建全部成功后才调用`TUNTAP_MAIN.set(main)`。disabled或non-root分支
发布fd仍为`-1`的完整Main。创建失败时逆序rollback且不发布Main。

fd使用`RawFd`是对齐VPP exit的明确所有权：normal config取得fd，config error path或唯一一次
main-loop exit负责close；static Main不在运行期take、替换或drop。第一阶段没有worker读取
`TuntapMain`，因此不需要`UnsafeCell`、`unsafe impl Send/Sync`、mutex、atomic或thread-local。
不得增加独立`TUNTAP_CONFIG`、lifecycle `init_function`、`global()`、installer、State/Instance
wrapper、`configure`、`activate`或`TuntapMain::delete`方法；删除由VPP同名`tuntap_exit`
lifecycle callback负责。`tuntap_config`只调用`TuntapMain::init(config)`并将成功返回的完整值
一次写入`TUNTAP_MAIN`，不增加额外的发布、interface-registration或Linux-cleanup helper。

### 2.3 Lifecycle callbacks

```rust
#[hammer_component_macros::config_function(
    name = "tuntap_config",
    section = "plugin.tuntap"
)]
fn tuntap_config(config: TuntapConfig) -> RuntimeResult<()>;

#[hammer_component_macros::main_loop_exit_function(
    name = "tuntap_exit"
)]
fn tuntap_exit() -> RuntimeResult<()>;
```

不存在 `tuntap_activate`、`TuntapMain::activate` 或 main-loop-enter registration。VPP 在
`tuntap_config` 内创建 Linux 设备并注册 interface；Hammer 第一阶段也在同一个 normal config
callback 内完成并发布 `TuntapMain`。因为 normal config 本身就是该 VPP owner 的初始化点，
不把配置提前成 early config，也不增加一个晚于或早于它的 init callback。graph/File work既然
延期，就没有需要延后的 activation。

## 3. 范围与完成定义

第一阶段完成条件：

1. tuntap plugin DSO 的 DeviceClass/HwClass image 在 `interface_main_init` 被消费；
2. `enabled = false` 不创建 Linux 或 Hammer interface，并发布fd为`-1`的完整 Main；
3. `enabled = true` 执行 VPP 对应 Linux provisioning；
4. config 从 `InterfaceMain` 的class-name indexes取得`tuntap`的两个class index；
5. 调用 `register_hardware_interface(dev_class, 0, hw_class, 0)`；
6. 保存对应 `hw_if_index`/`sw_if_index`，设置 hardware link up、software admin up；
7. config 任一步失败时按已完成步骤逆序清理，不发布 `TuntapMain`；
8. main-loop exit 将 Hammer interface down并删除，再将 Linux interface down、关闭
   persistence和fds；
9. class declarations 保留到 DSO 卸载，不随 interface instance 删除。

第一阶段不完成：IP 地址与 alias callback、FIB/table 过滤、punt callback、FileMain、RX/TX
queue、buffer I/O、packet counters、normal interface、多个实例、runtime CRUD、Binary API、
CLI 以及非 Linux backend。

## 4. VPP 证据账本

| ID | vendored VPP source | 已验证行为 | Hammer 决策 |
| --- | --- | --- | --- |
| V1 | `tuntap.c:115-120, 462-490` | 默认 name=`vnet`、MTU=`4352`；配置含 enable/disable、name、mtu、ethernet、have-normal-interface；未知输入报错 | 第一阶段保留创建所需四字段；normal-interface延期且未知字段失败 |
| V2 | `tuntap.c:492-509` | 默认 disabled；non-root warning后成功；TUN/TAP均带`IFF_NO_PI` | 结果逐项对齐，不把non-root改成error |
| V3 | `tuntap.c:510-530` | open `/dev/net/tun`、`TUNSETIFF`、`TUNSETPERSIST(1)` | 使用三个一对一 typed errors |
| V4 | `tuntap.c:532-563` | 创建 `PF_PACKET/SOCK_RAW` provisioning socket，取ifindex并bind | 第一阶段照做，不改成另一种socket语义 |
| V5 | `tuntap.c:565-608` | fd设nonblocking；设置MTU；读取并设置`IFF_UP|IFF_RUNNING`；TAP读取MAC | 第一阶段照做，errors逐调用点对齐 |
| V6 | `tuntap.c:610-640` | normal branch用Ethernet registration；默认branch读取两个已安装class index注册interface并置link/admin up | 第一阶段只实现默认branch并真实调用InterfaceMain；Rust从同一owner的class-name index取得安装副本index |
| V7 | `tuntap.c:642-648` | config最后把fd加入FileMain，read callback只调度RX node | FileMain/RX延期，因此不增加activation阶段 |
| V8 | `tuntap.c:650-659` | config error关闭已打开fds并返回primary error | Hammer逆序清理InterfaceMain与fds，primary category不改名 |
| V9 | `tuntap.c:411-450` | exit在未配置时no-op；cleanup socket、down或persist-off失败只warning；关闭fds并返回成功 | Linux cleanup结果对齐；Hammer额外释放其显式InterfaceMain owner实例 |
| V10 | `tuntap.c:922-975` | 静态`tuntap_interface_class`为P2P；静态`tuntap_dev_class`名为tuntap并提供name formatter/TX | class/image/name/P2P对齐；TX function延期 |
| V11 | `interface.c:820-1060` | `vnet_register_interface`创建hw/sw关系；没有TX function时走`no_output_nodes`仍成功 | 第一阶段class无TX，InterfaceMain仍创建实例 |
| V12 | `interface.c:1064-1130` | delete将flags置0、执行callbacks、删queues与sw/hw interface | Hammer `delete_hardware_interface`按该ownership清理 |
| V13 | `interface.c:1370-1469`、`interface.h:390-410, 592-610` | class由静态registration在interface init安装；owner写`c->index`并建立两个class-name indexes | plugin image必须在config前由InterfaceMain消费；active class副本与name indexes由同一owner写入 |
| V14 | `tuntap.c:675-886, 986-1012` | IPv4/IPv6 callback与per-thread dataplane state独立初始化 | 全部延期，不让IP成为第一阶段前置项 |
| V15 | `tuntap.h:13-57` 加 scoped full-tree search | `register_tuntap_inject_node_name`、`vnet_tap_connect*`/modify/delete只有声明，无vendored定义/call site | 不复制这些legacy public APIs |
| V16 | `interface_funcs.h:10-23` | `vnet_get_hw_interface`直接返回pool element；只有显式`vnet_get_hw_interface_or_null`才允许缺失 | 注册成功后的`tuntap_config`使用非空borrow，invalid index是owner invariant bug |

Vendored tree 对 legacy header symbols 的 scoped search 只有 `tuntap.h` 命中。VPP tests 中
`test_interface_crud.py` 把 legacy tuntap 标为 root-required TBD；`asf/test_tap.py` 属于另一套
`plugins/tap`。第 9 节测试因此由实现路径推导。

## 5. 实现前 Hammer 基线

| ID | Hammer source | 当前事实 | 前置缺口 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-service/src/interface_model.rs:35-165` | `DeviceClass`/`HwClass`类型存在；class index仍为`Option<u32>`，derive只生成name constructor，class flags仍是raw `u32` | class安装副本改用普通`u32 index`；完成第1.2节derive/typed flags |
| H2 | `interface_model.rs:167-192, 503-527, 1047-1064` | `InterfaceRegistrationImage`存在，但init只消费service image，未消费plugin DSO image | 完成第1.1节跨DSO安装 |
| H3 | `interface_model.rs:529-595, 748-753` | `register_hardware_interface`已有hw/sw pool创建，但未按目标完整执行class name/硬件callback语义；hardware accessor当前返回可空值 | 完成第1.3节owner行为和VPP direct lookup语义 |
| H4 | `interface_model.rs:676-714` | `delete_hardware_interface`已有queue/address/pool清理 | 复用并补齐VPP flags/callback顺序 |
| H5 | `interface_model.rs:304-317, 718-753` | 只有interface instance name index，没有VPP两个class-name indexes | `InterfaceMain`安装class时同步维护class vector和name index；formatter决定`tuntap-0` |
| H6 | `interface_model.rs:1047` 与 `net/mod.rs:114-142` | 当前代码仍用独立`INTERFACE_MAIN`再交给NetMain，且用class index `(0,0)`创建`local0` | tuntap只经NetMain owner访问；service注册local classes并从owner name index取index，禁止index 0假设 |
| H7 | `hammer-runtime/src/main_loop.rs:38-67` | normal config发生在graph materialization前，exit发生在最终barrier | 第一阶段无graph依赖，config直接完成；无需activate |
| H8 | `hammer-component-macros/src/lib.rs:2233-2340`、`hammer-service/src/interface.rs:44-57` | `runtime_error`保留owner typed source并标记subsystem；InterfaceError已使用相同边界 | TuntapConfigError使用`subsystem = "tuntap"`，不增加runtime error API |
| H9 | `hammer-plugins/transport/tcp/src/lib.rs:261-262, 364-390`、`hammer-plugins/net/ip/src/lookup.rs:105-139` | plugin Main由owner-local OnceLock按值持有，完整构造后一次set | TuntapMain同样按值发布；不增加可替换wrapper或第二个global |

## 6. `tuntap_config` 顺序与 InterfaceMain 交互

### 6.1 Config 前的 class 安装

class registration不属于`tuntap_config`动态操作。plugin load完成后，
`interface_main_init`先安装class并建立name indexes；`net_main_init`随后发布owner。config callback
开始时，`InterfaceMain::device_class_index("tuntap")`和
`InterfaceMain::hw_class_index("tuntap")`必须成功。未安装class表示startup registration
invariant被破坏，按programmer bug终止启动，不定义`Option`或`ClassMissing` recoverable
error。

### 6.2 Config transaction

VPP-aligned控制流为：

```text
parse config
-> construct local TuntapMain with fd = -1
-> disabled? TUNTAP_MAIN.set(main) -> success
-> non-root? warning -> TUNTAP_MAIN.set(main) -> success
-> main.is_ether = config.ethernet
-> open /dev/net/tun
-> TUNSETIFF(TUN|TAP, NO_PI)
-> TUNSETPERSIST(1)
-> open PF_PACKET/SOCK_RAW provisioning socket
-> SIOCGIFINDEX
-> bind provisioning socket
-> FIONBIO tuntap fd
-> SIOCSIFMTU
-> SIOCGIFFLAGS
-> SIOCSIFFLAGS(IFF_UP|IFF_RUNNING)
-> TAP only: SIOCGIFHWADDR
-> read "tuntap" indexes from InterfaceMain class-name indexes
-> register_hardware_interface(class indexes, instance 0)
-> obtain sw_if_index from the registered HwInterface invariant borrow
-> set hardware LINK_UP
-> set software ADMIN_UP
-> TUNTAP_MAIN.set(main)
```

没有任何 graph lookup、File registration或node scheduling，所以没有`activate`。

disabled 与 non-root warning-success 分支发布fd仍为`-1`的local Main。enabled 分支在local
Main上依次写入fd、MAC和hw/sw indices，全部成功后一次性发布。Linux syscall失败返回第1.4
节同名 concrete variant并标记`tuntap` subsystem；InterfaceMain owner operation失败保留
`InterfaceError`及其`interface` subsystem。rollback按逆序执行已完成步骤，并保留触发
rollback的primary error。

VPP `done:` 只close fds，可能在persist-on后的后续失败遗留persistent netdev。Hammer按照根
错误合同额外尝试persist-off并删除已建InterfaceMain实例；这是failure-atomicity加强，但不会
增加或改名primary VPP error。rollback cleanup failure按VPP exit语义warning并继续，不能用
cleanup error替换primary error。

### 6.3 Exit/delete

`tuntap_exit` 在 Main 未发布或`dev_net_tun_fd <= 0`时no-op。fd有效时，在final barrier内依次：

```text
InterfaceMain::delete_hardware_interface(hw_if_index)
-> open AF_INET/SOCK_STREAM cleanup socket
-> cleanup socket SIOCGIFFLAGS
-> cleanup socket SIOCSIFFLAGS(clear IFF_UP|IFF_RUNNING)
-> TUNSETPERSIST(0)
-> close provisioning fd
-> close /dev/net/tun fd
-> close cleanup socket
```

VPP process exit不回收其即将销毁的interface pool；Hammer显式调用 owner delete，保证集成测试
能观察到hw/sw slots和name index均已释放。这是Rust owner cleanup，不是runtime CRUD API。

`tuntap_exit` 只调用 `TUNTAP_MAIN.get()`；Main 尚未发布表示 normal config 在失败后退出，按
no-op处理。disabled与non-root由`dev_net_tun_fd <= 0`走VPP同一no-op判断。exit callback只执行
一次，close后进程退出；不得为了cleanup增加 `Option`、take、`TuntapMain::global()` 或可替换/
清空 Main 的 API。

和 VPP `tuntap_exit` 一致，cleanup socket open、Linux down和persist-off失败都只warning，仍
尝试其余cleanup并最终返回成功。Hammer InterfaceMain delete若失败也warning后继续
process-exit cleanup；不新增`TuntapDeleteError`，也不把exit改成startup-style fatal error。

## 7. 错误语义逐项对齐

| VPP branch | Rust concrete source | 返回/cleanup语义 |
| --- | --- | --- |
| unknown input | existing `ConfigFunctionParse` with TOML source | config失败，无mutation |
| disabled | 无error | success |
| `geteuid()!=0` | 无error | warning + success |
| `open /dev/net/tun` | `OpenDevNetTun` | config失败 |
| `ioctl TUNSETIFF` | `TunSetIff` | close fds，config失败 |
| `TUNSETPERSIST(1)` | `TunSetPersist` | close fds，config失败 |
| `socket` | `Socket` | rollback，config失败 |
| `SIOCGIFINDEX` | `GetInterfaceIndex` | rollback，config失败 |
| `bind` | `Bind` | rollback，config失败 |
| `FIONBIO` | `SetNonblocking` | rollback，config失败 |
| `SIOCSIFMTU` | `SetMtu` | rollback，config失败 |
| `SIOCGIFFLAGS` | `GetInterfaceFlags` | rollback，config失败 |
| `SIOCSIFFLAGS` | `SetInterfaceFlags` | rollback，config失败 |
| TAP `SIOCGIFHWADDR` | `GetHardwareAddress` | rollback，config失败 |
| InterfaceMain owner `Err` | original `InterfaceError` source | rollback，config失败；不转成Tuntap variant |
| exit cleanup socket/down/persist failure | 不定义returned error | warning，继续cleanup，exit成功 |

不得增加 invalid-name、invalid-MTU、invalid-mode、permission-policy、already-active、graph、
File、node或catch-all I/O error。VPP没有相应 config error branch，或该操作不在第一阶段。
name按VPP `strncpy(..., IFNAMSIZ - 1)`处理；MTU由kernel ioctl判定。

## 8. 后续能力与 Node 占位

下列名称只在ADR中占位。本阶段不得提交空type、空graph registration、空`process`或假
read/write callback：

```rust
// Deferred design names only; not phase-one implementation types.
struct TuntapRxNode;
struct TuntapTxNode;
struct TuntapThreadState;
```

后续独立ADR必须补齐：

| Slice | 必须设计的VPP合同 |
| --- | --- |
| File/RX | FileMain ownership、read readiness只置interrupt pending、readv与Buffer chain |
| TX | DeviceClass真实`NodeProcessFn`、writev、partial write、frame/buffer释放 |
| punt/inject | generic OS punt consumer、frame ownership、TUN IP/TAP Ethernet ingress |
| normal interface | VPP `have-normal-interface`与Ethernet registration；届时再增加对应配置 |
| address sync | IPv4/IPv6 callbacks、FIB table过滤、Linux aliases |
| per-thread state | worker启动前固定entries，不用thread-local或锁 |

在这些设计批准前，不定义`TuntapMode`、`TuntapEncapsulation`、`TuntapRxError`、
`TuntapTxError`，不注册FileMain，也不为`tx_function`设置占位函数。

## 9. 三向差异与测试矩阵

### 9.1 三向语义差异

| 维度 | 当前Hammer | 第一阶段目标 | VPP | 决策 |
| --- | --- | --- | --- | --- |
| class registration | 仅service image；class index为可空字段且无class-name index | plugin image在interface init消费；active class副本使用普通`u32 index`并同步建立name indexes | interface init写class index并建立name indexes | owner/lifecycle对齐；Rust不增加mutable plugin static |
| config phase | normal config可用 | config直接完成Linux+InterfaceMain | `tuntap_config`直接完成 | 对齐；无activate |
| mode | 无 | 只实现默认punt interface registration branch | 默认punt，可选normal | normal延期，不发明mode enum |
| Linux create | 无 | 完整open/ioctl/socket/bind/MTU/up/MAC | 同调用链 | 对齐 |
| InterfaceMain | generic pool API部分存在 | owner class-name index、hw/sw创建、link/admin up | class-name index与`vnet_register_interface`均由InterfaceMain拥有 | 对齐owner；Rust lookup替代C mutable static读取 |
| TX nodes | 当前class callback占位 | 不设置DeviceClass TX callback，不创建output/tx nodes | tuntap有真实TX | 有界延期；复用VPP no-output branch |
| File/RX | FileMain存在但无driver | 不注册 | VPP config注册read callback | 延期；不增加activate |
| rollback | owner APIs局部清理 | primary error不变，额外逆序rollback | `done:`只close fds | repository-required加强 |
| exit pool cleanup | 可显式delete | delete InterfaceMain实例+Linux资源 | Linux资源cleanup后进程结束 | 最终ownership结果一致 |

### 9.2 测试矩阵

| Test | Level/environment | 必须断言 | Evidence |
| --- | --- | --- | --- |
| `tuntap_classes_load_before_config` | real DSO integration | 两个class可从`InterfaceMain`按name取得普通`u32` index；index不假定0；config尚未执行 | V10,V13,H1-H2 |
| `tuntap_defaults_to_disabled` | plugin subprocess | 默认值对齐；Main两个fd均为`-1`；Linux与InterfaceMain均不变 | V1-V2 |
| `tuntap_rejects_unimplemented_fields` | config test | `mode`/`have_normal_interface`/地址字段parse失败且无mutation | V1 |
| `tuntap_non_root_matches_vpp` | unprivileged subprocess | warning+success；Main两个fd均为`-1`；无Linux/Hammer interface | V2 |
| `tuntap_main_publishes_complete_value_once` | plugin subprocess | config前Main未发布；成功发布值的fd、name、MTU、MAC和indices完整；无二次安装或额外wrapper | H7,H9 |
| `tuntap_linux_errors_match_vpp_operations` | owner-local syscall behavior tests | 每个failure site匹配具体variant/source且runtime subsystem为`tuntap`；无catch-all/string matching | V3-V5 |
| `tuntap_config_registers_interface_main` | privileged Linux CI namespace | Linux netdev存在；安装后的class indexes、非空hw/sw关系、name=`tuntap-0`、link/admin flags正确 | V3-V6,V10-V11,V16 |
| `tuntap_config_is_failure_atomic` | privileged/fault behavior | 任一步失败保留primary source；已建InterfaceMain与Linux资源逆序清理 | V8,H3-H4 |
| `tuntap_exit_deletes_both_owners` | privileged subprocess | hw/sw slots与name删除；Linux link down；persist off；fds closed | V9,V12 |
| `tuntap_exit_matches_vpp_warning_policy` | cleanup behavior | down/persist错误不跳过后续cleanup，exit仍成功 | V9 |
| `tuntap_phase_one_has_no_packet_path` | graph integration | 不注册tuntap RX/TX nodes、File interest、queues或punt consumer | V7,V14 |

测试不能读取源码/Cargo文本做`contains`或regex断言。真实TUN/TAP测试只在CI lab运行。
本地门禁只运行不需要host TUN/TAP的Rust tests；真实生命周期由CI运行ignored test。

## 10. 变更清单

### Add（已实现）

- `crates/hammer-plugins/device/tuntap/`与package `hammer-plugin-tuntap`；
- plugin-private config、展平的Main、VPP-aligned config error；
- tuntap DeviceClass/HwClass declarations与interface registration image；
- `tuntap_config` normal config callback；
- `tuntap_exit` main-loop-exit callback；
- unit/DSO integration tests与CI-only Linux lifecycle tests。

### Modify（已实现的 blocking prerequisites）

- 完成 ADR-0004 已确定的 startup DSO interface image 安装；本 ADR 不改变其 API；
- service local DeviceClass/HwClass与`NetMain::init`从owner class-name index读取，删除class index 0假设；
- `InterfaceMain`：普通`u32` class index、两个class-name indexes、typed flags、非空hardware lookup以及
  VPP-aligned create/delete callback顺序；
- class derives：支持真实fields并进入service distributed slices；不生成第二套index storage；
- workspace与plugin README：加入device plugin分类；
- `CONTEXT.md`：实现后增加Tuntap Main与Linux/InterfaceMain双owner术语。

### Not changed

- graph/node registration、FileMain、PuntNode、Buffer和worker state；
- IP/FIB/Ethernet packet input；
- Binary API、CLI与external client；
- runtime registry或plugin-specific capability carrier；
- `PluginMain`、`ConfigFunction`与`RuntimeError`；复用现有`runtime_error` conversion。

## 11. 新类型/API 审批

根`AGENTS.md`要求非平凡VPP工作的新type/API先获明确批准。issue #335 的端到端实现请求批准
以下限定surface；超出本表的API仍需另行批准：

| Item | Owner | Final responsibility |
| --- | --- | --- |
| `InterfaceMain::{device_class_index,hw_class_index}` | hammer-service | 从owner class-name indexes返回已安装class的普通`u32` index；缺失是startup invariant bug |
| `LocalDeviceClass`/`LocalHwClass` | hammer-service | 为`local0`提供VPP同名内建classes，避免index 0假设 |
| `HwClassFlags` | hammer-service | typed P2P class fact |
| `HwInterfaceFlags`/`SwInterfaceFlags` | hammer-service | typed link/admin facts |
| `InterfaceMain::{set_hardware_flags,set_software_flags}` | hammer-service | owner mutation与callbacks |
| `FormatDeviceNameFn`与typed `HwClass.flags` | hammer-service | 让generic interface registration按instance生成名称并保存P2P class fact |
| `InterfaceMain::hardware_interface -> &HwInterface` | hammer-service | 对齐VPP非空`vnet_get_hw_interface`；invalid registered index是owner invariant bug |
| expanded DeviceClass/HwClass derives | component macros/service contract | 完整class declaration与image contribution，不生成index wrapper/static |
| `TuntapConfig` | plugin | VPP-aligned parsed input subset |
| `TuntapConfigError` | plugin | 一对一Linux config syscall failures；`subsystem = "tuntap"` |
| `TuntapDeviceClass`/`TuntapHwClass` | plugin | VPP class declarations |
| `TuntapMain` | plugin | VPP对应字段组成的process-global owner，无State/Instance wrapper |
| `TuntapMain::init` | plugin | 接收config并完成local Main、Linux与InterfaceMain初始化；不拥有另一套delete lifecycle |
| `tuntap_config`/`tuntap_exit` | plugin lifecycle | normal config create与exit delete |

现有`register_hardware_interface`、`delete_hardware_interface`、`NetMain::global`和
`NetMain::interface_main`复用，不申请替代API；`hardware_interface`只把当前可空返回收紧为
VPP direct lookup语义。ADR-0004 的 registration image transport 也不在本轮重新设计或审批。
错误边界复用现有`runtime_error` macro，不申请替代API。第8节node名称不在本轮审批范围，
也不进入第一阶段实现。

## 12. Decision record 与 verdict

| ID | Final decision | Alignment |
| --- | --- | --- |
| D1 | class通过plugin InterfaceRegistrationImage在config前安装；active副本持有普通`u32 index`，config从InterfaceMain class-name index取得 | VPP owner、安装顺序与index语义对齐；避免Rust mutable static |
| D2 | config直接完成Linux与InterfaceMain创建；没有activate | VPP `tuntap_config`对齐 |
| D3 | 配置使用enabled/name/mtu/ethernet，不定义encapsulation/mode | VPP事实直接表达 |
| D4 | 第一阶段只走VPP默认punt interface registration branch | bounded slice；punt packet path延期 |
| D5 | Linux config errors一对一并标记`tuntap` subsystem；InterfaceError保留`interface` subsystem与source | 复用既有owner error设计 |
| D6 | exit显式删除InterfaceMain实例并清理Linux资源 | VPP最终结果加Rust owner completion |
| D7 | File、RX/TX、punt、normal、地址只作ADR占位 | 不提交空node/假能力 |
| D8 | Main展平为VPP对应字段；normal config对local Main赋值后一次性set | Hammer plugin Main ownership对齐 |

**Design verdict: Aligned for issue #335 implementation.** 第一阶段包括真实DeviceClass/HwClass注册、
`InterfaceMain` hw/sw interface交互、VPP字段形态的`TuntapMain`以及标明subsystem的owner-local
errors，并删除了没有VPP对应的activate阶段。实现必须先完成第1节interface前置能力，且不得
扩展第3节范围。

## 13. Implementation record

实现落点：

- `hammer-service` 的 `InterfaceMain` 从service与startup plugin images安装class副本并维护
  class-name indexes；`local0`与tuntap均按name取得class index。
- `hammer-component-macros` 的class derives向当前link image的service-owned distributed
  slices贡献registration，并解析`format_device_name`与typed `flags`。
- `hammer-plugin-tuntap`由`TuntapMain::init(config)`直接完成Linux provisioning、hw/sw interface
  注册和link/admin up；`tuntap_config`只安装成功返回的完整Main；`tuntap_exit`直接删除Hammer
  interface并执行VPP warning-only Linux cleanup，不经过额外helper。
- `TuntapMain`保持单个展平的owner-local `OnceLock<TuntapMain>`；没有State/Instance包装、
  class index cell、activate阶段或packet-path占位实现。
- InterfaceMain不定义聚合rollback error，始终返回原始owner error；rollback的次级callback
  error可观察但不替换primary error。

验证由三个层次组成：普通unit/integration tests覆盖配置、disabled/non-root分支、具体错误
source chain、class安装及hw/sw生命周期；ignored DSO test通过`PluginMain`加载真实动态库并消费
service registration image；privileged Linux CI在独立network namespace运行真实TUN创建与
删除。IP、FileMain、RX/TX Node、punt/inject、normal-interface、runtime CRUD、Binary API和CLI
仍按第8节延期。
