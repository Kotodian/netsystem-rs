use std::fmt;
use std::io;
#[cfg(target_os = "linux")]
use std::mem::size_of;
use std::os::fd::RawFd;
use std::sync::OnceLock;

use hammer_runtime::RuntimeResult;
use hammer_service::interface::{HwClassFlags, HwInterfaceFlags, SwInterfaceFlags};
use hammer_service::net::NetMain;

hammer_service::declare_interface_registration_image!();

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tuntap_interface_name
)]
struct TuntapDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "tuntap", flags = HwClassFlags::P2P)]
struct TuntapHwClass;

fn format_tuntap_interface_name(
    device_instance: u32,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(formatter, "tuntap-{device_instance}")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TuntapConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    ethernet: bool,
}

impl Default for TuntapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "vnet".to_owned(),
            mtu: 4_096 + 256,
            ethernet: false,
        }
    }
}

#[derive(Debug)]
struct TuntapMain {
    dev_net_tun_fd: RawFd,
    dev_tap_fd: RawFd,
    is_ether: bool,
    tun_name: String,
    mtu_bytes: u32,
    ether_dst_mac: [u8; 6],
    hw_if_index: u32,
    sw_if_index: u32,
}

static TUNTAP_MAIN: OnceLock<TuntapMain> = OnceLock::new();

impl TuntapMain {
    #[cfg(target_os = "linux")]
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
        if !config.enabled {
            return Ok(main);
        }
        if unsafe { libc::geteuid() } != 0 {
            tracing::warn!("tuntap disabled: must be superuser");
            return Ok(main);
        }

        main.is_ether = config.ethernet;
        let mut interface_registered = false;
        let startup_error = match 'configuration: {
            main.dev_net_tun_fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR) };
            if main.dev_net_tun_fd < 0 {
                break 'configuration Err(TuntapConfigError::OpenDevNetTun {
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
            for (destination, source) in ifr
                .ifr_name
                .iter_mut()
                .take(libc::IFNAMSIZ - 1)
                .zip(main.tun_name.bytes())
            {
                *destination = source as libc::c_char;
            }
            ifr.ifr_ifru.ifru_flags = if main.is_ether {
                (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short
            } else {
                (libc::IFF_TUN | libc::IFF_NO_PI) as libc::c_short
            };
            if unsafe { libc::ioctl(main.dev_net_tun_fd, libc::TUNSETIFF, &mut ifr) } < 0 {
                break 'configuration Err(TuntapConfigError::TunSetIff {
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            if unsafe { libc::ioctl(main.dev_net_tun_fd, libc::TUNSETPERSIST, 1) } < 0 {
                break 'configuration Err(TuntapConfigError::TunSetPersist {
                    source: io::Error::last_os_error(),
                }
                .into());
            }

            main.dev_tap_fd = unsafe {
                libc::socket(
                    libc::PF_PACKET,
                    libc::SOCK_RAW,
                    i32::from((libc::ETH_P_ALL as u16).to_be()),
                )
            };
            if main.dev_tap_fd < 0 {
                break 'configuration Err(TuntapConfigError::Socket {
                    source: io::Error::last_os_error(),
                }
                .into());
            }

            {
                let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
                for (destination, source) in ifr
                    .ifr_name
                    .iter_mut()
                    .take(libc::IFNAMSIZ - 1)
                    .zip(main.tun_name.bytes())
                {
                    *destination = source as libc::c_char;
                }
                if unsafe { libc::ioctl(main.dev_tap_fd, libc::SIOCGIFINDEX, &mut ifr) } < 0 {
                    break 'configuration Err(TuntapConfigError::GetInterfaceIndex {
                        source: io::Error::last_os_error(),
                    }
                    .into());
                }
                let sll = libc::sockaddr_ll {
                    sll_family: libc::AF_PACKET as libc::c_ushort,
                    sll_protocol: (libc::ETH_P_ALL as u16).to_be(),
                    sll_ifindex: unsafe { ifr.ifr_ifru.ifru_ifindex },
                    sll_hatype: 0,
                    sll_pkttype: 0,
                    sll_halen: 0,
                    sll_addr: [0; 8],
                };
                if unsafe {
                    libc::bind(
                        main.dev_tap_fd,
                        (&sll as *const libc::sockaddr_ll).cast(),
                        size_of::<libc::sockaddr_ll>() as libc::socklen_t,
                    )
                } < 0
                {
                    break 'configuration Err(TuntapConfigError::Bind {
                        source: io::Error::last_os_error(),
                    }
                    .into());
                }
            }

            let mut one = 1;
            if unsafe { libc::ioctl(main.dev_net_tun_fd, libc::FIONBIO, &mut one) } < 0 {
                break 'configuration Err(TuntapConfigError::SetNonblocking {
                    source: io::Error::last_os_error(),
                }
                .into());
            }

            ifr.ifr_ifru.ifru_mtu = main.mtu_bytes as libc::c_int;
            if unsafe { libc::ioctl(main.dev_tap_fd, libc::SIOCSIFMTU, &mut ifr) } < 0 {
                break 'configuration Err(TuntapConfigError::SetMtu {
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            if unsafe { libc::ioctl(main.dev_tap_fd, libc::SIOCGIFFLAGS, &mut ifr) } < 0 {
                break 'configuration Err(TuntapConfigError::GetInterfaceFlags {
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            unsafe {
                ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
            }
            if unsafe { libc::ioctl(main.dev_tap_fd, libc::SIOCSIFFLAGS, &mut ifr) } < 0 {
                break 'configuration Err(TuntapConfigError::SetInterfaceFlags {
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            if main.is_ether {
                if unsafe { libc::ioctl(main.dev_tap_fd, libc::SIOCGIFHWADDR, &mut ifr) } < 0 {
                    break 'configuration Err(TuntapConfigError::GetHardwareAddress {
                        source: io::Error::last_os_error(),
                    }
                    .into());
                }
                let address = unsafe { ifr.ifr_ifru.ifru_hwaddr.sa_data };
                for (destination, source) in main.ether_dst_mac.iter_mut().zip(address) {
                    *destination = source as u8;
                }
            }

            let interfaces = match NetMain::global() {
                Ok(net) => net.interface_main(),
                Err(error) => break 'configuration Err(error),
            };
            let device_class_index = interfaces.device_class_index("tuntap");
            let hw_class_index = interfaces.hw_class_index("tuntap");
            main.hw_if_index = match interfaces.register_hardware_interface(
                device_class_index,
                0,
                hw_class_index,
                0,
            ) {
                Ok(index) => index,
                Err(error) => break 'configuration Err(error.into()),
            };
            main.sw_if_index = interfaces
                .hardware_interface(main.hw_if_index)
                .sw_if_index();
            interface_registered = true;
            if let Err(error) =
                interfaces.set_hardware_flags(main.hw_if_index, HwInterfaceFlags::LINK_UP)
            {
                break 'configuration Err(error.into());
            }
            if let Err(error) =
                interfaces.set_software_flags(main.sw_if_index, SwInterfaceFlags::ADMIN_UP)
            {
                break 'configuration Err(error.into());
            }
            break 'configuration Ok(());
        } {
            Ok(()) => return Ok(main),
            Err(error) => error,
        };

        if interface_registered {
            let interfaces = NetMain::global()
                .expect("tuntap interface registration requires the network main")
                .interface_main();
            if let Err(error) = interfaces.delete_hardware_interface(main.hw_if_index) {
                tracing::warn!(%error, hw_if_index = main.hw_if_index, "tuntap interface initialization cleanup failed");
            }
        }
        if main.dev_net_tun_fd >= 0 {
            if unsafe { libc::ioctl(main.dev_net_tun_fd, libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap TUNSETPERSIST initialization cleanup failed");
            }
            unsafe { libc::close(main.dev_net_tun_fd) };
        }
        if main.dev_tap_fd >= 0 {
            unsafe { libc::close(main.dev_tap_fd) };
        }
        Err(startup_error)
    }

    #[cfg(not(target_os = "linux"))]
    fn init(config: TuntapConfig) -> RuntimeResult<Self> {
        let main = Self {
            dev_net_tun_fd: -1,
            dev_tap_fd: -1,
            is_ether: false,
            tun_name: config.name,
            mtu_bytes: config.mtu,
            ether_dst_mac: [0; 6],
            hw_if_index: 0,
            sw_if_index: 0,
        };
        if !config.enabled {
            return Ok(main);
        }
        Err(TuntapConfigError::OpenDevNetTun {
            source: io::Error::new(io::ErrorKind::Unsupported, "Linux TUN/TAP is unavailable"),
        }
        .into())
    }
}

#[hammer_component_macros::runtime_error(subsystem = "tuntap")]
#[derive(Debug, thiserror::Error)]
enum TuntapConfigError {
    #[error("open /dev/net/tun")]
    OpenDevNetTun {
        #[source]
        source: io::Error,
    },
    #[error("ioctl TUNSETIFF")]
    TunSetIff {
        #[source]
        source: io::Error,
    },
    #[error("TUNSETPERSIST")]
    TunSetPersist {
        #[source]
        source: io::Error,
    },
    #[error("socket")]
    Socket {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCGIFINDEX")]
    GetInterfaceIndex {
        #[source]
        source: io::Error,
    },
    #[error("bind")]
    Bind {
        #[source]
        source: io::Error,
    },
    #[error("ioctl FIONBIO")]
    SetNonblocking {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCSIFMTU")]
    SetMtu {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCGIFFLAGS")]
    GetInterfaceFlags {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCSIFFLAGS")]
    SetInterfaceFlags {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCGIFHWADDR")]
    GetHardwareAddress {
        #[source]
        source: io::Error,
    },
}

#[hammer_component_macros::config_function(name = "tuntap_config", section = "plugin.tuntap")]
fn tuntap_config(config: TuntapConfig) -> RuntimeResult<()> {
    let main = TuntapMain::init(config)?;
    assert!(
        TUNTAP_MAIN.set(main).is_ok(),
        "tuntap config callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::main_loop_exit_function(name = "tuntap_exit")]
fn tuntap_exit() -> RuntimeResult<()> {
    let Some(main) = TUNTAP_MAIN.get() else {
        return Ok(());
    };
    if main.dev_net_tun_fd <= 0 {
        return Ok(());
    }
    if let Ok(net) = NetMain::global()
        && let Err(error) = net
            .interface_main()
            .delete_hardware_interface(main.hw_if_index)
    {
        tracing::warn!(%error, hw_if_index = main.hw_if_index, "tuntap interface deletion failed");
    }
    #[cfg(target_os = "linux")]
    {
        let sfd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        if sfd < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap provisioning socket cleanup failed");
        }
        let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
        for (destination, source) in ifr
            .ifr_name
            .iter_mut()
            .take(libc::IFNAMSIZ - 1)
            .zip(main.tun_name.bytes())
        {
            *destination = source as libc::c_char;
        }
        if unsafe { libc::ioctl(sfd, libc::SIOCGIFFLAGS, &mut ifr) } < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap SIOCGIFFLAGS cleanup failed");
        }
        unsafe {
            ifr.ifr_ifru.ifru_flags &= !((libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short);
        }
        if unsafe { libc::ioctl(sfd, libc::SIOCSIFFLAGS, &mut ifr) } < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap SIOCSIFFLAGS cleanup failed");
        }
        if unsafe { libc::ioctl(main.dev_net_tun_fd, libc::TUNSETPERSIST, 0) } < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap TUNSETPERSIST cleanup failed");
        }
        if main.dev_tap_fd >= 0 {
            unsafe { libc::close(main.dev_tap_fd) };
        }
        unsafe { libc::close(main.dev_net_tun_fd) };
        if sfd >= 0 {
            unsafe { libc::close(sfd) };
        }
    }
    Ok(())
}

hammer_component_macros::declare_plugin!(
    name = "tuntap",
    load_after = [],
    init_functions = [],
    config_functions = [__CONFIG_FN_TUNTAP_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [__INIT_FN_TUNTAP_EXIT],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
    binary_api_methods = [],
);

#[cfg(test)]
mod tests {
    use std::error::Error;
    #[cfg(target_os = "linux")]
    use std::path::Path;
    #[cfg(target_os = "linux")]
    use std::sync::Arc;

    use hammer_service::interface::{HwInterfaceFlags, InterfaceMain, SwInterfaceFlags};
    use hammer_service::net::NetMain;

    use super::*;

    #[test]
    fn config_defaults_and_unimplemented_fields_are_explicit() {
        let config: TuntapConfig = toml::from_str("").unwrap();
        assert_eq!(config, TuntapConfig::default());
        assert!(toml::from_str::<TuntapConfig>("mode = \"punt-inject\"").is_err());
        assert!(toml::from_str::<TuntapConfig>("have_normal_interface = true").is_err());
        assert!(toml::from_str::<TuntapConfig>("address = \"192.0.2.1/24\"").is_err());
    }

    #[test]
    fn defaults_publish_a_disabled_main() {
        tuntap_config(TuntapConfig::default()).unwrap();
        let main = TUNTAP_MAIN.get().unwrap();
        assert_eq!(main.dev_net_tun_fd, -1);
        assert_eq!(main.dev_tap_fd, -1);
        assert!(!main.is_ether);
        assert_eq!(main.tun_name, "vnet");
        assert_eq!(main.mtu_bytes, 4_352);
        assert_eq!(main.ether_dst_mac, [0; 6]);
        assert_eq!(main.hw_if_index, 0);
        assert_eq!(main.sw_if_index, 0);
    }

    #[test]
    fn non_root_enable_matches_vpp_warning_success() {
        const CHILD: &str = "HAMMER_TUNTAP_NON_ROOT_TEST_CHILD";

        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        if std::env::var_os(CHILD).is_some() {
            tuntap_config(TuntapConfig {
                enabled: true,
                name: "hammer-non-root".to_owned(),
                mtu: 1_500,
                ethernet: true,
            })
            .unwrap();
            let main = TUNTAP_MAIN.get().unwrap();
            assert_eq!(main.dev_net_tun_fd, -1);
            assert_eq!(main.dev_tap_fd, -1);
            assert!(!main.is_ether);
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::non_root_enable_matches_vpp_warning_success",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn syscall_error_preserves_tuntap_subsystem_and_source() {
        let runtime_error = hammer_runtime::RuntimeError::from(TuntapConfigError::TunSetIff {
            source: io::Error::from_raw_os_error(libc::EINVAL),
        });
        let hammer_runtime::RuntimeError::Subsystem { subsystem, source } = runtime_error else {
            panic!("tuntap error must cross the runtime subsystem boundary");
        };
        assert_eq!(subsystem, "tuntap");
        let error = source.downcast_ref::<TuntapConfigError>().unwrap();
        let TuntapConfigError::TunSetIff { source } = error else {
            panic!("runtime source must retain the concrete TUNSETIFF category");
        };
        assert_eq!(source.raw_os_error(), Some(libc::EINVAL));
        assert!(error.source().is_some());
    }

    #[test]
    fn class_image_creates_and_deletes_interface_main_relationship() {
        hammer_runtime::ThreadMain::new().unwrap();
        let interfaces = InterfaceMain::new();
        interfaces
            .consume_registration_image(&HAMMER_INTERFACE_REGISTRATION_IMAGE)
            .unwrap();
        let device_class_index = interfaces.device_class_index("tuntap");
        let hw_class_index = interfaces.hw_class_index("tuntap");
        assert_ne!(device_class_index, interfaces.device_class_index("local"));
        assert_ne!(hw_class_index, interfaces.hw_class_index("local"));
        let hw_if_index = interfaces
            .register_hardware_interface(device_class_index, 0, hw_class_index, 0)
            .unwrap();
        let sw_if_index = interfaces.hardware_interface(hw_if_index).sw_if_index();
        assert_eq!(
            interfaces.hardware_interface(hw_if_index).dev_class_index,
            device_class_index
        );
        assert_eq!(
            interfaces.hardware_interface(hw_if_index).hw_class_index,
            hw_class_index
        );
        assert_eq!(interfaces.interface_index("tuntap-0"), Some(hw_if_index));
        interfaces
            .set_hardware_flags(hw_if_index, HwInterfaceFlags::LINK_UP)
            .unwrap();
        interfaces
            .set_software_flags(sw_if_index, SwInterfaceFlags::ADMIN_UP)
            .unwrap();
        assert_eq!(
            interfaces.hardware_interface(hw_if_index).flags,
            HwInterfaceFlags::LINK_UP
        );
        assert!(
            interfaces
                .software_interface(sw_if_index)
                .unwrap()
                .is_admin_up()
        );
        interfaces.delete_hardware_interface(hw_if_index).unwrap();
        assert_eq!(interfaces.interface_index("tuntap-0"), None);
        assert!(interfaces.software_interface(sw_if_index).is_none());
    }

    #[test]
    #[ignore = "requires the tuntap dynamic plugin to be built beside the test artifacts"]
    fn tuntap_dso_installs_class_image() {
        let mut plugins = hammer_runtime::PluginMain::default();
        plugins
            .load(env!("CARGO_PKG_VERSION"), &["tuntap".to_owned()])
            .unwrap();
        let image = plugins
            .get_plugin_symbol::<hammer_service::InterfaceRegistrationImage>(
                "tuntap",
                "HAMMER_INTERFACE_REGISTRATION_IMAGE",
            )
            .unwrap();
        let interfaces = InterfaceMain::new();
        // SAFETY: the service-owned export has this concrete type and
        // `plugins` retains the defining DSO through both lookups below.
        interfaces
            .consume_registration_image(unsafe { &*image })
            .unwrap();
        assert_ne!(
            interfaces.device_class_index("tuntap"),
            interfaces.device_class_index("local")
        );
        assert_ne!(
            interfaces.hw_class_index("tuntap"),
            interfaces.hw_class_index("local")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires root, /dev/net/tun, and an isolated network namespace"]
    fn tuntap_linux_lifecycle() {
        assert_eq!(unsafe { libc::geteuid() }, 0);
        hammer_runtime::ThreadMain::new().unwrap();
        let interfaces = Arc::new(InterfaceMain::new());
        interfaces
            .consume_registration_image(&HAMMER_INTERFACE_REGISTRATION_IMAGE)
            .unwrap();
        NetMain::init(Arc::clone(&interfaces)).unwrap();

        let name = "hammer335tun";
        assert!(!Path::new(&format!("/sys/class/net/{name}")).exists());
        tuntap_config(TuntapConfig {
            enabled: true,
            name: name.to_owned(),
            mtu: 4_352,
            ethernet: false,
        })
        .unwrap();
        let main = TUNTAP_MAIN.get().unwrap();
        assert!(main.dev_net_tun_fd >= 0);
        assert!(main.dev_tap_fd >= 0);
        let dev_net_tun_fd = main.dev_net_tun_fd;
        let dev_tap_fd = main.dev_tap_fd;
        assert!(Path::new(&format!("/sys/class/net/{name}")).exists());
        assert_eq!(
            interfaces.interface_index("tuntap-0"),
            Some(main.hw_if_index)
        );
        assert_eq!(
            interfaces.hardware_interface(main.hw_if_index).flags,
            HwInterfaceFlags::LINK_UP
        );
        assert_eq!(
            interfaces
                .hardware_interface(main.hw_if_index)
                .dev_class_index,
            interfaces.device_class_index("tuntap")
        );
        assert_eq!(
            interfaces
                .hardware_interface(main.hw_if_index)
                .hw_class_index,
            interfaces.hw_class_index("tuntap")
        );
        assert!(
            interfaces
                .software_interface(main.sw_if_index)
                .unwrap()
                .is_admin_up()
        );
        assert_eq!(
            interfaces
                .software_interface(main.sw_if_index)
                .unwrap()
                .hw_if_index(),
            Some(main.hw_if_index)
        );

        tuntap_exit().unwrap();
        assert_eq!(interfaces.interface_index("tuntap-0"), None);
        assert!(interfaces.software_interface(main.sw_if_index).is_none());
        assert!(!Path::new(&format!("/sys/class/net/{name}")).exists());
        assert_eq!(unsafe { libc::fcntl(dev_net_tun_fd, libc::F_GETFD) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        assert_eq!(unsafe { libc::fcntl(dev_tap_fd, libc::F_GETFD) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
    }
}
