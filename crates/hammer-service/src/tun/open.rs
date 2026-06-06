use std::io;
use std::os::fd::OwnedFd;

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
use std::mem;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;

use hammer_core::error::{CoreError, CoreResult};

use super::{TunFdIo, TunFdPacketFormat};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealTunOpenOptions {
    mtu: usize,
    requested_name: Option<String>,
    exclusive: bool,
}

impl RealTunOpenOptions {
    pub fn new(mtu: usize) -> Self {
        Self {
            mtu,
            requested_name: None,
            exclusive: false,
        }
    }

    pub fn with_requested_name(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if !name.is_empty() {
            self.requested_name = Some(name);
        }
        self
    }

    pub fn with_exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    pub fn mtu(&self) -> usize {
        self.mtu
    }

    pub fn requested_name(&self) -> Option<&str> {
        self.requested_name.as_deref()
    }

    pub fn exclusive(&self) -> bool {
        self.exclusive
    }
}

#[derive(Debug)]
pub struct RealTunOpen {
    fd: OwnedFd,
    name: String,
    mtu: usize,
    packet_format: TunFdPacketFormat,
}

impl RealTunOpen {
    pub fn open(options: RealTunOpenOptions) -> CoreResult<Self> {
        if options.mtu == 0 {
            return Err(CoreError::internal("TUN MTU must be greater than zero"));
        }
        open_platform_tun(options)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mtu(&self) -> usize {
        self.mtu
    }

    pub fn packet_format(&self) -> TunFdPacketFormat {
        self.packet_format
    }

    pub fn packet_format_for_platform() -> TunFdPacketFormat {
        TunFdPacketFormat::platform_default()
    }

    pub fn into_io(self) -> CoreResult<TunFdIo> {
        TunFdIo::from_owned_fd_with_format(self.fd, self.mtu, self.packet_format)
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn open_platform_tun(options: RealTunOpenOptions) -> CoreResult<RealTunOpen> {
    let fd = open_apple_utun_socket()?;
    let unit = options
        .requested_name()
        .map(apple_utun_unit_from_name)
        .transpose()?
        .unwrap_or(0);

    let control_id = apple_utun_control_id(fd.as_raw_fd())?;
    connect_apple_utun(fd.as_raw_fd(), control_id, unit)?;
    let name = apple_utun_name(fd.as_raw_fd())?;
    Ok(RealTunOpen {
        fd,
        name,
        mtu: options.mtu,
        packet_format: TunFdPacketFormat::AppleUtun,
    })
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
use std::os::fd::AsRawFd;

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn open_apple_utun_socket() -> CoreResult<OwnedFd> {
    let fd = unsafe { libc::socket(libc::PF_SYSTEM, libc::SOCK_DGRAM, libc::SYSPROTO_CONTROL) };
    if fd < 0 {
        return Err(os_error("open Apple utun socket"));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
use std::os::fd::FromRawFd;

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn apple_utun_control_id(fd: i32) -> CoreResult<u32> {
    let mut info: libc::ctl_info = unsafe { mem::zeroed() };
    let name = b"com.apple.net.utun_control\0";
    if name.len() > info.ctl_name.len() {
        return Err(CoreError::internal("Apple utun control name is too long"));
    }
    for (dst, src) in info.ctl_name.iter_mut().zip(name.iter().copied()) {
        *dst = src as libc::c_char;
    }
    let rc = unsafe { libc::ioctl(fd, libc::CTLIOCGINFO, &mut info) };
    if rc < 0 {
        return Err(os_error("lookup Apple utun control"));
    }
    Ok(info.ctl_id)
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn connect_apple_utun(fd: i32, control_id: u32, unit: u32) -> CoreResult<()> {
    let addr = libc::sockaddr_ctl {
        sc_len: mem::size_of::<libc::sockaddr_ctl>() as u8,
        sc_family: libc::AF_SYSTEM as u8,
        ss_sysaddr: libc::AF_SYS_CONTROL as u16,
        sc_id: control_id,
        sc_unit: unit,
        sc_reserved: [0; 5],
    };
    let rc = unsafe {
        libc::connect(
            fd,
            (&addr as *const libc::sockaddr_ctl).cast::<libc::sockaddr>(),
            mem::size_of_val(&addr) as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(os_error("connect Apple utun socket"));
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn apple_utun_name(fd: i32) -> CoreResult<String> {
    let mut name = [0u8; libc::IFNAMSIZ];
    let mut len = name.len() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SYSPROTO_CONTROL,
            libc::UTUN_OPT_IFNAME,
            name.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if rc < 0 {
        return Err(os_error("read Apple utun interface name"));
    }
    c_name_to_string(&name)
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
fn apple_utun_unit_from_name(name: &str) -> CoreResult<u32> {
    let Some(suffix) = name.strip_prefix("utun") else {
        return Err(CoreError::internal(format!(
            "Apple utun requested name must look like utunN, got {name}"
        )));
    };
    let index = suffix.parse::<u32>().map_err(|_| {
        CoreError::internal(format!(
            "Apple utun requested name must look like utunN, got {name}"
        ))
    })?;
    index
        .checked_add(1)
        .ok_or_else(|| CoreError::internal("Apple utun unit overflow"))
}

#[cfg(target_os = "linux")]
fn open_platform_tun(options: RealTunOpenOptions) -> CoreResult<RealTunOpen> {
    let fd = open_linux_tun_fd()?;
    let name = configure_linux_tun(fd.as_raw_fd(), &options)?;
    Ok(RealTunOpen {
        fd,
        name,
        mtu: options.mtu,
        packet_format: TunFdPacketFormat::RawIp,
    })
}

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

#[cfg(target_os = "linux")]
const LINUX_TUNSETIFF: libc::c_ulong = 0x4004_54ca;

#[cfg(target_os = "linux")]
fn open_linux_tun_fd() -> CoreResult<OwnedFd> {
    let path = c"/dev/net/tun";
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(os_error("open /dev/net/tun"));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn configure_linux_tun(fd: i32, options: &RealTunOpenOptions) -> CoreResult<String> {
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    write_linux_ifreq_name(&mut ifr, options.requested_name())?;
    ifr.ifr_ifru.ifru_flags = linux_tun_flags(options.exclusive())?;
    let rc = unsafe { libc::ioctl(fd, LINUX_TUNSETIFF, &mut ifr) };
    if rc < 0 {
        return Err(os_error("configure Linux TUN interface"));
    }
    let name = unsafe { ifr.ifr_name };
    c_name_to_string(name.as_slice())
}

#[cfg(target_os = "linux")]
fn write_linux_ifreq_name(ifr: &mut libc::ifreq, name: Option<&str>) -> CoreResult<()> {
    let Some(name) = name else {
        return Ok(());
    };
    let bytes = name.as_bytes();
    if bytes.len() >= libc::IFNAMSIZ {
        return Err(CoreError::internal(format!(
            "Linux TUN interface name is too long: {name}"
        )));
    }
    for (dst, src) in unsafe { ifr.ifr_name.as_mut() }
        .iter_mut()
        .zip(bytes.iter().copied())
    {
        *dst = src as libc::c_char;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_tun_flags(exclusive: bool) -> CoreResult<libc::c_short> {
    let mut flags = libc::IFF_TUN | libc::IFF_NO_PI;
    if exclusive {
        flags |= libc::IFF_TUN_EXCL;
    }
    libc::c_short::try_from(flags)
        .map_err(|_| CoreError::internal("Linux TUN flags do not fit c_short"))
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "linux"
)))]
fn open_platform_tun(_options: RealTunOpenOptions) -> CoreResult<RealTunOpen> {
    Err(CoreError::internal(
        "native TUN opener is unsupported on this platform",
    ))
}

fn os_error(context: &str) -> CoreError {
    CoreError::internal(format!("{context}: {}", io::Error::last_os_error()))
}

fn c_name_to_string(name: &[impl Copy + TryInto<u8>]) -> CoreResult<String> {
    let mut bytes = [0u8; 256];
    if name.len() > bytes.len() {
        return Err(CoreError::internal("interface name is too long"));
    }
    let mut end = 0usize;
    for (index, byte) in name.iter().enumerate() {
        let byte = (*byte)
            .try_into()
            .map_err(|_| CoreError::internal("interface name contains invalid byte"))?;
        if byte == 0 {
            break;
        }
        bytes[index] = byte;
        end = index + 1;
    }
    std::str::from_utf8(&bytes[..end])
        .map(ToOwned::to_owned)
        .map_err(|err| CoreError::internal(format!("interface name is not UTF-8: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_capture_requested_name_and_exclusive_flag() {
        let options = RealTunOpenOptions::new(1280)
            .with_requested_name("tun42")
            .with_exclusive(true);

        assert_eq!(options.mtu(), 1280);
        assert_eq!(options.requested_name(), Some("tun42"));
        assert!(options.exclusive());
    }

    #[test]
    fn real_opener_uses_platform_packet_format() {
        assert_eq!(
            RealTunOpen::packet_format_for_platform(),
            TunFdPacketFormat::platform_default()
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
    #[test]
    fn apple_utun_requested_name_maps_to_kernel_unit() {
        assert_eq!(apple_utun_unit_from_name("utun0").unwrap(), 1);
        assert_eq!(apple_utun_unit_from_name("utun7").unwrap(), 8);
        assert!(apple_utun_unit_from_name("tun7").is_err());
    }
}
