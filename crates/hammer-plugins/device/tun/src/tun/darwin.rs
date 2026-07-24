use std::io;
use std::mem::{size_of, size_of_val};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use super::TunError;

const CONTROL_NAME: &[u8] = b"com.apple.net.utun_control";
const CTLIOCGINFO: libc::c_ulong = 0xc064_4e03;
const SIOCSIFMTU: libc::c_ulong = 0x8020_6934;
pub(super) const UTUN_HEADER_LEN: usize = 4;

type Result<T> = std::result::Result<T, TunError>;

#[repr(C)]
struct ControlInfo {
    id: u32,
    name: [libc::c_uchar; 96],
}

#[repr(C)]
union InterfaceRequestValue {
    address: libc::sockaddr,
    mtu: libc::c_int,
    storage: [u8; 16],
}

#[repr(C)]
struct InterfaceRequest {
    name: [libc::c_char; libc::IFNAMSIZ],
    value: InterfaceRequestValue,
}

pub(super) fn open(requested_name: &str, mtu: u32) -> Result<(OwnedFd, String)> {
    let unit = parse_unit(requested_name)?;
    // SAFETY: socket has no pointer arguments. A successful raw descriptor is
    // transferred immediately into OwnedFd and therefore closed exactly once.
    let raw = unsafe { libc::socket(libc::PF_SYSTEM, libc::SOCK_DGRAM, libc::SYSPROTO_CONTROL) };
    if raw < 0 {
        return Err(TunError::Io {
            operation: "create utun control socket",
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: raw is a newly-created, uniquely-owned file descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };

    let mut info = ControlInfo {
        id: 0,
        name: [0; 96],
    };
    info.name[..CONTROL_NAME.len()].copy_from_slice(CONTROL_NAME);
    // SAFETY: fd is live and info is writable, aligned, repr(C), and has the
    // exact lifetime of this ioctl call.
    if unsafe { libc::ioctl(fd.as_raw_fd(), CTLIOCGINFO, &mut info) } < 0 {
        return Err(TunError::Io {
            operation: "resolve utun control id",
            source: io::Error::last_os_error(),
        });
    }

    let address = libc::sockaddr_ctl {
        sc_len: size_of::<libc::sockaddr_ctl>() as u8,
        sc_family: libc::AF_SYSTEM as u8,
        ss_sysaddr: libc::AF_SYS_CONTROL as u16,
        sc_id: info.id,
        sc_unit: unit,
        sc_reserved: [0; 5],
    };
    // SAFETY: address is an initialized sockaddr_ctl and the supplied byte
    // length exactly matches its repr(C) allocation.
    if unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&address as *const libc::sockaddr_ctl).cast(),
            size_of_val(&address) as libc::socklen_t,
        )
    } < 0
    {
        return Err(TunError::Io {
            operation: "connect utun control socket",
            source: io::Error::last_os_error(),
        });
    }

    set_nonblocking(fd.as_raw_fd())?;
    let name = kernel_name(fd.as_raw_fd())?;
    set_mtu(&name, mtu)?;
    Ok((fd, name))
}

fn parse_unit(name: &str) -> Result<u32> {
    let Some(suffix) = name.strip_prefix("utun") else {
        return Err(TunError::InvalidInterfaceName);
    };
    if suffix.is_empty() {
        return Ok(0);
    }
    suffix
        .parse::<u32>()
        .ok()
        .and_then(|unit| unit.checked_add(1))
        .ok_or(TunError::InvalidInterfaceName)
}

fn set_nonblocking(fd: RawFd) -> Result<()> {
    // SAFETY: fd is live and F_GETFL has no pointer argument.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(TunError::Io {
            operation: "read utun descriptor flags",
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: fd remains live and flags came from F_GETFL for this descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(TunError::Io {
            operation: "set utun nonblocking mode",
            source: io::Error::last_os_error(),
        });
    }
    Ok(())
}

fn kernel_name(fd: RawFd) -> Result<String> {
    let mut name = [0u8; libc::IFNAMSIZ];
    let mut length =
        libc::socklen_t::try_from(name.len()).map_err(|_| TunError::InterfaceNameLengthInvalid)?;
    // SAFETY: name is writable for length bytes and length itself is a live
    // socklen_t out-parameter for this call.
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SYSPROTO_CONTROL,
            libc::UTUN_OPT_IFNAME,
            name.as_mut_ptr().cast(),
            &mut length,
        )
    } < 0
    {
        return Err(TunError::Io {
            operation: "read utun kernel name",
            source: io::Error::last_os_error(),
        });
    }
    let length = usize::try_from(length).map_err(|_| TunError::InterfaceNameLengthInvalid)?;
    let bytes = name
        .get(..length)
        .ok_or(TunError::InterfaceNameLengthInvalid)?;
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| TunError::InterfaceNameNotUtf8)
}

fn set_mtu(name: &str, mtu: u32) -> Result<()> {
    let mtu = libc::c_int::try_from(mtu).map_err(|_| TunError::MtuOutOfRange)?;
    if name.len() >= libc::IFNAMSIZ {
        return Err(TunError::InvalidInterfaceName);
    }
    // SAFETY: socket has no pointer arguments. The descriptor is transferred
    // immediately to OwnedFd on success.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if raw < 0 {
        return Err(TunError::Io {
            operation: "create interface control socket",
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: raw is a newly-created, uniquely-owned file descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut request = InterfaceRequest {
        name: [0; libc::IFNAMSIZ],
        value: InterfaceRequestValue { mtu },
    };
    for (destination, source) in request.name.iter_mut().zip(name.as_bytes()) {
        *destination = *source as libc::c_char;
    }
    // SAFETY: request is initialized repr(C) storage matching Darwin ifreq for
    // SIOCSIFMTU, and fd remains live for the call.
    if unsafe { libc::ioctl(fd.as_raw_fd(), SIOCSIFMTU, &request) } < 0 {
        return Err(TunError::Io {
            operation: "set utun MTU",
            source: io::Error::last_os_error(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unit_supports_auto_and_explicit_utun_names() {
        assert_eq!(
            (parse_unit("utun").unwrap(), parse_unit("utun9").unwrap()),
            (0, 10)
        );
    }

    #[test]
    fn parse_unit_rejects_non_utun_names() {
        assert!(parse_unit("tun0").is_err());
    }
}
