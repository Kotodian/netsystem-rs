use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use super::TunError;

const TUN_DEVICE: &[u8] = b"/dev/net/tun\0";

type Result<T> = std::result::Result<T, TunError>;

pub(super) fn open(requested_name: &str, mtu: u32) -> Result<(OwnedFd, String)> {
    if requested_name.len() >= libc::IFNAMSIZ || requested_name.as_bytes().contains(&0) {
        return Err(TunError::InvalidInterfaceName);
    }

    // SAFETY: TUN_DEVICE is NUL-terminated, and these flags do not require an
    // additional variadic mode argument. A successful descriptor is
    // transferred immediately into OwnedFd and therefore closed exactly once.
    let raw = unsafe {
        libc::open(
            TUN_DEVICE.as_ptr().cast::<libc::c_char>(),
            libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if raw < 0 {
        return Err(TunError::Io {
            operation: "open /dev/net/tun",
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: raw is a newly-opened, uniquely-owned file descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };

    let mut request = libc::ifreq {
        ifr_name: [0; libc::IFNAMSIZ],
        ifr_ifru: libc::__c_anonymous_ifr_ifru {
            ifru_flags: (libc::IFF_TUN | libc::IFF_NO_PI) as libc::c_short,
        },
    };
    for (destination, source) in request.ifr_name.iter_mut().zip(requested_name.bytes()) {
        *destination = source as libc::c_char;
    }
    // SAFETY: fd refers to /dev/net/tun, request has libc's native ifreq
    // layout, and the mutable pointer remains valid for the entire ioctl.
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETIFF, &mut request) } < 0 {
        return Err(TunError::Io {
            operation: "configure Linux TUN interface",
            source: io::Error::last_os_error(),
        });
    }

    let name = interface_name(&request.ifr_name)?;
    set_mtu(&mut request, mtu)?;
    Ok((fd, name))
}

fn interface_name(name: &[libc::c_char; libc::IFNAMSIZ]) -> Result<String> {
    let length = name
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(TunError::InterfaceNameNotTerminated)?;
    if length == 0 {
        return Err(TunError::InterfaceNameEmpty);
    }
    // SAFETY: c_char and u8 have identical size and alignment. The source
    // array is initialized, live, and contains at least length elements.
    let bytes = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u8>(), length) };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| TunError::InterfaceNameNotUtf8)
}

fn set_mtu(request: &mut libc::ifreq, mtu: u32) -> Result<()> {
    let mtu = libc::c_int::try_from(mtu).map_err(|_| TunError::MtuOutOfRange)?;
    // SAFETY: socket has no pointer arguments. A successful descriptor is
    // transferred immediately into OwnedFd and therefore closed exactly once.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if raw < 0 {
        return Err(TunError::Io {
            operation: "create Linux interface control socket",
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: raw is a newly-created, uniquely-owned file descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    request.ifr_ifru = libc::__c_anonymous_ifr_ifru { ifru_mtu: mtu };
    // SAFETY: fd is a live AF_INET control socket, request has libc's native
    // ifreq layout, and the pointer remains valid for the entire ioctl.
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::SIOCSIFMTU, request) } < 0 {
        return Err(TunError::Io {
            operation: "set Linux TUN MTU",
            source: io::Error::last_os_error(),
        });
    }
    Ok(())
}
