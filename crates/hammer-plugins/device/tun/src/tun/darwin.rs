use std::ffi::c_void;
use std::io;
use std::mem::{size_of, size_of_val};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use hammer_core::error::{CoreError, CoreResult};

const CONTROL_NAME: &[u8] = b"com.apple.net.utun_control";
const CTLIOCGINFO: libc::c_ulong = 0xc064_4e03;
const SIOCSIFMTU: libc::c_ulong = 0x8020_6934;
const UTUN_HEADER_LEN: usize = 4;

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

pub(super) fn open(requested_name: &str, mtu: u32) -> CoreResult<(OwnedFd, String)> {
    let unit = parse_unit(requested_name)?;
    // SAFETY: socket has no pointer arguments. A successful raw descriptor is
    // transferred immediately into OwnedFd and therefore closed exactly once.
    let raw = unsafe { libc::socket(libc::PF_SYSTEM, libc::SOCK_DGRAM, libc::SYSPROTO_CONTROL) };
    if raw < 0 {
        return Err(last_error("create utun control socket"));
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
        return Err(last_error("resolve utun control id"));
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
        return Err(last_error("connect utun control socket"));
    }

    set_nonblocking(fd.as_raw_fd())?;
    let name = kernel_name(fd.as_raw_fd())?;
    set_mtu(&name, mtu)?;
    Ok((fd, name))
}

pub(super) fn try_recv(fd: RawFd, payload: &mut [u8]) -> CoreResult<Option<usize>> {
    let mut family = [0u8; UTUN_HEADER_LEN];
    let mut vectors = Vec::with_capacity(2);
    vectors.push(libc::iovec {
        iov_base: family.as_mut_ptr().cast::<c_void>(),
        iov_len: family.len(),
    });
    vectors.push(libc::iovec {
        iov_base: payload.as_mut_ptr().cast::<c_void>(),
        iov_len: payload.len(),
    });
    loop {
        // SAFETY: fd is required to remain live by the caller. Both iovec
        // entries reference live mutable byte slices for the full call and
        // their lengths are bounded by those allocations.
        let read = unsafe { libc::readv(fd, vectors.as_ptr(), 2) };
        if read >= 0 {
            let read = usize::try_from(read)
                .map_err(|_| CoreError::internal("utun read length overflow"))?;
            if read <= UTUN_HEADER_LEN {
                return Err(CoreError::internal("utun packet has no L3 payload"));
            }
            let family = u32::from_be_bytes(family);
            if family != libc::AF_INET as u32 && family != libc::AF_INET6 as u32 {
                return Err(CoreError::internal(
                    "utun packet has unsupported address family",
                ));
            }
            let payload_len = read - UTUN_HEADER_LEN;
            let version = payload
                .first()
                .map(|first| first >> 4)
                .ok_or_else(|| CoreError::internal("utun packet has empty L3 payload"))?;
            let expected = if family == libc::AF_INET as u32 { 4 } else { 6 };
            if version != expected {
                return Err(CoreError::internal(
                    "utun address family does not match the L3 version",
                ));
            }
            return Ok(Some(payload_len));
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::WouldBlock => return Ok(None),
            io::ErrorKind::Interrupted => continue,
            _ => return Err(CoreError::internal(format!("read utun packet: {error}"))),
        }
    }
}

pub(super) fn try_send(fd: RawFd, version: u8, segments: &[&[u8]]) -> CoreResult<bool> {
    let family = match version {
        4 => libc::AF_INET as u32,
        6 => libc::AF_INET6 as u32,
        _ => {
            return Err(CoreError::internal(
                "cannot send unsupported L3 version to utun",
            ));
        }
    };
    let vector_count = segments
        .len()
        .checked_add(1)
        .ok_or_else(|| CoreError::internal("utun buffer chain has too many segments"))?;
    let count = libc::c_int::try_from(vector_count)
        .map_err(|_| CoreError::internal("utun buffer chain has too many segments"))?;
    let header = family.to_be_bytes();
    let mut vectors = Vec::with_capacity(vector_count);
    vectors.push(libc::iovec {
        iov_base: header.as_ptr().cast::<c_void>().cast_mut(),
        iov_len: header.len(),
    });
    let mut total = header.len();
    for segment in segments {
        total = total
            .checked_add(segment.len())
            .ok_or_else(|| CoreError::internal("utun packet length overflow"))?;
        vectors.push(libc::iovec {
            iov_base: segment.as_ptr().cast::<c_void>().cast_mut(),
            iov_len: segment.len(),
        });
    }
    loop {
        // SAFETY: fd is required to remain live by the caller. Every iovec
        // points at a live immutable byte slice for the duration of writev;
        // libc does not mutate the pointed-to bytes.
        let written = unsafe { libc::writev(fd, vectors.as_ptr(), count) };
        if written >= 0 {
            let written = usize::try_from(written)
                .map_err(|_| CoreError::internal("utun write length overflow"))?;
            if written != total {
                return Err(CoreError::internal("partial utun datagram write"));
            }
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::WouldBlock => return Ok(false),
            io::ErrorKind::Interrupted => continue,
            _ => return Err(CoreError::internal(format!("write utun packet: {error}"))),
        }
    }
}

fn parse_unit(name: &str) -> CoreResult<u32> {
    let Some(suffix) = name.strip_prefix("utun") else {
        return Err(CoreError::internal("Darwin TUN name must start with utun"));
    };
    if suffix.is_empty() {
        return Ok(0);
    }
    suffix
        .parse::<u32>()
        .ok()
        .and_then(|unit| unit.checked_add(1))
        .ok_or_else(|| CoreError::internal("Darwin TUN name has an invalid unit"))
}

fn set_nonblocking(fd: RawFd) -> CoreResult<()> {
    // SAFETY: fd is live and F_GETFL has no pointer argument.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(last_error("read utun descriptor flags"));
    }
    // SAFETY: fd remains live and flags came from F_GETFL for this descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(last_error("set utun nonblocking mode"));
    }
    Ok(())
}

fn kernel_name(fd: RawFd) -> CoreResult<String> {
    let mut name = [0u8; libc::IFNAMSIZ];
    let mut length = libc::socklen_t::try_from(name.len())
        .map_err(|_| CoreError::internal("utun interface name buffer overflow"))?;
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
        return Err(last_error("read utun kernel name"));
    }
    let length = usize::try_from(length)
        .map_err(|_| CoreError::internal("utun interface name length overflow"))?;
    let bytes = name
        .get(..length)
        .ok_or_else(|| CoreError::internal("utun interface name length is invalid"))?;
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| CoreError::internal("utun interface name is not UTF-8"))
}

fn set_mtu(name: &str, mtu: u32) -> CoreResult<()> {
    let mtu = libc::c_int::try_from(mtu)
        .map_err(|_| CoreError::internal("utun MTU does not fit c_int"))?;
    if name.len() >= libc::IFNAMSIZ {
        return Err(CoreError::internal("utun interface name is too long"));
    }
    // SAFETY: socket has no pointer arguments. The descriptor is transferred
    // immediately to OwnedFd on success.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if raw < 0 {
        return Err(last_error("create interface control socket"));
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
        return Err(last_error("set utun MTU"));
    }
    Ok(())
}

fn last_error(operation: &str) -> CoreError {
    CoreError::internal(format!("{operation}: {}", io::Error::last_os_error()))
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
