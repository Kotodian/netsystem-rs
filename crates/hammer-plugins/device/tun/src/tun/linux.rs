use std::ffi::c_void;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

const TUN_DEVICE: &[u8] = b"/dev/net/tun\0";

pub(super) fn open(requested_name: &str, mtu: u32) -> CoreResult<(OwnedFd, String)> {
    if requested_name.len() >= libc::IFNAMSIZ || requested_name.as_bytes().contains(&0) {
        return Err(CoreError::internal("Linux TUN interface name is invalid"));
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
        return Err(last_error("open /dev/net/tun"));
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
        return Err(last_error("configure Linux TUN interface"));
    }

    let name = interface_name(&request.ifr_name)?;
    set_mtu(&mut request, mtu)?;
    Ok((fd, name))
}

pub(super) fn try_recv(fd: RawFd, payload: &mut [u8]) -> CoreResult<Option<usize>> {
    let mut vectors = Vec::with_capacity(1);
    vectors.push(libc::iovec {
        iov_base: payload.as_mut_ptr().cast::<c_void>(),
        iov_len: payload.len(),
    });
    loop {
        // SAFETY: fd is required to remain live by the caller. The sole iovec
        // references payload for the full call and is bounded by its allocation.
        let read = unsafe { libc::readv(fd, vectors.as_ptr(), 1) };
        if read >= 0 {
            let read = usize::try_from(read)
                .map_err(|_| CoreError::internal("Linux TUN read length overflow"))?;
            if read == 0 {
                return Err(CoreError::internal("Linux TUN packet has no L3 payload"));
            }
            match payload[0] >> 4 {
                4 | 6 => return Ok(Some(read)),
                _ => {
                    return Err(CoreError::internal(
                        "Linux TUN packet has unsupported L3 version",
                    ));
                }
            }
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::WouldBlock => return Ok(None),
            io::ErrorKind::Interrupted => continue,
            _ => {
                return Err(CoreError::internal(format!(
                    "read Linux TUN packet: {error}"
                )));
            }
        }
    }
}

pub(super) fn try_send(fd: RawFd, version: u8, segments: &[&[u8]]) -> CoreResult<bool> {
    if version != 4 && version != 6 {
        return Err(CoreError::internal(
            "cannot send unsupported L3 version to Linux TUN",
        ));
    }
    let packet_version = segments
        .iter()
        .find_map(|segment| segment.first())
        .map(|first| first >> 4)
        .ok_or_else(|| CoreError::internal("Linux TUN packet has no L3 payload"))?;
    if packet_version != version {
        return Err(CoreError::internal(
            "Linux TUN packet does not match the supplied L3 version",
        ));
    }
    let count = libc::c_int::try_from(segments.len())
        .map_err(|_| CoreError::internal("Linux TUN buffer chain has too many segments"))?;
    let mut vectors = Vec::with_capacity(segments.len());
    let mut total = 0usize;
    for segment in segments {
        total = total
            .checked_add(segment.len())
            .ok_or_else(|| CoreError::internal("Linux TUN packet length overflow"))?;
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
                .map_err(|_| CoreError::internal("Linux TUN write length overflow"))?;
            if written != total {
                return Err(CoreError::internal("partial Linux TUN packet write"));
            }
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::WouldBlock => return Ok(false),
            io::ErrorKind::Interrupted => continue,
            _ => {
                return Err(CoreError::internal(format!(
                    "write Linux TUN packet: {error}"
                )));
            }
        }
    }
}

fn interface_name(name: &[libc::c_char; libc::IFNAMSIZ]) -> CoreResult<String> {
    let length = name
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| CoreError::internal("Linux TUN interface name is not terminated"))?;
    if length == 0 {
        return Err(CoreError::internal("Linux TUN interface name is empty"));
    }
    // SAFETY: c_char and u8 have identical size and alignment. The source
    // array is initialized, live, and contains at least length elements.
    let bytes = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u8>(), length) };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| CoreError::internal("Linux TUN interface name is not UTF-8"))
}

fn set_mtu(request: &mut libc::ifreq, mtu: u32) -> CoreResult<()> {
    let mtu = libc::c_int::try_from(mtu)
        .map_err(|_| CoreError::internal("Linux TUN MTU does not fit c_int"))?;
    // SAFETY: socket has no pointer arguments. A successful descriptor is
    // transferred immediately into OwnedFd and therefore closed exactly once.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if raw < 0 {
        return Err(last_error("create Linux interface control socket"));
    }
    // SAFETY: raw is a newly-created, uniquely-owned file descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    request.ifr_ifru = libc::__c_anonymous_ifr_ifru { ifru_mtu: mtu };
    // SAFETY: fd is a live AF_INET control socket, request has libc's native
    // ifreq layout, and the pointer remains valid for the entire ioctl.
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::SIOCSIFMTU, request) } < 0 {
        return Err(last_error("set Linux TUN MTU"));
    }
    Ok(())
}

fn last_error(operation: &str) -> CoreError {
    CoreError::internal(format!("{operation}: {}", io::Error::last_os_error()))
}
