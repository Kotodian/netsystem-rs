use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::Interest;

use crate::{AttachError, RuntimeResult};

use super::MAX_ATTACH_DESCRIPTORS;

pub(super) fn duplicate(descriptor: RawFd) -> RuntimeResult<OwnedFd> {
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(AttachError::ControlSignalDuplicate {
            source: io::Error::last_os_error(),
        }
        .into());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

pub(super) async fn send(
    client: &tokio::net::UnixStream,
    payload: &[u8],
    descriptors: &[RawFd],
) -> RuntimeResult<()> {
    if descriptors.len() > MAX_ATTACH_DESCRIPTORS {
        return Err(AttachError::DescriptorCountTooLarge {
            actual: descriptors.len(),
            max: MAX_ATTACH_DESCRIPTORS,
        }
        .into());
    }
    let control_bytes = control_buffer_len(descriptors.len());
    let control_elements = control_bytes.div_ceil(size_of::<libc::cmsghdr>());
    let mut control = Vec::<libc::cmsghdr>::with_capacity(control_elements);
    control.resize_with(control_elements, || {
        // SAFETY: a cmsghdr contains only integer fields, for which zero is a
        // valid initialized value.
        unsafe { std::mem::zeroed() }
    });
    loop {
        client
            .writable()
            .await
            .map_err(|source| AttachError::Send { source })?;
        let sent = client.try_io(Interest::WRITABLE, || {
            let iov = libc::iovec {
                iov_base: payload.as_ptr().cast_mut().cast::<libc::c_void>(),
                iov_len: payload.len(),
            };
            let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
            message.msg_iov = std::ptr::from_ref(&iov).cast_mut();
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
            message.msg_controllen = control_bytes as _;
            unsafe {
                let header = libc::CMSG_FIRSTHDR(&message);
                if header.is_null() {
                    return Err(io::Error::other("attach descriptor header is missing"));
                }
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(descriptors) as u32) as _;
                std::ptr::copy_nonoverlapping(
                    descriptors.as_ptr(),
                    libc::CMSG_DATA(header).cast::<RawFd>(),
                    descriptors.len(),
                );
            }
            let sent = unsafe { libc::sendmsg(client.as_raw_fd(), &message, 0) };
            if sent < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(sent as usize)
            }
        });
        match sent {
            Ok(sent) if sent == payload.len() => return Ok(()),
            Ok(sent) => {
                if sent == 0 || sent > payload.len() {
                    return Err(AttachError::Send {
                        source: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "attach descriptor send made no progress",
                        ),
                    }
                    .into());
                }
                let mut written = sent;
                while written < payload.len() {
                    client
                        .writable()
                        .await
                        .map_err(|source| AttachError::Send { source })?;
                    match client.try_write(&payload[written..]) {
                        Ok(0) => {
                            return Err(AttachError::Send {
                                source: io::Error::new(
                                    io::ErrorKind::WriteZero,
                                    "attach metadata send made no progress",
                                ),
                            }
                            .into());
                        }
                        Ok(bytes) => written += bytes,
                        Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
                        Err(source) => return Err(AttachError::Send { source }.into()),
                    }
                }
                return Ok(());
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => return Err(AttachError::Send { source }.into()),
        }
    }
}

fn control_buffer_len(descriptor_count: usize) -> usize {
    let payload_bytes = size_of::<RawFd>()
        .checked_mul(descriptor_count)
        .unwrap_or(usize::MAX);
    let payload_bytes = u32::try_from(payload_bytes).unwrap_or(u32::MAX);
    // SAFETY: libc CMSG_SPACE only computes a control-buffer size.
    unsafe { libc::CMSG_SPACE(payload_bytes) as usize }
}
