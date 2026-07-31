use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::Interest;

use crate::{AttachError, RuntimeResult};

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
    metadata: &[u8],
    descriptors: &[RawFd],
) -> RuntimeResult<()> {
    loop {
        client
            .writable()
            .await
            .map_err(|source| AttachError::Send { source })?;
        let sent = client.try_io(Interest::WRITABLE, || {
            let iov = libc::iovec {
                iov_base: metadata.as_ptr().cast_mut().cast::<libc::c_void>(),
                iov_len: metadata.len(),
            };
            let mut control = [0_u8; 128];
            let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
            message.msg_iov = std::ptr::from_ref(&iov).cast_mut();
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
            message.msg_controllen = control.len() as _;
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
                message.msg_controllen = (*header).cmsg_len;
            }
            let sent = unsafe { libc::sendmsg(client.as_raw_fd(), &message, 0) };
            if sent < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(sent as usize)
            }
        });
        match sent {
            Ok(sent) if sent == metadata.len() => return Ok(()),
            Ok(sent) => {
                return Err(AttachError::MetadataWriteIncomplete {
                    expected: metadata.len(),
                    actual: sent,
                }
                .into());
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => return Err(AttachError::Send { source }.into()),
        }
    }
}
