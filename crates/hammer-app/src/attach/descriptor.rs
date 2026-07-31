use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use super::AppClientError;

pub(super) fn receive<const METADATA_BYTES: usize, const DESCRIPTOR_COUNT: usize>(
    stream: &UnixStream,
) -> Result<([u8; METADATA_BYTES], [OwnedFd; DESCRIPTOR_COUNT]), AppClientError> {
    let mut metadata = [0_u8; METADATA_BYTES];
    let mut control = [0_u8; 128];
    let mut iov = libc::iovec {
        iov_base: metadata.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: metadata.len(),
    };
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = std::ptr::from_mut(&mut iov);
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = control.len() as _;
    let received_bytes = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut message, 0) };
    if received_bytes < 0 {
        return Err(AppClientError::Receive {
            source: std::io::Error::last_os_error(),
        });
    }

    let mut descriptors = Vec::with_capacity(DESCRIPTOR_COUNT);
    let mut rights_headers = 0_usize;
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level != libc::SOL_SOCKET || (*header).cmsg_type != libc::SCM_RIGHTS {
                return Err(AppClientError::UnexpectedControl);
            }
            rights_headers += 1;
            let header_len = libc::CMSG_LEN(0) as usize;
            let control_len = (*header).cmsg_len as usize;
            if control_len < header_len {
                return Err(AppClientError::InvalidControlHeader);
            }
            let payload_len = control_len - header_len;
            if payload_len % size_of::<RawFd>() != 0 {
                return Err(AppClientError::InvalidDescriptorPayload);
            }
            let descriptor_count = payload_len / size_of::<RawFd>();
            let data = libc::CMSG_DATA(header).cast::<RawFd>();
            for index in 0..descriptor_count {
                descriptors.push(OwnedFd::from_raw_fd(data.add(index).read_unaligned()));
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if received_bytes as usize != metadata.len() {
        return Err(AppClientError::MetadataLength {
            expected: metadata.len(),
            actual: received_bytes as usize,
        });
    }
    if message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0 {
        return Err(AppClientError::ControlTruncated);
    }
    if rights_headers != 1 || descriptors.len() != DESCRIPTOR_COUNT {
        return Err(AppClientError::DescriptorCount {
            expected: DESCRIPTOR_COUNT,
            actual: descriptors.len(),
        });
    }
    for descriptor in &descriptors {
        let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            return Err(AppClientError::ReceivedDescriptorFlags {
                source: std::io::Error::last_os_error(),
            });
        }
        if unsafe {
            libc::fcntl(
                descriptor.as_raw_fd(),
                libc::F_SETFD,
                flags | libc::FD_CLOEXEC,
            )
        } < 0
        {
            return Err(AppClientError::ReceivedDescriptorCloseOnExec {
                source: std::io::Error::last_os_error(),
            });
        }
    }
    let descriptors = descriptors
        .try_into()
        .map_err(
            |descriptors: Vec<OwnedFd>| AppClientError::DescriptorCount {
                expected: DESCRIPTOR_COUNT,
                actual: descriptors.len(),
            },
        )?;
    Ok((metadata, descriptors))
}

pub(super) fn words<const WORDS: usize>(metadata: &[u8]) -> Result<[u64; WORDS], AppClientError> {
    if metadata.len() != WORDS * size_of::<u64>() {
        return Err(AppClientError::MetadataLength {
            expected: WORDS * size_of::<u64>(),
            actual: metadata.len(),
        });
    }
    let mut words = [0_u64; WORDS];
    for (word, bytes) in words
        .iter_mut()
        .zip(metadata.chunks_exact(size_of::<u64>()))
    {
        *word = u64::from_le_bytes(
            bytes
                .try_into()
                .expect("validated attach metadata contains complete words"),
        );
    }
    Ok(words)
}
