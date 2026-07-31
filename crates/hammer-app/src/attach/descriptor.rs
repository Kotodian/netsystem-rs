use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use hammer_runtime::attach::MAX_ATTACH_DESCRIPTORS;

use super::AppClientError;

pub(super) fn receive(
    stream: &UnixStream,
    metadata_len: usize,
) -> Result<(Vec<u8>, Vec<OwnedFd>), AppClientError> {
    let mut metadata = vec![0_u8; metadata_len];
    let control_capacity = control_buffer_len(MAX_ATTACH_DESCRIPTORS);
    let control_elements = control_capacity.div_ceil(size_of::<libc::cmsghdr>());
    let mut control = Vec::<libc::cmsghdr>::with_capacity(control_elements);
    control.resize_with(control_elements, || {
        // SAFETY: a cmsghdr contains only integer fields, for which zero is a
        // valid initialized value. The kernel overwrites received headers.
        unsafe { std::mem::zeroed() }
    });
    let mut iov = libc::iovec {
        iov_base: metadata.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: metadata.len(),
    };
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = std::ptr::from_mut(&mut iov);
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = control_capacity as _;
    let received_bytes = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut message, 0) };
    if received_bytes < 0 {
        return Err(AppClientError::Receive {
            source: std::io::Error::last_os_error(),
        });
    }
    if received_bytes as usize > metadata.len() {
        return Err(AppClientError::MetadataLength {
            expected: metadata.len(),
            actual: received_bytes as usize,
        });
    }
    if message.msg_controllen as usize > control_capacity {
        return Err(AppClientError::InvalidControlHeader);
    }
    let mut descriptors = Vec::with_capacity(MAX_ATTACH_DESCRIPTORS);
    let mut rights_headers = 0_usize;
    let mut control_error = None;
    unsafe {
        let control_start = message.msg_control as usize;
        let control_end = control_start
            .checked_add(message.msg_controllen as usize)
            .ok_or(AppClientError::InvalidControlHeader)?;
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            let header_start = header as usize;
            let Some(header_end) = header_start.checked_add(size_of::<libc::cmsghdr>()) else {
                control_error.get_or_insert(AppClientError::InvalidControlHeader);
                break;
            };
            if header_start < control_start || header_end > control_end {
                control_error.get_or_insert(AppClientError::InvalidControlHeader);
                break;
            }
            let control_len = (*header).cmsg_len as usize;
            let Some(message_end) = header_start.checked_add(control_len) else {
                control_error.get_or_insert(AppClientError::InvalidControlHeader);
                break;
            };
            if message_end > control_end {
                control_error.get_or_insert(AppClientError::InvalidControlHeader);
                break;
            }
            if (*header).cmsg_level != libc::SOL_SOCKET || (*header).cmsg_type != libc::SCM_RIGHTS {
                control_error.get_or_insert(AppClientError::UnexpectedControl);
            } else {
                rights_headers += 1;
                let header_len = libc::CMSG_LEN(0) as usize;
                if control_len < header_len {
                    control_error.get_or_insert(AppClientError::InvalidControlHeader);
                    break;
                }
                let payload_len = control_len - header_len;
                if payload_len % size_of::<RawFd>() != 0 {
                    control_error.get_or_insert(AppClientError::InvalidDescriptorPayload);
                }
                let descriptor_count = payload_len / size_of::<RawFd>();
                let data = libc::CMSG_DATA(header).cast::<RawFd>();
                for index in 0..descriptor_count {
                    descriptors.push(OwnedFd::from_raw_fd(data.add(index).read_unaligned()));
                }
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0 {
        return Err(AppClientError::ControlTruncated);
    }
    if let Some(error) = control_error {
        return Err(error);
    }
    if rights_headers > 1 {
        return Err(AppClientError::UnexpectedControl);
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
    let received_bytes = received_bytes as usize;
    if received_bytes < metadata.len() {
        let mut stream = stream;
        stream
            .read_exact(&mut metadata[received_bytes..])
            .map_err(|source| AppClientError::Receive { source })?;
    }
    Ok((metadata, descriptors))
}

pub(super) fn words_prefix<const WORDS: usize>(
    metadata: &[u8],
) -> Result<[u64; WORDS], AppClientError> {
    if metadata.len() < WORDS * size_of::<u64>() {
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

pub(super) fn words_slice(
    metadata: &[u8],
    offset_words: usize,
    count: usize,
) -> Result<Vec<u64>, AppClientError> {
    let start = offset_words
        .checked_mul(size_of::<u64>())
        .ok_or(AppClientError::InvalidDescriptorPayload)?;
    let end = count
        .checked_mul(size_of::<u64>())
        .and_then(|len| start.checked_add(len))
        .ok_or(AppClientError::InvalidDescriptorPayload)?;
    if metadata.len() < end {
        return Err(AppClientError::MetadataLength {
            expected: end,
            actual: metadata.len(),
        });
    }
    Ok(metadata[start..end]
        .chunks_exact(size_of::<u64>())
        .map(|bytes| {
            u64::from_le_bytes(
                bytes
                    .try_into()
                    .expect("validated metadata slice contains complete u64 words"),
            )
        })
        .collect::<Vec<_>>())
}

fn control_buffer_len(descriptor_count: usize) -> usize {
    let payload_bytes = size_of::<RawFd>()
        .checked_mul(descriptor_count)
        .unwrap_or(usize::MAX);
    let payload_bytes = u32::try_from(payload_bytes).unwrap_or(u32::MAX);
    // SAFETY: libc CMSG_SPACE only computes a control-buffer size.
    unsafe { libc::CMSG_SPACE(payload_bytes) as usize }
}
