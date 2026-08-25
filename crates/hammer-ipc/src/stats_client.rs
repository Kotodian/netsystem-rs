use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::ptr;

use memmap2::{Mmap, MmapOptions};
use socket2::{Domain, SockAddr, Socket, Type};

use hammer_stats::{
    Counter, DirectoryDataPointer, DirectoryEntry, DirectoryType, GaugeValue as ProtocolGauge,
    MetricValue, RingBufferHeader, ScalarBits, SharedHeader, StatsError, StatsResult,
    StringVectorPointer, ring_layout, vec_len, vector_element_offset,
};

pub struct StatsClient {
    mapping: Mmap,
}

impl StatsClient {
    pub fn connect(socket_path: impl AsRef<Path>) -> StatsResult<Self> {
        let socket_path = socket_path.as_ref();
        #[cfg(target_os = "linux")]
        let socket = Socket::new(Domain::UNIX, Type::SEQPACKET, None);
        #[cfg(not(target_os = "linux"))]
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None);
        let socket = socket.map_err(|source| StatsError::ClientConnect {
            path: socket_path.to_owned(),
            source,
        })?;
        socket
            .set_cloexec(true)
            .map_err(|source| StatsError::ClientConnect {
                path: socket_path.to_owned(),
                source,
            })?;
        let address = SockAddr::unix(socket_path).map_err(|source| StatsError::ClientConnect {
            path: socket_path.to_owned(),
            source,
        })?;
        socket
            .connect(&address)
            .map_err(|source| StatsError::ClientConnect {
                path: socket_path.to_owned(),
                source,
            })?;

        let control_size = unsafe { libc::CMSG_SPACE((size_of::<RawFd>() * 8) as u32) as usize };
        let mut control = vec![0_u8; control_size];
        #[cfg(not(target_os = "linux"))]
        let mut handoff = 0_u8;
        #[cfg(not(target_os = "linux"))]
        let mut iovec = libc::iovec {
            iov_base: (&mut handoff as *mut u8).cast(),
            iov_len: 1,
        };
        #[cfg(target_os = "linux")]
        let (iov_base, iov_len) = (ptr::null_mut(), 0);
        #[cfg(not(target_os = "linux"))]
        let (iov_base, iov_len) = (&mut iovec as *mut libc::iovec, 1);
        let mut message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iov_base,
            msg_iovlen: iov_len,
            msg_control: control.as_mut_ptr().cast(),
            msg_controllen: control.len().try_into().map_err(|_| {
                StatsError::ClientAncillaryData {
                    received_fds: 0,
                    malformed: true,
                }
            })?,
            msg_flags: 0,
        };
        let received = loop {
            // SAFETY: `message` points at the live control buffer for this
            // synchronous receive, and the socket owns the descriptor.
            let received = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, 0) };
            if received >= 0 {
                break received;
            }
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(StatsError::ClientReceive { source });
        };
        let mut received_fds = Vec::new();
        let mut received_fd_count = 0;
        let mut malformed = (message.msg_flags & libc::MSG_CTRUNC) != 0;
        unsafe {
            let mut current = libc::CMSG_FIRSTHDR(&message);
            while !current.is_null() {
                let length = (*current).cmsg_len as usize;
                let data_offset = libc::CMSG_LEN(0) as usize;
                if (*current).cmsg_level != libc::SOL_SOCKET
                    || (*current).cmsg_type != libc::SCM_RIGHTS
                    || length < data_offset
                {
                    malformed = true;
                } else {
                    let data_length = length - data_offset;
                    if data_length == 0 {
                        malformed = true;
                    } else {
                        let descriptor_count = data_length / size_of::<RawFd>();
                        if data_length % size_of::<RawFd>() != 0 {
                            malformed = true;
                        }
                        let data = libc::CMSG_DATA(current).cast::<RawFd>();
                        for descriptor in 0..descriptor_count {
                            received_fd_count += 1;
                            let raw_fd = ptr::read_unaligned(data.add(descriptor));
                            if raw_fd < 0 {
                                malformed = true;
                            } else {
                                received_fds.push(OwnedFd::from_raw_fd(raw_fd));
                            }
                        }
                    }
                }
                current = libc::CMSG_NXTHDR(&message, current);
            }
        }
        #[cfg(not(target_os = "linux"))]
        if received != 1 || handoff != 1 {
            return Err(StatsError::ClientReceive {
                source: io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid stats stream handoff frame",
                ),
            });
        }
        if malformed || received_fd_count != 1 || received_fds.len() != 1 {
            return Err(StatsError::ClientAncillaryData {
                received_fds: received_fd_count,
                malformed,
            });
        }
        let Some(segment_fd) = received_fds.pop() else {
            return Err(StatsError::ClientAncillaryData {
                received_fds: received_fd_count,
                malformed: true,
            });
        };

        let mut metadata = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `metadata` is valid writable storage for `fstat`.
        if unsafe { libc::fstat(segment_fd.as_raw_fd(), metadata.as_mut_ptr()) } < 0 {
            return Err(StatsError::ClientFstat {
                source: io::Error::last_os_error(),
            });
        }
        // SAFETY: `fstat` initialized every field on success.
        let metadata = unsafe { metadata.assume_init() };
        let length = usize::try_from(metadata.st_size).map_err(|_| StatsError::ClientFstat {
            source: io::Error::new(io::ErrorKind::InvalidData, "negative stats segment size"),
        })?;
        if length == 0 {
            return Err(StatsError::ClientFstat {
                source: io::Error::new(io::ErrorKind::InvalidData, "empty stats segment"),
            });
        }
        let mapping = unsafe { MmapOptions::new().len(length).map(&segment_fd) }
            .map_err(|source| StatsError::ClientMapping { source })?;
        drop(segment_fd);
        Ok(Self { mapping })
    }

    pub fn list(&self) -> StatsResult<Vec<String>> {
        for _ in 0..16 {
            if self.mapping.len() < size_of::<SharedHeader>() {
                return Err(StatsError::Protocol);
            }
            let header =
                unsafe { ptr::read_volatile(self.mapping.as_ptr().cast::<SharedHeader>()) };
            header.validate_version()?;
            if header.is_write_in_progress() {
                continue;
            }
            let epoch = header.epoch();
            let published_base = header.base() as usize;
            let published_directory = header.directory_vector() as usize;
            if published_base == 0 || published_directory < published_base {
                return Err(StatsError::Protocol);
            }
            let directory_relative = published_directory - published_base;
            let directory_header_offset = directory_relative
                .checked_sub(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?;
            let directory_offset = directory_header_offset
                .checked_add(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?;
            let directory_header_end = directory_header_offset
                .checked_add(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?;
            if directory_header_end > self.mapping.len() || directory_offset > self.mapping.len() {
                return Err(StatsError::Protocol);
            }
            let directory_header = unsafe {
                ptr::read_unaligned(
                    self.mapping
                        .as_ptr()
                        .add(directory_header_offset)
                        .cast::<[u8; 8]>(),
                )
            };
            let directory_length = usize::try_from(vec_len(Some(&directory_header)))
                .map_err(|_| StatsError::Protocol)?;
            let directory_bytes = directory_length
                .checked_mul(size_of::<DirectoryEntry>())
                .ok_or(StatsError::Protocol)?;
            let directory_end = directory_offset
                .checked_add(directory_bytes)
                .ok_or(StatsError::Protocol)?;
            if directory_end > self.mapping.len() {
                return Err(StatsError::Protocol);
            }
            let mut names = Vec::with_capacity(directory_length);
            for index in 0..directory_length {
                let entry_offset = vector_element_offset(
                    directory_header_offset,
                    directory_offset,
                    &directory_header,
                    index,
                    size_of::<DirectoryEntry>(),
                    self.mapping.len(),
                )?;
                let entry = unsafe {
                    ptr::read_unaligned(
                        self.mapping
                            .as_ptr()
                            .add(entry_offset)
                            .cast::<DirectoryEntry>(),
                    )
                };
                let name = entry.name()?.to_str().map_err(|_| StatsError::Protocol)?;
                names.push(name.to_owned());
            }
            let end_header =
                unsafe { ptr::read_volatile(self.mapping.as_ptr().cast::<SharedHeader>()) };
            end_header.validate_version()?;
            if !end_header.is_write_in_progress() && end_header.epoch() == epoch {
                return Ok(names);
            }
        }
        Err(StatsError::ClientRetryExhausted { operation: "list" })
    }

    pub fn read(&self, name: &str) -> StatsResult<MetricValue> {
        for _ in 0..16 {
            if self.mapping.len() < size_of::<SharedHeader>() {
                return Err(StatsError::Protocol);
            }
            let header =
                unsafe { ptr::read_volatile(self.mapping.as_ptr().cast::<SharedHeader>()) };
            header.validate_version()?;
            if header.is_write_in_progress() {
                continue;
            }
            let epoch = header.epoch();
            let published_base = header.base() as usize;
            let published_directory = header.directory_vector() as usize;
            if published_base == 0 || published_directory < published_base {
                return Err(StatsError::Protocol);
            }
            let directory_relative = published_directory - published_base;
            let directory_header_offset = directory_relative
                .checked_sub(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?;
            let directory_offset = directory_header_offset
                .checked_add(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?;
            if directory_header_offset
                .checked_add(size_of::<[u8; 8]>())
                .ok_or(StatsError::Protocol)?
                > self.mapping.len()
                || directory_offset > self.mapping.len()
            {
                return Err(StatsError::Protocol);
            }
            let directory_header = unsafe {
                ptr::read_unaligned(
                    self.mapping
                        .as_ptr()
                        .add(directory_header_offset)
                        .cast::<[u8; 8]>(),
                )
            };
            let directory_length = usize::try_from(vec_len(Some(&directory_header)))
                .map_err(|_| StatsError::Protocol)?;
            let directory_end = directory_offset
                .checked_add(
                    directory_length
                        .checked_mul(size_of::<DirectoryEntry>())
                        .ok_or(StatsError::Protocol)?,
                )
                .ok_or(StatsError::Protocol)?;
            if directory_end > self.mapping.len() {
                return Err(StatsError::Protocol);
            }
            let mut found = None;
            for index in 0..directory_length {
                let entry_offset = vector_element_offset(
                    directory_header_offset,
                    directory_offset,
                    &directory_header,
                    index,
                    size_of::<DirectoryEntry>(),
                    self.mapping.len(),
                )?;
                let entry = unsafe {
                    ptr::read_unaligned(
                        self.mapping
                            .as_ptr()
                            .add(entry_offset)
                            .cast::<DirectoryEntry>(),
                    )
                };
                if entry.name()?.to_bytes() == name.as_bytes() {
                    found = Some(entry);
                    break;
                }
            }
            let Some(entry) = found else {
                return Err(StatsError::MetricNotFound {
                    name: name.to_owned(),
                });
            };
            let kind = DirectoryType::try_from(entry.kind())?;
            let value = match kind {
                DirectoryType::ScalarIndex => MetricValue::Scalar(ScalarBits::try_from(&entry)?),
                DirectoryType::Gauge => MetricValue::Gauge(ProtocolGauge::try_from(&entry)?),
                DirectoryType::CounterVectorSimple => {
                    let published_outer = DirectoryDataPointer::try_from(&entry)?.as_ptr() as usize;
                    if published_outer == 0 {
                        MetricValue::Simple(Vec::new())
                    } else {
                        if published_outer < published_base {
                            return Err(StatsError::Protocol);
                        }
                        let outer_relative = published_outer - published_base;
                        let outer_header_offset = outer_relative
                            .checked_sub(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        let outer_offset = outer_header_offset
                            .checked_add(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        if outer_offset > self.mapping.len() {
                            return Err(StatsError::Protocol);
                        }
                        let outer_header = unsafe {
                            ptr::read_unaligned(
                                self.mapping
                                    .as_ptr()
                                    .add(outer_header_offset)
                                    .cast::<[u8; 8]>(),
                            )
                        };
                        let outer_length = usize::try_from(vec_len(Some(&outer_header)))
                            .map_err(|_| StatsError::Protocol)?;
                        let mut rows = Vec::with_capacity(outer_length);
                        for row in 0..outer_length {
                            let row_offset = vector_element_offset(
                                outer_header_offset,
                                outer_offset,
                                &outer_header,
                                row,
                                size_of::<*mut u8>(),
                                self.mapping.len(),
                            )?;
                            let published_inner = unsafe {
                                ptr::read_unaligned(
                                    self.mapping.as_ptr().add(row_offset).cast::<*mut u8>(),
                                ) as usize
                            };
                            if published_inner < published_base {
                                return Err(StatsError::Protocol);
                            }
                            let inner_relative = published_inner - published_base;
                            let inner_header_offset = inner_relative
                                .checked_sub(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            let inner_offset = inner_header_offset
                                .checked_add(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            if inner_offset > self.mapping.len() {
                                return Err(StatsError::Protocol);
                            }
                            let inner_header = unsafe {
                                ptr::read_unaligned(
                                    self.mapping
                                        .as_ptr()
                                        .add(inner_header_offset)
                                        .cast::<[u8; 8]>(),
                                )
                            };
                            let inner_length = usize::try_from(vec_len(Some(&inner_header)))
                                .map_err(|_| StatsError::Protocol)?;
                            let mut values = Vec::with_capacity(inner_length);
                            for column in 0..inner_length {
                                let value_offset = vector_element_offset(
                                    inner_header_offset,
                                    inner_offset,
                                    &inner_header,
                                    column,
                                    size_of::<u64>(),
                                    self.mapping.len(),
                                )?;
                                values.push(unsafe {
                                    ptr::read_unaligned(
                                        self.mapping.as_ptr().add(value_offset).cast::<u64>(),
                                    )
                                });
                            }
                            rows.push(values);
                        }
                        MetricValue::Simple(rows)
                    }
                }
                DirectoryType::CounterVectorCombined => {
                    let published_outer = DirectoryDataPointer::try_from(&entry)?.as_ptr() as usize;
                    if published_outer == 0 {
                        MetricValue::Combined(Vec::new())
                    } else {
                        if published_outer < published_base {
                            return Err(StatsError::Protocol);
                        }
                        let outer_relative = published_outer - published_base;
                        let outer_header_offset = outer_relative
                            .checked_sub(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        let outer_offset = outer_header_offset
                            .checked_add(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        if outer_offset > self.mapping.len() {
                            return Err(StatsError::Protocol);
                        }
                        let outer_header = unsafe {
                            ptr::read_unaligned(
                                self.mapping
                                    .as_ptr()
                                    .add(outer_header_offset)
                                    .cast::<[u8; 8]>(),
                            )
                        };
                        let outer_length = usize::try_from(vec_len(Some(&outer_header)))
                            .map_err(|_| StatsError::Protocol)?;
                        let mut rows = Vec::with_capacity(outer_length);
                        for row in 0..outer_length {
                            let row_offset = vector_element_offset(
                                outer_header_offset,
                                outer_offset,
                                &outer_header,
                                row,
                                size_of::<*mut u8>(),
                                self.mapping.len(),
                            )?;
                            let published_inner = unsafe {
                                ptr::read_unaligned(
                                    self.mapping.as_ptr().add(row_offset).cast::<*mut u8>(),
                                ) as usize
                            };
                            if published_inner < published_base {
                                return Err(StatsError::Protocol);
                            }
                            let inner_relative = published_inner - published_base;
                            let inner_header_offset = inner_relative
                                .checked_sub(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            let inner_offset = inner_header_offset
                                .checked_add(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            if inner_offset > self.mapping.len() {
                                return Err(StatsError::Protocol);
                            }
                            let inner_header = unsafe {
                                ptr::read_unaligned(
                                    self.mapping
                                        .as_ptr()
                                        .add(inner_header_offset)
                                        .cast::<[u8; 8]>(),
                                )
                            };
                            let inner_length = usize::try_from(vec_len(Some(&inner_header)))
                                .map_err(|_| StatsError::Protocol)?;
                            let mut values = Vec::with_capacity(inner_length);
                            for column in 0..inner_length {
                                let value_offset = vector_element_offset(
                                    inner_header_offset,
                                    inner_offset,
                                    &inner_header,
                                    column,
                                    size_of::<Counter>(),
                                    self.mapping.len(),
                                )?;
                                values.push(unsafe {
                                    ptr::read_unaligned(
                                        self.mapping.as_ptr().add(value_offset).cast::<Counter>(),
                                    )
                                });
                            }
                            rows.push(values);
                        }
                        MetricValue::Combined(rows)
                    }
                }
                DirectoryType::NameVector => {
                    let pointer = StringVectorPointer::try_from(&entry)?.as_ptr() as usize;
                    if pointer < published_base {
                        return Err(StatsError::Protocol);
                    }
                    let outer_relative = pointer - published_base;
                    let outer_header_offset = outer_relative
                        .checked_sub(size_of::<[u8; 8]>())
                        .ok_or(StatsError::Protocol)?;
                    let outer_offset = outer_header_offset
                        .checked_add(size_of::<[u8; 8]>())
                        .ok_or(StatsError::Protocol)?;
                    if outer_offset > self.mapping.len() {
                        return Err(StatsError::Protocol);
                    }
                    let outer_header = unsafe {
                        ptr::read_unaligned(
                            self.mapping
                                .as_ptr()
                                .add(outer_header_offset)
                                .cast::<[u8; 8]>(),
                        )
                    };
                    let outer_length = usize::try_from(vec_len(Some(&outer_header)))
                        .map_err(|_| StatsError::Protocol)?;
                    let mut names = Vec::with_capacity(outer_length);
                    for index in 0..outer_length {
                        let pointer_offset = vector_element_offset(
                            outer_header_offset,
                            outer_offset,
                            &outer_header,
                            index,
                            size_of::<*mut u8>(),
                            self.mapping.len(),
                        )?;
                        let published_name = unsafe {
                            ptr::read_unaligned(
                                self.mapping.as_ptr().add(pointer_offset).cast::<*mut u8>(),
                            ) as usize
                        };
                        if published_name < published_base {
                            return Err(StatsError::Protocol);
                        }
                        let name_relative = published_name - published_base;
                        if name_relative >= self.mapping.len() {
                            return Err(StatsError::Protocol);
                        }
                        let available = self.mapping.len() - name_relative;
                        let bytes = unsafe {
                            std::slice::from_raw_parts(
                                self.mapping.as_ptr().add(name_relative),
                                available,
                            )
                        };
                        let Some(end) = bytes.iter().position(|byte| *byte == 0) else {
                            return Err(StatsError::Protocol);
                        };
                        let value = std::str::from_utf8(&bytes[..end])
                            .map_err(|_| StatsError::Protocol)?
                            .to_owned();
                        names.push(value);
                    }
                    MetricValue::Names(names)
                }
                DirectoryType::HistogramLog2 => {
                    let published_outer = DirectoryDataPointer::try_from(&entry)?.as_ptr() as usize;
                    if published_outer == 0 {
                        MetricValue::Histogram(Vec::new())
                    } else {
                        if published_outer < published_base {
                            return Err(StatsError::Protocol);
                        }
                        let outer_relative = published_outer - published_base;
                        let outer_header_offset = outer_relative
                            .checked_sub(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        let outer_offset = outer_header_offset
                            .checked_add(size_of::<[u8; 8]>())
                            .ok_or(StatsError::Protocol)?;
                        if outer_offset > self.mapping.len() {
                            return Err(StatsError::Protocol);
                        }
                        let outer_header = unsafe {
                            ptr::read_unaligned(
                                self.mapping
                                    .as_ptr()
                                    .add(outer_header_offset)
                                    .cast::<[u8; 8]>(),
                            )
                        };
                        let outer_length = usize::try_from(vec_len(Some(&outer_header)))
                            .map_err(|_| StatsError::Protocol)?;
                        let mut rows = Vec::with_capacity(outer_length);
                        for row in 0..outer_length {
                            let row_offset = vector_element_offset(
                                outer_header_offset,
                                outer_offset,
                                &outer_header,
                                row,
                                size_of::<*mut u8>(),
                                self.mapping.len(),
                            )?;
                            let published_inner = unsafe {
                                ptr::read_unaligned(
                                    self.mapping.as_ptr().add(row_offset).cast::<*mut u8>(),
                                ) as usize
                            };
                            if published_inner < published_base {
                                return Err(StatsError::Protocol);
                            }
                            let inner_relative = published_inner - published_base;
                            let inner_header_offset = inner_relative
                                .checked_sub(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            let inner_offset = inner_header_offset
                                .checked_add(size_of::<[u8; 8]>())
                                .ok_or(StatsError::Protocol)?;
                            if inner_offset > self.mapping.len() {
                                return Err(StatsError::Protocol);
                            }
                            let inner_header = unsafe {
                                ptr::read_unaligned(
                                    self.mapping
                                        .as_ptr()
                                        .add(inner_header_offset)
                                        .cast::<[u8; 8]>(),
                                )
                            };
                            let inner_length = usize::try_from(vec_len(Some(&inner_header)))
                                .map_err(|_| StatsError::Protocol)?;
                            let mut values = Vec::with_capacity(inner_length);
                            for column in 0..inner_length {
                                let value_offset = vector_element_offset(
                                    inner_header_offset,
                                    inner_offset,
                                    &inner_header,
                                    column,
                                    size_of::<u64>(),
                                    self.mapping.len(),
                                )?;
                                values.push(unsafe {
                                    ptr::read_unaligned(
                                        self.mapping.as_ptr().add(value_offset).cast::<u64>(),
                                    )
                                });
                            }
                            rows.push(values);
                        }
                        MetricValue::Histogram(rows)
                    }
                }
                DirectoryType::RingBuffer => {
                    let published_ring = DirectoryDataPointer::try_from(&entry)?.as_ptr() as usize;
                    if published_ring < published_base {
                        return Err(StatsError::Protocol);
                    }
                    let ring_relative = published_ring - published_base;
                    if ring_relative >= self.mapping.len() {
                        return Err(StatsError::Protocol);
                    }
                    let ring_header = unsafe {
                        ptr::read_unaligned(
                            self.mapping
                                .as_ptr()
                                .add(ring_relative)
                                .cast::<RingBufferHeader>(),
                        )
                    };
                    let config = ring_header.config();
                    let (_, total) = ring_layout(config, 64, self.mapping.len() - ring_relative)?;
                    if total > self.mapping.len() - ring_relative {
                        return Err(StatsError::Protocol);
                    }
                    let entry_size =
                        usize::try_from(config.entry_size()).map_err(|_| StatsError::Protocol)?;
                    let ring_size =
                        usize::try_from(config.ring_size()).map_err(|_| StatsError::Protocol)?;
                    let thread_count =
                        usize::try_from(config.n_threads()).map_err(|_| StatsError::Protocol)?;
                    let data_offset = usize::try_from(ring_header.data_offset())
                        .map_err(|_| StatsError::Protocol)?;
                    let value_count = thread_count
                        .checked_mul(ring_size)
                        .ok_or(StatsError::Protocol)?;
                    let mut values = Vec::with_capacity(value_count);
                    for thread in 0..thread_count {
                        for slot in 0..ring_size {
                            let entry_offset = ring_relative
                                .checked_add(data_offset)
                                .and_then(|offset| {
                                    thread
                                        .checked_mul(ring_size)
                                        .and_then(|index| index.checked_add(slot))
                                        .and_then(|index| index.checked_mul(entry_size))
                                        .and_then(|delta| offset.checked_add(delta))
                                })
                                .ok_or(StatsError::Protocol)?;
                            let entry_end = entry_offset
                                .checked_add(entry_size)
                                .ok_or(StatsError::Protocol)?;
                            if entry_end > self.mapping.len() {
                                return Err(StatsError::Protocol);
                            }
                            values.push(unsafe {
                                std::slice::from_raw_parts(
                                    self.mapping.as_ptr().add(entry_offset),
                                    entry_size,
                                )
                                .to_vec()
                            });
                        }
                    }
                    MetricValue::Ring(values)
                }
                DirectoryType::Symlink | DirectoryType::Illegal | DirectoryType::Empty => {
                    return Err(StatsError::Protocol);
                }
            };
            let end_header =
                unsafe { ptr::read_volatile(self.mapping.as_ptr().cast::<SharedHeader>()) };
            end_header.validate_version()?;
            if !end_header.is_write_in_progress() && end_header.epoch() == epoch {
                return Ok(value);
            }
        }
        Err(StatsError::ClientRetryExhausted { operation: "read" })
    }
}
