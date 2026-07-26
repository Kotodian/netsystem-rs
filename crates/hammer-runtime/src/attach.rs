use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Weak};

use crossbeam_queue::ArrayQueue;
use hammer_infra::segment::Segment;
use tokio::io::Interest;
use tokio::sync::Notify;

use crate::app::{AppSession, SessionEventQueue, SessionOffsets};
use crate::{AttachError, RuntimeResult};

pub const ATTACH_PROTOCOL_VERSION: u64 = 1;
pub const ATTACH_DESCRIPTOR_COUNT: usize = 4;
pub const ATTACH_METADATA_WORDS: usize = 8;
pub const ATTACH_METADATA_BYTES: usize = ATTACH_METADATA_WORDS * size_of::<u64>();

#[derive(Clone)]
pub struct AppSessionPublication {
    session: Arc<AppSession>,
    session_segment: Segment,
    tx_event_segment: Segment,
    offsets: SessionOffsets,
}

impl AppSessionPublication {
    pub fn new(
        session: Arc<AppSession>,
        session_segment: Segment,
        tx_event_segment: Segment,
        offsets: SessionOffsets,
    ) -> RuntimeResult<Self> {
        if session_segment.shared_fd().is_none() || tx_event_segment.shared_fd().is_none() {
            return Err(AttachError::SegmentDescriptorMissing.into());
        }
        if session.evt_q().read_fd().is_none() {
            return Err(AttachError::SessionSignalMissing.into());
        }
        if session.tx_evt_q().write_fd().is_none() {
            return Err(AttachError::TxEventSignalMissing.into());
        }
        Ok(Self {
            session,
            session_segment,
            tx_event_segment,
            offsets,
        })
    }
}

struct AppSessionPublicationQueue {
    entries: ArrayQueue<AppSessionPublication>,
    ready: Notify,
}

#[derive(Clone)]
pub struct AppSessionPublisher {
    queue: Weak<AppSessionPublicationQueue>,
}

impl AppSessionPublisher {
    pub fn try_publish(&self, publication: &AppSessionPublication) -> RuntimeResult<()> {
        let queue = self
            .queue
            .upgrade()
            .ok_or(AttachError::PublicationQueueClosed)?;
        queue
            .entries
            .push(publication.clone())
            .map_err(|_| AttachError::PublicationQueueFull)?;
        queue.ready.notify_one();
        Ok(())
    }
}

pub struct AppServer {
    listener: std::os::unix::net::UnixListener,
    publications: Arc<AppSessionPublicationQueue>,
    capacity: usize,
}

impl AppServer {
    pub fn bind(path: &str, capacity: usize) -> RuntimeResult<Self> {
        if capacity == 0 {
            return Err(AttachError::PublicationCapacityInvalid.into());
        }
        let _ = std::fs::remove_file(path);
        let listener =
            std::os::unix::net::UnixListener::bind(path).map_err(|source| AttachError::Bind {
                path: path.into(),
                source,
            })?;
        listener
            .set_nonblocking(true)
            .map_err(|source| AttachError::ListenerNonblocking { source })?;
        Ok(Self {
            listener,
            publications: Arc::new(AppSessionPublicationQueue {
                entries: ArrayQueue::new(capacity),
                ready: Notify::new(),
            }),
            capacity,
        })
    }

    #[inline]
    pub fn publisher(&self) -> AppSessionPublisher {
        AppSessionPublisher {
            queue: Arc::downgrade(&self.publications),
        }
    }

    pub async fn serve(self: Arc<Self>) -> RuntimeResult<()> {
        let listener = self
            .listener
            .try_clone()
            .map_err(|source| AttachError::ListenerRegistration { source })?;
        let listener = tokio::net::UnixListener::from_std(listener)
            .map_err(|source| AttachError::ListenerRegistration { source })?;
        let mut clients: VecDeque<tokio::net::UnixStream> =
            VecDeque::with_capacity(self.capacity);
        let mut publication: Option<AppSessionPublication> = None;

        loop {
            if publication.is_some() && !clients.is_empty() {
                let (Some(client), Some(current)) = (clients.pop_front(), publication.take())
                else {
                    continue;
                };
                let attach_result: RuntimeResult<()> = async {
                    let descriptors: [RawFd; ATTACH_DESCRIPTOR_COUNT] = [
                        current
                            .session_segment
                            .shared_fd()
                            .ok_or(AttachError::SegmentDescriptorMissing)?,
                        current
                            .tx_event_segment
                            .shared_fd()
                            .ok_or(AttachError::SegmentDescriptorMissing)?,
                        current
                            .session
                            .evt_q()
                            .read_fd()
                            .ok_or(AttachError::SessionSignalMissing)?,
                        current
                            .session
                            .tx_evt_q()
                            .write_fd()
                            .ok_or(AttachError::TxEventSignalMissing)?,
                    ];
                    let words = [
                        ATTACH_PROTOCOL_VERSION,
                        current.session.session_handle().raw(),
                        current.session_segment.size() as u64,
                        current.tx_event_segment.size() as u64,
                        current.offsets.rx_fifo_off,
                        current.offsets.tx_fifo_off,
                        current.offsets.evt_q_off,
                        current.offsets.tx_evt_q_off,
                    ];
                    let mut metadata = [0_u8; ATTACH_METADATA_BYTES];
                    for (chunk, word) in metadata.chunks_exact_mut(size_of::<u64>()).zip(words) {
                        chunk.copy_from_slice(&word.to_le_bytes());
                    }

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
                                    return Err(io::Error::other(
                                        "attach control header is missing",
                                    ));
                                }
                                (*header).cmsg_level = libc::SOL_SOCKET;
                                (*header).cmsg_type = libc::SCM_RIGHTS;
                                (*header).cmsg_len = libc::CMSG_LEN(
                                    std::mem::size_of_val(&descriptors) as u32,
                                ) as _;
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
                            Ok(sent) if sent == metadata.len() => break,
                            Ok(sent) => {
                                return Err(AttachError::Send {
                                    source: io::Error::new(
                                        io::ErrorKind::WriteZero,
                                        format!(
                                            "attach metadata write was partial: expected {}, sent {sent}",
                                            metadata.len()
                                        ),
                                    ),
                                }
                                .into());
                            }
                            Err(source) if source.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(source) => return Err(AttachError::Send { source }.into()),
                        }
                    }
                    Ok(())
                }
                .await;

                if let Err(error) = attach_result {
                    tracing::warn!(%error, "failed to attach application client");
                    publication = Some(current);
                }
                continue;
            }

            if publication.is_none() {
                publication = self.publications.entries.pop();
                if publication.is_some() {
                    continue;
                }
            }

            tokio::select! {
                accepted = listener.accept(), if clients.len() < self.capacity => {
                    let (client, _) = accepted.map_err(|source| AttachError::Accept { source })?;
                    clients.push_back(client);
                }
                () = self.publications.ready.notified(), if publication.is_none() => {}
            }
        }
    }
}
