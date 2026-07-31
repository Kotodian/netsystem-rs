use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Weak};

use crossbeam_queue::ArrayQueue;
use hammer_infra::segment::Segment;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::sync::Notify;

use crate::app::{AppSession, ApplicationId, SessionEventQueue, SessionOffsets};
use crate::{AttachError, RuntimeResult};

pub const ATTACH_PROTOCOL_VERSION: u64 = 1;
pub const ATTACH_REQUEST_BYTES: usize = size_of::<u64>();
pub const ATTACH_REPLY_WORDS: usize = 3;
pub const ATTACH_REPLY_BYTES: usize = ATTACH_REPLY_WORDS * size_of::<u64>();
pub const ATTACH_STATUS_ACCEPTED: u64 = 0;
pub const ATTACH_STATUS_REJECTED: u64 = 1;
pub const ATTACH_DESCRIPTOR_COUNT: usize = 4;
pub const ATTACH_METADATA_WORDS: usize = 8;
pub const ATTACH_METADATA_BYTES: usize = ATTACH_METADATA_WORDS * size_of::<u64>();

#[derive(Clone)]
pub struct AppSessionPublication {
    session: Arc<AppSession>,
    application: ApplicationId,
    session_segment: Segment,
    tx_event_segment: Segment,
    offsets: SessionOffsets,
}

impl AppSessionPublication {
    pub fn new(
        session: Arc<AppSession>,
        application: ApplicationId,
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
            application,
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

    pub async fn serve<Attach, Detached, ApplicationError>(
        self: Arc<Self>,
        attach_application: Attach,
        application_detached: Detached,
    ) -> RuntimeResult<()>
    where
        Attach: Fn() -> Result<ApplicationId, ApplicationError>,
        Detached: Fn(ApplicationId),
        ApplicationError: std::fmt::Display,
    {
        let listener = self
            .listener
            .try_clone()
            .map_err(|source| AttachError::ListenerRegistration { source })?;
        let listener = tokio::net::UnixListener::from_std(listener)
            .map_err(|source| AttachError::ListenerRegistration { source })?;
        let (registration_tx, mut registration_rx) =
            tokio::sync::mpsc::channel::<RuntimeResult<tokio::net::UnixStream>>(self.capacity);
        let (detached_tx, mut detached_rx) = tokio::sync::mpsc::channel(self.capacity);
        let mut clients = HashMap::<ApplicationId, Arc<tokio::net::UnixStream>>::new();
        let mut publications = HashMap::<ApplicationId, VecDeque<AppSessionPublication>>::new();

        loop {
            while let Some(publication) = self.publications.entries.pop() {
                let application = publication.application;
                match clients.get(&application).cloned() {
                    Some(client) => {
                        if let Err(error) = send_publication(&client, &publication).await {
                            tracing::warn!(%error, ?application, "failed to publish App Session");
                            if clients.remove(&application).is_some() {
                                publications.remove(&application);
                                application_detached(application);
                            }
                        }
                    }
                    None => {
                        publications
                            .entry(application)
                            .or_default()
                            .push_back(publication);
                    }
                }
            }

            tokio::select! {
                accepted = listener.accept() => {
                    let (mut client, _) = accepted.map_err(|source| AttachError::Accept { source })?;
                    let registration_tx = registration_tx.clone();
                    tokio::spawn(async move {
                        let mut request = [0_u8; ATTACH_REQUEST_BYTES];
                        let result = match client.read_exact(&mut request).await {
                            Ok(_) => {
                                let version = u64::from_le_bytes(request);
                                if version != ATTACH_PROTOCOL_VERSION {
                                    Err(AttachError::Accept {
                                        source: io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            format!("unsupported attach protocol version {version}"),
                                        ),
                                    }
                                    .into())
                                } else {
                                    Ok(client)
                                }
                            }
                            Err(source) => Err(AttachError::Accept { source }.into()),
                        };
                        let _ = registration_tx.send(result).await;
                    });
                }
                Some(registration) = registration_rx.recv() => {
                    let Ok(mut client) = registration else {
                        continue;
                    };
                    if clients.len() >= self.capacity {
                        if let Err(error) = send_attach_reply(
                            &mut client,
                            ATTACH_STATUS_REJECTED,
                            None,
                        ).await {
                            tracing::warn!(%error, "failed to reject Application attach");
                        }
                        continue;
                    }
                    let application = match attach_application() {
                        Ok(application) => application,
                        Err(error) => {
                            tracing::warn!(%error, "Application attach was rejected");
                            if let Err(error) = send_attach_reply(
                                &mut client,
                                ATTACH_STATUS_REJECTED,
                                None,
                            ).await {
                                tracing::warn!(%error, "failed to reject Application attach");
                            }
                            continue;
                        }
                    };
                    assert!(
                        !clients.contains_key(&application),
                        "Application attach allocated an identity already held by a live client"
                    );
                    if let Err(error) = send_attach_reply(
                        &mut client,
                        ATTACH_STATUS_ACCEPTED,
                        Some(application),
                    ).await {
                        tracing::warn!(%error, ?application, "failed to complete Application attach");
                        application_detached(application);
                        continue;
                    }
                    let client = Arc::new(client);
                    clients.insert(application, Arc::clone(&client));
                    let detached_tx = detached_tx.clone();
                    let monitor = Arc::clone(&client);
                    tokio::spawn(async move {
                        let mut byte = [0_u8; 1];
                        loop {
                            if monitor.readable().await.is_err() {
                                break;
                            }
                            match monitor.try_read(&mut byte) {
                                Ok(0) | Ok(_) => break,
                                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
                                Err(_) => break,
                            }
                        }
                        let _ = detached_tx.send(application).await;
                    });
                    if let Some(mut pending) = publications.remove(&application) {
                        while let Some(publication) = pending.pop_front() {
                            if let Err(error) = send_publication(&client, &publication).await {
                                tracing::warn!(%error, ?application, "failed to publish App Session");
                                if clients.remove(&application).is_some() {
                                    application_detached(application);
                                }
                                break;
                            }
                        }
                    }
                }
                Some(application) = detached_rx.recv() => {
                    if clients.remove(&application).is_some() {
                        publications.remove(&application);
                        application_detached(application);
                    }
                }
                () = self.publications.ready.notified() => {}
            }
        }
    }
}

async fn send_attach_reply(
    client: &mut tokio::net::UnixStream,
    status: u64,
    application: Option<ApplicationId>,
) -> RuntimeResult<()> {
    let words = [
        ATTACH_PROTOCOL_VERSION,
        status,
        application.map_or(0, ApplicationId::raw),
    ];
    let mut reply = [0_u8; ATTACH_REPLY_BYTES];
    for (chunk, word) in reply.chunks_exact_mut(size_of::<u64>()).zip(words) {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    client
        .write_all(&reply)
        .await
        .map_err(|source| AttachError::Send { source }.into())
}

async fn send_publication(
    client: &tokio::net::UnixStream,
    current: &AppSessionPublication,
) -> RuntimeResult<()> {
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
                    return Err(io::Error::other("attach control header is missing"));
                }
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len =
                    libc::CMSG_LEN(std::mem::size_of_val(&descriptors) as u32) as _;
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
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => return Err(AttachError::Send { source }.into()),
        }
    }
}
