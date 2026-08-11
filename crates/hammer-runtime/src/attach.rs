use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::RawFd;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use hammer_infra::segment::Segment;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

use crate::app::{
    AppSession, ApplicationId, SessionAcceptedMsg, SessionConnectedMsg, SessionControlPayload,
    SessionMsgQueue, SessionMsgQueueError, SessionOffsets, SessionProducer, SingleProducer,
};
use crate::{AttachError, RuntimeError, RuntimeResult};

mod application;
mod descriptor;

use application::{ApplicationAttachment, AttachedApplication};

pub use application::{
    ApplicationMqPublication, BASE_DESCRIPTOR_COUNT as APPLICATION_MQ_BASE_DESCRIPTOR_COUNT,
    EXT_CONFIG_CHUNK_BYTES, EXT_CONFIG_CHUNK_COUNT, ExtConfigStore,
    METADATA_BYTES as APPLICATION_MQ_METADATA_BYTES,
    METADATA_WORDS as APPLICATION_MQ_METADATA_WORDS,
};

pub const ATTACH_PROTOCOL_VERSION: u64 = 4;
pub const ATTACH_REQUEST_BYTES: usize = size_of::<u64>();
pub const ATTACH_REPLY_WORDS: usize = 3;
pub const ATTACH_REPLY_BYTES: usize = ATTACH_REPLY_WORDS * size_of::<u64>();
pub const ATTACH_STATUS_ACCEPTED: u64 = 0;
pub const ATTACH_STATUS_REJECTED: u64 = 1;
pub const ATTACH_DESCRIPTOR_COUNT: usize = 2;
pub const ATTACH_METADATA_WORDS: usize = 6;
pub const ATTACH_METADATA_BYTES: usize = ATTACH_METADATA_WORDS * size_of::<u64>();
pub const MAX_ATTACH_DESCRIPTORS: usize = 128;

#[derive(Clone)]
pub struct AppSessionPublication {
    session: Arc<AppSession>,
    application: ApplicationId,
    session_segment: Segment,
    offsets: SessionOffsets,
    connected: Option<SessionConnectedMsg>,
    accepted: Option<SessionAcceptedMsg>,
    descriptors_sent: bool,
}

impl AppSessionPublication {
    pub fn new(
        session: Arc<AppSession>,
        application: ApplicationId,
        session_segment: Segment,
        offsets: SessionOffsets,
    ) -> RuntimeResult<Self> {
        if session_segment.shared_fd().is_none() {
            return Err(AttachError::SegmentDescriptorMissing.into());
        }
        if session.evt_q().read_fd().is_none() {
            return Err(AttachError::SessionSignalMissing.into());
        }
        Ok(Self {
            session,
            application,
            session_segment,
            offsets,
            connected: None,
            accepted: None,
            descriptors_sent: false,
        })
    }

    /// Completes the Session with a CONNECTED control payload. At most one of
    /// `set_connected`/`set_accepted` is set; CONNECTED wins when both are.
    pub fn set_connected(&mut self, message: SessionConnectedMsg) {
        self.connected = Some(message);
    }

    /// Completes the Session with an ACCEPTED control payload. Fails when a
    /// message is already set: CONNECTED wins when both would apply, and an
    /// accepted publication must not silently replace either one.
    pub fn set_accepted(&mut self, message: SessionAcceptedMsg) -> Result<(), AttachError> {
        if self.connected.is_some() || self.accepted.is_some() {
            return Err(AttachError::AcceptedPublicationUnavailable);
        }
        self.accepted = Some(message);
        Ok(())
    }

    /// The retained ACCEPTED message, when the publication has not yet been
    /// delivered to an attached Application.
    pub fn accepted_message(&self) -> Option<SessionAcceptedMsg> {
        self.accepted.clone()
    }
}

enum AppPublication {
    Session(AppSessionPublication),
    ConnectFailed {
        application: ApplicationId,
        message: SessionConnectedMsg,
    },
}

struct AppSessionPublicationQueue {
    entries: ArrayQueue<AppPublication>,
    ready: Notify,
}

enum PublicationSendError {
    Retry,
    Fatal(RuntimeError),
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
            .push(AppPublication::Session(publication.clone()))
            .map_err(|_| AttachError::PublicationQueueFull)?;
        queue.ready.notify_one();
        Ok(())
    }

    /// Queues an active-connect failure message on the existing attach
    /// publication queue. No Session descriptors are sent.
    pub fn try_publish_connect_failure(
        &self,
        application: ApplicationId,
        reply: SessionConnectedMsg,
    ) -> RuntimeResult<()> {
        let queue = self
            .queue
            .upgrade()
            .ok_or(AttachError::PublicationQueueClosed)?;
        queue
            .entries
            .push(AppPublication::ConnectFailed {
                application,
                message: reply,
            })
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

    pub async fn serve<Attach, Mq, Control, Detached, ApplicationError>(
        self: Arc<Self>,
        attach_application: Attach,
        application_mq_publication: Mq,
        application_session_control: Control,
        application_detached: Detached,
    ) -> RuntimeResult<()>
    where
        Attach: Fn() -> Result<ApplicationId, ApplicationError>,
        Mq: Fn(ApplicationId) -> Result<ApplicationMqPublication, ApplicationError>,
        Control: Fn(
            ApplicationId,
            &mut SessionMsgQueue<SingleProducer>,
            &mut SessionProducer,
        ) -> RuntimeResult<()>,
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
        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(self.capacity);
        let mut clients = HashMap::<ApplicationId, AttachedApplication>::new();
        let mut publications = HashMap::<ApplicationId, VecDeque<AppPublication>>::new();
        let mut retry_tick = tokio::time::interval(Duration::from_millis(1));
        retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            while let Some(publication) = self.publications.entries.pop() {
                let application = match &publication {
                    AppPublication::Session(publication) => publication.application,
                    AppPublication::ConnectFailed { application, .. } => *application,
                };
                let Some(client) = clients.get_mut(&application) else {
                    publications
                        .entry(application)
                        .or_default()
                        .push_back(publication);
                    continue;
                };
                let stream = Arc::clone(&client.stream);
                let replies = &mut client.replies;
                let mut publication = publication;
                match send_publication(&stream, replies, &mut publication).await {
                    Ok(()) => {}
                    Err(PublicationSendError::Retry) => {
                        publications
                            .entry(application)
                            .or_default()
                            .push_front(publication);
                    }
                    Err(PublicationSendError::Fatal(error)) => {
                        tracing::warn!(%error, ?application, "failed to publish App Session");
                        if clients.remove(&application).is_some() {
                            publications.remove(&application);
                            application_detached(application);
                        }
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
                    // VPP rejects a duplicate attach with SESSION_E_APP_ATTACHED
                    // before allocating and the API handler replies with an error
                    // retval, leaving the first application attached
                    // (application.c:1138-1139, session_api.c:775-781). Mirror
                    // that: reject the duplicate client and keep the live client,
                    // so no `application_detached` for the colliding identity.
                    if clients.contains_key(&application) {
                        tracing::warn!(
                            ?application,
                            "Application attach reused an identity held by a live client"
                        );
                        if let Err(error) = send_attach_reply(
                            &mut client,
                            ATTACH_STATUS_REJECTED,
                            None,
                        ).await {
                            tracing::warn!(%error, "failed to reject Application attach");
                        }
                        continue;
                    }
                    let application_mqs = match application_mq_publication(application) {
                        Ok(application_mqs) => application_mqs,
                        Err(error) => {
                            tracing::warn!(%error, ?application, "Application MQ resources were not ready");
                            if let Err(error) = send_attach_reply(
                                &mut client,
                                ATTACH_STATUS_REJECTED,
                                None,
                            ).await {
                                tracing::warn!(%error, "failed to reject Application attach");
                            }
                            application_detached(application);
                            continue;
                        }
                    };
                    let attachment = match ApplicationAttachment::create(application, application_mqs) {
                        Ok(attachment) => attachment,
                        Err(error) => {
                            tracing::error!(%error, ?application, "failed to create Application Session MQ resources");
                            if let Err(error) = send_attach_reply(
                                &mut client,
                                ATTACH_STATUS_REJECTED,
                                None,
                            ).await {
                                tracing::warn!(%error, "failed to reject Application attach");
                            }
                            application_detached(application);
                            continue;
                        }
                    };
                    let signal = match attachment.request_signal() {
                        Ok(signal) => signal,
                        Err(error) => {
                            tracing::error!(%error, ?application, "failed to observe Application Session MQ");
                            if let Err(error) = send_attach_reply(
                                &mut client,
                                ATTACH_STATUS_REJECTED,
                                None,
                            ).await {
                                tracing::warn!(%error, "failed to reject Application attach");
                            }
                            application_detached(application);
                            continue;
                        }
                    };
                    if let Err(error) = send_attach_reply(
                        &mut client,
                        ATTACH_STATUS_ACCEPTED,
                        Some(application),
                    ).await {
                        tracing::warn!(%error, ?application, "failed to complete Application attach");
                        application_detached(application);
                        continue;
                    }
                    if let Err(error) = attachment.publish(&client).await {
                        tracing::error!(%error, ?application, "failed to publish Application Session MQ resources");
                        application_detached(application);
                        continue;
                    }
                    let client = Arc::new(client);
                    clients.insert(
                        application,
                        AttachedApplication {
                            stream: Arc::clone(&client),
                            requests: attachment.requests,
                            replies: attachment.replies,
                        },
                    );
                    let control_ready = control_tx.clone();
                    let control_detached = detached_tx.clone();
                    tokio::spawn(async move {
                        if application::monitor(application, signal, control_ready)
                            .await
                            .is_err()
                        {
                            let _ = control_detached.send(application).await;
                        }
                    });
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
                        let Some(replies) = clients
                            .get_mut(&application)
                            .map(|client| &mut client.replies)
                        else {
                            publications.insert(application, pending);
                            continue;
                        };
                        match flush_publications(&client, replies, &mut pending).await {
                            Ok(()) | Err(PublicationSendError::Retry) => {
                                if !pending.is_empty() {
                                    publications.insert(application, pending);
                                }
                            }
                            Err(PublicationSendError::Fatal(error)) => {
                                tracing::warn!(%error, ?application, "failed to publish App Session");
                                if clients.remove(&application).is_some() {
                                    application_detached(application);
                                }
                            }
                        }
                    }
                }
                Some(application) = control_rx.recv() => {
                    let Some(client) = clients.get_mut(&application) else {
                        continue;
                    };
                    if let Err(error) = application_session_control(
                        application,
                        &mut client.requests,
                        &mut client.replies,
                    ) {
                        tracing::error!(%error, ?application, "Application Session MQ dispatch failed");
                        if clients.remove(&application).is_some() {
                            publications.remove(&application);
                            application_detached(application);
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
                _ = retry_tick.tick(), if publications.iter().any(|(application, pending)| {
                    !pending.is_empty() && clients.contains_key(application)
                }) => {
                    let pending_applications = publications
                        .keys()
                        .copied()
                        .filter(|application| clients.contains_key(application))
                        .collect::<Vec<_>>();
                    for application in pending_applications {
                        let Some(mut pending) = publications.remove(&application) else {
                            continue;
                        };
                        let Some(client) = clients
                            .get(&application)
                            .map(|client| Arc::clone(&client.stream))
                        else {
                            publications.insert(application, pending);
                            continue;
                        };
                        let Some(replies) = clients
                            .get_mut(&application)
                            .map(|client| &mut client.replies)
                        else {
                            publications.insert(application, pending);
                            continue;
                        };
                        match flush_publications(&client, replies, &mut pending).await {
                            Ok(()) | Err(PublicationSendError::Retry) => {
                                if !pending.is_empty() {
                                    publications.insert(application, pending);
                                }
                            }
                            Err(PublicationSendError::Fatal(error)) => {
                                tracing::warn!(%error, ?application, "failed to retry App Session publication");
                                if clients.remove(&application).is_some() {
                                    application_detached(application);
                                }
                            }
                        }
                    }
                }
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
    replies: &mut SessionProducer,
    current: &mut AppPublication,
) -> Result<(), PublicationSendError> {
    match current {
        AppPublication::ConnectFailed { message, .. } => {
            enqueue_publication_control(replies, message)
        }
        AppPublication::Session(current) => {
            if !current.descriptors_sent {
                let descriptors: [RawFd; ATTACH_DESCRIPTOR_COUNT] = [
                    current
                        .session_segment
                        .shared_fd()
                        .ok_or(AttachError::SegmentDescriptorMissing)
                        .map_err(|error| PublicationSendError::Fatal(error.into()))?,
                    current
                        .session
                        .evt_q()
                        .read_fd()
                        .ok_or(AttachError::SessionSignalMissing)
                        .map_err(|error| PublicationSendError::Fatal(error.into()))?,
                ];
                let words = [
                    ATTACH_PROTOCOL_VERSION,
                    current.session.session_handle().raw(),
                    current.session_segment.size() as u64,
                    current.offsets.rx_fifo_off,
                    current.offsets.tx_fifo_off,
                    current.offsets.evt_q_off,
                ];
                let mut metadata = [0_u8; ATTACH_METADATA_BYTES];
                for (chunk, word) in metadata.chunks_exact_mut(size_of::<u64>()).zip(words) {
                    chunk.copy_from_slice(&word.to_le_bytes());
                }

                descriptor::send(client, &metadata, &descriptors)
                    .await
                    .map_err(PublicationSendError::Fatal)?;
                current.descriptors_sent = true;
            }
            match (&current.connected, &current.accepted) {
                (Some(connected), _) => enqueue_publication_control(replies, connected),
                (None, Some(accepted)) => enqueue_publication_control(replies, accepted),
                (None, None) => Ok(()),
            }
        }
    }
}

async fn flush_publications(
    client: &tokio::net::UnixStream,
    replies: &mut SessionProducer,
    pending: &mut VecDeque<AppPublication>,
) -> Result<(), PublicationSendError> {
    while let Some(mut publication) = pending.pop_front() {
        match send_publication(client, replies, &mut publication).await {
            Ok(()) => {}
            Err(PublicationSendError::Retry) => {
                pending.push_front(publication);
                return Err(PublicationSendError::Retry);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn enqueue_publication_control<M: SessionControlPayload>(
    replies: &mut SessionProducer,
    message: &M,
) -> Result<(), PublicationSendError> {
    match replies.enqueue_control(message) {
        Ok(()) => Ok(()),
        Err(SessionMsgQueueError::ControlFull) => Err(PublicationSendError::Retry),
        Err(error) => Err(PublicationSendError::Fatal(
            AttachError::SessionControl { source: error }.into(),
        )),
    }
}
