use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_infra::segment::Segment;
use tokio::io::unix::AsyncFd;

use crate::app::{
    APPLICATION_SESSION_CONTROL_BYTES, ApplicationId, SessionEventQueue, SessionMsgQueue,
};
use crate::{AttachError, RuntimeResult};

use super::{ATTACH_PROTOCOL_VERSION, MAX_ATTACH_DESCRIPTORS, descriptor};

pub const BASE_DESCRIPTOR_COUNT: usize = 4;
pub const METADATA_WORDS: usize = 6;
pub const METADATA_BYTES: usize = METADATA_WORDS * size_of::<u64>();

const Q_NITEMS: u32 = 64;
const RING_NITEMS: u32 = 32;
const SEGMENT_BYTES: usize = 1024 * 1024;

static SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Runtime-neutral view of the per-Application Rx MQ resources published by
/// the daemon during attach.
#[derive(Clone)]
pub struct ApplicationMqPublication {
    segment: Segment,
    queues: Box<[Arc<SessionMsgQueue>]>,
    offsets: Box<[u64]>,
}

impl ApplicationMqPublication {
    pub fn new(
        segment: Segment,
        queues: Box<[Arc<SessionMsgQueue>]>,
        offsets: Box<[u64]>,
    ) -> Result<Self, AttachError> {
        if segment.shared_fd().is_none() {
            return Err(AttachError::ApplicationMqSegmentMissing);
        }
        if queues.is_empty() {
            return Err(AttachError::ApplicationMqWorkerCountZero);
        }
        if queues.len() != offsets.len() {
            return Err(AttachError::ApplicationMqQueueCountMismatch {
                queues: queues.len(),
                offsets: offsets.len(),
            });
        }
        let worker_descriptor_count = queues
            .len()
            .checked_add(BASE_DESCRIPTOR_COUNT)
            .ok_or(AttachError::ApplicationMqDescriptorCountOverflow)?;
        if worker_descriptor_count > MAX_ATTACH_DESCRIPTORS {
            return Err(AttachError::ApplicationMqDescriptorCountTooLarge {
                actual: worker_descriptor_count,
                max: MAX_ATTACH_DESCRIPTORS,
            });
        }
        let segment_size = segment.size() as u64;
        for (worker, (queue, offset)) in queues.iter().zip(&offsets).enumerate() {
            if *offset >= segment_size {
                return Err(AttachError::ApplicationMqOffsetOutOfRange {
                    worker,
                    offset: *offset,
                    segment_size,
                });
            }
            if queue.write_fd().is_none() {
                return Err(AttachError::ApplicationMqWriteSignalMissing { worker });
            }
        }
        Ok(Self {
            segment,
            queues,
            offsets,
        })
    }

    #[inline]
    fn worker_count(&self) -> usize {
        self.queues.len()
    }
}

pub(super) struct AttachedApplication {
    pub(super) stream: Arc<tokio::net::UnixStream>,
    pub(super) requests: Arc<SessionMsgQueue>,
    pub(super) replies: Arc<SessionMsgQueue>,
}

pub(super) struct ApplicationAttachment {
    segment: Segment,
    request_offset: u64,
    pub(super) requests: Arc<SessionMsgQueue>,
    reply_offset: u64,
    pub(super) replies: Arc<SessionMsgQueue>,
    application_mqs: ApplicationMqPublication,
}

impl ApplicationAttachment {
    pub(super) fn create(
        application: ApplicationId,
        application_mqs: ApplicationMqPublication,
    ) -> RuntimeResult<Self> {
        let sequence = SEGMENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let segment = Segment::shared(
            &format!(
                "hammer-app-mq-{}-{}-{sequence}",
                std::process::id(),
                application.raw()
            ),
            SEGMENT_BYTES,
        )
        .map_err(|source| AttachError::ControlSegmentCreate { source })?;
        let queue_bytes = SessionMsgQueue::layout_bytes_with_ctrl_element(
            Q_NITEMS,
            RING_NITEMS,
            APPLICATION_SESSION_CONTROL_BYTES,
        )
        .map_err(|source| AttachError::ControlQueueLayout { source })?;
        let request_offset = segment
            .alloc(queue_bytes, 64)
            .ok_or(AttachError::ControlSegmentCapacity)?;
        let reply_offset = segment
            .alloc(queue_bytes, 64)
            .ok_or(AttachError::ControlSegmentCapacity)?;
        let requests = Arc::new(
            unsafe {
                SessionMsgQueue::init_at_with_signal_and_ctrl_element(
                    segment.clone(),
                    request_offset,
                    Q_NITEMS,
                    RING_NITEMS,
                    APPLICATION_SESSION_CONTROL_BYTES,
                )
            }
            .map_err(|source| AttachError::ControlQueueInit { source })?,
        );
        let replies = Arc::new(
            unsafe {
                SessionMsgQueue::init_at_with_signal_and_ctrl_element(
                    segment.clone(),
                    reply_offset,
                    Q_NITEMS,
                    RING_NITEMS,
                    APPLICATION_SESSION_CONTROL_BYTES,
                )
            }
            .map_err(|source| AttachError::ControlQueueInit { source })?,
        );
        Ok(Self {
            segment,
            request_offset,
            requests,
            reply_offset,
            replies,
            application_mqs,
        })
    }

    pub(super) async fn publish(&self, client: &tokio::net::UnixStream) -> RuntimeResult<()> {
        let application_mqs = &self.application_mqs;
        let descriptors = [
            self.segment
                .shared_fd()
                .ok_or(AttachError::SegmentDescriptorMissing)?,
            self.requests
                .write_fd()
                .ok_or(AttachError::ControlSignalMissing)?,
            self.replies
                .read_fd()
                .ok_or(AttachError::ControlSignalMissing)?,
            application_mqs
                .segment
                .shared_fd()
                .ok_or(AttachError::ApplicationMqSegmentMissing)?,
        ];
        let mut payload =
            Vec::with_capacity(METADATA_BYTES + application_mqs.worker_count() * size_of::<u64>());
        let words = [
            ATTACH_PROTOCOL_VERSION,
            self.segment.size() as u64,
            self.request_offset,
            self.reply_offset,
            application_mqs.segment.size() as u64,
            application_mqs.worker_count() as u64,
        ];
        let mut metadata = [0_u8; METADATA_BYTES];
        for (chunk, word) in metadata.chunks_exact_mut(size_of::<u64>()).zip(words) {
            chunk.copy_from_slice(&word.to_le_bytes());
        }
        payload.extend_from_slice(&metadata);
        for offset in &application_mqs.offsets {
            payload.extend_from_slice(&offset.to_le_bytes());
        }
        let mut descriptors = descriptors.to_vec();
        for (worker, queue) in application_mqs.queues.iter().enumerate() {
            descriptors.push(
                queue
                    .write_fd()
                    .ok_or(AttachError::ApplicationMqWriteSignalMissing { worker })?,
            );
        }
        descriptor::send(client, &payload, &descriptors).await
    }

    pub(super) fn request_signal(&self) -> RuntimeResult<OwnedFd> {
        let descriptor = self
            .requests
            .read_fd()
            .ok_or(AttachError::ControlSignalMissing)?;
        descriptor::duplicate(descriptor)
    }
}

pub(super) async fn monitor(
    application: ApplicationId,
    signal: OwnedFd,
    ready: tokio::sync::mpsc::Sender<ApplicationId>,
) -> RuntimeResult<()> {
    let signal =
        AsyncFd::new(signal).map_err(|source| AttachError::ControlSignalRegistration { source })?;
    loop {
        let mut readiness = signal
            .readable()
            .await
            .map_err(|source| AttachError::ControlSignalRegistration { source })?;
        let read = readiness.try_io(|signal| {
            let mut notifications = [0_u8; 64];
            let bytes = unsafe {
                libc::read(
                    signal.get_ref().as_raw_fd(),
                    notifications.as_mut_ptr().cast::<libc::c_void>(),
                    notifications.len(),
                )
            };
            if bytes < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(bytes as usize)
            }
        });
        match read {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(_)) => {
                if ready.send(application).await.is_err() {
                    return Ok(());
                }
            }
            Ok(Err(source)) => return Err(AttachError::ControlSignalRead { source }.into()),
            Err(_) => {}
        }
    }
}
