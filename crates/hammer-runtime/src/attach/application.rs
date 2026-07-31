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

use super::{ATTACH_PROTOCOL_VERSION, descriptor};

pub const DESCRIPTOR_COUNT: usize = 3;
pub const METADATA_WORDS: usize = 4;
pub const METADATA_BYTES: usize = METADATA_WORDS * size_of::<u64>();

const Q_NITEMS: u32 = 64;
const RING_NITEMS: u32 = 32;
const SEGMENT_BYTES: usize = 1024 * 1024;

static SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
}

impl ApplicationAttachment {
    pub(super) fn create(application: ApplicationId) -> RuntimeResult<Self> {
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
        })
    }

    pub(super) async fn publish(&self, client: &tokio::net::UnixStream) -> RuntimeResult<()> {
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
        ];
        let words = [
            ATTACH_PROTOCOL_VERSION,
            self.segment.size() as u64,
            self.request_offset,
            self.reply_offset,
        ];
        let mut metadata = [0_u8; METADATA_BYTES];
        for (chunk, word) in metadata.chunks_exact_mut(size_of::<u64>()).zip(words) {
            chunk.copy_from_slice(&word.to_le_bytes());
        }
        descriptor::send(client, &metadata, &descriptors).await
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
