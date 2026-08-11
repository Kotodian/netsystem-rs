use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_infra::segment::Segment;
use tokio::io::unix::AsyncFd;

use crate::app::{ApplicationId, SessionMsgQueue, SessionProducer, SingleProducer};
use crate::{AttachError, RuntimeResult};

use super::{ATTACH_PROTOCOL_VERSION, MAX_ATTACH_DESCRIPTORS, descriptor};

pub const BASE_DESCRIPTOR_COUNT: usize = 4;
pub const METADATA_WORDS: usize = 7;
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
    /// Offset of the bounded ext-config store region in the Rx MQ segment;
    /// 0 means the daemon published no ext-config storage (VPP uses 0 for
    /// `ext_config` none, application_interface.h).
    ext_config_offset: u64,
}

impl ApplicationMqPublication {
    pub fn new(
        segment: Segment,
        queues: Box<[Arc<SessionMsgQueue>]>,
        offsets: Box<[u64]>,
        ext_config_offset: u64,
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
        if ext_config_offset != 0
            && !(ext_config_offset < segment_size
                && ExtConfigStore::from_shared(segment.clone(), ext_config_offset as usize).is_ok())
        {
            return Err(AttachError::ExtConfigOffsetOutOfRange);
        }
        Ok(Self {
            segment,
            queues,
            offsets,
            ext_config_offset,
        })
    }

    /// Builds the bounded ext-config store published by the daemon, when the
    /// publication carries one. The store lives in the Rx MQ segment shared
    /// with the Application (VPP `session_mq_get_ext_config`,
    /// session_node.c:80-100).
    pub fn ext_config_store(&self) -> Result<Option<ExtConfigStore>, AttachError> {
        if self.ext_config_offset == 0 {
            return Ok(None);
        }
        ExtConfigStore::from_shared(self.segment.clone(), self.ext_config_offset as usize).map(Some)
    }

    #[inline]
    fn worker_count(&self) -> usize {
        self.queues.len()
    }
}

pub(super) struct AttachedApplication {
    pub(super) stream: Arc<tokio::net::UnixStream>,
    pub(super) requests: SessionMsgQueue<SingleProducer>,
    pub(super) replies: SessionProducer,
}

pub(super) struct ApplicationAttachment {
    segment: Segment,
    request_offset: u64,
    pub(super) requests: SessionMsgQueue<SingleProducer>,
    reply_offset: u64,
    pub(super) replies: SessionProducer,
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
        let queue_bytes =
            SessionMsgQueue::<SingleProducer>::layout_bytes_with_control(Q_NITEMS, RING_NITEMS)
                .map_err(|source| AttachError::ControlQueueLayout { source })?;
        let request_offset = segment
            .alloc(queue_bytes, 64)
            .ok_or(AttachError::ControlSegmentCapacity)?;
        let reply_offset = segment
            .alloc(queue_bytes, 64)
            .ok_or(AttachError::ControlSegmentCapacity)?;
        let requests = unsafe {
            SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(
                segment.clone(),
                request_offset,
                Q_NITEMS,
                RING_NITEMS,
            )
        }
        .map_err(|source| AttachError::ControlQueueInit { source })?;
        let replies = unsafe {
            SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(
                segment.clone(),
                reply_offset,
                Q_NITEMS,
                RING_NITEMS,
            )
        }
        .map_err(|source| AttachError::ControlQueueInit { source })?;
        // The daemon owns the reply producer capability: it is claimed here
        // once (typed error on a second claim) and outlives the mapping.
        let replies = replies
            .claim_producer()
            .map_err(|source| AttachError::ControlQueueInit { source })?;
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
            application_mqs.ext_config_offset,
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

/// Byte capacity of one ExtConfig chunk payload.
pub const EXT_CONFIG_CHUNK_BYTES: usize = 64;

/// Fixed chunk count of an ExtConfig store region (bounded ownership).
pub const EXT_CONFIG_CHUNK_COUNT: usize = 32;

const EXT_CONFIG_CHUNK_NEXT_BYTES: usize = size_of::<u64>();
const EXT_CONFIG_CHUNK_LEN_BYTES: usize = size_of::<u16>();
const EXT_CONFIG_CHUNK_HEADER_BYTES: usize =
    EXT_CONFIG_CHUNK_NEXT_BYTES + EXT_CONFIG_CHUNK_LEN_BYTES + 6;
const EXT_CONFIG_CHUNK_STRIDE: usize = EXT_CONFIG_CHUNK_HEADER_BYTES + EXT_CONFIG_CHUNK_BYTES;

/// Chunk `next` state for a chunk that has been allocated and not yet freed.
///
/// A free-list link (`0..EXT_CONFIG_CHUNK_COUNT` or `u64::MAX` end marker)
/// means the chunk is free; `EXT_CONFIG_CHUNK_ALLOCATED` means the chunk is
/// exclusively owned by its allocating session. `free` claims the chunk by
/// CAS-ing this marker to the current head before linking, so a chunk can
/// never be linked into the free list twice (double free is rejected and the
/// list keeps each chunk at most once, preventing aliased allocations).
const EXT_CONFIG_CHUNK_ALLOCATED: u64 = u64::MAX - 1;

/// Bounded fixed-size storage for variable QUIC/TLS Session control
/// configuration, owned by the attach Application control segment.
///
/// VPP keeps per-Session transport config in shared-segment ext-config
/// storage referenced by a uword offset (`session_mq_get_ext_config`,
/// session_node.c:80-100). Here each chunk is one fixed slot and a lock-free
/// Treiber free stack under an `AtomicU64` head keeps ownership explicit and
/// bounded without locks.
#[derive(Clone)]
pub struct ExtConfigStore {
    segment: Segment,
    base: usize,
}

impl ExtConfigStore {
    /// Region layout: one atomic free-stack head plus the fixed chunk slots.
    pub const fn layout_bytes() -> usize {
        size_of::<u64>() + EXT_CONFIG_CHUNK_COUNT * EXT_CONFIG_CHUNK_STRIDE
    }

    /// Initializes the store at a fresh, zeroed region of `segment`.
    ///
    /// # Safety
    ///
    /// `offset` must be an aligned allocation within `segment` at least
    /// [`ExtConfigStore::layout_bytes`] large, with no live overlapping store
    /// or live chunk references.
    pub unsafe fn init_at(segment: Segment, offset: usize) -> Self {
        let store = Self {
            segment,
            base: offset,
        };
        let head: &AtomicU64 =
            unsafe { &*(store.segment.base().add(store.base).cast::<AtomicU64>()) };
        // The free stack starts full: the head points at chunk 0 and the
        // last chunk's next is the u64::MAX end marker.
        head.store(0, Ordering::Release);
        for chunk in 0..EXT_CONFIG_CHUNK_COUNT {
            let next = if chunk + 1 < EXT_CONFIG_CHUNK_COUNT {
                chunk as u64 + 1
            } else {
                u64::MAX
            };
            unsafe { store.chunk_next(chunk) }.store(next, Ordering::Relaxed);
        }
        store
    }

    /// Absolute segment offset of the store region.
    pub const fn offset(&self) -> usize {
        self.base
    }

    /// Binds to a store region initialized by the daemon in a shared segment.
    ///
    /// Unlike [`ExtConfigStore::init_at`], this is safe: it verifies that the
    /// region at `offset` lies within `segment` and is large enough for the
    /// whole store layout, so a malformed published offset cannot alias other
    /// segment state.
    pub fn from_shared(segment: Segment, offset: usize) -> Result<Self, AttachError> {
        let end = offset
            .checked_add(Self::layout_bytes())
            .ok_or(AttachError::ExtConfigOffsetOutOfRange)?;
        if end > segment.size() {
            return Err(AttachError::ExtConfigOffsetOutOfRange);
        }
        Ok(Self {
            segment,
            base: offset,
        })
    }

    /// Allocates one chunk, copying `data` (≤ EXT_CONFIG_CHUNK_BYTES) into it.
    ///
    /// Returns the absolute segment offset of the chunk; `u64::MAX` is not a
    /// valid store offset.
    pub fn alloc(&self, data: &[u8]) -> Result<u64, AttachError> {
        if data.len() > EXT_CONFIG_CHUNK_BYTES {
            return Err(AttachError::ExtConfigOversized {
                requested: data.len(),
                max: EXT_CONFIG_CHUNK_BYTES,
            });
        }
        let head: &AtomicU64 =
            unsafe { &*(self.segment.base().add(self.base).cast::<AtomicU64>()) };
        loop {
            let chunk = head.load(Ordering::Acquire);
            if chunk == u64::MAX {
                return Err(AttachError::ExtConfigExhausted);
            }
            let next = unsafe { self.chunk_next(chunk as usize) }.load(Ordering::Relaxed);
            if head
                .compare_exchange_weak(chunk, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let base = self.chunk_base(chunk as usize);
                let ptr = unsafe { self.segment.base().add(base) };
                unsafe {
                    ptr.add(EXT_CONFIG_CHUNK_NEXT_BYTES)
                        .cast::<u16>()
                        .write_unaligned(data.len() as u16);
                    ptr.add(EXT_CONFIG_CHUNK_HEADER_BYTES)
                        .copy_from_nonoverlapping(data.as_ptr(), data.len());
                }
                // Publish the chunk as allocated after its payload is visible;
                // readers observe the marker (Acquire) before the payload.
                unsafe { self.chunk_next(chunk as usize) }
                    .store(EXT_CONFIG_CHUNK_ALLOCATED, Ordering::Release);
                return Ok(base as u64);
            }
        }
    }

    /// Reads the data of the chunk at `offset`. Fails when the offset is out
    /// of range or the chunk is not allocated (freed, double-freed, or a
    /// stale reference).
    pub fn read(&self, offset: u64) -> Result<&[u8], AttachError> {
        let chunk = self
            .chunk_of(offset)
            .ok_or(AttachError::ExtConfigOffsetOutOfRange)?;
        let ptr = unsafe { self.segment.base().add(self.chunk_base(chunk)) };
        if unsafe { self.chunk_next(chunk) }.load(Ordering::Acquire) != EXT_CONFIG_CHUNK_ALLOCATED {
            return Err(AttachError::ExtConfigNotAllocated);
        }
        let len = unsafe {
            ptr.add(EXT_CONFIG_CHUNK_NEXT_BYTES)
                .cast::<u16>()
                .read_unaligned()
        } as usize;
        if len > EXT_CONFIG_CHUNK_BYTES {
            // The write path caps the length; an overlarge length means the
            // chunk is not in a readable allocated state.
            return Err(AttachError::ExtConfigNotAllocated);
        }
        Ok(unsafe { std::slice::from_raw_parts(ptr.add(EXT_CONFIG_CHUNK_HEADER_BYTES), len) })
    }

    /// Returns the chunk at `offset` to the free stack.
    ///
    /// The chunk is claimed by CAS-ing its allocated marker to the current
    /// head first, so a free of a chunk that is not allocated (double free or
    /// stale offset) is rejected instead of linking the chunk twice.
    pub fn free(&self, offset: u64) -> Result<(), AttachError> {
        let chunk = self
            .chunk_of(offset)
            .ok_or(AttachError::ExtConfigOffsetOutOfRange)?;
        let head: &AtomicU64 =
            unsafe { &*(self.segment.base().add(self.base).cast::<AtomicU64>()) };
        let next = unsafe { self.chunk_next(chunk) };
        let mut current = head.load(Ordering::Acquire);
        if next
            .compare_exchange(
                EXT_CONFIG_CHUNK_ALLOCATED,
                current,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(AttachError::ExtConfigNotAllocated);
        }
        // The chunk is now exclusively owned by this free; only the head
        // link remains, so a failed head CAS is safe to retry by re-writing
        // the chunk's next.
        loop {
            if head
                .compare_exchange_weak(current, chunk as u64, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            current = head.load(Ordering::Acquire);
            next.store(current, Ordering::Relaxed);
        }
    }

    #[inline]
    fn chunk_base(&self, chunk: usize) -> usize {
        self.base + size_of::<u64>() + chunk * EXT_CONFIG_CHUNK_STRIDE
    }

    /// # Safety
    ///
    /// `chunk` must be < EXT_CONFIG_CHUNK_COUNT within this store.
    #[inline]
    unsafe fn chunk_next(&self, chunk: usize) -> &AtomicU64 {
        unsafe {
            &*(self
                .segment
                .base()
                .add(self.chunk_base(chunk))
                .cast::<AtomicU64>())
        }
    }

    /// Maps a stored offset back to its chunk index (rejects out-of-range and
    /// misaligned offsets; `u64::MAX` is never a valid chunk base).
    fn chunk_of(&self, offset: u64) -> Option<usize> {
        let offset = offset as usize;
        let first = self.base + size_of::<u64>();
        let last = first + EXT_CONFIG_CHUNK_COUNT * EXT_CONFIG_CHUNK_STRIDE;
        if offset < first || offset >= last {
            return None;
        }
        let chunk = (offset - first) / EXT_CONFIG_CHUNK_STRIDE;
        if chunk >= EXT_CONFIG_CHUNK_COUNT || self.chunk_base(chunk) != offset {
            return None;
        }
        Some(chunk)
    }
}
