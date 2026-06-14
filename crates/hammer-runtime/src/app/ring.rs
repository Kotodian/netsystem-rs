use std::cell::RefCell;
use std::future::poll_fn;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::descriptor::Descriptor;
use hammer_infra::map::FlatHashTable;
use hammer_infra::ring::{CompletionDescriptor, LockFreeRing, RingEntry, SubmissionDescriptor};
use hammer_infra::vec::Vec;

use crate::app::data::{AppDataAddr, AppDataArea, AppDataAreaConfig};
use crate::app::layout::{AppRingExport, AppRingLayout, AppRingMemoryKind, ring_size_for_capacity};

pub enum AppOpTag {}
pub type AppOpId = Descriptor<AppOpTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppUserData(u64);

impl AppUserData {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppOpcode {
    Nop,
    Recv,
    Send,
    Close,
}

pub type AppSubmissionEntry = RingEntry<AppSqeDescriptor, ()>;
pub type AppCompletionEntry = RingEntry<AppCqeDescriptor, ()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppObjectRef {
    None,
    Operation(AppOpId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppSqeData {
    Nop,
    Recv { max_len: u32 },
    Send { data: AppDataAddr },
    Close,
}

pub type AppSqeDescriptor =
    SubmissionDescriptor<AppOpcode, Option<AppUserData>, AppObjectRef, AppSqeData>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppCqeFlags(u32);

impl AppCqeFlags {
    pub const NONE: Self = Self(0);
    pub const BUFFER: Self = Self(1 << 0);
    pub const FIN: Self = Self(1 << 1);

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppCqeData {
    None,
    Recv { data: AppDataAddr },
    Closed,
}

pub type AppCqeDescriptor =
    CompletionDescriptor<Option<AppUserData>, i32, AppCqeFlags, AppObjectRef, AppCqeData>;

#[derive(Debug)]
pub enum AppSqe {
    Nop {
        user_data: Option<AppUserData>,
    },
    Recv {
        user_data: Option<AppUserData>,
        op: AppOpId,
        max: usize,
    },
    Send {
        user_data: Option<AppUserData>,
        op: AppOpId,
        send: AppSend,
    },
    Close {
        user_data: Option<AppUserData>,
        op: AppOpId,
    },
}

impl AppSqe {
    #[inline]
    pub const fn nop(user_data: Option<AppUserData>) -> Self {
        Self::Nop { user_data }
    }

    #[inline]
    pub const fn recv(user_data: Option<AppUserData>, op: AppOpId, max: usize) -> Self {
        Self::Recv { user_data, op, max }
    }

    #[inline]
    pub fn send(user_data: Option<AppUserData>, op: AppOpId, send: AppSend) -> Self {
        Self::Send {
            user_data,
            op,
            send,
        }
    }

    #[inline]
    pub const fn close(user_data: Option<AppUserData>, op: AppOpId) -> Self {
        Self::Close { user_data, op }
    }

    #[inline]
    pub const fn user_data(&self) -> Option<AppUserData> {
        match self {
            Self::Nop { user_data }
            | Self::Recv { user_data, .. }
            | Self::Send { user_data, .. }
            | Self::Close { user_data, .. } => *user_data,
        }
    }

    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        match self {
            Self::Nop { .. } => AppOpcode::Nop,
            Self::Recv { .. } => AppOpcode::Recv,
            Self::Send { .. } => AppOpcode::Send,
            Self::Close { .. } => AppOpcode::Close,
        }
    }

    #[inline]
    pub const fn op(&self) -> Option<AppOpId> {
        match self {
            Self::Recv { op, .. } | Self::Send { op, .. } | Self::Close { op, .. } => Some(*op),
            Self::Nop { .. } => None,
        }
    }

    #[inline]
    pub const fn max(&self) -> Option<usize> {
        match self {
            Self::Recv { max, .. } => Some(*max),
            _ => None,
        }
    }

    #[inline]
    pub fn into_send(self) -> Option<AppSend> {
        match self {
            Self::Send { send, .. } => Some(send),
            _ => None,
        }
    }

    #[inline]
    pub fn descriptor(&self) -> HammerResult<AppSqeDescriptor> {
        match self {
            Self::Nop { user_data } => Ok(AppSqeDescriptor::new(
                AppOpcode::Nop,
                *user_data,
                AppObjectRef::None,
                AppSqeData::Nop,
            )),
            Self::Recv { user_data, op, max } => Ok(AppSqeDescriptor::new(
                AppOpcode::Recv,
                *user_data,
                AppObjectRef::Operation(*op),
                AppSqeData::Recv {
                    max_len: *max as u32,
                },
            )),
            Self::Send {
                user_data,
                op,
                send,
            } => send.descriptor(*user_data, *op),
            Self::Close { user_data, op } => Ok(AppSqeDescriptor::new(
                AppOpcode::Close,
                *user_data,
                AppObjectRef::Operation(*op),
                AppSqeData::Close,
            )),
        }
    }
}

#[derive(Debug)]
pub struct AppRecv {
    data: Option<AppDataAddr>,
    ring: AppRingHandle,
}

#[derive(Debug)]
pub enum AppCqeKind {
    Recv {
        op: AppOpId,
        recv: AppRecv,
        fin: bool,
    },
    Closed {
        op: Option<AppOpId>,
    },
}

impl AppCqeKind {
    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        match self {
            Self::Recv { .. } => AppOpcode::Recv,
            Self::Closed { .. } => AppOpcode::Close,
        }
    }
}

#[derive(Debug)]
pub struct AppCqe {
    inner: AppCqeView,
}

#[derive(Debug)]
pub struct AppCqeView {
    user_data: Option<AppUserData>,
    kind: AppCqeKind,
}

impl AppCqe {
    #[inline]
    pub const fn new(user_data: Option<AppUserData>, kind: AppCqeKind) -> Self {
        Self {
            inner: AppCqeView { user_data, kind },
        }
    }

    #[inline]
    pub fn recv(user_data: Option<AppUserData>, op: AppOpId, recv: AppRecv, fin: bool) -> Self {
        Self::new(user_data, AppCqeKind::Recv { op, recv, fin })
    }

    #[inline]
    pub const fn closed(user_data: Option<AppUserData>, op: Option<AppOpId>) -> Self {
        Self::new(user_data, AppCqeKind::Closed { op })
    }

    #[inline]
    pub const fn user_data(&self) -> Option<AppUserData> {
        self.inner.user_data
    }

    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        self.inner.kind.opcode()
    }

    #[inline]
    pub fn kind(&self) -> &AppCqeKind {
        &self.inner.kind
    }

    #[inline]
    pub fn into_send(self) -> Option<AppSend> {
        match self.inner.kind {
            AppCqeKind::Recv { recv, .. } => Some(recv.into_send()),
            _ => None,
        }
    }

    #[inline]
    pub fn into_recv(self) -> Option<AppRecv> {
        match self.inner.kind {
            AppCqeKind::Recv { recv, .. } => Some(recv),
            _ => None,
        }
    }

    #[inline]
    pub fn into_descriptor(self) -> HammerResult<Option<AppCqeDescriptor>> {
        Ok(Some(cqe_into_descriptor(self)))
    }

    #[inline]
    pub fn descriptor(self) -> HammerResult<Option<AppCqeDescriptor>> {
        self.into_descriptor()
    }
}

impl Deref for AppCqe {
    type Target = AppCqeView;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AppCqeView {
    #[inline]
    pub fn recv(&self) -> Option<&AppRecv> {
        match &self.kind {
            AppCqeKind::Recv { recv, .. } => Some(recv),
            _ => None,
        }
    }
}

impl AppRecv {
    #[inline]
    pub fn new(ring: AppRingHandle, data: AppDataAddr) -> Self {
        Self {
            data: Some(data),
            ring,
        }
    }

    #[inline]
    pub fn data(&self) -> AppDataAddr {
        self.data.expect("app recv released")
    }

    #[inline]
    pub fn copy_current(&self) -> HammerResult<std::vec::Vec<u8>> {
        self.ring.read_data(self.data())
    }

    #[inline]
    pub fn into_send(self) -> AppSend {
        let mut this = self;
        let data = this.data.take().expect("app recv released");
        AppSend::from_data(this.ring.clone(), data)
    }

    #[inline]
    pub fn into_data_addr(self) -> AppDataAddr {
        let mut this = self;
        this.data.take().expect("app recv released")
    }

    #[inline]
    pub fn release(mut self) {
        if let Some(data) = self.data.take() {
            let _ = self.ring.release_data(data);
        }
    }
}

impl Drop for AppRecv {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            let _ = self.ring.release_data(data);
        }
    }
}

#[derive(Debug)]
pub struct AppSend {
    payload: Option<AppSendPayload>,
}

#[derive(Debug)]
enum AppSendPayload {
    Data {
        data: AppDataAddr,
        ring: AppRingHandle,
    },
}

#[derive(Debug)]
pub struct AppSendData {
    data: Option<AppDataAddr>,
    data_area: Arc<AppDataArea>,
    free_chunks: Arc<LockFreeRing<u32>>,
}

impl Drop for AppSendData {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            let _ = self.data_area.release(data);
            let _ = self.free_chunks.enqueue_sp(data.chunk());
        }
    }
}

impl Drop for AppSend {
    fn drop(&mut self) {
        match self.payload.take() {
            Some(AppSendPayload::Data { data, ring }) => {
                let _ = ring.release_data(data);
            }
            None => {}
        }
    }
}

#[derive(Debug)]
struct AppRingWaker {
    waker: Option<Waker>,
}

impl AppRingWaker {
    #[inline]
    fn new() -> Self {
        Self { waker: None }
    }

    #[inline]
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn register(&mut self, cx: &mut Context<'_>) {
        let replace = match self.waker.as_ref() {
            Some(waker) => !waker.will_wake(cx.waker()),
            None => true,
        };
        if replace {
            self.waker = Some(cx.waker().clone());
        }
    }
}

impl Default for AppRingWaker {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingSubmission {
    user_data: Option<AppUserData>,
    object: AppObjectRef,
    payload: AppSqeData,
}

#[derive(Debug, Default)]
struct AppPendingSubmissionRegistry {
    recv_by_op: FlatHashTable<u64, PendingSubmission>,
}

impl AppPendingSubmissionRegistry {
    #[inline]
    fn record_descriptor(&mut self, descriptor: AppSqeDescriptor) {
        match descriptor.opcode() {
            AppOpcode::Recv => {
                let AppObjectRef::Operation(op) = descriptor.object() else {
                    return;
                };
                self.recv_by_op.insert(
                    op.value(),
                    PendingSubmission {
                        user_data: descriptor.user_data(),
                        object: descriptor.object(),
                        payload: descriptor.payload(),
                    },
                );
            }
            _ => {
                let _ = descriptor.payload();
            }
        }
    }

    #[inline]
    fn lookup_recv(&self, op: AppOpId) -> Option<PendingSubmission> {
        self.recv_by_op.lookup(&op.value())
    }

    #[inline]
    fn remove_recv(&mut self, op: AppOpId) -> Option<PendingSubmission> {
        self.recv_by_op.remove(&op.value())
    }
}

#[derive(Clone, Debug)]
pub struct AppRingHandle {
    submissions: Arc<LockFreeRing<AppSqeDescriptor>>,
    completions: Arc<LockFreeRing<AppCqeDescriptor>>,
    free_chunks: Arc<LockFreeRing<u32>>,
    submission_waker: Rc<RefCell<AppRingWaker>>,
    completion_waker: Rc<RefCell<AppRingWaker>>,
    pending_submissions: Rc<RefCell<AppPendingSubmissionRegistry>>,
    layout: AppRingLayout,
    data_area: Arc<AppDataArea>,
}

impl AppRingHandle {
    #[inline]
    pub fn new(submission_capacity: usize, completion_capacity: usize) -> Self {
        Self::with_data_area(
            submission_capacity,
            completion_capacity,
            2048,
            submission_capacity.max(completion_capacity).max(1),
        )
        .expect("default app ring data area")
    }

    pub fn with_data_area(
        submission_capacity: usize,
        completion_capacity: usize,
        data_chunk_size: usize,
        data_chunk_count: usize,
    ) -> HammerResult<Self> {
        if submission_capacity == 0 || completion_capacity == 0 || data_chunk_count == 0 {
            return Err(HammerError::internal(
                "app ring capacities must be non-zero",
            ));
        }
        let submission_ring_size = ring_size_for_capacity(submission_capacity);
        let completion_ring_size = ring_size_for_capacity(completion_capacity);
        let fill_ring_size = ring_size_for_capacity(data_chunk_count);
        let layout = AppRingLayout::new(
            submission_capacity,
            completion_capacity,
            data_chunk_size,
            data_chunk_count,
        );
        let free_chunks = Arc::new(
            LockFreeRing::with_capacity(fill_ring_size)
                .map_err(|_| HammerError::internal("invalid app fill ring capacity"))?,
        );
        for chunk in 0..data_chunk_count {
            free_chunks
                .enqueue_sp(chunk as u32)
                .map_err(|_| HammerError::internal("app fill ring initialization failed"))?;
        }
        Ok(Self {
            submissions: Arc::new(
                LockFreeRing::with_capacity(submission_ring_size)
                    .map_err(|_| HammerError::internal("invalid app submission ring capacity"))?,
            ),
            completions: Arc::new(
                LockFreeRing::with_capacity(completion_ring_size)
                    .map_err(|_| HammerError::internal("invalid app completion ring capacity"))?,
            ),
            free_chunks,
            submission_waker: Rc::new(RefCell::new(AppRingWaker::default())),
            completion_waker: Rc::new(RefCell::new(AppRingWaker::default())),
            pending_submissions: Rc::new(RefCell::new(AppPendingSubmissionRegistry::default())),
            layout,
            data_area: Arc::new(AppDataArea::new(AppDataAreaConfig {
                chunk_size: data_chunk_size,
                chunk_count: data_chunk_count,
            })?),
        })
    }

    #[inline]
    pub fn for_tests(submission_capacity: usize, completion_capacity: usize) -> Self {
        Self::new(submission_capacity, completion_capacity)
    }

    #[inline]
    pub fn layout(&self) -> AppRingLayout {
        self.layout
    }

    #[inline]
    pub fn export_layout(&self) -> AppRingExport {
        AppRingExport::new(AppRingMemoryKind::ProcessLocal, self.layout)
    }

    #[inline]
    pub fn alloc_data_for_bytes(&self, bytes: &[u8]) -> HammerResult<AppDataAddr> {
        let chunk = self
            .free_chunks
            .dequeue_sc()
            .ok_or_else(|| HammerError::internal("app data area is full"))?;
        let addr = self.data_area.alloc_chunk(chunk)?;
        match self.data_area.write(addr, bytes) {
            Ok(addr) => Ok(addr),
            Err(err) => {
                let _ = self.data_area.release(addr);
                let _ = self.free_chunks.enqueue_sp(chunk);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn alloc_data_from_dataplane_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> HammerResult<AppDataAddr> {
        let chunk = self
            .free_chunks
            .dequeue_sc()
            .ok_or_else(|| HammerError::internal("app data area is full"))?;
        let addr = self.data_area.alloc_chunk(chunk)?;
        match self.data_area.copy_from_buffer(addr, buffers, index) {
            Ok(addr) => Ok(addr),
            Err(err) => {
                let _ = self.data_area.release(addr);
                let _ = self.free_chunks.enqueue_sp(chunk);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn read_data(&self, addr: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
        self.data_area.read(addr)
    }

    #[inline]
    pub fn release_data(&self, addr: AppDataAddr) -> HammerResult<()> {
        self.data_area.release(addr)?;
        self.free_chunks
            .enqueue_sp(addr.chunk())
            .map_err(|_| HammerError::internal("app free chunk ring is full"))
    }

    #[inline]
    pub fn copy_data_from_send(&self, send: &AppSendData) -> HammerResult<AppDataAddr> {
        let source = send.data()?;
        let chunk = self
            .free_chunks
            .dequeue_sc()
            .ok_or_else(|| HammerError::internal("app data area is full"))?;
        let addr = self.data_area.alloc_chunk(chunk)?;
        match self
            .data_area
            .copy_from_area(addr, send.data_area(), source)
        {
            Ok(addr) => Ok(addr),
            Err(err) => {
                let _ = self.data_area.release(addr);
                let _ = self.free_chunks.enqueue_sp(chunk);
                Err(err)
            }
        }
    }

    #[inline]
    pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        match sqe {
            AppSqe::Send {
                user_data,
                op,
                send,
            } => {
                let data = send.into_data_addr()?;
                if let Err(err) = self.try_push_submission_descriptor(AppSqeDescriptor::new(
                    AppOpcode::Send,
                    user_data,
                    AppObjectRef::Operation(op),
                    AppSqeData::Send { data },
                )) {
                    let _ = self.release_data(data);
                    return Err(err);
                }
                Ok(())
            }
            other => {
                let descriptor = sqe_into_descriptor(other)?;
                self.try_push_submission_descriptor(descriptor)
            }
        }
    }

    #[inline]
    pub fn pop_submission(&self) -> Option<AppSqe> {
        let descriptor = self.submissions.dequeue_sc()?;
        Some(sqe_from_descriptor(descriptor, self))
    }

    #[inline]
    pub fn try_push_completion(&self, cqe: AppCqe) -> HammerResult<()> {
        let descriptor = cqe_into_descriptor(cqe);
        if let Err(err) = self.try_push_completion_descriptor(descriptor) {
            if let AppCqeData::Recv { data } = descriptor.payload() {
                let _ = self.release_data(data);
            }
            return Err(err);
        }
        Ok(())
    }

    #[inline]
    pub fn pop_completion(&self) -> Option<AppCqe> {
        let descriptor = self.completions.dequeue_sc()?;
        Some(cqe_from_descriptor(descriptor, self))
    }

    #[inline]
    pub async fn next_submission(&self) -> Option<AppSqe> {
        let descriptor = poll_fn(|cx| self.poll_next_submission_descriptor(cx)).await?;
        Some(sqe_from_descriptor(descriptor, self))
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        let descriptor = poll_fn(|cx| self.poll_next_completion_descriptor(cx)).await?;
        Some(cqe_from_descriptor(descriptor, self))
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()> {
        self.submissions
            .enqueue_sp(sqe)
            .map_err(|_| HammerError::internal("app submission descriptor ring full"))?;
        self.submission_waker.borrow_mut().wake();
        self.pending_submissions.borrow_mut().record_descriptor(sqe);
        Ok(())
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        let (descriptor, attachment) = entry.into_parts();
        if attachment.is_some() {
            return Err(HammerError::internal(
                "app submission entries do not accept buffer attachments",
            ));
        }
        if let Err(err) = self.try_push_submission_descriptor(descriptor) {
            if let AppSqeData::Send { data } = descriptor.payload() {
                let _ = self.release_data(data);
            }
            return Err(err);
        }
        Ok(())
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.completions
            .enqueue_sp(cqe)
            .map_err(|_| HammerError::internal("app completion descriptor ring full"))?;
        self.completion_waker.borrow_mut().wake();
        Ok(())
    }

    #[inline]
    pub fn send_from_data(&self, data: AppDataAddr) -> AppSend {
        AppSend::from_data(self.clone(), data)
    }

    #[inline]
    pub fn send_data_from_addr(&self, data: AppDataAddr) -> AppSendData {
        AppSendData {
            data: Some(data),
            data_area: Arc::clone(&self.data_area),
            free_chunks: Arc::clone(&self.free_chunks),
        }
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        let (descriptor, attachment) = entry.into_parts();
        if attachment.is_some() {
            return Err(HammerError::internal(
                "app completion entries do not accept buffer attachments",
            ));
        }
        self.try_push_completion_descriptor(descriptor)
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        op: AppOpId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let pending = {
            let pending = self.pending_submissions.borrow();
            match pending.lookup_recv(op) {
                Some(found) => found,
                None => {
                    buffers.free_index(index);
                    return Err(HammerError::internal(format!(
                        "pending recv submission missing for app op {}",
                        op.value()
                    )));
                }
            }
        };
        if !matches!(pending.payload, AppSqeData::Recv { .. }) {
            buffers.free_index(index);
            return Err(HammerError::internal("pending app op is not recv"));
        }
        self.try_complete_recv_pending(op, pending, buffers, index, fin)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        let descriptor = poll_fn(|cx| self.poll_next_submission_descriptor(cx)).await?;
        Some(submission_entry_from_descriptor(descriptor, self))
    }

    #[inline]
    pub fn pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
        let descriptor = self.pop_submission_descriptor()?;
        Some(submission_entry_from_descriptor(descriptor, self))
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        poll_fn(|cx| self.poll_next_submission_descriptor(cx)).await
    }

    #[inline]
    pub fn pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.submissions.dequeue_sc()
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        let descriptor = poll_fn(|cx| self.poll_next_completion_descriptor(cx)).await?;
        Some(completion_entry_from_descriptor(descriptor, self))
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        poll_fn(|cx| self.poll_next_completion_descriptor(cx)).await
    }

    #[inline]
    pub(crate) fn poll_next_completion(&self, cx: &mut Context<'_>) -> Poll<Option<AppCqe>> {
        let descriptor = match self.poll_next_completion_descriptor(cx) {
            Poll::Ready(Some(descriptor)) => descriptor,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(Some(cqe_from_descriptor(descriptor, self)))
    }

    #[inline]
    pub fn push_test_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        self.try_push_submission(sqe)
    }

    #[inline]
    pub fn take_test_submissions(&self, max: usize) -> Vec<AppSqe> {
        drain_submission_descriptors(self, max)
            .into_iter()
            .map(|descriptor| sqe_from_descriptor(descriptor, self))
            .collect()
    }

    #[inline]
    pub fn push_test_completion(&self, cqe: AppCqe) -> HammerResult<()> {
        self.try_push_completion(cqe)
    }

    #[inline]
    pub fn take_test_completions(&self, max: usize) -> Vec<AppCqe> {
        drain_completion_descriptors(self, max)
            .into_iter()
            .map(|descriptor| cqe_from_descriptor(descriptor, self))
            .collect()
    }

    #[inline]
    fn poll_next_submission_descriptor(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<AppSqeDescriptor>> {
        if let Some(descriptor) = self.submissions.dequeue_sc() {
            return Poll::Ready(Some(descriptor));
        }
        self.submission_waker.borrow_mut().register(cx);
        if let Some(descriptor) = self.submissions.dequeue_sc() {
            self.submission_waker.borrow_mut().waker = None;
            Poll::Ready(Some(descriptor))
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn poll_next_completion_descriptor(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<AppCqeDescriptor>> {
        if let Some(descriptor) = self.completions.dequeue_sc() {
            return Poll::Ready(Some(descriptor));
        }
        self.completion_waker.borrow_mut().register(cx);
        if let Some(descriptor) = self.completions.dequeue_sc() {
            self.completion_waker.borrow_mut().waker = None;
            Poll::Ready(Some(descriptor))
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn try_complete_recv_pending(
        &self,
        op: AppOpId,
        pending: PendingSubmission,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let data = match self.alloc_data_from_dataplane_buffer(&buffers, index) {
            Ok(data) => data,
            Err(err) => {
                buffers.free_index(index);
                return Err(err);
            }
        };
        buffers.free_index(index);
        self.complete_recv_pending_with_data(op, pending, data, fin)
    }

    #[inline]
    fn complete_recv_pending_with_data(
        &self,
        op: AppOpId,
        pending: PendingSubmission,
        data: AppDataAddr,
        fin: bool,
    ) -> HammerResult<()> {
        let descriptor = AppCqeDescriptor::new(
            pending.user_data,
            data.len() as i32,
            if fin {
                AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
            } else {
                AppCqeFlags::BUFFER
            },
            pending.object,
            AppCqeData::Recv { data },
        );
        if let Err(err) = self.try_push_completion_descriptor(descriptor) {
            let _ = self.release_data(data);
            return Err(err);
        }
        self.pending_submissions.borrow_mut().remove_recv(op);
        let _ = op;
        Ok(())
    }
}

#[inline]
fn drain_submission_descriptors(ring: &AppRingHandle, max: usize) -> Vec<AppSqeDescriptor> {
    let mut drained = Vec::new();
    for _ in 0..max {
        let Some(value) = ring.submissions.dequeue_sc() else {
            break;
        };
        drained.push(value);
    }
    drained
}

#[inline]
fn drain_completion_descriptors(ring: &AppRingHandle, max: usize) -> Vec<AppCqeDescriptor> {
    let mut drained = Vec::new();
    for _ in 0..max {
        let Some(value) = ring.completions.dequeue_sc() else {
            break;
        };
        drained.push(value);
    }
    drained
}

#[inline]
fn sqe_into_descriptor(sqe: AppSqe) -> HammerResult<AppSqeDescriptor> {
    match sqe {
        AppSqe::Nop { user_data } => Ok(AppSqeDescriptor::new(
            AppOpcode::Nop,
            user_data,
            AppObjectRef::None,
            AppSqeData::Nop,
        )),
        AppSqe::Recv { user_data, op, max } => Ok(AppSqeDescriptor::new(
            AppOpcode::Recv,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Recv {
                max_len: max as u32,
            },
        )),
        AppSqe::Send { .. } => Err(HammerError::internal(
            "send sqe conversion requires app ring data area",
        )),
        AppSqe::Close { user_data, op } => Ok(AppSqeDescriptor::new(
            AppOpcode::Close,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Close,
        )),
    }
}

#[inline]
fn sqe_from_descriptor(descriptor: AppSqeDescriptor, ring: &AppRingHandle) -> AppSqe {
    match descriptor.payload() {
        AppSqeData::Nop => AppSqe::nop(descriptor.user_data()),
        AppSqeData::Recv { max_len } => AppSqe::recv(
            descriptor.user_data(),
            op_from_descriptor(descriptor),
            max_len as usize,
        ),
        AppSqeData::Send { data } => AppSqe::send(
            descriptor.user_data(),
            op_from_descriptor(descriptor),
            AppSend::from_data(ring.clone(), data),
        ),
        AppSqeData::Close => AppSqe::close(descriptor.user_data(), op_from_descriptor(descriptor)),
    }
}

#[inline]
fn op_from_descriptor(descriptor: AppSqeDescriptor) -> AppOpId {
    match descriptor.object() {
        AppObjectRef::Operation(op) => op,
        other => panic!("app sqe expects operation object, got {other:?}"),
    }
}

#[inline]
fn submission_entry_from_descriptor(
    descriptor: AppSqeDescriptor,
    ring: &AppRingHandle,
) -> AppSubmissionEntry {
    let _ = ring;
    AppSubmissionEntry::new(descriptor)
}

#[inline]
fn cqe_into_descriptor(cqe: AppCqe) -> AppCqeDescriptor {
    let user_data = cqe.user_data();
    match cqe.inner.kind {
        AppCqeKind::Recv { op, recv, fin } => {
            let data = recv.into_data_addr();
            AppCqeDescriptor::new(
                user_data,
                data.len() as i32,
                if fin {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Operation(op),
                AppCqeData::Recv { data },
            )
        }
        AppCqeKind::Closed { op } => AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            op.map_or(AppObjectRef::None, AppObjectRef::Operation),
            AppCqeData::Closed,
        ),
    }
}

#[inline]
fn cqe_from_descriptor(descriptor: AppCqeDescriptor, ring: &AppRingHandle) -> AppCqe {
    match descriptor.payload() {
        AppCqeData::None => AppCqe::new(descriptor.user_data(), AppCqeKind::Closed { op: None }),
        AppCqeData::Recv { data } => AppCqe::recv(
            descriptor.user_data(),
            op_from_completion_descriptor(descriptor),
            AppRecv::new(ring.clone(), data),
            descriptor.flags().contains(AppCqeFlags::FIN),
        ),
        AppCqeData::Closed => AppCqe::new(
            descriptor.user_data(),
            AppCqeKind::Closed {
                op: match descriptor.object() {
                    AppObjectRef::Operation(op) => Some(op),
                    AppObjectRef::None => None,
                },
            },
        ),
    }
}

#[inline]
fn op_from_completion_descriptor(descriptor: AppCqeDescriptor) -> AppOpId {
    match descriptor.object() {
        AppObjectRef::Operation(op) => op,
        other => panic!("app cqe expects operation object, got {other:?}"),
    }
}

#[inline]
fn completion_entry_from_descriptor(
    descriptor: AppCqeDescriptor,
    ring: &AppRingHandle,
) -> AppCompletionEntry {
    let _ = ring;
    AppCompletionEntry::new(descriptor)
}

impl AppSend {
    #[inline]
    pub fn from_data(ring: AppRingHandle, data: AppDataAddr) -> Self {
        Self {
            payload: Some(AppSendPayload::Data { data, ring }),
        }
    }

    #[inline]
    pub fn data(&self) -> Option<AppDataAddr> {
        match self.payload.as_ref().expect("app send released") {
            AppSendPayload::Data { data, .. } => Some(*data),
        }
    }

    #[inline]
    pub fn copy_current(&self) -> HammerResult<std::vec::Vec<u8>> {
        match self.payload.as_ref().expect("app send released") {
            AppSendPayload::Data { data, ring } => ring.read_data(*data),
        }
    }

    #[inline]
    pub fn into_data_addr(self) -> HammerResult<AppDataAddr> {
        let mut this = self;
        match this.payload.take().expect("app send released") {
            AppSendPayload::Data { data, .. } => Ok(data),
        }
    }

    #[inline]
    pub(crate) fn into_transfer_data(self) -> HammerResult<AppSendData> {
        let mut this = self;
        match this.payload.take().expect("app send released") {
            AppSendPayload::Data { data, ring } => Ok(ring.send_data_from_addr(data)),
        }
    }

    #[inline]
    pub fn release(mut self) {
        match self.payload.take() {
            Some(AppSendPayload::Data { data, ring }) => {
                let _ = ring.release_data(data);
            }
            None => {}
        }
    }
}

impl AppSendData {
    #[inline]
    fn data(&self) -> HammerResult<AppDataAddr> {
        self.data
            .ok_or_else(|| HammerError::internal("app send data released"))
    }

    #[inline]
    fn data_area(&self) -> &AppDataArea {
        &self.data_area
    }

    #[inline]
    pub fn release(mut self) {
        if let Some(data) = self.data.take() {
            let _ = self.data_area.release(data);
            let _ = self.free_chunks.enqueue_sp(data.chunk());
        }
    }
}

impl AppSend {
    #[inline]
    pub fn descriptor(
        &self,
        user_data: Option<AppUserData>,
        op: AppOpId,
    ) -> HammerResult<AppSqeDescriptor> {
        let Some(data) = self.data() else {
            return Err(HammerError::internal(
                "send descriptor requires app data address",
            ));
        };
        Ok(AppSqeDescriptor::new(
            AppOpcode::Send,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Send { data },
        ))
    }
}
