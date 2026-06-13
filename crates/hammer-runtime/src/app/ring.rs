use std::cell::RefCell;
use std::future::poll_fn;
use std::ops::Deref;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::descriptor::Descriptor;
use hammer_infra::ring::{CompletionDescriptor, IndexedRing, RingEntry, SubmissionDescriptor};
use hammer_infra::vec::Vec;

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

#[derive(Debug)]
pub struct AppBufferLease {
    runtime: Option<DataPlaneBuffers>,
    index: BufferIndex,
}

#[derive(Debug)]
pub struct AppRegisteredBuffer {
    index: BufferIndex,
    lease: AppBufferLease,
}

pub type AppSubmissionEntry = RingEntry<AppSqeDescriptor, AppRegisteredBuffer>;
pub type AppCompletionEntry = RingEntry<AppCqeDescriptor, AppRegisteredBuffer>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppObjectRef {
    None,
    Operation(AppOpId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppSqeData {
    Nop,
    Recv { max_len: u32 },
    Send { buffer: BufferIndex },
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
    Recv { buffer: BufferIndex },
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

impl AppBufferLease {
    #[inline]
    pub fn from_buffer(runtime: DataPlaneBuffers, index: BufferIndex) -> Self {
        Self {
            runtime: Some(runtime),
            index,
        }
    }

    #[inline]
    pub fn index(&self) -> BufferIndex {
        self.index
    }

    #[inline]
    pub fn current_ptr(&self) -> HammerResult<*const u8> {
        self.runtime().current_ptr(self.index)
    }

    #[inline]
    pub fn current_mut_ptr(&self) -> HammerResult<*mut u8> {
        self.runtime().current_mut_ptr(self.index)
    }

    #[inline]
    pub fn current_len(&self) -> HammerResult<usize> {
        self.runtime().current_len(self.index)
    }

    #[inline]
    pub fn copy_current(&self) -> HammerResult<Vec<u8>> {
        self.runtime().copy_current(self.index)
    }

    #[inline]
    pub fn runtime(&self) -> &DataPlaneBuffers {
        self.runtime.as_ref().expect("app buffer lease released")
    }

    #[inline]
    pub fn release(mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.free_index(self.index);
        }
    }

    #[inline]
    pub fn into_recv(self) -> AppRecv {
        AppRecv::new(self)
    }
}

impl Drop for AppBufferLease {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.free_index(self.index);
        }
    }
}

impl AppRegisteredBuffer {
    #[inline]
    pub fn from_lease(lease: AppBufferLease) -> HammerResult<Self> {
        Ok(Self {
            index: lease.index(),
            lease,
        })
    }

    #[inline]
    pub const fn index(&self) -> BufferIndex {
        self.index
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        &self.lease
    }

    #[inline]
    pub fn into_parts(self) -> (BufferIndex, AppBufferLease) {
        (self.index, self.lease)
    }
}

#[derive(Debug)]
pub struct AppRecv {
    lease: AppBufferLease,
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
    pub fn descriptor(&self) -> HammerResult<Option<AppCqeDescriptor>> {
        let descriptor = match self.kind() {
            AppCqeKind::Recv { op, recv, fin } => Some(AppCqeDescriptor::new(
                self.user_data(),
                recv.lease().current_len()? as i32,
                if *fin {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Operation(*op),
                AppCqeData::Recv {
                    buffer: recv.lease().index(),
                },
            )),
            AppCqeKind::Closed { op } => Some(AppCqeDescriptor::new(
                self.user_data(),
                0,
                AppCqeFlags::NONE,
                op.map_or(AppObjectRef::None, AppObjectRef::Operation),
                AppCqeData::Closed,
            )),
        };
        Ok(descriptor)
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
    pub fn new(lease: AppBufferLease) -> Self {
        Self { lease }
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        &self.lease
    }

    #[inline]
    pub fn into_send(self) -> AppSend {
        AppSend { lease: self.lease }
    }

    #[inline]
    pub fn into_lease(self) -> AppBufferLease {
        self.lease
    }

    #[inline]
    pub fn release(self) {
        self.lease.release();
    }
}

#[derive(Debug)]
pub struct AppSend {
    lease: AppBufferLease,
}

#[derive(Debug)]
struct AppRingState<T> {
    ring: IndexedRing<T>,
    waker: Option<Waker>,
}

impl<T> AppRingState<T> {
    #[inline]
    fn new(capacity: usize) -> Self {
        Self {
            ring: IndexedRing::with_capacity(capacity),
            waker: None,
        }
    }

    #[inline]
    fn try_push(&mut self, value: T) -> Result<(), T> {
        let pushed = self.ring.try_push(value);
        if pushed.is_ok()
            && let Some(waker) = self.waker.take()
        {
            waker.wake();
        }
        pushed.map(|_| ())
    }

    #[inline]
    fn pop(&mut self) -> Option<T> {
        self.ring.pop().map(|(_, value)| value)
    }

    #[inline]
    fn poll_pop(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some((_, value)) = self.ring.pop() {
            return Poll::Ready(Some(value));
        }
        let replace = match self.waker.as_ref() {
            Some(waker) => !waker.will_wake(cx.waker()),
            None => true,
        };
        if replace {
            self.waker = Some(cx.waker().clone());
        }
        if let Some((_, value)) = self.ring.pop() {
            self.waker.take();
            Poll::Ready(Some(value))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
struct RegisteredBufferLease {
    index: BufferIndex,
    lease: AppBufferLease,
}

#[derive(Debug, Default)]
struct AppRingBufferRegistry {
    leases: Vec<RegisteredBufferLease>,
}

impl AppRingBufferRegistry {
    #[inline]
    fn register(&mut self, index: BufferIndex, lease: AppBufferLease) {
        if let Some(entry) = self.leases.iter_mut().find(|entry| entry.index == index) {
            entry.lease = lease;
            return;
        }
        self.leases.push(RegisteredBufferLease { index, lease });
    }

    #[inline]
    fn take(&mut self, index: BufferIndex) -> Option<AppBufferLease> {
        let index = self.leases.iter().position(|entry| entry.index == index)?;
        let last = self
            .leases
            .pop()
            .expect("buffer registry entry exists at computed index");
        if index == self.leases.len() {
            return Some(last.lease);
        }
        let mut removed = last;
        std::mem::swap(&mut self.leases[index], &mut removed);
        Some(removed.lease)
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingSubmission {
    user_data: Option<AppUserData>,
    object: AppObjectRef,
    opcode: AppOpcode,
    payload: AppSqeData,
}

#[derive(Debug, Default)]
struct AppPendingSubmissionRegistry {
    submissions: Vec<PendingSubmission>,
}

impl AppPendingSubmissionRegistry {
    #[inline]
    fn record_descriptor(&mut self, descriptor: AppSqeDescriptor) {
        match descriptor.opcode() {
            AppOpcode::Recv => {
                self.submissions.push(PendingSubmission {
                    user_data: descriptor.user_data(),
                    object: descriptor.object(),
                    opcode: descriptor.opcode(),
                    payload: descriptor.payload(),
                });
            }
            _ => {
                let _ = descriptor.payload();
            }
        }
    }

    #[inline]
    fn find_op(&self, op: AppOpId, opcode: AppOpcode) -> Option<(usize, PendingSubmission)> {
        self.submissions
            .iter()
            .copied()
            .enumerate()
            .find(|(_, pending)| {
                pending.object == AppObjectRef::Operation(op) && pending.opcode == opcode
            })
    }

    #[inline]
    fn remove_at(&mut self, index: usize) -> PendingSubmission {
        self.submissions
            .drain(index..index + 1)
            .next()
            .expect("pending submission exists at computed index")
    }
}

#[derive(Clone, Debug)]
pub struct AppRingHandle {
    submissions: Rc<RefCell<AppRingState<AppSqeDescriptor>>>,
    completions: Rc<RefCell<AppRingState<AppCqeDescriptor>>>,
    buffers: Rc<RefCell<AppRingBufferRegistry>>,
    pending_submissions: Rc<RefCell<AppPendingSubmissionRegistry>>,
}

impl AppRingHandle {
    #[inline]
    pub fn new(submission_capacity: usize, completion_capacity: usize) -> Self {
        Self {
            submissions: Rc::new(RefCell::new(AppRingState::new(submission_capacity))),
            completions: Rc::new(RefCell::new(AppRingState::new(completion_capacity))),
            buffers: Rc::new(RefCell::new(AppRingBufferRegistry::default())),
            pending_submissions: Rc::new(RefCell::new(AppPendingSubmissionRegistry::default())),
        }
    }

    #[inline]
    pub fn for_tests(submission_capacity: usize, completion_capacity: usize) -> Self {
        Self::new(submission_capacity, completion_capacity)
    }

    #[inline]
    pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        let descriptor = sqe_into_descriptor(sqe, &self.buffers)?;
        self.submissions
            .borrow_mut()
            .try_push(descriptor)
            .map_err(|_| HammerError::internal("app submission ring full"))
    }

    #[inline]
    pub fn pop_submission(&self) -> Option<AppSqe> {
        let descriptor = self.submissions.borrow_mut().pop()?;
        Some(sqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub fn try_push_completion(&self, cqe: AppCqe) -> HammerResult<()> {
        let Some(descriptor) = cqe_into_descriptor(cqe, &self.buffers)? else {
            return Err(HammerError::internal(
                "app completion descriptor is missing buffer payload",
            ));
        };
        self.completions
            .borrow_mut()
            .try_push(descriptor)
            .map_err(|_| HammerError::internal("app completion ring full"))
    }

    #[inline]
    pub fn pop_completion(&self) -> Option<AppCqe> {
        let descriptor = self.completions.borrow_mut().pop()?;
        Some(cqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_submission(&self) -> Option<AppSqe> {
        let descriptor = poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await?;
        Some(sqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        let descriptor = poll_fn(|cx| self.completions.borrow_mut().poll_pop(cx)).await?;
        Some(cqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()> {
        self.submissions
            .borrow_mut()
            .try_push(sqe)
            .map_err(|_| HammerError::internal("app submission descriptor ring full"))?;
        self.pending_submissions.borrow_mut().record_descriptor(sqe);
        Ok(())
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        let (descriptor, registered) = entry.into_parts();
        if let Some(registered) = registered {
            let (index, lease) = registered.into_parts();
            self.buffers.borrow_mut().register(index, lease);
            if let Err(err) = self.try_push_submission_descriptor(descriptor) {
                let _ = self.buffers.borrow_mut().take(index);
                return Err(err);
            }
            return Ok(());
        }
        self.try_push_submission_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.completions
            .borrow_mut()
            .try_push(cqe)
            .map_err(|_| HammerError::internal("app completion descriptor ring full"))
    }

    #[inline]
    pub fn take_buffer_lease(&self, index: BufferIndex) -> HammerResult<AppBufferLease> {
        self.buffers.borrow_mut().take(index).ok_or_else(|| {
            HammerError::internal(format!(
                "registered app buffer {}:{}:{} is missing",
                index.pool_id(),
                index.slot(),
                index.generation()
            ))
        })
    }

    #[inline]
    pub fn take_send_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.take_buffer_lease(index).map(AppSend::new)
    }

    #[inline]
    pub fn take_recv_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.take_buffer_lease(index).map(AppRecv::new)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        let (descriptor, registered) = entry.into_parts();
        if let Some(registered) = registered {
            let (index, lease) = registered.into_parts();
            self.buffers.borrow_mut().register(index, lease);
            if let Err(err) = self.try_push_completion_descriptor(descriptor) {
                let _ = self.buffers.borrow_mut().take(index);
                return Err(err);
            }
            return Ok(());
        }
        self.try_push_completion_descriptor(descriptor)
    }

    #[inline]
    pub(crate) fn try_complete_recv_buffer(
        &self,
        op: AppOpId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        self.try_complete_recv_lease(op, AppBufferLease::from_buffer(buffers, index), fin)
    }

    #[inline]
    pub(crate) fn try_complete_recv_lease(
        &self,
        op: AppOpId,
        lease: AppBufferLease,
        fin: bool,
    ) -> HammerResult<()> {
        let (pending_index, pending) = {
            let pending = self.pending_submissions.borrow();
            pending.find_op(op, AppOpcode::Recv).ok_or_else(|| {
                HammerError::internal(format!(
                    "pending recv submission missing for app op {}",
                    op.value()
                ))
            })?
        };
        if !matches!(pending.payload, AppSqeData::Recv { .. }) {
            return Err(HammerError::internal("pending app op is not recv"));
        }
        self.try_complete_recv_pending(op, pending_index, pending, lease, fin)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        let descriptor = poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await?;
        Some(submission_entry_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub fn pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
        let descriptor = self.pop_submission_descriptor()?;
        Some(submission_entry_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await
    }

    #[inline]
    pub fn pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.submissions.borrow_mut().pop()
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        let descriptor = poll_fn(|cx| self.completions.borrow_mut().poll_pop(cx)).await?;
        Some(completion_entry_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        poll_fn(|cx| self.completions.borrow_mut().poll_pop(cx)).await
    }

    #[inline]
    pub(crate) fn poll_next_completion(&self, cx: &mut Context<'_>) -> Poll<Option<AppCqe>> {
        let descriptor = match self.completions.borrow_mut().poll_pop(cx) {
            Poll::Ready(Some(descriptor)) => descriptor,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(Some(cqe_from_descriptor(descriptor, &self.buffers)))
    }

    #[inline]
    pub fn push_test_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        self.try_push_submission(sqe)
    }

    #[inline]
    pub fn take_test_submissions(&self, max: usize) -> Vec<AppSqe> {
        drain_ring_descriptors(&self.submissions, max)
            .into_iter()
            .map(|descriptor| sqe_from_descriptor(descriptor, &self.buffers))
            .collect()
    }

    #[inline]
    pub fn push_test_completion(&self, cqe: AppCqe) -> HammerResult<()> {
        self.try_push_completion(cqe)
    }

    #[inline]
    pub fn take_test_completions(&self, max: usize) -> Vec<AppCqe> {
        drain_ring_descriptors(&self.completions, max)
            .into_iter()
            .map(|descriptor| cqe_from_descriptor(descriptor, &self.buffers))
            .collect()
    }

    #[inline]
    fn try_complete_recv_pending(
        &self,
        op: AppOpId,
        pending_index: usize,
        pending: PendingSubmission,
        lease: AppBufferLease,
        fin: bool,
    ) -> HammerResult<()> {
        let registered = AppRegisteredBuffer::from_lease(lease)?;
        let descriptor = AppCqeDescriptor::new(
            pending.user_data,
            registered.lease().current_len()? as i32,
            if fin {
                AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
            } else {
                AppCqeFlags::BUFFER
            },
            pending.object,
            AppCqeData::Recv {
                buffer: registered.index(),
            },
        );
        self.try_push_completion_entry(AppCompletionEntry::with_attachment(
            descriptor, registered,
        ))?;
        self.pending_submissions
            .borrow_mut()
            .remove_at(pending_index);
        let _ = op;
        Ok(())
    }
}

#[inline]
fn drain_ring_descriptors<T>(ring: &Rc<RefCell<AppRingState<T>>>, max: usize) -> Vec<T> {
    let mut drained = Vec::new();
    let mut ring = ring.borrow_mut();
    for _ in 0..max {
        let Some(value) = ring.pop() else {
            break;
        };
        drained.push(value);
    }
    drained
}

#[inline]
fn sqe_into_descriptor(
    sqe: AppSqe,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> HammerResult<AppSqeDescriptor> {
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
        AppSqe::Send {
            user_data,
            op,
            send,
        } => {
            let lease = send.into_lease();
            let index = lease.index();
            buffers.borrow_mut().register(index, lease);
            Ok(AppSqeDescriptor::new(
                AppOpcode::Send,
                user_data,
                AppObjectRef::Operation(op),
                AppSqeData::Send { buffer: index },
            ))
        }
        AppSqe::Close { user_data, op } => Ok(AppSqeDescriptor::new(
            AppOpcode::Close,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Close,
        )),
    }
}

#[inline]
fn sqe_from_descriptor(
    descriptor: AppSqeDescriptor,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppSqe {
    match descriptor.payload() {
        AppSqeData::Nop => AppSqe::nop(descriptor.user_data()),
        AppSqeData::Recv { max_len } => AppSqe::recv(
            descriptor.user_data(),
            op_from_descriptor(descriptor),
            max_len as usize,
        ),
        AppSqeData::Send { buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered send lease for submission descriptor");
            AppSqe::send(
                descriptor.user_data(),
                op_from_descriptor(descriptor),
                AppSend::new(lease),
            )
        }
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
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppSubmissionEntry {
    match descriptor.payload() {
        AppSqeData::Send { buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered send lease for submission entry");
            AppSubmissionEntry::with_attachment(
                descriptor,
                AppRegisteredBuffer {
                    index: buffer,
                    lease,
                },
            )
        }
        _ => AppSubmissionEntry::new(descriptor),
    }
}

#[inline]
fn cqe_into_descriptor(
    cqe: AppCqe,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> HammerResult<Option<AppCqeDescriptor>> {
    let user_data = cqe.user_data();
    match cqe.inner.kind {
        AppCqeKind::Recv { op, recv, fin } => {
            let lease = recv.into_lease();
            let index = lease.index();
            let result = lease.current_len()? as i32;
            buffers.borrow_mut().register(index, lease);
            Ok(Some(AppCqeDescriptor::new(
                user_data,
                result,
                if fin {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Operation(op),
                AppCqeData::Recv { buffer: index },
            )))
        }
        AppCqeKind::Closed { op } => Ok(Some(AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            op.map_or(AppObjectRef::None, AppObjectRef::Operation),
            AppCqeData::Closed,
        ))),
    }
}

#[inline]
fn cqe_from_descriptor(
    descriptor: AppCqeDescriptor,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppCqe {
    match descriptor.payload() {
        AppCqeData::None => AppCqe::new(descriptor.user_data(), AppCqeKind::Closed { op: None }),
        AppCqeData::Recv { buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered recv lease for completion descriptor");
            AppCqe::recv(
                descriptor.user_data(),
                op_from_completion_descriptor(descriptor),
                AppRecv::new(lease),
                descriptor.flags().contains(AppCqeFlags::FIN),
            )
        }
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
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppCompletionEntry {
    match descriptor.payload() {
        AppCqeData::Recv { buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered recv lease for completion entry");
            AppCompletionEntry::with_attachment(
                descriptor,
                AppRegisteredBuffer {
                    index: buffer,
                    lease,
                },
            )
        }
        _ => AppCompletionEntry::new(descriptor),
    }
}

impl AppSend {
    #[inline]
    pub fn new(lease: AppBufferLease) -> Self {
        Self { lease }
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        &self.lease
    }

    #[inline]
    pub fn into_lease(self) -> AppBufferLease {
        self.lease
    }

    #[inline]
    pub fn release(self) {
        self.lease.release();
    }

    #[inline]
    pub fn descriptor(
        &self,
        user_data: Option<AppUserData>,
        op: AppOpId,
    ) -> HammerResult<AppSqeDescriptor> {
        Ok(AppSqeDescriptor::new(
            AppOpcode::Send,
            user_data,
            AppObjectRef::Operation(op),
            AppSqeData::Send {
                buffer: self.lease.index(),
            },
        ))
    }
}
