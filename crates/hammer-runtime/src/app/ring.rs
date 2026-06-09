use std::cell::RefCell;
use std::future::poll_fn;
use std::net::{Shutdown, SocketAddr};
use std::ops::Deref;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::descriptor::Descriptor;
use hammer_infra::ring::{CompletionDescriptor, IndexedRing, RingEntry, SubmissionDescriptor};
use hammer_infra::vec::Vec;

use crate::app::context::AppFlowId;

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

pub enum AppSocketTag {}
pub type AppSocketId = Descriptor<AppSocketTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppOpcode {
    Nop,
    Accept,
    Recv,
    RecvFrom,
    Send,
    SendTo,
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
    Flow(AppFlowId),
    Socket(AppSocketId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppSqeData {
    Nop,
    Accept,
    Recv {
        max_len: u32,
    },
    RecvFrom {
        max_len: u32,
    },
    Send {
        buffer: BufferIndex,
    },
    SendTo {
        buffer: BufferIndex,
        target: SocketAddr,
    },
    Close,
}

pub type AppSqeDescriptor = SubmissionDescriptor<AppOpcode, AppUserData, AppObjectRef, AppSqeData>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppCqeFlags(u32);

impl AppCqeFlags {
    pub const NONE: Self = Self(0);
    pub const BUFFER: Self = Self(1 << 0);
    pub const FIN: Self = Self(1 << 1);
    pub const TRUNCATED: Self = Self(1 << 2);

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
    Accepted {
        listener: AppSocketId,
        flow: AppFlowId,
    },
    Recv {
        flow: AppFlowId,
        buffer: BufferIndex,
    },
    RecvFrom {
        socket: AppSocketId,
        source: SocketAddr,
        buffer: BufferIndex,
    },
    Closed {
        flow: Option<AppFlowId>,
        socket: Option<AppSocketId>,
    },
}

pub type AppCqeDescriptor =
    CompletionDescriptor<AppUserData, i32, AppCqeFlags, AppObjectRef, AppCqeData>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppTcpShutdown {
    flow: AppFlowId,
    how: Shutdown,
}

impl AppTcpShutdown {
    #[inline]
    pub const fn new(flow: AppFlowId, how: Shutdown) -> Self {
        Self { flow, how }
    }

    #[inline]
    pub const fn flow(self) -> AppFlowId {
        self.flow
    }

    #[inline]
    pub const fn how(self) -> Shutdown {
        self.how
    }
}

#[derive(Debug)]
pub enum AppSqe {
    Nop {
        user_data: AppUserData,
    },
    Accept {
        user_data: AppUserData,
        socket: AppSocketId,
    },
    Recv {
        user_data: AppUserData,
        flow: AppFlowId,
        max: usize,
    },
    RecvFrom {
        user_data: AppUserData,
        socket: AppSocketId,
        max: usize,
    },
    Send {
        user_data: AppUserData,
        flow: AppFlowId,
        send: AppSend,
    },
    SendTo {
        user_data: AppUserData,
        socket: AppSocketId,
        target: SocketAddr,
        send: AppSend,
    },
    CloseFlow {
        user_data: AppUserData,
        flow: AppFlowId,
    },
}

impl AppSqe {
    #[inline]
    pub const fn nop(user_data: AppUserData) -> Self {
        Self::Nop { user_data }
    }

    #[inline]
    pub const fn recv(user_data: AppUserData, flow: AppFlowId, max: usize) -> Self {
        Self::Recv {
            user_data,
            flow,
            max,
        }
    }

    #[inline]
    pub const fn recv_from(user_data: AppUserData, socket: AppSocketId, max: usize) -> Self {
        Self::RecvFrom {
            user_data,
            socket,
            max,
        }
    }

    #[inline]
    pub fn send(user_data: AppUserData, flow: AppFlowId, send: AppSend) -> Self {
        Self::Send {
            user_data,
            flow,
            send,
        }
    }

    #[inline]
    pub fn send_to(
        user_data: AppUserData,
        socket: AppSocketId,
        target: SocketAddr,
        send: AppSend,
    ) -> Self {
        Self::SendTo {
            user_data,
            socket,
            target,
            send,
        }
    }

    #[inline]
    pub const fn close_flow(user_data: AppUserData, flow: AppFlowId) -> Self {
        Self::CloseFlow { user_data, flow }
    }

    #[inline]
    pub const fn user_data(&self) -> AppUserData {
        match self {
            Self::Nop { user_data }
            | Self::Accept { user_data, .. }
            | Self::Recv { user_data, .. }
            | Self::RecvFrom { user_data, .. }
            | Self::Send { user_data, .. }
            | Self::SendTo { user_data, .. }
            | Self::CloseFlow { user_data, .. } => *user_data,
        }
    }

    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        match self {
            Self::Nop { .. } => AppOpcode::Nop,
            Self::Accept { .. } => AppOpcode::Accept,
            Self::Recv { .. } => AppOpcode::Recv,
            Self::RecvFrom { .. } => AppOpcode::RecvFrom,
            Self::Send { .. } => AppOpcode::Send,
            Self::SendTo { .. } => AppOpcode::SendTo,
            Self::CloseFlow { .. } => AppOpcode::Close,
        }
    }

    #[inline]
    pub const fn transport(&self) -> Option<TransportKind> {
        match self {
            Self::Accept { .. } | Self::Recv { .. } | Self::Send { .. } => Some(TransportKind::Tcp),
            Self::RecvFrom { .. } | Self::SendTo { .. } => Some(TransportKind::Udp),
            Self::CloseFlow { .. } => None,
            Self::Nop { .. } => None,
        }
    }

    #[inline]
    pub const fn flow(&self) -> Option<AppFlowId> {
        match self {
            Self::Recv { flow, .. } | Self::Send { flow, .. } => Some(*flow),
            Self::CloseFlow { flow, .. } => Some(*flow),
            _ => None,
        }
    }

    #[inline]
    pub const fn socket(&self) -> Option<AppSocketId> {
        match self {
            Self::Accept { socket, .. }
            | Self::RecvFrom { socket, .. }
            | Self::SendTo { socket, .. } => Some(*socket),
            _ => None,
        }
    }

    #[inline]
    pub const fn max(&self) -> Option<usize> {
        match self {
            Self::Recv { max, .. } | Self::RecvFrom { max, .. } => Some(*max),
            _ => None,
        }
    }

    #[inline]
    pub const fn target(&self) -> Option<SocketAddr> {
        match self {
            Self::SendTo { target, .. } => Some(*target),
            _ => None,
        }
    }

    #[inline]
    pub fn into_send(self) -> Option<AppSend> {
        match self {
            Self::Send { send, .. } | Self::SendTo { send, .. } => Some(send),
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
            Self::Accept { user_data, socket } => Ok(AppSqeDescriptor::new(
                AppOpcode::Accept,
                *user_data,
                AppObjectRef::Socket(*socket),
                AppSqeData::Accept,
            )),
            Self::Recv {
                user_data,
                flow,
                max,
            } => Ok(AppSqeDescriptor::new(
                AppOpcode::Recv,
                *user_data,
                AppObjectRef::Flow(*flow),
                AppSqeData::Recv {
                    max_len: *max as u32,
                },
            )),
            Self::RecvFrom {
                user_data,
                socket,
                max,
            } => Ok(AppSqeDescriptor::new(
                AppOpcode::RecvFrom,
                *user_data,
                AppObjectRef::Socket(*socket),
                AppSqeData::RecvFrom {
                    max_len: *max as u32,
                },
            )),
            Self::Send {
                user_data,
                flow,
                send,
            } => send.descriptor(*user_data, *flow),
            Self::SendTo {
                user_data,
                socket,
                target,
                send,
            } => Ok(AppSqeDescriptor::new(
                AppOpcode::SendTo,
                *user_data,
                AppObjectRef::Socket(*socket),
                AppSqeData::SendTo {
                    buffer: send.lease().index(),
                    target: *target,
                },
            )),
            Self::CloseFlow { user_data, flow } => Ok(AppSqeDescriptor::new(
                AppOpcode::Close,
                *user_data,
                AppObjectRef::Flow(*flow),
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

impl Drop for AppBufferLease {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.free_index(self.index);
        }
    }
}

#[derive(Debug)]
pub struct AppRecv {
    lease: AppBufferLease,
}

#[derive(Debug)]
pub enum AppCqeKind {
    Accepted {
        listener: AppSocketId,
        flow: AppFlowId,
    },
    Recv {
        flow: AppFlowId,
        recv: AppRecv,
        fin: bool,
    },
    RecvFrom {
        socket: AppSocketId,
        source: SocketAddr,
        recv: Option<AppRecv>,
        truncated: bool,
    },
    Closed {
        flow: Option<AppFlowId>,
        socket: Option<AppSocketId>,
    },
}

impl AppCqeKind {
    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        match self {
            Self::Accepted { .. } => AppOpcode::Accept,
            Self::Recv { .. } => AppOpcode::Recv,
            Self::RecvFrom { .. } => AppOpcode::RecvFrom,
            Self::Closed { .. } => AppOpcode::Close,
        }
    }

    #[inline]
    pub const fn transport(&self) -> Option<TransportKind> {
        match self {
            Self::Accepted { .. } | Self::Recv { .. } => Some(TransportKind::Tcp),
            Self::RecvFrom { .. } => Some(TransportKind::Udp),
            Self::Closed { flow, socket } => {
                if flow.is_some() {
                    Some(TransportKind::Tcp)
                } else if socket.is_some() {
                    Some(TransportKind::Udp)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct AppCqe {
    inner: AppCqeView,
}

#[derive(Debug)]
pub struct AppCqeView {
    user_data: AppUserData,
    kind: AppCqeKind,
}

impl AppCqe {
    #[inline]
    pub const fn new(user_data: AppUserData, kind: AppCqeKind) -> Self {
        Self {
            inner: AppCqeView { user_data, kind },
        }
    }

    #[inline]
    pub fn recv(user_data: AppUserData, flow: AppFlowId, recv: AppRecv, fin: bool) -> Self {
        Self::new(user_data, AppCqeKind::Recv { flow, recv, fin })
    }

    #[inline]
    pub fn recv_from(
        user_data: AppUserData,
        socket: AppSocketId,
        source: SocketAddr,
        recv: AppRecv,
        truncated: bool,
    ) -> Self {
        Self::new(
            user_data,
            AppCqeKind::RecvFrom {
                socket,
                source,
                recv: Some(recv),
                truncated,
            },
        )
    }

    #[inline]
    pub const fn accepted(user_data: AppUserData, listener: AppSocketId, flow: AppFlowId) -> Self {
        Self::new(user_data, AppCqeKind::Accepted { listener, flow })
    }

    #[inline]
    pub const fn closed(user_data: AppUserData, flow: Option<AppFlowId>) -> Self {
        Self::new(user_data, AppCqeKind::Closed { flow, socket: None })
    }

    #[inline]
    pub const fn user_data(&self) -> AppUserData {
        self.inner.user_data
    }

    #[inline]
    pub const fn opcode(&self) -> AppOpcode {
        self.inner.kind.opcode()
    }

    #[inline]
    pub const fn transport(&self) -> Option<TransportKind> {
        self.inner.kind.transport()
    }

    #[inline]
    pub fn kind(&self) -> &AppCqeKind {
        &self.inner.kind
    }

    #[inline]
    pub fn into_send(self) -> Option<AppSend> {
        match self.inner.kind {
            AppCqeKind::Recv { recv, .. } => Some(recv.into_send()),
            AppCqeKind::RecvFrom {
                recv: Some(recv), ..
            } => Some(recv.into_send()),
            _ => None,
        }
    }

    #[inline]
    pub fn into_recv(self) -> Option<AppRecv> {
        match self.inner.kind {
            AppCqeKind::Recv { recv, .. } => Some(recv),
            AppCqeKind::RecvFrom {
                recv: Some(recv), ..
            } => Some(recv),
            _ => None,
        }
    }

    #[inline]
    pub fn descriptor(&self) -> HammerResult<Option<AppCqeDescriptor>> {
        let descriptor = match self.kind() {
            AppCqeKind::Accepted { listener, flow } => Some(AppCqeDescriptor::new(
                self.user_data(),
                0,
                AppCqeFlags::NONE,
                AppObjectRef::Socket(*listener),
                AppCqeData::Accepted {
                    listener: *listener,
                    flow: *flow,
                },
            )),
            AppCqeKind::Recv { flow, recv, fin } => Some(AppCqeDescriptor::new(
                self.user_data(),
                recv.lease().current_len()? as i32,
                if *fin {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::FIN)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Flow(*flow),
                AppCqeData::Recv {
                    flow: *flow,
                    buffer: recv.lease().index(),
                },
            )),
            AppCqeKind::RecvFrom {
                socket,
                source,
                recv: Some(recv),
                truncated,
            } => Some(AppCqeDescriptor::new(
                self.user_data(),
                recv.lease().current_len()? as i32,
                if *truncated {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::TRUNCATED)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Socket(*socket),
                AppCqeData::RecvFrom {
                    socket: *socket,
                    source: *source,
                    buffer: recv.lease().index(),
                },
            )),
            AppCqeKind::RecvFrom { recv: None, .. } => None,
            AppCqeKind::Closed { flow, socket } => Some(AppCqeDescriptor::new(
                self.user_data(),
                0,
                AppCqeFlags::NONE,
                match (*flow, *socket) {
                    (Some(flow), _) => AppObjectRef::Flow(flow),
                    (None, Some(socket)) => AppObjectRef::Socket(socket),
                    (None, None) => AppObjectRef::None,
                },
                AppCqeData::Closed {
                    flow: *flow,
                    socket: *socket,
                },
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
            AppCqeKind::RecvFrom {
                recv: Some(recv), ..
            } => Some(recv),
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
    // Descriptors only carry buffer indices; the lease stays here until the
    // consumer explicitly resolves that index back into a zero-copy buffer view.
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
    user_data: AppUserData,
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
            AppOpcode::Accept | AppOpcode::Recv | AppOpcode::RecvFrom => {
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
    fn find_flow_recv(&self, flow: AppFlowId) -> Option<(usize, PendingSubmission)> {
        self.submissions
            .iter()
            .copied()
            .enumerate()
            .find(|(_, pending)| {
                pending.object == AppObjectRef::Flow(flow)
                    && pending.opcode == AppOpcode::Recv
                    && matches!(pending.payload, AppSqeData::Recv { .. })
            })
    }

    #[inline]
    fn find_socket_recv_from(&self, socket: AppSocketId) -> Option<(usize, PendingSubmission)> {
        self.submissions
            .iter()
            .copied()
            .enumerate()
            .find(|(_, pending)| {
                pending.object == AppObjectRef::Socket(socket)
                    && pending.opcode == AppOpcode::RecvFrom
                    && matches!(pending.payload, AppSqeData::RecvFrom { .. })
            })
    }

    #[inline]
    fn find_socket_accept(&self, socket: AppSocketId) -> Option<(usize, PendingSubmission)> {
        self.submissions
            .iter()
            .copied()
            .enumerate()
            .find(|(_, pending)| {
                pending.object == AppObjectRef::Socket(socket)
                    && pending.opcode == AppOpcode::Accept
                    && matches!(pending.payload, AppSqeData::Accept)
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
    tcp_shutdowns: Rc<RefCell<AppRingState<AppTcpShutdown>>>,
    buffers: Rc<RefCell<AppRingBufferRegistry>>,
    pending_submissions: Rc<RefCell<AppPendingSubmissionRegistry>>,
}

impl AppRingHandle {
    #[inline]
    pub fn new(submission_capacity: usize, completion_capacity: usize) -> Self {
        Self {
            submissions: Rc::new(RefCell::new(AppRingState::new(submission_capacity))),
            completions: Rc::new(RefCell::new(AppRingState::new(completion_capacity))),
            tcp_shutdowns: Rc::new(RefCell::new(AppRingState::new(submission_capacity))),
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
        let descriptor =
            std::future::poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await?;
        Some(sqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        let descriptor = poll_fn(|cx| self.completions.borrow_mut().poll_pop(cx)).await?;
        Some(cqe_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub fn try_push_tcp_shutdown(&self, shutdown: AppTcpShutdown) -> HammerResult<()> {
        self.tcp_shutdowns
            .borrow_mut()
            .try_push(shutdown)
            .map_err(|_| HammerError::internal("app tcp shutdown ring full"))
    }

    #[inline]
    pub async fn next_tcp_shutdown(&self) -> Option<AppTcpShutdown> {
        poll_fn(|cx| self.tcp_shutdowns.borrow_mut().poll_pop(cx)).await
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
        flow: AppFlowId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let (pending_index, pending) = {
            let pending = self.pending_submissions.borrow();
            pending.find_flow_recv(flow).ok_or_else(|| {
                HammerError::internal(format!(
                    "pending recv submission missing for flow {}",
                    flow.value()
                ))
            })?
        };
        self.try_complete_recv_pending(
            flow,
            pending_index,
            pending,
            AppBufferLease::from_buffer(buffers, index),
            fin,
        )
    }

    #[inline]
    pub(crate) fn try_complete_recv_lease(
        &self,
        flow: AppFlowId,
        lease: AppBufferLease,
        fin: bool,
    ) -> HammerResult<()> {
        let (pending_index, pending) = {
            let pending = self.pending_submissions.borrow();
            pending.find_flow_recv(flow).ok_or_else(|| {
                HammerError::internal(format!(
                    "pending recv submission missing for flow {}",
                    flow.value()
                ))
            })?
        };
        self.try_complete_recv_pending(flow, pending_index, pending, lease, fin)
    }

    #[inline]
    pub(crate) fn try_complete_recv_from_buffer(
        &self,
        socket: AppSocketId,
        source: SocketAddr,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        truncated: bool,
    ) -> HammerResult<()> {
        let (pending_index, pending) = {
            let pending = self.pending_submissions.borrow();
            pending.find_socket_recv_from(socket).ok_or_else(|| {
                HammerError::internal(format!(
                    "pending recv_from submission missing for socket {}",
                    socket.value()
                ))
            })?
        };
        self.try_complete_recv_from_pending(
            socket,
            source,
            pending_index,
            pending,
            AppBufferLease::from_buffer(buffers, index),
            truncated,
        )
    }

    #[inline]
    pub(crate) fn try_complete_accept(
        &self,
        listener: AppSocketId,
        flow: AppFlowId,
    ) -> HammerResult<()> {
        let (pending_index, pending) = {
            let pending = self.pending_submissions.borrow();
            pending.find_socket_accept(listener).ok_or_else(|| {
                HammerError::internal(format!(
                    "pending accept submission missing for listener {}",
                    listener.value()
                ))
            })?
        };
        let descriptor = AppCqeDescriptor::new(
            pending.user_data,
            0,
            AppCqeFlags::NONE,
            pending.object,
            AppCqeData::Accepted { listener, flow },
        );
        self.try_push_completion_descriptor(descriptor)?;
        self.pending_submissions
            .borrow_mut()
            .remove_at(pending_index);
        Ok(())
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        let descriptor = poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await?;
        Some(submission_entry_from_descriptor(descriptor, &self.buffers))
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        poll_fn(|cx| self.submissions.borrow_mut().poll_pop(cx)).await
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
        flow: AppFlowId,
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
                flow,
                buffer: registered.index(),
            },
        );
        self.try_push_completion_entry(AppCompletionEntry::with_attachment(
            descriptor, registered,
        ))?;
        self.pending_submissions
            .borrow_mut()
            .remove_at(pending_index);
        Ok(())
    }

    #[inline]
    fn try_complete_recv_from_pending(
        &self,
        socket: AppSocketId,
        source: SocketAddr,
        pending_index: usize,
        pending: PendingSubmission,
        lease: AppBufferLease,
        truncated: bool,
    ) -> HammerResult<()> {
        let registered = AppRegisteredBuffer::from_lease(lease)?;
        let descriptor = AppCqeDescriptor::new(
            pending.user_data,
            registered.lease().current_len()? as i32,
            if truncated {
                AppCqeFlags::BUFFER.union(AppCqeFlags::TRUNCATED)
            } else {
                AppCqeFlags::BUFFER
            },
            pending.object,
            AppCqeData::RecvFrom {
                socket,
                source,
                buffer: registered.index(),
            },
        );
        self.try_push_completion_entry(AppCompletionEntry::with_attachment(
            descriptor, registered,
        ))?;
        self.pending_submissions
            .borrow_mut()
            .remove_at(pending_index);
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
        AppSqe::Accept { user_data, socket } => Ok(AppSqeDescriptor::new(
            AppOpcode::Accept,
            user_data,
            AppObjectRef::Socket(socket),
            AppSqeData::Accept,
        )),
        AppSqe::Recv {
            user_data,
            flow,
            max,
        } => Ok(AppSqeDescriptor::new(
            AppOpcode::Recv,
            user_data,
            AppObjectRef::Flow(flow),
            AppSqeData::Recv {
                max_len: max as u32,
            },
        )),
        AppSqe::RecvFrom {
            user_data,
            socket,
            max,
        } => Ok(AppSqeDescriptor::new(
            AppOpcode::RecvFrom,
            user_data,
            AppObjectRef::Socket(socket),
            AppSqeData::RecvFrom {
                max_len: max as u32,
            },
        )),
        AppSqe::Send {
            user_data,
            flow,
            send,
        } => {
            let lease = send.into_lease();
            let index = lease.index();
            buffers.borrow_mut().register(index, lease);
            Ok(AppSqeDescriptor::new(
                AppOpcode::Send,
                user_data,
                AppObjectRef::Flow(flow),
                AppSqeData::Send { buffer: index },
            ))
        }
        AppSqe::SendTo {
            user_data,
            socket,
            target,
            send,
        } => {
            let lease = send.into_lease();
            let index = lease.index();
            buffers.borrow_mut().register(index, lease);
            Ok(AppSqeDescriptor::new(
                AppOpcode::SendTo,
                user_data,
                AppObjectRef::Socket(socket),
                AppSqeData::SendTo {
                    buffer: index,
                    target,
                },
            ))
        }
        AppSqe::CloseFlow { user_data, flow } => Ok(AppSqeDescriptor::new(
            AppOpcode::Close,
            user_data,
            AppObjectRef::Flow(flow),
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
        AppSqeData::Accept => AppSqe::Accept {
            user_data: descriptor.user_data(),
            socket: match descriptor.object() {
                AppObjectRef::Socket(socket) => socket,
                other => panic!("accept sqe expects socket object, got {other:?}"),
            },
        },
        AppSqeData::Recv { max_len } => AppSqe::recv(
            descriptor.user_data(),
            match descriptor.object() {
                AppObjectRef::Flow(flow) => flow,
                other => panic!("recv sqe expects flow object, got {other:?}"),
            },
            max_len as usize,
        ),
        AppSqeData::RecvFrom { max_len } => AppSqe::recv_from(
            descriptor.user_data(),
            match descriptor.object() {
                AppObjectRef::Socket(socket) => socket,
                other => panic!("recv_from sqe expects socket object, got {other:?}"),
            },
            max_len as usize,
        ),
        AppSqeData::Send { buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered send lease for submission descriptor");
            AppSqe::send(
                descriptor.user_data(),
                match descriptor.object() {
                    AppObjectRef::Flow(flow) => flow,
                    other => panic!("send sqe expects flow object, got {other:?}"),
                },
                AppSend::new(lease),
            )
        }
        AppSqeData::SendTo { buffer, target } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered send lease for send_to descriptor");
            AppSqe::send_to(
                descriptor.user_data(),
                match descriptor.object() {
                    AppObjectRef::Socket(socket) => socket,
                    other => panic!("send_to sqe expects socket object, got {other:?}"),
                },
                target,
                AppSend::new(lease),
            )
        }
        AppSqeData::Close => AppSqe::close_flow(
            descriptor.user_data(),
            match descriptor.object() {
                AppObjectRef::Flow(flow) => flow,
                other => panic!("close sqe expects flow object, got {other:?}"),
            },
        ),
    }
}

#[inline]
fn submission_entry_from_descriptor(
    descriptor: AppSqeDescriptor,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppSubmissionEntry {
    match descriptor.payload() {
        AppSqeData::Send { buffer } | AppSqeData::SendTo { buffer, .. } => {
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
        AppCqeKind::Accepted { listener, flow } => Ok(Some(AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            AppObjectRef::Socket(listener),
            AppCqeData::Accepted { listener, flow },
        ))),
        AppCqeKind::Recv { flow, recv, fin } => {
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
                AppObjectRef::Flow(flow),
                AppCqeData::Recv {
                    flow,
                    buffer: index,
                },
            )))
        }
        AppCqeKind::RecvFrom {
            socket,
            source,
            recv,
            truncated,
        } => {
            let Some(recv) = recv else {
                return Ok(None);
            };
            let lease = recv.into_lease();
            let index = lease.index();
            let result = lease.current_len()? as i32;
            buffers.borrow_mut().register(index, lease);
            Ok(Some(AppCqeDescriptor::new(
                user_data,
                result,
                if truncated {
                    AppCqeFlags::BUFFER.union(AppCqeFlags::TRUNCATED)
                } else {
                    AppCqeFlags::BUFFER
                },
                AppObjectRef::Socket(socket),
                AppCqeData::RecvFrom {
                    socket,
                    source,
                    buffer: index,
                },
            )))
        }
        AppCqeKind::Closed { flow, socket } => Ok(Some(AppCqeDescriptor::new(
            user_data,
            0,
            AppCqeFlags::NONE,
            match (flow, socket) {
                (Some(flow), _) => AppObjectRef::Flow(flow),
                (None, Some(socket)) => AppObjectRef::Socket(socket),
                (None, None) => AppObjectRef::None,
            },
            AppCqeData::Closed { flow, socket },
        ))),
    }
}

#[inline]
fn cqe_from_descriptor(
    descriptor: AppCqeDescriptor,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppCqe {
    match descriptor.payload() {
        AppCqeData::None => AppCqe::new(
            descriptor.user_data(),
            AppCqeKind::Closed {
                flow: None,
                socket: None,
            },
        ),
        AppCqeData::Accepted { listener, flow } => {
            AppCqe::accepted(descriptor.user_data(), listener, flow)
        }
        AppCqeData::Recv { flow, buffer } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered recv lease for completion descriptor");
            AppCqe::recv(
                descriptor.user_data(),
                flow,
                AppRecv::new(lease),
                descriptor.flags().contains(AppCqeFlags::FIN),
            )
        }
        AppCqeData::RecvFrom {
            socket,
            source,
            buffer,
        } => {
            let lease = buffers
                .borrow_mut()
                .take(buffer)
                .expect("registered recv lease for recv_from descriptor");
            AppCqe::recv_from(
                descriptor.user_data(),
                socket,
                source,
                AppRecv::new(lease),
                descriptor.flags().contains(AppCqeFlags::TRUNCATED),
            )
        }
        AppCqeData::Closed { flow, socket } => {
            AppCqe::new(descriptor.user_data(), AppCqeKind::Closed { flow, socket })
        }
    }
}

#[inline]
fn completion_entry_from_descriptor(
    descriptor: AppCqeDescriptor,
    buffers: &Rc<RefCell<AppRingBufferRegistry>>,
) -> AppCompletionEntry {
    match descriptor.payload() {
        AppCqeData::Recv { buffer, .. } | AppCqeData::RecvFrom { buffer, .. } => {
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
        user_data: AppUserData,
        flow: AppFlowId,
    ) -> HammerResult<AppSqeDescriptor> {
        Ok(AppSqeDescriptor::new(
            AppOpcode::Send,
            user_data,
            AppObjectRef::Flow(flow),
            AppSqeData::Send {
                buffer: self.lease.index(),
            },
        ))
    }
}
