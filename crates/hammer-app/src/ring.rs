use std::net::SocketAddr;

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;

use crate::{AppRecvFuture, AppRuntime};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppUserData {
    inner: hammer_runtime::app::AppUserData,
}

impl AppUserData {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self {
            inner: hammer_runtime::app::AppUserData::new(value),
        }
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.inner.value()
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppUserData {
        self.inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSocketId {
    inner: hammer_runtime::app::AppSocketId,
}

impl AppSocketId {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self {
            inner: hammer_runtime::app::AppSocketId::new(value),
        }
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.inner.value()
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.inner.slot()
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.inner.generation()
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppSocketId {
        self.inner
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppSocketId) -> Self {
        Self { inner }
    }
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

impl AppOpcode {
    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppOpcode {
        match self {
            Self::Nop => hammer_runtime::app::AppOpcode::Nop,
            Self::Accept => hammer_runtime::app::AppOpcode::Accept,
            Self::Recv => hammer_runtime::app::AppOpcode::Recv,
            Self::RecvFrom => hammer_runtime::app::AppOpcode::RecvFrom,
            Self::Send => hammer_runtime::app::AppOpcode::Send,
            Self::SendTo => hammer_runtime::app::AppOpcode::SendTo,
            Self::Close => hammer_runtime::app::AppOpcode::Close,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    Tcp,
    Udp,
}

#[repr(transparent)]
#[derive(Debug)]
pub struct AppBufferLease {
    inner: hammer_runtime::app::AppBufferLease,
}

impl AppBufferLease {
    #[inline]
    pub fn from_buffer(runtime: DataPlaneBuffers, index: BufferIndex) -> Self {
        Self {
            inner: hammer_runtime::app::AppBufferLease::from_buffer(runtime, index),
        }
    }

    #[inline]
    pub fn index(&self) -> BufferIndex {
        self.inner.index()
    }

    #[inline]
    pub fn current_ptr(&self) -> HammerResult<*const u8> {
        self.inner.current_ptr()
    }

    #[inline]
    pub fn current_mut_ptr(&self) -> HammerResult<*mut u8> {
        self.inner.current_mut_ptr()
    }

    #[inline]
    pub fn current_len(&self) -> HammerResult<usize> {
        self.inner.current_len()
    }

    #[inline]
    pub fn copy_current(&self) -> HammerResult<Vec<u8>> {
        self.inner.copy_current()
    }

    #[inline]
    pub fn runtime(&self) -> &DataPlaneBuffers {
        self.inner.runtime()
    }

    #[inline]
    pub fn release(self) {
        self.inner.release();
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppBufferLease) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_inner(self) -> hammer_runtime::app::AppBufferLease {
        self.inner
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct AppRegisteredBuffer {
    inner: hammer_runtime::app::AppRegisteredBuffer,
}

impl AppRegisteredBuffer {
    #[inline]
    pub fn from_lease(lease: AppBufferLease) -> HammerResult<Self> {
        Ok(Self {
            inner: hammer_runtime::app::AppRegisteredBuffer::from_lease(lease.into_inner())?,
        })
    }

    #[inline]
    pub fn index(&self) -> BufferIndex {
        self.inner.index()
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        // SAFETY: `AppBufferLease` is a transparent newtype over the runtime lease.
        unsafe { &*std::ptr::from_ref(self.inner.lease()).cast::<AppBufferLease>() }
    }

    #[inline]
    pub fn into_parts(self) -> (BufferIndex, AppBufferLease) {
        let (index, lease) = self.inner.into_parts();
        (index, AppBufferLease::from_inner(lease))
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppRegisteredBuffer) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_inner(self) -> hammer_runtime::app::AppRegisteredBuffer {
        self.inner
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct AppSubmissionEntry {
    inner: hammer_runtime::app::AppSubmissionEntry,
}

impl AppSubmissionEntry {
    #[inline]
    pub fn new(descriptor: AppSqeDescriptor) -> Self {
        Self {
            inner: hammer_runtime::app::AppSubmissionEntry::new(descriptor.into_inner()),
        }
    }

    #[inline]
    pub fn with_attachment(descriptor: AppSqeDescriptor, attachment: AppRegisteredBuffer) -> Self {
        Self {
            inner: hammer_runtime::app::AppSubmissionEntry::with_attachment(
                descriptor.into_inner(),
                attachment.into_inner(),
            ),
        }
    }

    #[inline]
    pub fn descriptor(&self) -> AppSqeDescriptor {
        AppSqeDescriptor::from_inner(*self.inner.descriptor())
    }

    #[inline]
    pub fn attachment(&self) -> Option<&AppRegisteredBuffer> {
        self.inner.attachment().map(|attachment| unsafe {
            // SAFETY: `AppRegisteredBuffer` is a transparent newtype over the runtime type.
            &*std::ptr::from_ref(attachment).cast::<AppRegisteredBuffer>()
        })
    }

    #[inline]
    pub fn into_parts(self) -> (AppSqeDescriptor, Option<AppRegisteredBuffer>) {
        let (descriptor, attachment) = self.inner.into_parts();
        (
            AppSqeDescriptor::from_inner(descriptor),
            attachment.map(AppRegisteredBuffer::from_inner),
        )
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppSubmissionEntry) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_inner(self) -> hammer_runtime::app::AppSubmissionEntry {
        self.inner
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct AppCompletionEntry {
    inner: hammer_runtime::app::AppCompletionEntry,
}

impl AppCompletionEntry {
    #[inline]
    pub fn new(descriptor: AppCqeDescriptor) -> Self {
        Self {
            inner: hammer_runtime::app::AppCompletionEntry::new(descriptor.into_inner()),
        }
    }

    #[inline]
    pub fn with_attachment(descriptor: AppCqeDescriptor, attachment: AppRegisteredBuffer) -> Self {
        Self {
            inner: hammer_runtime::app::AppCompletionEntry::with_attachment(
                descriptor.into_inner(),
                attachment.into_inner(),
            ),
        }
    }

    #[inline]
    pub fn descriptor(&self) -> AppCqeDescriptor {
        AppCqeDescriptor::from_inner(*self.inner.descriptor())
    }

    #[inline]
    pub fn attachment(&self) -> Option<&AppRegisteredBuffer> {
        self.inner.attachment().map(|attachment| unsafe {
            // SAFETY: `AppRegisteredBuffer` is a transparent newtype over the runtime type.
            &*std::ptr::from_ref(attachment).cast::<AppRegisteredBuffer>()
        })
    }

    #[inline]
    pub fn into_parts(self) -> (AppCqeDescriptor, Option<AppRegisteredBuffer>) {
        let (descriptor, attachment) = self.inner.into_parts();
        (
            AppCqeDescriptor::from_inner(descriptor),
            attachment.map(AppRegisteredBuffer::from_inner),
        )
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppCompletionEntry) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_inner(self) -> hammer_runtime::app::AppCompletionEntry {
        self.inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppObjectRef {
    None,
    Flow(crate::AppFlowId),
    Socket(AppSocketId),
}

impl AppObjectRef {
    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppObjectRef {
        match self {
            Self::None => hammer_runtime::app::AppObjectRef::None,
            Self::Flow(flow) => hammer_runtime::app::AppObjectRef::Flow(flow.into_inner()),
            Self::Socket(socket) => hammer_runtime::app::AppObjectRef::Socket(socket.into_inner()),
        }
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppObjectRef) -> Self {
        match inner {
            hammer_runtime::app::AppObjectRef::None => Self::None,
            hammer_runtime::app::AppObjectRef::Flow(flow) => {
                Self::Flow(crate::AppFlowId::new(flow.value()))
            }
            hammer_runtime::app::AppObjectRef::Socket(socket) => {
                Self::Socket(AppSocketId::from_inner(socket))
            }
        }
    }
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

impl AppSqeData {
    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppSqeData {
        match self {
            Self::Nop => hammer_runtime::app::AppSqeData::Nop,
            Self::Accept => hammer_runtime::app::AppSqeData::Accept,
            Self::Recv { max_len } => hammer_runtime::app::AppSqeData::Recv { max_len },
            Self::RecvFrom { max_len } => hammer_runtime::app::AppSqeData::RecvFrom { max_len },
            Self::Send { buffer } => hammer_runtime::app::AppSqeData::Send { buffer },
            Self::SendTo { buffer, target } => {
                hammer_runtime::app::AppSqeData::SendTo { buffer, target }
            }
            Self::Close => hammer_runtime::app::AppSqeData::Close,
        }
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppSqeData) -> Self {
        match inner {
            hammer_runtime::app::AppSqeData::Nop => Self::Nop,
            hammer_runtime::app::AppSqeData::Accept => Self::Accept,
            hammer_runtime::app::AppSqeData::Recv { max_len } => Self::Recv { max_len },
            hammer_runtime::app::AppSqeData::RecvFrom { max_len } => Self::RecvFrom { max_len },
            hammer_runtime::app::AppSqeData::Send { buffer } => Self::Send { buffer },
            hammer_runtime::app::AppSqeData::SendTo { buffer, target } => {
                Self::SendTo { buffer, target }
            }
            hammer_runtime::app::AppSqeData::Close => Self::Close,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSqeDescriptor {
    inner: hammer_runtime::app::AppSqeDescriptor,
}

impl AppSqeDescriptor {
    #[inline]
    pub fn new(
        opcode: AppOpcode,
        user_data: AppUserData,
        object: AppObjectRef,
        payload: AppSqeData,
    ) -> Self {
        Self {
            inner: hammer_runtime::app::AppSqeDescriptor::new(
                opcode.into_inner(),
                user_data.into_inner(),
                object.into_inner(),
                payload.into_inner(),
            ),
        }
    }

    #[inline]
    pub fn user_data(&self) -> AppUserData {
        AppUserData {
            inner: self.inner.user_data(),
        }
    }

    #[inline]
    pub fn opcode(&self) -> AppOpcode {
        match self.inner.opcode() {
            hammer_runtime::app::AppOpcode::Nop => AppOpcode::Nop,
            hammer_runtime::app::AppOpcode::Accept => AppOpcode::Accept,
            hammer_runtime::app::AppOpcode::Recv => AppOpcode::Recv,
            hammer_runtime::app::AppOpcode::RecvFrom => AppOpcode::RecvFrom,
            hammer_runtime::app::AppOpcode::Send => AppOpcode::Send,
            hammer_runtime::app::AppOpcode::SendTo => AppOpcode::SendTo,
            hammer_runtime::app::AppOpcode::Close => AppOpcode::Close,
        }
    }

    #[inline]
    pub fn object(&self) -> AppObjectRef {
        AppObjectRef::from_inner(self.inner.object())
    }

    #[inline]
    pub fn payload(&self) -> AppSqeData {
        AppSqeData::from_inner(self.inner.payload())
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppSqeDescriptor) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppSqeDescriptor {
        self.inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppCqeFlags {
    inner: hammer_runtime::app::AppCqeFlags,
}

impl AppCqeFlags {
    pub const NONE: Self = Self {
        inner: hammer_runtime::app::AppCqeFlags::NONE,
    };
    pub const BUFFER: Self = Self {
        inner: hammer_runtime::app::AppCqeFlags::BUFFER,
    };
    pub const FIN: Self = Self {
        inner: hammer_runtime::app::AppCqeFlags::FIN,
    };
    pub const TRUNCATED: Self = Self {
        inner: hammer_runtime::app::AppCqeFlags::TRUNCATED,
    };

    #[inline]
    pub const fn bits(self) -> u32 {
        self.inner.bits()
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.inner.contains(other.inner)
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppCqeFlags) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppCqeFlags {
        self.inner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppCqeData {
    None,
    Accepted {
        listener: AppSocketId,
        flow: crate::AppFlowId,
    },
    Recv {
        flow: crate::AppFlowId,
        buffer: BufferIndex,
    },
    RecvFrom {
        socket: AppSocketId,
        source: SocketAddr,
        buffer: BufferIndex,
    },
    Closed {
        flow: Option<crate::AppFlowId>,
        socket: Option<AppSocketId>,
    },
}

impl AppCqeData {
    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppCqeData) -> Self {
        match inner {
            hammer_runtime::app::AppCqeData::None => Self::None,
            hammer_runtime::app::AppCqeData::Accepted { listener, flow } => Self::Accepted {
                listener: AppSocketId::from_inner(listener),
                flow: crate::AppFlowId::new(flow.value()),
            },
            hammer_runtime::app::AppCqeData::Recv { flow, buffer } => Self::Recv {
                flow: crate::AppFlowId::new(flow.value()),
                buffer,
            },
            hammer_runtime::app::AppCqeData::RecvFrom {
                socket,
                source,
                buffer,
            } => Self::RecvFrom {
                socket: AppSocketId::from_inner(socket),
                source,
                buffer,
            },
            hammer_runtime::app::AppCqeData::Closed { flow, socket } => Self::Closed {
                flow: match flow {
                    Some(flow) => Some(crate::AppFlowId::new(flow.value())),
                    None => None,
                },
                socket: match socket {
                    Some(socket) => Some(AppSocketId::from_inner(socket)),
                    None => None,
                },
            },
        }
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppCqeData {
        match self {
            Self::None => hammer_runtime::app::AppCqeData::None,
            Self::Accepted { listener, flow } => hammer_runtime::app::AppCqeData::Accepted {
                listener: listener.into_inner(),
                flow: flow.into_inner(),
            },
            Self::Recv { flow, buffer } => hammer_runtime::app::AppCqeData::Recv {
                flow: flow.into_inner(),
                buffer,
            },
            Self::RecvFrom {
                socket,
                source,
                buffer,
            } => hammer_runtime::app::AppCqeData::RecvFrom {
                socket: socket.into_inner(),
                source,
                buffer,
            },
            Self::Closed { flow, socket } => hammer_runtime::app::AppCqeData::Closed {
                flow: match flow {
                    Some(flow) => Some(flow.into_inner()),
                    None => None,
                },
                socket: match socket {
                    Some(socket) => Some(socket.into_inner()),
                    None => None,
                },
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppCqeDescriptor {
    inner: hammer_runtime::app::AppCqeDescriptor,
}

impl AppCqeDescriptor {
    #[inline]
    pub fn new(
        user_data: AppUserData,
        result: i32,
        flags: AppCqeFlags,
        object: AppObjectRef,
        payload: AppCqeData,
    ) -> Self {
        Self {
            inner: hammer_runtime::app::AppCqeDescriptor::new(
                user_data.into_inner(),
                result,
                flags.into_inner(),
                object.into_inner(),
                payload.into_inner(),
            ),
        }
    }

    #[inline]
    pub fn user_data(&self) -> AppUserData {
        AppUserData {
            inner: self.inner.user_data(),
        }
    }

    #[inline]
    pub fn result(&self) -> i32 {
        self.inner.result()
    }

    #[inline]
    pub fn flags(&self) -> AppCqeFlags {
        AppCqeFlags::from_inner(self.inner.flags())
    }

    #[inline]
    pub fn object(&self) -> AppObjectRef {
        AppObjectRef::from_inner(self.inner.object())
    }

    #[inline]
    pub fn payload(&self) -> AppCqeData {
        AppCqeData::from_inner(self.inner.payload())
    }

    #[inline]
    pub(crate) const fn from_inner(inner: hammer_runtime::app::AppCqeDescriptor) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppCqeDescriptor {
        self.inner
    }
}

#[derive(Debug)]
pub struct AppRecv {
    inner: hammer_runtime::app::AppRecv,
}

impl AppRecv {
    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppRecv) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        // SAFETY: `AppBufferLease` is a transparent newtype over the runtime lease.
        unsafe { &*std::ptr::from_ref(self.inner.lease()).cast::<AppBufferLease>() }
    }

    #[inline]
    pub fn into_send(self) -> AppSend {
        AppSend::from_inner(self.inner.into_send())
    }

    #[inline]
    pub fn into_lease(self) -> AppBufferLease {
        AppBufferLease::from_inner(self.inner.into_lease())
    }

    #[inline]
    pub fn release(self) {
        self.inner.release();
    }
}

#[derive(Debug)]
pub struct AppSend {
    inner: hammer_runtime::app::AppSend,
}

impl AppSend {
    #[inline]
    pub fn new(lease: AppBufferLease) -> Self {
        Self {
            inner: hammer_runtime::app::AppSend::new(lease.into_inner()),
        }
    }

    #[inline]
    pub fn lease(&self) -> &AppBufferLease {
        // SAFETY: `AppBufferLease` is a transparent newtype over the runtime lease.
        unsafe { &*std::ptr::from_ref(self.inner.lease()).cast::<AppBufferLease>() }
    }

    #[inline]
    pub fn into_lease(self) -> AppBufferLease {
        AppBufferLease::from_inner(self.inner.into_lease())
    }

    #[inline]
    pub fn release(self) {
        self.inner.release();
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppSend) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_inner(self) -> hammer_runtime::app::AppSend {
        self.inner
    }
}

#[derive(Clone)]
pub struct AppRing {
    inner: AppRuntime,
}

impl AppRing {
    #[inline]
    pub fn new(inner: AppRuntime) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn runtime(&self) -> &AppRuntime {
        &self.inner
    }

    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        self.inner.recv()
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.inner.send(send).await
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_submission_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.inner.try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner.next_submission_descriptor().await
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner.next_submission_entry().await
    }

    #[inline]
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.inner.take_submission_buffer(index)
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, descriptor: AppCqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_completion_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.inner.try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.inner.next_completion_descriptor().await
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.inner.next_completion_entry().await
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.inner.take_completion_buffer(index)
    }
}
