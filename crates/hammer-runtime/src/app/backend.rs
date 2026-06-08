use hammer_adapter::BufferIndex;
use hammer_core::error::HammerResult;

use crate::app::context::AppFlowId;
use crate::app::ring::{
    AppBufferLease, AppCompletionEntry, AppCqe, AppCqeDescriptor, AppRecv, AppRingHandle, AppSend,
    AppSqe, AppSqeDescriptor, AppSubmissionEntry,
};

#[derive(Clone, Debug)]
pub struct AppBackendRecvQueue {
    ring: AppRingHandle,
}

impl AppBackendRecvQueue {
    #[inline]
    pub(crate) fn new(ring: AppRingHandle) -> Self {
        Self { ring }
    }

    #[inline]
    pub async fn push(&self, cqe: AppCqe) -> HammerResult<()> {
        self.try_push(cqe)
    }

    #[inline]
    pub fn try_push(&self, cqe: AppCqe) -> HammerResult<()> {
        self.ring.try_push_completion(cqe)
    }

    #[inline]
    pub async fn next(&self) -> AppCqe {
        self.ring
            .next_completion()
            .await
            .expect("app completion ring should remain open")
    }

    #[inline]
    pub(crate) fn ring(&self) -> AppRingHandle {
        self.ring.clone()
    }
}

#[derive(Clone, Debug)]
pub struct AppBackendSendQueue {
    ring: AppRingHandle,
}

impl AppBackendSendQueue {
    #[inline]
    pub(crate) fn new(ring: AppRingHandle) -> Self {
        Self { ring }
    }

    #[inline]
    pub async fn push(&self, sqe: AppSqe) -> HammerResult<()> {
        self.try_push(sqe)
    }

    #[inline]
    pub fn try_push(&self, sqe: AppSqe) -> HammerResult<()> {
        self.ring.try_push_submission(sqe)
    }

    #[inline]
    pub async fn next(&self) -> AppSqe {
        self.ring
            .next_submission()
            .await
            .expect("app submission ring should remain open")
    }

    #[inline]
    pub(crate) fn ring(&self) -> AppRingHandle {
        self.ring.clone()
    }
}

#[derive(Clone, Debug)]
pub struct AppBackend {
    flow: AppFlowId,
    ring: AppRingHandle,
    recv: AppBackendRecvQueue,
    send: AppBackendSendQueue,
}

impl AppBackend {
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self::with_flow(capacity, AppFlowId::new(0))
    }

    #[inline]
    pub(crate) fn with_flow(capacity: usize, flow: AppFlowId) -> Self {
        let ring = AppRingHandle::new(capacity, capacity);
        Self {
            flow,
            recv: AppBackendRecvQueue::new(ring.clone()),
            send: AppBackendSendQueue::new(ring.clone()),
            ring,
        }
    }

    #[inline]
    pub async fn complete_recv(&self, lease: AppBufferLease) -> HammerResult<()> {
        self.try_complete_recv(lease)
    }

    #[inline]
    pub fn try_complete_recv(&self, lease: AppBufferLease) -> HammerResult<()> {
        self.ring.try_complete_recv_lease(self.flow, lease, false)
    }

    #[inline]
    pub async fn next_send(&self) -> Option<AppSend> {
        self.next_sqe().await.and_then(AppSqe::into_send)
    }

    #[inline]
    pub fn try_push_sqe_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()> {
        self.ring.try_push_submission_descriptor(sqe)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.ring.try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_sqe_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.ring.next_submission_descriptor().await
    }

    #[inline]
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.ring.take_send_buffer(index)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.ring.next_submission_entry().await
    }

    #[inline]
    pub async fn push_cqe(&self, cqe: AppCqe) -> HammerResult<()> {
        self.recv.push(cqe).await
    }

    #[inline]
    pub fn try_push_cqe(&self, cqe: AppCqe) -> HammerResult<()> {
        self.recv.try_push(cqe)
    }

    #[inline]
    pub async fn next_cqe(&self) -> AppCqe {
        self.recv.next().await
    }

    #[inline]
    pub fn try_push_cqe_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.ring.try_push_completion_descriptor(cqe)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.ring.try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_cqe_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.ring.next_completion_descriptor().await
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.ring.take_recv_buffer(index)
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.ring.next_completion_entry().await
    }

    #[inline]
    pub async fn push_sqe(&self, sqe: AppSqe) -> HammerResult<()> {
        self.send.push(sqe).await
    }

    #[inline]
    pub fn try_push_sqe(&self, sqe: AppSqe) -> HammerResult<()> {
        self.send.try_push(sqe)
    }

    #[inline]
    pub async fn next_sqe(&self) -> Option<AppSqe> {
        Some(self.send.next().await)
    }

    #[inline]
    pub fn flow(&self) -> AppFlowId {
        self.flow
    }

    #[inline]
    pub fn ring_handle(&self) -> AppRingHandle {
        self.ring.clone()
    }

    #[inline]
    pub(crate) fn recv_queue(&self) -> AppBackendRecvQueue {
        self.recv.clone()
    }

    #[inline]
    pub(crate) fn send_queue(&self) -> AppBackendSendQueue {
        self.send.clone()
    }
}
