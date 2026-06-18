use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo::FifoQueue;
use hammer_infra::map::FlatHashTable;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRingHandle, AppSendData, AppSqeData,
    AppSqeDescriptor,
};

use crate::session::{SessionId, SessionReadyQueue};

const UNBOUND_SESSION: SessionId = SessionId::new(u64::MAX);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionAppCloseSubmission {
    session_id: SessionId,
    op: AppOpId,
}

impl SessionAppCloseSubmission {
    #[inline]
    pub(crate) const fn new(session_id: SessionId, op: AppOpId) -> Self {
        Self { session_id, op }
    }

    #[inline]
    pub(crate) const fn session_id(self) -> SessionId {
        self.session_id
    }

    #[inline]
    #[cfg(test)]
    pub(crate) const fn op(self) -> AppOpId {
        self.op
    }
}

#[derive(Debug)]
struct SessionAppTxProgress {
    send: AppSendData,
    sent_len: usize,
}

impl SessionAppTxProgress {
    #[inline]
    fn new(send: AppSendData) -> Self {
        Self { send, sent_len: 0 }
    }

    #[inline]
    fn remaining_len(&self) -> CoreResult<usize> {
        let total = self.send.len()?;
        Ok(total.saturating_sub(self.sent_len))
    }

    #[inline]
    fn copy_pending_bytes(&self, max_len: usize) -> CoreResult<hammer_infra::vec::Vec<u8>> {
        let len = self.remaining_len()?.min(max_len);
        self.send.copy_range(self.sent_len, len)
    }

    #[inline]
    fn commit_bytes(&mut self, len: usize) -> CoreResult<bool> {
        let remaining = self.remaining_len()?;
        if len > remaining {
            return Err(CoreError::internal(
                "session app tx commit exceeds remaining length",
            ));
        }
        self.sent_len += len;
        Ok(self.sent_len >= self.send.len()?)
    }

    #[inline]
    fn finish(self) {
        self.send.release();
    }
}

type SessionAppTxQueue = FifoQueue<SessionAppTxProgress>;

#[derive(Debug)]
pub struct SessionAppRuntime {
    ring: Option<AppRingHandle>,
    session_slots: FlatHashTable<u64, SessionId>,
    pending_sends: Pool<SessionAppTxQueue>,
    pending_send_queues: FlatHashTable<u64, PoolIndex>,
    ready_send_sessions: SessionReadyQueue,
    drained_closes: hammer_infra::vec::Vec<SessionAppCloseSubmission>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            ring: None,
            session_slots: FlatHashTable::new(),
            pending_sends: Pool::with_capacity(1024),
            pending_send_queues: FlatHashTable::new(),
            ready_send_sessions: SessionReadyQueue::new(),
            drained_closes: hammer_infra::vec::Vec::new(),
        }
    }

    #[inline]
    pub fn bind_ring(&mut self, session_id: SessionId, op: AppOpId, ring: AppRingHandle) {
        self.ring.get_or_insert(ring);
        self.session_slots.insert(op.value(), session_id);
    }

    #[inline]
    pub fn unbind_ring(&mut self, op: AppOpId) -> Option<AppRingHandle> {
        self.session_slots.lookup(&op.value())?;
        self.session_slots.insert(op.value(), UNBOUND_SESSION);
        self.ring.clone()
    }

    #[inline]
    pub fn complete_recv(
        &self,
        op: AppOpId,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: hammer_adapter::BufferIndex,
        fin: bool,
    ) -> CoreResult<()> {
        let Some(ring) = self.ring.as_ref() else {
            buffers.free_index(index);
            return Ok(());
        };
        ring.try_complete_recv_buffer(op, buffers, index, fin)
    }

    #[inline]
    pub fn complete_closed(&self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.ring.as_ref() else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::closed(None, Some(op)))
    }

    #[inline]
    pub fn complete_connected(&self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.ring.as_ref() else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::connected(None, op))
    }

    pub fn drain_submissions(&mut self) -> CoreResult<()> {
        let Some(ring) = self.ring.clone() else {
            return Ok(());
        };
        while let Some(descriptor) = ring.pop_submission_descriptor() {
            self.handle_submission_descriptor(descriptor)?;
        }
        Ok(())
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn take_drained_closes(
        &mut self,
    ) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        self.drained_closes.drain(..).collect()
    }

    #[inline]
    pub(crate) fn push_pending_send(&mut self, session_id: SessionId, send: AppSendData) {
        let key = session_id.get();
        let progress = SessionAppTxProgress::new(send);
        match self.pending_send_queues.lookup(&key) {
            Some(index) => {
                self.pending_sends
                    .get_mut(index)
                    .expect("pending send queue index is valid")
                    .push_back(progress);
            }
            None => {
                let mut queue = FifoQueue::new();
                queue.push_back(progress);
                let index = self
                    .pending_sends
                    .insert(queue)
                    .expect("session app pending send queue pool capacity exhausted");
                self.pending_send_queues.insert(key, index);
                self.ready_send_sessions.mark_ready(session_id);
            }
        }
    }

    pub(crate) fn copy_pending_send_bytes(
        &self,
        session_id: SessionId,
        max_len: usize,
    ) -> CoreResult<Option<hammer_infra::vec::Vec<u8>>> {
        let Some(queue) = self.pending_queue(session_id) else {
            return Ok(None);
        };
        let Some(send) = queue.front() else {
            return Ok(None);
        };
        Ok(Some(send.copy_pending_bytes(max_len)?))
    }

    pub(crate) fn commit_pending_send_bytes(
        &mut self,
        session_id: SessionId,
        len: usize,
    ) -> CoreResult<bool> {
        let key = session_id.get();
        let index = self
            .pending_send_queues
            .lookup(&key)
            .ok_or_else(|| CoreError::internal("session app tx queue is missing"))?;
        let completed = {
            let queue = self
                .pending_sends
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session app tx queue index is invalid"))?;
            let send = queue
                .front_mut()
                .ok_or_else(|| CoreError::internal("session app tx progress is missing"))?;
            send.commit_bytes(len)?
        };
        if completed {
            let queue = self
                .pending_sends
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session app tx queue index is invalid"))?;
            let send = queue
                .pop_front()
                .ok_or_else(|| CoreError::internal("session app tx progress is missing"))?;
            send.finish();
            if queue.is_empty() {
                self.pending_send_queues.remove(&key);
                self.pending_sends.remove(index);
            }
        }
        Ok(completed)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_queue(session_id)
            .is_some_and(|queue| !queue.is_empty())
    }

    pub(crate) fn pending_send_len(&self, session_id: SessionId) -> CoreResult<Option<usize>> {
        let Some(queue) = self.pending_queue(session_id) else {
            return Ok(None);
        };
        let Some(send) = queue.front() else {
            return Ok(None);
        };
        Ok(Some(send.remaining_len()?))
    }

    pub(crate) fn take_ready_send_sessions(&mut self, out: &mut hammer_infra::vec::Vec<SessionId>) {
        for session_id in self.ready_send_sessions.take_ready_sessions() {
            out.push(session_id);
        }
    }

    #[inline]
    pub(crate) fn pending_closes(&self) -> &[SessionAppCloseSubmission] {
        self.drained_closes.as_slice()
    }

    fn handle_submission_descriptor(&mut self, descriptor: AppSqeDescriptor) -> CoreResult<()> {
        match descriptor.opcode() {
            AppOpcode::Send => {
                if let AppSqeData::Send { data } = descriptor.payload() {
                    let op = app_op_from_descriptor(descriptor);
                    let Some(session_id) = self.session_for_op(op) else {
                        return Ok(());
                    };
                    if let Some(ring) = self.ring.clone() {
                        let send: AppSendData = ring.send_from_data(data).try_into()?;
                        self.push_pending_send(session_id, send);
                    }
                }
            }
            AppOpcode::Close => {
                let op = app_op_from_descriptor(descriptor);
                if let Some(session_id) = self.session_for_op(op) {
                    self.drained_closes
                        .push(SessionAppCloseSubmission::new(session_id, op));
                }
            }
            AppOpcode::Recv | AppOpcode::Nop => {}
        }
        Ok(())
    }

    fn pending_queue(&self, session_id: SessionId) -> Option<&SessionAppTxQueue> {
        let index = self.pending_send_queues.lookup(&session_id.get())?;
        self.pending_sends.get(index)
    }

    #[inline]
    fn session_for_op(&self, op: AppOpId) -> Option<SessionId> {
        self.session_slots
            .lookup(&op.value())
            .filter(|session_id| *session_id != UNBOUND_SESSION)
    }
}

impl Default for SessionAppRuntime {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn app_op_from_descriptor(descriptor: AppSqeDescriptor) -> AppOpId {
    match descriptor.object() {
        AppObjectRef::Operation(op) => op,
        other => panic!("session app submission expects operation object, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::app::{AppRingHandle, AppSendData};

    #[test]
    fn pending_send_progress_is_committed_by_session_id() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let first: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"first").expect("first"))
            .try_into()
            .expect("first transfer");
        let second: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"second").expect("second"))
            .try_into()
            .expect("second transfer");

        let mut app = SessionAppRuntime::new();
        let session_a = SessionId::new(10);
        let session_b = SessionId::new(20);

        app.push_pending_send(session_a, first);
        app.push_pending_send(session_b, second);

        let first_bytes = app
            .copy_pending_send_bytes(session_a, 8)
            .expect("copy first")
            .expect("first pending");
        assert_eq!(first_bytes.as_slice(), b"first");
        assert!(
            app.commit_pending_send_bytes(session_a, 5)
                .expect("commit first")
        );
        assert!(!app.has_pending_send(session_a));
        assert!(app.has_pending_send(session_b));
    }

    #[test]
    fn pending_send_progress_preserves_second_session_after_first_completes() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let first: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"first").expect("first"))
            .try_into()
            .expect("first transfer");
        let second: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"second").expect("second"))
            .try_into()
            .expect("second transfer");
        let mut app = SessionAppRuntime::new();
        let session_a = SessionId::new(10);
        let session_b = SessionId::new(20);

        app.push_pending_send(session_a, first);
        app.push_pending_send(session_b, second);

        assert!(
            app.commit_pending_send_bytes(session_a, 5)
                .expect("commit first")
        );
        assert!(!app.has_pending_send(session_a));
        assert_eq!(
            app.copy_pending_send_bytes(session_b, 8)
                .expect("copy second")
                .expect("second pending")
                .as_slice(),
            b"second"
        );
    }

    #[test]
    fn pending_sends_for_same_session_are_fifo() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let first: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"first").expect("first"))
            .try_into()
            .expect("first transfer");
        let second: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"second").expect("second"))
            .try_into()
            .expect("second transfer");
        let mut app = SessionAppRuntime::new();
        let session = SessionId::new(30);

        app.push_pending_send(session, first);
        app.push_pending_send(session, second);

        assert_eq!(
            app.copy_pending_send_bytes(session, 16)
                .expect("copy first")
                .expect("first pending")
                .as_slice(),
            b"first"
        );
        assert!(
            app.commit_pending_send_bytes(session, 5)
                .expect("commit first")
        );
        assert_eq!(
            app.copy_pending_send_bytes(session, 16)
                .expect("copy second")
                .expect("second pending")
                .as_slice(),
            b"second"
        );
    }

    #[test]
    fn poll_ready_send_sessions_returns_only_newly_pending_sessions() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"bytes").expect("bytes"))
            .try_into()
            .expect("transfer");
        let mut app = SessionAppRuntime::new();
        let session = SessionId::new(40);
        let mut ready = hammer_infra::vec::Vec::new();

        app.push_pending_send(session, send);
        app.take_ready_send_sessions(&mut ready);
        assert_eq!(ready.as_slice(), &[session]);

        ready.clear();
        app.take_ready_send_sessions(&mut ready);
        assert!(ready.is_empty());
    }

    #[test]
    fn unfinished_send_tracks_progress_without_exposing_entry() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
            .try_into()
            .expect("transfer");
        let mut app = SessionAppRuntime::new();
        let session_id = SessionId::new(1);

        app.push_pending_send(session_id, send);

        let first = app
            .copy_pending_send_bytes(session_id, 4)
            .expect("copy first")
            .expect("pending");
        assert_eq!(first.as_slice(), b"abcd");
        assert!(
            !app.commit_pending_send_bytes(session_id, 4)
                .expect("partial")
        );

        let second = app
            .copy_pending_send_bytes(session_id, 8)
            .expect("copy second")
            .expect("pending after partial");
        assert_eq!(second.as_slice(), b"ef");
        assert!(
            app.commit_pending_send_bytes(session_id, 2)
                .expect("finish")
        );
        assert!(!app.has_pending_send(session_id));
    }
}
