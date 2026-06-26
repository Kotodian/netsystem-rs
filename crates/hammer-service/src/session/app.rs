use crossbeam_utils::CachePadded;
use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRingHandle, AppSendData, AppSqeData,
    AppSqeDescriptor,
};

use crate::session::{SessionId, SessionReadyQueue};

const UNBOUND_SESSION: SessionId = SessionId::new(u64::MAX);
const DEFAULT_APP_SESSION_CAPACITY: usize = 1024;

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
}

#[derive(Debug)]
struct SessionAppRuntimeHot {
    buffers: DataPlaneBuffers,
    pending_sends: FlatHashTable<u64, BufferIndex>,
    tx_ready_sessions: SessionReadyQueue,
    ready_sessions: SessionReadyQueue,
}

#[derive(Debug)]
struct SessionAppRuntimeControl {
    ring: Option<AppRingHandle>,
    session_slots: FlatHashTable<u64, SessionId>,
    drained_closes: hammer_infra::vec::Vec<SessionAppCloseSubmission>,
}

#[derive(Debug)]
pub struct SessionAppRuntime {
    hot: CachePadded<SessionAppRuntimeHot>,
    control: CachePadded<SessionAppRuntimeControl>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new(buffers: DataPlaneBuffers) -> Self {
        Self {
            hot: CachePadded::new(SessionAppRuntimeHot {
                buffers,
                pending_sends: FlatHashTable::with_capacity(DEFAULT_APP_SESSION_CAPACITY),
                tx_ready_sessions: SessionReadyQueue::new(),
                ready_sessions: SessionReadyQueue::new(),
            }),
            control: CachePadded::new(SessionAppRuntimeControl {
                ring: None,
                session_slots: FlatHashTable::with_capacity(DEFAULT_APP_SESSION_CAPACITY),
                drained_closes: hammer_infra::vec::Vec::new(),
            }),
        }
    }

    #[inline]
    pub fn bind_ring(&mut self, session_id: SessionId, op: AppOpId, ring: AppRingHandle) {
        self.control.ring.get_or_insert(ring);
        self.control.session_slots.insert(op.value(), session_id);
    }

    #[inline]
    pub fn set_ring(&mut self, ring: AppRingHandle) {
        self.control.ring.get_or_insert(ring);
    }

    #[inline]
    pub fn unbind_ring(&mut self, op: AppOpId) -> Option<AppRingHandle> {
        self.control.session_slots.lookup(&op.value())?;
        self.control
            .session_slots
            .insert(op.value(), UNBOUND_SESSION);
        self.control.ring.clone()
    }

    #[inline]
    pub fn complete_recv(
        &self,
        op: AppOpId,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: hammer_adapter::BufferIndex,
        fin: bool,
    ) -> CoreResult<bool> {
        let Some(ring) = self.control.ring.as_ref() else {
            buffers.free_index(index);
            return Ok(true);
        };
        ring.try_complete_recv_buffer(op, buffers, index, fin)
    }

    #[inline]
    pub fn complete_closed(&self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.control.ring.as_ref() else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::closed(None, Some(op)))
    }

    #[inline]
    pub fn complete_connected(&self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.control.ring.as_ref() else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::connected(None, op))
    }

    pub fn drain_submissions(&mut self) -> CoreResult<()> {
        let Some(ring) = self.control.ring.clone() else {
            return Ok(());
        };
        // Drain in batches of 64 to amortise atomic head/tail traffic on the
        // MPMC submission ring. The buffer lives on the stack so we avoid
        // per-iteration allocation. `pop_submission_batch` issues L1
        // prefetches for the next slot inside `LockFreeRing::dequeue_batch`.
        let mut batch: [AppSqeDescriptor; 64] =
            [AppSqeDescriptor::new(AppOpcode::Nop, None, AppObjectRef::None, AppSqeData::Nop); 64];
        loop {
            let taken = ring.pop_submission_batch(&mut batch);
            if taken == 0 {
                break;
            }
            for descriptor in batch[..taken].iter().copied() {
                self.handle_submission_descriptor(descriptor)?;
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn take_drained_closes(
        &mut self,
    ) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        self.control.drained_closes.drain(..).collect()
    }

    #[inline]
    pub(crate) fn push_pending_send(&mut self, session_id: SessionId, send: AppSendData) {
        let key = session_id.get();
        let head = self
            .copy_send_into_session_chain(&send)
            .expect("session app send copy into session chain");
        send.release();
        if let Some(existing) = self.hot.pending_sends.lookup(&key) {
            self.buffers()
                .append_existing_chain(existing, head)
                .expect("session app append pending send chain");
        } else {
            self.hot.pending_sends.insert(key, head);
        }
        self.hot.tx_ready_sessions.mark_ready(session_id);
    }

    pub(crate) fn release_pending_send_bytes(
        &mut self,
        session_id: SessionId,
        len: usize,
    ) -> CoreResult<bool> {
        if len == 0 {
            return Ok(false);
        }
        let key = session_id.get();
        let head = self
            .hot
            .pending_sends
            .lookup(&key)
            .ok_or_else(|| CoreError::internal("session app tx chain is missing"))?;
        let buffers = self.buffers();
        let total_len = buffers
            .current_len(head)?
            .checked_add(buffers.total_len_not_including_first(head)?)
            .ok_or_else(|| CoreError::internal("session app tx chain length overflow"))?;
        if len > total_len {
            return Err(CoreError::internal(
                "session app tx release exceeds pending length",
            ));
        }
        if len == total_len {
            let _ = buffers;
            self.hot.pending_sends.remove(&key);
            self.buffers().free_index(head);
            return Ok(true);
        }
        let mut buffer = buffers.get_buffer_mut(head)?;
        buffer.advance(len as isize)?;
        Ok(false)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.hot.pending_sends.lookup(&session_id.get()).is_some()
    }

    pub(crate) fn pending_send_len(&self, session_id: SessionId) -> CoreResult<Option<usize>> {
        let Some(head) = self.hot.pending_sends.lookup(&session_id.get()) else {
            return Ok(None);
        };
        let buffers = self.buffers();
        let total_len = buffers
            .current_len(head)?
            .checked_add(buffers.total_len_not_including_first(head)?)
            .ok_or_else(|| CoreError::internal("session app tx chain length overflow"))?;
        Ok(Some(total_len))
    }

    #[inline]
    pub(crate) fn pending_send_head(&self, session_id: SessionId) -> Option<BufferIndex> {
        self.hot.pending_sends.lookup(&session_id.get())
    }

    #[inline]
    pub(crate) fn free_pending_send(&mut self, session_id: SessionId) {
        let Some(head) = self.hot.pending_sends.remove(&session_id.get()) else {
            return;
        };
        self.buffers().free_index(head);
    }

    pub(crate) fn take_ready_tx_sessions(&mut self, out: &mut hammer_infra::vec::Vec<SessionId>) {
        for session_id in self.hot.tx_ready_sessions.take_ready_sessions() {
            out.push(session_id);
        }
    }

    pub(crate) fn take_ready_sessions(&mut self, out: &mut hammer_infra::vec::Vec<SessionId>) {
        for session_id in self.hot.ready_sessions.take_ready_sessions() {
            out.push(session_id);
        }
    }

    fn handle_submission_descriptor(&mut self, descriptor: AppSqeDescriptor) -> CoreResult<()> {
        match descriptor.opcode() {
            AppOpcode::Send => {
                if let AppSqeData::Send { data } = descriptor.payload() {
                    let op = app_op_from_descriptor(descriptor);
                    let Some(session_id) = self.session_for_op(op) else {
                        return Ok(());
                    };
                    if let Some(ring) = self.control.ring.clone() {
                        let send: AppSendData = ring.send_from_data(data).try_into()?;
                        self.push_pending_send(session_id, send);
                    }
                }
            }
            AppOpcode::Close => {
                let op = app_op_from_descriptor(descriptor);
                if let Some(session_id) = self.session_for_op(op) {
                    self.control
                        .drained_closes
                        .push(SessionAppCloseSubmission::new(session_id, op));
                }
            }
            AppOpcode::Recv => {
                let op = app_op_from_descriptor(descriptor);
                if let Some(session_id) = self.session_for_op(op) {
                    self.hot.ready_sessions.mark_ready(session_id);
                }
            }
            AppOpcode::Nop => {}
        }
        Ok(())
    }

    #[inline]
    fn buffers(&self) -> &DataPlaneBuffers {
        &self.hot.buffers
    }

    fn copy_send_into_session_chain(&self, send: &AppSendData) -> CoreResult<BufferIndex> {
        let buffers = self.buffers();
        let total_len = send.len()?;
        let head = buffers.alloc_index()?;
        let mut current = head;
        let mut copied = 0usize;

        while copied < total_len {
            let writable_len = {
                let mut buffer = buffers.get_buffer_mut(current)?;
                buffer.writable_tail_mut().len()
            };
            if writable_len == 0 {
                return Err(CoreError::internal(
                    "session app tx buffer has no writable tail capacity",
                ));
            }
            let chunk_len = writable_len.min(total_len - copied);
            let written = {
                let mut buffer = buffers.get_buffer_mut(current)?;
                let writable = buffer.writable_tail_mut();
                let written = send
                    .copy_to(copied, &mut writable[..chunk_len])
                    .map_err(CoreError::from)?;
                buffer.commit_writable_tail(written)?;
                written
            };
            if written == 0 {
                return Err(CoreError::internal(
                    "session app tx send copy returned zero bytes",
                ));
            }
            copied += written;
            if copied < total_len {
                let next = buffers.alloc_index()?;
                buffers.append_existing_chain(current, next)?;
                current = next;
            }
        }

        Ok(head)
    }

    #[inline]
    fn session_for_op(&self, op: AppOpId) -> Option<SessionId> {
        self.control
            .session_slots
            .lookup(&op.value())
            .filter(|session_id| *session_id != UNBOUND_SESSION)
    }
}

impl Default for SessionAppRuntime {
    #[inline]
    fn default() -> Self {
        Self::new(DataPlaneBuffers::with_buffer_capacity(2048, 1))
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
    use hammer_adapter::DataPlaneBuffers;
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

        let buffers = DataPlaneBuffers::with_buffer_capacity(512, 8);
        let mut app = SessionAppRuntime::new(buffers.clone());
        let session_a = SessionId::new(10);
        let session_b = SessionId::new(20);

        app.push_pending_send(session_a, first);
        app.push_pending_send(session_b, second);

        assert_eq!(
            app.pending_send_len(session_a).expect("pending len"),
            Some(5)
        );
        let first = app.pending_send_head(session_a).expect("first pending");
        assert_eq!(buffers.copy_current_chain(first).expect("copied"), b"first");
        app.free_pending_send(session_a);
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
        let buffers = DataPlaneBuffers::with_buffer_capacity(512, 8);
        let mut app = SessionAppRuntime::new(buffers.clone());
        let session_a = SessionId::new(10);
        let session_b = SessionId::new(20);

        app.push_pending_send(session_a, first);
        app.push_pending_send(session_b, second);

        assert_eq!(
            app.pending_send_len(session_a).expect("pending len"),
            Some(5)
        );
        app.free_pending_send(session_a);
        assert!(!app.has_pending_send(session_a));
        let second = app.pending_send_head(session_b).expect("second pending");
        assert_eq!(
            buffers.copy_current_chain(second).expect("copied"),
            b"second"
        );
        app.free_pending_send(session_b);
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
        let buffers = DataPlaneBuffers::with_buffer_capacity(512, 8);
        let mut app = SessionAppRuntime::new(buffers.clone());
        let session = SessionId::new(30);

        app.push_pending_send(session, first);
        app.push_pending_send(session, second);

        assert_eq!(
            buffers
                .copy_current_chain(app.pending_send_head(session).expect("copied head"))
                .expect("copied"),
            b"firstsecond"
        );
        app.free_pending_send(session);
    }

    #[test]
    fn poll_ready_send_sessions_returns_only_newly_pending_sessions() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"bytes").expect("bytes"))
            .try_into()
            .expect("transfer");
        let mut app = SessionAppRuntime::new(DataPlaneBuffers::with_buffer_capacity(512, 8));
        let session = SessionId::new(40);
        let mut ready = hammer_infra::vec::Vec::new();

        app.push_pending_send(session, send);
        app.take_ready_tx_sessions(&mut ready);
        assert_eq!(ready.as_slice(), &[session]);

        ready.clear();
        app.take_ready_tx_sessions(&mut ready);
        assert!(ready.is_empty());
    }

    #[test]
    fn unfinished_send_tracks_progress_without_exposing_entry() {
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
            .try_into()
            .expect("transfer");
        let buffers = DataPlaneBuffers::with_buffer_capacity(512, 8);
        let mut app = SessionAppRuntime::new(buffers.clone());
        let session_id = SessionId::new(1);

        app.push_pending_send(session_id, send);
        assert!(
            !app.release_pending_send_bytes(session_id, 4)
                .expect("partial")
        );
        assert_eq!(
            app.pending_send_len(session_id).expect("pending len"),
            Some(2)
        );
        assert!(
            app.release_pending_send_bytes(session_id, 2)
                .expect("finish")
        );
        assert!(!app.has_pending_send(session_id));
    }
}
