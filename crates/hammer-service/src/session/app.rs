use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRingHandle, AppSendData, AppSqeData,
    AppSqeDescriptor,
};

use crate::session::SessionId;

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
    session_id: SessionId,
    send: AppSendData,
    sent_len: usize,
}

impl SessionAppTxProgress {
    #[inline]
    fn new(session_id: SessionId, send: AppSendData) -> Self {
        Self {
            session_id,
            send,
            sent_len: 0,
        }
    }

    #[inline]
    const fn session_id(&self) -> SessionId {
        self.session_id
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

#[derive(Debug)]
pub struct SessionAppRuntime {
    ring: Option<AppRingHandle>,
    session_slots: FlatHashTable<u64, SessionId>,
    pending_sends: hammer_infra::vec::Vec<SessionAppTxProgress>,
    drained_closes: hammer_infra::vec::Vec<SessionAppCloseSubmission>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            ring: None,
            session_slots: FlatHashTable::new(),
            pending_sends: hammer_infra::vec::Vec::new(),
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
        self.pending_sends
            .push(SessionAppTxProgress::new(session_id, send));
    }

    pub(crate) fn copy_pending_send_bytes(
        &self,
        session_id: SessionId,
        max_len: usize,
    ) -> CoreResult<Option<hammer_infra::vec::Vec<u8>>> {
        let Some(send) = self
            .pending_sends
            .iter()
            .find(|send| send.session_id() == session_id)
        else {
            return Ok(None);
        };
        Ok(Some(send.copy_pending_bytes(max_len)?))
    }

    pub(crate) fn commit_pending_send_bytes(
        &mut self,
        session_id: SessionId,
        len: usize,
    ) -> CoreResult<bool> {
        let index = self
            .pending_sends
            .iter()
            .position(|send| send.session_id() == session_id)
            .ok_or_else(|| CoreError::internal("session app tx progress is missing"))?;
        let completed = {
            let send = &mut self.pending_sends.as_mut_slice()[index];
            send.commit_bytes(len)?
        };
        if completed {
            let send = self.pending_sends.remove(index);
            send.finish();
        }
        Ok(completed)
    }

    #[inline]
    pub(crate) fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_sends
            .iter()
            .any(|send| send.session_id() == session_id)
    }

    pub(crate) fn pending_send_session_ids(&self, out: &mut hammer_infra::vec::Vec<SessionId>) {
        for send in &self.pending_sends {
            if !out
                .iter()
                .any(|session_id| *session_id == send.session_id())
            {
                out.push(send.session_id());
            }
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
