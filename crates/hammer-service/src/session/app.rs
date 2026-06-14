use hammer_core::error::CoreResult;
use hammer_infra::map::FlatHashTable;
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRingHandle, AppSend, AppSqeData, AppSqeDescriptor,
};

use crate::session::SessionId;

const UNBOUND_SESSION: SessionId = SessionId::new(u64::MAX);

#[derive(Debug)]
pub struct SessionAppSendSubmission {
    pub session_id: SessionId,
    pub op: AppOpId,
    pub send: AppSend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionAppCloseSubmission {
    pub session_id: SessionId,
    pub op: AppOpId,
}

#[derive(Debug)]
pub struct SessionAppRuntime {
    ring: Option<AppRingHandle>,
    session_slots: FlatHashTable<u64, SessionId>,
    drained_sends: hammer_infra::vec::Vec<SessionAppSendSubmission>,
    drained_closes: hammer_infra::vec::Vec<SessionAppCloseSubmission>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            ring: None,
            session_slots: FlatHashTable::new(),
            drained_sends: hammer_infra::vec::Vec::new(),
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
    pub fn take_drained_sends(&mut self) -> hammer_infra::vec::Vec<SessionAppSendSubmission> {
        self.drained_sends.drain(..).collect()
    }

    #[inline]
    pub fn take_drained_closes(&mut self) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        self.drained_closes.drain(..).collect()
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
                        let send = ring.send_from_data(data);
                        self.drained_sends.push(SessionAppSendSubmission {
                            session_id,
                            op,
                            send,
                        });
                    }
                }
            }
            AppOpcode::Close => {
                let op = app_op_from_descriptor(descriptor);
                if let Some(session_id) = self.session_for_op(op) {
                    self.drained_closes
                        .push(SessionAppCloseSubmission { session_id, op });
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
