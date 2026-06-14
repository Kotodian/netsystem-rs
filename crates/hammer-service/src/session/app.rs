use hammer_core::error::CoreResult;
use hammer_infra::map::FlatHashTable;
use hammer_runtime::app::{
    AppCqe, AppObjectRef, AppOpId, AppOpcode, AppRecv, AppRingHandle, AppSend, AppSqeDescriptor,
    AppSqeData,
};

use crate::session::SessionId;

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

#[derive(Debug, Clone)]
struct AppRingBinding {
    op: AppOpId,
    ring: AppRingHandle,
}

#[derive(Debug)]
pub struct SessionAppRuntime {
    bindings: hammer_infra::vec::Vec<AppRingBinding>,
    binding_slots: FlatHashTable<u64, usize>,
    session_slots: FlatHashTable<u64, SessionId>,
    drained_sends: hammer_infra::vec::Vec<SessionAppSendSubmission>,
    drained_closes: hammer_infra::vec::Vec<SessionAppCloseSubmission>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new() -> Self {
        Self {
            bindings: hammer_infra::vec::Vec::new(),
            binding_slots: FlatHashTable::new(),
            session_slots: FlatHashTable::new(),
            drained_sends: hammer_infra::vec::Vec::new(),
            drained_closes: hammer_infra::vec::Vec::new(),
        }
    }

    #[inline]
    pub fn bind_ring(&mut self, session_id: SessionId, op: AppOpId, ring: AppRingHandle) {
        if let Some(slot) = self.binding_slots.lookup(&op.value()) {
            self.bindings[slot] = AppRingBinding { op, ring };
            self.session_slots.insert(op.value(), session_id);
            return;
        }
        let slot = self.bindings.len();
        self.bindings.push(AppRingBinding { op, ring });
        self.binding_slots.insert(op.value(), slot);
        self.session_slots.insert(op.value(), session_id);
    }

    #[inline]
    pub fn unbind_ring(&mut self, op: AppOpId) -> Option<AppRingHandle> {
        let slot = self.binding_slots.lookup(&op.value())?;
        let removed = swap_remove_binding(&mut self.bindings, slot)?;
        self.rebuild_indexes();
        Some(removed.ring)
    }

    #[inline]
    pub fn complete_recv(&self, op: AppOpId, recv: AppRecv, fin: bool) -> CoreResult<()> {
        let Some(ring) = self.ring_for_op(op) else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::recv(None, op, recv, fin))
    }

    #[inline]
    pub fn complete_closed(&self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.ring_for_op(op) else {
            return Ok(());
        };
        ring.try_push_completion(AppCqe::closed(None, Some(op)))
    }

    pub fn drain_submissions_for_op(&mut self, op: AppOpId) -> CoreResult<()> {
        let Some(ring) = self.ring_for_op(op).cloned() else {
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

    #[inline]
    fn ring_for_op(&self, op: AppOpId) -> Option<&AppRingHandle> {
        let slot = self.binding_slots.lookup(&op.value())?;
        let binding = self.bindings.get(slot)?;
        debug_assert_eq!(binding.op, op);
        Some(&binding.ring)
    }

    fn handle_submission_descriptor(
        &mut self,
        descriptor: AppSqeDescriptor,
    ) -> CoreResult<()> {
        match descriptor.opcode() {
            AppOpcode::Send => {
                if let AppSqeData::Send { buffer } = descriptor.payload() {
                    let op = app_op_from_descriptor(descriptor);
                    let Some(session_id) = self.session_for_op(op) else {
                        return Ok(());
                    };
                    if let Some(ring) = self.ring_for_op(op).cloned() {
                        let send = ring.take_send_buffer(buffer)?;
                        self.drained_sends
                            .push(SessionAppSendSubmission { session_id, op, send });
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
        self.session_slots.lookup(&op.value())
    }

    fn rebuild_indexes(&mut self) {
        let old_session_slots = std::mem::replace(
            &mut self.session_slots,
            FlatHashTable::with_capacity(self.bindings.len().max(1) * 2),
        );
        self.binding_slots = FlatHashTable::with_capacity(self.bindings.len().max(1) * 2);
        for (slot, binding) in self.bindings.iter().enumerate() {
            self.binding_slots.insert(binding.op.value(), slot);
            if let Some(session_id) = old_session_slots.lookup(&binding.op.value()) {
                self.session_slots.insert(binding.op.value(), session_id);
            }
        }
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

#[inline]
fn swap_remove_binding(
    bindings: &mut hammer_infra::vec::Vec<AppRingBinding>,
    slot: usize,
) -> Option<AppRingBinding> {
    if slot >= bindings.len() {
        return None;
    }
    let last = bindings.pop()?;
    if slot == bindings.len() {
        return Some(last);
    }
    let removed = std::mem::replace(&mut bindings[slot], last);
    Some(removed)
}
