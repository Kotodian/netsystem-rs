pub mod tcp;

use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::app::AppBackend;

use crate::session::{
    AppSessionCompletion, AppSessionId, AppSessionSubmission, AppSessionTimerExpiry,
    AppSessionTimerToken, WorkerSessionRuntime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionProtocolId(u16);

impl SessionProtocolId {
    #[inline(always)]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u16 {
        self.0
    }
}

pub struct SessionProtocolContext<'a> {
    worker: DataWorkerId,
    runtime: &'a mut WorkerSessionRuntime,
}

impl<'a> SessionProtocolContext<'a> {
    #[inline]
    pub fn new(worker: DataWorkerId, runtime: &'a mut WorkerSessionRuntime) -> Self {
        Self { worker, runtime }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn attach_app_backend(
        &mut self,
        session_id: AppSessionId,
        backend: AppBackend,
    ) -> CoreResult<()> {
        self.runtime.attach_app_backend(session_id, backend)
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: AppSessionId) {
        self.runtime.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.runtime.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: AppSessionId, token: AppSessionTimerToken) -> bool {
        self.runtime.cancel_timer(session_id, token)
    }

    #[inline]
    pub fn complete(&mut self, completion: AppSessionCompletion) -> CoreResult<()> {
        self.runtime.complete(completion)
    }
}

pub trait SessionProtocolOps {
    fn handle_submission(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        submission: AppSessionSubmission,
    ) -> CoreResult<()>;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()>;

    fn handle_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        session_id: AppSessionId,
    ) -> CoreResult<()>;
}

struct SessionProtocolSlot {
    name: &'static str,
    ops: std::boxed::Box<dyn SessionProtocolOps>,
}

pub struct SessionProtocolRegistry {
    protocols: hammer_infra::vec::Vec<SessionProtocolSlot>,
    session_protocols: hammer_infra::map::FlatHashTable<u64, u16>,
}

impl SessionProtocolRegistry {
    #[inline]
    pub fn new() -> Self {
        Self {
            protocols: hammer_infra::vec::Vec::new(),
            session_protocols: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn register(
        &mut self,
        name: &'static str,
        ops: std::boxed::Box<dyn SessionProtocolOps>,
    ) -> CoreResult<SessionProtocolId> {
        let slot = u16::try_from(self.protocols.len())
            .map_err(|_| CoreError::internal("session protocol registry overflow"))?;
        self.protocols.push(SessionProtocolSlot { name, ops });
        Ok(SessionProtocolId::new(slot))
    }

    pub fn bind_session(
        &mut self,
        session_id: AppSessionId,
        protocol_id: SessionProtocolId,
    ) -> CoreResult<()> {
        self.protocol_slot(protocol_id)?;
        self.session_protocols
            .insert(session_id.get(), protocol_id.get());
        Ok(())
    }

    pub fn protocol_for_session(&self, session_id: AppSessionId) -> CoreResult<SessionProtocolId> {
        self.session_protocols
            .lookup(&session_id.get())
            .map(SessionProtocolId::new)
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "session protocol binding missing for session {}",
                    session_id.get()
                ))
            })
    }

    pub fn protocol_mut(
        &mut self,
        protocol_id: SessionProtocolId,
    ) -> CoreResult<&mut dyn SessionProtocolOps> {
        let slot = self.protocol_slot(protocol_id)?;
        Ok(self.protocols[slot].ops.as_mut())
    }

    pub fn protocol_name(&self, protocol_id: SessionProtocolId) -> CoreResult<&'static str> {
        let slot = self.protocol_slot(protocol_id)?;
        Ok(self.protocols[slot].name)
    }

    pub fn dispatch_submission(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        submission: AppSessionSubmission,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(submission.session_id())?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_submission(&mut context, submission)
    }

    pub fn dispatch_timer_expiry(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(expiry.session_id())?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_timer_expiry(&mut context, expiry)
    }

    pub fn dispatch_ready(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        session_id: AppSessionId,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(session_id)?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_ready(&mut context, session_id)
    }

    fn protocol_slot(&self, protocol_id: SessionProtocolId) -> CoreResult<usize> {
        let slot = protocol_id.get() as usize;
        if slot >= self.protocols.len() {
            return Err(CoreError::internal(format!(
                "session protocol id {} is invalid",
                protocol_id.get()
            )));
        }
        Ok(slot)
    }
}
