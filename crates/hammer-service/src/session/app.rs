use std::net::Shutdown;

use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::app::{
    AppBackend, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppObjectRef, AppRegisteredBuffer,
    AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown, AppUserData,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSessionId(u64);

impl AppSessionId {
    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub enum AppSessionSubmission {
    Send(AppSessionSend),
    Recv(AppSessionRecv),
    Close(AppSessionClose),
    Shutdown(AppSessionShutdown),
}

impl AppSessionSubmission {
    #[inline]
    pub fn session_id(&self) -> AppSessionId {
        match self {
            Self::Send(send) => send.session_id(),
            Self::Recv(recv) => recv.session_id(),
            Self::Close(close) => close.session_id(),
            Self::Shutdown(shutdown) => shutdown.session_id(),
        }
    }
}

#[derive(Debug)]
pub struct AppSessionSend {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
    registered: AppRegisteredBuffer,
}

impl AppSessionSend {
    #[inline]
    pub fn new(
        session_id: AppSessionId,
        descriptor: AppSqeDescriptor,
        registered: AppRegisteredBuffer,
    ) -> Self {
        Self {
            session_id,
            descriptor,
            registered,
        }
    }

    #[inline]
    pub const fn session_id(&self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub fn descriptor(&self) -> &AppSqeDescriptor {
        &self.descriptor
    }

    #[inline]
    pub fn registered(&self) -> &AppRegisteredBuffer {
        &self.registered
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionRecv {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
    max_len: u32,
}

impl AppSessionRecv {
    #[inline]
    pub const fn new(session_id: AppSessionId, descriptor: AppSqeDescriptor, max_len: u32) -> Self {
        Self {
            session_id,
            descriptor,
            max_len,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub const fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }

    #[inline]
    pub const fn max_len(self) -> u32 {
        self.max_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionClose {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
}

impl AppSessionClose {
    #[inline]
    pub const fn new(session_id: AppSessionId, descriptor: AppSqeDescriptor) -> Self {
        Self {
            session_id,
            descriptor,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub const fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionShutdown {
    session_id: AppSessionId,
    shutdown: AppTcpShutdown,
}

impl AppSessionShutdown {
    #[inline]
    pub const fn new(session_id: AppSessionId, shutdown: AppTcpShutdown) -> Self {
        Self {
            session_id,
            shutdown,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub fn shutdown(self) -> AppTcpShutdown {
        self.shutdown
    }

    #[inline]
    pub fn how(self) -> Shutdown {
        self.shutdown.how()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionCompletion {
    session_id: AppSessionId,
    user_data: AppUserData,
    result: i32,
    flags: AppCqeFlags,
    data: AppCqeData,
}

impl AppSessionCompletion {
    #[inline]
    pub const fn new(
        session_id: AppSessionId,
        user_data: AppUserData,
        result: i32,
        flags: AppCqeFlags,
        data: AppCqeData,
    ) -> Self {
        Self {
            session_id,
            user_data,
            result,
            flags,
            data,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }
}

#[derive(Clone, Debug)]
struct AppSessionBackend {
    session_id: AppSessionId,
    flow: hammer_runtime::app::AppFlowId,
    backend: AppBackend,
}

pub struct AppSessionAppIngress {
    backends: hammer_infra::vec::Vec<AppSessionBackend>,
    backend_slots: hammer_infra::map::FlatHashTable<u64, usize>,
}

impl AppSessionAppIngress {
    #[inline]
    pub fn new() -> Self {
        Self {
            backends: hammer_infra::vec::Vec::new(),
            backend_slots: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn attach_backend(
        &mut self,
        session_id: AppSessionId,
        backend: AppBackend,
    ) -> CoreResult<()> {
        if self.backend_slots.lookup(&session_id.get()).is_some() {
            return Err(CoreError::internal(format!(
                "app session backend already attached for session {}",
                session_id.get()
            )));
        }
        let slot = self.backends.len();
        self.backends.push(AppSessionBackend {
            session_id,
            flow: backend.flow(),
            backend,
        });
        self.backend_slots.insert(session_id.get(), slot);
        Ok(())
    }

    pub fn poll_submissions(
        &mut self,
        submissions: &mut hammer_infra::vec::Vec<AppSessionSubmission>,
    ) -> CoreResult<usize> {
        let backends: hammer_infra::vec::Vec<AppSessionBackend> =
            self.backends.iter().cloned().collect();
        let mut polled = 0usize;
        for app_backend in backends {
            while let Some(entry) = app_backend.backend.try_pop_submission_entry() {
                self.handle_submission(&app_backend, entry, submissions)?;
                polled += 1;
            }
            while let Some(shutdown) = app_backend.backend.try_pop_tcp_shutdown() {
                if shutdown.flow() != app_backend.flow {
                    return Err(CoreError::internal(format!(
                        "app session shutdown flow {} does not match attached flow {}",
                        shutdown.flow().value(),
                        app_backend.flow.value()
                    )));
                }
                submissions.push(AppSessionSubmission::Shutdown(AppSessionShutdown::new(
                    app_backend.session_id,
                    shutdown,
                )));
                polled += 1;
            }
        }
        Ok(polled)
    }

    pub fn complete(&mut self, completion: AppSessionCompletion) -> CoreResult<()> {
        let slot = self
            .backend_slots
            .lookup(&completion.session_id.get())
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "app session completion backend missing for session {}",
                    completion.session_id.get()
                ))
            })?;
        let app_backend = self
            .backends
            .get(slot)
            .ok_or_else(|| CoreError::internal("app session backend slot is invalid"))?;
        app_backend
            .backend
            .try_push_cqe_descriptor(AppCqeDescriptor::new(
                completion.user_data,
                completion.result,
                completion.flags,
                AppObjectRef::Flow(app_backend.flow),
                completion.data,
            ))
            .map_err(CoreError::from)
    }

    fn handle_submission(
        &mut self,
        app_backend: &AppSessionBackend,
        entry: AppSubmissionEntry,
        submissions: &mut hammer_infra::vec::Vec<AppSessionSubmission>,
    ) -> CoreResult<()> {
        let (descriptor, registered) = entry.into_parts();
        if descriptor.object() != AppObjectRef::Flow(app_backend.flow) {
            return Err(CoreError::internal(format!(
                "app session submission object {:?} does not match attached flow {}",
                descriptor.object(),
                app_backend.flow.value()
            )));
        }
        match descriptor.payload() {
            AppSqeData::Send { buffer } => {
                let registered = registered.ok_or_else(|| {
                    CoreError::internal("app session send submission is missing registered buffer")
                })?;
                if registered.index() != buffer {
                    return Err(CoreError::internal(
                        "app session send buffer index mismatch",
                    ));
                }
                submissions.push(AppSessionSubmission::Send(AppSessionSend::new(
                    app_backend.session_id,
                    descriptor,
                    registered,
                )));
            }
            AppSqeData::Recv { max_len } => {
                submissions.push(AppSessionSubmission::Recv(AppSessionRecv::new(
                    app_backend.session_id,
                    descriptor,
                    max_len,
                )));
            }
            AppSqeData::Close => {
                submissions.push(AppSessionSubmission::Close(AppSessionClose::new(
                    app_backend.session_id,
                    descriptor,
                )));
            }
            AppSqeData::Nop => {
                app_backend
                    .backend
                    .try_push_cqe_descriptor(AppCqeDescriptor::new(
                        descriptor.user_data(),
                        0,
                        AppCqeFlags::NONE,
                        AppObjectRef::Flow(app_backend.flow),
                        AppCqeData::None,
                    ))
                    .map_err(CoreError::from)?;
            }
            AppSqeData::Accept | AppSqeData::RecvFrom { .. } | AppSqeData::SendTo { .. } => {
                return Err(CoreError::internal(format!(
                    "unsupported app session submission opcode {:?}",
                    descriptor.opcode()
                )));
            }
        }
        Ok(())
    }
}
