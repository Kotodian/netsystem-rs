use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use thiserror::Error;

use crate::{DataWorkerId, Engine, RuntimeError, RuntimeResult};

/// Compact identity for a registered Session App in one link image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[repr(transparent)]
pub struct SessionAppId(u32);

impl SessionAppId {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// Plugin-owned opaque Session App context identity.
pub type SessionAppContext = u64;

/// Worker-init installer for one concrete Session App callback table.
///
/// The callback table itself lives in `hammer-service` because its callbacks
/// receive the worker-local `SessionWorker`. Runtime only stores this
/// install function pointer so the registration image remains ABI-safe.
pub type SessionAppInstall = fn(&mut Engine) -> RuntimeResult<()>;
pub type SessionAppDestroy = fn(DataWorkerId, SessionAppContext);

/// Static identity and worker-init glue declared by one Session App.
#[derive(Debug, Clone, Copy)]
pub struct SessionAppRegistration {
    name: &'static str,
    install: SessionAppInstall,
    destroy: SessionAppDestroy,
}

impl SessionAppRegistration {
    #[doc(hidden)]
    #[inline]
    pub const fn new(
        name: &'static str,
        install: SessionAppInstall,
        destroy: SessionAppDestroy,
    ) -> Self {
        Self {
            name,
            install,
            destroy,
        }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[doc(hidden)]
    #[inline]
    pub fn install(self, engine: &mut Engine) -> RuntimeResult<()> {
        (self.install)(engine)
    }

    #[doc(hidden)]
    #[inline]
    pub fn destroy(self, worker: DataWorkerId, context: SessionAppContext) {
        (self.destroy)(worker, context)
    }
}

#[hammer_component_macros::runtime_error(subsystem = "session-app")]
#[derive(Debug, Error)]
enum SessionAppContextError {
    #[error("Session App worker {worker} is outside worker count {worker_count}")]
    WorkerOutOfRange { worker: usize, worker_count: usize },
    #[error("Session App context storage was initialized for {actual} workers, not {expected}")]
    WorkerCountMismatch { expected: usize, actual: usize },
    #[error("Session App context {context:#x} is not owned by worker {worker}")]
    Missing {
        worker: usize,
        context: SessionAppContext,
    },
    #[error("Session App context storage for worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

/// Worker-local plugin-owned Session App state, kept private to the plugin.
#[doc(hidden)]
pub struct SessionAppContexts<P> {
    workers: Box<[ThreadOwned<Pool<SessionAppContextSlot<P>>>]>,
    capacity: usize,
}

struct SessionAppContextSlot<P> {
    app: P,
}

impl<P> SessionAppContexts<P>
where
    P: Send,
{
    pub fn new(worker_count: usize, capacity: usize) -> Self {
        Self {
            workers: (0..worker_count)
                .map(|_| ThreadOwned::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            capacity,
        }
    }

    pub fn insert(
        &self,
        worker: DataWorkerId,
        worker_count: usize,
        app: P,
    ) -> RuntimeResult<SessionAppContext> {
        let contexts = self.worker(worker, worker_count)?;
        match contexts.with_mut(|_| ()) {
            Ok(()) => {}
            Err(ThreadOwnedError::NotInstalled) => {
                if let Err(contexts) = contexts.install(Pool::with_capacity(self.capacity)) {
                    drop(contexts);
                }
            }
            Err(source) => return Err(context_worker_access(worker, source)),
        }
        Ok(contexts
            .with_mut(|contexts| u64::from(contexts.insert(SessionAppContextSlot { app })))
            .map_err(|source| context_worker_access(worker, source))?)
    }

    pub fn with_mut<R>(
        &self,
        worker: DataWorkerId,
        context: SessionAppContext,
        operation: impl FnOnce(&mut P) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let contexts = self.worker(worker, self.workers.len())?;
        Ok(contexts
            .with_mut(|contexts| {
                let index = context_index(context).ok_or(SessionAppContextError::Missing {
                    worker: worker.slot(),
                    context,
                })?;
                let app = contexts
                    .get_mut(index)
                    .ok_or(SessionAppContextError::Missing {
                        worker: worker.slot(),
                        context,
                    })?;
                operation(&mut app.app).map_err(SessionAppContextOperationError::Operation)
            })
            .map_err(|source| context_worker_access(worker, source))??)
    }

    pub fn remove(&self, worker: DataWorkerId, context: SessionAppContext) {
        let Some(contexts) = self.workers.get(worker.slot()) else {
            tracing::error!(
                worker = worker.slot(),
                ?context,
                "Session App context worker is missing"
            );
            return;
        };
        let Some(index) = context_index(context) else {
            tracing::error!(
                worker = worker.slot(),
                ?context,
                "Session App context index is invalid"
            );
            return;
        };
        match contexts.with_mut(|contexts| {
            drop(contexts.remove(index));
        }) {
            Ok(()) => {}
            Err(source) => {
                tracing::error!(worker = worker.slot(), ?context, %source, "failed to access Session App context worker");
            }
        }
    }

    fn worker(
        &self,
        worker: DataWorkerId,
        worker_count: usize,
    ) -> RuntimeResult<&ThreadOwned<Pool<SessionAppContextSlot<P>>>> {
        if self.workers.len() != worker_count {
            return Err(RuntimeError::from(
                SessionAppContextError::WorkerCountMismatch {
                    expected: self.workers.len(),
                    actual: worker_count,
                },
            ));
        }
        self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(SessionAppContextError::WorkerOutOfRange {
                worker: worker.slot(),
                worker_count,
            })
        })
    }
}

enum SessionAppContextOperationError {
    Missing(SessionAppContextError),
    Operation(RuntimeError),
}

impl From<SessionAppContextError> for SessionAppContextOperationError {
    fn from(error: SessionAppContextError) -> Self {
        Self::Missing(error)
    }
}

impl From<SessionAppContextOperationError> for RuntimeError {
    fn from(error: SessionAppContextOperationError) -> Self {
        match error {
            SessionAppContextOperationError::Missing(error) => RuntimeError::from(error),
            SessionAppContextOperationError::Operation(error) => error,
        }
    }
}

fn context_worker_access(worker: DataWorkerId, source: ThreadOwnedError) -> RuntimeError {
    RuntimeError::from(SessionAppContextError::WorkerAccess {
        worker: worker.slot(),
        source,
    })
}

#[inline]
fn context_index(context: SessionAppContext) -> Option<u32> {
    u32::try_from(context).ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn plugin_contexts_keep_http2_stream_mapping_private() {
        let worker = DataWorkerId::new(0);
        let contexts = SessionAppContexts::<HashMap<u64, u64>>::new(1, 8);
        let connection = contexts
            .insert(worker, 1, HashMap::new())
            .expect("insert HTTP/2 connection context");

        contexts
            .with_mut(worker, connection, |streams| {
                streams.insert(1, 11);
                streams.insert(3, 13);
                Ok(())
            })
            .expect("private HTTP/2 stream map");

        let other = contexts
            .insert(worker, 1, HashMap::new())
            .expect("insert independent HTTP/2 context");
        assert_ne!(connection, other);
        contexts
            .with_mut(worker, other, |streams| {
                assert!(streams.is_empty());
                Ok(())
            })
            .expect("connection contexts stay independent");
    }

    #[test]
    fn plugin_contexts_express_http3_lower_and_internal_streams() {
        let worker = DataWorkerId::new(0);
        let contexts = SessionAppContexts::<u64>::new(1, 16);
        let connection = contexts
            .insert(worker, 1, 7)
            .expect("HTTP/3 connection context");
        let request = contexts
            .insert(worker, 1, 7)
            .expect("HTTP/3 request stream context");
        let control = contexts
            .insert(worker, 1, 7)
            .expect("HTTP/3 control stream context");

        let mut private_streams = vec![request, control];
        private_streams.push(
            contexts
                .insert(worker, 1, 7)
                .expect("HTTP/3 QPACK stream context"),
        );

        for stream in private_streams {
            contexts
                .with_mut(worker, stream, |owner| {
                    assert_eq!(*owner, 7);
                    Ok(())
                })
                .expect("HTTP/3 stream owner remains private");
        }
        contexts
            .with_mut(worker, connection, |owner| {
                assert_eq!(*owner, 7);
                Ok(())
            })
            .expect("HTTP/3 connection context remains private");
    }
}
