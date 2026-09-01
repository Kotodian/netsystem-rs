use crate::FileMain;
use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;

impl GlobalMain {
    #[inline]
    pub fn ensure_main_thread(&self) -> RuntimeResult<()> {
        if self.main.thread_index() != 0 {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        Ok(())
    }

    pub fn ensure_main_thread_with_barrier(&self) -> RuntimeResult<()> {
        self.ensure_main_thread()?;
        let barrier = crate::barrier::global();
        if barrier
            .as_ref()
            .is_some_and(|barrier| barrier.worker_count() != 0 && !barrier.is_pending())
        {
            return Err(RuntimeError::ControlRequiresWorkerBarrier);
        }
        Ok(())
    }

    pub fn file_main(&self) -> &'static FileMain {
        crate::file::FILE_MAIN
            .get()
            .expect("FileMain is initialized before runtime use")
    }

    pub(crate) fn poll_file_readiness(&mut self) -> RuntimeResult<usize> {
        let graph = self.main.nodes();
        self.file_main()
            .poll_for_worker(self.main.thread_index(), graph)
    }

    pub fn set_ipc_listener(&mut self, listener: tokio::net::TcpListener) {
        self.ipc_listener = Some(listener);
    }

    pub fn take_ipc_listener(&mut self) -> Option<tokio::net::TcpListener> {
        self.ipc_listener.take()
    }

    pub fn install_current(&mut self) {
        super::CURRENT_GLOBAL_MAIN.with(|cell| {
            *cell.borrow_mut() = Some(self as *mut GlobalMain);
        });
    }

    pub fn with_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut GlobalMain) -> R,
    {
        super::CURRENT_GLOBAL_MAIN.with(|cell| {
            let ptr = *cell.borrow();
            ptr.map(|pointer| {
                // SAFETY: install_current stores the owning thread's main
                // context and callers only access it on that same thread.
                let main = unsafe { &mut *pointer };
                f(main)
            })
        })
    }

    pub fn uninstall_current() {
        super::CURRENT_GLOBAL_MAIN.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

#[inline]
pub fn ensure_main_thread() -> RuntimeResult<()> {
    match GlobalMain::with_current(|main| main.ensure_main_thread()) {
        Some(result) => result,
        None => Err(RuntimeError::ControlRequiresMainThread),
    }
}

pub fn ensure_main_thread_with_barrier() -> RuntimeResult<()> {
    match GlobalMain::with_current(|main| main.ensure_main_thread_with_barrier()) {
        Some(result) => result,
        None => Err(RuntimeError::ControlRequiresMainThread),
    }
}

pub(crate) fn thread_panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}
