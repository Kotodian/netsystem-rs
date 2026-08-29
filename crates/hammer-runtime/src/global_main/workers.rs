use std::sync::Arc;
use std::thread::JoinHandle;

use crate::DataWorkerId;
use crate::config::Worker;
use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;
use crate::spawn::{DataRemoteLocalQueue, DataRemoteLocalQueueError};

impl GlobalMain {
    #[inline]
    pub fn configured_worker_count(&self) -> usize {
        self.worker_config.count
    }

    pub fn schedule_on_worker(
        &self,
        worker: DataWorkerId,
        task: impl FnOnce() + Send + 'static,
    ) -> RuntimeResult<()> {
        if self.main.thread_index() != 0 {
            return Err(RuntimeError::WorkerControlRequiresGlobalMain);
        }
        if worker.slot() >= self.worker_config.count {
            return Err(RuntimeError::DataWorkerIndexOutOfRange {
                worker: worker.slot(),
                worker_count: self.worker_config.count,
            });
        }
        let queue = self
            .worker_control_queues
            .get(worker.slot())
            .ok_or(RuntimeError::WorkerControlUnavailable { worker })?;
        queue.push(task).map_err(|error| match error {
            DataRemoteLocalQueueError::Closed => RuntimeError::WorkerControlClosed { worker },
            DataRemoteLocalQueueError::Full { capacity } => {
                RuntimeError::WorkerControlQueueFull { worker, capacity }
            }
        })
    }

    #[inline]
    pub(crate) fn worker_config(&self) -> &Worker {
        &self.worker_config
    }

    pub(crate) fn install_worker_control_queues(&mut self, queues: Arc<[DataRemoteLocalQueue]>) {
        self.worker_control_queues = queues;
    }

    pub(crate) fn retain_worker_threads(
        &mut self,
        threads: &mut Vec<JoinHandle<RuntimeResult<()>>>,
    ) -> RuntimeResult<()> {
        if !self.worker_threads.is_empty() {
            return Err(RuntimeError::DataWorkersAlreadyStarted);
        }
        self.worker_threads.extend(threads.drain(..));
        Ok(())
    }
}
