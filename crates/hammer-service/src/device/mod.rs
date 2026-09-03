//! Device-input authority. Interface instances and queues are owned by
//! [`crate::interface::InterfaceMain`]; this type only owns worker scheduling
//! and receive accounting.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use hammer_runtime::{DataWorkerId, GlobalMain, RuntimeError, RuntimeResult};

pub use crate::interface_model::{DeviceClass, HwClass, HwInterface, SwInterface};

#[derive(Debug, thiserror::Error)]
pub enum DeviceError {
    #[error("device worker index {worker} is outside range {first}..{last}")]
    WorkerOutOfRange {
        worker: usize,
        first: usize,
        last: usize,
    },
    #[error("device input queue {queue_index} is not registered")]
    QueueNotRegistered { queue_index: u32 },
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

#[derive(Clone)]
pub struct DeviceMain {
    workers: Arc<Vec<AtomicU64>>,
    first_worker_thread_index: usize,
    last_worker_thread_index: usize,
    next_worker_thread_index: usize,
}

unsafe impl Send for DeviceMain {}
unsafe impl Sync for DeviceMain {}

impl DeviceMain {
    pub fn new(worker_count: usize) -> Self {
        Self {
            workers: Arc::new((0..worker_count).map(|_| AtomicU64::new(0)).collect()),
            first_worker_thread_index: 0,
            last_worker_thread_index: worker_count,
            next_worker_thread_index: 0,
        }
    }

    pub fn global() -> RuntimeResult<&'static DeviceMain> {
        DEVICE_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::device::DeviceMain",
            })
    }

    pub fn init(engine: &mut GlobalMain) -> RuntimeResult<Arc<DeviceMain>> {
        let main = DeviceMain::new(engine.configured_worker_count());
        let shared = Arc::new(main.clone());
        DEVICE_MAIN
            .set(main)
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "device" })?;
        Ok(shared)
    }

    pub fn worker_range(&self) -> Range<usize> {
        self.first_worker_thread_index..self.last_worker_thread_index
    }
    pub fn increment_rx_packets(
        &self,
        worker: DataWorkerId,
        count: u64,
    ) -> Result<(), DeviceError> {
        let slot = worker.slot();
        let counter = self
            .workers
            .get(slot)
            .ok_or(DeviceError::WorkerOutOfRange {
                worker: slot,
                first: self.first_worker_thread_index,
                last: self.last_worker_thread_index,
            })?;
        counter.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    pub fn aggregate_rx_packets(&self) -> u64 {
        self.workers
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    }

    pub fn next_worker(&mut self) -> Option<DataWorkerId> {
        if self.next_worker_thread_index >= self.last_worker_thread_index {
            return None;
        }
        let worker = DataWorkerId::new(u32::try_from(self.next_worker_thread_index).ok()?);
        self.next_worker_thread_index += 1;
        Some(worker)
    }

    pub fn schedule_input(&mut self, worker: DataWorkerId, queue_index: u32) -> RuntimeResult<()> {
        if worker.slot() >= self.last_worker_thread_index {
            return Err(RuntimeError::subsystem(
                "device",
                DeviceError::WorkerOutOfRange {
                    worker: worker.slot(),
                    first: self.first_worker_thread_index,
                    last: self.last_worker_thread_index,
                },
            ));
        }
        if queue_index == u32::MAX {
            return Err(RuntimeError::subsystem(
                "device",
                DeviceError::QueueNotRegistered { queue_index },
            ));
        }
        Ok(())
    }
}

pub static DEVICE_MAIN: OnceLock<DeviceMain> = OnceLock::new();

#[hammer_component_macros::init_function(name = "device_main_init", runs_after = ["net_main_init"])]
fn init_device_main(engine: &mut GlobalMain) -> RuntimeResult<Arc<DeviceMain>> {
    DeviceMain::init(engine)
}
