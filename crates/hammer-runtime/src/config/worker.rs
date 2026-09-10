//! `[worker]` config section: dataplane worker thread model + CPU/scheduler/NUMA.
//!
//! Main thread does not run packets. Worker threads run the packet graph.
//! The `app` runtime (app session FIFO/message queue) runs on its own core, distinct
//! from both the main (control) core and the worker (dataplane) cores — the
//! three are independent, mirroring VPP's separation of main-core, worker
//! cores, and any control/app work that must not contend with packet processing.
//!
//! Platform surfaces:
//! - Linux: `[worker.cpu]` (main/app/worker cores), `[worker.scheduler]`
//!   (policy/priority), `[worker.numa]`.
//! - macOS: `[worker.scheduler]` (qos). No CPU affinity or NUMA on XNU.
//!
//! Defaults are derived from `hammer-service`/`hammer-runtime`/`hammer-core`
//! production constants (see per-field doc comments for sources).

// Default impls below carry production constants (not zero values) and/or use
// `#[cfg]`-gated fields, so they cannot be replaced by `#[derive(Default)]`.
#![allow(clippy::derivable_impls)]

use std::sync::OnceLock;
use std::time::Duration;

use hammer_infra::PageSize;
use serde::de::DeserializeOwned;

use crate::error::{RuntimeError, RuntimeResult};

// hammer-service/src/service.rs
pub(crate) const WORKER_THREADS: usize = 2;
pub(crate) const WORKER_STACK_SIZE: usize = 2 * 1024 * 1024;
pub(crate) const MAX_BLOCKING_THREADS: usize = 4;
// hammer-runtime/src/spawn.rs
pub(crate) const WORKER_IDLE_SLICE: Duration = Duration::from_millis(1);
const BUFFER_SLOT_BYTES: usize = 2_048;
const BUFFER_SLOTS_PER_NUMA: usize = 4_096;
// hammer-core/src/data_plane/buffer.rs
const BUFFER_FRAME_POOL_SIZE: usize = 64;
// hammer-runtime/src/handoff.rs (DataPlaneHandoff::new(workers, cap))
const HANDOFF_QUEUE_CAPACITY: usize = 1_024;
// hammer-runtime/src/spawn.rs (DataRemoteLocalQueue::new(cap))
const WORKER_CONTROL_QUEUE_CAPACITY: usize = 1_024;
// hammer-runtime/src/app/session.rs AppSessionConfig::DEFAULT
const APP_SESSION_FIFO_CAPACITY: usize = 64 * 1024;
const APP_SESSION_EVENT_QUEUE_CAPACITY: usize = 16;

static COUNT: OnceLock<usize> = OnceLock::new();
static STACK_SIZE: OnceLock<usize> = OnceLock::new();
static MAX_BLOCKING: OnceLock<usize> = OnceLock::new();
static IDLE_SLICE: OnceLock<Duration> = OnceLock::new();
static BUFFER: OnceLock<WorkerBuffer> = OnceLock::new();
static HANDOFF: OnceLock<WorkerHandoff> = OnceLock::new();
static CONTROL: OnceLock<WorkerControl> = OnceLock::new();
static APP_SESSION: OnceLock<WorkerAppSession> = OnceLock::new();
static SCHEDULER: OnceLock<WorkerScheduler> = OnceLock::new();
#[cfg(target_os = "linux")]
static CPU: OnceLock<WorkerCpu> = OnceLock::new();
#[cfg(target_os = "linux")]
static NUMA: OnceLock<WorkerNuma> = OnceLock::new();

fn take_value<T: DeserializeOwned>(
    section: &mut toml::Table,
    name: &'static str,
    aliases: &[&'static str],
) -> RuntimeResult<Option<T>> {
    let mut value = section.remove(name);
    for alias in aliases {
        if let Some(alias_value) = section.remove(*alias) {
            if value.is_some() {
                return Err(RuntimeError::WorkerConfigurationFieldDuplicate { field: name, alias });
            }
            value = Some(alias_value);
        }
    }
    value
        .map(|value| value.try_into::<T>())
        .transpose()
        .map_err(|source| RuntimeError::WorkerConfigurationFieldParse {
            field: name,
            source,
        })
}

pub(crate) fn install(mut section: toml::Table) -> RuntimeResult<()> {
    if COUNT.get().is_some() {
        return Err(RuntimeError::WorkerConfigurationAlreadyInitialized);
    }

    let count = take_value(&mut section, "count", &["workers"])?.unwrap_or(WORKER_THREADS);
    let stack_size = take_value(&mut section, "stack_size", &[])?.unwrap_or(WORKER_STACK_SIZE);
    let max_blocking_threads =
        take_value(&mut section, "max_blocking_threads", &[])?.unwrap_or(MAX_BLOCKING_THREADS);
    let idle_slice = take_value::<humantime_serde::Serde<Duration>>(
        &mut section,
        "idle_slice",
        &["poll_interval", "poll_sleep"],
    )?
    .map(humantime_serde::Serde::into_inner)
    .unwrap_or(WORKER_IDLE_SLICE);
    let buffer: WorkerBuffer = take_value(&mut section, "buffer", &[])?.unwrap_or_default();
    let handoff: WorkerHandoff = take_value(&mut section, "handoff", &[])?.unwrap_or_default();
    let control: WorkerControl = take_value(&mut section, "control", &[])?.unwrap_or_default();
    let app_session: WorkerAppSession =
        take_value(&mut section, "app_session", &[])?.unwrap_or_default();
    let scheduler: WorkerScheduler =
        take_value(&mut section, "scheduler", &[])?.unwrap_or_default();
    #[cfg(target_os = "linux")]
    let cpu: WorkerCpu = take_value(&mut section, "cpu", &[])?.unwrap_or_default();
    #[cfg(target_os = "linux")]
    let numa: WorkerNuma = take_value(&mut section, "numa", &[])?.unwrap_or_default();

    if let Some((name, _)) = section.iter().next() {
        return Err(RuntimeError::WorkerConfigurationFieldUnknown {
            field: name.clone(),
        });
    }
    if count == 0 {
        return Err(RuntimeError::WorkerCountZero);
    }
    if stack_size == 0 {
        return Err(RuntimeError::WorkerStackSizeZero);
    }
    if max_blocking_threads == 0 {
        return Err(RuntimeError::WorkerBlockingThreadCountZero);
    }
    buffer.validate()?;
    handoff.validate()?;
    control.validate()?;
    app_session.validate()?;
    scheduler.validate()?;
    #[cfg(target_os = "linux")]
    {
        cpu.validate(count)?;
        numa.validate()?;
    }

    assert!(COUNT.set(count).is_ok());
    assert!(STACK_SIZE.set(stack_size).is_ok());
    assert!(MAX_BLOCKING.set(max_blocking_threads).is_ok());
    assert!(IDLE_SLICE.set(idle_slice).is_ok());
    assert!(BUFFER.set(buffer).is_ok());
    assert!(HANDOFF.set(handoff).is_ok());
    assert!(CONTROL.set(control).is_ok());
    assert!(APP_SESSION.set(app_session).is_ok());
    assert!(SCHEDULER.set(scheduler).is_ok());
    #[cfg(target_os = "linux")]
    {
        assert!(CPU.set(cpu).is_ok());
        assert!(NUMA.set(numa).is_ok());
    }
    Ok(())
}

pub fn worker_count() -> usize {
    *COUNT
        .get()
        .expect("worker configuration is installed before lifecycle initialization")
}

pub(crate) fn stack_size() -> usize {
    *STACK_SIZE
        .get()
        .expect("worker configuration is installed before thread setup")
}

pub(crate) fn max_blocking_threads() -> usize {
    *MAX_BLOCKING
        .get()
        .expect("worker configuration is installed before thread setup")
}

pub(crate) fn idle_slice() -> Duration {
    *IDLE_SLICE
        .get()
        .expect("worker configuration is installed before thread setup")
}

pub(crate) fn buffer() -> &'static WorkerBuffer {
    BUFFER
        .get()
        .expect("worker configuration is installed before Buffer setup")
}

pub(crate) fn handoff() -> &'static WorkerHandoff {
    HANDOFF
        .get()
        .expect("worker configuration is installed before handoff setup")
}

pub(crate) fn control() -> &'static WorkerControl {
    CONTROL
        .get()
        .expect("worker configuration is installed before worker control setup")
}

pub(crate) fn app_session() -> &'static WorkerAppSession {
    APP_SESSION
        .get()
        .expect("worker configuration is installed before App Session setup")
}

pub(crate) fn scheduler() -> &'static WorkerScheduler {
    SCHEDULER
        .get()
        .expect("worker configuration is installed before thread setup")
}

#[cfg(target_os = "linux")]
pub(crate) fn cpu() -> &'static WorkerCpu {
    CPU.get()
        .expect("worker configuration is installed before CPU setup")
}

#[cfg(target_os = "linux")]
pub(crate) fn numa() -> &'static WorkerNuma {
    NUMA.get()
        .expect("worker configuration is installed before NUMA setup")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerBuffer {
    /// Per-slot byte capacity (VPP `buffers.data-size`).
    #[serde(alias = "data_size")]
    pub slot_bytes: usize,
    /// Slots per NUMA node (VPP `buffers.buffers-per-numa`); on non-Linux this
    /// is the total slot count since there is no NUMA partitioning.
    #[serde(alias = "buffers_per_numa")]
    pub slots_per_numa: usize,
    /// Initial buffer frame pool size (`hammer_core::data_plane::DEFAULT_BUFFER_FRAME_POOL_SIZE`).
    pub frame_pool_size: usize,
    /// Packet storage page policy. Omission requests default HugeTLB with
    /// runtime-owned ordinary-page fallback.
    pub page_size: Option<PageSize>,
}

impl Default for WorkerBuffer {
    fn default() -> Self {
        Self {
            slot_bytes: BUFFER_SLOT_BYTES,
            slots_per_numa: BUFFER_SLOTS_PER_NUMA,
            frame_pool_size: BUFFER_FRAME_POOL_SIZE,
            page_size: None,
        }
    }
}

impl WorkerBuffer {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        if self.slot_bytes == 0 {
            return Err(RuntimeError::config_validation(
                "worker.buffer.slot_bytes must be non-zero",
            ));
        }
        if self.slots_per_numa == 0 {
            return Err(RuntimeError::config_validation(
                "worker.buffer.slots_per_numa must be non-zero",
            ));
        }
        if self.frame_pool_size == 0 {
            return Err(RuntimeError::config_validation(
                "worker.buffer.frame_pool_size must be non-zero",
            ));
        }
        if self
            .page_size
            .is_some_and(|page_size| !page_size.is_supported_on_current_platform())
        {
            return Err(RuntimeError::config_validation(
                "worker.buffer.page_size is unsupported on this platform",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerHandoff {
    /// Per-worker packet handoff queue capacity.
    pub queue_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerControl {
    /// Per-worker Main Thread to Data Worker control queue capacity.
    pub queue_capacity: usize,
}

impl Default for WorkerControl {
    fn default() -> Self {
        Self {
            queue_capacity: WORKER_CONTROL_QUEUE_CAPACITY,
        }
    }
}

impl WorkerControl {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        if self.queue_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "worker.control.queue_capacity must be non-zero",
            ));
        }
        Ok(())
    }
}

impl Default for WorkerHandoff {
    fn default() -> Self {
        Self {
            queue_capacity: HANDOFF_QUEUE_CAPACITY,
        }
    }
}

impl WorkerHandoff {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        if self.queue_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "worker.handoff.queue_capacity must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerAppSession {
    /// Per-session RX/TX FIFO capacity.
    pub fifo_capacity: usize,
    /// Usable event queue entries per session.
    pub evt_q_capacity: usize,
}

impl Default for WorkerAppSession {
    fn default() -> Self {
        Self {
            fifo_capacity: APP_SESSION_FIFO_CAPACITY,
            evt_q_capacity: APP_SESSION_EVENT_QUEUE_CAPACITY,
        }
    }
}

impl WorkerAppSession {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        if self.fifo_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "worker.app_session.fifo_capacity must be non-zero",
            ));
        }
        if self.evt_q_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "worker.app_session.evt_q_capacity must be non-zero",
            ));
        }
        Ok(())
    }
}

/// CPU pinning (Linux only). The three core slots are independent:
/// `main_core` runs the control thread (no packets), `app_core` runs the app
/// session FIFO/message queue runtime, and `worker_cores` run the dataplane packet graph.
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerCpu {
    /// Core for the control (main) thread. Does not run packets.
    pub main_core: Option<usize>,
    /// Core for the app session FIFO/message queue runtime. Independent of worker cores.
    pub app_core: Option<usize>,
    /// Cores for dataplane worker threads. When empty, the runtime pins
    /// workers automatically (skipping main/app cores). When set, its length
    /// must match `worker.count`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_cores: Vec<usize>,
}

#[cfg(target_os = "linux")]
impl Default for WorkerCpu {
    fn default() -> Self {
        Self {
            main_core: None,
            app_core: None,
            worker_cores: Vec::new(),
        }
    }
}

#[cfg(target_os = "linux")]
impl WorkerCpu {
    pub(crate) fn validate(&self, worker_count: usize) -> RuntimeResult<()> {
        let mut cores = std::collections::HashSet::new();
        for slot in self.main_core.into_iter().chain(self.app_core) {
            if !cores.insert(slot) {
                return Err(RuntimeError::config_validation(format!(
                    "worker.cpu core {slot} assigned to more than one role"
                )));
            }
        }
        for core in &self.worker_cores {
            if !cores.insert(*core) {
                return Err(RuntimeError::config_validation(format!(
                    "worker.cpu core {core} assigned to more than one role"
                )));
            }
        }
        if !self.worker_cores.is_empty() && self.worker_cores.len() != worker_count {
            return Err(RuntimeError::config_validation(format!(
                "worker.cpu.worker_cores length ({}) must match worker.count ({})",
                self.worker_cores.len(),
                worker_count
            )));
        }
        Ok(())
    }
}

/// Scheduling. Linux: policy + priority. macOS: QoS class. The two shapes are
/// discriminated by target.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerScheduler {
    /// Linux scheduling policy. Ignored on macOS.
    #[cfg(target_os = "linux")]
    pub policy: SchedulerPolicy,
    /// Linux scheduling priority (only meaningful for `fifo`/`rr`).
    #[cfg(target_os = "linux")]
    pub priority: i32,
    /// macOS QoS class. Ignored on Linux.
    #[cfg(target_os = "macos")]
    pub qos: QosClass,
}

impl Default for WorkerScheduler {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            policy: SchedulerPolicy::default(),
            #[cfg(target_os = "linux")]
            priority: 0,
            #[cfg(target_os = "macos")]
            qos: QosClass::default(),
        }
    }
}

impl WorkerScheduler {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        #[cfg(target_os = "linux")]
        {
            use SchedulerPolicy::*;
            match self.policy {
                Other | Batch | Idle => {
                    if self.priority != 0 {
                        return Err(RuntimeError::config_validation(
                            "worker.scheduler.priority must be 0 unless policy is fifo/rr",
                        ));
                    }
                }
                Fifo | Rr => {
                    if self.priority < 1 || self.priority > 99 {
                        return Err(RuntimeError::config_validation(
                            "worker.scheduler.priority must be 1..=99 for fifo/rr",
                        ));
                    }
                }
            }
        }
        let _ = self;
        Ok(())
    }
}

/// Linux scheduling policy (mirrors VPP `scheduler-policy`).
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SchedulerPolicy {
    #[default]
    Other,
    Batch,
    Idle,
    Fifo,
    Rr,
}

/// macOS QoS class (`pthread_set_qos_class_self_np`).
#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum QosClass {
    UserInteractive,
    UserInitiated,
    #[default]
    Default,
    Utility,
    Background,
}

/// NUMA-aware buffer allocation (Linux only).
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerNuma {
    /// When true, allocate per-worker buffers from the NUMA node local to the
    /// worker's pinned core (probed via `getcpu`). When false, buffers come
    /// from the default arena.
    pub enabled: bool,
}

#[cfg(target_os = "linux")]
impl Default for WorkerNuma {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[cfg(target_os = "linux")]
impl WorkerNuma {
    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        Ok(())
    }
}
