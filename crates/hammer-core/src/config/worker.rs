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

use std::time::Duration;

use crate::error::{HammerError, HammerResult};

// hammer-service/src/service.rs
const WORKER_THREADS: usize = 2;
const WORKER_STACK_SIZE: usize = 2 * 1024 * 1024;
const MAX_BLOCKING_THREADS: usize = 4;
// hammer-runtime/src/spawn.rs
const WORKER_IDLE_SLICE: Duration = Duration::from_millis(1);
const BUFFER_SLOT_BYTES: usize = 2_048;
const BUFFER_SLOTS_PER_NUMA: usize = 4_096;
// hammer-core/src/data_plane/buffer.rs
const BUFFER_FRAME_POOL_SIZE: usize = 64;
// hammer-runtime/src/handoff.rs (DataPlaneHandoff::new(workers, cap))
const HANDOFF_QUEUE_CAPACITY: usize = 1_024;
// hammer-runtime/src/app/session.rs AppSessionConfig::DEFAULT
const APP_SESSION_FIFO_CAPACITY: usize = 64 * 1024;
const APP_SESSION_EVENT_QUEUE_CAPACITY: usize = 16;

fn default_instruction_set() -> String {
    "native".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Worker {
    /// Number of dataplane worker threads running the packet graph.
    pub count: usize,
    /// Worker thread stack size in bytes.
    pub stack_size: usize,
    /// Tokio blocking thread pool cap for the worker runtime.
    pub max_blocking_threads: usize,
    /// VPP-style poll interval: how long a worker parks when no packets are
    /// pending. `idle_slice` is kept as the serialized field for existing
    /// configs; `poll_interval` is accepted as an input alias.
    #[serde(with = "humantime_serde", alias = "poll_interval")]
    pub idle_slice: Duration,
    pub buffer: WorkerBuffer,
    pub handoff: WorkerHandoff,
    pub app_session: WorkerAppSession,
    /// CPU instruction set for dataplane batch processing.
    /// Accepted values: "native" (CPU feature-detect), "scalar", "sse2",
    /// "avx2", "avx512", "neon". Default: "native".
    #[serde(default = "default_instruction_set")]
    pub instruction_set: String,
    /// CPU pinning. Linux only; absent on macOS (XNU has no thread
    /// affinity). The three cores are independent: `main_core` runs the
    /// control thread, `app_core` runs the app session/ring runtime, and
    /// `worker_cores` run the dataplane packet graph.
    #[cfg(target_os = "linux")]
    pub cpu: WorkerCpu,
    /// Scheduling policy / QoS. Linux exposes policy+priority; macOS exposes
    /// QoS class. The two shapes are mutually exclusive by target.
    pub scheduler: WorkerScheduler,
    /// NUMA-aware buffer allocation. Linux only.
    #[cfg(target_os = "linux")]
    pub numa: WorkerNuma,
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            count: WORKER_THREADS,
            stack_size: WORKER_STACK_SIZE,
            max_blocking_threads: MAX_BLOCKING_THREADS,
            idle_slice: WORKER_IDLE_SLICE,
            buffer: WorkerBuffer::default(),
            handoff: WorkerHandoff::default(),
            app_session: WorkerAppSession::default(),
            instruction_set: default_instruction_set(),
            #[cfg(target_os = "linux")]
            cpu: WorkerCpu::default(),
            scheduler: WorkerScheduler::default(),
            #[cfg(target_os = "linux")]
            numa: WorkerNuma::default(),
        }
    }
}

impl Worker {
    pub fn is_default(&self) -> bool {
        *self == Worker::default()
    }

    pub fn validate(&self) -> HammerResult<()> {
        if self.count == 0 {
            return Err(HammerError::config_validation(
                "worker.count must be non-zero",
            ));
        }
        if self.stack_size == 0 {
            return Err(HammerError::config_validation(
                "worker.stack_size must be non-zero",
            ));
        }
        self.buffer.validate()?;
        self.handoff.validate()?;
        self.app_session.validate()?;
        #[cfg(target_os = "linux")]
        {
            self.cpu.validate(self.count)?;
            self.numa.validate()?;
        }
        self.scheduler.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerBuffer {
    /// Per-slot byte capacity (VPP `buffers.data-size`).
    pub slot_bytes: usize,
    /// Slots per NUMA node (VPP `buffers.buffers-per-numa`); on non-Linux this
    /// is the total slot count since there is no NUMA partitioning.
    pub slots_per_numa: usize,
    /// Initial buffer frame pool size (`hammer_core::data_plane::DEFAULT_BUFFER_FRAME_POOL_SIZE`).
    pub frame_pool_size: usize,
}

impl Default for WorkerBuffer {
    fn default() -> Self {
        Self {
            slot_bytes: BUFFER_SLOT_BYTES,
            slots_per_numa: BUFFER_SLOTS_PER_NUMA,
            frame_pool_size: BUFFER_FRAME_POOL_SIZE,
        }
    }
}

impl WorkerBuffer {
    fn validate(&self) -> HammerResult<()> {
        if self.slot_bytes == 0 {
            return Err(HammerError::config_validation(
                "worker.buffer.slot_bytes must be non-zero",
            ));
        }
        if self.slots_per_numa == 0 {
            return Err(HammerError::config_validation(
                "worker.buffer.slots_per_numa must be non-zero",
            ));
        }
        if self.frame_pool_size == 0 {
            return Err(HammerError::config_validation(
                "worker.buffer.frame_pool_size must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerHandoff {
    /// Per-worker handoff queue capacity (`DataPlaneHandoff::new(workers, cap)`).
    pub queue_capacity: usize,
    /// Registered internal node handle for the handoff ingress node.
    pub node_handle: u32,
}

impl Default for WorkerHandoff {
    fn default() -> Self {
        Self {
            queue_capacity: HANDOFF_QUEUE_CAPACITY,
            node_handle: 1,
        }
    }
}

impl WorkerHandoff {
    fn validate(&self) -> HammerResult<()> {
        if self.queue_capacity == 0 {
            return Err(HammerError::config_validation(
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
    fn validate(&self) -> HammerResult<()> {
        if self.fifo_capacity == 0 {
            return Err(HammerError::config_validation(
                "worker.app_session.fifo_capacity must be non-zero",
            ));
        }
        if self.evt_q_capacity == 0 {
            return Err(HammerError::config_validation(
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
    fn validate(&self, worker_count: usize) -> HammerResult<()> {
        let mut cores = std::collections::HashSet::new();
        for slot in self.main_core.into_iter().chain(self.app_core) {
            if !cores.insert(slot) {
                return Err(HammerError::config_validation(format!(
                    "worker.cpu core {slot} assigned to more than one role"
                )));
            }
        }
        for core in &self.worker_cores {
            if !cores.insert(*core) {
                return Err(HammerError::config_validation(format!(
                    "worker.cpu core {core} assigned to more than one role"
                )));
            }
        }
        if !self.worker_cores.is_empty() && self.worker_cores.len() != worker_count {
            return Err(HammerError::config_validation(format!(
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
    fn validate(&self) -> HammerResult<()> {
        #[cfg(target_os = "linux")]
        {
            use SchedulerPolicy::*;
            match self.policy {
                Other | Batch | Idle => {
                    if self.priority != 0 {
                        return Err(HammerError::config_validation(
                            "worker.scheduler.priority must be 0 unless policy is fifo/rr",
                        ));
                    }
                }
                Fifo | Rr => {
                    if self.priority < 1 || self.priority > 99 {
                        return Err(HammerError::config_validation(
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
    fn validate(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_defaults_match_production_constants() {
        let worker = Worker::default();
        assert_eq!(worker.count, WORKER_THREADS);
        assert_eq!(worker.stack_size, WORKER_STACK_SIZE);
        assert_eq!(worker.max_blocking_threads, MAX_BLOCKING_THREADS);
        assert_eq!(worker.idle_slice, WORKER_IDLE_SLICE);
        assert_eq!(worker.buffer.slot_bytes, BUFFER_SLOT_BYTES);
        assert_eq!(worker.buffer.slots_per_numa, BUFFER_SLOTS_PER_NUMA);
        assert_eq!(worker.buffer.frame_pool_size, BUFFER_FRAME_POOL_SIZE);
        assert_eq!(worker.handoff.queue_capacity, HANDOFF_QUEUE_CAPACITY);
        assert_eq!(worker.app_session.fifo_capacity, APP_SESSION_FIFO_CAPACITY);
        assert_eq!(
            worker.app_session.evt_q_capacity,
            APP_SESSION_EVENT_QUEUE_CAPACITY
        );
    }

    #[test]
    fn parse_worker_partial_fill() {
        let worker: Worker = toml::from_str("count = 4\n").expect("parse");
        assert_eq!(worker.count, 4);
        assert_eq!(worker.stack_size, WORKER_STACK_SIZE);
    }

    #[test]
    fn parse_worker_idle_slice() {
        let worker: Worker = toml::from_str(r#"idle_slice = "25ms""#).expect("parse");
        assert_eq!(worker.idle_slice, Duration::from_millis(25));
    }

    #[test]
    fn parse_worker_poll_interval_alias() {
        let worker: Worker = toml::from_str(r#"poll_interval = "25ms""#).expect("parse");
        assert_eq!(worker.idle_slice, Duration::from_millis(25));
    }

    #[test]
    fn parse_worker_rejects_obsolete_frame_capacity() {
        let err = toml::from_str::<Worker>(
            r#"
            [buffer]
            frame_capacity = 128
            "#,
        )
        .expect_err("obsolete frame_capacity must be rejected");
        assert!(err.to_string().contains("frame_capacity"));
    }

    #[test]
    fn validate_rejects_zero_count() {
        let worker = Worker {
            count: 0,
            ..Default::default()
        };
        let err = worker.validate().expect_err("reject");
        assert!(err.to_string().contains("worker.count must be non-zero"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn validate_rejects_overlapping_cores() {
        let worker = Worker {
            cpu: WorkerCpu {
                main_core: Some(0),
                app_core: Some(0),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = worker.validate().expect_err("reject");
        assert!(err.to_string().contains("assigned to more than one role"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn validate_rejects_worker_cores_length_mismatch() {
        let worker = Worker {
            count: 2,
            cpu: WorkerCpu {
                worker_cores: vec![1],
                ..Default::default()
            },
            ..Default::default()
        };
        let err = worker.validate().expect_err("reject");
        assert!(err.to_string().contains("must match worker.count"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_worker_linux_scheduler_fifo() {
        let worker: Worker = toml::from_str(
            r#"
[scheduler]
policy = "fifo"
priority = 10
"#,
        )
        .expect("parse");
        assert_eq!(worker.scheduler.policy, SchedulerPolicy::Fifo);
        assert_eq!(worker.scheduler.priority, 10);
        worker.validate().expect("valid fifo scheduler");
    }

    #[test]
    fn instruction_set_defaults_to_native() {
        let worker = Worker::default();
        assert_eq!(worker.instruction_set, "native");
    }

    #[test]
    fn instruction_set_accepts_native() {
        let worker: Worker = toml::from_str("instruction_set = \"native\"").unwrap();
        assert_eq!(worker.instruction_set, "native");
    }

    #[test]
    fn instruction_set_accepts_avx512() {
        let worker: Worker = toml::from_str("instruction_set = \"avx512\"").unwrap();
        assert_eq!(worker.instruction_set, "avx512");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_worker_macos_qos() {
        let worker: Worker = toml::from_str(
            r#"
[scheduler]
qos = "userInteractive"
"#,
        )
        .expect("parse");
        assert_eq!(worker.scheduler.qos, QosClass::UserInteractive);
        worker.validate().expect("valid macos qos");
    }
}
