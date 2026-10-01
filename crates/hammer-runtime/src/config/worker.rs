//! `[cpu]` and `[worker]` startup configuration.
//!
//! The `[cpu]` section owns the VPP-style CPU topology.
//! `[worker]` owns Data Worker resource sizes; it does not select CPUs or
//! determine the number of Data Workers.
//!
//! Defaults are derived from `hammer-service`/`hammer-runtime`/`hammer-core`
//! production constants (see per-field doc comments for sources).

// Default impls below carry production constants (not zero values) and/or use
// `#[cfg]`-gated fields, so they cannot be replaced by `#[derive(Default)]`.
#![allow(clippy::derivable_impls)]

use std::sync::OnceLock;
use std::time::Duration;

use hammer_component_macros::config_function;
use hammer_infra::{PageSize, bitmap::Bitmap};
use serde::de::DeserializeOwned;
use serde::de::{Deserializer, SeqAccess, Visitor};
use serde::ser::{SerializeSeq, Serializer};
use std::fmt;

use crate::error::{RuntimeError, RuntimeResult};
use crate::file::WorkerFilePollMode;

// hammer-service/src/service.rs
pub(crate) const WORKER_STACK_SIZE: usize = 2 * 1024 * 1024;
pub(crate) const DEFAULT_WORKER_COUNT: usize = 2;
pub(crate) const MAX_BLOCKING_THREADS: usize = 4;
// hammer-runtime/src/spawn.rs
pub(crate) const WORKER_IDLE_SLICE: Duration = Duration::from_millis(1);
const BUFFER_SLOT_BYTES: usize = 2_048;
const BUFFER_SLOTS_PER_NUMA: usize = 4_096;
// hammer-core/src/data_plane/buffer.rs
const BUFFER_FRAME_POOL_SIZE: usize = 64;
// hammer-runtime/src/handoff.rs (DataPlaneHandoff::new(workers, cap))
const HANDOFF_QUEUE_CAPACITY: usize = 1_024;
// hammer-runtime/src/app/session.rs AppSessionConfig::DEFAULT
const APP_SESSION_FIFO_CAPACITY: usize = 64 * 1024;
const APP_SESSION_EVENT_QUEUE_CAPACITY: usize = 16;

static STACK_SIZE: OnceLock<usize> = OnceLock::new();
static MAX_BLOCKING: OnceLock<usize> = OnceLock::new();
static IDLE_SLICE: OnceLock<Duration> = OnceLock::new();
static FILE_POLL: OnceLock<WorkerFilePollMode> = OnceLock::new();
static BUFFER: OnceLock<WorkerBuffer> = OnceLock::new();
static HANDOFF: OnceLock<WorkerHandoff> = OnceLock::new();
static APP_SESSION: OnceLock<WorkerAppSession> = OnceLock::new();
static CPU: OnceLock<CpuConfig> = OnceLock::new();
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
    if STACK_SIZE.get().is_some() {
        return Err(RuntimeError::WorkerConfigurationAlreadyInitialized);
    }

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
    let file_poll = take_value(&mut section, "file_poll", &[])?.unwrap_or_default();
    let buffer: WorkerBuffer = take_value(&mut section, "buffer", &[])?.unwrap_or_default();
    let handoff: WorkerHandoff = take_value(&mut section, "handoff", &[])?.unwrap_or_default();
    let app_session: WorkerAppSession =
        take_value(&mut section, "app_session", &[])?.unwrap_or_default();
    #[cfg(target_os = "linux")]
    let numa: WorkerNuma = take_value(&mut section, "numa", &[])?.unwrap_or_default();

    if let Some((name, _)) = section.iter().next() {
        return Err(RuntimeError::WorkerConfigurationFieldUnknown {
            field: name.clone(),
        });
    }
    if stack_size == 0 {
        return Err(RuntimeError::WorkerStackSizeZero);
    }
    if max_blocking_threads == 0 {
        return Err(RuntimeError::WorkerBlockingThreadCountZero);
    }
    buffer.validate()?;
    handoff.validate()?;
    app_session.validate()?;
    #[cfg(target_os = "linux")]
    numa.validate()?;

    assert!(STACK_SIZE.set(stack_size).is_ok());
    assert!(MAX_BLOCKING.set(max_blocking_threads).is_ok());
    assert!(IDLE_SLICE.set(idle_slice).is_ok());
    assert!(FILE_POLL.set(file_poll).is_ok());
    assert!(BUFFER.set(buffer).is_ok());
    assert!(HANDOFF.set(handoff).is_ok());
    assert!(APP_SESSION.set(app_session).is_ok());
    #[cfg(target_os = "linux")]
    assert!(NUMA.set(numa).is_ok());
    Ok(())
}

pub fn worker_count() -> usize {
    CPU.get()
        .expect("CPU configuration is installed before worker startup")
        .worker_count()
}

pub(crate) fn cpu() -> &'static CpuConfig {
    CPU.get()
        .expect("CPU configuration is installed before thread setup")
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

pub(crate) fn file_poll() -> WorkerFilePollMode {
    *FILE_POLL
        .get()
        .expect("worker configuration is installed before File polling")
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

pub(crate) fn app_session() -> &'static WorkerAppSession {
    APP_SESSION
        .get()
        .expect("worker configuration is installed before App Session setup")
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

/// VPP-style CPU section. `workers` and `corelist_workers` are mutually
/// exclusive: a core bitmap determines the worker count from its set bits.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CpuConfig {
    /// Number of Data Workers when no explicit worker core list is supplied.
    pub workers: Option<usize>,
    /// Core for the control (main) thread. Does not run packets.
    #[serde(alias = "main-core")]
    pub main_core: Option<usize>,
    /// Explicit Data Worker cores, corresponding to VPP `corelist-workers`.
    #[serde(
        alias = "corelist-workers",
        default,
        deserialize_with = "deserialize_corelist",
        serialize_with = "serialize_corelist",
        skip_serializing_if = "bitmap_is_empty"
    )]
    pub corelist_workers: Bitmap,
    /// Number of available CPUs to skip before assigning main/worker CPUs.
    #[serde(alias = "skip-cores")]
    pub skip_cores: usize,
    /// Interpret CPU numbers relative to the process affinity mask.
    pub relative: bool,
}

fn deserialize_corelist<'de, D>(deserializer: D) -> Result<Bitmap, D::Error>
where
    D: Deserializer<'de>,
{
    struct CoreListVisitor;

    impl<'de> Visitor<'de> for CoreListVisitor {
        type Value = Bitmap;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a CPU number sequence or a VPP corelist range string")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bitmap = Bitmap::new();
            while let Some(core) = sequence.next_element::<usize>()? {
                bitmap.set(core);
            }
            Ok(bitmap)
        }

        fn visit_str<E>(self, ranges: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            parse_corelist_ranges(ranges)
        }

        fn visit_string<E>(self, ranges: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_str(&ranges)
        }
    }

    deserializer.deserialize_any(CoreListVisitor)
}

fn parse_corelist_ranges<E>(ranges: &str) -> Result<Bitmap, E>
where
    E: serde::de::Error,
{
    let mut bitmap = Bitmap::new();
    for range in ranges.split(',') {
        let range = range.trim();
        if range.is_empty() {
            return Err(E::custom("cpu.corelist-workers contains an empty item"));
        }
        if let Some((first, last)) = range.split_once('-') {
            let first = first.trim().parse::<usize>().map_err(|_| {
                E::custom("cpu.corelist-workers contains an invalid range")
            })?;
            let last = last
                .trim()
                .parse::<usize>()
                .map_err(|_| E::custom("cpu.corelist-workers contains an invalid range"))?;
            if first > last {
                return Err(E::custom("cpu.corelist-workers range is descending"));
            }
            for core in first..=last {
                bitmap.set(core);
            }
        } else {
            let core = range.parse::<usize>().map_err(|_| {
                E::custom("cpu.corelist-workers contains an invalid CPU number")
            })?;
            bitmap.set(core);
        }
    }
    Ok(bitmap)
}

fn serialize_corelist<S>(bitmap: &Bitmap, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut sequence = serializer.serialize_seq(None)?;
    for core in bitmap.iter_set() {
        sequence.serialize_element(&core)?;
    }
    sequence.end()
}

fn bitmap_is_empty(bitmap: &Bitmap) -> bool {
    bitmap.is_empty()
}

impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            workers: None,
            main_core: None,
            corelist_workers: Bitmap::new(),
            skip_cores: 0,
            relative: false,
        }
    }
}

impl CpuConfig {
    pub(crate) fn worker_count(&self) -> usize {
        self.corelist_workers
            .is_empty()
            .then(|| self.workers.unwrap_or(DEFAULT_WORKER_COUNT))
            .unwrap_or_else(|| self.corelist_workers.count_set())
    }

    pub(crate) fn validate(&self) -> RuntimeResult<()> {
        if self.workers == Some(0) || self.worker_count() == 0 {
            return Err(RuntimeError::WorkerCountZero);
        }
        if !self.corelist_workers.is_empty() && self.workers.is_some() {
            return Err(RuntimeError::config_validation(
                "cpu.workers and cpu.corelist_workers are mutually exclusive",
            ));
        }
        if self.skip_cores != 0 && self.main_core.is_none() {
            return Err(RuntimeError::config_validation(
                "cpu.main_core is required when cpu.skip_cores is set",
            ));
        }
        if !self.corelist_workers.is_empty() && self.main_core.is_none() {
            return Err(RuntimeError::config_validation(
                "cpu.main_core is required when cpu.corelist_workers is set",
            ));
        }
        if self.relative && self.main_core.is_none() {
            return Err(RuntimeError::config_validation(
                "cpu.main_core is required in relative mode",
            ));
        }
        if let Some(main_core) = self.main_core {
            if self.corelist_workers.is_set(main_core) {
                return Err(RuntimeError::config_validation(format!(
                    "cpu core {main_core} assigned to more than one role"
                )));
            }
        }
        Ok(())
    }
}

/// Resolved scheduler policy retained by each runtime thread descriptor.
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
                            "worker scheduler priority must be 0 unless policy is fifo/rr",
                        ));
                    }
                }
                Fifo | Rr => {
                    if self.priority < 1 || self.priority > 99 {
                        return Err(RuntimeError::config_validation(
                            "worker scheduler priority must be 1..=99 for fifo/rr",
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

#[config_function(name = "runtime_cpu_config", section = "cpu", early = true)]
fn configure_cpu(config: CpuConfig) -> RuntimeResult<()> {
    if CPU.get().is_some() {
        return Err(RuntimeError::WorkerConfigurationAlreadyInitialized);
    }
    config.validate()?;
    assert!(CPU.set(config).is_ok());
    Ok(())
}

#[config_function(name = "runtime_worker_config", section = "worker", early = true)]
fn configure_worker(section: toml::Table) -> RuntimeResult<()> {
    install(section)
}
