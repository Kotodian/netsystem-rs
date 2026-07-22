//! Runtime-owned startup configuration schemas.
//!
//! Each type is deserialized only for its registered TOML section. There is no
//! runtime aggregate configuration object and no parsed document is retained.

pub mod memory;
pub mod trace;
pub mod worker;

pub use memory::Memory;
pub use trace::{Trace, TraceInput};
#[cfg(target_os = "macos")]
pub use worker::QosClass;
#[cfg(target_os = "linux")]
pub use worker::{SchedulerPolicy, WorkerCpu, WorkerNuma};
pub use worker::{Worker, WorkerScheduler};
