//! Runtime-owned startup configuration schemas.
//!
//! Each type is deserialized only for its registered TOML section. There is no
//! runtime aggregate configuration object and no parsed document is retained.

pub mod physmem;
pub mod stats;
pub mod trace;
pub mod worker;

pub use stats::StatsConfig;
pub use trace::{Trace, TraceInput};
pub use worker::CpuConfig;
#[cfg(target_os = "linux")]
pub use worker::WorkerNuma;
pub use worker::{WorkerAppSession, WorkerBuffer, WorkerHandoff};
