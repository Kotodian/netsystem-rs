//! `[stats]` configuration for the VPP-shaped statistics collector.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::RuntimeResult;
use crate::file::FILE_MAIN;
use crate::{DataPlaneMain, File, RuntimeError};
use hammer_component_macros::Stats;
use hammer_stats::{StatsMain, Timestamp, stats_segment_socket};

pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const STATS_SEGMENT_SIZE: usize = 32 << 20;

fn default_update_interval() -> Duration {
    DEFAULT_UPDATE_INTERVAL
}

fn deserialize_socket_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let path = <PathBuf as serde::Deserialize>::deserialize(deserializer)?;
    if path.as_os_str().is_empty() {
        return Err(serde::de::Error::custom("stats socket_path is required"));
    }
    Ok(path)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    #[serde(deserialize_with = "deserialize_socket_path")]
    pub socket_path: PathBuf,
    #[serde(default = "default_update_interval", with = "humantime_serde")]
    pub update_interval: Duration,
}

static STATS_CONFIG: OnceLock<StatsConfig> = OnceLock::new();

#[derive(Stats)]
pub(crate) struct Sys {
    heartbeat: Timestamp,
    last_stats_clear: Timestamp,
    boottime: Timestamp,
}

#[hammer_component_macros::process_node(name = "statseg-collector-process")]
fn stat_segment_collector_process(
    _: &mut DataPlaneMain,
) -> impl std::future::Future<Output = RuntimeResult<()>> + Send + 'static {
    async move {
        let sys = Sys::global();
        let stats_main = StatsMain::global()?;
        let config = stats_config();
        let boottime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| RuntimeError::SystemClockBeforeUnixEpoch { source })?
            .as_secs();
        sys.boottime.store(&stats_main, boottime)?;

        loop {
            sys.heartbeat.increment(&stats_main)?;
            tokio::time::sleep(config.update_interval).await;
        }
    }
}

pub(crate) fn stats_config() -> &'static StatsConfig {
    STATS_CONFIG
        .get()
        .expect("stats configuration is installed before stats initialization")
}

#[hammer_component_macros::config_function(
    name = "runtime_stats_config",
    section = "stats",
    early = true,
    required = true
)]
fn configure_stats(config: StatsConfig) -> RuntimeResult<()> {
    assert!(
        STATS_CONFIG.set(config).is_ok(),
        "stats configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(name = "stats_main_init")]
fn init_stats_main(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let config = stats_config();
    if config.socket_path.as_os_str().is_empty() {
        return Err(crate::RuntimeError::ConfigValidation {
            message: "stats socket_path is required".to_owned(),
        });
    }
    if StatsMain::global().is_ok() {
        return Ok(());
    }
    let listener = StatsMain::init("stat segment", STATS_SEGMENT_SIZE, &config.socket_path)?;
    FILE_MAIN
        .get()
        .expect("FileMain is initialized before stats startup")
        .add(File::new(
            listener,
            "stats segment socket".to_owned(),
            0,
            stats_segment_socket::file_functions::<crate::NodeMain, crate::RuntimeError>(),
        ))?;
    Ok(())
}
