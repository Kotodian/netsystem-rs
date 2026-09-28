//! Shared-memory Binary API Process and segment lifecycle.

use std::ffi::CString;
use std::fs::{OpenOptions, remove_file};
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use hammer_infra::svm::queue::{SvmQueueError, SvmQueueOperation};
use hammer_infra::svm::region::SvmRegion;
use hammer_ipc::binary_api::memory_shared::MapError;
use hammer_ipc::binary_api::{ApiMain, control, memclnt};
use hammer_runtime::{DataPlaneMain, RuntimeError, RuntimeResult};

static API_SEGMENT_CONFIG: OnceLock<ApiSegmentConfig> = OnceLock::new();

#[hammer_component_macros::config_function(
    name = "api_segment_config",
    section = "api-segment",
    early = true
)]
fn configure_api_segment(config: ApiSegmentConfig) -> RuntimeResult<()> {
    config.validate()?;
    let mut api = ApiMain::new(hammer_ipc::binary_api::MEMCLNT_LAST + 1);
    api.set_api_region_name(config.region_name.clone());
    api.set_api_uid(config.uid);
    api.set_api_gid(config.gid);
    api.set_global_base_va(config.global_base_va as u64);
    api.set_global_size(config.global_size as u64);
    api.set_global_pvt_heap_size(config.global_private_heap_size as u64);
    api.set_api_pvt_heap_size(config.api_private_heap_size as u64);
    api.set_api_size(config.api_size as u64);
    api.install();
    assert!(
        API_SEGMENT_CONFIG.set(config).is_ok(),
        "API segment configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "binary_api_init",
    runs_after = ["vpe_api_init"],
    runs_before = ["stats_main_init"]
)]
fn init() -> RuntimeResult<()> {
    let segment = API_SEGMENT_CONFIG
        .get()
        .expect("API segment configuration is installed before initialization");
    let region_name = segment.region_name.trim_start_matches('/');
    let (root_path, api_path) = if segment.root_path.as_os_str().is_empty() {
        (
            PathBuf::from("/dev/shm/global_vm"),
            PathBuf::from("/dev/shm").join(region_name),
        )
    } else {
        let prefix = if segment.root_path.is_absolute() {
            segment.root_path.clone()
        } else {
            PathBuf::from("/dev/shm").join(&segment.root_path)
        };
        let mut root_name = prefix.as_os_str().to_os_string();
        root_name.push("-global_vm");
        let mut api_name = prefix.as_os_str().to_os_string();
        api_name.push("-");
        api_name.push(region_name);
        (PathBuf::from(root_name), PathBuf::from(api_name))
    };
    for path in [&root_path, &api_path] {
        match remove_file(path) {
            Ok(()) => (),
            Err(source) if source.kind() == io::ErrorKind::NotFound => (),
            Err(source) => {
                return Err(MapError::Open {
                    path: path.clone(),
                    source,
                }
                .into());
            }
        }
    }
    let root_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&root_path)
        .map_err(|source| MapError::Open {
            path: root_path.clone(),
            source,
        })?;
    if unsafe {
        libc::fchown(
            root_file.as_raw_fd(),
            segment.uid as libc::uid_t,
            segment.gid as libc::gid_t,
        )
    } != 0
    {
        let source = io::Error::last_os_error();
        tracing::warn!(path = %root_path.display(), ?source, "API root backing ownership unchanged");
    }
    let api = ApiMain::current();
    let root = SvmRegion::create(
        api.global_base_va(),
        &api.root_region_config(),
        root_file.into(),
    )
    .map_err(|source| MapError::Region { source })?;
    unsafe { api.map_shared_region(root, &api_path, true) }?;
    Ok(())
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_binary_api() -> RuntimeResult<()> {
    if ApiMain::current().is_mapped() {
        unsafe { ApiMain::current().unmap_shared_regions() }
            .map_err(|source| RuntimeError::from(MapError::Region { source }))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct ApiSegmentConfig {
    pub region_name: String,
    #[serde(rename = "prefix")]
    pub root_path: PathBuf,
    #[serde(deserialize_with = "deserialize_uid")]
    pub uid: u32,
    #[serde(deserialize_with = "deserialize_gid")]
    pub gid: u32,
    #[serde(rename = "baseva")]
    pub global_base_va: usize,
    #[serde(deserialize_with = "deserialize_size")]
    pub global_size: usize,
    #[serde(rename = "global-pvt-heap-size", deserialize_with = "deserialize_size")]
    pub global_private_heap_size: usize,
    #[serde(rename = "api-pvt-heap-size", deserialize_with = "deserialize_size")]
    pub api_private_heap_size: usize,
    #[serde(deserialize_with = "deserialize_size")]
    pub api_size: usize,
}

impl Default for ApiSegmentConfig {
    fn default() -> Self {
        Self {
            region_name: "/vpe-api".to_owned(),
            root_path: PathBuf::new(),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            global_base_va: 0x1_3000_0000,
            global_size: 64 << 20,
            global_private_heap_size: 128 << 10,
            api_private_heap_size: 128 << 10,
            api_size: 16 << 20,
        }
    }
}

impl ApiSegmentConfig {
    fn validate(&self) -> RuntimeResult<()> {
        if self.region_name.is_empty() {
            return Err(RuntimeError::config_validation(
                "api-segment.region-name must not be empty",
            ));
        }
        if self.global_base_va == 0 {
            return Err(RuntimeError::config_validation(
                "api-segment.baseva must be non-zero",
            ));
        }
        for (name, bytes) in [
            ("global-size", self.global_size),
            ("global-pvt-heap-size", self.global_private_heap_size),
            ("api-pvt-heap-size", self.api_private_heap_size),
            ("api-size", self.api_size),
        ] {
            if bytes == 0 {
                return Err(RuntimeError::config_validation(format!(
                    "api-segment.{name} must be non-zero"
                )));
            }
        }
        Ok(())
    }
}

fn deserialize_uid<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Uid;
    impl<'de> serde::de::Visitor<'de> for Uid {
        type Value = u32;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a numeric uid or account name")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u32, E> {
            let name = CString::new(value).map_err(E::custom)?;
            let entry = unsafe { libc::getpwnam(name.as_ptr()) };
            if entry.is_null() {
                return Err(E::custom(format_args!("account `{value}` does not exist")));
            }
            Ok(unsafe { (*entry).pw_uid })
        }
    }
    deserializer.deserialize_any(Uid)
}

fn deserialize_gid<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Gid;
    impl<'de> serde::de::Visitor<'de> for Gid {
        type Value = u32;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a numeric gid or group name")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<u32, E> {
            u32::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u32, E> {
            let name = CString::new(value).map_err(E::custom)?;
            let entry = unsafe { libc::getgrnam(name.as_ptr()) };
            if entry.is_null() {
                return Err(E::custom(format_args!("group `{value}` does not exist")));
            }
            Ok(unsafe { (*entry).gr_gid })
        }
    }
    deserializer.deserialize_any(Gid)
}

fn deserialize_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Size;
    impl<'de> serde::de::Visitor<'de> for Size {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a byte count or a number followed by M or G")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<usize, E> {
            usize::try_from(value).map_err(E::custom)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<usize, E> {
            usize::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<usize, E> {
            let value = value.trim();
            let (number, multiplier) = match value.as_bytes().last() {
                Some(b'M') => (&value[..value.len() - 1], 1_u64 << 20),
                Some(b'G') => (&value[..value.len() - 1], 1_u64 << 30),
                _ => (value, 1),
            };
            let bytes = number.trim().parse::<u64>().map_err(E::custom)?;
            let bytes = bytes
                .checked_mul(multiplier)
                .ok_or_else(|| E::custom("API segment size overflows u64"))?;
            usize::try_from(bytes).map_err(E::custom)
        }
    }
    deserializer.deserialize_any(Size)
}

#[hammer_component_macros::process_node(name = "binary-api")]
fn binary_api_clnt(
    main: &mut DataPlaneMain,
) -> impl std::future::Future<Output = RuntimeResult<()>> + Send + 'static {
    let api = ApiMain::current();
    let mapped = api.is_mapped();
    let initialized = (|| {
        control::setup_message_id_table(api);
        hammer_runtime::init::run_api_init(main)
    })();
    async move {
        initialized?;
        if !mapped {
            return Ok(());
        }
        let scan_interval = Duration::from_secs(10);
        let mut next_scan = Instant::now() + scan_interval;
        loop {
            let drain_started = Instant::now();
            loop {
                match memclnt::receive() {
                    Ok(false) => break,
                    Ok(true) => (),
                    Err(
                        source @ SvmQueueError::SignalAfterCommit {
                            operation: SvmQueueOperation::Sub,
                            ..
                        },
                    )
                    | Err(
                        source @ SvmQueueError::EventSignalAfterCommit {
                            operation: SvmQueueOperation::Sub,
                            ..
                        },
                    ) => tracing::warn!(?source, "memory API dequeue notification failed"),
                    Err(source) => {
                        tracing::error!(?source, "memory API input disabled after queue error");
                        return Ok(());
                    }
                }
                if drain_started.elapsed() >= Duration::from_micros(10) {
                    break;
                }
            }
            let now = Instant::now();
            if now >= next_scan {
                hammer_runtime::worker_thread_barrier_sync!({
                    api.dead_client_scan(now);
                });
                next_scan = Instant::now() + scan_interval;
            }
            let wake_at = (Instant::now() + Duration::from_micros(400)).min(next_scan);
            tokio::task::yield_now().await;
            tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)).await;
        }
    }
}
